package mq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/glebarez/sqlite"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type sampleOrderEvent struct {
	OrderNo string `json:"order_no"`
	Amount  int64  `json:"amount"`
}

func TestMemoryBroker_QueueAndBroadcastAndContextPropagation(t *testing.T) {
	pkgredis.SetRedis(nil)

	tp, err := trace.InitTracer(trace.Config{ServiceName: "test-mq-memory"})
	require.NoError(t, err)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	broker, err := NewBroker(Config{
		Driver:      DriverMemory,
		ServiceName: "service-order",
	})
	require.NoError(t, err)
	defer func() { _ = broker.Close(context.Background()) }()

	// 1. 验证同组竞争消费 (Queue 模式)：两个同属 "worker-group" 的订阅者合计只消费每条消息 1 次
	var groupTotal int32
	err = broker.Subscribe("blade.order.created", func(ctx context.Context, msg Message) error {
		atomic.AddInt32(&groupTotal, 1)
		return nil
	}, WithGroup("worker-group"))
	require.NoError(t, err)

	err = broker.Subscribe("blade.order.created", func(ctx context.Context, msg Message) error {
		atomic.AddInt32(&groupTotal, 1)
		return nil
	}, WithGroup("worker-group"))
	require.NoError(t, err)

	// 2. 验证广播模式 (WithBroadcast)：两个广播订阅者都能收到全量消息，且能还原 TenantID 和 TraceID
	var broadcast1Count, broadcast2Count int32
	receivedTenant := make(chan string, 1)
	receivedTrace := make(chan string, 1)

	err = broker.Subscribe("blade.cache.invalidate", func(ctx context.Context, msg Message) error {
		atomic.AddInt32(&broadcast1Count, 1)
		select {
		case receivedTenant <- tenant.FromTenant(ctx):
		default:
		}
		select {
		case receivedTrace <- trace.TraceIDFromContext(ctx):
		default:
		}
		return nil
	}, WithBroadcast())
	require.NoError(t, err)

	err = broker.Subscribe("blade.cache.invalidate", func(ctx context.Context, msg Message) error {
		atomic.AddInt32(&broadcast2Count, 1)
		return nil
	}, WithBroadcast())
	require.NoError(t, err)

	for i := 0; i < 6; i++ {
		require.NoError(t, broker.Publish(context.Background(), "blade.order.created", sampleOrderEvent{
			OrderNo: "ORD-1",
			Amount:  100,
		}))
	}

	pubCtx := tenant.WithTenant(context.Background(), "888888")
	pubCtx, span := trace.StartSpan(pubCtx, "publish-cache-event")
	expectedTraceID := trace.TraceIDFromContext(pubCtx)
	require.NotEmpty(t, expectedTraceID)

	require.NoError(t, broker.Publish(pubCtx, "blade.cache.invalidate", map[string]string{"role": "admin"}))
	span.End()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&groupTotal) == 6 &&
			atomic.LoadInt32(&broadcast1Count) == 1 &&
			atomic.LoadInt32(&broadcast2Count) == 1
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, "888888", <-receivedTenant)
	assert.Equal(t, expectedTraceID, <-receivedTrace)
}

func TestMemoryBroker_RetryAndDLQAndPanicRecovery(t *testing.T) {
	pkgredis.SetRedis(nil)

	broker := NewMemoryBroker(Config{ServiceName: "test-dlq"})
	defer func() { _ = broker.Close(context.Background()) }()

	dlqCh := make(chan Message, 2)
	err := broker.Subscribe("blade.payment.charge.dlq", func(ctx context.Context, msg Message) error {
		dlqCh <- msg
		return nil
	})
	require.NoError(t, err)

	var attempts int32
	err = broker.Subscribe("blade.payment.charge", func(ctx context.Context, msg Message) error {
		cur := atomic.AddInt32(&attempts, 1)
		if cur == 1 {
			panic("unexpected nil pointer in payment handler")
		}
		return errors.New("gateway timeout")
	}, WithMaxRetries(2), WithRetryBackoff(5*time.Millisecond))
	require.NoError(t, err)

	require.NoError(t, broker.Publish(context.Background(), "blade.payment.charge", `{"order_id":99}`, WithKey("99")))

	select {
	case dlqMsg := <-dlqCh:
		assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
		assert.Equal(t, "blade.payment.charge", dlqMsg.Headers[HeaderOriginalTopic])
		assert.Contains(t, dlqMsg.Headers[HeaderDeadReason], "gateway timeout")
		assert.Equal(t, "2", dlqMsg.Headers[HeaderRetryCount])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message in DLQ")
	}
}

func TestRedisStreamBroker_StreamAndDelayedDelivery(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	_, err = pkgredis.InitRedis(pkgredis.Config{Host: mr.Addr()})
	require.NoError(t, err)
	defer pkgredis.SetRedis(nil)

	broker, err := NewBroker(Config{
		Driver:            DriverRedis,
		ServiceName:       "service-system",
		BlockTimeout:      20 * time.Millisecond,
		DelayPollInterval: 15 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = broker.Close(context.Background()) }()

	receivedCh := make(chan sampleOrderEvent, 4)
	err = broker.Subscribe("blade.stream.orders", func(ctx context.Context, msg Message) error {
		var ev sampleOrderEvent
		if err := msg.DecodeJSON(&ev); err != nil {
			return err
		}
		receivedCh <- ev
		return nil
	}, WithGroup("order-consumers"))
	require.NoError(t, err)

	// 1. 测试即时 Redis Stream 消息
	require.NoError(t, broker.Publish(context.Background(), "blade.stream.orders", sampleOrderEvent{
		OrderNo: "NOW-001",
		Amount:  200,
	}))

	select {
	case ev := <-receivedCh:
		assert.Equal(t, "NOW-001", ev.OrderNo)
		assert.Equal(t, int64(200), ev.Amount)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for immediate redis stream message")
	}

	// 2. 测试基于 Redis ZSet 的分布式延迟消息
	start := time.Now()
	require.NoError(t, broker.Publish(context.Background(), "blade.stream.orders", sampleOrderEvent{
		OrderNo: "DELAY-002",
		Amount:  500,
	}, WithDelay(60*time.Millisecond)))

	select {
	case ev := <-receivedCh:
		assert.Equal(t, "DELAY-002", ev.OrderNo)
		assert.GreaterOrEqual(t, time.Since(start), 45*time.Millisecond)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delayed redis stream message")
	}
}

func TestKafkaBroker_HeadersPartitionAndBroadcast(t *testing.T) {
	pkgredis.SetRedis(nil)

	broker, err := NewBroker(Config{
		Driver:      DriverKafka,
		ServiceName: "service-gateway",
		Kafka: KafkaConfig{
			Brokers:    []string{"127.0.0.1:9092"},
			Partitions: 4,
		},
	})
	require.NoError(t, err)
	defer func() { _ = broker.Close(context.Background()) }()

	var node1Count, node2Count int32
	tenantCh := make(chan string, 2)

	require.NoError(t, broker.Subscribe("blade.kafka.rbac", func(ctx context.Context, msg Message) error {
		atomic.AddInt32(&node1Count, 1)
		tenantCh <- tenant.FromTenant(ctx)
		return nil
	}, WithBroadcast()))

	require.NoError(t, broker.Subscribe("blade.kafka.rbac", func(ctx context.Context, msg Message) error {
		atomic.AddInt32(&node2Count, 1)
		return nil
	}, WithBroadcast()))

	ctx := tenant.WithTenant(context.Background(), "990001")
	require.NoError(t, broker.Publish(ctx, "blade.kafka.rbac", map[string]any{"action": "grant"}, WithKey("role-1")))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&node1Count) == 1 && atomic.LoadInt32(&node2Count) == 1
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, "990001", <-tenantCh)
}

type mockBusinessOrder struct {
	ID      int64  `gorm:"primaryKey"`
	OrderNo string `gorm:"size:64"`
}

func TestTransactionalOutbox_CommitAndRollback(t *testing.T) {
	pkgredis.SetRedis(nil)

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&mockBusinessOrder{}, &OutboxRecord{}))

	broker := NewMemoryBroker(Config{ServiceName: "service-outbox-test"})
	defer func() { _ = broker.Close(context.Background()) }()

	received := make(chan sampleOrderEvent, 4)
	require.NoError(t, broker.Subscribe("blade.outbox.order", func(ctx context.Context, msg Message) error {
		var ev sampleOrderEvent
		if uErr := msg.DecodeJSON(&ev); uErr != nil {
			return uErr
		}
		received <- ev
		return nil
	}))

	relay := NewOutboxRelay(db, broker, OutboxRelayOptions{
		PollInterval: 20 * time.Millisecond,
		BatchSize:    10,
	})

	ctx := tenant.WithTenant(context.Background(), "000000")

	// 1. 验证业务事务回滚时：Outbox 表中无记录，且绝不会有消息发往 Broker
	rollbackErr := ExecuteInTx(ctx, db, "blade.outbox.order", sampleOrderEvent{OrderNo: "ROLLBACK-1", Amount: 99}, func(tx *gorm.DB) error {
		if err := tx.Create(&mockBusinessOrder{ID: 1, OrderNo: "ROLLBACK-1"}).Error; err != nil {
			return err
		}
		return errors.New("simulated business validation failure")
	})
	require.Error(t, rollbackErr)

	var outboxCount int64
	require.NoError(t, db.Model(&OutboxRecord{}).Count(&outboxCount).Error)
	assert.Equal(t, int64(0), outboxCount)

	// 2. 验证业务事务提交时：业务记录与 Outbox 记录原子落库，Relay 扫描后成功投递并标记为 OutboxStatusSent
	commitErr := ExecuteInTx(ctx, db, "blade.outbox.order", sampleOrderEvent{OrderNo: "COMMIT-1", Amount: 888}, func(tx *gorm.DB) error {
		return tx.Create(&mockBusinessOrder{ID: 2, OrderNo: "COMMIT-1"}).Error
	}, WithKey("2"))
	require.NoError(t, commitErr)

	sentCount, err := relay.ProcessPendingOnce(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, sentCount)

	select {
	case ev := <-received:
		assert.Equal(t, "COMMIT-1", ev.OrderNo)
		assert.Equal(t, int64(888), ev.Amount)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for outbox relayed message")
	}

	var rec OutboxRecord
	require.NoError(t, db.First(&rec).Error)
	assert.Equal(t, OutboxStatusSent, rec.Status)
}
