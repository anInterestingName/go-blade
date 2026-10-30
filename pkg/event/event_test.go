// Package event 提供异步领域事件总线的单元测试，涵盖多订阅者并发投递、上下文透传、Panic 隔离与优雅关机排空。
package event

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsyncBus_MultiSubscriber_And_ContextPropagation(t *testing.T) {
	holder, err := trace.InitTracer(trace.Config{
		ServiceName: "test-event-bus",
		Sampler:     1.0,
	})
	require.NoError(t, err)
	defer func() { _ = holder.Shutdown(context.Background()) }()

	bus := NewAsyncBus(WithWorkers(4), WithBufferSize(128))
	defer func() { _ = bus.Close(context.Background()) }()

	var wg sync.WaitGroup
	wg.Add(2)

	var (
		sub1Tenant  string
		sub1TraceID string
		sub1Payload any
		sub2Tenant  string
		sub2TraceID string
		sub2Payload any
	)

	bus.Subscribe("blade.user.created", func(ctx context.Context, msg Message) error {
		defer wg.Done()
		sub1Tenant = tenant.FromTenant(ctx)
		sub1TraceID = trace.TraceIDFromContext(ctx)
		sub1Payload = msg.Payload
		return nil
	})

	bus.Subscribe("blade.user.created", func(ctx context.Context, msg Message) error {
		defer wg.Done()
		sub2Tenant = tenant.FromTenant(ctx)
		sub2TraceID = trace.TraceIDFromContext(ctx)
		sub2Payload = msg.Payload
		return nil
	})

	reqCtx := tenant.WithTenant(context.Background(), "100088")
	reqCtx, span := trace.StartSpan(reqCtx, "create-user-request")
	expectedTraceID := trace.TraceIDFromContext(reqCtx)
	span.End()
	require.NotEmpty(t, expectedTraceID)

	err = bus.Publish(reqCtx, "blade.user.created", map[string]string{"account": "alice"})
	require.NoError(t, err)

	wg.Wait()

	assert.Equal(t, "100088", sub1Tenant)
	assert.Equal(t, expectedTraceID, sub1TraceID)
	assert.Equal(t, map[string]string{"account": "alice"}, sub1Payload)

	assert.Equal(t, "100088", sub2Tenant)
	assert.Equal(t, expectedTraceID, sub2TraceID)
	assert.Equal(t, map[string]string{"account": "alice"}, sub2Payload)
}

func TestAsyncBus_PanicIsolation(t *testing.T) {
	bus := NewAsyncBus(WithWorkers(2), WithBufferSize(64))
	defer func() { _ = bus.Close(context.Background()) }()

	var normalExecuted atomic.Int32
	doneCh := make(chan struct{}, 2)

	// 第一个订阅者直接触发 Panic
	bus.Subscribe("blade.order.paid", func(ctx context.Context, msg Message) error {
		panic("simulated subscriber crash")
	})

	// 第二个订阅者返回普通业务错误
	bus.Subscribe("blade.order.paid", func(ctx context.Context, msg Message) error {
		normalExecuted.Add(1)
		doneCh <- struct{}{}
		return errors.New("non-fatal business error")
	})

	// 第三个订阅者正常成功完成
	bus.Subscribe("blade.order.paid", func(ctx context.Context, msg Message) error {
		normalExecuted.Add(1)
		doneCh <- struct{}{}
		return nil
	})

	err := bus.Publish(context.Background(), "blade.order.paid", "order-001")
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		select {
		case <-doneCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for healthy subscribers after panic")
		}
	}

	assert.Equal(t, int32(2), normalExecuted.Load(), "单个消费者 Panic 或返回 Error 绝不能影响同 Topic 其他消费者")
}

func TestAsyncBus_GracefulDrainOnClose(t *testing.T) {
	bus := NewAsyncBus(WithWorkers(2), WithBufferSize(128))

	const totalMessages = 30
	var processed atomic.Int32

	bus.Subscribe("blade.drain.test", func(ctx context.Context, msg Message) error {
		time.Sleep(5 * time.Millisecond)
		processed.Add(1)
		return nil
	})

	for i := 0; i < totalMessages; i++ {
		require.NoError(t, bus.Publish(context.Background(), "blade.drain.test", i))
	}

	// 调用 Close 排空所有在途消息
	closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, bus.Close(closeCtx))

	assert.Equal(t, int32(totalMessages), processed.Load(), "Close 应当等待队列中所有已投递事件全部处理完毕")

	// 关闭后再次发布应返回 ErrBusClosed
	err := bus.Publish(context.Background(), "blade.drain.test", "after-close")
	assert.ErrorIs(t, err, ErrBusClosed)
}

func TestMQBusAdapter_And_ConcurrentPublishWhileClose(t *testing.T) {
	// 1. 测试 MQBusAdapter 桥接能力
	broker := mq.NewMemoryBroker(mq.Config{Driver: mq.DriverMemory, ServiceName: "test-event-adapter"})
	adapter := NewMQBusAdapter(broker, true, "adapter-group")
	defer func() { _ = adapter.Close(context.Background()) }()

	var received atomic.Int32
	adapter.Subscribe(TopicRBACChanged, func(ctx context.Context, msg Message) error {
		if tenant.FromTenant(ctx) == "100001" {
			received.Add(1)
		}
		return nil
	})

	ctx := tenant.WithTenant(context.Background(), "100001")
	require.NoError(t, adapter.Publish(ctx, TopicRBACChanged, RBACChangedEvent{
		TenantID: "100001",
		Action:   "grant_role_menus",
	}))

	require.Eventually(t, func() bool {
		return received.Load() == 1
	}, time.Second, 10*time.Millisecond)

	// 2. 高并发 Publish 与 Close 竞争测试（验证无 send on closed channel panic）
	bus := NewBus(WithWorkers(4), WithBufferSize(16))
	bus.Subscribe("blade.race.topic", func(ctx context.Context, msg Message) error {
		return nil
	})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = bus.Publish(context.Background(), "blade.race.topic", j)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond)
		_ = bus.Close(context.Background())
	}()
	wg.Wait()
}
