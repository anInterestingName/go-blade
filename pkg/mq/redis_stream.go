// Package mq 提供基于 Redis ZSet 与进程内内存最小堆双模自适应的分布式延迟消息调度支持。
package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-blade/pkg/lock"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/snowflake"
	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// RedisDelayedZSetKey Redis 存储延迟到期消息的有序集合键名（Score 为到期毫秒时间戳）。
	RedisDelayedZSetKey = "blade:mq:delayed:zset"
	// RedisDelayedLockKey 多实例抢占扫描 Redis 延迟消息队列的分布式锁键名。
	RedisDelayedLockKey = "blade:mq:delayed:lock"
)

// delayedEnvelope 序列化存储于延迟队列中的完整消息载体。
type delayedEnvelope struct {
	Message   Message   `json:"message"`
	DeliverAt time.Time `json:"deliver_at"`
}

// delayScheduler 负责管理进程内与 Redis ZSet 延迟消息的到期投递。
type delayScheduler struct {
	broker       Broker
	pollInterval time.Duration

	mu      sync.Mutex
	items   []delayedEnvelope
	stopCh  chan struct{}
	stopped bool
	wg      sync.WaitGroup
}

func newDelayScheduler(broker Broker, pollInterval time.Duration) *delayScheduler {
	if pollInterval <= 0 {
		pollInterval = 25 * time.Millisecond
	}
	ds := &delayScheduler{
		broker:       broker,
		pollInterval: pollInterval,
		stopCh:       make(chan struct{}),
	}
	ds.wg.Add(1)
	go ds.loop()
	return ds
}

// Schedule 将一条消息加入延迟队列（优先写入 Redis ZSet，若 Redis 不可用则写入进程内内存队列）。
func (ds *delayScheduler) Schedule(ctx context.Context, msg Message, delay time.Duration) error {
	deliverAt := time.Now().UTC().Add(delay)
	env := delayedEnvelope{
		Message:   msg,
		DeliverAt: deliverAt,
	}

	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		raw, mErr := json.Marshal(env)
		if mErr == nil {
			score := deliverAt.UnixMilli()
			if _, zErr := rds.ZaddCtx(ctx, RedisDelayedZSetKey, score, string(raw)); zErr == nil {
				return nil
			}
		}
	}

	ds.mu.Lock()
	defer ds.mu.Unlock()
	if ds.stopped {
		return ErrBrokerClosed
	}
	ds.items = append(ds.items, env)
	return nil
}

func (ds *delayScheduler) Stop() {
	ds.mu.Lock()
	if ds.stopped {
		ds.mu.Unlock()
		return
	}
	ds.stopped = true
	close(ds.stopCh)
	ds.mu.Unlock()
	ds.wg.Wait()
}

func (ds *delayScheduler) loop() {
	defer ds.wg.Done()
	ticker := time.NewTicker(ds.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ds.stopCh:
			ds.drainMemoryDue(time.Now().UTC())
			return
		case <-ticker.C:
			now := time.Now().UTC()
			ds.drainMemoryDue(now)
			ds.drainRedisDue(now)
		}
	}
}

func (ds *delayScheduler) drainMemoryDue(now time.Time) {
	ds.mu.Lock()
	if len(ds.items) == 0 {
		ds.mu.Unlock()
		return
	}

	var due []delayedEnvelope
	remaining := ds.items[:0]
	for _, item := range ds.items {
		if !now.Before(item.DeliverAt) {
			due = append(due, item)
		} else {
			remaining = append(remaining, item)
		}
	}
	ds.items = remaining
	ds.mu.Unlock()

	for _, env := range due {
		ds.deliverNow(env.Message)
	}
}

func (ds *delayScheduler) drainRedisDue(now time.Time) {
	rds, err := pkgredis.GetRedis()
	if err != nil || rds == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 使用短效分布式锁确保多副本同时扫描 ZSet 时不会重复投递同一条延迟消息
	acquired, lockErr := lock.Acquire(ctx, RedisDelayedLockKey, 2*time.Second)
	if lockErr != nil {
		return
	}
	defer func() {
		_ = acquired.Unlock(context.Background())
	}()

	maxScore := now.UnixMilli()
	members, err := rds.ZrangebyscoreWithScoresAndLimitCtx(ctx, RedisDelayedZSetKey, 0, maxScore, 0, 100)
	if err != nil || len(members) == 0 {
		return
	}

	for _, pair := range members {
		removed, remErr := rds.ZremCtx(ctx, RedisDelayedZSetKey, pair.Key)
		if remErr != nil || removed <= 0 {
			continue
		}
		var env delayedEnvelope
		if uErr := json.Unmarshal([]byte(pair.Key), &env); uErr != nil {
			logx.Errorf("[mq] unmarshal delayed message failed: %v", uErr)
			continue
		}
		ds.deliverNow(env.Message)
	}
}

func (ds *delayScheduler) deliverNow(msg Message) {
	if ds.broker == nil {
		return
	}
	ctx := context.Background()
	if err := ds.broker.Publish(ctx, msg.Topic, msg.Payload, WithKey(msg.Key), WithHeaders(msg.Headers)); err != nil {
		logx.Errorf("[mq] deliver delayed message id=%s topic=%s failed: %v", msg.ID, msg.Topic, err)
	}
}

// MemoryBroker 基于进程内并发安全通道实现的内存消息队列驱动。
// 原生支持同组竞争消费 (Queue/Group) 与跨组广播消费 (Broadcast)。
type MemoryBroker struct {
	cfg Config

	mu     sync.RWMutex
	groups map[string]map[string]*memoryGroupConsumer
	closed bool

	delaySched *delayScheduler
	wg         sync.WaitGroup
}

type memoryGroupConsumer struct {
	topic    string
	group    string
	subOpts  SubscribeOptions
	queue    chan Message
	mu       sync.RWMutex
	handlers []HandlerFunc
}

// NewMemoryBroker 创建并启动一个进程内内存消息队列 Broker 实例。
func NewMemoryBroker(cfg Config) *MemoryBroker {
	cfg.applyDefaults()
	b := &MemoryBroker{
		cfg:    cfg,
		groups: make(map[string]map[string]*memoryGroupConsumer),
	}
	b.delaySched = newDelayScheduler(b, cfg.DelayPollInterval)
	return b
}

// Publish 向指定主题发布消息（若设置了 WithDelay 则进入延迟调度器，否则立即分发至该主题下的各个消费组）。
func (b *MemoryBroker) Publish(ctx context.Context, topic string, payload any, opts ...PublishOption) error {
	msg, pubOpts, err := BuildMessage(ctx, topic, payload, opts...)
	if err != nil {
		return err
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrBrokerClosed
	}
	if pubOpts.Delay > 0 {
		b.mu.RUnlock()
		return b.delaySched.Schedule(ctx, msg, pubOpts.Delay)
	}

	topicGroups := b.groups[msg.Topic]
	if len(topicGroups) == 0 {
		b.mu.RUnlock()
		return nil
	}
	consumers := make([]*memoryGroupConsumer, 0, len(topicGroups))
	for _, gc := range topicGroups {
		consumers = append(consumers, gc)
	}
	b.mu.RUnlock()

	for _, gc := range consumers {
		select {
		case gc.queue <- cloneMessage(msg):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Subscribe 在指定主题上注册消费者。
// - 相同 Group 名称的多个消费者共享同一个通道，实现集群竞争消费（每条消息仅由组内一个消费者处理）；
// - 不同 Group 名称（或开启 WithBroadcast）拥有独立通道，实现广播消费（每个消费组均收到完整消息副本）。
func (b *MemoryBroker) Subscribe(topic string, handler HandlerFunc, opts ...SubscribeOption) error {
	cleanTopic := strings.TrimSpace(topic)
	if cleanTopic == "" {
		return ErrEmptyTopic
	}
	if handler == nil {
		return ErrNilHandler
	}
	subOpts := BuildSubscribeOptions(b.cfg.ServiceName, opts...)

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrBrokerClosed
	}

	topicGroups, ok := b.groups[cleanTopic]
	if !ok {
		topicGroups = make(map[string]*memoryGroupConsumer)
		b.groups[cleanTopic] = topicGroups
	}

	gc, exists := topicGroups[subOpts.Group]
	if !exists {
		gc = &memoryGroupConsumer{
			topic:   cleanTopic,
			group:   subOpts.Group,
			subOpts: subOpts,
			queue:   make(chan Message, b.cfg.BufferSize),
		}
		topicGroups[subOpts.Group] = gc
	}

	gc.mu.Lock()
	gc.handlers = append(gc.handlers, handler)
	handlerIdx := len(gc.handlers) - 1
	gc.mu.Unlock()

	// 每个订阅注册开启一个消费协程从该 Group 共享队列中竞争拉取
	b.wg.Add(1)
	go func(consumer *memoryGroupConsumer, h HandlerFunc, _ int) {
		defer b.wg.Done()
		for msg := range consumer.queue {
			_ = ExecuteHandlerWithRetry(context.Background(), b, msg, consumer.subOpts, h)
		}
	}(gc, handler, handlerIdx)

	return nil
}

// Close 优雅停止内存消息队列并等待所有在途消费任务处理完成。
func (b *MemoryBroker) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if b.delaySched != nil {
		b.delaySched.Stop()
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
	} else {
		b.closed = true
		for _, topicGroups := range b.groups {
			for _, gc := range topicGroups {
				close(gc.queue)
			}
		}
		b.mu.Unlock()
	}

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func cloneMessage(msg Message) Message {
	copied := msg
	if len(msg.Payload) > 0 {
		copied.Payload = make([]byte, len(msg.Payload))
		copy(copied.Payload, msg.Payload)
	}
	if len(msg.Headers) > 0 {
		copied.Headers = make(map[string]string, len(msg.Headers))
		for k, v := range msg.Headers {
			copied.Headers[k] = v
		}
	}
	return copied
}

const (
	// RedisStreamKeyPrefix Redis Streams 主题流在 Redis 中的统一键名前缀。
	RedisStreamKeyPrefix = "blade:mq:stream:"
)

// RedisStreamBroker 基于 Redis Streams (XADD / XREADGROUP / XACK) 与 Memory 自动回退实现的双模消息队列驱动。
type RedisStreamBroker struct {
	cfg      Config
	fallback *MemoryBroker

	mu        sync.RWMutex
	client    redis.UniversalClient
	ownClient bool
	closed    bool
	stopCh    chan struct{}
	wg        sync.WaitGroup

	delaySched  *delayScheduler
	consumerSeq int64
}

// NewRedisStreamBroker 创建基于 Redis Streams（并在无 Redis 时自动回退 Memory）的双模消息队列 Broker。
func NewRedisStreamBroker(cfg Config) (*RedisStreamBroker, error) {
	cfg.applyDefaults()
	fb := NewMemoryBroker(cfg)
	b := &RedisStreamBroker{
		cfg:      cfg,
		fallback: fb,
		stopCh:   make(chan struct{}),
	}
	b.delaySched = newDelayScheduler(b, cfg.DelayPollInterval)
	return b, nil
}

// resolveRedisClient 动态解析可用的 go-redis 客户端（支持在 Broker 创建后才调用 pkgredis.InitRedis 的场景）。
func (b *RedisStreamBroker) resolveRedisClient() redis.UniversalClient {
	b.mu.RLock()
	if b.client != nil {
		c := b.client
		b.mu.RUnlock()
		return c
	}
	b.mu.RUnlock()

	rds, err := pkgredis.GetRedis()
	if err != nil || rds == nil || rds.Addr == "" {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client != nil {
		return b.client
	}

	client := redis.NewClient(&redis.Options{
		Addr:     rds.Addr,
		Password: rds.Pass,
	})
	b.client = client
	b.ownClient = true
	return b.client
}

// Publish 向 Redis Streams 投递消息（XADD）；若全局 Redis 未初始化，则无缝回退至内存队列投递。
func (b *RedisStreamBroker) Publish(ctx context.Context, topic string, payload any, opts ...PublishOption) error {
	msg, pubOpts, err := BuildMessage(ctx, topic, payload, opts...)
	if err != nil {
		return err
	}

	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return ErrBrokerClosed
	}
	b.mu.RUnlock()

	if pubOpts.Delay > 0 {
		return b.delaySched.Schedule(ctx, msg, pubOpts.Delay)
	}

	cli := b.resolveRedisClient()
	if cli == nil {
		return b.fallback.Publish(ctx, topic, payload, opts...)
	}

	headersJSON, _ := json.Marshal(msg.Headers)
	values := map[string]any{
		"id":          msg.ID,
		"topic":       msg.Topic,
		"key":         msg.Key,
		"payload":     string(msg.Payload),
		"headers":     string(headersJSON),
		"tenant_id":   msg.TenantID,
		"trace_id":    msg.TraceID,
		"occurred_at": strconv.FormatInt(msg.OccurredAt.UnixNano(), 10),
	}

	streamKey := RedisStreamKeyPrefix + msg.Topic
	xArgs := &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: b.cfg.StreamMaxLen,
		Approx: true,
		Values: values,
	}
	if err := cli.XAdd(ctx, xArgs).Err(); err != nil {
		return fmt.Errorf("redis stream xadd failed on %s: %w", streamKey, err)
	}
	return nil
}

// Subscribe 注册 Redis Streams 消费者组（XGROUP CREATE ... MKSTREAM + XREADGROUP + XACK），同时挂载内存回退订阅。
func (b *RedisStreamBroker) Subscribe(topic string, handler HandlerFunc, opts ...SubscribeOption) error {
	cleanTopic := strings.TrimSpace(topic)
	if cleanTopic == "" {
		return ErrEmptyTopic
	}
	if handler == nil {
		return ErrNilHandler
	}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrBrokerClosed
	}
	b.consumerSeq++
	seq := b.consumerSeq
	stopCh := b.stopCh
	b.mu.Unlock()

	// 同步注册到 fallback 内存 Broker，确保无 Redis 降级时同样正常工作
	if err := b.fallback.Subscribe(cleanTopic, handler, opts...); err != nil {
		return err
	}

	subOpts := BuildSubscribeOptions(b.cfg.ServiceName, opts...)
	consumerName := fmt.Sprintf("%s-consumer-%d-%s", b.cfg.ServiceName, seq, snowflake.GenIDString())

	b.wg.Add(1)
	go b.consumeStreamLoop(stopCh, cleanTopic, subOpts, consumerName, handler)
	return nil
}

func (b *RedisStreamBroker) consumeStreamLoop(
	stopCh <-chan struct{},
	topic string,
	subOpts SubscribeOptions,
	consumerName string,
	handler HandlerFunc,
) {
	defer b.wg.Done()
	streamKey := RedisStreamKeyPrefix + topic
	groupCreated := false

	pollInterval := b.cfg.BlockTimeout
	if pollInterval <= 0 {
		pollInterval = 50 * time.Millisecond
	}
	if pollInterval > 100*time.Millisecond {
		pollInterval = 50 * time.Millisecond
	}

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		cli := b.resolveRedisClient()
		if cli == nil {
			select {
			case <-stopCh:
				return
			case <-time.After(pollInterval):
				continue
			}
		}

		if !groupCreated {
			// 使用 "0" 作为起始游标创建消费者组，确保已发布的消息不会因异步建组延迟而漏消费
			err := cli.XGroupCreateMkStream(context.Background(), streamKey, subOpts.Group, "0").Err()
			if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
				select {
				case <-stopCh:
					return
				case <-time.After(pollInterval):
					continue
				}
			}
			groupCreated = true
		}

		streams, err := cli.XReadGroup(context.Background(), &redis.XReadGroupArgs{
			Group:    subOpts.Group,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
			Count:    16,
			Block:    pollInterval,
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			select {
			case <-stopCh:
				return
			case <-time.After(pollInterval):
				continue
			}
		}

		for _, s := range streams {
			for _, xmsg := range s.Messages {
				msg := decodeRedisStreamMessage(topic, xmsg)
				_ = ExecuteHandlerWithRetry(context.Background(), b, msg, subOpts, handler)
				_ = cli.XAck(context.Background(), streamKey, subOpts.Group, xmsg.ID).Err()
			}
		}
	}
}

func decodeRedisStreamMessage(defaultTopic string, xmsg redis.XMessage) Message {
	getString := func(k string) string {
		if v, ok := xmsg.Values[k]; ok && v != nil {
			switch s := v.(type) {
			case string:
				return s
			case []byte:
				return string(s)
			default:
				return fmt.Sprintf("%v", v)
			}
		}
		return ""
	}

	msgID := getString("id")
	if msgID == "" {
		msgID = xmsg.ID
	}
	topic := getString("topic")
	if topic == "" {
		topic = defaultTopic
	}
	var headers map[string]string
	if rawHeaders := getString("headers"); rawHeaders != "" {
		_ = json.Unmarshal([]byte(rawHeaders), &headers)
	}
	var occurredAt time.Time
	if nanoStr := getString("occurred_at"); nanoStr != "" {
		if ns, err := strconv.ParseInt(nanoStr, 10, 64); err == nil && ns > 0 {
			occurredAt = time.Unix(0, ns).UTC()
		}
	}
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}

	return Message{
		ID:         msgID,
		Topic:      topic,
		Key:        getString("key"),
		Payload:    []byte(getString("payload")),
		Headers:    headers,
		TenantID:   getString("tenant_id"),
		TraceID:    getString("trace_id"),
		OccurredAt: occurredAt,
	}
}

// Close 优雅关闭 RedisStreamBroker 及内部回退队列。
func (b *RedisStreamBroker) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if b.delaySched != nil {
		b.delaySched.Stop()
	}

	b.mu.Lock()
	if !b.closed {
		b.closed = true
		close(b.stopCh)
	}
	ownClient := b.ownClient
	cli := b.client
	b.client = nil
	b.mu.Unlock()

	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}

	if ownClient && cli != nil {
		_ = cli.Close()
	}
	return b.fallback.Close(ctx)
}

func init() {
	RegisterDriver(DriverMemory, func(cfg Config) (Broker, error) {
		return NewMemoryBroker(cfg), nil
	})
	RegisterDriver(DriverRedis, func(cfg Config) (Broker, error) {
		return NewRedisStreamBroker(cfg)
	})
	RegisterDriver(DriverAuto, func(cfg Config) (Broker, error) {
		return NewRedisStreamBroker(cfg)
	})
}
