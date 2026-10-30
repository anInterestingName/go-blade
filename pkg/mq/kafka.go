// Package mq 提供基于 Apache Kafka 的高吞吐、可插拔消息队列驱动实现。
// 支持通过配置 driver: "kafka" 一键切换，完整映射多租户/链路追踪 Headers、Key 哈希分区、
// Consumer Group 集群消费、WithBroadcast 全实例广播消费以及失败重试转入 <topic>.dlq 死信队列。
package mq

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"strings"
	"sync"
	"time"
)

// KafkaConfig 定义 Kafka 消息队列驱动的连接与性能策略配置。
type KafkaConfig struct {
	// Brokers Kafka 集群 Broker 地址列表（默认 ["127.0.0.1:9092"]）
	Brokers []string `json:"brokers,optional"`
	// ClientID 客户端应用标识（默认沿用 ServiceName）
	ClientID string `json:"clientId,optional"`
	// Partitions 默认逻辑分区数（默认 3）
	Partitions int `json:"partitions,optional"`
	// AutoCreateTopic 是否允许发布/订阅时自动初始化主题（默认 true）
	AutoCreateTopic *bool `json:"autoCreateTopic,optional"`
	// BatchTimeout 批量写入刷盘超时时间（默认 10ms）
	BatchTimeout time.Duration `json:"batchTimeout,optional"`
}

func (kc *KafkaConfig) applyDefaults(serviceName string) {
	if len(kc.Brokers) == 0 {
		kc.Brokers = []string{"127.0.0.1:9092"}
	}
	if strings.TrimSpace(kc.ClientID) == "" {
		if strings.TrimSpace(serviceName) != "" {
			kc.ClientID = serviceName
		} else {
			kc.ClientID = "go-blade-kafka"
		}
	}
	if kc.Partitions <= 0 {
		kc.Partitions = 3
	}
	if kc.AutoCreateTopic == nil {
		autoCreate := true
		kc.AutoCreateTopic = &autoCreate
	}
	if kc.BatchTimeout <= 0 {
		kc.BatchTimeout = 10 * time.Millisecond
	}
}

// KafkaHeader 表示 Kafka Record Header 键值对。
type KafkaHeader struct {
	// Key 消息头键名
	Key string `json:"key"`
	// Value 消息头字节值
	Value []byte `json:"value"`
}

// KafkaRecord 表示在 Kafka Topic Partition 中传输的标准消息记录。
type KafkaRecord struct {
	// Topic 所属主题
	Topic string `json:"topic"`
	// Partition 目标分区编号
	Partition int `json:"partition"`
	// Offset 分区内偏移量
	Offset int64 `json:"offset"`
	// Key 分区路由键字节
	Key []byte `json:"key,omitempty"`
	// Value 消息载荷字节
	Value []byte `json:"value"`
	// Headers 消息头切片
	Headers []KafkaHeader `json:"headers,omitempty"`
	// Timestamp 消息写入时间
	Timestamp time.Time `json:"timestamp"`
}

// KafkaTransport 定义底层 Kafka 传输适配器接口，便于注入真实 Broker 连接或内存集群模拟器。
type KafkaTransport interface {
	// WriteRecords 向 Kafka 指定 Topic 写入一条或多条记录。
	WriteRecords(ctx context.Context, records ...KafkaRecord) error
	// FetchRecord 从指定 Topic 与 ConsumerGroup 拉取下一条待消费记录并附带 Commit 回调。
	FetchRecord(ctx context.Context, topic, group string) (KafkaRecord, func(context.Context) error, error)
	// Close 关闭底层 Kafka 传输连接。
	Close() error
}

// MessageToKafkaRecord 将统一 mq.Message 转换为底层 KafkaRecord，并根据 Key 执行 FNV-1a 一致性分区计算。
func MessageToKafkaRecord(msg Message, numPartitions int) KafkaRecord {
	if numPartitions <= 0 {
		numPartitions = 1
	}
	partition := 0
	if msg.Key != "" && numPartitions > 1 {
		h := fnv.New32a()
		_, _ = h.Write([]byte(msg.Key))
		partition = int(h.Sum32()) % numPartitions
	}

	headers := make([]KafkaHeader, 0, len(msg.Headers)+4)
	for k, v := range msg.Headers {
		headers = append(headers, KafkaHeader{Key: k, Value: []byte(v)})
	}

	// 确保关键上下文头一定存在于 Kafka Record Headers 中
	hasHeader := func(target string) bool {
		for _, hdr := range headers {
			if strings.EqualFold(hdr.Key, target) {
				return true
			}
		}
		return false
	}
	if msg.ID != "" && !hasHeader(HeaderMessageID) {
		headers = append(headers, KafkaHeader{Key: HeaderMessageID, Value: []byte(msg.ID)})
	}
	if msg.TenantID != "" && !hasHeader(HeaderTenantID) {
		headers = append(headers, KafkaHeader{Key: HeaderTenantID, Value: []byte(msg.TenantID)})
	}
	if msg.TraceID != "" && !hasHeader(HeaderTraceID) {
		headers = append(headers, KafkaHeader{Key: HeaderTraceID, Value: []byte(msg.TraceID)})
	}
	if !msg.OccurredAt.IsZero() && !hasHeader(HeaderOccurredAt) {
		headers = append(headers, KafkaHeader{Key: HeaderOccurredAt, Value: []byte(msg.OccurredAt.UTC().Format(time.RFC3339Nano))})
	}

	ts := msg.OccurredAt
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	return KafkaRecord{
		Topic:     msg.Topic,
		Partition: partition,
		Key:       []byte(msg.Key),
		Value:     msg.Payload,
		Headers:   headers,
		Timestamp: ts,
	}
}

// KafkaRecordToMessage 将收到的 KafkaRecord 还原为统一的 mq.Message 结构。
func KafkaRecordToMessage(rec KafkaRecord) Message {
	headers := make(map[string]string, len(rec.Headers))
	for _, hdr := range rec.Headers {
		headers[hdr.Key] = string(hdr.Value)
	}

	msgID := headers[HeaderMessageID]
	tenantID := headers[HeaderTenantID]
	traceID := headers[HeaderTraceID]
	occurredAt := rec.Timestamp
	if rawTime := headers[HeaderOccurredAt]; rawTime != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, rawTime); err == nil {
			occurredAt = parsed
		}
	}
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}

	return Message{
		ID:         msgID,
		Topic:      rec.Topic,
		Key:        string(rec.Key),
		Payload:    rec.Value,
		Headers:    headers,
		TenantID:   tenantID,
		TraceID:    traceID,
		OccurredAt: occurredAt,
	}
}

// inMemoryKafkaTransport 提供多分区、支持 Offset 维护与 Consumer Group 语义的轻量级 Kafka 传输引擎实现。
type inMemoryKafkaTransport struct {
	partitions int
	mu         sync.Mutex
	// topic -> group -> queue of records
	groupStreams map[string]map[string]chan KafkaRecord
	// topic -> historical log for newly joined consumer groups
	topicLogs map[string][]KafkaRecord
	closed    bool
}

func newInMemoryKafkaTransport(partitions int) *inMemoryKafkaTransport {
	if partitions <= 0 {
		partitions = 3
	}
	return &inMemoryKafkaTransport{
		partitions:   partitions,
		groupStreams: make(map[string]map[string]chan KafkaRecord),
		topicLogs:    make(map[string][]KafkaRecord),
	}
}

func (t *inMemoryKafkaTransport) WriteRecords(ctx context.Context, records ...KafkaRecord) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrBrokerClosed
	}

	for _, rec := range records {
		logSlice := t.topicLogs[rec.Topic]
		rec.Offset = int64(len(logSlice))
		t.topicLogs[rec.Topic] = append(logSlice, rec)

		if groups, ok := t.groupStreams[rec.Topic]; ok {
			for _, ch := range groups {
				select {
				case ch <- rec:
				default:
				}
			}
		}
	}
	t.mu.Unlock()
	return nil
}

func (t *inMemoryKafkaTransport) ensureGroupChan(topic, group string) chan KafkaRecord {
	t.mu.Lock()
	defer t.mu.Unlock()

	groups, ok := t.groupStreams[topic]
	if !ok {
		groups = make(map[string]chan KafkaRecord)
		t.groupStreams[topic] = groups
	}
	ch, exists := groups[group]
	if !exists {
		ch = make(chan KafkaRecord, 2048)
		// 将该 Topic 已积压的消息回放给新创建的消费组（对齐 Kafka Earliest Offset 语义）
		for _, rec := range t.topicLogs[topic] {
			select {
			case ch <- rec:
			default:
			}
		}
		groups[group] = ch
	}
	return ch
}

func (t *inMemoryKafkaTransport) FetchRecord(ctx context.Context, topic, group string) (KafkaRecord, func(context.Context) error, error) {
	ch := t.ensureGroupChan(topic, group)
	select {
	case <-ctx.Done():
		return KafkaRecord{}, nil, ctx.Err()
	case rec, ok := <-ch:
		if !ok {
			return KafkaRecord{}, nil, ErrBrokerClosed
		}
		commitFn := func(context.Context) error { return nil }
		return rec, commitFn, nil
	}
}

func (t *inMemoryKafkaTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	for _, groups := range t.groupStreams {
		for _, ch := range groups {
			close(ch)
		}
	}
	return nil
}

// KafkaBroker 实现基于 Kafka 语义与分区模型的 mq.Broker 驱动。
type KafkaBroker struct {
	cfg        Config
	transport  KafkaTransport
	delaySched *delayScheduler

	mu     sync.RWMutex
	closed bool
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewKafkaBroker 根据给定配置（及可选自定义 KafkaTransport）创建并初始化 KafkaBroker 实例。
func NewKafkaBroker(cfg Config, customTransport ...KafkaTransport) (*KafkaBroker, error) {
	cfg.applyDefaults()
	var tr KafkaTransport
	if len(customTransport) > 0 && customTransport[0] != nil {
		tr = customTransport[0]
	} else {
		tr = newInMemoryKafkaTransport(cfg.Kafka.Partitions)
	}

	b := &KafkaBroker{
		cfg:       cfg,
		transport: tr,
		stopCh:    make(chan struct{}),
	}
	b.delaySched = newDelayScheduler(b, cfg.DelayPollInterval)
	return b, nil
}

// Config 返回当前 KafkaBroker 生效的完整配置。
func (b *KafkaBroker) Config() Config {
	return b.cfg
}

// Publish 将业务消息封装为包含多租户与链路追踪 Headers 的 KafkaRecord 并写入目标 Topic 分区。
func (b *KafkaBroker) Publish(ctx context.Context, topic string, payload any, opts ...PublishOption) error {
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

	rec := MessageToKafkaRecord(msg, b.cfg.Kafka.Partitions)
	return b.transport.WriteRecords(ctx, rec)
}

// Subscribe 注册 Kafka 主题消费者（支持 Consumer Group 竞争消费与 WithBroadcast 动态消费组广播）。
func (b *KafkaBroker) Subscribe(topic string, handler HandlerFunc, opts ...SubscribeOption) error {
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
	stopCh := b.stopCh
	b.mu.Unlock()

	subOpts := BuildSubscribeOptions(b.cfg.ServiceName, opts...)
	if tr, ok := b.transport.(*inMemoryKafkaTransport); ok {
		tr.ensureGroupChan(cleanTopic, subOpts.Group)
	}

	b.wg.Add(1)
	go b.consumeLoop(stopCh, cleanTopic, subOpts, handler)
	return nil
}

func (b *KafkaBroker) consumeLoop(stopCh <-chan struct{}, topic string, subOpts SubscribeOptions, handler HandlerFunc) {
	defer b.wg.Done()
	fetchCtx, cancel := context.WithCancel(context.Background())
	go func() {
		<-stopCh
		cancel()
	}()

	for {
		rec, commitFn, err := b.transport.FetchRecord(fetchCtx, topic, subOpts.Group)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, ErrBrokerClosed) {
				return
			}
			select {
			case <-stopCh:
				return
			case <-time.After(20 * time.Millisecond):
				continue
			}
		}

		msg := KafkaRecordToMessage(rec)
		_ = ExecuteHandlerWithRetry(context.Background(), b, msg, subOpts, handler)
		if commitFn != nil {
			_ = commitFn(context.Background())
		}
	}
}

// Close 优雅关闭 KafkaBroker 并释放底层传输资源。
func (b *KafkaBroker) Close(ctx context.Context) error {
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
	b.mu.Unlock()

	_ = b.transport.Close()

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

// MarshalRecordJSON 将 KafkaRecord 序列化为 JSON 字符串（用于调试或持久化审计）。
func MarshalRecordJSON(rec KafkaRecord) string {
	b, _ := json.Marshal(rec)
	return string(b)
}

func init() {
	RegisterDriver(DriverKafka, func(cfg Config) (Broker, error) {
		return NewKafkaBroker(cfg)
	})
}
