// Package mq 提供可插拔、多驱动自适应（支持 Redis Streams + Memory 双模、Kafka 及外部扩展驱动）的分布式消息队列底座。
// 原生支持多租户上下文 (TenantID) 与 OpenTelemetry W3C TraceContext (traceparent) 自动注入与跨进程还原、
// 集群竞争消费 (Queue/Group) 与全实例广播消费 (Broadcast)、消费端 Panic 隔离恢复、指数退避重试与死信队列 (<topic>.dlq) 转发、
// 分布式延迟消息投递 (WithDelay) 以及 GORM 本地事务发件箱 (Transactional Outbox)。
package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"github.com/zeromicro/go-zero/core/logx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	// DriverAuto 双模自适应驱动：当全局或配置的 Redis 可用时使用 Redis Streams，否则自动回退至进程内 Memory 驱动。
	DriverAuto = "auto"
	// DriverMemory 进程内内存队列驱动（适用于单机开发与无外部依赖单元测试）。
	DriverMemory = "memory"
	// DriverRedis 基于 Redis Streams (XADD / XREADGROUP / XACK) 的持久化流消息队列驱动。
	DriverRedis = "redis"
	// DriverKafka 基于 Apache Kafka 的高吞吐分布式消息队列驱动。
	DriverKafka = "kafka"

	// DefaultGroup 未显式指定消费组名称时的默认集群消费组标识。
	DefaultGroup = "blade-default-group"
	// DLQTopicSuffix 消费重试耗尽后自动转发的死信队列主题后缀。
	DLQTopicSuffix = ".dlq"

	// HeaderMessageID 消息头中存储全局唯一雪花 ID 的键名。
	HeaderMessageID = "x-blade-msg-id"
	// HeaderTenantID 消息头中存储多租户编号的键名。
	HeaderTenantID = "x-blade-tenant-id"
	// HeaderTraceID 消息头中存储链路追踪 TraceID 的键名。
	HeaderTraceID = "x-blade-trace-id"
	// HeaderOccurredAt 消息头中存储消息产生时间戳 (RFC3339Nano) 的键名。
	HeaderOccurredAt = "x-blade-occurred-at"
	// HeaderOriginalTopic 死信消息头中记录原始主题名称的键名。
	HeaderOriginalTopic = "x-blade-original-topic"
	// HeaderDeadReason 死信消息头中记录最终消费失败原因的键名。
	HeaderDeadReason = "x-blade-dead-reason"
	// HeaderRetryCount 死信消息头中记录已执行的重试次数的键名。
	HeaderRetryCount = "x-blade-retry-count"
)

var (
	// ErrBrokerClosed 表示消息队列 Broker 已关闭，拒绝继续发布或订阅。
	ErrBrokerClosed = errors.New("mq broker is closed")
	// ErrEmptyTopic 表示消息主题名称不能为空。
	ErrEmptyTopic = errors.New("mq topic must not be empty")
	// ErrNilHandler 表示消息订阅处理函数不能为空。
	ErrNilHandler = errors.New("mq subscription handler must not be nil")
	// ErrUnsupportedDriver 表示请求的消息队列驱动类型尚未注册。
	ErrUnsupportedDriver = errors.New("unsupported mq driver")
)

// Message 定义在消息队列中传输的标准消息载体（Envelope）。
type Message struct {
	// ID 消息全局唯一标识（默认使用雪花算法生成）
	ID string `json:"id"`
	// Topic 目标消息主题名称（如 "blade.audit.api"）
	Topic string `json:"topic"`
	// Key 业务路由/分区键（用于 Kafka 等按 Key 哈希保证同分区顺序消费）
	Key string `json:"key,omitempty"`
	// Payload 消息载荷原始字节流（JSON 或 Protobuf 编码字节）
	Payload []byte `json:"payload"`
	// Headers 跨进程传递的扩展元数据键值对
	Headers map[string]string `json:"headers,omitempty"`
	// TenantID 消息发布时的租户编号
	TenantID string `json:"tenant_id"`
	// TraceID 消息发布时的分布式链路追踪 ID
	TraceID string `json:"trace_id"`
	// OccurredAt 消息产生时间
	OccurredAt time.Time `json:"occurred_at"`
}

// DecodeJSON 将 Message.Payload 反序列化到目标结构体指针 target 中。
func (m Message) DecodeJSON(target any) error {
	if len(m.Payload) == 0 {
		return errors.New("mq message payload is empty")
	}
	return json.Unmarshal(m.Payload, target)
}

// HandlerFunc 定义消息队列消费者的业务处理函数签名。
// 当返回 nil 时表示消费成功并自动提交 ACK/Offset；当返回非 nil 错误或发生 Panic 时触发重试或死信转发。
type HandlerFunc func(ctx context.Context, msg Message) error

// PublishOptions 定义发布单条消息时的可选策略参数。
type PublishOptions struct {
	// Key 消息分区路由键
	Key string
	// Headers 附加的自定义消息头
	Headers map[string]string
	// Delay 延迟投递时长（大于 0 时通过延迟调度器到期后投递至目标 Topic）
	Delay time.Duration
}

// PublishOption 定义发布消息的函数式配置项。
type PublishOption func(*PublishOptions)

// WithKey 设置消息的分区路由 Key（确保相同 Key 的消息路由至同一分区）。
func WithKey(key string) PublishOption {
	return func(o *PublishOptions) {
		o.Key = strings.TrimSpace(key)
	}
}

// WithHeaders 为发布的消息附加自定义键值对 Headers。
func WithHeaders(headers map[string]string) PublishOption {
	return func(o *PublishOptions) {
		if len(headers) == 0 {
			return
		}
		if o.Headers == nil {
			o.Headers = make(map[string]string, len(headers))
		}
		for k, v := range headers {
			o.Headers[k] = v
		}
	}
}

// WithDelay 设置消息的延迟投递时间（如 30*time.Second）。
// 当 duration > 0 时，消息将先进入分布式延迟队列，到期后自动投递至目标 Topic。
func WithDelay(duration time.Duration) PublishOption {
	return func(o *PublishOptions) {
		if duration > 0 {
			o.Delay = duration
		}
	}
}

// SubscribeOptions 定义订阅主题时的消费者组、广播模式与重试死信策略。
type SubscribeOptions struct {
	// Group 消费者组名称（默认 "blade-default-group"）
	Group string
	// Broadcast 是否启用全实例广播模式（启用后每个服务实例拥有独立消费组，接收全量消息副本）
	Broadcast bool
	// MaxRetries 消费返回错误或 Panic 时的最大重试次数（默认 3 次，0 表示不重试）
	MaxRetries int
	// RetryBackoff 首次重试的基础退避等待时间（默认 50ms，按指数退避递增）
	RetryBackoff time.Duration
	// DisableDLQ 消费重试耗尽后是否禁用死信队列 (<topic>.dlq) 自动转发
	DisableDLQ bool
}

// SubscribeOption 定义订阅消息的函数式配置项。
type SubscribeOption func(*SubscribeOptions)

// WithGroup 指定集群竞争消费的消费者组名称（同一消费组内的多个实例负载均衡分担消费）。
func WithGroup(group string) SubscribeOption {
	return func(o *SubscribeOptions) {
		if g := strings.TrimSpace(group); g != "" {
			o.Group = g
		}
	}
}

// WithBroadcast 启用全实例广播消费模式。
// 适用于多副本网关权限缓存失效等需要将同一事件通知到每一个运行实例的场景。
func WithBroadcast() SubscribeOption {
	return func(o *SubscribeOptions) {
		o.Broadcast = true
	}
}

// WithMaxRetries 设置单条消息消费失败时的最大重试次数（超过后若未禁用 DLQ 则自动转入 <topic>.dlq）。
func WithMaxRetries(maxRetries int) SubscribeOption {
	return func(o *SubscribeOptions) {
		if maxRetries >= 0 {
			o.MaxRetries = maxRetries
		}
	}
}

// WithRetryBackoff 设置消费失败重试的基础退避间隔。
func WithRetryBackoff(backoff time.Duration) SubscribeOption {
	return func(o *SubscribeOptions) {
		if backoff > 0 {
			o.RetryBackoff = backoff
		}
	}
}

// WithDisableDLQ 设置重试耗尽后是否禁用自动转发至死信队列 (<topic>.dlq)。
func WithDisableDLQ(disable bool) SubscribeOption {
	return func(o *SubscribeOptions) {
		o.DisableDLQ = disable
	}
}

// Broker 定义统一的可插拔消息队列核心接口契约。
type Broker interface {
	// Publish 向指定主题发布一条消息（payload 支持 []byte、string 或任意可 JSON 序列化的结构体）。
	Publish(ctx context.Context, topic string, payload any, opts ...PublishOption) error
	// Subscribe 订阅指定主题的消息，支持集群消费组模式与全实例广播模式。
	Subscribe(topic string, handler HandlerFunc, opts ...SubscribeOption) error
	// Close 优雅关闭消息队列 Broker，停止接收新消息并排空在途消费任务。
	Close(ctx context.Context) error
}

// Config 定义多驱动消息队列的统一配置结构体（兼容 YAML/JSON 反序列化）。
type Config struct {
	// Driver 驱动类型：可选 "auto"（默认，Redis Streams + Memory 双模）、"memory"、"redis"、"kafka"
	Driver string `json:"driver,optional"`
	// ServiceName 当前服务名称，用于生成默认消费组与广播实例标识
	ServiceName string `json:"serviceName,optional"`
	// Workers 内存或流消费并发 Worker 数量（默认 4）
	Workers int `json:"workers,optional"`
	// BufferSize 内存缓冲队列容量（默认 1024）
	BufferSize int `json:"bufferSize,optional"`
	// StreamMaxLen Redis Streams 单个流的最大近似保留长度（默认 10000）
	StreamMaxLen int64 `json:"streamMaxLen,optional"`
	// BlockTimeout 流拉取单次阻塞轮询时间（默认 200ms）
	BlockTimeout time.Duration `json:"blockTimeout,optional"`
	// DelayPollInterval 分布式延迟队列到期扫描周期（默认 50ms）
	DelayPollInterval time.Duration `json:"delayPollInterval,optional"`
	// Kafka Kafka 驱动专属配置
	Kafka KafkaConfig `json:"kafka,optional"`
}

// applyDefaults 补全 Config 的零值默认参数。
func (c *Config) applyDefaults() {
	c.Driver = strings.ToLower(strings.TrimSpace(c.Driver))
	if c.Driver == "" {
		c.Driver = DriverAuto
	}
	if strings.TrimSpace(c.ServiceName) == "" {
		c.ServiceName = "go-blade"
	}
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.BufferSize <= 0 {
		c.BufferSize = 1024
	}
	if c.StreamMaxLen <= 0 {
		c.StreamMaxLen = 10000
	}
	if c.BlockTimeout <= 0 {
		c.BlockTimeout = 200 * time.Millisecond
	}
	if c.DelayPollInterval <= 0 {
		c.DelayPollInterval = 50 * time.Millisecond
	}
	c.Kafka.applyDefaults(c.ServiceName)
}

// DriverFactory 定义创建具体消息队列 Broker 驱动实例的工厂函数签名。
type DriverFactory func(cfg Config) (Broker, error)

var (
	driverMu        sync.RWMutex
	driverFactories = make(map[string]DriverFactory)

	defaultBrokerMu sync.Mutex
	defaultBroker   Broker
)

// RegisterDriver 注册一个自定义消息队列驱动工厂（用于扩展如 kafka、rocketmq、nats 等驱动）。
func RegisterDriver(name string, factory DriverFactory) {
	clean := strings.ToLower(strings.TrimSpace(name))
	if clean == "" || factory == nil {
		return
	}
	driverMu.Lock()
	defer driverMu.Unlock()
	driverFactories[clean] = factory
}

// NewBroker 根据给定的 Config 创建并初始化对应的消息队列 Broker 实例。
// 默认支持 "auto"（Redis Streams + Memory 双模）、"memory"、"redis" 与 "kafka"。
func NewBroker(cfg Config) (Broker, error) {
	cfg.applyDefaults()

	driverMu.RLock()
	factory, exists := driverFactories[cfg.Driver]
	driverMu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDriver, cfg.Driver)
	}
	return factory(cfg)
}

// DefaultBroker 获取全局默认的双模自适应消息队列 Broker 单例。
func DefaultBroker() Broker {
	defaultBrokerMu.Lock()
	defer defaultBrokerMu.Unlock()
	if defaultBroker == nil {
		b, err := NewBroker(Config{Driver: DriverAuto})
		if err != nil {
			b = NewMemoryBroker(Config{Driver: DriverMemory})
		}
		defaultBroker = b
	}
	return defaultBroker
}

// SetDefaultBroker 替换全局默认的 Broker 实例（便于在服务启动或单元测试中注入特定 Broker）。
func SetDefaultBroker(b Broker) {
	defaultBrokerMu.Lock()
	defer defaultBrokerMu.Unlock()
	defaultBroker = b
}

// EncodePayload 将任意业务载荷编码为字节切片：
// - []byte 原样返回副本；
// - string 转为 []byte；
// - Message 提取其 Payload；
// - 其他类型统一执行 JSON 序列化。
func EncodePayload(payload any) ([]byte, error) {
	if payload == nil {
		return []byte("null"), nil
	}
	switch v := payload.(type) {
	case []byte:
		out := make([]byte, len(v))
		copy(out, v)
		return out, nil
	case string:
		return []byte(v), nil
	case Message:
		return v.Payload, nil
	case *Message:
		if v == nil {
			return nil, nil
		}
		return v.Payload, nil
	default:
		return json.Marshal(v)
	}
}

// BuildMessage 从上下文与发布选项构造标准 Message 信封，自动注入雪花 ID、TenantID、TraceID 与 W3C traceparent。
func BuildMessage(ctx context.Context, topic string, payload any, opts ...PublishOption) (Message, PublishOptions, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cleanTopic := strings.TrimSpace(topic)
	if cleanTopic == "" {
		return Message{}, PublishOptions{}, ErrEmptyTopic
	}

	var pubOpts PublishOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&pubOpts)
		}
	}

	rawBytes, err := EncodePayload(payload)
	if err != nil {
		return Message{}, pubOpts, fmt.Errorf("encode mq payload failed: %w", err)
	}

	headers := make(map[string]string, len(pubOpts.Headers)+6)
	for k, v := range pubOpts.Headers {
		headers[k] = v
	}

	// 注入 OpenTelemetry W3C TraceContext (traceparent, baggage)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))

	tenantID := tenant.FromTenant(ctx)
	traceID := trace.TraceIDFromContext(ctx)
	msgID := snowflake.GenIDString()
	now := time.Now().UTC()

	headers[HeaderMessageID] = msgID
	if tenantID != "" {
		headers[HeaderTenantID] = tenantID
	}
	if traceID != "" {
		headers[HeaderTraceID] = traceID
	}
	headers[HeaderOccurredAt] = now.Format(time.RFC3339Nano)

	msg := Message{
		ID:         msgID,
		Topic:      cleanTopic,
		Key:        pubOpts.Key,
		Payload:    rawBytes,
		Headers:    headers,
		TenantID:   tenantID,
		TraceID:    traceID,
		OccurredAt: now,
	}
	return msg, pubOpts, nil
}

// ExtractMessageContext 从收到的 Message 中提取并还原包含 TenantID、TraceID 与 OpenTelemetry SpanContext 的衍生 Context。
func ExtractMessageContext(baseCtx context.Context, msg Message) (context.Context, oteltrace.Span) {
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx := baseCtx
	if len(msg.Headers) > 0 {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(msg.Headers))
	}

	tenantID := strings.TrimSpace(msg.TenantID)
	if tenantID == "" && len(msg.Headers) > 0 {
		tenantID = strings.TrimSpace(msg.Headers[HeaderTenantID])
	}
	if tenantID != "" {
		ctx = tenant.WithTenant(ctx, tenantID)
	}

	traceID := strings.TrimSpace(msg.TraceID)
	if traceID == "" && len(msg.Headers) > 0 {
		traceID = strings.TrimSpace(msg.Headers[HeaderTraceID])
	}
	if traceID != "" {
		ctx = trace.WithTraceID(ctx, traceID)
		// 若 Header 未携带完整 traceparent 但存在 32 位合法 Hex TraceID，则构造远程 SpanContext
		if span := oteltrace.SpanFromContext(ctx); span == nil || !span.SpanContext().IsValid() {
			if len(traceID) == 32 {
				if tid, err := oteltrace.TraceIDFromHex(traceID); err == nil {
					sc := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
						TraceID:    tid,
						SpanID:     oteltrace.SpanID{1},
						TraceFlags: oteltrace.FlagsSampled,
						Remote:     true,
					})
					ctx = oteltrace.ContextWithSpanContext(ctx, sc)
				}
			}
		}
	}

	spanName := "mq.consume/" + msg.Topic
	ctx, span := otel.Tracer("go-blade-mq").Start(ctx, spanName, oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	return ctx, span
}

// BuildSubscribeOptions 解析并补全 SubscribeOptions 的默认值。
// 当启用 Broadcast 模式时，自动在消费组名后追加唯一实例标识以确保每个订阅者拥有独立消费流。
func BuildSubscribeOptions(serviceName string, opts ...SubscribeOption) SubscribeOptions {
	subOpts := SubscribeOptions{
		Group:        DefaultGroup,
		MaxRetries:   3,
		RetryBackoff: 20 * time.Millisecond,
	}
	if s := strings.TrimSpace(serviceName); s != "" && s != "go-blade" {
		subOpts.Group = s + "-group"
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&subOpts)
		}
	}
	if strings.TrimSpace(subOpts.Group) == "" {
		subOpts.Group = DefaultGroup
	}
	if subOpts.Broadcast {
		subOpts.Group = fmt.Sprintf("%s-broadcast-%s", subOpts.Group, snowflake.GenIDString())
	}
	return subOpts
}

// ExecuteHandlerWithRetry 执行消费者 Handler，内置 Panic 捕获恢复、指数退避重试与死信队列 (<topic>.dlq) 自动转发。
func ExecuteHandlerWithRetry(ctx context.Context, broker Broker, msg Message, subOpts SubscribeOptions, handler HandlerFunc) error {
	msgCtx, span := ExtractMessageContext(ctx, msg)
	defer span.End()

	if msg.TenantID == "" {
		msg.TenantID = tenant.FromTenant(msgCtx)
	}
	if msg.TraceID == "" {
		msg.TraceID = trace.TraceIDFromContext(msgCtx)
	}

	maxAttempts := subOpts.MaxRetries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	backoff := subOpts.RetryBackoff
	if backoff <= 0 {
		backoff = 10 * time.Millisecond
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			wait := backoff * time.Duration(1<<(attempt-1))
			if wait > time.Second {
				wait = time.Second
			}
			select {
			case <-time.After(wait):
			case <-msgCtx.Done():
				return msgCtx.Err()
			}
		}

		lastErr = invokeConsumerSafely(msgCtx, msg, handler)
		if lastErr == nil {
			return nil
		}
		logx.WithContext(msgCtx).Errorf("[mq] consume topic=%s msg_id=%s attempt=%d/%d failed: %v",
			msg.Topic, msg.ID, attempt+1, maxAttempts, lastErr)
	}

	// 若重试耗尽且未禁用 DLQ，且当前 Topic 本身不是死信队列，则自动转发至 <topic>.dlq
	if !subOpts.DisableDLQ && broker != nil && !strings.HasSuffix(msg.Topic, DLQTopicSuffix) {
		dlqTopic := msg.Topic + DLQTopicSuffix
		dlqHeaders := make(map[string]string, len(msg.Headers)+3)
		for k, v := range msg.Headers {
			dlqHeaders[k] = v
		}
		dlqHeaders[HeaderOriginalTopic] = msg.Topic
		dlqHeaders[HeaderDeadReason] = lastErr.Error()
		dlqHeaders[HeaderRetryCount] = fmt.Sprintf("%d", subOpts.MaxRetries)

		if pubErr := broker.Publish(msgCtx, dlqTopic, msg.Payload, WithKey(msg.Key), WithHeaders(dlqHeaders)); pubErr != nil {
			logx.WithContext(msgCtx).Errorf("[mq] forward message %s to DLQ %s failed: %v", msg.ID, dlqTopic, pubErr)
		}
	}

	return lastErr
}

func invokeConsumerSafely(ctx context.Context, msg Message, handler HandlerFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("consumer panic: %v", r)
		}
	}()
	return handler(ctx, msg)
}
