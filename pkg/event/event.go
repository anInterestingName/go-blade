// Package event 提供支持并发 Worker 池、上下文租户/链路透传、Panic 隔离恢复与优雅关机排空的异步领域事件总线底座。
package event

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"github.com/zeromicro/go-zero/core/logx"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const (
	// DefaultWorkers 异步事件总线默认并发 Worker 协程数。
	DefaultWorkers = 4
	// DefaultBufferSize 异步事件总线默认内存缓冲队列容量。
	DefaultBufferSize = 1024
)

var (
	// ErrBusClosed 表示事件总线已关闭，拒绝继续发布新事件。
	ErrBusClosed = errors.New("event bus is closed")
	// ErrEmptyTopic 表示事件主题名称不能为空。
	ErrEmptyTopic = errors.New("event topic must not be empty")
	// ErrQueueFull 表示异步事件队列已满且无法在当前上下文内投递。
	ErrQueueFull = errors.New("event queue is full")
)

// Message 定义领域事件总线传递的标准事件消息载体。
type Message struct {
	// ID 事件全局唯一标识（默认采用雪花算法生成）
	ID string `json:"id"`
	// Topic 事件主题名称（如 "blade.audit.api"）
	Topic string `json:"topic"`
	// TenantID 事件触发时的租户编号
	TenantID string `json:"tenant_id"`
	// TraceID 事件触发时的分布式链路追踪 ID
	TraceID string `json:"trace_id"`
	// Payload 事件携带的业务载荷数据
	Payload any `json:"payload"`
	// OccurredAt 事件发生时间戳
	OccurredAt time.Time `json:"occurred_at"`
}

// Event 是 Message 的语义类型别名，兼容不同业务域对领域事件的命名习惯。
type Event = Message

// HandlerFunc 定义异步领域事件消费者的处理函数签名。
type HandlerFunc func(ctx context.Context, msg Message) error

// Handler 是 HandlerFunc 的语义类型别名。
type Handler = HandlerFunc

// Bus 定义领域事件总线的标准接口契约。
type Bus interface {
	// Subscribe 订阅指定主题的领域事件，支持同一主题注册多个消费者处理器。
	Subscribe(topic string, handler HandlerFunc)
	// Publish 向指定主题异步发布一条领域事件，自动提取并透传当前上下文的 TenantID 与 TraceID。
	Publish(ctx context.Context, topic string, payload any) error
	// Close 优雅关闭事件总线并排空所有在途事件（直到队列处理完毕或 ctx 超时）。
	Close(ctx context.Context) error
}

// Options 定义创建 AsyncBus 时的配置参数。
type Options struct {
	// Workers 异步消费 Worker 协程数（默认 4）
	Workers int
	// BufferSize 事件缓冲通道大小（默认 1024）
	BufferSize int
}

// Option 定义 AsyncBus 的函数式配置项。
type Option func(*Options)

// WithWorkers 设置异步事件总线的 Worker 协程数量。
func WithWorkers(workers int) Option {
	return func(o *Options) {
		if workers > 0 {
			o.Workers = workers
		}
	}
}

// WithBufferSize 设置异步事件总线的缓冲队列大小。
func WithBufferSize(size int) Option {
	return func(o *Options) {
		if size > 0 {
			o.BufferSize = size
		}
	}
}

type eventTask struct {
	msg     Message
	spanCtx oteltrace.SpanContext
}

// AsyncBus 基于内存通道与并发 Worker 池实现的异步领域事件总线。
// 支持多订阅者分发、TenantID/TraceID 上下文自动跨协程保持、单消费者 Panic 捕获隔离以及优雅停机排空。
type AsyncBus struct {
	workers    int
	bufferSize int

	mu          sync.RWMutex
	subscribers map[string][]HandlerFunc
	closed      bool

	queue     chan eventTask
	wg        sync.WaitGroup
	closeOnce sync.Once
}

var (
	defaultBusMu sync.Mutex
	defaultBus   *AsyncBus
)

// NewAsyncBus 根据可选配置创建并启动一个并发安全的异步事件总线实例。
func NewAsyncBus(opts ...Option) *AsyncBus {
	cfg := Options{
		Workers:    DefaultWorkers,
		BufferSize: DefaultBufferSize,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.Workers <= 0 {
		cfg.Workers = DefaultWorkers
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultBufferSize
	}

	b := &AsyncBus{
		workers:     cfg.Workers,
		bufferSize:  cfg.BufferSize,
		subscribers: make(map[string][]HandlerFunc),
		queue:       make(chan eventTask, cfg.BufferSize),
	}

	for i := 0; i < b.workers; i++ {
		b.wg.Add(1)
		go b.workerLoop()
	}

	return b
}

// NewBus 创建并启动一个异步事件总线实例（NewAsyncBus 的语义别名）。
func NewBus(opts ...Option) *AsyncBus {
	return NewAsyncBus(opts...)
}

// DefaultBus 获取全局默认的异步领域事件总线单例。
// 若默认单例尚未初始化或已被关闭，则自动重新初始化一个活跃实例。
func DefaultBus() *AsyncBus {
	defaultBusMu.Lock()
	defer defaultBusMu.Unlock()

	if defaultBus == nil || defaultBus.isClosed() {
		defaultBus = NewAsyncBus()
	}
	return defaultBus
}

// Subscribe 为指定主题注册一个事件消费处理函数。
func (b *AsyncBus) Subscribe(topic string, handler HandlerFunc) {
	cleanTopic := strings.TrimSpace(topic)
	if cleanTopic == "" || handler == nil {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.subscribers[cleanTopic] = append(b.subscribers[cleanTopic], handler)
}

// Publish 将业务载荷封装为标准领域事件并投递至异步队列。
// 自动从 ctx 提取 TenantID 与 TraceID，在异步 Worker 执行 Handler 时自动还原到衍生 Context 中。
func (b *AsyncBus) Publish(ctx context.Context, topic string, payload any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	cleanTopic := strings.TrimSpace(topic)
	if cleanTopic == "" {
		return ErrEmptyTopic
	}

	tenantID := tenant.FromTenant(ctx)
	traceID := trace.TraceIDFromContext(ctx)
	var spanCtx oteltrace.SpanContext
	if span := oteltrace.SpanFromContext(ctx); span != nil && span.SpanContext().IsValid() {
		spanCtx = span.SpanContext()
	} else if len(traceID) == 32 {
		if tid, err := oteltrace.TraceIDFromHex(traceID); err == nil {
			spanCtx = oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
				TraceID:    tid,
				SpanID:     oteltrace.SpanID{1},
				TraceFlags: oteltrace.FlagsSampled,
				Remote:     true,
			})
		}
	}

	msg := Message{
		ID:         snowflake.GenIDString(),
		Topic:      cleanTopic,
		TenantID:   tenantID,
		TraceID:    traceID,
		Payload:    payload,
		OccurredAt: time.Now(),
	}

	task := eventTask{
		msg:     msg,
		spanCtx: spanCtx,
	}

	// 持有读锁完成闭合状态检查与入队，防止与 Close(ctx) 关闭 channel 产生竞态 Panic
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return ErrBusClosed
	}

	select {
	case b.queue <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close 优雅关闭异步事件总线（Graceful Drain）：
// 停止接收新事件并关闭底层队列，等待所有在途事件消费完成，或者直到传入的 ctx 超时取消。
func (b *AsyncBus) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		close(b.queue)
		b.mu.Unlock()
	})

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

func (b *AsyncBus) isClosed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.closed
}

func (b *AsyncBus) workerLoop() {
	defer b.wg.Done()
	for task := range b.queue {
		b.dispatch(task)
	}
}

func (b *AsyncBus) dispatch(task eventTask) {
	b.mu.RLock()
	handlers := b.subscribers[task.msg.Topic]
	if len(handlers) == 0 {
		b.mu.RUnlock()
		return
	}
	copied := make([]HandlerFunc, len(handlers))
	copy(copied, handlers)
	b.mu.RUnlock()

	baseCtx := context.Background()
	if task.msg.TenantID != "" {
		baseCtx = tenant.WithTenant(baseCtx, task.msg.TenantID)
	}
	if task.msg.TraceID != "" {
		baseCtx = trace.WithTraceID(baseCtx, task.msg.TraceID)
	}
	if task.spanCtx.IsValid() {
		baseCtx = oteltrace.ContextWithSpanContext(baseCtx, task.spanCtx)
	}

	for _, h := range copied {
		b.invokeHandlerSafely(baseCtx, task.msg, h)
	}
}

func (b *AsyncBus) invokeHandlerSafely(ctx context.Context, msg Message, h HandlerFunc) {
	defer func() {
		if r := recover(); r != nil {
			logx.WithContext(ctx).Errorf("[event] subscriber panic recovered on topic=%s event_id=%s: %v", msg.Topic, msg.ID, r)
		}
	}()

	if err := h(ctx, msg); err != nil {
		logx.WithContext(ctx).Errorf("[event] subscriber handler error on topic=%s event_id=%s: %v", msg.Topic, msg.ID, err)
	}
}
