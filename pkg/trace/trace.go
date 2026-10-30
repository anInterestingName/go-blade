// Package trace 提供基于 OpenTelemetry 的分布式链路追踪初始化、Span 创建与 HTTP 追踪中间件能力。
package trace

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

const (
	// HeaderTraceID HTTP 响应头中回写链路追踪 TraceID 的键名 ("X-Trace-Id")。
	HeaderTraceID = "X-Trace-Id"
)

// Config 链路追踪配置。
type Config struct {
	// Endpoint OTLP HTTP 或 Jaeger 上报端点 (如 http://127.0.0.1:4318/v1/traces 或 http://127.0.0.1:14268/api/traces)
	Endpoint string `json:"endpoint,optional"`
	// Sampler 采样比率 (0.0 至 1.0，默认为 1.0 全量采样)
	Sampler float64 `json:"sampler,optional"`
	// ServiceName 当前微服务名 (如 service-gateway, service-auth)
	ServiceName string `json:"serviceName,optional"`
}

// TracerProviderHolder 包装 TracerProvider 与释放逻辑。
type TracerProviderHolder struct {
	tp *sdktrace.TracerProvider
}

// InitTracer 初始化全局 OpenTelemetry TracerProvider。
// 若 cfg.Endpoint 包含 "/api/traces" 则兼容 Jaeger Exporter；
// 若 cfg.Endpoint 为其他非空地址则使用标准 OTLP HTTP Exporter；
// 若 cfg.Endpoint 为空则创建无 Exporter 的内存采样 TracerProvider，确保本地总是能生成有效的 TraceID 和 SpanID。
func InitTracer(cfg Config) (*TracerProviderHolder, error) {
	if cfg.Sampler <= 0 {
		cfg.Sampler = 1.0
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "go-blade-service"
	}

	var exp sdktrace.SpanExporter
	var err error
	endpoint := strings.TrimSpace(cfg.Endpoint)
	if endpoint != "" {
		if strings.Contains(endpoint, "/api/traces") {
			exp, err = jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(endpoint)))
			if err != nil {
				return nil, fmt.Errorf("create jaeger exporter failed: %w", err)
			}
		} else {
			if !strings.Contains(endpoint, "://") {
				endpoint = "http://" + endpoint
			}
			exp, err = otlptracehttp.New(context.Background(), otlptracehttp.WithEndpointURL(endpoint))
			if err != nil {
				return nil, fmt.Errorf("create otlp http exporter failed: %w", err)
			}
		}
	}

	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(cfg.ServiceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create trace resource failed: %w", err)
	}

	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.Sampler))

	var tpOpts []sdktrace.TracerProviderOption
	tpOpts = append(tpOpts, sdktrace.WithSampler(sampler), sdktrace.WithResource(res))
	if exp != nil {
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp))
	}

	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return &TracerProviderHolder{tp: tp}, nil
}

// Shutdown 优雅关闭 Tracer 上报通道并刷盘。
func (h *TracerProviderHolder) Shutdown(ctx context.Context) error {
	if h != nil && h.tp != nil {
		return h.tp.Shutdown(ctx)
	}
	return nil
}

// StartSpan 基于全局 TracerProvider 开启一个新的子 Span，返回携带新 Span 的上下文与 Span 实例。
func StartSpan(ctx context.Context, spanName string) (context.Context, oteltrace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(spanName) == "" {
		spanName = "unnamed-span"
	}
	return otel.Tracer("go-blade").Start(ctx, spanName)
}

// TraceMiddleware 构造 HTTP 链路追踪中间件：
// 提取上游 W3C traceparent 请求头、开启 Server Span、将生成的 TraceID 写入响应头 X-Trace-Id 并在请求结束时结束 Span。
func TraceMiddleware(serviceName string) func(http.HandlerFunc) http.HandlerFunc {
	if strings.TrimSpace(serviceName) == "" {
		serviceName = "go-blade-http"
	}
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
			spanName := r.Method + " " + r.URL.Path
			ctx, span := otel.Tracer(serviceName).Start(ctx, spanName, oteltrace.WithSpanKind(oteltrace.SpanKindServer))
			defer span.End()

			if traceID := TraceIDFromContext(ctx); traceID != "" {
				w.Header().Set(HeaderTraceID, traceID)
			}

			next(w, r.WithContext(ctx))
		}
	}
}

type traceIDKeyType struct{}

var traceIDCtxKey = traceIDKeyType{}

// WithTraceID 将指定的链路追踪 TraceID 字符串注入到 context.Context 中并返回衍生上下文。
// 适用于异步事件消费、后台任务等无活跃 Span 或跨协程场景下的 TraceID 兜底透传。
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, traceIDCtxKey, strings.TrimSpace(traceID))
}

// TraceIDFromContext 从 context.Context 中提取当前活跃 Span 的 TraceID 字符串。
// 若无活跃 Span，则回退检查由 WithTraceID 注入的上下文 TraceID。
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	span := oteltrace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().IsValid() {
		return span.SpanContext().TraceID().String()
	}
	if v, ok := ctx.Value(traceIDCtxKey).(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return ""
}

// SpanIDFromContext 从 context.Context 中提取当前活跃 Span 的 SpanID 字符串。
func SpanIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	span := oteltrace.SpanFromContext(ctx)
	if span != nil && span.SpanContext().IsValid() {
		return span.SpanContext().SpanID().String()
	}
	return ""
}

// MetadataCarrier 将 google.golang.org/grpc/metadata.MD 适配为 OpenTelemetry propagation.TextMapCarrier 接口。
type MetadataCarrier struct {
	// MD 底层 gRPC 元数据映射表。
	MD metadata.MD
}

// Get 从 gRPC Metadata 中读取指定键（忽略大小写）的首个值。
func (c MetadataCarrier) Get(key string) string {
	if c.MD == nil {
		return ""
	}
	vals := c.MD.Get(strings.ToLower(key))
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// Set 将键值对写入 gRPC Metadata（键名统一转为小写以符合 HTTP/2 与 gRPC Metadata 规范）。
func (c MetadataCarrier) Set(key, value string) {
	if c.MD == nil {
		return
	}
	c.MD.Set(strings.ToLower(key), value)
}

// Keys 返回当前 gRPC Metadata 中的所有键列表。
func (c MetadataCarrier) Keys() []string {
	if c.MD == nil {
		return nil
	}
	keys := make([]string, 0, len(c.MD))
	for k := range c.MD {
		keys = append(keys, k)
	}
	return keys
}

// InjectGRPCMetadata 将当前 ctx 中的 OpenTelemetry TraceContext（如 W3C traceparent）注入到 gRPC Metadata 中。
func InjectGRPCMetadata(ctx context.Context, md metadata.MD) {
	if ctx == nil || md == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, MetadataCarrier{MD: md})
}

// ExtractGRPCMetadata 从 gRPC Metadata 中提取上游传递的 OpenTelemetry TraceContext 并写入返回的 context.Context。
func ExtractGRPCMetadata(ctx context.Context, md metadata.MD) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if md == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, MetadataCarrier{MD: md})
}
