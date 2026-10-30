// Package trace 提供分布式链路追踪初始化、Span 创建与 HTTP 中间件的单元测试。
package trace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"google.golang.org/grpc/metadata"
)

func TestTrace_InitAndContextExtraction(t *testing.T) {
	holder, err := InitTracer(Config{
		ServiceName: "test-trace-service",
		Sampler:     1.0,
	})
	require.NoError(t, err)
	defer func() {
		_ = holder.Shutdown(context.Background())
	}()

	// 未开启 Span 时返回空字符串
	emptyTraceID := TraceIDFromContext(context.Background())
	assert.Empty(t, emptyTraceID)

	// 启动一个测试 Span
	tracer := otel.Tracer("test-tracer")
	ctx, span := tracer.Start(context.Background(), "test-operation")
	defer span.End()

	traceID := TraceIDFromContext(ctx)
	spanID := SpanIDFromContext(ctx)

	assert.NotEmpty(t, traceID)
	assert.NotEmpty(t, spanID)
	assert.Len(t, traceID, 32)
	assert.Len(t, spanID, 16)
}

func TestTrace_InitExportersAndStartSpan(t *testing.T) {
	// 1. OTLP HTTP Exporter 初始化
	otlpHolder, err := InitTracer(Config{
		Endpoint:    "http://127.0.0.1:4318/v1/traces",
		ServiceName: "service-otlp-test",
		Sampler:     1.0,
	})
	require.NoError(t, err)
	require.NotNil(t, otlpHolder)

	// 2. Jaeger Exporter 兼容初始化
	jaegerHolder, err := InitTracer(Config{
		Endpoint:    "http://127.0.0.1:14268/api/traces",
		ServiceName: "service-jaeger-test",
		Sampler:     1.0,
	})
	require.NoError(t, err)
	require.NotNil(t, jaegerHolder)
	_ = jaegerHolder.Shutdown(context.Background())
	_ = otlpHolder.Shutdown(context.Background())

	// 3. 切回无 Exporter 内存采样模式并验证 StartSpan 父子关系
	memHolder, err := InitTracer(Config{
		ServiceName: "service-memory-test",
		Sampler:     1.0,
	})
	require.NoError(t, err)
	defer func() {
		_ = memHolder.Shutdown(context.Background())
	}()

	parentCtx, parentSpan := StartSpan(context.Background(), "parent-op")
	defer parentSpan.End()

	childCtx, childSpan := StartSpan(parentCtx, "child-op")
	defer childSpan.End()

	parentTraceID := TraceIDFromContext(parentCtx)
	childTraceID := TraceIDFromContext(childCtx)
	parentSpanID := SpanIDFromContext(parentCtx)
	childSpanID := SpanIDFromContext(childCtx)

	assert.NotEmpty(t, parentTraceID)
	assert.Equal(t, parentTraceID, childTraceID)
	assert.NotEqual(t, parentSpanID, childSpanID)
}

func TestTrace_TraceMiddleware(t *testing.T) {
	holder, err := InitTracer(Config{
		ServiceName: "service-gateway",
		Sampler:     1.0,
	})
	require.NoError(t, err)
	defer func() {
		_ = holder.Shutdown(context.Background())
	}()

	var handlerTraceID, handlerSpanID string
	mw := TraceMiddleware("service-gateway")
	handler := mw(func(w http.ResponseWriter, r *http.Request) {
		handlerTraceID = TraceIDFromContext(r.Context())
		handlerSpanID = SpanIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	// 场景 1：无上游 traceparent 时自动开启根 Span 并回写 X-Trace-Id
	req1 := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
	rec1 := httptest.NewRecorder()
	handler(rec1, req1)

	assert.Equal(t, http.StatusOK, rec1.Code)
	assert.NotEmpty(t, handlerTraceID)
	assert.NotEmpty(t, handlerSpanID)
	assert.Equal(t, handlerTraceID, rec1.Header().Get(HeaderTraceID))

	// 场景 2：携带上游 W3C traceparent 时继承相同 TraceID 并生成新子 SpanID
	upstreamCtx, upstreamSpan := StartSpan(context.Background(), "upstream-client")
	defer upstreamSpan.End()
	upstreamTraceID := TraceIDFromContext(upstreamCtx)
	upstreamSpanID := SpanIDFromContext(upstreamCtx)

	req2 := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", nil)
	otel.GetTextMapPropagator().Inject(upstreamCtx, propagation.HeaderCarrier(req2.Header))
	rec2 := httptest.NewRecorder()
	handler(rec2, req2)

	assert.Equal(t, upstreamTraceID, handlerTraceID)
	assert.Equal(t, upstreamTraceID, rec2.Header().Get(HeaderTraceID))
	assert.NotEqual(t, upstreamSpanID, handlerSpanID)
}

func TestTrace_GRPCMetadataCarrier(t *testing.T) {
	holder, err := InitTracer(Config{
		ServiceName: "service-grpc-carrier-test",
		Sampler:     1.0,
	})
	require.NoError(t, err)
	defer func() {
		_ = holder.Shutdown(context.Background())
	}()

	parentCtx, parentSpan := StartSpan(context.Background(), "grpc-client-root")
	defer parentSpan.End()
	expectedTraceID := TraceIDFromContext(parentCtx)
	parentSpanID := SpanIDFromContext(parentCtx)

	md := metadata.New(nil)
	InjectGRPCMetadata(parentCtx, md)

	carrier := MetadataCarrier{MD: md}
	assert.NotEmpty(t, carrier.Get("traceparent"))
	assert.Contains(t, carrier.Keys(), "traceparent")

	extractedCtx := ExtractGRPCMetadata(context.Background(), md)
	serverCtx, serverSpan := StartSpan(extractedCtx, "grpc-server-child")
	defer serverSpan.End()

	assert.Equal(t, expectedTraceID, TraceIDFromContext(serverCtx))
	assert.NotEqual(t, parentSpanID, SpanIDFromContext(serverCtx))
}
