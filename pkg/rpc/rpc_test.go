// Package rpc 提供统一的 gRPC 客户端与服务端拦截器、TraceContext 传播及 Prometheus 指标打点的单元测试。
package rpc

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/metrics"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
)

// customCodec 允许在单测中直接传递 string 避免强制依赖 proto.Message 结构体
type customCodec struct{}

func (customCodec) Marshal(v any) ([]byte, error) {
	if s, ok := v.(*string); ok {
		return []byte(*s), nil
	}
	if s, ok := v.(string); ok {
		return []byte(s), nil
	}
	return []byte{}, nil
}

func (customCodec) Unmarshal(data []byte, v any) error {
	if s, ok := v.(*string); ok {
		*s = string(data)
	}
	return nil
}

func (customCodec) Name() string {
	return "custom"
}

func init() {
	encoding.RegisterCodec(customCodec{})
}

func TestUnaryInterceptors_TenantPropagation(t *testing.T) {
	// 创建测试 Listener
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	var receivedTenant string

	// 模拟 gRPC 服务端注册 Dummy Handler
	srv := NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.TestService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Ping",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					var in string
					_ = dec(&in)
					handler := func(innerCtx context.Context, req any) (any, error) {
						receivedTenant = tenant.FromTenant(innerCtx)
						reply := "pong"
						return &reply, nil
					}
					if interceptor != nil {
						return interceptor(ctx, &in, &grpc.UnaryServerInfo{FullMethod: "/test.TestService/Ping"}, handler)
					}
					return handler(ctx, &in)
				},
			},
		},
	}, struct{}{})

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	// 客户端连接，使用 customCodec 绕过 protobuf 强类型校验
	conn, err := Dial(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype((customCodec{}).Name())),
	)
	require.NoError(t, err)
	defer conn.Close()

	// 1. 测试未显式设置租户时，客户端与服务端默认使用 DefaultTenantID ("000000")
	var reply string
	pingReq := "ping"
	err = conn.Invoke(context.Background(), "/test.TestService/Ping", &pingReq, &reply)
	assert.NoError(t, err)
	assert.Equal(t, "pong", reply)
	assert.Equal(t, tenant.DefaultTenantID, receivedTenant)

	// 2. 测试显式注入自定义租户 "tenant_888"
	ctxCustom := tenant.WithTenant(context.Background(), "tenant_888")
	err = conn.Invoke(ctxCustom, "/test.TestService/Ping", &pingReq, &reply)
	assert.NoError(t, err)
	assert.Equal(t, "tenant_888", receivedTenant)
}

func TestUnaryInterceptors_FullClaimsPropagation(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	var receivedClaims *jwt.BladeClaims
	var receivedTenant string

	srv := NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.TestService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "CheckIdentity",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					var in string
					_ = dec(&in)
					handler := func(innerCtx context.Context, req any) (any, error) {
						receivedTenant = tenant.FromTenant(innerCtx)
						receivedClaims = jwt.FromClaims(innerCtx)
						reply := "verified"
						return &reply, nil
					}
					if interceptor != nil {
						return interceptor(ctx, &in, &grpc.UnaryServerInfo{FullMethod: "/test.TestService/CheckIdentity"}, handler)
					}
					return handler(ctx, &in)
				},
			},
		},
	}, struct{}{})

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	conn, err := Dial(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype((customCodec{}).Name())),
	)
	require.NoError(t, err)
	defer conn.Close()

	expectedClaims := &jwt.BladeClaims{
		TenantID: "100001",
		UserID:   9007199254740993,
		DeptID:   9007199254740995,
		RoleID:   "101,102",
		RoleName: "admin,editor",
		Account:  "blade_admin",
		UserName: "张管理员",
		NickName: "小张",
	}

	ctx := jwt.WithClaims(context.Background(), expectedClaims)
	var reply string
	req := "hello"
	err = conn.Invoke(ctx, "/test.TestService/CheckIdentity", &req, &reply)
	require.NoError(t, err)
	assert.Equal(t, "verified", reply)
	assert.Equal(t, "100001", receivedTenant)
	require.NotNil(t, receivedClaims)
	assert.Equal(t, expectedClaims.UserID, receivedClaims.UserID)
	assert.Equal(t, expectedClaims.DeptID, receivedClaims.DeptID)
	assert.Equal(t, expectedClaims.RoleID, receivedClaims.RoleID)
	assert.Equal(t, expectedClaims.RoleName, receivedClaims.RoleName)
	assert.Equal(t, expectedClaims.Account, receivedClaims.Account)
	assert.Equal(t, expectedClaims.UserName, receivedClaims.UserName)
	assert.Equal(t, expectedClaims.NickName, receivedClaims.NickName)
}

func TestServerUnaryInterceptor_PanicRecover(t *testing.T) {
	interceptor := ServerUnaryInterceptor()

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.TestService/PanicMethod",
	}

	panicHandler := func(ctx context.Context, req any) (any, error) {
		panic("database connection failure simulate")
	}

	resp, err := interceptor(context.Background(), nil, info, panicHandler)
	assert.Nil(t, resp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "database connection failure simulate")
}

func TestServerUnaryInterceptor_MetadataExtract(t *testing.T) {
	interceptor := ServerUnaryInterceptor()

	info := &grpc.UnaryServerInfo{
		FullMethod: "/test.TestService/Echo",
	}

	var extractedTenant string
	handler := func(ctx context.Context, req any) (any, error) {
		extractedTenant = tenant.FromTenant(ctx)
		return "ok", nil
	}

	// 构造附带 Incoming Metadata 的 context
	md := metadata.Pairs(MetadataTenantID, "tenant_999")
	ctxWithMD := metadata.NewIncomingContext(context.Background(), md)

	resp, err := interceptor(ctxWithMD, nil, info, handler)
	assert.NoError(t, err)
	assert.Equal(t, "ok", resp)
	assert.Equal(t, "tenant_999", extractedTenant)
}

func TestUnaryInterceptors_TracePropagationAndMetrics(t *testing.T) {
	tpHolder, err := trace.InitTracer(trace.Config{
		ServiceName: "test-rpc-observability",
		Sampler:     1.0,
	})
	require.NoError(t, err)
	defer func() {
		_ = tpHolder.Shutdown(context.Background())
	}()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	var serverTraceID, serverSpanID, serverReqID string
	var serverIncomingTraceParent string

	srv := NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.ObservabilityService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "TraceAndMetrics",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					var in string
					_ = dec(&in)
					handler := func(innerCtx context.Context, req any) (any, error) {
						serverTraceID = trace.TraceIDFromContext(innerCtx)
						serverSpanID = trace.SpanIDFromContext(innerCtx)
						if v, ok := innerCtx.Value(MetadataRequestID).(string); ok {
							serverReqID = v
						}
						if md, ok := metadata.FromIncomingContext(innerCtx); ok {
							if vals := md.Get("traceparent"); len(vals) > 0 {
								serverIncomingTraceParent = vals[0]
							}
						}
						if in == "not_found" {
							return nil, status.Error(codes.NotFound, "resource not found")
						}
						reply := "traced"
						return &reply, nil
					}
					if interceptor != nil {
						return interceptor(ctx, &in, &grpc.UnaryServerInfo{FullMethod: "/test.ObservabilityService/TraceAndMetrics"}, handler)
					}
					return handler(ctx, &in)
				},
			},
		},
	}, struct{}{})

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	conn, err := Dial(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype((customCodec{}).Name())),
	)
	require.NoError(t, err)
	defer conn.Close()

	// 1. 发起携带上游根 Span 的 gRPC 调用，验证父子 TraceID 一致性与 SpanID 层级演进
	rootCtx, rootSpan := trace.StartSpan(context.Background(), "http-gateway-entry")
	defer rootSpan.End()
	expectedTraceID := trace.TraceIDFromContext(rootCtx)
	rootSpanID := trace.SpanIDFromContext(rootCtx)

	var reply string
	reqOK := "ok"
	err = conn.Invoke(rootCtx, "/test.ObservabilityService/TraceAndMetrics", &reqOK, &reply)
	require.NoError(t, err)
	assert.Equal(t, "traced", reply)
	assert.NotEmpty(t, serverIncomingTraceParent)
	assert.Equal(t, expectedTraceID, serverTraceID, "跨进程 gRPC 服务端必须继承上游相同的 TraceID")
	assert.Equal(t, expectedTraceID, serverReqID, "MetadataRequestID 应自动关联当前 TraceID")
	assert.NotEmpty(t, serverSpanID)
	assert.NotEqual(t, rootSpanID, serverSpanID, "服务端必须开启新的 SpanKindServer 子 Span")

	// 2. 发起返回 NotFound 错误的 gRPC 调用，验证状态码指标采集
	reqNotFound := "not_found"
	err = conn.Invoke(rootCtx, "/test.ObservabilityService/TraceAndMetrics", &reqNotFound, &reply)
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))

	// 3. 验证 Prometheus /metrics 中已自动记录 OK 与 NotFound 的 gRPC 指标
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	metrics.MetricsHandler()(metricsRec, metricsReq)
	require.Equal(t, http.StatusOK, metricsRec.Code)
	metricsBody := metricsRec.Body.String()

	assert.Contains(t, metricsBody, `blade_grpc_requests_total{code="OK",method="/test.ObservabilityService/TraceAndMetrics",service="go-blade-grpc"} 1`)
	assert.Contains(t, metricsBody, `blade_grpc_requests_total{code="NotFound",method="/test.ObservabilityService/TraceAndMetrics",service="go-blade-grpc"} 1`)
	assert.Contains(t, metricsBody, `blade_grpc_request_duration_seconds_count{method="/test.ObservabilityService/TraceAndMetrics",service="go-blade-grpc"} 2`)
}

// TestCircuitBreaker_StateTransitionsAndBusinessErrorExemption 验证熔断器三态转换、快速失败与业务状态码豁免。
func TestCircuitBreaker_StateTransitionsAndBusinessErrorExemption(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	var serverCalls atomic.Int32
	srv := NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.ResilienceService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Call",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					var in string
					_ = dec(&in)
					handler := func(innerCtx context.Context, req any) (any, error) {
						serverCalls.Add(1)
						switch in {
						case "unavailable":
							return nil, status.Error(codes.Unavailable, "downstream unavailable")
						case "not_found":
							return nil, status.Error(codes.NotFound, "entity not found")
						case "invalid_arg":
							return nil, status.Error(codes.InvalidArgument, "bad argument")
						default:
							reply := "ok"
							return &reply, nil
						}
					}
					if interceptor != nil {
						return interceptor(ctx, &in, &grpc.UnaryServerInfo{FullMethod: "/test.ResilienceService/Call"}, handler)
					}
					return handler(ctx, &in)
				},
			},
		},
	}, struct{}{})

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	breaker := NewCircuitBreaker(BreakerConfig{
		FailureThreshold: 3,
		OpenTimeout:      80 * time.Millisecond,
		HalfOpenMaxCalls: 1,
	})

	conn, err := DialWithConfig(ClientConfig{
		Target:  lis.Addr().String(),
		Breaker: breaker,
	},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype((customCodec{}).Name())),
	)
	require.NoError(t, err)
	defer conn.Close()

	var reply string

	// 1. 业务异常（NotFound / InvalidArgument）不应计入系统故障，连续调用也不触发熔断
	for i := 0; i < 5; i++ {
		req := "not_found"
		callErr := conn.Invoke(context.Background(), "/test.ResilienceService/Call", &req, &reply)
		require.Error(t, callErr)
		assert.Equal(t, codes.NotFound, status.Code(callErr))

		reqInvalid := "invalid_arg"
		callErr = conn.Invoke(context.Background(), "/test.ResilienceService/Call", &reqInvalid, &reply)
		require.Error(t, callErr)
		assert.Equal(t, codes.InvalidArgument, status.Code(callErr))
	}
	assert.Equal(t, StateClosed, breaker.State())

	// 2. 连续 3 次系统级故障 codes.Unavailable 达到阈值，触发 StateOpen
	serverCalls.Store(0)
	for i := 0; i < 3; i++ {
		req := "unavailable"
		callErr := conn.Invoke(context.Background(), "/test.ResilienceService/Call", &req, &reply)
		require.Error(t, callErr)
		assert.Equal(t, codes.Unavailable, status.Code(callErr))
	}
	assert.Equal(t, StateOpen, breaker.State())
	assert.Equal(t, int32(3), serverCalls.Load())

	// 3. 处于 StateOpen 且未过冷却期时，拦截器直接快速失败，请求不会打到下游服务端
	reqOK := "ok"
	fastFailErr := conn.Invoke(context.Background(), "/test.ResilienceService/Call", &reqOK, &reply)
	require.Error(t, fastFailErr)
	assert.Equal(t, codes.Unavailable, status.Code(fastFailErr))
	assert.Contains(t, fastFailErr.Error(), "circuit breaker is open")
	assert.Equal(t, int32(3), serverCalls.Load(), "熔断开启期间不应向服务端发送请求")

	// 4. 等待冷却时间 OpenTimeout 过后，自动进入 StateHalfOpen，放行探测请求并恢复至 StateClosed
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, StateHalfOpen, breaker.State())

	recoverErr := conn.Invoke(context.Background(), "/test.ResilienceService/Call", &reqOK, &reply)
	require.NoError(t, recoverErr)
	assert.Equal(t, "ok", reply)
	assert.Equal(t, StateClosed, breaker.State())
	assert.Equal(t, int32(4), serverCalls.Load())
}

// TestClientResilienceInterceptor_TransientRetryAndTimeout 验证客户端瞬态错误自动重试与超时控制。
func TestClientResilienceInterceptor_TransientRetryAndTimeout(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	var attempts atomic.Int32
	srv := NewServer()
	srv.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.RetryService",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "FlakyCall",
				Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
					var in string
					_ = dec(&in)
					handler := func(innerCtx context.Context, req any) (any, error) {
						if in == "slow" {
							select {
							case <-time.After(200 * time.Millisecond):
								reply := "slow-done"
								return &reply, nil
							case <-innerCtx.Done():
								return nil, status.FromContextError(innerCtx.Err()).Err()
							}
						}
						cur := attempts.Add(1)
						if cur <= 2 {
							return nil, status.Error(codes.Unavailable, "temporary network glitch")
						}
						reply := "recovered-on-attempt-3"
						return &reply, nil
					}
					if interceptor != nil {
						return interceptor(ctx, &in, &grpc.UnaryServerInfo{FullMethod: "/test.RetryService/FlakyCall"}, handler)
					}
					return handler(ctx, &in)
				},
			},
		},
	}, struct{}{})

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	breaker := NewCircuitBreaker(BreakerConfig{
		FailureThreshold: 5,
		OpenTimeout:      time.Second,
	})

	conn, err := DialWithConfig(ClientConfig{
		Target:     lis.Addr().String(),
		Timeout:    60,
		MaxRetries: 2,
		Breaker:    breaker,
	},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype((customCodec{}).Name())),
	)
	require.NoError(t, err)
	defer conn.Close()

	// 1. 前 2 次返回 codes.Unavailable，第 3 次返回成功，客户端自动重试并拿到正确响应，且熔断器保持闭合
	var reply string
	req := "flaky"
	err = conn.Invoke(context.Background(), "/test.RetryService/FlakyCall", &req, &reply)
	require.NoError(t, err)
	assert.Equal(t, "recovered-on-attempt-3", reply)
	assert.Equal(t, int32(3), attempts.Load())
	assert.Equal(t, StateClosed, breaker.State())

	// 2. 测试客户端单次调用超时控制生效（Timeout=60ms，下游耗时 200ms，返回 DeadlineExceeded）
	reqSlow := "slow"
	err = conn.Invoke(context.Background(), "/test.RetryService/FlakyCall", &reqSlow, &reply)
	require.Error(t, err)
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
}
