package httpclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_TraceTenantPropagation_And_TransientRetry(t *testing.T) {
	tp, err := trace.InitTracer(trace.Config{ServiceName: "test-httpclient"})
	require.NoError(t, err)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	var attempts int32
	var capturedTraceParent, capturedTenant string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(&attempts, 1)
		capturedTraceParent = r.Header.Get("traceparent")
		capturedTenant = r.Header.Get(tenant.HeaderTenantID)
		if cur < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"temporary overload"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	cli := NewClient(Config{
		ServiceName:  "external-payment",
		Timeout:      2 * time.Second,
		MaxRetries:   2,
		RetryBackoff: 5 * time.Millisecond,
	})

	ctx := tenant.WithTenant(context.Background(), "666888")
	ctx, span := trace.StartSpan(ctx, "caller-op")
	defer span.End()

	var resp map[string]string
	err = cli.DoJSON(ctx, http.MethodPost, srv.URL+"/v1/charge/998877", map[string]int{"amount": 100}, &resp)
	require.NoError(t, err)
	assert.Equal(t, "ok", resp["status"])
	assert.Equal(t, int32(3), atomic.LoadInt32(&attempts))
	assert.NotEmpty(t, capturedTraceParent)
	assert.Equal(t, "666888", capturedTenant)
}

func TestClient_CircuitBreaker_4xxExemptionAnd5xxTrip(t *testing.T) {
	var statusCode atomic.Int32
	statusCode.Store(http.StatusBadRequest)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := int(statusCode.Load())
		w.WriteHeader(code)
	}))
	defer srv.Close()

	cli := NewClient(Config{
		ServiceName:      "flaky-downstream",
		MaxRetries:       0,
		FailureThreshold: 3,
		Cooldown:         60 * time.Millisecond,
	})

	// 1. 连续 5 次 400 Bad Request 业务错误不应触发熔断
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/items", nil)
		resp, err := cli.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, StateClosed, cli.State())
	}

	// 2. 切换为 500 Internal Server Error，连续 3 次后熔断器进入 Open 状态
	statusCode.Store(http.StatusInternalServerError)
	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/items", nil)
		resp, err := cli.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}
	assert.Equal(t, StateOpen, cli.State())

	// Open 状态下请求立即被拒绝并返回 ErrCircuitOpen
	reqBlocked, _ := http.NewRequest(http.MethodGet, srv.URL+"/items", nil)
	_, err := cli.Do(reqBlocked)
	require.ErrorIs(t, err, ErrCircuitOpen)

	// 3. 等待 Cooldown 冷却后，State() 应自动演进为 StateHalfOpen，且下游恢复 200 OK 后探测成功恢复至 Closed
	time.Sleep(75 * time.Millisecond)
	assert.Equal(t, StateHalfOpen, cli.State())
	statusCode.Store(http.StatusOK)
	reqRecover, _ := http.NewRequest(http.MethodGet, srv.URL+"/items", nil)
	resp, err := cli.Do(reqRecover)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, StateClosed, cli.State())
}
