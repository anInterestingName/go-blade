// Package metrics 提供 Prometheus HTTP 与 gRPC 监控指标收集器及中间件的单元测试。
package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetrics_HTTPMiddlewareAndHandler(t *testing.T) {
	mw := MetricsMiddleware("service-gateway")
	bizHandler := mw(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/blade-auth/oauth/token" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":401}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"code":200}`))
	})

	// 发送 2 次 200 请求和 1 次 401 请求
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/blade-system/user/list", nil)
		rec := httptest.NewRecorder()
		bizHandler(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	req401 := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", nil)
	rec401 := httptest.NewRecorder()
	bizHandler(rec401, req401)
	require.Equal(t, http.StatusUnauthorized, rec401.Code)

	// 记录一次 gRPC 调用指标
	RecordGRPC("service-system", "/system.v1.UserService/GetUser", "OK", 15*time.Millisecond)
	RecordGRPC("service-auth", "/auth.v1.AuthService/Verify", "Unauthenticated", 5*time.Millisecond)

	// 请求 /metrics 验证 Prometheus 文本输出
	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRec := httptest.NewRecorder()
	MetricsHandler()(metricsRec, metricsReq)

	require.Equal(t, http.StatusOK, metricsRec.Code)
	body := metricsRec.Body.String()

	assert.Contains(t, body, `blade_http_requests_total{method="GET",path="/blade-system/user/list",service="service-gateway",status="200"} 2`)
	assert.Contains(t, body, `blade_http_requests_total{method="POST",path="/blade-auth/oauth/token",service="service-gateway",status="401"} 1`)
	assert.Contains(t, body, `blade_http_request_duration_seconds_count{method="GET",path="/blade-system/user/list",service="service-gateway"} 2`)
	assert.Contains(t, body, `blade_grpc_requests_total{code="OK",method="/system.v1.UserService/GetUser",service="service-system"} 1`)
	assert.Contains(t, body, `blade_grpc_requests_total{code="Unauthenticated",method="/auth.v1.AuthService/Verify",service="service-auth"} 1`)
	assert.Contains(t, body, `blade_grpc_request_duration_seconds_count{method="/system.v1.UserService/GetUser",service="service-system"} 1`)
}

func TestMetrics_IsolatedCollector(t *testing.T) {
	c := NewCollector()
	c.RecordHTTP("service-desk", "GET", "/blade-desk/notice/list", "200", 8*time.Millisecond)
	c.RecordGRPC("service-desk", "/desk.v1.NoticeService/List", "OK", 4*time.Millisecond)
	c.RecordDB("blade_notice", "query", "OK", 2*time.Millisecond)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	c.Handler()(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, `blade_http_requests_total{method="GET",path="/blade-desk/notice/list",service="service-desk",status="200"} 1`)
	assert.Contains(t, body, `blade_grpc_requests_total{code="OK",method="/desk.v1.NoticeService/List",service="service-desk"} 1`)
	assert.Contains(t, body, `blade_db_queries_total{operation="query",status="OK",table="blade_notice"} 1`)
}

func TestNormalizePath_AntiHighCardinality(t *testing.T) {
	assert.Equal(t, "/blade-system/user/:id", NormalizePath("/blade-system/user/1782390182301"))
	assert.Equal(t, "/blade-system/dept/:id/detail", NormalizePath("/blade-system/dept/550e8400-e29b-41d4-a716-446655440000/detail"))
	assert.Equal(t, "/blade-system/user/list", NormalizePath("/blade-system/user/list"))
	assert.Equal(t, "/", NormalizePath(""))
}
