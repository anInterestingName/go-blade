// Package middleware 提供微服务通用的 HTTP 中间件单元测试。
package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/xerr"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

func setupIdempotentTestRedis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := zeroredis.MustNewRedis(zeroredis.RedisConf{
		Host: mr.Addr(),
		Type: zeroredis.NodeType,
	})
	pkgredis.SetRedis(client)
	t.Cleanup(func() {
		mr.Close()
		pkgredis.SetRedis(nil)
	})
	return mr
}

func TestIdempotentMiddleware_PassWithoutHeaderOrGet(t *testing.T) {
	_ = setupIdempotentTestRedis(t)

	mw := IdempotentMiddleware(5 * time.Second)
	called := 0
	handler := mw(func(w http.ResponseWriter, r *http.Request) {
		called++
		response.OkJson(r.Context(), w, "ok")
	})

	// 1. 未携带幂等头的 POST 请求应放行多次
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for POST without header, got %d", rec.Code)
		}
	}

	// 2. 携带幂等头的 GET 请求不拦截
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/orders", nil)
		req.Header.Set(HeaderIdempotencyKey, "same-get-token")
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for GET with header, got %d", rec.Code)
		}
	}

	if called != 4 {
		t.Fatalf("expected handler called 4 times, got %d", called)
	}
}

func TestIdempotentMiddleware_BlockDuplicateSubmission(t *testing.T) {
	mr := setupIdempotentTestRedis(t)

	mw := IdempotentMiddleware(5 * time.Second)
	called := 0
	handler := mw(func(w http.ResponseWriter, r *http.Request) {
		called++
		response.OkJson(r.Context(), w, "created")
	})

	// 第一次提交：携带 Idempotency-Key，放行
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil)
	req1.Header.Set(HeaderIdempotencyKey, "token-1001")
	rec1 := httptest.NewRecorder()
	handler(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected first request to return 200, got %d", rec1.Code)
	}

	// 第二次重复提交（相同 method + path + token）：拦截返回 409 Conflict
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil)
	req2.Header.Set(HeaderBladeIdempotentToken, "token-1001")
	rec2 := httptest.NewRecorder()
	handler(rec2, req2)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("expected duplicate request to return 409 Conflict, got %d", rec2.Code)
	}

	var resp response.Response[any]
	if err := json.Unmarshal(rec2.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if resp.Code != xerr.RequestParamError {
		t.Errorf("expected business code %d, got %d", xerr.RequestParamError, resp.Code)
	}
	if !strings.Contains(resp.Msg, "请勿重复提交请求") {
		t.Errorf("expected msg '请勿重复提交请求', got %q", resp.Msg)
	}

	// TTL 过期后再次提交应允许通过
	mr.FastForward(6 * time.Second)
	req3 := httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil)
	req3.Header.Set(HeaderIdempotencyKey, "token-1001")
	rec3 := httptest.NewRecorder()
	handler(rec3, req3)
	if rec3.Code != http.StatusOK {
		t.Fatalf("expected request after TTL expiration to return 200, got %d", rec3.Code)
	}

	if called != 2 {
		t.Fatalf("expected handler called twice, got %d", called)
	}

	// 跨租户隔离验证：不同租户使用相同的 Idempotency-Key 不应互相阻塞
	reqTenantA := httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil)
	reqTenantA.Header.Set("Blade-Tenant-Id", "100001")
	reqTenantA.Header.Set(HeaderIdempotencyKey, "shared-token-999")
	recTenantA := httptest.NewRecorder()
	handler(recTenantA, reqTenantA)
	if recTenantA.Code != http.StatusOK {
		t.Fatalf("expected tenant 100001 request to succeed, got %d", recTenantA.Code)
	}

	reqTenantB := httptest.NewRequest(http.MethodPost, "/api/v1/orders", nil)
	reqTenantB.Header.Set("Blade-Tenant-Id", "200002")
	reqTenantB.Header.Set(HeaderIdempotencyKey, "shared-token-999")
	recTenantB := httptest.NewRecorder()
	handler(recTenantB, reqTenantB)
	if recTenantB.Code != http.StatusOK {
		t.Fatalf("expected tenant 200002 with same idempotency key to succeed without cross-tenant collision, got %d", recTenantB.Code)
	}
}
