// Package ratelimit 提供 Redis 分布式限流、进程内内存回退限流与 HTTP 429 中间件的单元测试。
package ratelimit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

func setupMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)

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

func TestLimiter_RedisDistributedMode(t *testing.T) {
	mr := setupMiniredis(t)
	ctx := context.Background()

	limiter := NewLimiter(Config{
		Rate:      3,
		Window:    2 * time.Second,
		KeyPrefix: "blade:ratelimit:",
	})

	// 前 3 次请求应被放行
	for i := 0; i < 3; i++ {
		allowed, err := limiter.Allow(ctx, "tenant1:/api/login:127.0.0.1")
		require.NoError(t, err)
		assert.True(t, allowed, "request %d should be allowed", i+1)
	}

	// 第 4 次请求超出阈值应返回 false
	allowed, err := limiter.Allow(ctx, "tenant1:/api/login:127.0.0.1")
	require.NoError(t, err)
	assert.False(t, allowed, "4th request should be rate limited")

	// 不同 key 互不影响
	otherAllowed, err := limiter.Allow(ctx, "tenant2:/api/login:127.0.0.1")
	require.NoError(t, err)
	assert.True(t, otherAllowed)

	// 推进 Redis 时间窗口过期后自动恢复放行
	mr.FastForward(3 * time.Second)
	recovered, err := limiter.Allow(ctx, "tenant1:/api/login:127.0.0.1")
	require.NoError(t, err)
	assert.True(t, recovered, "request should be allowed after window expiration")
}

func TestLimiter_MemoryFallbackAndConcurrency(t *testing.T) {
	// 确保全局 Redis 未初始化，走进程内内存限流回退
	pkgredis.SetRedis(nil)
	ctx := context.Background()

	limiter := NewLimiter(Config{
		Rate:   5,
		Window: 80 * time.Millisecond,
	})

	const totalRequests = 20
	var allowedCount int32
	var wg sync.WaitGroup
	wg.Add(totalRequests)

	for i := 0; i < totalRequests; i++ {
		go func() {
			defer wg.Done()
			ok, err := limiter.Allow(ctx, "mem:concurrent:user")
			if err == nil && ok {
				atomic.AddInt32(&allowedCount, 1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int32(5), allowedCount, "only 5 requests should pass within window")

	// 等待窗口过期后恢复放行
	time.Sleep(100 * time.Millisecond)
	ok, err := limiter.Allow(ctx, "mem:concurrent:user")
	require.NoError(t, err)
	assert.True(t, ok, "should recover after in-memory window expires")
}

func TestRateLimitMiddleware_HTTP429AndTenantIsolation(t *testing.T) {
	pkgredis.SetRedis(nil)

	limiter := NewLimiter(Config{
		Rate:   2,
		Window: time.Second,
	})

	mw := RateLimitMiddleware(limiter, nil)
	handler := tenant.TenantMiddleware(mw(func(w http.ResponseWriter, r *http.Request) {
		response.OkJson(r.Context(), w, "ok")
	}))

	// 租户 000000 连续发起 2 次请求成功
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/captcha", nil)
		req.RemoteAddr = "10.0.0.12:54321"
		req.Header.Set(tenant.HeaderTenantID, "000000")
		rec := httptest.NewRecorder()
		handler(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// 第 3 次请求触发 429 限流拦截并返回统一 Response JSON
	reqLimited := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/captcha", nil)
	reqLimited.RemoteAddr = "10.0.0.12:54321"
	reqLimited.Header.Set(tenant.HeaderTenantID, "000000")
	recLimited := httptest.NewRecorder()
	handler(recLimited, reqLimited)

	require.Equal(t, http.StatusTooManyRequests, recLimited.Code)

	var body response.Response[any]
	err := json.Unmarshal(recLimited.Body.Bytes(), &body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusTooManyRequests, body.Code)
	assert.False(t, body.Success)
	assert.Equal(t, TooManyRequestsMessage, body.Msg)

	// 另一租户 999999 不受影响
	reqOtherTenant := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/captcha", nil)
	reqOtherTenant.RemoteAddr = "10.0.0.12:54321"
	reqOtherTenant.Header.Set(tenant.HeaderTenantID, "999999")
	recOtherTenant := httptest.NewRecorder()
	handler(recOtherTenant, reqOtherTenant)
	require.Equal(t, http.StatusOK, recOtherTenant.Code)
}
