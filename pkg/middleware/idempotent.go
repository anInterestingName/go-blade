// Package middleware 提供微服务通用的 HTTP 中间件，包括 JWT 鉴权拦截、上下文注入、跨域支持与防重放幂等保护。
package middleware

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-blade/pkg/lock"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
)

const (
	// HeaderIdempotencyKey 标准 HTTP 幂等请求头名称。
	HeaderIdempotencyKey = "Idempotency-Key"
	// HeaderBladeIdempotentToken 对标 Blade 规范的自定义幂等令牌请求头名称。
	HeaderBladeIdempotentToken = "Blade-Idempotent-Token"
	// IdempotentKeyPrefix 幂等锁在 Redis/内存锁中的统一键前缀。
	IdempotentKeyPrefix = "blade:idempotent:"
	// DefaultIdempotentTTL 默认幂等防重放窗口有效期（10秒）。
	DefaultIdempotentTTL = 10 * time.Second
)

// IdempotentMiddleware 构造防重复提交的 HTTP 幂等拦截中间件。
// 默认对非幂等写请求（POST, PUT, PATCH, DELETE）检查请求头 `Idempotency-Key` 或 `Blade-Idempotent-Token`；
// 若未携带该请求头则直接放行（兼容常规请求）；
// 若携带了该请求头，则在有效期内锁定 `blade:idempotent:<method>:<path>:<token>`，
// 有效期内重复请求将直接返回 HTTP 409 Conflict 与统一业务错误码 xerr.RequestParamError ("请勿重复提交请求")。
func IdempotentMiddleware(ttl time.Duration) func(http.HandlerFunc) http.HandlerFunc {
	if ttl <= 0 {
		ttl = DefaultIdempotentTTL
	}

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !isWriteMethod(r.Method) {
				next(w, r)
				return
			}

			token := extractIdempotentToken(r)
			if token == "" {
				next(w, r)
				return
			}

			tenantID := tenant.FromTenant(r.Context())
			if hdrTenant := strings.TrimSpace(r.Header.Get(tenant.HeaderTenantID)); hdrTenant != "" && tenantID == tenant.DefaultTenantID {
				tenantID = hdrTenant
			}
			lockKey := fmt.Sprintf("%s%s:%s:%s:%s", IdempotentKeyPrefix, tenantID, strings.ToUpper(r.Method), r.URL.Path, token)
			_, err := lock.Acquire(r.Context(), lockKey, ttl)
			if err != nil {
				response.FailJsonWithStatus(r.Context(), w, http.StatusConflict, xerr.RequestParamError, "请勿重复提交请求")
				return
			}

			next(w, r)
		}
	}
}

func isWriteMethod(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func extractIdempotentToken(r *http.Request) string {
	if token := strings.TrimSpace(r.Header.Get(HeaderIdempotencyKey)); token != "" {
		return token
	}
	return strings.TrimSpace(r.Header.Get(HeaderBladeIdempotentToken))
}
