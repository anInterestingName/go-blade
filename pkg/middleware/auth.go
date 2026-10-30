// Package middleware 提供微服务通用的 HTTP 中间件，包括 JWT 鉴权拦截、上下文注入与跨域支持。
package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
)

const (
	// HeaderBladeAuth SpringBlade 自定义凭证请求头名称 ("Blade-Auth")
	HeaderBladeAuth = "Blade-Auth"
	// HeaderAuthorization 标准 HTTP 鉴权请求头名称 ("Authorization")
	HeaderAuthorization = "Authorization"
)

// WithClaims 将解析出的 BladeClaims 用户凭据载荷注入到 context.Context 中。
// 内部委托给 jwt.WithClaims，确保跨包（HTTP 中间件、gRPC 拦截器、GORM 插件）共用统一 Context Key。
func WithClaims(ctx context.Context, claims *jwt.BladeClaims) context.Context {
	return jwt.WithClaims(ctx, claims)
}

// FromClaims 从 context.Context 中提取当前用户的 BladeClaims 载荷信息。
// 若未登录或提取失败，返回 nil。
func FromClaims(ctx context.Context) *jwt.BladeClaims {
	return jwt.FromClaims(ctx)
}

// MatchWhitelist 判断请求路径 path 是否匹配白名单规则 pattern。
// 支持精确匹配、以 "*" 结尾的前缀通配、或目录边界前缀匹配（防止 /ping 误匹配 /ping_admin）。
func MatchWhitelist(path, pattern string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	if pattern == path {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(path, prefix)
	}
	dirPrefix := strings.TrimSuffix(pattern, "/") + "/"
	return strings.HasPrefix(path, dirPrefix)
}

// AuthMiddleware 构造 HTTP 统一鉴权拦截中间件（基于对称 JWTHandler）。
// jwtHandler: JWT 编解码处理器；whiteList: 允许免登直接放行的 URL 路径或通配白名单列表。
func AuthMiddleware(jwtHandler *jwt.JWTHandler, whiteList ...string) func(http.HandlerFunc) http.HandlerFunc {
	return AuthMiddlewareWithValidator(jwtHandler, whiteList...)
}

// AuthMiddlewareWithValidator 构造支持任意 TokenValidator（含 JWTHandler、JWKSValidator 及 CompositeValidator）的 HTTP 鉴权中间件。
func AuthMiddlewareWithValidator(validator jwt.TokenValidator, whiteList ...string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path

			// 1. 白名单放行校验
			for _, pattern := range whiteList {
				if MatchWhitelist(path, pattern) {
					next(w, r)
					return
				}
			}

			// 2. 提取凭证字符串（优先从 Blade-Auth，其次从 Authorization 获取）
			tokenStr := r.Header.Get(HeaderBladeAuth)
			if tokenStr == "" {
				tokenStr = r.Header.Get(HeaderAuthorization)
			}
			tokenStr = strings.TrimSpace(tokenStr)

			if tokenStr == "" {
				response.FailJsonWithStatus(r.Context(), w, http.StatusUnauthorized, xerr.Unauthorized, "请求未授权，请先登录")
				return
			}

			if validator == nil {
				response.FailJsonWithStatus(r.Context(), w, http.StatusUnauthorized, xerr.TokenInvalid, xerr.MapErrMsg(xerr.TokenInvalid))
				return
			}

			// 3. 解析与校验 Token
			claims, err := validator.ValidateToken(r.Context(), tokenStr)
			if err != nil {
				if err == jwt.ErrTokenExpired {
					response.FailJsonWithStatus(r.Context(), w, http.StatusUnauthorized, xerr.TokenExpired, xerr.MapErrMsg(xerr.TokenExpired))
					return
				}
				if err == jwt.ErrTokenRevoked {
					response.FailJsonWithStatus(r.Context(), w, http.StatusUnauthorized, xerr.Unauthorized, "令牌已注销，请重新登录")
					return
				}
				response.FailJsonWithStatus(r.Context(), w, http.StatusUnauthorized, xerr.TokenInvalid, xerr.MapErrMsg(xerr.TokenInvalid))
				return
			}

			// 4. 严格校验租户一致性：若请求头显式传入了 Blade-Tenant-Id 且与 Token 所属租户不一致（含 000000），一律拒绝
			headerTenant := strings.TrimSpace(r.Header.Get(tenant.HeaderTenantID))
			if headerTenant != "" && claims.TenantID != "" && headerTenant != claims.TenantID {
				response.FailJsonWithStatus(r.Context(), w, http.StatusForbidden, xerr.Forbidden, "租户信息不匹配，拒绝跨租户访问")
				return
			}

			// 5. 将已验证的租户与用户信息注入请求上下文并放行
			ctx := r.Context()
			if claims.TenantID != "" {
				ctx = tenant.WithTenant(ctx, claims.TenantID)
			}
			ctx = WithClaims(ctx, claims)
			next(w, r.WithContext(ctx))
		}
	}
}
