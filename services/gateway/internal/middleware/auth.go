// Package middleware 提供统一网关层的核心中间件（租户提取、跨域与鉴权拦截）。
package middleware

import (
	"net/http"
	"strings"

	"github.com/go-blade/pkg/jwt"
	pkgmiddleware "github.com/go-blade/pkg/middleware"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
)

// GatewayAuthMiddleware 网关统一鉴权与租户提取中间件。
type GatewayAuthMiddleware struct {
	validator jwt.TokenValidator
	whitelist []string
}

// NewGatewayAuthMiddleware 构造网关鉴权中间件（支持任意 TokenValidator，含 JWTHandler、JWKSValidator 或 CompositeValidator）。
func NewGatewayAuthMiddleware(validator jwt.TokenValidator, whitelist []string) *GatewayAuthMiddleware {
	return &GatewayAuthMiddleware{
		validator: validator,
		whitelist: whitelist,
	}
}

// Handle 执行 HTTP 请求前置拦截。
func (m *GatewayAuthMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// 1. 提取请求头中的原始 Blade-Tenant-Id
		rawHeaderTenant := strings.TrimSpace(r.Header.Get(tenant.HeaderTenantID))

		// 2. 检查是否匹配白名单路径
		path := r.URL.Path
		if m.isWhitelisted(path) {
			tenantID := rawHeaderTenant
			if tenantID == "" {
				tenantID = tenant.DefaultTenantID
			}
			ctx = tenant.WithTenant(ctx, tenantID)
			next(w, r.WithContext(ctx))
			return
		}

		// 3. 提取 Token (优先 Blade-Auth，次选 Authorization)
		token := strings.TrimSpace(r.Header.Get(pkgmiddleware.HeaderBladeAuth))
		if token == "" {
			token = strings.TrimSpace(r.Header.Get(pkgmiddleware.HeaderAuthorization))
		}

		if token == "" {
			response.FailJsonWithStatus(ctx, w, http.StatusUnauthorized, xerr.Unauthorized, "未提供认证令牌")
			return
		}

		if m.validator == nil {
			response.FailJsonWithStatus(ctx, w, http.StatusUnauthorized, xerr.TokenInvalid, "令牌校验器未就绪")
			return
		}

		// 4. 解析与校验 Token
		claims, err := m.validator.ValidateToken(ctx, token)
		if err != nil {
			if err == jwt.ErrTokenExpired {
				response.FailJsonWithStatus(ctx, w, http.StatusUnauthorized, xerr.TokenExpired, "令牌已过期，请重新登录")
				return
			}
			if err == jwt.ErrTokenRevoked {
				response.FailJsonWithStatus(ctx, w, http.StatusUnauthorized, xerr.Unauthorized, "令牌已注销，请重新登录")
				return
			}
			response.FailJsonWithStatus(ctx, w, http.StatusUnauthorized, xerr.TokenInvalid, "令牌无效或签名不匹配")
			return
		}

		// 5. 严格校验租户一致性（防止跨租户越权操作，绝不豁免 "000000"）
		if rawHeaderTenant != "" && claims.TenantID != "" && rawHeaderTenant != claims.TenantID {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.Forbidden, "租户信息不匹配，拒绝跨租户访问"))
			return
		}

		// 6. 强制以已验证的 claims.TenantID 作为当前上下文租户，防止未传 Header 时退回 "000000"
		if claims.TenantID != "" {
			ctx = tenant.WithTenant(ctx, claims.TenantID)
		} else if rawHeaderTenant != "" {
			ctx = tenant.WithTenant(ctx, rawHeaderTenant)
		} else {
			ctx = tenant.WithTenant(ctx, tenant.DefaultTenantID)
		}

		// 7. 将当前用户 Claims 注入上下文传递给下游
		ctx = pkgmiddleware.WithClaims(ctx, claims)
		next(w, r.WithContext(ctx))
	}
}

// isWhitelisted 检查目标路径是否属于白名单。
func (m *GatewayAuthMiddleware) isWhitelisted(path string) bool {
	for _, pattern := range m.whitelist {
		if pkgmiddleware.MatchWhitelist(path, pattern) {
			return true
		}
	}
	return false
}
