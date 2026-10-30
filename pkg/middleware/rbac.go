// Package middleware 提供微服务通用的 HTTP 中间件，包括 JWT 鉴权拦截、RBAC 权限校验、上下文注入与跨域支持。
package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/xerr"
)

const (
	// SuperAdminRoleAdministrator SpringBlade 默认超级管理员角色别名 ("administrator")
	SuperAdminRoleAdministrator = "administrator"
	// SuperAdminRoleAdmin 常用超级管理员角色简写 ("admin")
	SuperAdminRoleAdmin = "admin"
)

type permissionsCtxKeyType struct{}

var permissionsCtxKey = permissionsCtxKeyType{}

// WithPermissions 将当前用户拥有的权限码集合（如 ["user:add", "user:edit"]）注入到 context.Context 中。
func WithPermissions(ctx context.Context, perms []string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	copied := make([]string, 0, len(perms))
	for _, p := range perms {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			copied = append(copied, trimmed)
		}
	}
	return context.WithValue(ctx, permissionsCtxKey, copied)
}

// FromPermissions 从 context.Context 中提取当前用户拥有的权限码切片。
// 若未设置权限上下文，返回 nil。
func FromPermissions(ctx context.Context) []string {
	if ctx == nil {
		return nil
	}
	if perms, ok := ctx.Value(permissionsCtxKey).([]string); ok {
		return perms
	}
	return nil
}

// isSuperAdminRole 判断给定的逗号分隔角色字符串中是否包含超级管理员角色（administrator 或 admin）。
func isSuperAdminRole(roleNames string) bool {
	for _, r := range splitNormalized(roleNames) {
		lower := strings.ToLower(r)
		if lower == SuperAdminRoleAdministrator || lower == SuperAdminRoleAdmin {
			return true
		}
	}
	return false
}

// splitNormalized 按逗号切分并去除首尾空白。
func splitNormalized(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			res = append(res, t)
		}
	}
	return res
}

// RequireRoles 构造接口级角色校验 HTTP 中间件。
// 检查当前请求上下文 FromClaims(r.Context()).RoleName（支持逗号分隔的多个角色名，如 "admin,user"）：
// 若包含超级管理员角色（"administrator" 或 "admin"）或匹配任意 allowedRoles 则放行；
// 否则返回 HTTP 403 Forbidden 与 xerr.Forbidden 统一 JSON 响应。
func RequireRoles(allowedRoles ...string) func(http.HandlerFunc) http.HandlerFunc {
	allowedSet := make(map[string]struct{}, len(allowedRoles))
	for _, role := range allowedRoles {
		for _, item := range splitNormalized(role) {
			allowedSet[strings.ToLower(item)] = struct{}{}
		}
	}

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			claims := FromClaims(r.Context())
			if claims == nil {
				response.FailJsonWithStatus(r.Context(), w, http.StatusForbidden, xerr.Forbidden, "权限不足，拒绝访问")
				return
			}

			// 超级管理员角色直接放行
			if isSuperAdminRole(claims.RoleName) {
				next(w, r)
				return
			}

			// 若未配置任何目标角色限制，默认已登录即可放行
			if len(allowedSet) == 0 {
				next(w, r)
				return
			}

			for _, userRole := range splitNormalized(claims.RoleName) {
				if _, ok := allowedSet[strings.ToLower(userRole)]; ok {
					next(w, r)
					return
				}
			}

			response.FailJsonWithStatus(r.Context(), w, http.StatusForbidden, xerr.Forbidden, "权限不足，拒绝访问")
		}
	}
}

// RequirePermissions 构造接口级权限码校验 HTTP 中间件。
// 检查当前请求上下文中的权限码集合 FromPermissions(r.Context())（或超级管理员角色 administrator / admin）：
// 当具备全部 requiredPerms（或拥有 "*" 通配权限 / 超级管理员身份）时放行，否则返回 HTTP 403 Forbidden。
func RequirePermissions(requiredPerms ...string) func(http.HandlerFunc) http.HandlerFunc {
	normalizedRequired := make([]string, 0, len(requiredPerms))
	for _, perm := range requiredPerms {
		if trimmed := strings.TrimSpace(perm); trimmed != "" {
			normalizedRequired = append(normalizedRequired, trimmed)
		}
	}

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			claims := FromClaims(ctx)

			// 超级管理员角色直接豁免权限码检查
			if claims != nil && isSuperAdminRole(claims.RoleName) {
				next(w, r)
				return
			}

			if len(normalizedRequired) == 0 {
				next(w, r)
				return
			}

			userPerms := FromPermissions(ctx)
			if len(userPerms) == 0 {
				response.FailJsonWithStatus(ctx, w, http.StatusForbidden, xerr.Forbidden, "权限不足，拒绝访问")
				return
			}

			permSet := make(map[string]struct{}, len(userPerms))
			for _, p := range userPerms {
				permSet[strings.TrimSpace(p)] = struct{}{}
			}

			// 支持 "*:*:*" 或 "*" 全局通配权限码
			if _, hasAll := permSet["*"]; hasAll {
				next(w, r)
				return
			}
			if _, hasAll := permSet["*:*:*"]; hasAll {
				next(w, r)
				return
			}

			for _, reqPerm := range normalizedRequired {
				if _, ok := permSet[reqPerm]; !ok {
					response.FailJsonWithStatus(ctx, w, http.StatusForbidden, xerr.Forbidden, "权限不足，拒绝访问")
					return
				}
			}

			next(w, r)
		}
	}
}
