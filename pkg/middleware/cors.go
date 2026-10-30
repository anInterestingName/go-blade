// Package middleware 提供微服务通用的 HTTP 中间件，包括 JWT 鉴权拦截、上下文注入与跨域支持。
package middleware

import (
	"net/http"
)

// CorsMiddleware 构造标准跨域资源共享 (CORS) 中间件。
// 全面放行 SpringBlade / Saber 前端常用的自定义请求头（Blade-Auth、Blade-Tenant-Id 等），
// 当请求携带 Origin 且需要凭据 (Credentials) 时动态回显 Origin，优雅处理浏览器 OPTIONS 预检请求。
func CorsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" {
			// 根据 W3C CORS 规范：当 Allow-Credentials 为 true 时，Allow-Origin 不能为通配符 *
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		} else {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}

		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With, Blade-Auth, Blade-Tenant-Id")
		w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type")

		// 处理预检请求 (OPTIONS)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next(w, r)
	}
}
