// Package handler 负责 HTTP 路由请求接入、参数解析与统一结果响应输出。
package handler

import (
	"net/http"

	"github.com/go-blade/pkg/middleware"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/services/auth/internal/logic"
	"github.com/go-blade/services/auth/internal/svc"
	"github.com/go-blade/services/auth/internal/types"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/rest/httpx"
)

// RegisterHandlers 注册认证微服务的所有 HTTP 路由并挂载中间件。
func RegisterHandlers(server *rest.Server, serverCtx *svc.ServiceContext) {
	// 全局中间件：跨域 CORS 与 租户上下文注入
	server.Use(rest.Middleware(middleware.CorsMiddleware))
	server.Use(rest.Middleware(tenant.TenantMiddleware))

	// 认证与令牌核心路由（免登录白名单路由）
	server.AddRoutes(
		[]rest.Route{
			{
				Method:  http.MethodGet,
				Path:    "/blade-auth/oauth/captcha",
				Handler: CaptchaHandler(serverCtx),
			},
			{
				Method:  http.MethodPost,
				Path:    "/blade-auth/oauth/token",
				Handler: TokenHandler(serverCtx),
			},
			{
				Method:  http.MethodPost,
				Path:    "/blade-auth/oauth/refresh-token",
				Handler: RefreshTokenHandler(serverCtx),
			},
			{
				Method:  http.MethodGet,
				Path:    "/blade-auth/oauth/login",
				Handler: GetLoginChallengeHandler(serverCtx),
			},
			{
				Method:  http.MethodPost,
				Path:    "/blade-auth/oauth/login",
				Handler: AcceptLoginChallengeHandler(serverCtx),
			},
			{
				Method:  http.MethodPost,
				Path:    "/blade-auth/oauth/consent",
				Handler: AcceptConsentChallengeHandler(serverCtx),
			},
			{
				Method:  http.MethodPost,
				Path:    "/blade-auth/oauth/logout",
				Handler: AcceptLogoutChallengeHandler(serverCtx),
			},
			// 为支持跨域预检，显式注册 OPTIONS 路由
			{
				Method:  http.MethodOptions,
				Path:    "/blade-auth/oauth/captcha",
				Handler: optionsHandler,
			},
			{
				Method:  http.MethodOptions,
				Path:    "/blade-auth/oauth/token",
				Handler: optionsHandler,
			},
			{
				Method:  http.MethodOptions,
				Path:    "/blade-auth/oauth/refresh-token",
				Handler: optionsHandler,
			},
			{
				Method:  http.MethodOptions,
				Path:    "/blade-auth/oauth/login",
				Handler: optionsHandler,
			},
			{
				Method:  http.MethodOptions,
				Path:    "/blade-auth/oauth/consent",
				Handler: optionsHandler,
			},
			{
				Method:  http.MethodOptions,
				Path:    "/blade-auth/oauth/logout",
				Handler: optionsHandler,
			},
		},
	)

	// 受保护路由示例（演示 AuthMiddleware 挂载生效与用户信息上下文提取）
	authMiddleware := rest.Middleware(middleware.AuthMiddleware(serverCtx.JWTHandler))
	server.AddRoutes(
		rest.WithMiddlewares(
			[]rest.Middleware{authMiddleware},
			rest.Route{
				Method:  http.MethodGet,
				Path:    "/blade-auth/oauth/user-info",
				Handler: UserInfoHandler(serverCtx),
			},
		),
	)
}

func optionsHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

// UserInfoHandler 获取受保护的用户信息处理函数（验证 AuthMiddleware 上下文传递）。
func UserInfoHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims := middleware.FromClaims(r.Context())
		response.ResultResponse(r.Context(), w, claims, nil)
	}
}

// CaptchaHandler 获取验证码处理函数。
func CaptchaHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := logic.NewCaptchaLogic(r.Context(), svcCtx)
		resp, err := l.Captcha()
		response.ResultResponse(r.Context(), w, resp, err)
	}
}

// TokenHandler 用户登录颁发 Token 处理函数。
func TokenHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.TokenReq
		if err := httpx.Parse(r, &req); err != nil {
			response.ResultResponse(r.Context(), w, nil, err)
			return
		}

		l := logic.NewTokenLogic(r.Context(), svcCtx)
		resp, err := l.Token(&req)
		response.ResultResponse(r.Context(), w, resp, err)
	}
}

// RefreshTokenHandler 刷新 Token 处理函数。
func RefreshTokenHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.RefreshTokenReq
		if err := httpx.Parse(r, &req); err != nil {
			response.ResultResponse(r.Context(), w, nil, err)
			return
		}

		l := logic.NewRefreshTokenLogic(r.Context(), svcCtx)
		resp, err := l.RefreshToken(&req)
		response.ResultResponse(r.Context(), w, resp, err)
	}
}

// GetLoginChallengeHandler 查询 Ory Hydra 登录挑战处理函数。
func GetLoginChallengeHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.GetLoginChallengeReq
		_ = httpx.Parse(r, &req)
		if req.LoginChallenge == "" {
			req.LoginChallenge = r.URL.Query().Get("login_challenge")
		}
		if req.LoginChallenge == "" {
			req.LoginChallenge = r.URL.Query().Get("loginChallenge")
		}

		l := logic.NewHydraLogic(r.Context(), svcCtx)
		resp, err := l.GetLoginChallenge(&req)
		response.ResultResponse(r.Context(), w, resp, err)
	}
}

// AcceptLoginChallengeHandler 提交多租户账号密码与验证码完成 Ory Hydra 登录挑战处理函数。
func AcceptLoginChallengeHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.AcceptLoginChallengeReq
		if err := httpx.Parse(r, &req); err != nil {
			response.ResultResponse(r.Context(), w, nil, err)
			return
		}
		if req.LoginChallenge == "" {
			req.LoginChallenge = r.URL.Query().Get("login_challenge")
		}

		l := logic.NewHydraLogic(r.Context(), svcCtx)
		resp, err := l.AcceptLoginChallenge(&req)
		response.ResultResponse(r.Context(), w, resp, err)
	}
}

// AcceptConsentChallengeHandler 确认授权并注入 BladeClaims 完成 Ory Hydra 授权挑战处理函数。
func AcceptConsentChallengeHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.AcceptConsentChallengeReq
		if err := httpx.Parse(r, &req); err != nil {
			response.ResultResponse(r.Context(), w, nil, err)
			return
		}
		if req.ConsentChallenge == "" {
			req.ConsentChallenge = r.URL.Query().Get("consent_challenge")
		}

		l := logic.NewHydraLogic(r.Context(), svcCtx)
		resp, err := l.AcceptConsentChallenge(&req)
		response.ResultResponse(r.Context(), w, resp, err)
	}
}

// AcceptLogoutChallengeHandler 完成 Ory Hydra 登出挑战或本地 JWT 令牌主动吊销处理函数。
func AcceptLogoutChallengeHandler(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req types.AcceptLogoutChallengeReq
		if r.ContentLength != 0 {
			_ = httpx.Parse(r, &req)
		}
		if req.LogoutChallenge == "" {
			req.LogoutChallenge = r.URL.Query().Get("logout_challenge")
		}
		if req.LogoutChallenge == "" {
			req.LogoutChallenge = r.URL.Query().Get("logoutChallenge")
		}
		if req.AccessToken == "" {
			req.AccessToken = r.Header.Get(middleware.HeaderBladeAuth)
		}
		if req.AccessToken == "" {
			req.AccessToken = r.Header.Get(middleware.HeaderAuthorization)
		}

		l := logic.NewHydraLogic(r.Context(), svcCtx)
		resp, err := l.AcceptLogoutChallenge(&req)
		response.ResultResponse(r.Context(), w, resp, err)
	}
}
