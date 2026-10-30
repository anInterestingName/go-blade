// Package hydra 封装 Ory Hydra v2 标准 Admin REST API 客户端接口与实现。
package hydra

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-blade/pkg/httpclient"
)

// OAuth2Client Ory Hydra OAuth 2.1 客户端摘要信息。
type OAuth2Client struct {
	// ClientID 客户端唯一标识
	ClientID string `json:"client_id"`
	// ClientName 客户端显示名称
	ClientName string `json:"client_name,omitempty"`
}

// LoginRequest Ory Hydra 登录挑战请求详情。
type LoginRequest struct {
	// Challenge 登录挑战标识
	Challenge string `json:"challenge"`
	// Skip 是否可跳过登录认证（用户已在 Hydra 存在有效会话）
	Skip bool `json:"skip"`
	// Subject 已认证主体标识
	Subject string `json:"subject"`
	// Client 发起授权请求的 OAuth2 客户端信息
	Client OAuth2Client `json:"client"`
	// RequestURL 原始 OAuth2 授权请求 URL
	RequestURL string `json:"request_url,omitempty"`
	// RequestedScope 客户端请求授权的 scope 列表
	RequestedScope []string `json:"requested_scope,omitempty"`
	// RequestedAccessTokenAudience 客户端请求的目标受众列表
	RequestedAccessTokenAudience []string `json:"requested_access_token_audience,omitempty"`
	// Context 附加上下文元数据
	Context map[string]any `json:"context,omitempty"`
}

// AcceptLoginBody 接受 Ory Hydra 登录挑战的请求体载荷。
type AcceptLoginBody struct {
	// Subject 认证通过的用户唯一主体标识
	Subject string `json:"subject"`
	// Remember 是否让 Hydra 记住本次登录状态
	Remember bool `json:"remember,omitempty"`
	// RememberFor 登录状态记住时长（秒）
	RememberFor int64 `json:"remember_for,omitempty"`
	// ACR 认证上下文类别引用
	ACR string `json:"acr,omitempty"`
	// Context 传递给后续 Consent 阶段的自定义上下文元数据（如 BladeClaims）
	Context map[string]any `json:"context,omitempty"`
}

// ConsentRequest Ory Hydra 授权挑战请求详情。
type ConsentRequest struct {
	// Challenge 授权挑战标识
	Challenge string `json:"challenge"`
	// Skip 是否可跳过用户交互授权
	Skip bool `json:"skip"`
	// Subject 登录阶段已确认的用户主体标识
	Subject string `json:"subject"`
	// Client 发起授权请求的 OAuth2 客户端信息
	Client OAuth2Client `json:"client"`
	// RequestedScope 客户端请求授权的 scope 列表
	RequestedScope []string `json:"requested_scope,omitempty"`
	// RequestedAccessTokenAudience 客户端请求的目标受众列表
	RequestedAccessTokenAudience []string `json:"requested_access_token_audience,omitempty"`
	// Context 登录阶段传递过来的自定义上下文元数据
	Context map[string]any `json:"context,omitempty"`
}

// ConsentSession 注入到 Ory Hydra Access Token 与 ID Token 的自定义会话声明。
type ConsentSession struct {
	// AccessToken 注入到 JWT Access Token 或内省响应中的自定义 Claims
	AccessToken map[string]any `json:"access_token,omitempty"`
	// IDToken 注入到 OIDC ID Token 中的自定义 Claims
	IDToken map[string]any `json:"id_token,omitempty"`
}

// AcceptConsentBody 接受 Ory Hydra 授权挑战的请求体载荷。
type AcceptConsentBody struct {
	// GrantScope 最终授予客户端的权限范围列表
	GrantScope []string `json:"grant_scope,omitempty"`
	// GrantAccessTokenAudience 最终授予客户端的目标受众列表
	GrantAccessTokenAudience []string `json:"grant_access_token_audience,omitempty"`
	// Remember 是否记住本次授权决定
	Remember bool `json:"remember,omitempty"`
	// RememberFor 记住授权决定的有效时长（秒）
	RememberFor int64 `json:"remember_for,omitempty"`
	// Session 注入到 Access Token 和 ID Token 的自定义声明载荷
	Session *ConsentSession `json:"session,omitempty"`
}

// LogoutRequest Ory Hydra 登出挑战请求详情。
type LogoutRequest struct {
	// Challenge 登出挑战标识
	Challenge string `json:"challenge"`
	// Subject 请求登出的用户主体标识
	Subject string `json:"subject"`
	// Sid 会话 ID
	Sid string `json:"sid,omitempty"`
	// RequestURL 原始登出请求 URL
	RequestURL string `json:"request_url,omitempty"`
	// RPInitiated 是否由依赖方（RP）发起
	RPInitiated bool `json:"rp_initiated,omitempty"`
}

// RedirectResponse Ory Hydra 接受挑战后返回的重定向响应结构。
type RedirectResponse struct {
	// RedirectTo 下一步浏览器重定向地址
	RedirectTo string `json:"redirect_to"`
}

// AdminClient 定义 Ory Hydra v2 Admin API 核心协同接口。
type AdminClient interface {
	// GetLoginRequest 查询登录挑战详情。
	GetLoginRequest(ctx context.Context, challenge string) (*LoginRequest, error)
	// AcceptLoginRequest 接受登录挑战并返回重定向 URL。
	AcceptLoginRequest(ctx context.Context, challenge string, body AcceptLoginBody) (*RedirectResponse, error)
	// GetConsentRequest 查询授权挑战详情。
	GetConsentRequest(ctx context.Context, challenge string) (*ConsentRequest, error)
	// AcceptConsentRequest 接受授权挑战并注入自定义 Token 会话载荷，返回重定向 URL。
	AcceptConsentRequest(ctx context.Context, challenge string, body AcceptConsentBody) (*RedirectResponse, error)
	// GetLogoutRequest 查询登出挑战详情。
	GetLogoutRequest(ctx context.Context, challenge string) (*LogoutRequest, error)
	// AcceptLogoutRequest 接受登出挑战并返回重定向 URL。
	AcceptLogoutRequest(ctx context.Context, challenge string) (*RedirectResponse, error)
}

// HTTPAdminClient 基于 pkg/httpclient 治理底座的 Ory Hydra v2 Admin REST API 客户端实现。
// 自动具备 OpenTelemetry W3C traceparent 透传、Prometheus 指标采集、瞬态重试与熔断保护能力。
type HTTPAdminClient struct {
	baseURL string
	client  *httpclient.Client
}

// NewHTTPAdminClient 构造并初始化 HTTPAdminClient 实例。
// baseURL 为 Hydra Admin 服务地址（例如 "http://localhost:4445"）。
func NewHTTPAdminClient(baseURL string, rawClient ...*http.Client) *HTTPAdminClient {
	govClient := httpclient.NewClient(httpclient.Config{
		ServiceName:  "ory-hydra-admin",
		Timeout:      10 * time.Second,
		MaxRetries:   1,
		RetryBackoff: 30 * time.Millisecond,
	}, rawClient...)
	return &HTTPAdminClient{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		client:  govClient,
	}
}

// GetLoginRequest 调用 GET /admin/oauth2/auth/requests/login 查询登录挑战详情。
func (c *HTTPAdminClient) GetLoginRequest(ctx context.Context, challenge string) (*LoginRequest, error) {
	if strings.TrimSpace(challenge) == "" {
		return nil, errors.New("login_challenge cannot be empty")
	}
	endpoint := fmt.Sprintf("%s/admin/oauth2/auth/requests/login?login_challenge=%s", c.baseURL, url.QueryEscape(challenge))
	var res LoginRequest
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// AcceptLoginRequest 调用 PUT /admin/oauth2/auth/requests/login/accept 接受登录挑战。
func (c *HTTPAdminClient) AcceptLoginRequest(ctx context.Context, challenge string, body AcceptLoginBody) (*RedirectResponse, error) {
	if strings.TrimSpace(challenge) == "" {
		return nil, errors.New("login_challenge cannot be empty")
	}
	endpoint := fmt.Sprintf("%s/admin/oauth2/auth/requests/login/accept?login_challenge=%s", c.baseURL, url.QueryEscape(challenge))
	var res RedirectResponse
	if err := c.doJSON(ctx, http.MethodPut, endpoint, body, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// GetConsentRequest 调用 GET /admin/oauth2/auth/requests/consent 查询授权挑战详情。
func (c *HTTPAdminClient) GetConsentRequest(ctx context.Context, challenge string) (*ConsentRequest, error) {
	if strings.TrimSpace(challenge) == "" {
		return nil, errors.New("consent_challenge cannot be empty")
	}
	endpoint := fmt.Sprintf("%s/admin/oauth2/auth/requests/consent?consent_challenge=%s", c.baseURL, url.QueryEscape(challenge))
	var res ConsentRequest
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// AcceptConsentRequest 调用 PUT /admin/oauth2/auth/requests/consent/accept 接受授权挑战并注入 Token 会话载荷。
func (c *HTTPAdminClient) AcceptConsentRequest(ctx context.Context, challenge string, body AcceptConsentBody) (*RedirectResponse, error) {
	if strings.TrimSpace(challenge) == "" {
		return nil, errors.New("consent_challenge cannot be empty")
	}
	endpoint := fmt.Sprintf("%s/admin/oauth2/auth/requests/consent/accept?consent_challenge=%s", c.baseURL, url.QueryEscape(challenge))
	var res RedirectResponse
	if err := c.doJSON(ctx, http.MethodPut, endpoint, body, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// GetLogoutRequest 调用 GET /admin/oauth2/auth/requests/logout 查询登出挑战详情。
func (c *HTTPAdminClient) GetLogoutRequest(ctx context.Context, challenge string) (*LogoutRequest, error) {
	if strings.TrimSpace(challenge) == "" {
		return nil, errors.New("logout_challenge cannot be empty")
	}
	endpoint := fmt.Sprintf("%s/admin/oauth2/auth/requests/logout?logout_challenge=%s", c.baseURL, url.QueryEscape(challenge))
	var res LogoutRequest
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// AcceptLogoutRequest 调用 PUT /admin/oauth2/auth/requests/logout/accept 接受登出挑战。
func (c *HTTPAdminClient) AcceptLogoutRequest(ctx context.Context, challenge string) (*RedirectResponse, error) {
	if strings.TrimSpace(challenge) == "" {
		return nil, errors.New("logout_challenge cannot be empty")
	}
	endpoint := fmt.Sprintf("%s/admin/oauth2/auth/requests/logout/accept?logout_challenge=%s", c.baseURL, url.QueryEscape(challenge))
	var res RedirectResponse
	if err := c.doJSON(ctx, http.MethodPut, endpoint, map[string]any{}, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *HTTPAdminClient) doJSON(ctx context.Context, method, endpoint string, reqBody any, respOut any) error {
	if c == nil || c.baseURL == "" || c.client == nil {
		return errors.New("hydra admin client base url is not configured")
	}
	if err := c.client.DoJSON(ctx, method, endpoint, reqBody, respOut); err != nil {
		return fmt.Errorf("hydra admin api failed: %w", err)
	}
	return nil
}
