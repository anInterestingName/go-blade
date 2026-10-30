// Package types 定义认证服务的请求与响应数据模型。
// 字段命名完全对齐 api/proto/auth/v1/auth.proto 与 SpringBlade / Saber 前端规范。
package types

// CaptchaResp 获取验证码响应。
type CaptchaResp struct {
	// CaptchaKey 验证码唯一缓存凭证
	CaptchaKey string `json:"captcha_key"`
	// Image 验证码图片 Base64 或占位数据，严禁泄露明文数字！
	Image string `json:"image"`
}

// TokenReq 登录颁发 Token 请求。
// 同时兼容 snake_case 与 camelCase 字段格式（适配直连 Auth 与经 Gateway 转发的请求）。
type TokenReq struct {
	// TenantID 租户编号（snake_case，前端不传则默认为 "000000"）
	TenantID string `json:"tenant_id,optional"`
	// TenantIDCamel 租户编号（camelCase 兼容字段）
	TenantIDCamel string `json:"tenantId,optional"`
	// Account 用户名/账号
	Account string `json:"account"`
	// Password 登录密码
	Password string `json:"password"`
	// CaptchaKey 验证码缓存凭证（snake_case）
	CaptchaKey string `json:"captcha_key,optional"`
	// CaptchaKeyCamel 验证码缓存凭证（camelCase 兼容字段）
	CaptchaKeyCamel string `json:"captchaKey,optional"`
	// CaptchaCode 用户输入的验证码（snake_case）
	CaptchaCode string `json:"captcha_code,optional"`
	// CaptchaCodeCamel 用户输入的验证码（camelCase 兼容字段）
	CaptchaCodeCamel string `json:"captchaCode,optional"`
	// GrantType 授权模式（snake_case，默认为 "captcha"）
	GrantType string `json:"grant_type,optional"`
	// GrantTypeCamel 授权模式（camelCase 兼容字段）
	GrantTypeCamel string `json:"grantType,optional"`
}

// Normalize 归一化 TokenReq 中 snake_case 与 camelCase 兼容字段。
func (r *TokenReq) Normalize() {
	if r == nil {
		return
	}
	if r.TenantID == "" {
		r.TenantID = r.TenantIDCamel
	}
	if r.CaptchaKey == "" {
		r.CaptchaKey = r.CaptchaKeyCamel
	}
	if r.CaptchaCode == "" {
		r.CaptchaCode = r.CaptchaCodeCamel
	}
	if r.GrantType == "" {
		r.GrantType = r.GrantTypeCamel
	}
}

// RefreshTokenReq 刷新 Token 请求。
// 同时兼容 snake_case 与 camelCase 字段格式。
type RefreshTokenReq struct {
	// RefreshToken 长效刷新令牌（snake_case）
	RefreshToken string `json:"refresh_token,optional"`
	// RefreshTokenCamel 长效刷新令牌（camelCase 兼容字段）
	RefreshTokenCamel string `json:"refreshToken,optional"`
}

// Normalize 归一化 RefreshTokenReq 中 snake_case 与 camelCase 兼容字段。
func (r *RefreshTokenReq) Normalize() {
	if r == nil {
		return
	}
	if r.RefreshToken == "" {
		r.RefreshToken = r.RefreshTokenCamel
	}
}

// GetLoginChallengeReq 查询 Ory Hydra 登录挑战请求参数。
type GetLoginChallengeReq struct {
	// LoginChallenge 登录挑战标识（Query 参数 login_challenge）
	LoginChallenge string `form:"login_challenge,optional" json:"login_challenge,optional"`
	// LoginChallengeCamel 登录挑战标识（camelCase 兼容字段）
	LoginChallengeCamel string `form:"loginChallenge,optional" json:"loginChallenge,optional"`
}

// Normalize 归一化 GetLoginChallengeReq 中 snake_case 与 camelCase 兼容字段。
func (r *GetLoginChallengeReq) Normalize() {
	if r == nil {
		return
	}
	if r.LoginChallenge == "" {
		r.LoginChallenge = r.LoginChallengeCamel
	}
}

// GetLoginChallengeResp 查询 Ory Hydra 登录挑战响应结果。
type GetLoginChallengeResp struct {
	// Challenge 登录挑战标识
	Challenge string `json:"challenge"`
	// Skip 是否可跳过交互式登录
	Skip bool `json:"skip"`
	// Subject 已认证主体标识
	Subject string `json:"subject,omitempty"`
	// ClientID OAuth 2.1 客户端标识
	ClientID string `json:"client_id,omitempty"`
	// RedirectTo 当 skip 为 true 时直接接受登录后的重定向地址
	RedirectTo string `json:"redirect_to,omitempty"`
}

// AcceptLoginChallengeReq 提交账号密码完成 Ory Hydra 登录挑战请求参数。
type AcceptLoginChallengeReq struct {
	// LoginChallenge 登录挑战标识（snake_case）
	LoginChallenge string `form:"login_challenge,optional" json:"login_challenge,optional"`
	// LoginChallengeCamel 登录挑战标识（camelCase 兼容字段）
	LoginChallengeCamel string `form:"loginChallenge,optional" json:"loginChallenge,optional"`
	// TenantID 租户编号（snake_case）
	TenantID string `json:"tenant_id,optional"`
	// TenantIDCamel 租户编号（camelCase 兼容字段）
	TenantIDCamel string `json:"tenantId,optional"`
	// Account 登录账号
	Account string `json:"account"`
	// Password 登录密码
	Password string `json:"password"`
	// CaptchaKey 验证码缓存凭证（snake_case）
	CaptchaKey string `json:"captcha_key,optional"`
	// CaptchaKeyCamel 验证码缓存凭证（camelCase 兼容字段）
	CaptchaKeyCamel string `json:"captchaKey,optional"`
	// CaptchaCode 用户输入的验证码（snake_case）
	CaptchaCode string `json:"captcha_code,optional"`
	// CaptchaCodeCamel 用户输入的验证码（camelCase 兼容字段）
	CaptchaCodeCamel string `json:"captchaCode,optional"`
	// Remember 是否记住本次登录会话
	Remember bool `json:"remember,optional"`
	// RememberFor 记住登录会话时长（秒，snake_case）
	RememberFor int64 `json:"remember_for,optional"`
	// RememberForCamel 记住登录会话时长（秒，camelCase 兼容字段）
	RememberForCamel int64 `json:"rememberFor,optional"`
}

// Normalize 归一化 AcceptLoginChallengeReq 中 snake_case 与 camelCase 兼容字段。
func (r *AcceptLoginChallengeReq) Normalize() {
	if r == nil {
		return
	}
	if r.LoginChallenge == "" {
		r.LoginChallenge = r.LoginChallengeCamel
	}
	if r.TenantID == "" {
		r.TenantID = r.TenantIDCamel
	}
	if r.CaptchaKey == "" {
		r.CaptchaKey = r.CaptchaKeyCamel
	}
	if r.CaptchaCode == "" {
		r.CaptchaCode = r.CaptchaCodeCamel
	}
	if r.RememberFor == 0 {
		r.RememberFor = r.RememberForCamel
	}
}

// AcceptConsentChallengeReq 确认授权完成 Ory Hydra 授权挑战请求参数。
type AcceptConsentChallengeReq struct {
	// ConsentChallenge 授权挑战标识（snake_case）
	ConsentChallenge string `form:"consent_challenge,optional" json:"consent_challenge,optional"`
	// ConsentChallengeCamel 授权挑战标识（camelCase 兼容字段）
	ConsentChallengeCamel string `form:"consentChallenge,optional" json:"consentChallenge,optional"`
	// GrantScope 批准授予的权限范围列表（snake_case）
	GrantScope []string `json:"grant_scope,optional"`
	// GrantScopeCamel 批准授予的权限范围列表（camelCase 兼容字段）
	GrantScopeCamel []string `json:"grantScope,optional"`
	// Remember 是否记住本次授权决定
	Remember bool `json:"remember,optional"`
	// RememberFor 记住授权决定的有效时长（秒，snake_case）
	RememberFor int64 `json:"remember_for,optional"`
	// RememberForCamel 记住授权决定的有效时长（秒，camelCase 兼容字段）
	RememberForCamel int64 `json:"rememberFor,optional"`
}

// Normalize 归一化 AcceptConsentChallengeReq 中 snake_case 与 camelCase 兼容字段。
func (r *AcceptConsentChallengeReq) Normalize() {
	if r == nil {
		return
	}
	if r.ConsentChallenge == "" {
		r.ConsentChallenge = r.ConsentChallengeCamel
	}
	if len(r.GrantScope) == 0 && len(r.GrantScopeCamel) > 0 {
		r.GrantScope = r.GrantScopeCamel
	}
	if r.RememberFor == 0 {
		r.RememberFor = r.RememberForCamel
	}
}

// AcceptLogoutChallengeReq 完成 Ory Hydra 登出挑战或本地 JWT 主动注销请求参数。
type AcceptLogoutChallengeReq struct {
	// LogoutChallenge 登出挑战标识（snake_case）
	LogoutChallenge string `form:"logout_challenge,optional" json:"logout_challenge,optional"`
	// LogoutChallengeCamel 登出挑战标识（camelCase 兼容字段）
	LogoutChallengeCamel string `form:"logoutChallenge,optional" json:"logoutChallenge,optional"`
	// AccessToken 待主动吊销的访问令牌（snake_case）
	AccessToken string `form:"access_token,optional" json:"access_token,optional"`
	// AccessTokenCamel 待主动吊销的访问令牌（camelCase 兼容字段）
	AccessTokenCamel string `form:"accessToken,optional" json:"accessToken,optional"`
	// RefreshToken 待主动吊销的刷新令牌（snake_case）
	RefreshToken string `form:"refresh_token,optional" json:"refresh_token,optional"`
	// RefreshTokenCamel 待主动吊销的刷新令牌（camelCase 兼容字段）
	RefreshTokenCamel string `form:"refreshToken,optional" json:"refreshToken,optional"`
}

// Normalize 归一化 AcceptLogoutChallengeReq 中 snake_case 与 camelCase 兼容字段。
func (r *AcceptLogoutChallengeReq) Normalize() {
	if r == nil {
		return
	}
	if r.LogoutChallenge == "" {
		r.LogoutChallenge = r.LogoutChallengeCamel
	}
	if r.AccessToken == "" {
		r.AccessToken = r.AccessTokenCamel
	}
	if r.RefreshToken == "" {
		r.RefreshToken = r.RefreshTokenCamel
	}
}

// ChallengeRedirectResp Ory Hydra 挑战确认或主动登出后的统一响应结果。
type ChallengeRedirectResp struct {
	// RedirectTo 下一步重定向地址（Ory Hydra 挑战流返回）
	RedirectTo string `json:"redirect_to,omitempty"`
	// Revoked 本地 JWT 令牌是否已完成黑名单吊销
	Revoked bool `json:"revoked,omitempty"`
}
