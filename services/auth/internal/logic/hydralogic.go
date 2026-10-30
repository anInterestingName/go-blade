// Package logic 实现认证微服务的具体业务逻辑。
package logic

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
	"github.com/go-blade/services/auth/internal/hydra"
	"github.com/go-blade/services/auth/internal/model"
	"github.com/go-blade/services/auth/internal/svc"
	"github.com/go-blade/services/auth/internal/types"
	"github.com/zeromicro/go-zero/core/logx"
)

// HydraLogic Ory Hydra OAuth 2.1 Challenge 协同业务逻辑处理器。
type HydraLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// NewHydraLogic 构造 HydraLogic 实例。
func NewHydraLogic(ctx context.Context, svcCtx *svc.ServiceContext) *HydraLogic {
	return &HydraLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *HydraLogic) ensureHydraClient() (hydra.AdminClient, error) {
	if l.svcCtx == nil || l.svcCtx.HydraClient == nil {
		l.Errorf("HydraClient is not initialized")
		return nil, xerr.NewErrMsg("Hydra 认证基础设施未配置或未就绪")
	}
	return l.svcCtx.HydraClient, nil
}

// GetLoginChallenge 查询 Hydra Login Request；若 skip 为 true，则直接调用 AcceptLoginRequest 并返回 redirect_to。
func (l *HydraLogic) GetLoginChallenge(req *types.GetLoginChallengeReq) (*types.GetLoginChallengeResp, error) {
	if req == nil {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "请求参数不能为空")
	}
	req.Normalize()
	challenge := strings.TrimSpace(req.LoginChallenge)
	if challenge == "" {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "login_challenge 不能为空")
	}

	cli, err := l.ensureHydraClient()
	if err != nil {
		return nil, err
	}

	loginReq, err := cli.GetLoginRequest(l.ctx, challenge)
	if err != nil {
		l.Errorf("GetLoginRequest failed: %v", err)
		return nil, xerr.NewErrMsg("查询 Hydra 登录挑战失败")
	}

	resp := &types.GetLoginChallengeResp{
		Challenge: loginReq.Challenge,
		Skip:      loginReq.Skip,
		Subject:   loginReq.Subject,
		ClientID:  loginReq.Client.ClientID,
	}
	if resp.Challenge == "" {
		resp.Challenge = challenge
	}

	if loginReq.Skip {
		acceptBody := hydra.AcceptLoginBody{
			Subject: loginReq.Subject,
			Context: loginReq.Context,
		}
		if len(acceptBody.Context) == 0 && loginReq.Subject != "" {
			if claimsCtx := l.loadClaimsMapBySubject(loginReq.Subject); len(claimsCtx) > 0 {
				acceptBody.Context = claimsCtx
			}
		}
		redirectResp, err := cli.AcceptLoginRequest(l.ctx, challenge, acceptBody)
		if err != nil {
			l.Errorf("AcceptLoginRequest on skip=true failed: %v", err)
			return nil, xerr.NewErrMsg("接受 Hydra 免密登录挑战失败")
		}
		resp.RedirectTo = redirectResp.RedirectTo
	}

	return resp, nil
}

// AcceptLoginChallenge 校验验证码、多租户用户状态与密码，通过后将 BladeClaims 写入 Hydra AcceptLogin 的 context 并返回 redirect_to。
func (l *HydraLogic) AcceptLoginChallenge(req *types.AcceptLoginChallengeReq) (*types.ChallengeRedirectResp, error) {
	if req == nil {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "请求参数不能为空")
	}
	req.Normalize()
	challenge := strings.TrimSpace(req.LoginChallenge)
	if challenge == "" {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "login_challenge 不能为空")
	}

	cli, err := l.ensureHydraClient()
	if err != nil {
		return nil, err
	}

	// 复用 TokenLogic 的验证码、多租户账号与密码校验逻辑
	tokenLogic := NewTokenLogic(l.ctx, l.svcCtx)
	payload, user, err := tokenLogic.VerifyCredentials(&types.TokenReq{
		TenantID:    req.TenantID,
		Account:     req.Account,
		Password:    req.Password,
		CaptchaKey:  req.CaptchaKey,
		CaptchaCode: req.CaptchaCode,
	})
	if err != nil {
		return nil, err
	}

	subject := fmt.Sprintf("%s:%d", payload.TenantID, user.ID.Int64())
	claimsContext := buildClaimsMap(payload)

	redirectResp, err := cli.AcceptLoginRequest(l.ctx, challenge, hydra.AcceptLoginBody{
		Subject:     subject,
		Remember:    req.Remember,
		RememberFor: req.RememberFor,
		Context:     claimsContext,
	})
	if err != nil {
		l.Errorf("AcceptLoginRequest failed: %v", err)
		return nil, xerr.NewErrMsg("确认 Hydra 登录挑战失败")
	}

	return &types.ChallengeRedirectResp{
		RedirectTo: redirectResp.RedirectTo,
	}, nil
}

// AcceptConsentChallenge 查询 Hydra Consent Request 获取 subject 与 context（或回查数据库），构造完整 BladeClaims 注入 session.access_token 与 session.id_token。
func (l *HydraLogic) AcceptConsentChallenge(req *types.AcceptConsentChallengeReq) (*types.ChallengeRedirectResp, error) {
	if req == nil {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "请求参数不能为空")
	}
	req.Normalize()
	challenge := strings.TrimSpace(req.ConsentChallenge)
	if challenge == "" {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "consent_challenge 不能为空")
	}

	cli, err := l.ensureHydraClient()
	if err != nil {
		return nil, err
	}

	consentReq, err := cli.GetConsentRequest(l.ctx, challenge)
	if err != nil {
		l.Errorf("GetConsentRequest failed: %v", err)
		return nil, xerr.NewErrMsg("查询 Hydra 授权挑战失败")
	}

	claimsMap := l.resolveConsentClaims(consentReq)

	grantScope := req.GrantScope
	if len(grantScope) == 0 {
		grantScope = consentReq.RequestedScope
	}

	accessTokenClaims := make(map[string]any, len(claimsMap)+1)
	idTokenClaims := make(map[string]any, len(claimsMap))
	for k, v := range claimsMap {
		accessTokenClaims[k] = v
		idTokenClaims[k] = v
	}
	accessTokenClaims["category"] = jwt.TokenTypeAccess

	redirectResp, err := cli.AcceptConsentRequest(l.ctx, challenge, hydra.AcceptConsentBody{
		GrantScope:               grantScope,
		GrantAccessTokenAudience: consentReq.RequestedAccessTokenAudience,
		Remember:                 req.Remember,
		RememberFor:              req.RememberFor,
		Session: &hydra.ConsentSession{
			AccessToken: accessTokenClaims,
			IDToken:     idTokenClaims,
		},
	})
	if err != nil {
		l.Errorf("AcceptConsentRequest failed: %v", err)
		return nil, xerr.NewErrMsg("确认 Hydra 授权挑战失败")
	}

	return &types.ChallengeRedirectResp{
		RedirectTo: redirectResp.RedirectTo,
	}, nil
}

// AcceptLogoutChallenge 接受 Hydra 登出挑战或执行本地 JWT 令牌黑名单吊销。
func (l *HydraLogic) AcceptLogoutChallenge(req *types.AcceptLogoutChallengeReq) (*types.ChallengeRedirectResp, error) {
	if req == nil {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "请求参数不能为空")
	}
	req.Normalize()
	challenge := strings.TrimSpace(req.LogoutChallenge)
	accessToken := strings.TrimSpace(req.AccessToken)
	refreshToken := strings.TrimSpace(req.RefreshToken)

	revoked := false
	if accessToken != "" {
		var claims *jwt.BladeClaims
		if l.svcCtx != nil && l.svcCtx.JWTHandler != nil {
			claims, _ = l.svcCtx.JWTHandler.ParseTokenCtx(l.ctx, accessToken)
		}
		if err := jwt.RevokeTokenWithClaims(l.ctx, accessToken, claims); err == nil {
			revoked = true
		}
	}
	if refreshToken != "" {
		if l.svcCtx != nil && l.svcCtx.JWTHandler != nil {
			_ = l.svcCtx.JWTHandler.RevokeRefreshToken(l.ctx, refreshToken)
		}
		if err := jwt.RevokeToken(l.ctx, refreshToken, 0); err == nil {
			revoked = true
		}
	}

	if challenge == "" {
		if revoked {
			return &types.ChallengeRedirectResp{
				Revoked: true,
			}, nil
		}
		return nil, xerr.NewCodeError(xerr.RequestParamError, "logout_challenge 或认证令牌不能为空")
	}

	cli, err := l.ensureHydraClient()
	if err != nil {
		return nil, err
	}

	redirectResp, err := cli.AcceptLogoutRequest(l.ctx, challenge)
	if err != nil {
		l.Errorf("AcceptLogoutRequest failed: %v", err)
		return nil, xerr.NewErrMsg("确认 Hydra 登出挑战失败")
	}

	return &types.ChallengeRedirectResp{
		RedirectTo: redirectResp.RedirectTo,
		Revoked:    revoked,
	}, nil
}

func buildClaimsMap(payload *jwt.TokenPayload) map[string]any {
	return map[string]any{
		"tenant_id": payload.TenantID,
		"user_id":   strconv.FormatInt(payload.UserID, 10),
		"dept_id":   strconv.FormatInt(payload.DeptID, 10),
		"role_id":   payload.RoleID,
		"role_name": payload.RoleName,
		"account":   payload.Account,
		"user_name": payload.UserName,
		"nick_name": payload.NickName,
	}
}

func (l *HydraLogic) resolveConsentClaims(consentReq *hydra.ConsentRequest) map[string]any {
	if consentReq == nil {
		return map[string]any{}
	}
	// 若 Context 中已包含完整的 BladeClaims 核心字段，则优先提取并标准化；若缺失则根据 Subject 回查数据库补齐
	if len(consentReq.Context) > 0 {
		if hasCoreClaims(consentReq.Context) {
			return normalizeClaimsMap(consentReq.Context)
		}
	}
	if consentReq.Subject != "" {
		if dbClaims := l.loadClaimsMapBySubject(consentReq.Subject); len(dbClaims) > 0 {
			// 合并 Context 中可能附带的自定义扩展字段
			for k, v := range consentReq.Context {
				if _, exists := dbClaims[k]; !exists {
					dbClaims[k] = v
				}
			}
			return dbClaims
		}
	}
	if len(consentReq.Context) > 0 {
		return normalizeClaimsMap(consentReq.Context)
	}
	return map[string]any{}
}

func hasCoreClaims(ctxMap map[string]any) bool {
	_, hasTenant := ctxMap["tenant_id"]
	_, hasUser := ctxMap["user_id"]
	_, hasAccount := ctxMap["account"]
	return hasTenant && hasUser && hasAccount
}

func normalizeClaimsMap(raw map[string]any) map[string]any {
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		out[k] = v
	}
	if uid, ok := out["user_id"]; ok {
		out["user_id"] = toStringID(uid)
	}
	if did, ok := out["dept_id"]; ok {
		out["dept_id"] = toStringID(did)
	}
	return out
}

func toStringID(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatInt(int64(val), 10)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func (l *HydraLogic) loadClaimsMapBySubject(subject string) map[string]any {
	if l.svcCtx == nil || l.svcCtx.DB == nil {
		return nil
	}
	tenantID, userID, ok := parseSubject(subject, l.ctx)
	if !ok {
		return nil
	}

	dbCtx := tenant.WithTenant(l.ctx, tenantID)
	var user model.User
	if err := l.svcCtx.DB.WithContext(dbCtx).Where("id = ? AND is_deleted = 0", userID).First(&user).Error; err != nil {
		return nil
	}
	tokenLogic := NewTokenLogic(l.ctx, l.svcCtx)
	roleName := tokenLogic.resolveRoleName(dbCtx, user)

	return buildClaimsMap(&jwt.TokenPayload{
		TenantID: user.TenantID,
		UserID:   user.ID.Int64(),
		DeptID:   user.DeptID.Int64(),
		RoleID:   user.RoleID,
		RoleName: roleName,
		Account:  user.Account,
		UserName: user.Name,
		NickName: user.RealName,
	})
}

func parseSubject(subject string, ctx context.Context) (string, int64, bool) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return "", 0, false
	}
	if idx := strings.IndexByte(subject, ':'); idx > 0 {
		tID := strings.TrimSpace(subject[:idx])
		uStr := strings.TrimSpace(subject[idx+1:])
		if uid, err := strconv.ParseInt(uStr, 10, 64); err == nil && uid > 0 {
			if tID == "" {
				tID = tenant.FromTenant(ctx)
			}
			return tID, uid, true
		}
	}
	if uid, err := strconv.ParseInt(subject, 10, 64); err == nil && uid > 0 {
		return tenant.FromTenant(ctx), uid, true
	}
	return "", 0, false
}
