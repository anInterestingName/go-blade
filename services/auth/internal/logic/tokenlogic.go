// Package logic 实现认证微服务的具体业务逻辑。
package logic

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/go-blade/pkg/jwt"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
	"github.com/go-blade/services/auth/internal/model"
	"github.com/go-blade/services/auth/internal/svc"
	"github.com/go-blade/services/auth/internal/types"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// TokenLogic Token 业务逻辑处理器。
type TokenLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// NewTokenLogic 构造 TokenLogic 实例。
func NewTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *TokenLogic {
	return &TokenLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// Token 处理用户登录，校验验证码与账号密码，签发双 Token。
func (l *TokenLogic) Token(req *types.TokenReq) (*jwt.TokenResult, error) {
	if req == nil {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "请求参数不能为空")
	}
	req.Normalize()

	payload, _, err := l.VerifyCredentials(req)
	if err != nil {
		return nil, err
	}

	tokenResult, err := l.svcCtx.JWTHandler.GenerateTokenCtx(l.ctx, *payload)
	if err != nil {
		l.Errorf("GenerateToken failed: %v", err)
		return nil, xerr.NewErrMsg("生成令牌失败")
	}

	return tokenResult, nil
}

// VerifyCredentials 校验验证码、多租户账号状态与密码，返回组装好的 TokenPayload 与用户实体。
func (l *TokenLogic) VerifyCredentials(req *types.TokenReq) (*jwt.TokenPayload, *model.User, error) {
	if req == nil {
		return nil, nil, xerr.NewCodeError(xerr.RequestParamError, "请求参数不能为空")
	}
	req.Normalize()

	// 1. 验证码防重放校验
	captchaKey := strings.TrimSpace(req.CaptchaKey)
	captchaCode := strings.TrimSpace(req.CaptchaCode)
	if captchaKey == "" || captchaCode == "" {
		return nil, nil, xerr.NewCodeError(xerr.CaptchaError, "请输入验证码")
	}

	pass, err := pkgredis.VerifyCaptcha(l.ctx, captchaKey, captchaCode, true)
	if err != nil {
		l.Errorf("VerifyCaptcha failed: %v", err)
		return nil, nil, xerr.NewErrMsg("验证码服务异常")
	}
	if !pass {
		return nil, nil, xerr.NewCodeError(xerr.CaptchaError, xerr.MapErrMsg(xerr.CaptchaError))
	}

	// 2. 账号与密码基本非空校验
	account := strings.TrimSpace(req.Account)
	password := strings.TrimSpace(req.Password)
	if account == "" || password == "" {
		return nil, nil, xerr.NewCodeError(xerr.RequestParamError, "账号或密码不能为空")
	}

	// 3. 租户识别（优先请求体，次选 context，默认 "000000"）
	tenantID := strings.TrimSpace(req.TenantID)
	if tenantID == "" {
		tenantID = tenant.FromTenant(l.ctx)
	}

	// 4. 数据库真实用户凭据与状态校验（严禁任何假账号兜底）
	if l.svcCtx.DB == nil {
		l.Errorf("database is not initialized")
		return nil, nil, xerr.NewErrMsg("认证数据源未就绪")
	}

	dbCtx := tenant.WithTenant(l.ctx, tenantID)
	var user model.User
	err = l.svcCtx.DB.WithContext(dbCtx).Where("account = ? AND is_deleted = 0", account).First(&user).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, xerr.NewCodeError(xerr.UserNotFound, "用户不存在或已被禁用")
		}
		l.Errorf("Query user failed: %v", err)
		return nil, nil, xerr.NewErrMsg("查询用户信息异常")
	}

	// 检查账号状态（1: 正常, 2: 冻结）
	if user.Status != 1 {
		return nil, nil, xerr.NewCodeError(xerr.Forbidden, "当前账号已被冻结，请联系管理员")
	}

	// 比对 bcrypt 哈希密码
	if !user.CheckPassword(password) {
		return nil, nil, xerr.NewCodeError(xerr.PasswordError, "用户名或密码错误")
	}

	// 5. 解析角色别名（若存在 blade_role 表且查到角色记录则取真实 role_alias，否则安全回退）
	roleName := l.resolveRoleName(dbCtx, user)

	payload := &jwt.TokenPayload{
		TenantID: user.TenantID,
		UserID:   user.ID.Int64(),
		DeptID:   user.DeptID.Int64(),
		RoleID:   user.RoleID,
		RoleName: roleName,
		Account:  user.Account,
		UserName: user.Name,
		NickName: user.RealName,
	}

	return payload, &user, nil
}

func (l *TokenLogic) resolveRoleName(ctx context.Context, user model.User) string {
	roleIDs := parseRoleIDs(user.RoleID)
	if len(roleIDs) > 0 && l.svcCtx.DB != nil && l.svcCtx.DB.Migrator().HasTable(&model.Role{}) {
		var roles []model.Role
		if err := l.svcCtx.DB.WithContext(ctx).Where("id IN ? AND is_deleted = 0", roleIDs).Order("sort asc, id asc").Find(&roles).Error; err == nil && len(roles) > 0 {
			aliases := make([]string, 0, len(roles))
			for _, r := range roles {
				if alias := strings.TrimSpace(r.RoleAlias); alias != "" {
					aliases = append(aliases, alias)
				}
			}
			if len(aliases) > 0 {
				return strings.Join(aliases, ",")
			}
		}
	}
	if user.Account == "admin" {
		return "administrator"
	}
	return "user"
}

func parseRoleIDs(raw string) []int64 {
	parts := strings.Split(raw, ",")
	ids := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if id, err := strconv.ParseInt(p, 10, 64); err == nil && id > 0 {
			ids = append(ids, id)
		}
	}
	return ids
}
