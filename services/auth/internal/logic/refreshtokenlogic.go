// Package logic 实现认证微服务的具体业务逻辑。
package logic

import (
	"context"
	"strings"

	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/xerr"
	"github.com/go-blade/services/auth/internal/svc"
	"github.com/go-blade/services/auth/internal/types"
	"github.com/zeromicro/go-zero/core/logx"
)

// RefreshTokenLogic 刷新 Token 逻辑处理器。
type RefreshTokenLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// NewRefreshTokenLogic 构造 RefreshTokenLogic 实例。
func NewRefreshTokenLogic(ctx context.Context, svcCtx *svc.ServiceContext) *RefreshTokenLogic {
	return &RefreshTokenLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// RefreshToken 校验 RefreshToken 并换发新的双 Token。
func (l *RefreshTokenLogic) RefreshToken(req *types.RefreshTokenReq) (*jwt.TokenResult, error) {
	if req != nil {
		req.Normalize()
	}
	var refreshTokenStr string
	if req != nil {
		refreshTokenStr = strings.TrimSpace(req.RefreshToken)
	}
	if refreshTokenStr == "" {
		return nil, xerr.NewCodeError(xerr.RequestParamError, "刷新令牌不能为空")
	}

	tokenResult, err := l.svcCtx.JWTHandler.RefreshTokenCtx(l.ctx, refreshTokenStr)
	if err != nil {
		if err == jwt.ErrTokenExpired {
			return nil, xerr.NewCodeError(xerr.TokenExpired, "刷新令牌已过期，请重新登录")
		}
		return nil, xerr.NewCodeError(xerr.TokenInvalid, "刷新令牌无效或已被使用")
	}

	return tokenResult, nil
}
