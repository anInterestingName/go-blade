// Package logic 实现认证微服务的具体业务逻辑。
package logic

import (
	"context"
	"time"

	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/xerr"
	"github.com/go-blade/services/auth/internal/svc"
	"github.com/go-blade/services/auth/internal/types"
	"github.com/zeromicro/go-zero/core/logx"
)

// CaptchaLogic 验证码业务逻辑处理器。
type CaptchaLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

// NewCaptchaLogic 构造 CaptchaLogic 实例。
func NewCaptchaLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CaptchaLogic {
	return &CaptchaLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

// Captcha 生成验证码并存入缓存。
// 注意：严禁在生产响应中返回明文 Code，此处返回随机 Key 与真实 PNG Base64 图像数据，保障安全防刷。
func (l *CaptchaLogic) Captcha() (*types.CaptchaResp, error) {
	res, err := pkgredis.GenerateCaptcha(l.ctx, 5*time.Minute)
	if err != nil {
		l.Errorf("GenerateCaptcha err: %v", err)
		return nil, xerr.NewErrMsg("生成验证码失败")
	}

	return &types.CaptchaResp{
		CaptchaKey: res.Key,
		Image:      res.Image,
	}, nil
}
