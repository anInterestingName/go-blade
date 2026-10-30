// Package server 实现认证微服务的 gRPC Server 接口契约。
package server

import (
	"context"
	"strings"

	authv1 "github.com/go-blade/api/proto/auth/v1"
	"github.com/go-blade/pkg/jwt"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/xerr"
	"github.com/go-blade/services/auth/internal/logic"
	"github.com/go-blade/services/auth/internal/svc"
	"github.com/go-blade/services/auth/internal/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthServer 实现 authv1.AuthServiceServer 接口。
type AuthServer struct {
	authv1.UnimplementedAuthServiceServer
	svcCtx *svc.ServiceContext
}

// NewAuthServer 构造 AuthServer 实例。
func NewAuthServer(svcCtx *svc.ServiceContext) *AuthServer {
	return &AuthServer{
		svcCtx: svcCtx,
	}
}

// GetCaptcha 处理 gRPC 获取图形验证码请求。
func (s *AuthServer) GetCaptcha(ctx context.Context, req *authv1.GetCaptchaRequest) (*authv1.GetCaptchaResponse, error) {
	res, err := logic.NewCaptchaLogic(ctx, s.svcCtx).Captcha()
	if err != nil {
		return nil, xerr.ToGrpcError(err)
	}
	return &authv1.GetCaptchaResponse{
		CaptchaKey: res.CaptchaKey,
		Image:      res.Image,
	}, nil
}

// Login 处理 gRPC 登录请求。
func (s *AuthServer) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}

	tokenLogic := logic.NewTokenLogic(ctx, s.svcCtx)
	tokenRes, err := tokenLogic.Token(&types.TokenReq{
		TenantID:    req.TenantId,
		Account:     req.Account,
		Password:    req.Password,
		CaptchaKey:  req.CaptchaKey,
		CaptchaCode: req.CaptchaCode,
	})
	if err != nil {
		return nil, xerr.ToGrpcError(err)
	}

	return &authv1.LoginResponse{
		TokenResult: &authv1.TokenResult{
			AccessToken:  tokenRes.AccessToken,
			TokenType:    tokenRes.TokenType,
			RefreshToken: tokenRes.RefreshToken,
			ExpiresIn:    tokenRes.ExpiresIn,
			License:      tokenRes.License,
		},
	}, nil
}

// RefreshToken 处理 gRPC 刷新令牌请求。
func (s *AuthServer) RefreshToken(ctx context.Context, req *authv1.RefreshTokenRequest) (*authv1.RefreshTokenResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}

	refreshLogic := logic.NewRefreshTokenLogic(ctx, s.svcCtx)
	tokenRes, err := refreshLogic.RefreshToken(&types.RefreshTokenReq{
		RefreshToken: req.RefreshToken,
	})
	if err != nil {
		return nil, xerr.ToGrpcError(err)
	}

	return &authv1.RefreshTokenResponse{
		TokenResult: &authv1.TokenResult{
			AccessToken:  tokenRes.AccessToken,
			TokenType:    tokenRes.TokenType,
			RefreshToken: tokenRes.RefreshToken,
			ExpiresIn:    tokenRes.ExpiresIn,
			License:      tokenRes.License,
		},
	}, nil
}

// ValidateToken 处理令牌合法性校验与用户信息 Claims 解析。
func (s *AuthServer) ValidateToken(ctx context.Context, req *authv1.ValidateTokenRequest) (*authv1.ValidateTokenResponse, error) {
	if req == nil || req.Token == "" {
		return nil, status.Errorf(codes.InvalidArgument, "token cannot be empty")
	}

	claims, err := s.svcCtx.JWTHandler.ParseTokenCtx(ctx, req.Token)
	if err != nil {
		if err == jwt.ErrTokenExpired {
			return nil, status.Errorf(codes.Unauthenticated, "token has expired")
		}
		if err == jwt.ErrTokenRevoked {
			return nil, status.Errorf(codes.Unauthenticated, "token has been revoked")
		}
		return nil, status.Errorf(codes.Unauthenticated, "invalid token: %v", err)
	}

	// 租户匹配安全检查（若 incoming metadata 中显式传入了 blade-tenant-id 且与令牌租户不一致，坚决拒绝跨租户访问）
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(pkgrpc.MetadataTenantID); len(vals) > 0 {
			if incomingTenant := strings.TrimSpace(vals[0]); incomingTenant != "" && incomingTenant != claims.TenantID {
				return nil, status.Errorf(codes.PermissionDenied, "租户信息不匹配，拒绝跨租户访问")
			}
		}
	}

	return &authv1.ValidateTokenResponse{
		BladeClaims: &authv1.BladeClaims{
			TenantId: claims.TenantID,
			UserId:   claims.UserID,
			DeptId:   claims.DeptID,
			RoleId:   claims.RoleID,
			RoleName: claims.RoleName,
			Account:  claims.Account,
			UserName: claims.UserName,
			NickName: claims.NickName,
			Category: claims.Category,
		},
	}, nil
}

// GetLoginChallenge 处理 gRPC 查询 Ory Hydra 登录挑战请求。
func (s *AuthServer) GetLoginChallenge(ctx context.Context, req *authv1.GetLoginChallengeRequest) (*authv1.GetLoginChallengeResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}
	res, err := logic.NewHydraLogic(ctx, s.svcCtx).GetLoginChallenge(&types.GetLoginChallengeReq{
		LoginChallenge: req.LoginChallenge,
	})
	if err != nil {
		return nil, xerr.ToGrpcError(err)
	}
	return &authv1.GetLoginChallengeResponse{
		Challenge:  res.Challenge,
		Skip:       res.Skip,
		Subject:    res.Subject,
		ClientId:   res.ClientID,
		RedirectTo: res.RedirectTo,
	}, nil
}

// AcceptLoginChallenge 处理 gRPC 校验凭据并接受 Ory Hydra 登录挑战请求。
func (s *AuthServer) AcceptLoginChallenge(ctx context.Context, req *authv1.AcceptLoginChallengeRequest) (*authv1.AcceptLoginChallengeResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}
	res, err := logic.NewHydraLogic(ctx, s.svcCtx).AcceptLoginChallenge(&types.AcceptLoginChallengeReq{
		LoginChallenge: req.LoginChallenge,
		TenantID:       req.TenantId,
		Account:        req.Account,
		Password:       req.Password,
		CaptchaKey:     req.CaptchaKey,
		CaptchaCode:    req.CaptchaCode,
		Remember:       req.Remember,
		RememberFor:    req.RememberFor,
	})
	if err != nil {
		return nil, xerr.ToGrpcError(err)
	}
	return &authv1.AcceptLoginChallengeResponse{
		RedirectTo: res.RedirectTo,
	}, nil
}

// AcceptConsentChallenge 处理 gRPC 确认授权并注入 BladeClaims 的 Ory Hydra 授权挑战请求。
func (s *AuthServer) AcceptConsentChallenge(ctx context.Context, req *authv1.AcceptConsentChallengeRequest) (*authv1.AcceptConsentChallengeResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}
	res, err := logic.NewHydraLogic(ctx, s.svcCtx).AcceptConsentChallenge(&types.AcceptConsentChallengeReq{
		ConsentChallenge: req.ConsentChallenge,
		GrantScope:       req.GrantScope,
		Remember:         req.Remember,
		RememberFor:      req.RememberFor,
	})
	if err != nil {
		return nil, xerr.ToGrpcError(err)
	}
	return &authv1.AcceptConsentChallengeResponse{
		RedirectTo: res.RedirectTo,
	}, nil
}

// AcceptLogoutChallenge 处理 gRPC 接受 Ory Hydra 登出挑战请求。
func (s *AuthServer) AcceptLogoutChallenge(ctx context.Context, req *authv1.AcceptLogoutChallengeRequest) (*authv1.AcceptLogoutChallengeResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "request cannot be nil")
	}
	res, err := logic.NewHydraLogic(ctx, s.svcCtx).AcceptLogoutChallenge(&types.AcceptLogoutChallengeReq{
		LogoutChallenge: req.LogoutChallenge,
	})
	if err != nil {
		return nil, xerr.ToGrpcError(err)
	}
	return &authv1.AcceptLogoutChallengeResponse{
		RedirectTo: res.RedirectTo,
	}, nil
}
