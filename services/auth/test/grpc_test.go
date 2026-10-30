package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"image/png"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	authv1 "github.com/go-blade/api/proto/auth/v1"
	pkgredis "github.com/go-blade/pkg/redis"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
	"github.com/go-blade/services/auth/internal/config"
	"github.com/go-blade/services/auth/internal/model"
	"github.com/go-blade/services/auth/internal/server"
	"github.com/go-blade/services/auth/internal/svc"
)

func TestAuthGRPCWorkflow_Integration(t *testing.T) {
	// 1. 启动嵌入式 miniredis
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	_, err = pkgredis.InitRedis(pkgredis.Config{
		Host: mr.Addr(),
		Type: "node",
	})
	require.NoError(t, err)

	// 2. 初始化嵌入式 SQLite 模拟持久化用户表并挂载多租户插件
	db, err := gorm.Open(sqlite.Open("file:auth_grpc_test?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Use(tenant.NewTenantPlugin()))

	err = db.AutoMigrate(&model.User{}, &model.Role{})
	require.NoError(t, err)

	// 插入租户为 "000000" 的初始测试用户和租户 "100001" 的测试用户
	hashedPwd, err := model.HashPassword("123456")
	require.NoError(t, err)

	ctx0 := tenant.WithTenant(context.Background(), "000000")
	err = db.WithContext(ctx0).Create(&model.User{
		ID:       10001,
		TenantID: "000000",
		Account:  "grpc_admin",
		Password: hashedPwd,
		Name:     "gRPC Administrator",
		RealName: "管理员",
		DeptID:   1123598813738675200,
		RoleID:   "1,2",
		Status:   1,
	}).Error
	require.NoError(t, err)

	ctx1 := tenant.WithTenant(context.Background(), "100001")
	err = db.WithContext(ctx1).Create(&model.User{
		ID:       20001,
		TenantID: "100001",
		Account:  "tenant_user",
		Password: hashedPwd,
		Name:     "Tenant User",
		RealName: "租户用户",
		DeptID:   1123598813738675200,
		RoleID:   "2",
		Status:   1,
	}).Error
	require.NoError(t, err)

	// 3. 构建 ServiceContext
	c := config.Config{
		Auth: config.AuthConfig{
			AccessSecret:  "test-secret-key-32bytes-auth-grpc",
			AccessExpire:  3600,
			RefreshExpire: 7200,
			Issuer:        "blade",
		},
	}
	svcCtx := svc.NewServiceContext(c)
	svcCtx.DB = db

	// 4. 启动真实的 gRPC Server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	grpcServer := pkgrpc.NewServer()
	authServer := server.NewAuthServer(svcCtx)
	authv1.RegisterAuthServiceServer(grpcServer, authServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// 5. 使用 pkg/rpc.Dial 建立 gRPC 客户端连接（自动携带租户拦截器）
	clientConn, err := pkgrpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer clientConn.Close()

	authClient := authv1.NewAuthServiceClient(clientConn)

	// 6. 验证 gRPC GetCaptcha 返回真实 PNG 图片（尺寸 > 1x1）
	captchaGrpcResp, err := authClient.GetCaptcha(ctx0, &authv1.GetCaptchaRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, captchaGrpcResp.CaptchaKey)
	require.True(t, strings.HasPrefix(captchaGrpcResp.Image, "data:image/png;base64,"))
	rawPNG, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(captchaGrpcResp.Image, "data:image/png;base64,"))
	require.NoError(t, err)
	imgCfg, err := png.DecodeConfig(bytes.NewReader(rawPNG))
	require.NoError(t, err)
	assert.Greater(t, imgCfg.Width, 1)
	assert.Greater(t, imgCfg.Height, 1)

	// 7. 发起 gRPC Login 请求
	captchaRes, err := pkgredis.GenerateCaptchaWithLength(context.Background(), 4, 1*time.Minute)
	require.NoError(t, err)

	loginResp, err := authClient.Login(ctx0, &authv1.LoginRequest{
		TenantId:    "000000",
		Account:     "grpc_admin",
		Password:    "123456",
		CaptchaKey:  captchaRes.Key,
		CaptchaCode: captchaRes.Code,
	})
	require.NoError(t, err)
	require.NotNil(t, loginResp.TokenResult)
	assert.NotEmpty(t, loginResp.TokenResult.AccessToken)
	assert.NotEmpty(t, loginResp.TokenResult.RefreshToken)
	assert.Equal(t, "bearer", loginResp.TokenResult.TokenType)

	accessToken := loginResp.TokenResult.AccessToken
	refreshToken := loginResp.TokenResult.RefreshToken

	// 8. 发起 gRPC ValidateToken 校验 AccessToken
	validateResp, err := authClient.ValidateToken(ctx0, &authv1.ValidateTokenRequest{
		Token: accessToken,
	})
	require.NoError(t, err)
	require.NotNil(t, validateResp.BladeClaims)
	assert.Equal(t, "000000", validateResp.BladeClaims.TenantId)
	assert.Equal(t, "grpc_admin", validateResp.BladeClaims.Account)
	assert.Equal(t, "gRPC Administrator", validateResp.BladeClaims.UserName)
	assert.Equal(t, "access_token", validateResp.BladeClaims.Category)

	// 9. 验证 ValidateToken 跨租户拦截（包括传入 "000000" 检验 "100001" 的 Token 也绝不豁免）
	capTenant1, err := pkgredis.GenerateCaptchaWithLength(context.Background(), 4, 1*time.Minute)
	require.NoError(t, err)
	loginTenant1, err := authClient.Login(ctx1, &authv1.LoginRequest{
		TenantId:    "100001",
		Account:     "tenant_user",
		Password:    "123456",
		CaptchaKey:  capTenant1.Key,
		CaptchaCode: capTenant1.Code,
	})
	require.NoError(t, err)
	_, err = authClient.ValidateToken(ctx0, &authv1.ValidateTokenRequest{
		Token: loginTenant1.TokenResult.AccessToken,
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.PermissionDenied, st.Code())

	// 10. 等待 1 秒使 iat / nbf 变更，发起 gRPC RefreshToken 换发新 Token 并测试重放拦截
	time.Sleep(1050 * time.Millisecond)
	refreshResp, err := authClient.RefreshToken(context.Background(), &authv1.RefreshTokenRequest{
		RefreshToken: refreshToken,
	})
	require.NoError(t, err)
	require.NotNil(t, refreshResp.TokenResult)
	assert.NotEmpty(t, refreshResp.TokenResult.AccessToken)
	assert.NotEqual(t, accessToken, refreshResp.TokenResult.AccessToken)

	// 第二次重放同一个 RefreshToken 必须返回 TokenInvalid 业务错误
	_, err = authClient.RefreshToken(context.Background(), &authv1.RefreshTokenRequest{
		RefreshToken: refreshToken,
	})
	require.Error(t, err)
	stReplay, ok := status.FromError(err)
	require.True(t, ok)
	bizCode, _, hasBiz := xerr.ParseGrpcBizError(stReplay.Message())
	require.True(t, hasBiz)
	assert.Equal(t, xerr.TokenInvalid, bizCode)
}
