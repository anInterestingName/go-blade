// Package test 提供网关服务的端到端集成与安全回归测试。
package test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	authv1 "github.com/go-blade/api/proto/auth/v1"
	deskv1 "github.com/go-blade/api/proto/desk/v1"
	systemv1 "github.com/go-blade/api/proto/system/v1"
	"github.com/go-blade/pkg/auditlog"
	"github.com/go-blade/pkg/event"
	pkgjwt "github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/mq"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/trace"
	"github.com/go-blade/services/gateway/internal/config"
	"github.com/go-blade/services/gateway/internal/handler"
	"github.com/go-blade/services/gateway/internal/svc"
	"github.com/zeromicro/go-zero/rest"
)

// mockAuthServer 模拟下游 Auth gRPC Server
type mockAuthServer struct {
	authv1.UnimplementedAuthServiceServer
	mu          sync.Mutex
	lastLogin   *authv1.LoginRequest
	lastRefresh *authv1.RefreshTokenRequest
}

func (m *mockAuthServer) GetCaptcha(ctx context.Context, req *authv1.GetCaptchaRequest) (*authv1.GetCaptchaResponse, error) {
	return &authv1.GetCaptchaResponse{
		CaptchaKey: "mock-captcha-key-001",
		Image:      "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==",
	}, nil
}

func (m *mockAuthServer) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	m.mu.Lock()
	m.lastLogin = req
	m.mu.Unlock()

	return &authv1.LoginResponse{
		TokenResult: &authv1.TokenResult{
			AccessToken:  "mock-access-token-12345",
			TokenType:    "bearer",
			RefreshToken: "mock-refresh-token-67890",
			ExpiresIn:    7200,
			License:      "powered by go-blade",
		},
	}, nil
}

func (m *mockAuthServer) RefreshToken(ctx context.Context, req *authv1.RefreshTokenRequest) (*authv1.RefreshTokenResponse, error) {
	m.mu.Lock()
	m.lastRefresh = req
	m.mu.Unlock()

	return &authv1.RefreshTokenResponse{
		TokenResult: &authv1.TokenResult{
			AccessToken:  "mock-refreshed-access-token",
			TokenType:    "bearer",
			RefreshToken: "mock-refreshed-refresh-token",
			ExpiresIn:    7200,
			License:      "powered by go-blade",
		},
	}, nil
}

func (m *mockAuthServer) GetLoginChallenge(ctx context.Context, req *authv1.GetLoginChallengeRequest) (*authv1.GetLoginChallengeResponse, error) {
	return &authv1.GetLoginChallengeResponse{
		Challenge:  req.GetLoginChallenge(),
		Skip:       false,
		ClientId:   "blade-web",
		RedirectTo: "",
	}, nil
}

func (m *mockAuthServer) AcceptLoginChallenge(ctx context.Context, req *authv1.AcceptLoginChallengeRequest) (*authv1.AcceptLoginChallengeResponse, error) {
	return &authv1.AcceptLoginChallengeResponse{
		RedirectTo: "http://localhost:4444/oauth2/auth?login_verifier=ok_" + req.GetLoginChallenge(),
	}, nil
}

func (m *mockAuthServer) AcceptConsentChallenge(ctx context.Context, req *authv1.AcceptConsentChallengeRequest) (*authv1.AcceptConsentChallengeResponse, error) {
	return &authv1.AcceptConsentChallengeResponse{
		RedirectTo: "http://localhost:8000/callback?code=auth_code_" + req.GetConsentChallenge(),
	}, nil
}

func (m *mockAuthServer) AcceptLogoutChallenge(ctx context.Context, req *authv1.AcceptLogoutChallengeRequest) (*authv1.AcceptLogoutChallengeResponse, error) {
	return &authv1.AcceptLogoutChallengeResponse{
		RedirectTo: "http://localhost:8000/login",
	}, nil
}

// mockDeskServer 模拟下游 Desk 协同工作台 gRPC Server
type mockDeskServer struct {
	deskv1.UnimplementedDeskServiceServer
}

func (m *mockDeskServer) ListNotices(ctx context.Context, req *deskv1.ListNoticesRequest) (*deskv1.ListNoticesResponse, error) {
	claims := pkgjwt.FromClaims(ctx)
	var createUser, createDept int64
	if claims != nil {
		createUser = claims.UserID
		createDept = claims.DeptID
	}
	return &deskv1.ListNoticesResponse{
		Records: []*deskv1.NoticeInfo{
			{
				Id:          9007199254742001,
				TenantId:    "100001",
				Title:       "系统升级公告",
				Category:    1,
				ReleaseTime: "2026-10-26 10:00:00",
				Content:     "内容详情",
				Status:      1,
				CreateUser:  createUser,
				CreateDept:  createDept,
				CreateTime:  "2026-10-26 09:00:00",
			},
		},
		Total:   1,
		Size:    req.GetSize(),
		Current: req.GetCurrent(),
	}, nil
}

func (m *mockDeskServer) SaveNotice(ctx context.Context, req *deskv1.SaveNoticeRequest) (*deskv1.SaveNoticeResponse, error) {
	return &deskv1.SaveNoticeResponse{
		Id: 9007199254742001,
	}, nil
}

// mockSystemServer 模拟下游 System gRPC Server
type mockSystemServer struct {
	systemv1.UnimplementedSystemServiceServer
	mu               sync.Mutex
	lastIncomingMD   metadata.MD
	lastClaims       *pkgjwt.BladeClaims
	returnInternal   bool
	lastUserID       int64
	lastRouteRoleIDs string
	customButtons    []string
	lastSaveUser     *systemv1.SaveUserRequest
}

func (m *mockSystemServer) GetTenant(ctx context.Context, req *systemv1.GetTenantRequest) (*systemv1.GetTenantResponse, error) {
	m.mu.Lock()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		m.lastIncomingMD = md.Copy()
	}
	m.lastClaims = pkgjwt.FromClaims(ctx)
	returnInternal := m.returnInternal
	m.mu.Unlock()

	if returnInternal {
		return nil, status.Error(codes.Internal, "sql: database connection refused")
	}

	return &systemv1.GetTenantResponse{
		Tenant: &systemv1.TenantInfo{
			TenantId:      req.GetTenantId(),
			TenantName:    "测试租户",
			Linkman:       "张三",
			ContactNumber: "13800000000",
			Status:        1,
		},
	}, nil
}

func (m *mockSystemServer) GetUser(ctx context.Context, req *systemv1.GetUserRequest) (*systemv1.GetUserResponse, error) {
	m.mu.Lock()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		m.lastIncomingMD = md.Copy()
	}
	m.lastClaims = pkgjwt.FromClaims(ctx)
	m.lastUserID = req.GetUserId()
	returnInternal := m.returnInternal
	m.mu.Unlock()

	if returnInternal {
		return nil, status.Error(codes.Internal, "sql: database connection refused")
	}

	return &systemv1.GetUserResponse{
		User: &systemv1.UserInfo{
			Id:       9007199254740993,
			TenantId: "100001",
			Account:  "tenant_user",
			Name:     "租户测试员",
			RealName: "李四",
			Email:    "lisi@example.com",
			Phone:    "13900000000",
			DeptId:   9007199254740995,
			RoleId:   "9007199254740999",
			Status:   1,
		},
		Roles:       []string{"user"},
		Permissions: []string{"system:user:view"},
	}, nil
}

func (m *mockSystemServer) GetUserRoutes(ctx context.Context, req *systemv1.GetUserRoutesRequest) (*systemv1.GetUserRoutesResponse, error) {
	m.mu.Lock()
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		m.lastIncomingMD = md.Copy()
	}
	m.lastUserID = req.GetUserId()
	m.lastRouteRoleIDs = req.GetRoleIds()
	returnInternal := m.returnInternal
	buttons := m.customButtons
	m.mu.Unlock()

	if returnInternal {
		return nil, status.Error(codes.Internal, "sql: database connection refused")
	}

	if buttons == nil {
		buttons = []string{"user:add"}
	}

	return &systemv1.GetUserRoutesResponse{
		Routes: []*systemv1.MenuInfo{
			{
				Id:        9007199254741001,
				ParentId:  9007199254740997,
				Code:      "system",
				Name:      "系统管理",
				Path:      "/system",
				Component: "Layout",
				Icon:      "setting",
				Sort:      1,
				Category:  1,
			},
		},
		Buttons: buttons,
	}, nil
}

func (m *mockSystemServer) SaveUser(ctx context.Context, req *systemv1.SaveUserRequest) (*systemv1.SaveUserResponse, error) {
	m.mu.Lock()
	m.lastSaveUser = req
	m.mu.Unlock()
	return &systemv1.SaveUserResponse{
		Id: 9007199254743001,
	}, nil
}

func (m *mockSystemServer) SaveDept(ctx context.Context, req *systemv1.SaveDeptRequest) (*systemv1.SaveDeptResponse, error) {
	return &systemv1.SaveDeptResponse{
		Id: 9007199254743002,
	}, nil
}

func (m *mockSystemServer) SaveRole(ctx context.Context, req *systemv1.SaveRoleRequest) (*systemv1.SaveRoleResponse, error) {
	return &systemv1.SaveRoleResponse{
		Id: 9007199254743003,
	}, nil
}

func (m *mockSystemServer) GrantRoleMenus(ctx context.Context, req *systemv1.GrantRoleMenusRequest) (*systemv1.GrantRoleMenusResponse, error) {
	return &systemv1.GrantRoleMenusResponse{
		Success: true,
	}, nil
}

func (m *mockSystemServer) ListUsers(ctx context.Context, req *systemv1.ListUsersRequest) (*systemv1.ListUsersResponse, error) {
	return &systemv1.ListUsersResponse{
		Records: []*systemv1.UserInfo{
			{
				Id:       9007199254740993,
				TenantId: "100001",
				Account:  "tenant_user",
				Name:     "租户测试员",
				DeptId:   9007199254740995,
				RoleId:   "9007199254740999",
				Status:   1,
			},
		},
		Total:   1,
		Size:    req.GetSize(),
		Current: req.GetCurrent(),
	}, nil
}

func (m *mockSystemServer) ListAuditLogs(ctx context.Context, req *systemv1.ListAuditLogsRequest) (*systemv1.ListAuditLogsResponse, error) {
	return &systemv1.ListAuditLogsResponse{
		Records: []*systemv1.AuditLogInfo{
			{
				Id:          9007199254749901,
				TenantId:    "100001",
				ServiceName: "service-gateway",
				UserId:      9007199254740991,
				Account:     "audit_admin",
				Method:      "POST",
				Path:        "/blade-system/user/submit",
				RequestBody: `{"account":"audited_new_user","password":"******"}`,
				StatusCode:  200,
				DurationMs:  12,
				ClientIp:    "127.0.0.1",
				TraceId:     "mock-trace-id-001",
				CreatedAt:   "2026-10-27 22:00:00",
			},
		},
		Total:   1,
		Size:    req.GetSize(),
		Current: req.GetCurrent(),
	}, nil
}

func TestGateway_E2E_Workflow(t *testing.T) {
	tpHolder, err := trace.InitTracer(trace.Config{ServiceName: "service-gateway-test"})
	require.NoError(t, err)
	defer func() { _ = tpHolder.Shutdown(context.Background()) }()

	// 1. 启动模拟的下游 Auth, System & Desk gRPC Server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	authSrv := &mockAuthServer{}
	sysSrv := &mockSystemServer{}
	deskSrv := &mockDeskServer{}

	grpcServer := pkgrpc.NewServer()
	authv1.RegisterAuthServiceServer(grpcServer, authSrv)
	systemv1.RegisterSystemServiceServer(grpcServer, sysSrv)
	deskv1.RegisterDeskServiceServer(grpcServer, deskSrv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// 2. 初始化模拟 JWKS 公钥服务与 Gateway 服务
	rsaPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	kid := "hydra-key-01"
	jwksServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nStr := base64.RawURLEncoding.EncodeToString(rsaPriv.N.Bytes())
		eStr := base64.RawURLEncoding.EncodeToString(big.NewInt(int64(rsaPriv.E)).Bytes())
		_ = json.NewEncoder(w).Encode(pkgjwt.JSONWebKeySet{
			Keys: []pkgjwt.JSONWebKey{
				{Kty: "RSA", Kid: kid, Use: "sig", Alg: "RS256", N: nStr, E: eStr},
			},
		})
	}))
	defer jwksServer.Close()

	secretKey := "test-gateway-secret-32bytes-key"
	c := config.Config{
		RestConf: rest.RestConf{
			Host: "127.0.0.1",
			Port: 8080,
		},
		Auth: config.AuthConfig{
			AccessSecret: secretKey,
			JWKSURL:      jwksServer.URL,
			Whitelist: []string{
				"/ping",
				"/blade-auth/oauth/captcha",
				"/blade-auth/oauth/token",
				"/blade-auth/oauth/refresh-token",
			},
		},
		Upstreams: config.UpstreamConfig{
			Auth: pkgrpc.ClientConfig{
				Target: lis.Addr().String(),
			},
			System: pkgrpc.ClientConfig{
				Target: lis.Addr().String(),
			},
			Desk: pkgrpc.ClientConfig{
				Target: lis.Addr().String(),
			},
		},
	}

	svcCtx := svc.NewServiceContext(c)
	defer svcCtx.Close()

	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()
	handler.RegisterHandlers(server, svcCtx)

	jwtHandler, err := pkgjwt.NewJWTHandler(secretKey, 2*time.Hour, 24*time.Hour, "blade")
	require.NoError(t, err)

	// 3. 测试探活白名单接口 GET /ping
	t.Run("Ping_Whitelist", func(t *testing.T) {
		reqPing := httptest.NewRequest(http.MethodGet, "/ping", nil)
		wPing := httptest.NewRecorder()
		server.ServeHTTP(wPing, reqPing)

		assert.Equal(t, http.StatusOK, wPing.Code)
		var respPing map[string]any
		err := json.Unmarshal(wPing.Body.Bytes(), &respPing)
		require.NoError(t, err)
		assert.Equal(t, float64(200), respPing["code"])
		assert.Equal(t, "pong", respPing["data"])
	})

	// 4. 测试验证码接口与请求格式兼容 (snake_case & camelCase)
	t.Run("Captcha_And_RequestFormat_Compatibility", func(t *testing.T) {
		// 4.1 GET /blade-auth/oauth/captcha
		reqCaptcha := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/captcha", nil)
		wCaptcha := httptest.NewRecorder()
		server.ServeHTTP(wCaptcha, reqCaptcha)

		assert.Equal(t, http.StatusOK, wCaptcha.Code)
		var respCaptcha map[string]any
		require.NoError(t, json.Unmarshal(wCaptcha.Body.Bytes(), &respCaptcha))
		assert.Equal(t, float64(200), respCaptcha["code"])
		captchaData := respCaptcha["data"].(map[string]any)
		assert.Equal(t, "mock-captcha-key-001", captchaData["captcha_key"])
		assert.NotEmpty(t, captchaData["image"])

		// 4.2 POST /blade-auth/oauth/token (camelCase)
		loginCamel := map[string]string{
			"tenantId":    "100001",
			"account":     "admin",
			"password":    "123456",
			"captchaKey":  "camel-key",
			"captchaCode": "1111",
		}
		bodyCamel, _ := json.Marshal(loginCamel)
		reqLoginCamel := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(bodyCamel))
		reqLoginCamel.Header.Set("Content-Type", "application/json")
		wLoginCamel := httptest.NewRecorder()
		server.ServeHTTP(wLoginCamel, reqLoginCamel)

		assert.Equal(t, http.StatusOK, wLoginCamel.Code)
		authSrv.mu.Lock()
		assert.Equal(t, "100001", authSrv.lastLogin.GetTenantId())
		assert.Equal(t, "camel-key", authSrv.lastLogin.GetCaptchaKey())
		assert.Equal(t, "1111", authSrv.lastLogin.GetCaptchaCode())
		authSrv.mu.Unlock()

		// 4.3 POST /blade-auth/oauth/token (snake_case)
		loginSnake := map[string]string{
			"tenant_id":    "100002",
			"account":      "admin",
			"password":     "123456",
			"captcha_key":  "snake-key",
			"captcha_code": "2222",
		}
		bodySnake, _ := json.Marshal(loginSnake)
		reqLoginSnake := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(bodySnake))
		reqLoginSnake.Header.Set("Content-Type", "application/json")
		wLoginSnake := httptest.NewRecorder()
		server.ServeHTTP(wLoginSnake, reqLoginSnake)

		assert.Equal(t, http.StatusOK, wLoginSnake.Code)
		authSrv.mu.Lock()
		assert.Equal(t, "100002", authSrv.lastLogin.GetTenantId())
		assert.Equal(t, "snake-key", authSrv.lastLogin.GetCaptchaKey())
		assert.Equal(t, "2222", authSrv.lastLogin.GetCaptchaCode())
		authSrv.mu.Unlock()

		// 4.4 POST /blade-auth/oauth/refresh-token (camelCase & snake_case)
		refreshCamel, _ := json.Marshal(map[string]string{"refreshToken": "rt-camel-001"})
		reqRefCamel := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/refresh-token", bytes.NewReader(refreshCamel))
		reqRefCamel.Header.Set("Content-Type", "application/json")
		wRefCamel := httptest.NewRecorder()
		server.ServeHTTP(wRefCamel, reqRefCamel)
		assert.Equal(t, http.StatusOK, wRefCamel.Code)
		authSrv.mu.Lock()
		assert.Equal(t, "rt-camel-001", authSrv.lastRefresh.GetRefreshToken())
		authSrv.mu.Unlock()

		refreshSnake, _ := json.Marshal(map[string]string{"refresh_token": "rt-snake-002"})
		reqRefSnake := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/refresh-token", bytes.NewReader(refreshSnake))
		reqRefSnake.Header.Set("Content-Type", "application/json")
		wRefSnake := httptest.NewRecorder()
		server.ServeHTTP(wRefSnake, reqRefSnake)
		assert.Equal(t, http.StatusOK, wRefSnake.Code)
		authSrv.mu.Lock()
		assert.Equal(t, "rt-snake-002", authSrv.lastRefresh.GetRefreshToken())
		authSrv.mu.Unlock()
	})

	// 5. 测试非白名单受保护接口（无 Token 访问） -> 401 Unauthorized
	t.Run("Protected_Without_Token", func(t *testing.T) {
		reqNoToken := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/validate-token", nil)
		wNoToken := httptest.NewRecorder()
		server.ServeHTTP(wNoToken, reqNoToken)

		var respNoToken map[string]any
		require.NoError(t, json.Unmarshal(wNoToken.Body.Bytes(), &respNoToken))
		assert.Equal(t, float64(401), respNoToken["code"])
	})

	// 6. 测试跨租户访问拦截与租户上下文强制绑定
	t.Run("Tenant_Isolation_And_CrossTenant_Block", func(t *testing.T) {
		tokenTenant100001, err := jwtHandler.GenerateToken(pkgjwt.TokenPayload{
			TenantID: "100001",
			UserID:   9007199254740993,
			DeptID:   9007199254740995,
			RoleID:   "9007199254740999",
			Account:  "tenant_user",
			UserName: "租户测试员",
		})
		require.NoError(t, err)

		// 6.1 不传 Blade-Tenant-Id 请求头时，下游 gRPC 收到的 blade-tenant-id 必须是 Token 内的 "100001"，绝不能退回 "000000"
		reqNoHeader := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
		reqNoHeader.Header.Set("Blade-Auth", tokenTenant100001.AccessToken)
		wNoHeader := httptest.NewRecorder()
		server.ServeHTTP(wNoHeader, reqNoHeader)

		var respNoHeader map[string]any
		require.NoError(t, json.Unmarshal(wNoHeader.Body.Bytes(), &respNoHeader))
		assert.Equal(t, float64(200), respNoHeader["code"])

		sysSrv.mu.Lock()
		mdTenants := sysSrv.lastIncomingMD.Get(pkgrpc.MetadataTenantID)
		downstreamClaims := sysSrv.lastClaims
		sysSrv.mu.Unlock()
		require.NotEmpty(t, mdTenants)
		assert.Equal(t, "100001", mdTenants[0], "未传 Blade-Tenant-Id 时必须以 Token 中的租户 100001 传递给下游，严禁退回 000000")
		require.NotNil(t, downstreamClaims, "下游 gRPC 服务必须能从 Context 直接获取完整 BladeClaims")
		assert.Equal(t, int64(9007199254740993), downstreamClaims.UserID)
		assert.Equal(t, int64(9007199254740995), downstreamClaims.DeptID)
		assert.Equal(t, "9007199254740999", downstreamClaims.RoleID)
		assert.Equal(t, "tenant_user", downstreamClaims.Account)

		// 6.2 持有 "100001" Token 的客户端篡改请求头 Blade-Tenant-Id: 000000 企图越权访问管理租户 -> 必须返回 403 Forbidden
		reqCross000000 := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
		reqCross000000.Header.Set("Blade-Auth", tokenTenant100001.AccessToken)
		reqCross000000.Header.Set("Blade-Tenant-Id", "000000")
		wCross000000 := httptest.NewRecorder()
		server.ServeHTTP(wCross000000, reqCross000000)

		var respCross map[string]any
		require.NoError(t, json.Unmarshal(wCross000000.Body.Bytes(), &respCross))
		assert.Equal(t, float64(403), respCross["code"])
		assert.Contains(t, respCross["msg"], "拒绝跨租户访问")
	})

	// 6.5 测试 JWKS 非对称 RSA 令牌通过网关验签并向后透传完整 Claims
	t.Run("JWKS_RSA_Token_Validation_And_Claims_Propagation", func(t *testing.T) {
		hydraClaims := pkgjwt.BladeClaims{
			TenantID: "100001",
			UserID:   9007199254740993,
			DeptID:   9007199254740995,
			RoleID:   "9007199254740999",
			RoleName: "hydra_admin",
			Account:  "hydra_user",
			UserName: "Hydra测试员",
			RegisteredClaims: jwt.RegisteredClaims{
				Issuer:    "http://localhost:4444/",
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
				IssuedAt:  jwt.NewNumericDate(time.Now()),
			},
		}
		rsaToken := jwt.NewWithClaims(jwt.SigningMethodRS256, hydraClaims)
		rsaToken.Header["kid"] = kid
		rsaTokenStr, err := rsaToken.SignedString(rsaPriv)
		require.NoError(t, err)

		reqJWKS := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
		reqJWKS.Header.Set("Authorization", "Bearer "+rsaTokenStr)
		wJWKS := httptest.NewRecorder()
		server.ServeHTTP(wJWKS, reqJWKS)

		var respJWKS map[string]any
		require.NoError(t, json.Unmarshal(wJWKS.Body.Bytes(), &respJWKS))
		assert.Equal(t, float64(200), respJWKS["code"])

		sysSrv.mu.Lock()
		claimsFromJWKS := sysSrv.lastClaims
		sysSrv.mu.Unlock()
		require.NotNil(t, claimsFromJWKS)
		assert.Equal(t, "hydra_user", claimsFromJWKS.Account)
		assert.Equal(t, "hydra_admin", claimsFromJWKS.RoleName)
		assert.Equal(t, int64(9007199254740995), claimsFromJWKS.DeptID)
	})

	// 7. 测试雪花 ID 精度保留（超过 JS MAX_SAFE_INTEGER 9007199254740991 的 ID 必须序列化为 JSON 字符串）
	t.Run("Snowflake_ID_Precision_Preserved", func(t *testing.T) {
		tokenRes, err := jwtHandler.GenerateToken(pkgjwt.TokenPayload{
			TenantID: "100001",
			UserID:   9007199254740993,
			RoleID:   "9007199254740999",
			Account:  "tenant_user",
			UserName: "租户测试员",
		})
		require.NoError(t, err)

		// 7.1 GET /blade-system/user/info
		reqUser := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
		reqUser.Header.Set("Blade-Auth", tokenRes.AccessToken)
		wUser := httptest.NewRecorder()
		server.ServeHTTP(wUser, reqUser)

		var respUser map[string]any
		require.NoError(t, json.Unmarshal(wUser.Body.Bytes(), &respUser))
		assert.Equal(t, float64(200), respUser["code"])
		dataMap := respUser["data"].(map[string]any)
		userMap := dataMap["user"].(map[string]any)
		// 验证 JSON 反序列化后类型为 string 且精度无损
		idStr, ok := userMap["id"].(string)
		require.True(t, ok, "data.user.id 必须序列化为 JSON string")
		assert.Equal(t, "9007199254740993", idStr)
		deptIDStr, ok := userMap["dept_id"].(string)
		require.True(t, ok, "data.user.dept_id 必须序列化为 JSON string")
		assert.Equal(t, "9007199254740995", deptIDStr)

		// 7.2 GET /blade-system/menu/routes
		reqRoutes := httptest.NewRequest(http.MethodGet, "/blade-system/menu/routes", nil)
		reqRoutes.Header.Set("Blade-Auth", tokenRes.AccessToken)
		wRoutes := httptest.NewRecorder()
		server.ServeHTTP(wRoutes, reqRoutes)

		var respRoutes map[string]any
		require.NoError(t, json.Unmarshal(wRoutes.Body.Bytes(), &respRoutes))
		assert.Equal(t, float64(200), respRoutes["code"])
		routesData := respRoutes["data"].(map[string]any)
		routesList := routesData["routes"].([]any)
		require.Len(t, routesList, 1)
		firstRoute := routesList[0].(map[string]any)
		assert.Equal(t, "9007199254741001", firstRoute["id"])
		assert.Equal(t, "9007199254740997", firstRoute["parent_id"])
	})

	// 8. 测试下游 gRPC 内部错误脱敏（绝不向前端暴露 sql: 等敏感信息）
	t.Run("Internal_Error_Desensitization", func(t *testing.T) {
		sysSrv.mu.Lock()
		sysSrv.returnInternal = true
		sysSrv.mu.Unlock()
		defer func() {
			sysSrv.mu.Lock()
			sysSrv.returnInternal = false
			sysSrv.mu.Unlock()
		}()

		tokenRes, err := jwtHandler.GenerateToken(pkgjwt.TokenPayload{
			TenantID: "100001",
			UserID:   9007199254740993,
			Account:  "tenant_user",
		})
		require.NoError(t, err)

		reqUser := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
		reqUser.Header.Set("Blade-Auth", tokenRes.AccessToken)
		wUser := httptest.NewRecorder()
		server.ServeHTTP(wUser, reqUser)

		rawBody := wUser.Body.String()
		assert.NotContains(t, rawBody, "sql:", "响应体绝不能泄露底层数据库错误")

		var respErr map[string]any
		require.NoError(t, json.Unmarshal(wUser.Body.Bytes(), &respErr))
		assert.Equal(t, float64(500), respErr["code"])
		assert.Equal(t, "服务器内部错误", respErr["msg"])
	})

	// 9. 测试 Ory Hydra Challenge、System 分页、Desk 通知公告、OSS 上传及 Metrics/Trace 全链路集成
	t.Run("Hydra_SystemIPage_DeskNotice_OSS_And_Observability", func(t *testing.T) {
		// 9.1 Hydra Login & Consent Challenge 路由（免登白名单）
		reqHydraGet := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/login?login_challenge=lc_123", nil)
		wHydraGet := httptest.NewRecorder()
		server.ServeHTTP(wHydraGet, reqHydraGet)
		assert.Equal(t, http.StatusOK, wHydraGet.Code)
		assert.NotEmpty(t, wHydraGet.Header().Get(trace.HeaderTraceID), "响应头必须携带 X-Trace-Id")

		consentBody, _ := json.Marshal(map[string]any{
			"consent_challenge": "cc_456",
			"grant_scope":       []string{"openid", "profile"},
		})
		reqConsent := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/consent", bytes.NewReader(consentBody))
		reqConsent.Header.Set("Content-Type", "application/json")
		wConsent := httptest.NewRecorder()
		server.ServeHTTP(wConsent, reqConsent)
		assert.Equal(t, http.StatusOK, wConsent.Code)
		var respConsent map[string]any
		require.NoError(t, json.Unmarshal(wConsent.Body.Bytes(), &respConsent))
		assert.Equal(t, float64(200), respConsent["code"])

		tokenRes, err := jwtHandler.GenerateToken(pkgjwt.TokenPayload{
			TenantID: "100001",
			UserID:   9007199254740993,
			DeptID:   9007199254740995,
			RoleID:   "9007199254740999",
			Account:  "tenant_user",
			UserName: "租户测试员",
		})
		require.NoError(t, err)

		// 9.2 System 用户分页查询 IPage
		reqUserList := httptest.NewRequest(http.MethodGet, "/blade-system/user/list?current=1&size=10", nil)
		reqUserList.Header.Set("Blade-Auth", tokenRes.AccessToken)
		wUserList := httptest.NewRecorder()
		server.ServeHTTP(wUserList, reqUserList)
		assert.Equal(t, http.StatusOK, wUserList.Code)
		var respUserPage map[string]any
		require.NoError(t, json.Unmarshal(wUserList.Body.Bytes(), &respUserPage))
		assert.Equal(t, float64(200), respUserPage["code"])
		pageData := respUserPage["data"].(map[string]any)
		assert.Equal(t, float64(1), pageData["total"])

		// 9.3 Desk 通知公告分页查询（验证雪花 ID 与创建人/创建部门透传）
		reqNotices := httptest.NewRequest(http.MethodGet, "/blade-desk/notice/list?current=1&size=10", nil)
		reqNotices.Header.Set("Blade-Auth", tokenRes.AccessToken)
		wNotices := httptest.NewRecorder()
		server.ServeHTTP(wNotices, reqNotices)
		assert.Equal(t, http.StatusOK, wNotices.Code)
		var respNotices map[string]any
		require.NoError(t, json.Unmarshal(wNotices.Body.Bytes(), &respNotices))
		assert.Equal(t, float64(200), respNotices["code"])
		noticePage := respNotices["data"].(map[string]any)
		noticeRecords := noticePage["records"].([]any)
		require.Len(t, noticeRecords, 1)
		firstNotice := noticeRecords[0].(map[string]any)
		assert.Equal(t, "9007199254742001", firstNotice["id"])
		assert.Equal(t, "9007199254740993", firstNotice["create_user"])
		assert.Equal(t, "9007199254740995", firstNotice["create_dept"])

		// 9.4 OSS 对象存储上传 (/blade-resource/oss/endpoint/put-file)
		var buf bytes.Buffer
		mw := multipart.NewWriter(&buf)
		fw, err := mw.CreateFormFile("file", "avatar.png")
		require.NoError(t, err)
		_, _ = fw.Write([]byte("fake-png-content"))
		_ = mw.Close()

		reqUpload := httptest.NewRequest(http.MethodPost, "/blade-resource/oss/endpoint/put-file", &buf)
		reqUpload.Header.Set("Content-Type", mw.FormDataContentType())
		reqUpload.Header.Set("Blade-Auth", tokenRes.AccessToken)
		wUpload := httptest.NewRecorder()
		server.ServeHTTP(wUpload, reqUpload)
		assert.Equal(t, http.StatusOK, wUpload.Code)
		var respUpload map[string]any
		require.NoError(t, json.Unmarshal(wUpload.Body.Bytes(), &respUpload))
		assert.Equal(t, float64(200), respUpload["code"])
		ossData := respUpload["data"].(map[string]any)
		assert.True(t, strings.HasPrefix(ossData["name"].(string), "100001/"), "OSS 对象路径必须按租户 100001 隔离")

		// 9.5 Prometheus /metrics 与健康检查探针 (/healthz/live, /healthz/ready) 端点暴露验证
		reqLive := httptest.NewRequest(http.MethodGet, "/healthz/live", nil)
		wLive := httptest.NewRecorder()
		server.ServeHTTP(wLive, reqLive)
		assert.Equal(t, http.StatusOK, wLive.Code)
		var respLive map[string]any
		require.NoError(t, json.Unmarshal(wLive.Body.Bytes(), &respLive))
		assert.Equal(t, float64(200), respLive["code"])
		assert.Equal(t, "UP", respLive["data"].(map[string]any)["status"])

		reqReady := httptest.NewRequest(http.MethodGet, "/healthz/ready", nil)
		wReady := httptest.NewRecorder()
		server.ServeHTTP(wReady, reqReady)
		assert.Equal(t, http.StatusOK, wReady.Code)
		var respReady map[string]any
		require.NoError(t, json.Unmarshal(wReady.Body.Bytes(), &respReady))
		assert.Equal(t, float64(200), respReady["code"])
		assert.Equal(t, "UP", respReady["data"].(map[string]any)["status"])

		reqMetrics := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		wMetrics := httptest.NewRecorder()
		server.ServeHTTP(wMetrics, reqMetrics)
		assert.Equal(t, http.StatusOK, wMetrics.Code)
		assert.Contains(t, wMetrics.Body.String(), "blade_http_requests_total")
		assert.Contains(t, wMetrics.Body.String(), "blade_grpc_requests_total")
	})

	// 10. 测试主动登出吊销闭环：签发 Token -> 正常访问受保护接口返回 200 -> 调用 POST /blade-auth/oauth/logout 主动登出 -> 再次用同一 Token 访问受保护接口返回 HTTP 401
	t.Run("Logout_Token_Revocation_ClosedLoop", func(t *testing.T) {
		logoutTokenRes, err := jwtHandler.GenerateToken(pkgjwt.TokenPayload{
			TenantID: "100001",
			UserID:   9007199254740993,
			DeptID:   9007199254740995,
			RoleID:   "9007199254740999",
			Account:  "logout_test_user",
			UserName: "登出测试员",
		})
		require.NoError(t, err)

		// 10.1 登出前正常访问受保护接口 /blade-system/user/info 与 /blade-auth/oauth/validate-token 均返回 200
		reqBefore := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
		reqBefore.Header.Set("Authorization", "Bearer "+logoutTokenRes.AccessToken)
		wBefore := httptest.NewRecorder()
		server.ServeHTTP(wBefore, reqBefore)
		assert.Equal(t, http.StatusOK, wBefore.Code)
		var respBefore map[string]any
		require.NoError(t, json.Unmarshal(wBefore.Body.Bytes(), &respBefore))
		assert.Equal(t, float64(200), respBefore["code"])

		reqValBefore := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/validate-token", nil)
		reqValBefore.Header.Set("Authorization", "Bearer "+logoutTokenRes.AccessToken)
		wValBefore := httptest.NewRecorder()
		server.ServeHTTP(wValBefore, reqValBefore)
		assert.Equal(t, http.StatusOK, wValBefore.Code)

		// 10.2 调用 POST /blade-auth/oauth/logout 携带 Authorization: Bearer <token> 主动登出（不带 logout_challenge）
		reqLogout := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/logout", nil)
		reqLogout.Header.Set("Authorization", "Bearer "+logoutTokenRes.AccessToken)
		wLogout := httptest.NewRecorder()
		server.ServeHTTP(wLogout, reqLogout)
		assert.Equal(t, http.StatusOK, wLogout.Code)
		var respLogout map[string]any
		require.NoError(t, json.Unmarshal(wLogout.Body.Bytes(), &respLogout))
		assert.Equal(t, float64(200), respLogout["code"])
		logoutData := respLogout["data"].(map[string]any)
		assert.Equal(t, true, logoutData["revoked"])

		// 10.3 再次携带同一 AccessToken 访问 /blade-system/user/info 立即被拒绝并返回 HTTP 401
		reqAfter := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
		reqAfter.Header.Set("Authorization", "Bearer "+logoutTokenRes.AccessToken)
		wAfter := httptest.NewRecorder()
		server.ServeHTTP(wAfter, reqAfter)
		assert.Equal(t, http.StatusUnauthorized, wAfter.Code)
		var respAfter map[string]any
		require.NoError(t, json.Unmarshal(wAfter.Body.Bytes(), &respAfter))
		assert.Equal(t, float64(401), respAfter["code"])

		// 10.4 再次携带同一 AccessToken 访问 /blade-auth/oauth/validate-token 同样返回 HTTP 401
		reqValAfter := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/validate-token", nil)
		reqValAfter.Header.Set("Blade-Auth", logoutTokenRes.AccessToken)
		wValAfter := httptest.NewRecorder()
		server.ServeHTTP(wValAfter, reqValAfter)
		assert.Equal(t, http.StatusUnauthorized, wValAfter.Code)
	})

	// 11. 测试网关写接口 pkg/validator 中文参数校验与 RBAC 权限缓存/守卫闭环
	t.Run("Validator_ChineseError_And_RBAC_PermissionGuard_ClosedLoop", func(t *testing.T) {
		adminTokenRes, err := jwtHandler.GenerateToken(pkgjwt.TokenPayload{
			TenantID: "100001",
			UserID:   9007199254740991,
			DeptID:   9007199254740995,
			RoleID:   "1",
			RoleName: "administrator",
			Account:  "super_admin",
			UserName: "超级管理员",
		})
		require.NoError(t, err)

		// 11.1 新增用户时为空账号 -> 返回 code: 400 及中文提示 "登录账号不能为空"
		emptyAccountBody, _ := json.Marshal(map[string]any{
			"name": "测试员",
		})
		reqEmptyAcc := httptest.NewRequest(http.MethodPost, "/blade-system/user/submit", bytes.NewReader(emptyAccountBody))
		reqEmptyAcc.Header.Set("Content-Type", "application/json")
		reqEmptyAcc.Header.Set("Blade-Auth", adminTokenRes.AccessToken)
		wEmptyAcc := httptest.NewRecorder()
		server.ServeHTTP(wEmptyAcc, reqEmptyAcc)
		assert.Equal(t, http.StatusOK, wEmptyAcc.Code)
		var respEmptyAcc map[string]any
		require.NoError(t, json.Unmarshal(wEmptyAcc.Body.Bytes(), &respEmptyAcc))
		assert.Equal(t, float64(400), respEmptyAcc["code"])
		assert.Contains(t, respEmptyAcc["msg"], "登录账号不能为空")

		// 11.2 新增用户时提交非法邮箱 -> 返回 code: 400 及中文提示 "电子邮箱格式不正确"
		badEmailBody, _ := json.Marshal(map[string]any{
			"account": "valid_account",
			"email":   "not-an-email",
		})
		reqBadEmail := httptest.NewRequest(http.MethodPost, "/blade-system/user/submit", bytes.NewReader(badEmailBody))
		reqBadEmail.Header.Set("Content-Type", "application/json")
		reqBadEmail.Header.Set("Blade-Auth", adminTokenRes.AccessToken)
		wBadEmail := httptest.NewRecorder()
		server.ServeHTTP(wBadEmail, reqBadEmail)
		var respBadEmail map[string]any
		require.NoError(t, json.Unmarshal(wBadEmail.Body.Bytes(), &respBadEmail))
		assert.Equal(t, float64(400), respBadEmail["code"])
		assert.Contains(t, respBadEmail["msg"], "电子邮箱格式不正确")

		// 11.3 新增用户时提交非法手机号 -> 返回 code: 400 及中文提示 "手机号码格式不正确"
		badPhoneBody, _ := json.Marshal(map[string]any{
			"account": "valid_account",
			"email":   "ok@example.com",
			"phone":   "12345",
		})
		reqBadPhone := httptest.NewRequest(http.MethodPost, "/blade-system/user/submit", bytes.NewReader(badPhoneBody))
		reqBadPhone.Header.Set("Content-Type", "application/json")
		reqBadPhone.Header.Set("Blade-Auth", adminTokenRes.AccessToken)
		wBadPhone := httptest.NewRecorder()
		server.ServeHTTP(wBadPhone, reqBadPhone)
		var respBadPhone map[string]any
		require.NoError(t, json.Unmarshal(wBadPhone.Body.Bytes(), &respBadPhone))
		assert.Equal(t, float64(400), respBadPhone["code"])
		assert.Contains(t, respBadPhone["msg"], "手机号码格式不正确")

		// 11.4 新增部门时为空部门名 -> 返回 code: 400 及中文提示 "部门名称不能为空"
		emptyDeptBody, _ := json.Marshal(map[string]any{
			"full_name": "某部门全称",
		})
		reqEmptyDept := httptest.NewRequest(http.MethodPost, "/blade-system/dept/submit", bytes.NewReader(emptyDeptBody))
		reqEmptyDept.Header.Set("Content-Type", "application/json")
		reqEmptyDept.Header.Set("Blade-Auth", adminTokenRes.AccessToken)
		wEmptyDept := httptest.NewRecorder()
		server.ServeHTTP(wEmptyDept, reqEmptyDept)
		var respEmptyDept map[string]any
		require.NoError(t, json.Unmarshal(wEmptyDept.Body.Bytes(), &respEmptyDept))
		assert.Equal(t, float64(400), respEmptyDept["code"])
		assert.Contains(t, respEmptyDept["msg"], "部门名称不能为空")

		// 11.5 RBAC 权限拦截：非超管角色 "staff" 当前仅拥有 ["user:add"] 权限，调用 /blade-system/role/submit (需 "role:add") 返回 HTTP 403 Forbidden
		staffTokenRes, err := jwtHandler.GenerateToken(pkgjwt.TokenPayload{
			TenantID: "100001",
			UserID:   9007199254740998,
			DeptID:   9007199254740995,
			RoleID:   "200",
			RoleName: "staff",
			Account:  "staff_user",
			UserName: "普通职员",
		})
		require.NoError(t, err)

		roleSubmitBody, _ := json.Marshal(map[string]any{
			"role_name":  "测试角色",
			"role_alias": "test_role",
		})
		reqRoleDenied := httptest.NewRequest(http.MethodPost, "/blade-system/role/submit", bytes.NewReader(roleSubmitBody))
		reqRoleDenied.Header.Set("Content-Type", "application/json")
		reqRoleDenied.Header.Set("Blade-Auth", staffTokenRes.AccessToken)
		wRoleDenied := httptest.NewRecorder()
		server.ServeHTTP(wRoleDenied, reqRoleDenied)
		assert.Equal(t, http.StatusForbidden, wRoleDenied.Code)
		var respRoleDenied map[string]any
		require.NoError(t, json.Unmarshal(wRoleDenied.Body.Bytes(), &respRoleDenied))
		assert.Equal(t, float64(403), respRoleDenied["code"])

		// 但该非超管角色调用拥有权限的 /blade-system/user/submit ("user:add") 正常返回 200
		validUserBody, _ := json.Marshal(map[string]any{
			"account": "new_staff",
			"email":   "new_staff@example.com",
			"phone":   "13812345678",
		})
		reqUserAllowed := httptest.NewRequest(http.MethodPost, "/blade-system/user/submit", bytes.NewReader(validUserBody))
		reqUserAllowed.Header.Set("Content-Type", "application/json")
		reqUserAllowed.Header.Set("Blade-Auth", staffTokenRes.AccessToken)
		wUserAllowed := httptest.NewRecorder()
		server.ServeHTTP(wUserAllowed, reqUserAllowed)
		assert.Equal(t, http.StatusOK, wUserAllowed.Code)
		var respUserAllowed map[string]any
		require.NoError(t, json.Unmarshal(wUserAllowed.Body.Bytes(), &respUserAllowed))
		assert.Equal(t, float64(200), respUserAllowed["code"])

		// 11.6 超级管理员调用 /blade-system/role/grant 为该角色新增 "role:add" 权限后，PermCache 自动失效，再次请求 /blade-system/role/submit 立即 200 通过
		sysSrv.mu.Lock()
		sysSrv.customButtons = []string{"user:add", "role:add"}
		sysSrv.mu.Unlock()
		defer func() {
			sysSrv.mu.Lock()
			sysSrv.customButtons = nil
			sysSrv.mu.Unlock()
		}()

		grantBody, _ := json.Marshal(map[string]any{
			"role_ids": []string{"200"},
			"menu_ids": []string{"9007199254741001"},
		})
		reqGrant := httptest.NewRequest(http.MethodPost, "/blade-system/role/grant", bytes.NewReader(grantBody))
		reqGrant.Header.Set("Content-Type", "application/json")
		reqGrant.Header.Set("Blade-Auth", adminTokenRes.AccessToken)
		wGrant := httptest.NewRecorder()
		server.ServeHTTP(wGrant, reqGrant)
		assert.Equal(t, http.StatusOK, wGrant.Code)

		reqRoleAfterGrant := httptest.NewRequest(http.MethodPost, "/blade-system/role/submit", bytes.NewReader(roleSubmitBody))
		reqRoleAfterGrant.Header.Set("Content-Type", "application/json")
		reqRoleAfterGrant.Header.Set("Blade-Auth", staffTokenRes.AccessToken)
		wRoleAfterGrant := httptest.NewRecorder()
		server.ServeHTTP(wRoleAfterGrant, reqRoleAfterGrant)
		assert.Equal(t, http.StatusOK, wRoleAfterGrant.Code)
		var respRoleAfterGrant map[string]any
		require.NoError(t, json.Unmarshal(wRoleAfterGrant.Body.Bytes(), &respRoleAfterGrant))
		assert.Equal(t, float64(200), respRoleAfterGrant["code"])
	})

	// 12. 测试网关挂载 AuditMiddleware 操作审计日志：捕获操作人、TraceID、状态码，严格脱敏 password 与 phone，且下游 Handler 正常读取原始请求体
	t.Run("AuditLog_Desensitization_And_Downstream_BodyPreservation", func(t *testing.T) {
		require.NotNil(t, svcCtx.AuditRecorder)

		adminTokenRes, err := jwtHandler.GenerateToken(pkgjwt.TokenPayload{
			TenantID: "100001",
			UserID:   9007199254740991,
			DeptID:   9007199254740995,
			RoleID:   "1",
			RoleName: "administrator",
			Account:  "audit_admin",
			UserName: "审计管理员",
		})
		require.NoError(t, err)

		// 12.1 触发 POST /blade-auth/oauth/token 登录写请求
		loginPayload := map[string]string{
			"tenant_id":    "100001",
			"account":      "login_audit_user",
			"password":     "SuperSecretLoginPass!2026",
			"captcha_key":  "cap-key-999",
			"captcha_code": "6789",
		}
		loginBytes, _ := json.Marshal(loginPayload)
		reqLogin := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(loginBytes))
		reqLogin.Header.Set("Content-Type", "application/json")
		wLogin := httptest.NewRecorder()
		server.ServeHTTP(wLogin, reqLogin)
		assert.Equal(t, http.StatusOK, wLogin.Code)

		// 验证下游 AuthService 依然收到了未脱敏的原始密码与验证码
		authSrv.mu.Lock()
		assert.Equal(t, "SuperSecretLoginPass!2026", authSrv.lastLogin.GetPassword())
		assert.Equal(t, "6789", authSrv.lastLogin.GetCaptchaCode())
		authSrv.mu.Unlock()

		// 12.2 触发 POST /blade-system/user/submit 新增用户写请求（包含 password 与 phone）
		saveUserPayload := map[string]any{
			"account":  "audited_new_user",
			"password": "RawUserInitPass#888",
			"name":     "被审计新用户",
			"email":    "audited@example.com",
			"phone":    "13812345678",
		}
		saveUserBytes, _ := json.Marshal(saveUserPayload)
		reqSaveUser := httptest.NewRequest(http.MethodPost, "/blade-system/user/submit", bytes.NewReader(saveUserBytes))
		reqSaveUser.Header.Set("Content-Type", "application/json")
		reqSaveUser.Header.Set("Blade-Auth", adminTokenRes.AccessToken)
		wSaveUser := httptest.NewRecorder()
		server.ServeHTTP(wSaveUser, reqSaveUser)
		assert.Equal(t, http.StatusOK, wSaveUser.Code)

		// 验证下游 SystemService 收到完整的原始 password 与 phone
		sysSrv.mu.Lock()
		require.NotNil(t, sysSrv.lastSaveUser)
		assert.Equal(t, "RawUserInitPass#888", sysSrv.lastSaveUser.GetPassword())
		assert.Equal(t, "13812345678", sysSrv.lastSaveUser.GetPhone())
		sysSrv.mu.Unlock()

		// 验证 AuditRecorder 中捕获到的最新两条审计日志
		entries := svcCtx.AuditRecorder.Entries()
		require.GreaterOrEqual(t, len(entries), 2)

		loginAudit := entries[len(entries)-2]
		assert.Equal(t, "/blade-auth/oauth/token", loginAudit.Path)
		assert.Equal(t, "100001", loginAudit.TenantID)
		assert.Equal(t, "login_audit_user", loginAudit.Account)
		assert.Equal(t, http.StatusOK, loginAudit.StatusCode)
		assert.NotEmpty(t, loginAudit.TraceID)
		assert.NotContains(t, loginAudit.RequestBody, "SuperSecretLoginPass!2026")
		assert.NotContains(t, loginAudit.RequestBody, "6789")
		assert.Contains(t, loginAudit.RequestBody, auditlog.MaskedPlaceholder)

		userAudit := entries[len(entries)-1]
		assert.Equal(t, "/blade-system/user/submit", userAudit.Path)
		assert.Equal(t, "100001", userAudit.TenantID)
		assert.Equal(t, "service-gateway", userAudit.ServiceName)
		assert.Equal(t, int64(9007199254740991), userAudit.UserID.Int64())
		assert.Equal(t, "audit_admin", userAudit.Account)
		assert.Equal(t, http.StatusOK, userAudit.StatusCode)
		assert.Equal(t, wSaveUser.Header().Get(trace.HeaderTraceID), userAudit.TraceID)
		assert.NotEmpty(t, userAudit.TraceID)
		assert.NotContains(t, userAudit.RequestBody, "RawUserInitPass#888")
		assert.NotContains(t, userAudit.RequestBody, "13812345678")

		var auditedBodyMap map[string]any
		require.NoError(t, json.Unmarshal([]byte(userAudit.RequestBody), &auditedBodyMap))
		assert.Equal(t, "******", auditedBodyMap["password"])
		assert.Equal(t, "138****5678", auditedBodyMap["phone"])

		// 12.3 验证 GET /blade-system/log/api/list 分页查询接口与雪花 ID 精度保护
		reqAuditList := httptest.NewRequest(http.MethodGet, "/blade-system/log/api/list?current=1&size=10&account=audit_admin", nil)
		reqAuditList.Header.Set("Blade-Auth", adminTokenRes.AccessToken)
		wAuditList := httptest.NewRecorder()
		server.ServeHTTP(wAuditList, reqAuditList)
		assert.Equal(t, http.StatusOK, wAuditList.Code)
		assert.Contains(t, wAuditList.Body.String(), `"id":"9007199254749901"`)
		assert.Contains(t, wAuditList.Body.String(), `"user_id":"9007199254740991"`)
	})

	// 13. 测试多网关实例通过 MQ 广播模式 (WithBroadcast) 实时同步失效 RBAC 权限缓存
	t.Run("MultiGateway_RBAC_Cache_Invalidation_Via_MQ_Broadcast", func(t *testing.T) {
		sharedBroker := mq.NewMemoryBroker(mq.Config{
			Driver:      mq.DriverMemory,
			ServiceName: "shared-mq-cluster",
		})
		defer func() { _ = sharedBroker.Close(context.Background()) }()

		// 构造两个独立的网关 ServiceContext 实例（代表 K8s 中的两个 Gateway Pod 副本）
		gwCtx1 := svc.NewServiceContext(c, sharedBroker)
		defer gwCtx1.Close()
		gwCtx2 := svc.NewServiceContext(c, sharedBroker)
		defer gwCtx2.Close()

		// 预先在两个网关副本的 PermCache 中写入租户 100001 与租户 000000 的旧权限缓存
		require.NoError(t, gwCtx1.PermCache.Set(context.Background(), "100001:9001:200", []string{"user:view"}, 5*time.Minute))
		require.NoError(t, gwCtx2.PermCache.Set(context.Background(), "100001:9002:200", []string{"user:view"}, 5*time.Minute))
		require.NoError(t, gwCtx2.PermCache.Set(context.Background(), "000000:1:1", []string{"*"}, 5*time.Minute))

		// 模拟 System 服务发布租户 100001 的 RBAC 权限变更广播事件
		err := sharedBroker.Publish(context.Background(), event.TopicRBACChanged, event.RBACChangedEvent{
			TenantID: "100001",
			Action:   "grant_role_menus",
			RoleIDs:  []int64{200},
		})
		require.NoError(t, err)

		// 验证两个网关实例均收到广播并清除了租户 100001 的权限缓存，且未误删租户 000000 的缓存
		require.Eventually(t, func() bool {
			_, hit1, _ := gwCtx1.PermCache.Get(context.Background(), "100001:9001:200")
			_, hit2, _ := gwCtx2.PermCache.Get(context.Background(), "100001:9002:200")
			return !hit1 && !hit2
		}, 2*time.Second, 10*time.Millisecond)

		_, hitOtherTenant, _ := gwCtx2.PermCache.Get(context.Background(), "000000:1:1")
		assert.True(t, hitOtherTenant)
	})
}
