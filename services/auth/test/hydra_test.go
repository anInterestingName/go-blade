package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	authv1 "github.com/go-blade/api/proto/auth/v1"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/response"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
	"github.com/go-blade/services/auth/internal/hydra"
	"github.com/go-blade/services/auth/internal/server"
	"github.com/go-blade/services/auth/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// mockHydraState 记录 httptest 模拟 Hydra Admin API 的状态。
type mockHydraState struct {
	mu               sync.Mutex
	lastLoginBody    hydra.AcceptLoginBody
	lastConsentBody  hydra.AcceptConsentBody
	savedContext     map[string]any
	savedSubject     string
	logoutAcceptedCh string
}

func startMockHydraAdminServer(t *testing.T, state *mockHydraState) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/admin/oauth2/auth/requests/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ch := r.URL.Query().Get("login_challenge")
		w.Header().Set("Content-Type", "application/json")
		switch ch {
		case "lc_skip_true":
			_ = json.NewEncoder(w).Encode(hydra.LoginRequest{
				Challenge: ch,
				Skip:      true,
				Subject:   "000000:1123598821738675200",
				Client:    hydra.OAuth2Client{ClientID: "saber-web", ClientName: "Saber Frontend"},
			})
		case "lc_normal", "lc_grpc":
			_ = json.NewEncoder(w).Encode(hydra.LoginRequest{
				Challenge:      ch,
				Skip:           false,
				Subject:        "",
				Client:         hydra.OAuth2Client{ClientID: "saber-web", ClientName: "Saber Frontend"},
				RequestedScope: []string{"openid", "offline_access", "profile"},
			})
		default:
			http.Error(w, `{"error":"not_found"}`, http.StatusNotFound)
		}
	})

	mux.HandleFunc("/admin/oauth2/auth/requests/login/accept", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ch := r.URL.Query().Get("login_challenge")
		var body hydra.AcceptLoginBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		state.mu.Lock()
		state.lastLoginBody = body
		state.savedSubject = body.Subject
		state.savedContext = body.Context
		state.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hydra.RedirectResponse{
			RedirectTo: "http://hydra.example.com/oauth2/auth?login_verifier=" + ch,
		})
	})

	mux.HandleFunc("/admin/oauth2/auth/requests/consent", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ch := r.URL.Query().Get("consent_challenge")
		state.mu.Lock()
		sub := state.savedSubject
		ctxMap := state.savedContext
		state.mu.Unlock()

		if ch == "cc_db_fallback" {
			// 测试当 Context 为空时，通过 Subject 回查数据库组装 BladeClaims
			sub = "000000:1123598821738675200"
			ctxMap = nil
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hydra.ConsentRequest{
			Challenge:                    ch,
			Skip:                         false,
			Subject:                      sub,
			Client:                       hydra.OAuth2Client{ClientID: "saber-web"},
			RequestedScope:               []string{"openid", "profile"},
			RequestedAccessTokenAudience: []string{"https://api.go-blade.dev"},
			Context:                      ctxMap,
		})
	})

	mux.HandleFunc("/admin/oauth2/auth/requests/consent/accept", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ch := r.URL.Query().Get("consent_challenge")
		var body hydra.AcceptConsentBody
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		state.mu.Lock()
		state.lastConsentBody = body
		state.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hydra.RedirectResponse{
			RedirectTo: "http://hydra.example.com/oauth2/auth?consent_verifier=" + ch,
		})
	})

	mux.HandleFunc("/admin/oauth2/auth/requests/logout", func(w http.ResponseWriter, r *http.Request) {
		ch := r.URL.Query().Get("logout_challenge")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hydra.LogoutRequest{
			Challenge: ch,
			Subject:   "000000:1123598821738675200",
		})
	})

	mux.HandleFunc("/admin/oauth2/auth/requests/logout/accept", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ch := r.URL.Query().Get("logout_challenge")
		state.mu.Lock()
		state.logoutAcceptedCh = ch
		state.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(hydra.RedirectResponse{
			RedirectTo: "http://saber.example.com/login?logged_out=" + ch,
		})
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestHydraChallengeFlow_HTTPAndGRPC_Integration(t *testing.T) {
	_, restServer, svcCtx, _ := setupTestServer(t)

	hydraState := &mockHydraState{}
	hydraSrv := startMockHydraAdminServer(t, hydraState)
	svcCtx.HydraClient = hydra.NewHTTPAdminClient(hydraSrv.URL)

	// 1. 测试 HTTP GET /blade-auth/oauth/login (skip=false)
	{
		req := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/login?login_challenge=lc_normal", nil)
		rec := httptest.NewRecorder()
		restServer.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp response.Response[types.GetLoginChallengeResp]
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.True(t, resp.Success)
		assert.Equal(t, "lc_normal", resp.Data.Challenge)
		assert.False(t, resp.Data.Skip)
		assert.Equal(t, "saber-web", resp.Data.ClientID)
		assert.Empty(t, resp.Data.RedirectTo)
	}

	// 2. 测试 HTTP GET /blade-auth/oauth/login (skip=true 自动 AcceptLogin 并返回 redirect_to)
	{
		req := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/login?login_challenge=lc_skip_true", nil)
		rec := httptest.NewRecorder()
		restServer.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp response.Response[types.GetLoginChallengeResp]
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.True(t, resp.Success)
		assert.True(t, resp.Data.Skip)
		assert.Equal(t, "000000:1123598821738675200", resp.Data.Subject)
		assert.Equal(t, "http://hydra.example.com/oauth2/auth?login_verifier=lc_skip_true", resp.Data.RedirectTo)
	}

	// 3. 测试 HTTP POST /blade-auth/oauth/login 密码错误时拒绝且不调用 Hydra AcceptLogin
	{
		capWrong, err := pkgredis.GenerateCaptcha(context.Background(), time.Minute)
		require.NoError(t, err)

		reqBody, _ := json.Marshal(types.AcceptLoginChallengeReq{
			LoginChallenge: "lc_normal",
			TenantID:       "000000",
			Account:        "admin",
			Password:       "wrong_password",
			CaptchaKey:     capWrong.Key,
			CaptchaCode:    capWrong.Code,
		})
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/login", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		restServer.ServeHTTP(rec, req)

		var resp response.Response[any]
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.False(t, resp.Success)
		assert.Equal(t, xerr.PasswordError, resp.Code)
	}

	// 4. 测试 HTTP POST /blade-auth/oauth/login 验证码+多租户账号密码校验成功后向 Hydra 发送 subject 与 context
	{
		capOK, err := pkgredis.GenerateCaptcha(context.Background(), time.Minute)
		require.NoError(t, err)

		reqBody, _ := json.Marshal(types.AcceptLoginChallengeReq{
			LoginChallenge: "lc_normal",
			TenantID:       "000000",
			Account:        "admin",
			Password:       "admin123",
			CaptchaKey:     capOK.Key,
			CaptchaCode:    capOK.Code,
			Remember:       true,
			RememberFor:    3600,
		})
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/login", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		restServer.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp response.Response[types.ChallengeRedirectResp]
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.True(t, resp.Success)
		assert.Equal(t, "http://hydra.example.com/oauth2/auth?login_verifier=lc_normal", resp.Data.RedirectTo)

		hydraState.mu.Lock()
		loginBody := hydraState.lastLoginBody
		hydraState.mu.Unlock()

		assert.Equal(t, "000000:1123598821738675200", loginBody.Subject)
		assert.True(t, loginBody.Remember)
		assert.Equal(t, int64(3600), loginBody.RememberFor)
		assert.Equal(t, "000000", loginBody.Context["tenant_id"])
		assert.Equal(t, "1123598821738675200", loginBody.Context["user_id"])
		assert.Equal(t, "1123598813738675200", loginBody.Context["dept_id"])
		assert.Equal(t, "1,2", loginBody.Context["role_id"])
		assert.Equal(t, "administrator", loginBody.Context["role_name"])
		assert.Equal(t, "admin", loginBody.Context["account"])
		assert.Equal(t, "系统管理员", loginBody.Context["user_name"])
		assert.Equal(t, "管理员", loginBody.Context["nick_name"])
	}

	// 5. 测试 HTTP POST /blade-auth/oauth/consent 自动将 BladeClaims 注入 session.access_token 和 session.id_token
	{
		reqBody, _ := json.Marshal(types.AcceptConsentChallengeReq{
			ConsentChallenge: "cc_normal",
			GrantScope:       []string{"openid", "profile"},
			Remember:         true,
			RememberFor:      7200,
		})
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/consent", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		restServer.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp response.Response[types.ChallengeRedirectResp]
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.True(t, resp.Success)
		assert.Equal(t, "http://hydra.example.com/oauth2/auth?consent_verifier=cc_normal", resp.Data.RedirectTo)

		hydraState.mu.Lock()
		consentBody := hydraState.lastConsentBody
		hydraState.mu.Unlock()

		require.NotNil(t, consentBody.Session)
		assert.Equal(t, []string{"openid", "profile"}, consentBody.GrantScope)
		assert.Equal(t, []string{"https://api.go-blade.dev"}, consentBody.GrantAccessTokenAudience)
		assert.Equal(t, "000000", consentBody.Session.AccessToken["tenant_id"])
		assert.Equal(t, "1123598821738675200", consentBody.Session.AccessToken["user_id"])
		assert.Equal(t, "1123598813738675200", consentBody.Session.AccessToken["dept_id"])
		assert.Equal(t, "administrator", consentBody.Session.AccessToken["role_name"])
		assert.Equal(t, "admin", consentBody.Session.AccessToken["account"])
		assert.Equal(t, "access_token", consentBody.Session.AccessToken["category"])

		assert.Equal(t, "000000", consentBody.Session.IDToken["tenant_id"])
		assert.Equal(t, "1123598821738675200", consentBody.Session.IDToken["user_id"])
		assert.Equal(t, "admin", consentBody.Session.IDToken["account"])
		assert.Equal(t, "系统管理员", consentBody.Session.IDToken["user_name"])
	}

	// 6. 测试当 Hydra Consent Context 为空时，根据 Subject 回查数据库注入完整 BladeClaims
	{
		reqBody, _ := json.Marshal(types.AcceptConsentChallengeReq{
			ConsentChallenge: "cc_db_fallback",
		})
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/consent", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		restServer.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		hydraState.mu.Lock()
		consentBody := hydraState.lastConsentBody
		hydraState.mu.Unlock()

		require.NotNil(t, consentBody.Session)
		assert.Equal(t, []string{"openid", "profile"}, consentBody.GrantScope)
		assert.Equal(t, "000000", consentBody.Session.AccessToken["tenant_id"])
		assert.Equal(t, "1123598821738675200", consentBody.Session.AccessToken["user_id"])
		assert.Equal(t, "administrator", consentBody.Session.AccessToken["role_name"])
	}

	// 7. 测试 HTTP POST /blade-auth/oauth/logout 完成登出挑战
	{
		reqBody, _ := json.Marshal(types.AcceptLogoutChallengeReq{
			LogoutChallenge: "lgc_001",
		})
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/logout", bytes.NewReader(reqBody))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		restServer.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		var resp response.Response[types.ChallengeRedirectResp]
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.True(t, resp.Success)
		assert.Equal(t, "http://saber.example.com/login?logged_out=lgc_001", resp.Data.RedirectTo)
	}

	// 8. 测试 gRPC 4 个 Hydra Challenge 接口完整闭环
	{
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer lis.Close()

		grpcServer := pkgrpc.NewServer()
		authv1.RegisterAuthServiceServer(grpcServer, server.NewAuthServer(svcCtx))
		go func() {
			_ = grpcServer.Serve(lis)
		}()
		defer grpcServer.Stop()

		conn, err := pkgrpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		defer conn.Close()

		client := authv1.NewAuthServiceClient(conn)
		ctx := tenant.WithTenant(context.Background(), "000000")

		// gRPC GetLoginChallenge
		getLoginResp, err := client.GetLoginChallenge(ctx, &authv1.GetLoginChallengeRequest{
			LoginChallenge: "lc_grpc",
		})
		require.NoError(t, err)
		assert.Equal(t, "lc_grpc", getLoginResp.Challenge)
		assert.False(t, getLoginResp.Skip)
		assert.Equal(t, "saber-web", getLoginResp.ClientId)

		// gRPC AcceptLoginChallenge (错误密码拒绝)
		capBad, err := pkgredis.GenerateCaptcha(context.Background(), time.Minute)
		require.NoError(t, err)
		_, err = client.AcceptLoginChallenge(ctx, &authv1.AcceptLoginChallengeRequest{
			LoginChallenge: "lc_grpc",
			TenantId:       "000000",
			Account:        "admin",
			Password:       "bad_pwd",
			CaptchaKey:     capBad.Key,
			CaptchaCode:    capBad.Code,
		})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		bizCode, _, hasBiz := xerr.ParseGrpcBizError(st.Message())
		require.True(t, hasBiz)
		assert.Equal(t, xerr.PasswordError, bizCode)

		// gRPC AcceptLoginChallenge (正确凭据)
		capGrpc, err := pkgredis.GenerateCaptcha(context.Background(), time.Minute)
		require.NoError(t, err)
		acceptLoginResp, err := client.AcceptLoginChallenge(ctx, &authv1.AcceptLoginChallengeRequest{
			LoginChallenge: "lc_grpc",
			TenantId:       "000000",
			Account:        "admin",
			Password:       "admin123",
			CaptchaKey:     capGrpc.Key,
			CaptchaCode:    capGrpc.Code,
			Remember:       true,
			RememberFor:    1800,
		})
		require.NoError(t, err)
		assert.Equal(t, "http://hydra.example.com/oauth2/auth?login_verifier=lc_grpc", acceptLoginResp.RedirectTo)

		// gRPC AcceptConsentChallenge
		acceptConsentResp, err := client.AcceptConsentChallenge(ctx, &authv1.AcceptConsentChallengeRequest{
			ConsentChallenge: "cc_grpc",
			GrantScope:       []string{"openid"},
			Remember:         true,
			RememberFor:      1800,
		})
		require.NoError(t, err)
		assert.Equal(t, "http://hydra.example.com/oauth2/auth?consent_verifier=cc_grpc", acceptConsentResp.RedirectTo)

		// gRPC AcceptLogoutChallenge
		acceptLogoutResp, err := client.AcceptLogoutChallenge(ctx, &authv1.AcceptLogoutChallengeRequest{
			LogoutChallenge: "lgc_grpc",
		})
		require.NoError(t, err)
		assert.Equal(t, "http://saber.example.com/login?logged_out=lgc_grpc", acceptLogoutResp.RedirectTo)
	}
}
