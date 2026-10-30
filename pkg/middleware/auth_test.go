package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/xerr"
)

func TestAuthMiddleware(t *testing.T) {
	jwtHandler, err := jwt.NewJWTHandler("test-secret-key-123", time.Hour, 24*time.Hour, "blade")
	if err != nil {
		t.Fatalf("create jwt handler failed: %v", err)
	}

	// 签发测试令牌
	payload := jwt.TokenPayload{
		TenantID: "000000",
		UserID:   998877,
		Account:  "blade_user",
		UserName: "BladeUser",
	}
	tokenRes, err := jwtHandler.GenerateToken(payload)
	if err != nil {
		t.Fatalf("generate token failed: %v", err)
	}

	authMw := AuthMiddleware(jwtHandler, "/api/public", "/oauth/captcha")

	// 目标 Handler：若鉴权通过，返回当前上下文中的 Account
	targetHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims := FromClaims(r.Context())
		if claims != nil {
			response.OkJson(r.Context(), w, claims.Account)
		} else {
			response.OkJson(r.Context(), w, "anonymous")
		}
	})

	chained := authMw(targetHandler)

	// 1. 测试白名单放行
	t.Run("WhiteList Allow", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/public/info", nil)
		rec := httptest.NewRecorder()
		chained(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		var resp response.Response[string]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Data != "anonymous" {
			t.Fatalf("expected anonymous in whitelist, got %s", resp.Data)
		}
	})

	// 2. 测试未携带 Token 被拦截
	t.Run("No Token Unauthorized", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/protected/user", nil)
		rec := httptest.NewRecorder()
		chained(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected HTTP 401 Unauthorized, got HTTP %d", rec.Code)
		}
		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Code != xerr.Unauthorized {
			t.Fatalf("expected 401 Unauthorized in body code, got %d", resp.Code)
		}
	})

	// 3. 测试通过 Blade-Auth 头正常鉴权并通过
	t.Run("Valid Blade-Auth Header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/protected/user", nil)
		req.Header.Set(HeaderBladeAuth, tokenRes.AccessToken)
		rec := httptest.NewRecorder()
		chained(rec, req)

		var resp response.Response[string]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Code != 200 || resp.Data != "blade_user" {
			t.Fatalf("expected blade_user in response data, got %+v", resp)
		}
	})

	// 4. 测试通过 Authorization Bearer 正常鉴权
	t.Run("Valid Authorization Bearer Header", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/protected/user", nil)
		req.Header.Set(HeaderAuthorization, "Bearer "+tokenRes.AccessToken)
		rec := httptest.NewRecorder()
		chained(rec, req)

		var resp response.Response[string]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Code != 200 || resp.Data != "blade_user" {
			t.Fatalf("expected blade_user in response data, got %+v", resp)
		}
	})

	// 5. 测试 RefreshToken 不能用于请求业务接口（类型隔离）
	t.Run("RefreshToken Rejected", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/protected/user", nil)
		req.Header.Set(HeaderBladeAuth, tokenRes.RefreshToken)
		rec := httptest.NewRecorder()
		chained(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected HTTP 401 for rejected refresh token, got %d", rec.Code)
		}
		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Code != xerr.TokenInvalid {
			t.Fatalf("expected TokenInvalid code for refresh token, got %d", resp.Code)
		}
	})

	// 6. 测试非法字符串 Token
	t.Run("Invalid Token Format", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/api/protected/user", nil)
		req.Header.Set(HeaderBladeAuth, "invalid.jwt.token")
		rec := httptest.NewRecorder()
		chained(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected HTTP 401 for invalid token, got %d", rec.Code)
		}
		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Code != xerr.TokenInvalid {
			t.Fatalf("expected TokenInvalid, got %d", resp.Code)
		}
	})

	// 7. 测试白名单不会因非目录前缀匹配而发生绕过（如 /oauth/captcha 不应放行 /oauth/captcha_secret）
	t.Run("Whitelist Non-Directory Prefix Blocked", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/oauth/captcha_secret", nil)
		rec := httptest.NewRecorder()
		chained(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected HTTP 401 for non-matching prefix /oauth/captcha_secret, got %d", rec.Code)
		}
	})

	// 8. 测试普通租户伪造 000000 管理租户 Header 被严格拦截
	t.Run("Cross Tenant To 000000 Blocked", func(t *testing.T) {
		tenantAToken, err := jwtHandler.GenerateToken(jwt.TokenPayload{
			TenantID: "100001",
			UserID:   888,
			Account:  "tenant_a_user",
		})
		if err != nil {
			t.Fatalf("generate tenant A token failed: %v", err)
		}
		req := httptest.NewRequest("GET", "/api/protected/user", nil)
		req.Header.Set(HeaderBladeAuth, tenantAToken.AccessToken)
		req.Header.Set("Blade-Tenant-Id", "000000")
		rec := httptest.NewRecorder()
		chained(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected HTTP 403 Forbidden when tenant 100001 sends Blade-Tenant-Id: 000000, got %d", rec.Code)
		}
	})
}

func TestCorsMiddleware(t *testing.T) {
	handler := CorsMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	// 测试带 Origin 的 OPTIONS 预检请求（必须动态回显 Origin 并设置 Allow-Credentials 为 true）
	reqOpt := httptest.NewRequest("OPTIONS", "/api/data", nil)
	reqOpt.Header.Set("Origin", "http://localhost:8080")
	recOpt := httptest.NewRecorder()
	handler(recOpt, reqOpt)

	if recOpt.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for OPTIONS, got %d", recOpt.Code)
	}
	if recOpt.Header().Get("Access-Control-Allow-Origin") != "http://localhost:8080" {
		t.Fatalf("expected Origin to be echoed back, got %s", recOpt.Header().Get("Access-Control-Allow-Origin"))
	}
	if recOpt.Header().Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatalf("expected Allow-Credentials to be true")
	}

	// 测试普通不带 Origin 的请求（降级为 *）
	reqNoOrigin := httptest.NewRequest("GET", "/api/data", nil)
	recNoOrigin := httptest.NewRecorder()
	handler(recNoOrigin, reqNoOrigin)
	if recNoOrigin.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("expected * when Origin header is empty, got %s", recNoOrigin.Header().Get("Access-Control-Allow-Origin"))
	}

	// 测试普通请求
	reqGet := httptest.NewRequest("GET", "/api/data", nil)
	recGet := httptest.NewRecorder()
	handler(recGet, reqGet)

	if recGet.Code != http.StatusOK || recGet.Body.String() != "ok" {
		t.Fatalf("expected GET to pass through, got %d: %s", recGet.Code, recGet.Body.String())
	}
}
