package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/glebarez/sqlite"
	"github.com/go-blade/pkg/jwt"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/xerr"
	"github.com/go-blade/services/auth/internal/config"
	"github.com/go-blade/services/auth/internal/handler"
	"github.com/go-blade/services/auth/internal/model"
	"github.com/go-blade/services/auth/internal/svc"
	"github.com/go-blade/services/auth/internal/types"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"gorm.io/gorm"
)

func setupTestServer(t *testing.T) (*miniredis.Miniredis, *rest.Server, *svc.ServiceContext, *gorm.DB) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	redisConf := zeroredis.RedisConf{
		Host: mr.Addr(),
		Type: zeroredis.NodeType,
	}

	redisClient := zeroredis.MustNewRedis(redisConf)
	pkgredis.SetRedis(redisClient)

	// 初始化 SQLite 内存数据库并挂载租户插件
	dsn := "file:" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	if err := db.Use(tenant.NewTenantPlugin()); err != nil {
		t.Fatalf("failed to use tenant plugin: %v", err)
	}
	if err := db.AutoMigrate(&model.User{}, &model.Role{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}

	// 插入真实测试角色与用户 (admin/admin123, 租户 000000)
	hashedPwd, _ := model.HashPassword("admin123")
	userAdmin := model.User{
		ID:       1123598821738675200,
		TenantID: "000000",
		Account:  "admin",
		Password: hashedPwd,
		Name:     "系统管理员",
		RealName: "管理员",
		DeptID:   1123598813738675200,
		RoleID:   "1,2",
		Status:   1, // 正常状态
	}
	// 禁用状态用户 (frozen_user/123456)
	frozenPwd, _ := model.HashPassword("123456")
	userFrozen := model.User{
		ID:       1123598821738675201,
		TenantID: "000000",
		Account:  "frozen_user",
		Password: frozenPwd,
		Name:     "冻结用户",
		Status:   2, // 冻结状态
	}

	ctx := tenant.WithTenant(context.Background(), "000000")
	db.WithContext(ctx).Create(&model.Role{
		ID:        1,
		TenantID:  "000000",
		RoleName:  "超级管理员",
		RoleAlias: "administrator",
		Sort:      1,
	})
	db.WithContext(ctx).Create(&userAdmin)
	db.WithContext(ctx).Create(&userFrozen)

	t.Cleanup(func() {
		mr.Close()
		pkgredis.SetRedis(nil)
	})

	c := config.Config{
		RestConf: rest.RestConf{
			Host: "127.0.0.1",
			Port: 8888,
		},
		Auth: config.AuthConfig{
			AccessSecret:  "blade-secret-key-for-test-32bytes",
			AccessExpire:  3600,
			RefreshExpire: 86400,
			Issuer:        "blade",
		},
		Redis: pkgredis.Config{
			Host: mr.Addr(),
			Type: zeroredis.NodeType,
		},
	}

	server := rest.MustNewServer(c.RestConf)
	svcCtx := svc.NewServiceContext(c)
	svcCtx.DB = db // 注入测试数据库
	handler.RegisterHandlers(server, svcCtx)

	return mr, server, svcCtx, db
}

func TestAuthWorkflow_Integration(t *testing.T) {
	_, server, svcCtx, db := setupTestServer(t)

	// 1. 测试 CORS 预检请求 OPTIONS（必须动态回显 Origin 并设置 Allow-Credentials 为 true）
	{
		req := httptest.NewRequest(http.MethodOptions, "/blade-auth/oauth/token", nil)
		req.Header.Set("Origin", "http://localhost:8080")
		req.Header.Set("Access-Control-Request-Method", "POST")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected status 204 for OPTIONS, got %d", rec.Code)
		}
		if origin := rec.Header().Get("Access-Control-Allow-Origin"); origin != "http://localhost:8080" {
			t.Errorf("expected Access-Control-Allow-Origin 'http://localhost:8080', got %s", origin)
		}
		if cred := rec.Header().Get("Access-Control-Allow-Credentials"); cred != "true" {
			t.Errorf("expected Access-Control-Allow-Credentials 'true', got %s", cred)
		}
	}

	// 2. 测试获取验证码 GET /blade-auth/oauth/captcha（验证返回无明文数字，且图片解码后尺寸 > 1x1）
	var captchaKey string
	{
		req := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/captcha", nil)
		req.Header.Set("Blade-Tenant-Id", "000000")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected status 200 for captcha, got %d, body: %s", rec.Code, rec.Body.String())
		}

		var resp response.Response[types.CaptchaResp]
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal captcha response failed: %v", err)
		}
		if !resp.Success || resp.Code != 200 {
			t.Fatalf("captcha request business failure: %+v", resp)
		}
		if resp.Data.CaptchaKey == "" {
			t.Fatalf("empty captcha_key in response")
		}
		if !strings.HasPrefix(resp.Data.Image, "data:image/png;base64,") {
			t.Fatalf("expected PNG data URL, got: %s", resp.Data.Image)
		}
		rawPNG, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(resp.Data.Image, "data:image/png;base64,"))
		if err != nil {
			t.Fatalf("failed to base64 decode captcha image: %v", err)
		}
		imgCfg, err := png.DecodeConfig(bytes.NewReader(rawPNG))
		if err != nil {
			t.Fatalf("failed to decode PNG config: %v", err)
		}
		if imgCfg.Width <= 1 || imgCfg.Height <= 1 {
			t.Fatalf("expected real captcha image > 1x1, got %dx%d", imgCfg.Width, imgCfg.Height)
		}
		captchaKey = resp.Data.CaptchaKey
	}

	// 从底层 Redis 取出该验证码（模拟用户真实肉眼识别图片后输入）
	rds, _ := pkgredis.GetRedis()
	realCode, _ := rds.GetCtx(context.Background(), pkgredis.CaptchaKeyPrefix+captchaKey)

	// 3. 测试错误验证码登录 POST /blade-auth/oauth/token
	{
		loginReq := types.TokenReq{
			TenantID:    "000000",
			Account:     "admin",
			Password:    "admin123",
			CaptchaKey:  captchaKey,
			CaptchaCode: "0000_wrong",
		}
		body, _ := json.Marshal(loginReq)
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Success || resp.Code != xerr.CaptchaError {
			t.Fatalf("expected login to fail with wrong captcha, got: %+v", resp)
		}
	}

	// 4. 测试错误密码拒绝
	{
		loginReq := types.TokenReq{
			TenantID:    "000000",
			Account:     "admin",
			Password:    "wrong_pwd",
			CaptchaKey:  captchaKey,
			CaptchaCode: realCode,
		}
		body, _ := json.Marshal(loginReq)
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Success || resp.Code != xerr.PasswordError {
			t.Fatalf("expected PasswordError, got %+v", resp)
		}
	}

	// 5. 测试 DB == nil 时用 "admin" / "123456" 登录必须被拒绝（返回 500 内部错误，绝不签发假管理员 Token）
	{
		svcCtx.DB = nil
		resCapNilDB, _ := pkgredis.GenerateCaptcha(context.Background(), time.Minute)
		loginReq := types.TokenReq{
			TenantID:    "000000",
			Account:     "admin",
			Password:    "123456",
			CaptchaKey:  resCapNilDB.Key,
			CaptchaCode: resCapNilDB.Code,
		}
		body, _ := json.Marshal(loginReq)
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		svcCtx.DB = db // 恢复数据库

		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Success || resp.Code != xerr.ServerCommonError {
			t.Fatalf("expected 500 error when DB is nil, got: %+v", resp)
		}
	}

	// 6. 测试真实用户密码正确登录 (admin / admin123)
	resCaptcha, _ := pkgredis.GenerateCaptcha(context.Background(), time.Minute)
	var tokenResult jwt.TokenResult
	{
		loginReq := types.TokenReq{
			TenantID:    "000000",
			Account:     "admin",
			Password:    "admin123",
			CaptchaKey:  resCaptcha.Key,
			CaptchaCode: resCaptcha.Code,
		}
		body, _ := json.Marshal(loginReq)
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected login 200, got %d, body: %s", rec.Code, rec.Body.String())
		}

		var resp response.Response[jwt.TokenResult]
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal login response failed: %v", err)
		}
		if !resp.Success || resp.Data.AccessToken == "" || resp.Data.RefreshToken == "" {
			t.Fatalf("unexpected login failure: %+v", resp)
		}
		tokenResult = resp.Data
	}

	// 7. 验证码单次防重放消费：再次使用同一套验证码登录，必须被拦截
	{
		loginReq := types.TokenReq{
			TenantID:    "000000",
			Account:     "admin",
			Password:    "admin123",
			CaptchaKey:  resCaptcha.Key,
			CaptchaCode: resCaptcha.Code,
		}
		body, _ := json.Marshal(loginReq)
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Success || resp.Code != xerr.CaptchaError {
			t.Fatalf("replay attack succeeded! expected CaptchaError, got %+v", resp)
		}
	}

	// 8. 测试已冻结用户拦截
	{
		resCap, _ := pkgredis.GenerateCaptcha(context.Background(), time.Minute)
		loginReq := types.TokenReq{
			TenantID:    "000000",
			Account:     "frozen_user",
			Password:    "123456",
			CaptchaKey:  resCap.Key,
			CaptchaCode: resCap.Code,
		}
		body, _ := json.Marshal(loginReq)
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		var resp response.Response[any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if resp.Success || resp.Code != xerr.Forbidden {
			t.Fatalf("expected Forbidden for frozen user, got %+v", resp)
		}
	}

	// 9. 访问受保护路由：未携带 Token 返回真正的 HTTP 401
	{
		req := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/user-info", nil)
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected HTTP 401 Unauthorized for unprotected route, got %d", rec.Code)
		}
	}

	// 10. 访问受保护路由：携带合法 AccessToken 正常放行
	{
		req := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/user-info", nil)
		req.Header.Set("Blade-Auth", tokenResult.AccessToken)
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 with valid access token, got %d", rec.Code)
		}

		var resp response.Response[map[string]any]
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		if !resp.Success || resp.Data["account"] != "admin" || resp.Data["role_name"] != "administrator" {
			t.Fatalf("expected admin user claims with administrator role, got %+v", resp)
		}
	}

	// 11. 刷新令牌换发测试 POST /blade-auth/oauth/refresh-token 及二次重放拦截
	{
		refreshReq := types.RefreshTokenReq{
			RefreshToken: tokenResult.RefreshToken,
		}
		body, _ := json.Marshal(refreshReq)
		req := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/refresh-token", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for refresh-token, got %d, body: %s", rec.Code, rec.Body.String())
		}

		var resp response.Response[jwt.TokenResult]
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal refresh token response failed: %v", err)
		}
		if !resp.Success || resp.Data.AccessToken == "" {
			t.Fatalf("refresh token exchange failed: %+v", resp)
		}

		// 同一个 RefreshToken 第二次重放刷新必须被拦截，返回 xerr.TokenInvalid
		reqReplay := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/refresh-token", bytes.NewReader(body))
		reqReplay.Header.Set("Content-Type", "application/json")
		recReplay := httptest.NewRecorder()
		server.ServeHTTP(recReplay, reqReplay)

		var replayResp response.Response[any]
		_ = json.Unmarshal(recReplay.Body.Bytes(), &replayResp)
		if replayResp.Success || replayResp.Code != xerr.TokenInvalid {
			t.Fatalf("expected replay refresh token to fail with TokenInvalid, got: %+v", replayResp)
		}
	}

	// 12. 验证 camelCase (tenantId, captchaKey, captchaCode, refreshToken) 请求体直连 Auth 同样能登录和刷新成功
	{
		resCapCamel, _ := pkgredis.GenerateCaptcha(context.Background(), time.Minute)
		camelLoginBody := []byte(`{
			"tenantId": "000000",
			"account": "admin",
			"password": "admin123",
			"captchaKey": "` + resCapCamel.Key + `",
			"captchaCode": "` + resCapCamel.Code + `"
		}`)
		reqLogin := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(camelLoginBody))
		reqLogin.Header.Set("Content-Type", "application/json")
		recLogin := httptest.NewRecorder()
		server.ServeHTTP(recLogin, reqLogin)

		var loginResp response.Response[jwt.TokenResult]
		if err := json.Unmarshal(recLogin.Body.Bytes(), &loginResp); err != nil {
			t.Fatalf("unmarshal camelCase login response failed: %v", err)
		}
		if !loginResp.Success || loginResp.Data.AccessToken == "" || loginResp.Data.RefreshToken == "" {
			t.Fatalf("camelCase login failed: %+v", loginResp)
		}

		camelRefreshBody := []byte(`{"refreshToken": "` + loginResp.Data.RefreshToken + `"}`)
		reqRefresh := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/refresh-token", bytes.NewReader(camelRefreshBody))
		reqRefresh.Header.Set("Content-Type", "application/json")
		recRefresh := httptest.NewRecorder()
		server.ServeHTTP(recRefresh, reqRefresh)

		var refreshResp response.Response[jwt.TokenResult]
		if err := json.Unmarshal(recRefresh.Body.Bytes(), &refreshResp); err != nil {
			t.Fatalf("unmarshal camelCase refresh response failed: %v", err)
		}
		if !refreshResp.Success || refreshResp.Data.AccessToken == "" {
			t.Fatalf("camelCase refresh token failed: %+v", refreshResp)
		}

		// 13. 测试 Auth 服务主动登出吊销闭环：携带 Bearer Token 调用 POST /blade-auth/oauth/logout -> 再次访问 /blade-auth/oauth/user-info 返回 HTTP 401
		activeAccessToken := refreshResp.Data.AccessToken
		reqLogout := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/logout", nil)
		reqLogout.Header.Set("Authorization", "Bearer "+activeAccessToken)
		recLogout := httptest.NewRecorder()
		server.ServeHTTP(recLogout, reqLogout)
		if recLogout.Code != http.StatusOK {
			t.Fatalf("expected 200 on logout, got %d: %s", recLogout.Code, recLogout.Body.String())
		}
		var logoutResp response.Response[types.ChallengeRedirectResp]
		if err := json.Unmarshal(recLogout.Body.Bytes(), &logoutResp); err != nil {
			t.Fatalf("unmarshal logout response failed: %v", err)
		}
		if !logoutResp.Success || !logoutResp.Data.Revoked {
			t.Fatalf("expected revoked=true on logout, got %+v", logoutResp)
		}

		reqAfterLogout := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/user-info", nil)
		reqAfterLogout.Header.Set("Authorization", "Bearer "+activeAccessToken)
		recAfterLogout := httptest.NewRecorder()
		server.ServeHTTP(recAfterLogout, reqAfterLogout)
		if recAfterLogout.Code != http.StatusUnauthorized {
			t.Fatalf("expected HTTP 401 after token logout revocation, got %d", recAfterLogout.Code)
		}
	}
}
