// Package test 提供网关与下游 Auth、System、Desk 四大微服务无 Mock 全栈内存端到端联调测试。
package test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/rest"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	authv1 "github.com/go-blade/api/proto/auth/v1"
	deskv1 "github.com/go-blade/api/proto/desk/v1"
	systemv1 "github.com/go-blade/api/proto/system/v1"
	"github.com/go-blade/pkg/database"
	"github.com/go-blade/pkg/middleware"
	"github.com/go-blade/pkg/mq"
	pkgredis "github.com/go-blade/pkg/redis"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"github.com/go-blade/pkg/xerr"

	authtestkit "github.com/go-blade/services/auth/testkit"
	desktestkit "github.com/go-blade/services/desk/testkit"
	gwcfg "github.com/go-blade/services/gateway/internal/config"
	gwhandler "github.com/go-blade/services/gateway/internal/handler"
	gwsvc "github.com/go-blade/services/gateway/internal/svc"
	systestkit "github.com/go-blade/services/system/testkit"
)

// TestFullStack_FourServices_RealE2E 在进程内真实拉起 Gateway + Auth + System + Desk 四个微服务（无任何 Mock 桩），
// 共享 SQLite 内存数据库（含 TenantPlugin/AuditPlugin/DataScopePlugin）、Miniredis 与内存 MQ Broker，
// 端到端验证认证全周期、RBAC 权限动态回收与 MQ 广播缓存实时失效、敏感字段脱敏异步审计落库、多租户隔离与通知公告 SSE 实时流推送。
func TestFullStack_FourServices_RealE2E(t *testing.T) {
	// 1. 初始化 OpenTelemetry 追踪器
	tpHolder, err := trace.InitTracer(trace.Config{ServiceName: "fullstack-e2e-test", Sampler: 1.0})
	require.NoError(t, err)
	defer func() { _ = tpHolder.Shutdown(context.Background()) }()

	// 2. 启动 Miniredis 并绑定到全局 pkg/redis（供验证码、Token 刷新防重放、黑名单吊销、分布式锁与缓存使用）
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient := zeroredis.MustNewRedis(zeroredis.RedisConf{
		Host: mr.Addr(),
		Type: zeroredis.NodeType,
	})
	pkgredis.SetRedis(redisClient)
	defer pkgredis.SetRedis(nil)

	// 3. 初始化共享 SQLite 内存数据库，挂载多租户隔离、自动审计与 DataScope 行级数据权限三大插件
	dsn := "file:fullstack_e2e_" + strings.ReplaceAll(t.Name(), "/", "_") + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Use(tenant.NewTenantPlugin()))
	require.NoError(t, db.Use(database.NewAuditPlugin()))
	require.NoError(t, db.Use(database.NewDataScopePlugin()))

	// 4. 创建共享的内存 MQ Broker（串联 Gateway <-> System 审计日志落库 & RBAC 缓存失效广播 & Desk 公告实时推送）
	sharedBroker := mq.NewMemoryBroker(mq.Config{
		Driver:      mq.DriverMemory,
		ServiceName: "fullstack-e2e-broker",
		Workers:     4,
		BufferSize:  512,
	})
	defer func() { _ = sharedBroker.Close(context.Background()) }()

	// 5. 使用各服务 testkit 构建真实的 Auth、System、Desk gRPC Server 实例并自动迁移全表
	jwtSecret := "fullstack-e2e-shared-jwt-secret-key-32b"

	realAuthServer, err := authtestkit.NewEmbeddedAuthServer(db, redisClient, jwtSecret)
	require.NoError(t, err)

	realSystemServer, err := systestkit.NewEmbeddedSystemServer(db, sharedBroker)
	require.NoError(t, err)

	realDeskServer, err := desktestkit.NewEmbeddedDeskServer(db, sharedBroker)
	require.NoError(t, err)

	// 6. 写入与 deploy/sql/02_seed_data.sql 一致的初始种子数据
	seedFullStackDatabase(t, db)

	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer grpcLis.Close()

	grpcServer := pkgrpc.NewServer()
	authv1.RegisterAuthServiceServer(grpcServer, realAuthServer)
	systemv1.RegisterSystemServiceServer(grpcServer, realSystemServer)
	deskv1.RegisterDeskServiceServer(grpcServer, realDeskServer)

	go func() {
		_ = grpcServer.Serve(grpcLis)
	}()
	defer grpcServer.Stop()

	// 7. 启动真实的 Gateway HTTP 服务，通过 gRPC 连接上述真实微服务
	gwConfig := gwcfg.Config{
		RestConf: rest.RestConf{
			Host: "127.0.0.1",
			Port: 0,
		},
		Auth: gwcfg.AuthConfig{
			AccessSecret: jwtSecret,
			Whitelist: []string{
				"/ping",
				"/blade-auth/oauth/captcha",
				"/blade-auth/oauth/token",
				"/blade-auth/oauth/refresh-token",
			},
		},
		Upstreams: gwcfg.UpstreamConfig{
			Auth:   pkgrpc.ClientConfig{Target: grpcLis.Addr().String()},
			System: pkgrpc.ClientConfig{Target: grpcLis.Addr().String()},
			Desk:   pkgrpc.ClientConfig{Target: grpcLis.Addr().String()},
		},
	}

	gwSvcCtx := gwsvc.NewServiceContext(gwConfig, sharedBroker)
	defer gwSvcCtx.Close()

	gwServer := rest.MustNewServer(gwConfig.RestConf)
	defer gwServer.Stop()
	gwhandler.RegisterHandlers(gwServer, gwSvcCtx)

	// 辅助函数：通过网关获取图形验证码并从 Miniredis 提取真实答案后登录
	loginViaGateway := func(t *testing.T, tenantID, account, password string) (accessToken, refreshToken string) {
		t.Helper()
		// Step A: GET /blade-auth/oauth/captcha
		reqCap := httptest.NewRequest(http.MethodGet, "/blade-auth/oauth/captcha", nil)
		reqCap.Header.Set(tenant.HeaderTenantID, tenantID)
		wCap := httptest.NewRecorder()
		gwServer.ServeHTTP(wCap, reqCap)
		require.Equal(t, http.StatusOK, wCap.Code)

		var capResp struct {
			Code int               `json:"code"`
			Data map[string]string `json:"data"`
		}
		require.NoError(t, json.Unmarshal(wCap.Body.Bytes(), &capResp))
		require.Equal(t, xerr.OK, capResp.Code)
		captchaKey := capResp.Data["captcha_key"]
		require.NotEmpty(t, captchaKey)
		assert.True(t, strings.HasPrefix(capResp.Data["image"], "data:image/png;base64,"))

		// 从 Miniredis 读取真实生成的 4 位验证码
		captchaCode, err := mr.Get(pkgredis.CaptchaKeyPrefix + captchaKey)
		require.NoError(t, err)
		require.Len(t, captchaCode, 4)

		// Step B: POST /blade-auth/oauth/token
		loginPayload, _ := json.Marshal(map[string]string{
			"tenant_id":    tenantID,
			"account":      account,
			"password":     password,
			"captcha_key":  captchaKey,
			"captcha_code": captchaCode,
		})
		reqLogin := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/token", bytes.NewReader(loginPayload))
		reqLogin.Header.Set("Content-Type", "application/json")
		reqLogin.Header.Set(tenant.HeaderTenantID, tenantID)
		wLogin := httptest.NewRecorder()
		gwServer.ServeHTTP(wLogin, reqLogin)
		require.Equal(t, http.StatusOK, wLogin.Code)

		var loginResp struct {
			Code int `json:"code"`
			Data struct {
				AccessToken  string `json:"access_token"`
				RefreshToken string `json:"refresh_token"`
			} `json:"data"`
			Msg string `json:"msg"`
		}
		require.NoError(t, json.Unmarshal(wLogin.Body.Bytes(), &loginResp))
		require.Equal(t, xerr.OK, loginResp.Code, "login failed: %s", loginResp.Msg)
		require.NotEmpty(t, loginResp.Data.AccessToken)
		require.NotEmpty(t, loginResp.Data.RefreshToken)

		// 验证验证码已被 Lua 脚本原子销毁（防重放）
		assert.False(t, mr.Exists(pkgredis.CaptchaKeyPrefix+captchaKey), "验证码消费后应立即从 Redis 删除")

		return loginResp.Data.AccessToken, loginResp.Data.RefreshToken
	}

	// =========================================================================
	// 场景 1：真实认证全链路（获取验证码 -> 种子账号登录 -> 查询用户与路由 -> 刷新令牌防重放）
	// =========================================================================
	var adminAccessToken string
	t.Run("1_SeedAdmin_CaptchaLogin_Refresh_AntiReplay", func(t *testing.T) {
		accessTok, refreshTok := loginViaGateway(t, "000000", "admin", "admin123")

		// 用签发的 AccessToken 查询当前登录人信息 GET /blade-system/user/info
		reqInfo := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
		reqInfo.Header.Set(middleware.HeaderBladeAuth, "bearer "+accessTok)
		reqInfo.Header.Set(tenant.HeaderTenantID, "000000")
		wInfo := httptest.NewRecorder()
		gwServer.ServeHTTP(wInfo, reqInfo)
		require.Equal(t, http.StatusOK, wInfo.Code)
		assert.Contains(t, wInfo.Body.String(), `"account":"admin"`)
		assert.Contains(t, wInfo.Body.String(), `"1123598821738675201"`) // 64 位雪花 ID 字符串精度无损

		// 刷新 Token：第一次成功，第二次拿旧 RefreshToken 重放必须拦截
		refreshBody, _ := json.Marshal(map[string]string{"refresh_token": refreshTok})
		reqRef1 := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/refresh-token", bytes.NewReader(refreshBody))
		reqRef1.Header.Set("Content-Type", "application/json")
		wRef1 := httptest.NewRecorder()
		gwServer.ServeHTTP(wRef1, reqRef1)
		require.Equal(t, http.StatusOK, wRef1.Code)

		var ref1Resp struct {
			Code int `json:"code"`
			Data struct {
				AccessToken  string `json:"access_token"`
				RefreshToken string `json:"refresh_token"`
			} `json:"data"`
		}
		require.NoError(t, json.Unmarshal(wRef1.Body.Bytes(), &ref1Resp))
		require.Equal(t, xerr.OK, ref1Resp.Code)
		adminAccessToken = ref1Resp.Data.AccessToken

		// 重放同一个旧 RefreshToken 必须返回 40002 TokenInvalid
		reqRef2 := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/refresh-token", bytes.NewReader(refreshBody))
		reqRef2.Header.Set("Content-Type", "application/json")
		wRef2 := httptest.NewRecorder()
		gwServer.ServeHTTP(wRef2, reqRef2)
		var ref2Resp struct {
			Code int `json:"code"`
		}
		require.NoError(t, json.Unmarshal(wRef2.Body.Bytes(), &ref2Resp))
		assert.Equal(t, xerr.TokenInvalid, ref2Resp.Code, "旧 RefreshToken 重放必须被拒绝")
	})

	// =========================================================================
	// 场景 2：RBAC 细粒度权限控制 + 角色授权变更通过 MQ 广播实时失效网关权限缓存
	// =========================================================================
	t.Run("2_RBAC_DynamicGrantAndRevoke_With_MQCacheInvalidation", func(t *testing.T) {
		// 普通用户 staff_user 登录（所属角色 1123598816738675202 初始仅有 notice_view，无 dept:add 权限）
		staffToken, _ := loginViaGateway(t, "000000", "staff_user", "staff123")

		// 1. 尝试调用需要 dept:add 权限的 POST /blade-system/dept/submit -> 应返回 403 Forbidden
		deptPayload, _ := json.Marshal(map[string]any{
			"parent_id": "1123598813738675201",
			"dept_name": "测试中心",
			"full_name": "质量保障测试中心",
			"sort":      3,
		})
		reqAddDept1 := httptest.NewRequest(http.MethodPost, "/blade-system/dept/submit", bytes.NewReader(deptPayload))
		reqAddDept1.Header.Set("Content-Type", "application/json")
		reqAddDept1.Header.Set(middleware.HeaderBladeAuth, "bearer "+staffToken)
		wAddDept1 := httptest.NewRecorder()
		gwServer.ServeHTTP(wAddDept1, reqAddDept1)
		assert.Equal(t, http.StatusForbidden, wAddDept1.Code, "未授权 dept:add 前应拦截返回 403")

		// 1b. 普通员工尝试越权调用租户管理、系统参数修改、公告发布或删除接口 -> 均应被网关 RBAC 拦截返回 403 Forbidden
		protectedEndpoints := []struct {
			path string
			body string
		}{
			{"/blade-system/tenant/submit", `{"tenant_name":"非法租户"}`},
			{"/blade-system/tenant/remove", `{"ids":["1123598811738675201"]}`},
			{"/blade-system/param/submit", `{"param_name":"p","param_key":"k","param_value":"v"}`},
			{"/blade-system/param/remove", `{"ids":["1"]}`},
			{"/blade-desk/notice/submit", `{"title":"越权公告"}`},
			{"/blade-desk/notice/remove", `{"ids":["1"]}`},
		}
		for _, ep := range protectedEndpoints {
			reqForbidden := httptest.NewRequest(http.MethodPost, ep.path, strings.NewReader(ep.body))
			reqForbidden.Header.Set("Content-Type", "application/json")
			reqForbidden.Header.Set(middleware.HeaderBladeAuth, "bearer "+staffToken)
			wForbidden := httptest.NewRecorder()
			gwServer.ServeHTTP(wForbidden, reqForbidden)
			assert.Equal(t, http.StatusForbidden, wForbidden.Code, "普通员工越权调用 %s 必须返回 403", ep.path)
		}

		// 2. 超级管理员调用 POST /blade-system/role/grant 为普通角色授予 dept:add 按钮菜单 (1123598815738675316)
		// System 服务更新数据库后会向 MQ 发布 "blade.system.rbac.changed" 广播事件，网关自动清除 PermCache
		grantPayload, _ := json.Marshal(map[string]any{
			"role_id":  "1123598816738675202",
			"menu_ids": []string{"1123598815738675100", "1123598815738675316"},
		})
		reqGrant := httptest.NewRequest(http.MethodPost, "/blade-system/role/grant", bytes.NewReader(grantPayload))
		reqGrant.Header.Set("Content-Type", "application/json")
		reqGrant.Header.Set(middleware.HeaderBladeAuth, "bearer "+adminAccessToken)
		wGrant := httptest.NewRecorder()
		gwServer.ServeHTTP(wGrant, reqGrant)
		require.Equal(t, http.StatusOK, wGrant.Code)

		// 等待 MQ 广播清空网关缓存，普通用户再次提交新增部门应立即 200 成功
		require.Eventually(t, func() bool {
			reqRetry := httptest.NewRequest(http.MethodPost, "/blade-system/dept/submit", bytes.NewReader(deptPayload))
			reqRetry.Header.Set("Content-Type", "application/json")
			reqRetry.Header.Set(middleware.HeaderBladeAuth, "bearer "+staffToken)
			wRetry := httptest.NewRecorder()
			gwServer.ServeHTTP(wRetry, reqRetry)
			if wRetry.Code != http.StatusOK {
				return false
			}
			var r struct {
				Code int `json:"code"`
			}
			_ = json.Unmarshal(wRetry.Body.Bytes(), &r)
			return r.Code == xerr.OK
		}, 2*time.Second, 20*time.Millisecond, "角色授权后网关 RBAC 缓存应通过 MQ 广播实时失效并放行请求")

		// 3. 超级管理员回收该角色的 dept:add 权限 -> 普通用户再次请求应立即恢复 403 拦截
		revokePayload, _ := json.Marshal(map[string]any{
			"role_id":  "1123598816738675202",
			"menu_ids": []string{"1123598815738675100"},
		})
		reqRevoke := httptest.NewRequest(http.MethodPost, "/blade-system/role/grant", bytes.NewReader(revokePayload))
		reqRevoke.Header.Set("Content-Type", "application/json")
		reqRevoke.Header.Set(middleware.HeaderBladeAuth, "bearer "+adminAccessToken)
		wRevoke := httptest.NewRecorder()
		gwServer.ServeHTTP(wRevoke, reqRevoke)
		require.Equal(t, http.StatusOK, wRevoke.Code)

		require.Eventually(t, func() bool {
			reqAfterRevoke := httptest.NewRequest(http.MethodPost, "/blade-system/dept/submit", bytes.NewReader(deptPayload))
			reqAfterRevoke.Header.Set("Content-Type", "application/json")
			reqAfterRevoke.Header.Set(middleware.HeaderBladeAuth, "bearer "+staffToken)
			wAfterRevoke := httptest.NewRecorder()
			gwServer.ServeHTTP(wAfterRevoke, reqAfterRevoke)
			return wAfterRevoke.Code == http.StatusForbidden
		}, 2*time.Second, 20*time.Millisecond, "回收角色权限后网关应实时拦截返回 403")
	})

	// =========================================================================
	// 场景 3：敏感字段脱敏 + MQ 异步审计日志落盘至 System 数据库查询闭环
	// =========================================================================
	t.Run("3_AuditLog_Desensitization_MQPersistence_And_QueryClosedLoop", func(t *testing.T) {
		// 通过网关新增一名用户（携带明文 password 和 phone）
		newUserPayload, _ := json.Marshal(map[string]any{
			"account":   "e2e_audited_user",
			"password":  "SuperSecretPass!2026",
			"name":      "审计测试员",
			"real_name": "李四",
			"email":     "lisi@goblade.io",
			"phone":     "13812345678",
			"dept_id":   "1123598813738675201",
			"role_id":   "1123598816738675202",
			"status":    1,
		})
		reqCreateUser := httptest.NewRequest(http.MethodPost, "/blade-system/user/submit", bytes.NewReader(newUserPayload))
		reqCreateUser.Header.Set("Content-Type", "application/json")
		reqCreateUser.Header.Set(middleware.HeaderBladeAuth, "bearer "+adminAccessToken)
		wCreateUser := httptest.NewRecorder()
		gwServer.ServeHTTP(wCreateUser, reqCreateUser)
		require.Equal(t, http.StatusOK, wCreateUser.Code)

		// 验证：1. 新用户可以用明文密码 "SuperSecretPass!2026" 正常登录（说明下游收到的 Body 未被脱敏破坏）
		newUserTok, _ := loginViaGateway(t, "000000", "e2e_audited_user", "SuperSecretPass!2026")
		assert.NotEmpty(t, newUserTok)

		// 验证：1b. 再次使用相同账号名创建用户，应被拦截并返回业务错误码 40008 (DataAlreadyExist)
		reqDupUser := httptest.NewRequest(http.MethodPost, "/blade-system/user/submit", bytes.NewReader(newUserPayload))
		reqDupUser.Header.Set("Content-Type", "application/json")
		reqDupUser.Header.Set(middleware.HeaderBladeAuth, "bearer "+adminAccessToken)
		wDupUser := httptest.NewRecorder()
		gwServer.ServeHTTP(wDupUser, reqDupUser)
		var dupResp struct {
			Code int `json:"code"`
		}
		require.NoError(t, json.Unmarshal(wDupUser.Body.Bytes(), &dupResp))
		assert.Equal(t, xerr.DataAlreadyExist, dupResp.Code, "同租户下重复创建相同登录账号必须返回 40008 DataAlreadyExist")

		// 验证：2. 调用 GET /blade-system/log/api/list 查询持久化到 blade_log_api 表的审计日志，验证密码与手机号已脱敏
		require.Eventually(t, func() bool {
			reqAudit := httptest.NewRequest(http.MethodGet, "/blade-system/log/api/list?path=/blade-system/user/submit&current=1&size=10", nil)
			reqAudit.Header.Set(middleware.HeaderBladeAuth, "bearer "+adminAccessToken)
			wAudit := httptest.NewRecorder()
			gwServer.ServeHTTP(wAudit, reqAudit)
			if wAudit.Code != http.StatusOK {
				return false
			}
			bodyStr := wAudit.Body.String()
			return strings.Contains(bodyStr, "e2e_audited_user") &&
				!strings.Contains(bodyStr, "SuperSecretPass!2026") &&
				strings.Contains(bodyStr, "******") &&
				strings.Contains(bodyStr, "138****5678")
		}, 2*time.Second, 20*time.Millisecond, "System 服务应异步消费 MQ 审计日志并持久化脱敏后的请求体")
	})

	// =========================================================================
	// 场景 4：通知公告发布 -> MQ 广播 -> 网关多租户 SSE 实时流推送与跨租户物理隔离
	// =========================================================================
	t.Run("4_DeskNotice_SSE_RealtimePush_And_MultiTenantIsolation", func(t *testing.T) {
		// 租户 100001 管理员登录
		tenantAToken, _ := loginViaGateway(t, "100001", "tenant_a_admin", "admin123")

		// 在网关 SSEHub 注册租户 000000 与租户 100001 的在线订阅客户端
		client000000, unreg0 := gwSvcCtx.SSEHub.Register("000000", 1123598821738675201)
		defer unreg0()
		client100001, unreg1 := gwSvcCtx.SSEHub.Register("100001", 2123598821738675201)
		defer unreg1()

		// 租户 000000 通过网关发布一条新公告 POST /blade-desk/notice/submit
		noticePayload, _ := json.Marshal(map[string]any{
			"title":        "2026双十一系统升级通知",
			"category":     1,
			"release_time": "2026-10-27 23:30:00",
			"content":      "定于今晚凌晨进行平滑升级",
			"status":       1,
		})
		reqNotice := httptest.NewRequest(http.MethodPost, "/blade-desk/notice/submit", bytes.NewReader(noticePayload))
		reqNotice.Header.Set("Content-Type", "application/json")
		reqNotice.Header.Set(middleware.HeaderBladeAuth, "bearer "+adminAccessToken)
		wNotice := httptest.NewRecorder()
		gwServer.ServeHTTP(wNotice, reqNotice)
		require.Equal(t, http.StatusOK, wNotice.Code)

		// 验证：1. 租户 000000 的 SSE 客户端实时收到 notice.published 事件
		select {
		case ev := <-client000000.Events:
			assert.Equal(t, "notice.published", ev.Event)
			formatted := string(ev.Format())
			assert.Contains(t, formatted, "2026双十一系统升级通知")
		case <-time.After(2 * time.Second):
			t.Fatal("租户 000000 SSE 客户端未能在超时时间内收到实时公告推送")
		}

		// 验证：2. 租户 100001 的 SSE 客户端绝对收不到租户 000000 的公告事件
		select {
		case unexpectedEv := <-client100001.Events:
			t.Fatalf("跨租户 SSE 泄露！租户 100001 收到了租户 000000 的事件: %+v", unexpectedEv)
		case <-time.After(80 * time.Millisecond):
			// 符合预期：无跨租户泄露
		}

		// 验证：3. 租户 100001 调用 GET /blade-desk/notice/list 查不到租户 000000 的公告（数据库多租户隔离）
		reqListA := httptest.NewRequest(http.MethodGet, "/blade-desk/notice/list", nil)
		reqListA.Header.Set(middleware.HeaderBladeAuth, "bearer "+tenantAToken)
		wListA := httptest.NewRecorder()
		gwServer.ServeHTTP(wListA, reqListA)
		require.Equal(t, http.StatusOK, wListA.Code)
		assert.NotContains(t, wListA.Body.String(), "2026双十一系统升级通知")

		// 验证：4. 租户 100001 伪造 Header Blade-Tenant-Id: 000000 企图越权访问 -> 网关直接拦截 403
		reqCross := httptest.NewRequest(http.MethodGet, "/blade-desk/notice/list", nil)
		reqCross.Header.Set(middleware.HeaderBladeAuth, "bearer "+tenantAToken)
		reqCross.Header.Set(tenant.HeaderTenantID, "000000")
		wCross := httptest.NewRecorder()
		gwServer.ServeHTTP(wCross, reqCross)
		var crossResp struct {
			Code int `json:"code"`
		}
		require.NoError(t, json.Unmarshal(wCross.Body.Bytes(), &crossResp))
		assert.Equal(t, xerr.Forbidden, crossResp.Code, "伪造跨租户 Header 必须被网关拒绝")
	})

	// =========================================================================
	// 场景 5：登出主动吊销 Token 闭环（POST /blade-auth/oauth/logout -> 再次访问返回 401）
	// =========================================================================
	t.Run("5_Logout_TokenBlacklist_Revocation", func(t *testing.T) {
		reqLogout := httptest.NewRequest(http.MethodPost, "/blade-auth/oauth/logout", bytes.NewReader([]byte(`{}`)))
		reqLogout.Header.Set("Content-Type", "application/json")
		reqLogout.Header.Set(middleware.HeaderBladeAuth, "bearer "+adminAccessToken)
		wLogout := httptest.NewRecorder()
		gwServer.ServeHTTP(wLogout, reqLogout)
		require.Equal(t, http.StatusOK, wLogout.Code)
		assert.Contains(t, wLogout.Body.String(), `"revoked":true`)

		// 携带已登出的 Token 再次访问受保护接口必须返回 HTTP 401
		reqAfter := httptest.NewRequest(http.MethodGet, "/blade-system/user/info", nil)
		reqAfter.Header.Set(middleware.HeaderBladeAuth, "bearer "+adminAccessToken)
		wAfter := httptest.NewRecorder()
		gwServer.ServeHTTP(wAfter, reqAfter)
		assert.Equal(t, http.StatusUnauthorized, wAfter.Code, "已登出吊销的 Token 再次请求应返回 401")
	})
}

// seedFullStackDatabase 向内存数据库中注入与 deploy/sql/02_seed_data.sql 一致的种子数据及多租户测试数据。
func seedFullStackDatabase(t *testing.T, db *gorm.DB) {
	t.Helper()

	// 1. 租户
	require.NoError(t, db.Exec(`INSERT INTO blade_tenant (id, tenant_id, tenant_name, linkman, contact_number, status, is_deleted) VALUES
		(1123598811738675201, '000000', '管理组', '系统管理员', '18888888888', 1, 0),
		(2123598811738675201, '100001', '企业租户A', '张经理', '13900000001', 1, 0)`).Error)

	// 2. 角色（超管与普通员工）
	require.NoError(t, db.Exec(`INSERT INTO blade_role (id, tenant_id, parent_id, role_name, role_alias, data_scope_type, sort, is_deleted) VALUES
		(1123598816738675201, '000000', 0, '超级管理员', 'administrator', 1, 1, 0),
		(1123598816738675202, '000000', 0, '普通用户', 'user', 3, 2, 0),
		(2123598816738675201, '100001', 0, '租户A管理员', 'administrator', 1, 1, 0)`).Error)

	// 3. 部门
	require.NoError(t, db.Exec(`INSERT INTO blade_dept (id, tenant_id, parent_id, dept_name, full_name, sort, is_deleted) VALUES
		(1123598813738675201, '000000', 0, '总部', 'go-blade 集团总部', 1, 0)`).Error)

	// 4. 用户（使用 02_seed_data.sql 中的真实 bcrypt 哈希密码）
	adminHash := "$2a$10$jcV1t6Hr03Ma0wddRitEPOBkG8cI6LeDI1jOGlmydT6HmyGKudPDa" // admin123
	staffHashBytes, err := bcrypt.GenerateFromPassword([]byte("staff123"), bcrypt.DefaultCost)
	require.NoError(t, err)
	staffHash := string(staffHashBytes)

	require.NoError(t, db.Exec(`INSERT INTO blade_user (id, tenant_id, account, password, name, real_name, dept_id, role_id, status, is_deleted) VALUES
		(1123598821738675201, '000000', 'admin', ?, '管理员', '系统管理员', 1123598813738675201, '1123598816738675201', 1, 0),
		(1123598821738675202, '000000', 'staff_user', ?, '普通员工', '王五', 1123598813738675201, '1123598816738675202', 1, 0),
		(2123598821738675201, '100001', 'tenant_a_admin', ?, '租户A管理员', '张经理', 0, '2123598816738675201', 1, 0)`,
		adminHash, staffHash, adminHash).Error)

	// 5. 菜单与按钮权限
	require.NoError(t, db.Exec(`INSERT INTO blade_menu (id, parent_id, code, name, path, category, action, sort, is_deleted) VALUES
		(1123598815738675100, 0, 'desk', '工作台', '/desk', 1, '', 1, 0),
		(1123598815738675200, 0, 'system', '系统管理', '/system', 1, '', 2, 0),
		(1123598815738675313, 1123598815738675100, 'notice_view', '公告查看', '', 2, 'desk:notice:view', 1, 0),
		(1123598815738675316, 1123598815738675200, 'gw_dept_add', '部门提交', '', 2, 'dept:add', 2, 0)`).Error)

	// 初始普通角色仅绑定 desk 与 notice_view
	require.NoError(t, db.Exec(`INSERT INTO blade_role_menu (id, role_id, menu_id) VALUES
		(1123598817738675022, 1123598816738675202, 1123598815738675100),
		(1123598817738675024, 1123598816738675202, 1123598815738675313)`).Error)
}
