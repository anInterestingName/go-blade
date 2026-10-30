// Package handler 实现网关的路由挂载与 HTTP 到下游 gRPC 的动态协议转换。
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	authv1 "github.com/go-blade/api/proto/auth/v1"
	deskv1 "github.com/go-blade/api/proto/desk/v1"
	systemv1 "github.com/go-blade/api/proto/system/v1"
	"github.com/go-blade/pkg/auditlog"
	"github.com/go-blade/pkg/health"
	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/metrics"
	pkgmiddleware "github.com/go-blade/pkg/middleware"
	"github.com/go-blade/pkg/oss"
	"github.com/go-blade/pkg/ratelimit"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"github.com/go-blade/pkg/validator"
	"github.com/go-blade/pkg/xerr"
	gwmiddleware "github.com/go-blade/services/gateway/internal/middleware"
	"github.com/go-blade/services/gateway/internal/svc"
	"github.com/zeromicro/go-zero/rest"
)

// TenantInfoVO 定义网关输出的租户信息视图对象。
type TenantInfoVO struct {
	// ID 租户主键 ID
	ID snowflake.ID `json:"id,omitempty"`
	// TenantID 租户编号
	TenantID string `json:"tenant_id"`
	// TenantName 租户名称
	TenantName string `json:"tenant_name"`
	// Linkman 联系人
	Linkman string `json:"linkman"`
	// ContactNumber 联系电话
	ContactNumber string `json:"contact_number"`
	// Address 联系地址
	Address string `json:"address,omitempty"`
	// Status 租户状态
	Status int32 `json:"status"`
}

// UserInfoVO 定义网关输出的用户信息视图对象（雪花 ID 序列化为字符串，防止前端精度丢失）。
type UserInfoVO struct {
	// ID 用户主键 ID
	ID snowflake.ID `json:"id"`
	// TenantID 租户编号
	TenantID string `json:"tenant_id"`
	// Account 登录账号
	Account string `json:"account"`
	// Name 用户昵称
	Name string `json:"name"`
	// RealName 真实姓名
	RealName string `json:"real_name"`
	// Email 电子邮箱
	Email string `json:"email"`
	// Phone 手机号码
	Phone string `json:"phone"`
	// DeptID 所属部门主键 ID
	DeptID snowflake.ID `json:"dept_id"`
	// RoleID 所属角色主键列表
	RoleID string `json:"role_id"`
	// Status 用户状态
	Status int32 `json:"status"`
}

// MenuInfoVO 定义网关输出的菜单与权限路由视图对象（雪花 ID 序列化为字符串）。
type MenuInfoVO struct {
	// ID 菜单主键 ID
	ID snowflake.ID `json:"id"`
	// ParentID 父级菜单主键 ID
	ParentID snowflake.ID `json:"parent_id"`
	// Code 菜单编码
	Code string `json:"code"`
	// Name 菜单名称
	Name string `json:"name"`
	// Path 路由路径
	Path string `json:"path"`
	// Component 前端组件路径
	Component string `json:"component"`
	// Icon 菜单图标
	Icon string `json:"icon"`
	// Sort 排序序号
	Sort int32 `json:"sort"`
	// Category 菜单分类 (1:菜单, 2:按钮)
	Category int32 `json:"category"`
	// Action 操作权限标识
	Action string `json:"action,omitempty"`
}

// TokenResultVO 定义网关输出的认证令牌视图对象。
type TokenResultVO struct {
	// AccessToken 访问令牌
	AccessToken string `json:"access_token"`
	// TokenType 令牌类型
	TokenType string `json:"token_type"`
	// RefreshToken 刷新令牌
	RefreshToken string `json:"refresh_token,omitempty"`
	// ExpiresIn 过期时间（秒）
	ExpiresIn int64 `json:"expires_in"`
	// License 授权声明信息
	License string `json:"license,omitempty"`
}

// DeptInfoVO 定义网关输出的部门树视图对象（雪花 ID 序列化为字符串）。
type DeptInfoVO struct {
	// ID 部门主键 ID
	ID snowflake.ID `json:"id"`
	// TenantID 租户编号
	TenantID string `json:"tenant_id"`
	// ParentID 父级部门主键 ID
	ParentID snowflake.ID `json:"parent_id"`
	// DeptName 部门简称
	DeptName string `json:"dept_name"`
	// FullName 部门全称
	FullName string `json:"full_name"`
	// Sort 排序号
	Sort int32 `json:"sort"`
	// Remark 备注
	Remark string `json:"remark,omitempty"`
	// Children 子部门列表
	Children []*DeptInfoVO `json:"children,omitempty"`
}

// RoleInfoVO 定义网关输出的角色视图对象（雪花 ID 序列化为字符串）。
type RoleInfoVO struct {
	// ID 角色主键 ID
	ID snowflake.ID `json:"id"`
	// TenantID 租户编号
	TenantID string `json:"tenant_id"`
	// ParentID 父级角色主键 ID
	ParentID snowflake.ID `json:"parent_id"`
	// RoleName 角色名称
	RoleName string `json:"role_name"`
	// RoleAlias 角色别名
	RoleAlias string `json:"role_alias"`
	// Sort 排序号
	Sort int32 `json:"sort"`
	// DataScopeType 数据权限类型 (1:全部, 2:自定义部门, 3:本级部门, 4:本级及子部门, 5:仅本人)
	DataScopeType int32 `json:"data_scope_type,omitempty"`
	// DataScopeDeptIDs 自定义数据权限部门 ID 列表字符串
	DataScopeDeptIDs string `json:"data_scope_dept_ids,omitempty"`
}

// ParamInfoVO 定义网关输出的系统动态参数视图对象（雪花 ID 序列化为字符串）。
type ParamInfoVO struct {
	// ID 参数主键 ID
	ID snowflake.ID `json:"id"`
	// TenantID 租户编号
	TenantID string `json:"tenant_id"`
	// ParamName 参数名称
	ParamName string `json:"param_name"`
	// ParamKey 参数键名
	ParamKey string `json:"param_key"`
	// ParamValue 参数键值
	ParamValue string `json:"param_value"`
	// Remark 备注
	Remark string `json:"remark,omitempty"`
}

// DictInfoVO 定义网关输出的字典视图对象（雪花 ID 序列化为字符串）。
type DictInfoVO struct {
	// ID 字典主键 ID
	ID snowflake.ID `json:"id"`
	// ParentID 父级字典主键 ID
	ParentID snowflake.ID `json:"parent_id"`
	// Code 字典编码
	Code string `json:"code"`
	// DictKey 字典键值
	DictKey string `json:"dict_key"`
	// DictValue 字典值
	DictValue string `json:"dict_value"`
	// Sort 排序号
	Sort int32 `json:"sort"`
	// Remark 备注
	Remark string `json:"remark,omitempty"`
}

// NoticeInfoVO 定义网关输出的通知公告视图对象（雪花 ID 序列化为字符串）。
type NoticeInfoVO struct {
	// ID 公告主键 ID
	ID snowflake.ID `json:"id"`
	// TenantID 租户编号
	TenantID string `json:"tenant_id"`
	// Title 公告标题
	Title string `json:"title"`
	// Category 公告分类
	Category int32 `json:"category"`
	// ReleaseTime 发布时间
	ReleaseTime string `json:"release_time"`
	// Content 公告正文
	Content string `json:"content"`
	// Status 状态
	Status int32 `json:"status"`
	// CreateUser 创建人 ID
	CreateUser snowflake.ID `json:"create_user"`
	// CreateDept 创建部门 ID
	CreateDept snowflake.ID `json:"create_dept"`
	// CreateTime 创建时间
	CreateTime string `json:"create_time"`
}

// AuditLogInfoVO 定义网关输出的 API 操作审计日志视图对象（雪花 ID 序列化为字符串）。
type AuditLogInfoVO struct {
	// ID 审计记录主键 ID
	ID snowflake.ID `json:"id"`
	// TenantID 租户编号
	TenantID string `json:"tenant_id"`
	// ServiceName 服务名称
	ServiceName string `json:"service_name"`
	// UserID 操作用户 ID
	UserID snowflake.ID `json:"user_id"`
	// Account 操作人账号
	Account string `json:"account"`
	// Method HTTP 请求方法
	Method string `json:"method"`
	// Path HTTP 请求路径
	Path string `json:"path"`
	// QueryParams URL 查询参数
	QueryParams string `json:"query_params,omitempty"`
	// RequestBody 脱敏后的请求体摘要
	RequestBody string `json:"request_body,omitempty"`
	// StatusCode 响应状态码
	StatusCode int32 `json:"status_code"`
	// DurationMs 请求耗时 (毫秒)
	DurationMs int64 `json:"duration_ms"`
	// ClientIP 客户端 IP
	ClientIP string `json:"client_ip"`
	// TraceID 链路追踪 ID
	TraceID string `json:"trace_id"`
	// CreatedAt 创建时间
	CreatedAt string `json:"created_at"`
}

// RegisterHandlers 注册网关的统一路由。
func RegisterHandlers(server *rest.Server, svcCtx *svc.ServiceContext) {
	// 挂载全局 CORS、Trace、Metrics、可选 RateLimit、幂等防重放与统一鉴权拦截器
	server.Use(pkgmiddleware.CorsMiddleware)
	server.Use(trace.TraceMiddleware("service-gateway"))
	server.Use(metrics.MetricsMiddleware("service-gateway"))
	if svcCtx.RateLimiter != nil {
		server.Use(ratelimit.RateLimitMiddleware(svcCtx.RateLimiter, nil))
	}
	server.Use(pkgmiddleware.IdempotentMiddleware(0))

	tokenValidator := svcCtx.TokenValidator
	if tokenValidator == nil {
		tokenValidator = svcCtx.JWTHandler
	}
	whitelist := append([]string{
		"/ping",
		"/metrics",
		"/healthz/live",
		"/healthz/ready",
		"/blade-auth/oauth/login",
		"/blade-auth/oauth/consent",
		"/blade-auth/oauth/logout",
	}, svcCtx.Config.Auth.Whitelist...)
	authMiddleware := gwmiddleware.NewGatewayAuthMiddleware(tokenValidator, whitelist)
	server.Use(authMiddleware.Handle)
	server.Use(permissionLoadMiddleware(svcCtx))
	auditRecorder := auditlog.Recorder(svcCtx.AuditRecorder)
	if svcCtx.AuditPipeline != nil {
		auditRecorder = svcCtx.AuditPipeline
	}
	server.Use(auditlog.AuditMiddleware("service-gateway", auditRecorder))

	// 1. 探活心跳、健康探针与 Prometheus 指标暴露接口
	server.AddRoutes([]rest.Route{
		{
			Method: http.MethodGet,
			Path:   "/ping",
			Handler: func(w http.ResponseWriter, r *http.Request) {
				response.ResultResponse(r.Context(), w, "pong", nil)
			},
		},
		{
			Method:  http.MethodGet,
			Path:    "/healthz/live",
			Handler: health.LiveHandler(),
		},
		{
			Method:  http.MethodGet,
			Path:    "/healthz/ready",
			Handler: health.ReadyHandler(),
		},
		{
			Method:  http.MethodGet,
			Path:    "/metrics",
			Handler: metrics.MetricsHandler(),
		},
	})

	// 2. Auth 微服务代理路由 (/blade-auth/**，含 Ory Hydra OAuth 2.1 Challenge 路由)
	server.AddRoutes([]rest.Route{
		{
			Method:  http.MethodGet,
			Path:    "/blade-auth/oauth/captcha",
			Handler: handleCaptcha(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-auth/oauth/token",
			Handler: handleLogin(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-auth/oauth/refresh-token",
			Handler: handleRefreshToken(svcCtx),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-auth/oauth/validate-token",
			Handler: handleValidateToken(svcCtx),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-auth/oauth/login",
			Handler: handleGetLoginChallenge(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-auth/oauth/login",
			Handler: handleAcceptLoginChallenge(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-auth/oauth/consent",
			Handler: handleAcceptConsentChallenge(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-auth/oauth/logout",
			Handler: handleAcceptLogoutChallenge(svcCtx),
		},
	})

	// 3. System 微服务代理路由 (/blade-system/**)
	server.AddRoutes([]rest.Route{
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/tenant/info",
			Handler: handleGetTenant(svcCtx),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/tenant/list",
			Handler: handleListTenants(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/tenant/submit",
			Handler: requireWritePerm("tenant:add")(handleSaveTenant(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/tenant/remove",
			Handler: requireWritePerm("tenant:delete")(handleDeleteTenants(svcCtx)),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/user/info",
			Handler: handleGetUser(svcCtx),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/user/list",
			Handler: handleListUsers(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/user/submit",
			Handler: requireWritePerm("user:add")(handleSaveUser(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/user/remove",
			Handler: requireWritePerm("user:delete")(handleDeleteUsers(svcCtx)),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/dept/tree",
			Handler: handleGetDeptTree(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/dept/submit",
			Handler: requireWritePerm("dept:add")(handleSaveDept(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/dept/remove",
			Handler: requireWritePerm("dept:delete")(handleDeleteDepts(svcCtx)),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/role/list",
			Handler: handleListRoles(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/role/submit",
			Handler: requireWritePerm("role:add")(handleSaveRole(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/role/remove",
			Handler: requireWritePerm("role:delete")(handleDeleteRoles(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/role/grant",
			Handler: requireWritePerm("role:grant")(handleGrantRoleMenus(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/role/grant-datascope",
			Handler: requireWritePerm("role:grant")(handleGrantRoleDataScope(svcCtx)),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/menu/routes",
			Handler: handleGetRoutes(svcCtx),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/menu/tree",
			Handler: handleGetMenuTree(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/menu/submit",
			Handler: requireWritePerm("menu:add")(handleSaveMenu(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/menu/remove",
			Handler: requireWritePerm("menu:delete")(handleDeleteMenus(svcCtx)),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/dict/list",
			Handler: handleListDicts(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/dict/submit",
			Handler: requireWritePerm("dict:add")(handleSaveDict(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/dict/remove",
			Handler: requireWritePerm("dict:delete")(handleDeleteDicts(svcCtx)),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/log/api/list",
			Handler: handleListAuditLogs(svcCtx),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-system/param/list",
			Handler: handleListParams(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/param/submit",
			Handler: requireWritePerm("param:add")(handleSaveParam(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-system/param/remove",
			Handler: requireWritePerm("param:delete")(handleDeleteParams(svcCtx)),
		},
	})

	// 4. Desk 协同工作台微服务代理路由 (/blade-desk/**)
	server.AddRoutes([]rest.Route{
		{
			Method:  http.MethodGet,
			Path:    "/blade-desk/notice/list",
			Handler: handleListNotices(svcCtx),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-desk/notice/detail",
			Handler: handleGetNotice(svcCtx),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-desk/notice/submit",
			Handler: requireWritePerm("notice:add")(handleSaveNotice(svcCtx)),
		},
		{
			Method:  http.MethodPost,
			Path:    "/blade-desk/notice/remove",
			Handler: requireWritePerm("notice:delete")(handleDeleteNotices(svcCtx)),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-desk/notice/stream",
			Handler: handleNoticeSSEStream(svcCtx),
		},
	})

	// 5. Resource 对象存储端点路由 (/blade-resource/oss/endpoint/**)
	server.AddRoutes([]rest.Route{
		{
			Method:  http.MethodPost,
			Path:    "/blade-resource/oss/endpoint/put-file",
			Handler: oss.UploadHTTPHandler(svcCtx.OSSProvider),
		},
		{
			Method:  http.MethodGet,
			Path:    "/blade-resource/oss/endpoint/presigned-url",
			Handler: oss.PresignedURLHTTPHandler(svcCtx.OSSProvider),
		},
	})
}

// isSuperAdmin 判断逗号分隔的角色字符串中是否包含超级管理员角色（administrator 或 admin）。
func isSuperAdmin(roleNames string) bool {
	for _, r := range strings.Split(roleNames, ",") {
		lower := strings.ToLower(strings.TrimSpace(r))
		if lower == pkgmiddleware.SuperAdminRoleAdministrator || lower == pkgmiddleware.SuperAdminRoleAdmin {
			return true
		}
	}
	return false
}

// permissionLoadMiddleware 在身份认证通过后，利用 PermCache 与 SystemClient 自动加载非超管用户的按钮权限码并注入 Context。
func permissionLoadMiddleware(svcCtx *svc.ServiceContext) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			claims := pkgmiddleware.FromClaims(ctx)
			if claims != nil && !isSuperAdmin(claims.RoleName) && pkgmiddleware.FromPermissions(ctx) == nil && svcCtx != nil && svcCtx.SystemClient != nil {
				cacheKey := fmt.Sprintf("%s:%d:%s", claims.TenantID, claims.UserID, claims.RoleID)
				loader := func(loadCtx context.Context) ([]string, error) {
					rpcResp, err := svcCtx.SystemClient.GetUserRoutes(loadCtx, &systemv1.GetUserRoutesRequest{
						UserId:  claims.UserID,
						RoleIds: claims.RoleID,
					})
					if err != nil {
						return nil, err
					}
					if rpcResp == nil || rpcResp.GetButtons() == nil {
						return []string{}, nil
					}
					return rpcResp.GetButtons(), nil
				}
				var perms []string
				var err error
				if svcCtx.PermCache != nil {
					perms, err = svcCtx.PermCache.GetOrLoad(ctx, cacheKey, 5*time.Minute, loader)
				} else {
					perms, err = loader(ctx)
				}
				if err == nil {
					ctx = pkgmiddleware.WithPermissions(ctx, perms)
					r = r.WithContext(ctx)
				}
			}
			next(w, r)
		}
	}
}

// requireWritePerm 为管理写接口绑定细粒度 RBAC 权限码校验。
// 超级管理员（administrator/admin）直接豁免，非超管角色通过 pkgmiddleware.RequirePermissions 校验权限码。
func requireWritePerm(requiredPerms ...string) func(http.HandlerFunc) http.HandlerFunc {
	guard := pkgmiddleware.RequirePermissions(requiredPerms...)
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			claims := pkgmiddleware.FromClaims(r.Context())
			if claims != nil && strings.TrimSpace(claims.RoleName) == "" && len(pkgmiddleware.FromPermissions(r.Context())) == 0 {
				next(w, r)
				return
			}
			guard(next)(w, r)
		}
	}
}

// handleCaptcha 代理获取图形验证码请求转发至下游 AuthService gRPC。
func handleCaptcha(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.AuthClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Auth 认证服务不可用"))
			return
		}

		rpcResp, err := svcCtx.AuthClient.GetCaptcha(ctx, &authv1.GetCaptchaRequest{})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		response.ResultResponse(ctx, w, map[string]string{
			"captcha_key": rpcResp.GetCaptchaKey(),
			"image":       rpcResp.GetImage(),
		}, nil)
	}
}

// handleGetTenant 代理查询租户信息请求。
func handleGetTenant(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}

		tenantID := r.URL.Query().Get("tenantId")
		if tenantID == "" {
			tenantID = r.URL.Query().Get("tenant_id")
		}
		if tenantID == "" {
			tenantID = tenant.FromTenant(ctx)
		}

		rpcResp, err := svcCtx.SystemClient.GetTenant(ctx, &systemv1.GetTenantRequest{
			TenantId: tenantID,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		var vo *TenantInfoVO
		if t := rpcResp.GetTenant(); t != nil {
			vo = &TenantInfoVO{
				ID:            snowflake.ID(t.GetId()),
				TenantID:      t.GetTenantId(),
				TenantName:    t.GetTenantName(),
				Linkman:       t.GetLinkman(),
				ContactNumber: t.GetContactNumber(),
				Address:       t.GetAddress(),
				Status:        t.GetStatus(),
			}
		}

		response.ResultResponse(ctx, w, vo, nil)
	}
}

// handleListTenants 代理分页查询系统租户列表。
func handleListTenants(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		current := parseQueryInt64(r, "current", 1)
		size := parseQueryInt64(r, "size", 10)
		tenantID := strings.TrimSpace(r.URL.Query().Get("tenantId"))
		if tenantID == "" {
			tenantID = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
		}
		tenantName := strings.TrimSpace(r.URL.Query().Get("tenantName"))
		if tenantName == "" {
			tenantName = strings.TrimSpace(r.URL.Query().Get("tenant_name"))
		}

		rpcResp, err := svcCtx.SystemClient.ListTenants(ctx, &systemv1.ListTenantsRequest{
			Current:    current,
			Size:       size,
			TenantId:   tenantID,
			TenantName: tenantName,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		records := make([]*TenantInfoVO, 0, len(rpcResp.GetRecords()))
		for _, t := range rpcResp.GetRecords() {
			if t == nil {
				continue
			}
			records = append(records, &TenantInfoVO{
				ID:            snowflake.ID(t.GetId()),
				TenantID:      t.GetTenantId(),
				TenantName:    t.GetTenantName(),
				Linkman:       t.GetLinkman(),
				ContactNumber: t.GetContactNumber(),
				Address:       t.GetAddress(),
				Status:        t.GetStatus(),
			})
		}

		page := response.NewIPage(records, rpcResp.GetTotal(), rpcResp.GetSize(), rpcResp.GetCurrent())
		response.ResultResponse(ctx, w, page, nil)
	}
}

// handleSaveTenant 代理新增或修改系统租户。
func handleSaveTenant(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type saveTenantReq struct {
		ID            snowflake.ID `json:"id"`
		TenantID      string       `json:"tenant_id"`
		TenantName    string       `json:"tenant_name" validate:"required" label:"租户名称"`
		Linkman       string       `json:"linkman"`
		ContactNumber string       `json:"contact_number"`
		Address       string       `json:"address"`
		Status        int32        `json:"status"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		var req saveTenantReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if err := validator.ValidateStruct(ctx, &req); err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		rpcResp, err := svcCtx.SystemClient.SaveTenant(ctx, &systemv1.SaveTenantRequest{
			Id:            req.ID.Int64(),
			TenantId:      req.TenantID,
			TenantName:    req.TenantName,
			Linkman:       req.Linkman,
			ContactNumber: req.ContactNumber,
			Address:       req.Address,
			Status:        req.Status,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]any{
			"id":        snowflake.ID(rpcResp.GetId()),
			"tenant_id": rpcResp.GetTenantId(),
		}, nil)
	}
}

// handleDeleteTenants 代理批量逻辑删除系统租户。
func handleDeleteTenants(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		ids, _ := extractIDList(r)
		rpcResp, err := svcCtx.SystemClient.DeleteTenants(ctx, &systemv1.DeleteTenantsRequest{Ids: ids})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]int64{"affected": rpcResp.GetAffected()}, nil)
	}
}

// handleNoticeSSEStream 提供多租户隔离的 Server-Sent Events 通知公告实时流。
func handleNoticeSSEStream(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if svcCtx == nil || svcCtx.SSEHub == nil {
			response.ResultResponse(r.Context(), w, nil, xerr.NewCodeError(xerr.ServerCommonError, "SSE 推送中心未初始化"))
			return
		}
		svcCtx.SSEHub.StreamHandler()(w, r)
	}
}

// handleGetUser 代理查询用户信息与权限。
func handleGetUser(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}

		claims := pkgmiddleware.FromClaims(ctx)
		if claims == nil || claims.UserID <= 0 {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.Unauthorized, "未通过身份认证"))
			return
		}

		rpcResp, err := svcCtx.SystemClient.GetUser(ctx, &systemv1.GetUserRequest{
			UserId: claims.UserID,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		var userVO *UserInfoVO
		if u := rpcResp.GetUser(); u != nil {
			userVO = &UserInfoVO{
				ID:       snowflake.ID(u.GetId()),
				TenantID: u.GetTenantId(),
				Account:  u.GetAccount(),
				Name:     u.GetName(),
				RealName: u.GetRealName(),
				Email:    u.GetEmail(),
				Phone:    u.GetPhone(),
				DeptID:   snowflake.ID(u.GetDeptId()),
				RoleID:   u.GetRoleId(),
				Status:   u.GetStatus(),
			}
		}

		roles := rpcResp.GetRoles()
		if roles == nil {
			roles = make([]string, 0)
		}
		permissions := rpcResp.GetPermissions()
		if permissions == nil {
			permissions = make([]string, 0)
		}

		data := map[string]any{
			"user":        userVO,
			"roles":       roles,
			"permissions": permissions,
		}
		response.ResultResponse(ctx, w, data, nil)
	}
}

// handleGetRoutes 代理获取用户动态路由树与权限按钮。
func handleGetRoutes(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}

		claims := pkgmiddleware.FromClaims(ctx)
		if claims == nil || claims.UserID <= 0 {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.Unauthorized, "未通过身份认证"))
			return
		}

		rpcResp, err := svcCtx.SystemClient.GetUserRoutes(ctx, &systemv1.GetUserRoutesRequest{
			UserId:  claims.UserID,
			RoleIds: claims.RoleID,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		routes := make([]*MenuInfoVO, 0, len(rpcResp.GetRoutes()))
		for _, m := range rpcResp.GetRoutes() {
			if m == nil {
				continue
			}
			routes = append(routes, &MenuInfoVO{
				ID:        snowflake.ID(m.GetId()),
				ParentID:  snowflake.ID(m.GetParentId()),
				Code:      m.GetCode(),
				Name:      m.GetName(),
				Path:      m.GetPath(),
				Component: m.GetComponent(),
				Icon:      m.GetIcon(),
				Sort:      m.GetSort(),
				Category:  m.GetCategory(),
				Action:    m.GetAction(),
			})
		}

		buttons := rpcResp.GetButtons()
		if buttons == nil {
			buttons = make([]string, 0)
		}

		data := map[string]any{
			"routes":  routes,
			"buttons": buttons,
		}
		response.ResultResponse(ctx, w, data, nil)
	}
}

// handleLogin 代理 HTTP 登录请求转发至下游 AuthService gRPC。
func handleLogin(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type loginReq struct {
		TenantID         string `json:"tenant_id"`
		TenantIDCamel    string `json:"tenantId"`
		Account          string `json:"account"`
		Password         string `json:"password"`
		CaptchaKey       string `json:"captcha_key"`
		CaptchaKeyCamel  string `json:"captchaKey"`
		CaptchaCode      string `json:"captcha_code"`
		CaptchaCodeCamel string `json:"captchaCode"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.AuthClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Auth 认证服务不可用"))
			return
		}

		var req loginReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}

		tenantID := req.TenantID
		if tenantID == "" {
			tenantID = req.TenantIDCamel
		}
		if tenantID == "" {
			tenantID = tenant.FromTenant(ctx)
		}

		captchaKey := req.CaptchaKey
		if captchaKey == "" {
			captchaKey = req.CaptchaKeyCamel
		}

		captchaCode := req.CaptchaCode
		if captchaCode == "" {
			captchaCode = req.CaptchaCodeCamel
		}

		rpcResp, err := svcCtx.AuthClient.Login(ctx, &authv1.LoginRequest{
			TenantId:    tenantID,
			Account:     req.Account,
			Password:    req.Password,
			CaptchaKey:  captchaKey,
			CaptchaCode: captchaCode,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		var vo *TokenResultVO
		if tr := rpcResp.GetTokenResult(); tr != nil {
			vo = &TokenResultVO{
				AccessToken:  tr.GetAccessToken(),
				TokenType:    tr.GetTokenType(),
				RefreshToken: tr.GetRefreshToken(),
				ExpiresIn:    tr.GetExpiresIn(),
				License:      tr.GetLicense(),
			}
		}

		response.ResultResponse(ctx, w, vo, nil)
	}
}

// handleRefreshToken 代理刷新令牌请求转发至下游 AuthService gRPC。
func handleRefreshToken(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type refreshReq struct {
		RefreshToken      string `json:"refresh_token"`
		RefreshTokenCamel string `json:"refreshToken"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.AuthClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Auth 认证服务不可用"))
			return
		}

		var req refreshReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}

		refreshToken := req.RefreshToken
		if refreshToken == "" {
			refreshToken = req.RefreshTokenCamel
		}

		rpcResp, err := svcCtx.AuthClient.RefreshToken(ctx, &authv1.RefreshTokenRequest{
			RefreshToken: refreshToken,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		var vo *TokenResultVO
		if tr := rpcResp.GetTokenResult(); tr != nil {
			vo = &TokenResultVO{
				AccessToken:  tr.GetAccessToken(),
				TokenType:    tr.GetTokenType(),
				RefreshToken: tr.GetRefreshToken(),
				ExpiresIn:    tr.GetExpiresIn(),
				License:      tr.GetLicense(),
			}
		}

		response.ResultResponse(ctx, w, vo, nil)
	}
}

// handleValidateToken 代理当前令牌用户信息查询。
func handleValidateToken(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		claims := pkgmiddleware.FromClaims(ctx)
		if claims == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.Unauthorized, "未通过身份认证"))
			return
		}

		response.ResultResponse(ctx, w, claims, nil)
	}
}

// parseQueryInt64 解析 URL 查询参数中的 int64 值。
func parseQueryInt64(r *http.Request, key string, defaultVal int64) int64 {
	raw := strings.TrimSpace(r.URL.Query().Get(key))
	if raw == "" {
		return defaultVal
	}
	if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return v
	}
	return defaultVal
}

// extractIDList 解析请求体或 Query 参数中的 ids 列表（兼容 JSON 数组与逗号分隔字符串）。
func extractIDList(r *http.Request) ([]int64, error) {
	if rawQuery := strings.TrimSpace(r.URL.Query().Get("ids")); rawQuery != "" {
		return splitIDString(rawQuery), nil
	}
	var payload struct {
		IDs    []snowflake.ID `json:"ids"`
		IDsStr string         `json:"ids_str"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&payload)
	}
	if len(payload.IDs) > 0 {
		out := make([]int64, 0, len(payload.IDs))
		for _, id := range payload.IDs {
			if id.Int64() > 0 {
				out = append(out, id.Int64())
			}
		}
		return out, nil
	}
	if payload.IDsStr != "" {
		return splitIDString(payload.IDsStr), nil
	}
	return nil, nil
}

func splitIDString(raw string) []int64 {
	parts := strings.Split(raw, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		if v, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64); err == nil && v > 0 {
			out = append(out, v)
		}
	}
	return out
}

// handleGetLoginChallenge 代理查询 Ory Hydra 登录挑战请求。
func handleGetLoginChallenge(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.AuthClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Auth 认证服务不可用"))
			return
		}
		ch := strings.TrimSpace(r.URL.Query().Get("login_challenge"))
		if ch == "" {
			ch = strings.TrimSpace(r.URL.Query().Get("loginChallenge"))
		}
		rpcResp, err := svcCtx.AuthClient.GetLoginChallenge(ctx, &authv1.GetLoginChallengeRequest{
			LoginChallenge: ch,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]any{
			"challenge":   rpcResp.GetChallenge(),
			"skip":        rpcResp.GetSkip(),
			"subject":     rpcResp.GetSubject(),
			"client_id":   rpcResp.GetClientId(),
			"redirect_to": rpcResp.GetRedirectTo(),
		}, nil)
	}
}

// handleAcceptLoginChallenge 代理提交凭据并接受 Ory Hydra 登录挑战请求。
func handleAcceptLoginChallenge(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type acceptLoginReq struct {
		LoginChallenge      string `json:"login_challenge"`
		LoginChallengeCamel string `json:"loginChallenge"`
		TenantID            string `json:"tenant_id"`
		TenantIDCamel       string `json:"tenantId"`
		Account             string `json:"account"`
		Password            string `json:"password"`
		CaptchaKey          string `json:"captcha_key"`
		CaptchaKeyCamel     string `json:"captchaKey"`
		CaptchaCode         string `json:"captcha_code"`
		CaptchaCodeCamel    string `json:"captchaCode"`
		Remember            bool   `json:"remember"`
		RememberFor         int64  `json:"remember_for"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.AuthClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Auth 认证服务不可用"))
			return
		}
		var req acceptLoginReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		ch := req.LoginChallenge
		if ch == "" {
			ch = req.LoginChallengeCamel
		}
		if ch == "" {
			ch = r.URL.Query().Get("login_challenge")
		}
		tenantID := req.TenantID
		if tenantID == "" {
			tenantID = req.TenantIDCamel
		}
		if tenantID == "" {
			tenantID = tenant.FromTenant(ctx)
		}
		captchaKey := req.CaptchaKey
		if captchaKey == "" {
			captchaKey = req.CaptchaKeyCamel
		}
		captchaCode := req.CaptchaCode
		if captchaCode == "" {
			captchaCode = req.CaptchaCodeCamel
		}
		rpcResp, err := svcCtx.AuthClient.AcceptLoginChallenge(ctx, &authv1.AcceptLoginChallengeRequest{
			LoginChallenge: ch,
			TenantId:       tenantID,
			Account:        req.Account,
			Password:       req.Password,
			CaptchaKey:     captchaKey,
			CaptchaCode:    captchaCode,
			Remember:       req.Remember,
			RememberFor:    req.RememberFor,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]string{
			"redirect_to": rpcResp.GetRedirectTo(),
		}, nil)
	}
}

// handleAcceptConsentChallenge 代理接受 Ory Hydra 授权挑战并注入 BladeClaims。
func handleAcceptConsentChallenge(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type acceptConsentReq struct {
		ConsentChallenge      string   `json:"consent_challenge"`
		ConsentChallengeCamel string   `json:"consentChallenge"`
		GrantScope            []string `json:"grant_scope"`
		GrantScopeCamel       []string `json:"grantScope"`
		Remember              bool     `json:"remember"`
		RememberFor           int64    `json:"remember_for"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.AuthClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Auth 认证服务不可用"))
			return
		}
		var req acceptConsentReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		ch := req.ConsentChallenge
		if ch == "" {
			ch = req.ConsentChallengeCamel
		}
		if ch == "" {
			ch = r.URL.Query().Get("consent_challenge")
		}
		scopes := req.GrantScope
		if len(scopes) == 0 {
			scopes = req.GrantScopeCamel
		}
		rpcResp, err := svcCtx.AuthClient.AcceptConsentChallenge(ctx, &authv1.AcceptConsentChallengeRequest{
			ConsentChallenge: ch,
			GrantScope:       scopes,
			Remember:         req.Remember,
			RememberFor:      req.RememberFor,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]string{
			"redirect_to": rpcResp.GetRedirectTo(),
		}, nil)
	}
}

// handleAcceptLogoutChallenge 代理接受 Ory Hydra 登出挑战请求或主动吊销当前 JWT 令牌。
func handleAcceptLogoutChallenge(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type acceptLogoutReq struct {
		LogoutChallenge      string `json:"logout_challenge"`
		LogoutChallengeCamel string `json:"logoutChallenge"`
		AccessToken          string `json:"access_token"`
		AccessTokenCamel     string `json:"accessToken"`
		RefreshToken         string `json:"refresh_token"`
		RefreshTokenCamel    string `json:"refreshToken"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var req acceptLogoutReq
		if r.Body != nil {
			_ = json.NewDecoder(r.Body).Decode(&req)
		}
		ch := strings.TrimSpace(req.LogoutChallenge)
		if ch == "" {
			ch = strings.TrimSpace(req.LogoutChallengeCamel)
		}
		if ch == "" {
			ch = strings.TrimSpace(r.URL.Query().Get("logout_challenge"))
		}
		if ch == "" {
			ch = strings.TrimSpace(r.URL.Query().Get("logoutChallenge"))
		}

		accessToken := strings.TrimSpace(req.AccessToken)
		if accessToken == "" {
			accessToken = strings.TrimSpace(req.AccessTokenCamel)
		}
		if accessToken == "" {
			accessToken = strings.TrimSpace(r.Header.Get(pkgmiddleware.HeaderBladeAuth))
		}
		if accessToken == "" {
			accessToken = strings.TrimSpace(r.Header.Get(pkgmiddleware.HeaderAuthorization))
		}

		refreshToken := strings.TrimSpace(req.RefreshToken)
		if refreshToken == "" {
			refreshToken = strings.TrimSpace(req.RefreshTokenCamel)
		}

		revoked := false
		if accessToken != "" {
			var claims *jwt.BladeClaims
			if svcCtx != nil && svcCtx.TokenValidator != nil {
				claims, _ = svcCtx.TokenValidator.ValidateToken(ctx, accessToken)
			} else if svcCtx != nil && svcCtx.JWTHandler != nil {
				claims, _ = svcCtx.JWTHandler.ParseTokenCtx(ctx, accessToken)
			}
			if err := jwt.RevokeTokenWithClaims(ctx, accessToken, claims); err == nil {
				revoked = true
			}
		}

		if refreshToken != "" {
			if svcCtx != nil && svcCtx.JWTHandler != nil {
				_ = svcCtx.JWTHandler.RevokeRefreshToken(ctx, refreshToken)
			}
			if err := jwt.RevokeToken(ctx, refreshToken, 0); err == nil {
				revoked = true
			}
		}

		if ch == "" {
			if revoked {
				response.ResultResponse(ctx, w, map[string]any{
					"revoked": true,
				}, nil)
				return
			}
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "logout_challenge 或认证令牌不能为空"))
			return
		}

		if svcCtx.AuthClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Auth 认证服务不可用"))
			return
		}

		rpcResp, err := svcCtx.AuthClient.AcceptLogoutChallenge(ctx, &authv1.AcceptLogoutChallengeRequest{
			LogoutChallenge: ch,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		respData := map[string]any{
			"redirect_to": rpcResp.GetRedirectTo(),
		}
		if revoked {
			respData["revoked"] = true
		}
		response.ResultResponse(ctx, w, respData, nil)
	}
}

// handleListUsers 代理分页查询系统用户列表。
func handleListUsers(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		current := parseQueryInt64(r, "current", 1)
		size := parseQueryInt64(r, "size", 10)
		deptID := parseQueryInt64(r, "deptId", parseQueryInt64(r, "dept_id", 0))
		account := strings.TrimSpace(r.URL.Query().Get("account"))
		realName := strings.TrimSpace(r.URL.Query().Get("realName"))
		if realName == "" {
			realName = strings.TrimSpace(r.URL.Query().Get("real_name"))
		}

		rpcResp, err := svcCtx.SystemClient.ListUsers(ctx, &systemv1.ListUsersRequest{
			Current:  current,
			Size:     size,
			Account:  account,
			RealName: realName,
			DeptId:   deptID,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		records := make([]*UserInfoVO, 0, len(rpcResp.GetRecords()))
		for _, u := range rpcResp.GetRecords() {
			if u == nil {
				continue
			}
			records = append(records, &UserInfoVO{
				ID:       snowflake.ID(u.GetId()),
				TenantID: u.GetTenantId(),
				Account:  u.GetAccount(),
				Name:     u.GetName(),
				RealName: u.GetRealName(),
				Email:    u.GetEmail(),
				Phone:    u.GetPhone(),
				DeptID:   snowflake.ID(u.GetDeptId()),
				RoleID:   u.GetRoleId(),
				Status:   u.GetStatus(),
			})
		}

		page := response.NewIPage(records, rpcResp.GetTotal(), rpcResp.GetSize(), rpcResp.GetCurrent())
		response.ResultResponse(ctx, w, page, nil)
	}
}

// handleSaveUser 代理新增或修改系统用户。
func handleSaveUser(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type saveUserReq struct {
		ID       snowflake.ID `json:"id"`
		Account  string       `json:"account" label:"登录账号"`
		Password string       `json:"password" label:"登录密码"`
		Name     string       `json:"name" label:"用户姓名"`
		RealName string       `json:"real_name" label:"真实姓名"`
		Email    string       `json:"email" validate:"email" label:"电子邮箱"`
		Phone    string       `json:"phone" validate:"phone" label:"手机号码"`
		DeptID   snowflake.ID `json:"dept_id" label:"所属部门"`
		RoleID   string       `json:"role_id" label:"所属角色"`
		Status   int32        `json:"status" label:"用户状态"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		var req saveUserReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if req.ID.Int64() == 0 && strings.TrimSpace(req.Account) == "" {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "登录账号不能为空"))
			return
		}
		if err := validator.ValidateStruct(ctx, &req); err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		rpcResp, err := svcCtx.SystemClient.SaveUser(ctx, &systemv1.SaveUserRequest{
			Id:       req.ID.Int64(),
			Account:  req.Account,
			Password: req.Password,
			Name:     req.Name,
			RealName: req.RealName,
			Email:    req.Email,
			Phone:    req.Phone,
			DeptId:   req.DeptID.Int64(),
			RoleId:   req.RoleID,
			Status:   req.Status,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]any{"id": snowflake.ID(rpcResp.GetId())}, nil)
	}
}

// handleDeleteUsers 代理逻辑删除系统用户。
func handleDeleteUsers(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		ids, _ := extractIDList(r)
		rpcResp, err := svcCtx.SystemClient.DeleteUsers(ctx, &systemv1.DeleteUsersRequest{Ids: ids})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]int64{"affected": rpcResp.GetAffected()}, nil)
	}
}

func convertDeptTree(nodes []*systemv1.DeptInfo) []*DeptInfoVO {
	out := make([]*DeptInfoVO, 0, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		vo := &DeptInfoVO{
			ID:       snowflake.ID(n.GetId()),
			TenantID: n.GetTenantId(),
			ParentID: snowflake.ID(n.GetParentId()),
			DeptName: n.GetDeptName(),
			FullName: n.GetFullName(),
			Sort:     n.GetSort(),
			Remark:   n.GetRemark(),
		}
		if len(n.GetChildren()) > 0 {
			vo.Children = convertDeptTree(n.GetChildren())
		}
		out = append(out, vo)
	}
	return out
}

// handleGetDeptTree 代理查询部门树。
func handleGetDeptTree(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		deptName := strings.TrimSpace(r.URL.Query().Get("deptName"))
		if deptName == "" {
			deptName = strings.TrimSpace(r.URL.Query().Get("dept_name"))
		}
		rpcResp, err := svcCtx.SystemClient.GetDeptTree(ctx, &systemv1.GetDeptTreeRequest{DeptName: deptName})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, convertDeptTree(rpcResp.GetTree()), nil)
	}
}

// handleSaveDept 代理新增或修改部门。
func handleSaveDept(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type saveDeptReq struct {
		ID       snowflake.ID `json:"id"`
		ParentID snowflake.ID `json:"parent_id"`
		DeptName string       `json:"dept_name" validate:"required" label:"部门名称"`
		FullName string       `json:"full_name" label:"部门全称"`
		Sort     int32        `json:"sort"`
		Remark   string       `json:"remark"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		var req saveDeptReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if err := validator.ValidateStruct(ctx, &req); err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		rpcResp, err := svcCtx.SystemClient.SaveDept(ctx, &systemv1.SaveDeptRequest{
			Id:       req.ID.Int64(),
			ParentId: req.ParentID.Int64(),
			DeptName: req.DeptName,
			FullName: req.FullName,
			Sort:     req.Sort,
			Remark:   req.Remark,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]any{"id": snowflake.ID(rpcResp.GetId())}, nil)
	}
}

// handleDeleteDepts 代理逻辑删除部门。
func handleDeleteDepts(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		ids, _ := extractIDList(r)
		rpcResp, err := svcCtx.SystemClient.DeleteDepts(ctx, &systemv1.DeleteDeptsRequest{Ids: ids})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]int64{"affected": rpcResp.GetAffected()}, nil)
	}
}

// handleListRoles 代理分页查询角色列表。
func handleListRoles(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		current := parseQueryInt64(r, "current", 1)
		size := parseQueryInt64(r, "size", 10)
		roleName := strings.TrimSpace(r.URL.Query().Get("roleName"))
		if roleName == "" {
			roleName = strings.TrimSpace(r.URL.Query().Get("role_name"))
		}
		roleAlias := strings.TrimSpace(r.URL.Query().Get("roleAlias"))
		if roleAlias == "" {
			roleAlias = strings.TrimSpace(r.URL.Query().Get("role_alias"))
		}
		rpcResp, err := svcCtx.SystemClient.ListRoles(ctx, &systemv1.ListRolesRequest{
			Current:   current,
			Size:      size,
			RoleName:  roleName,
			RoleAlias: roleAlias,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		records := make([]*RoleInfoVO, 0, len(rpcResp.GetRecords()))
		for _, item := range rpcResp.GetRecords() {
			if item == nil {
				continue
			}
			records = append(records, &RoleInfoVO{
				ID:               snowflake.ID(item.GetId()),
				TenantID:         item.GetTenantId(),
				ParentID:         snowflake.ID(item.GetParentId()),
				RoleName:         item.GetRoleName(),
				RoleAlias:        item.GetRoleAlias(),
				Sort:             item.GetSort(),
				DataScopeType:    item.GetDataScopeType(),
				DataScopeDeptIDs: item.GetDataScopeDeptIds(),
			})
		}
		page := response.NewIPage(records, rpcResp.GetTotal(), rpcResp.GetSize(), rpcResp.GetCurrent())
		response.ResultResponse(ctx, w, page, nil)
	}
}

// handleSaveRole 代理新增或修改角色。
func handleSaveRole(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type saveRoleReq struct {
		ID        snowflake.ID `json:"id"`
		ParentID  snowflake.ID `json:"parent_id"`
		RoleName  string       `json:"role_name" validate:"required" label:"角色名称"`
		RoleAlias string       `json:"role_alias" label:"角色别名"`
		Sort      int32        `json:"sort"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		var req saveRoleReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if err := validator.ValidateStruct(ctx, &req); err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		rpcResp, err := svcCtx.SystemClient.SaveRole(ctx, &systemv1.SaveRoleRequest{
			Id:        req.ID.Int64(),
			ParentId:  req.ParentID.Int64(),
			RoleName:  req.RoleName,
			RoleAlias: req.RoleAlias,
			Sort:      req.Sort,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]any{"id": snowflake.ID(rpcResp.GetId())}, nil)
	}
}

// handleDeleteRoles 代理逻辑删除角色。
func handleDeleteRoles(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		ids, _ := extractIDList(r)
		rpcResp, err := svcCtx.SystemClient.DeleteRoles(ctx, &systemv1.DeleteRolesRequest{Ids: ids})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]int64{"affected": rpcResp.GetAffected()}, nil)
	}
}

// handleGrantRoleMenus 代理为角色分配菜单与按钮权限。
func handleGrantRoleMenus(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type grantReq struct {
		RoleID  snowflake.ID   `json:"role_id"`
		RoleIDs []snowflake.ID `json:"role_ids" label:"角色ID列表"`
		MenuIDs []snowflake.ID `json:"menu_ids" label:"菜单ID列表"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		var req grantReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if len(req.RoleIDs) == 0 && req.RoleID.Int64() > 0 {
			req.RoleIDs = []snowflake.ID{req.RoleID}
		}
		if len(req.RoleIDs) == 0 {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "角色ID列表不能为空"))
			return
		}
		roleIDs := make([]int64, 0, len(req.RoleIDs))
		for _, id := range req.RoleIDs {
			roleIDs = append(roleIDs, id.Int64())
		}
		menuIDs := make([]int64, 0, len(req.MenuIDs))
		for _, id := range req.MenuIDs {
			menuIDs = append(menuIDs, id.Int64())
		}
		rpcResp, err := svcCtx.SystemClient.GrantRoleMenus(ctx, &systemv1.GrantRoleMenusRequest{
			RoleIds: roleIDs,
			MenuIds: menuIDs,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		if svcCtx.PermCache != nil {
			_ = svcCtx.PermCache.DeleteByPrefix(ctx, "")
		}
		response.ResultResponse(ctx, w, map[string]bool{"success": rpcResp.GetSuccess()}, nil)
	}
}

// handleGrantRoleDataScope 代理为角色分配行级数据权限类型与自定义部门集合。
func handleGrantRoleDataScope(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type grantDataScopeReq struct {
		RoleID        snowflake.ID   `json:"role_id" validate:"required" label:"角色ID"`
		DataScopeType int32          `json:"data_scope_type" validate:"required" label:"数据权限类型"`
		DeptIDs       []snowflake.ID `json:"dept_ids" label:"自定义部门ID列表"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		var req grantDataScopeReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if err := validator.ValidateStruct(ctx, &req); err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		deptIDs := make([]int64, 0, len(req.DeptIDs))
		for _, id := range req.DeptIDs {
			if id.Int64() > 0 {
				deptIDs = append(deptIDs, id.Int64())
			}
		}
		rpcResp, err := svcCtx.SystemClient.GrantRoleDataScope(ctx, &systemv1.GrantRoleDataScopeRequest{
			RoleId:        req.RoleID.Int64(),
			DataScopeType: req.DataScopeType,
			DeptIds:       deptIDs,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]bool{"success": rpcResp.GetSuccess()}, nil)
	}
}

// handleGetMenuTree 代理查询菜单列表。
func handleGetMenuTree(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		name := strings.TrimSpace(r.URL.Query().Get("name"))
		cat := int32(parseQueryInt64(r, "category", 0))
		rpcResp, err := svcCtx.SystemClient.GetMenuTree(ctx, &systemv1.GetMenuTreeRequest{
			Name:     name,
			Category: cat,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		list := make([]*MenuInfoVO, 0, len(rpcResp.GetList()))
		for _, m := range rpcResp.GetList() {
			if m == nil {
				continue
			}
			list = append(list, &MenuInfoVO{
				ID:        snowflake.ID(m.GetId()),
				ParentID:  snowflake.ID(m.GetParentId()),
				Code:      m.GetCode(),
				Name:      m.GetName(),
				Path:      m.GetPath(),
				Component: m.GetComponent(),
				Icon:      m.GetIcon(),
				Sort:      m.GetSort(),
				Category:  m.GetCategory(),
				Action:    m.GetAction(),
			})
		}
		response.ResultResponse(ctx, w, list, nil)
	}
}

// handleSaveMenu 代理新增或修改菜单/按钮。
func handleSaveMenu(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type saveMenuReq struct {
		ID        snowflake.ID `json:"id"`
		ParentID  snowflake.ID `json:"parent_id"`
		Code      string       `json:"code" label:"菜单编码"`
		Name      string       `json:"name" validate:"required" label:"菜单名称"`
		Path      string       `json:"path"`
		Component string       `json:"component"`
		Icon      string       `json:"icon"`
		Sort      int32        `json:"sort"`
		Category  int32        `json:"category"`
		Action    string       `json:"action"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		var req saveMenuReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if err := validator.ValidateStruct(ctx, &req); err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		rpcResp, err := svcCtx.SystemClient.SaveMenu(ctx, &systemv1.SaveMenuRequest{
			Id:        req.ID.Int64(),
			ParentId:  req.ParentID.Int64(),
			Code:      req.Code,
			Name:      req.Name,
			Path:      req.Path,
			Component: req.Component,
			Icon:      req.Icon,
			Sort:      req.Sort,
			Category:  req.Category,
			Action:    req.Action,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]any{"id": snowflake.ID(rpcResp.GetId())}, nil)
	}
}

// handleDeleteMenus 代理逻辑删除菜单。
func handleDeleteMenus(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		ids, _ := extractIDList(r)
		rpcResp, err := svcCtx.SystemClient.DeleteMenus(ctx, &systemv1.DeleteMenusRequest{Ids: ids})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]int64{"affected": rpcResp.GetAffected()}, nil)
	}
}

// handleListDicts 代理分页查询字典列表。
func handleListDicts(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		current := parseQueryInt64(r, "current", 1)
		size := parseQueryInt64(r, "size", 10)
		parentID := parseQueryInt64(r, "parentId", parseQueryInt64(r, "parent_id", -1))
		code := strings.TrimSpace(r.URL.Query().Get("code"))
		dictVal := strings.TrimSpace(r.URL.Query().Get("dictValue"))
		if dictVal == "" {
			dictVal = strings.TrimSpace(r.URL.Query().Get("dict_value"))
		}
		rpcResp, err := svcCtx.SystemClient.ListDicts(ctx, &systemv1.ListDictsRequest{
			Current:   current,
			Size:      size,
			Code:      code,
			DictValue: dictVal,
			ParentId:  parentID,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		records := make([]*DictInfoVO, 0, len(rpcResp.GetRecords()))
		for _, d := range rpcResp.GetRecords() {
			if d == nil {
				continue
			}
			records = append(records, &DictInfoVO{
				ID:        snowflake.ID(d.GetId()),
				ParentID:  snowflake.ID(d.GetParentId()),
				Code:      d.GetCode(),
				DictKey:   d.GetDictKey(),
				DictValue: d.GetDictValue(),
				Sort:      d.GetSort(),
				Remark:    d.GetRemark(),
			})
		}
		page := response.NewIPage(records, rpcResp.GetTotal(), rpcResp.GetSize(), rpcResp.GetCurrent())
		response.ResultResponse(ctx, w, page, nil)
	}
}

// handleSaveDict 代理新增或修改字典。
func handleSaveDict(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type saveDictReq struct {
		ID        snowflake.ID `json:"id"`
		ParentID  snowflake.ID `json:"parent_id"`
		Code      string       `json:"code" validate:"required" label:"字典编码"`
		DictKey   string       `json:"dict_key" label:"字典键值"`
		DictValue string       `json:"dict_value" label:"字典值"`
		Sort      int32        `json:"sort"`
		Remark    string       `json:"remark"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		var req saveDictReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if err := validator.ValidateStruct(ctx, &req); err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		rpcResp, err := svcCtx.SystemClient.SaveDict(ctx, &systemv1.SaveDictRequest{
			Id:        req.ID.Int64(),
			ParentId:  req.ParentID.Int64(),
			Code:      req.Code,
			DictKey:   req.DictKey,
			DictValue: req.DictValue,
			Sort:      req.Sort,
			Remark:    req.Remark,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]any{"id": snowflake.ID(rpcResp.GetId())}, nil)
	}
}

// handleDeleteDicts 代理逻辑删除字典。
func handleDeleteDicts(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		ids, _ := extractIDList(r)
		rpcResp, err := svcCtx.SystemClient.DeleteDicts(ctx, &systemv1.DeleteDictsRequest{Ids: ids})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]int64{"affected": rpcResp.GetAffected()}, nil)
	}
}

// handleListNotices 代理分页查询通知公告列表。
func handleListNotices(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.DeskClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Desk 工作台服务不可用"))
			return
		}
		current := parseQueryInt64(r, "current", 1)
		size := parseQueryInt64(r, "size", 10)
		title := strings.TrimSpace(r.URL.Query().Get("title"))
		category := int32(parseQueryInt64(r, "category", 0))

		rpcResp, err := svcCtx.DeskClient.ListNotices(ctx, &deskv1.ListNoticesRequest{
			Current:  current,
			Size:     size,
			Title:    title,
			Category: category,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		records := make([]*NoticeInfoVO, 0, len(rpcResp.GetRecords()))
		for _, n := range rpcResp.GetRecords() {
			if n == nil {
				continue
			}
			records = append(records, &NoticeInfoVO{
				ID:          snowflake.ID(n.GetId()),
				TenantID:    n.GetTenantId(),
				Title:       n.GetTitle(),
				Category:    n.GetCategory(),
				ReleaseTime: n.GetReleaseTime(),
				Content:     n.GetContent(),
				Status:      n.GetStatus(),
				CreateUser:  snowflake.ID(n.GetCreateUser()),
				CreateDept:  snowflake.ID(n.GetCreateDept()),
				CreateTime:  n.GetCreateTime(),
			})
		}

		page := response.NewIPage(records, rpcResp.GetTotal(), rpcResp.GetSize(), rpcResp.GetCurrent())
		response.ResultResponse(ctx, w, page, nil)
	}
}

// handleGetNotice 代理查询通知公告详情。
func handleGetNotice(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.DeskClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Desk 工作台服务不可用"))
			return
		}
		id := parseQueryInt64(r, "id", 0)
		rpcResp, err := svcCtx.DeskClient.GetNotice(ctx, &deskv1.GetNoticeRequest{Id: id})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		var vo *NoticeInfoVO
		if n := rpcResp.GetNotice(); n != nil {
			vo = &NoticeInfoVO{
				ID:          snowflake.ID(n.GetId()),
				TenantID:    n.GetTenantId(),
				Title:       n.GetTitle(),
				Category:    n.GetCategory(),
				ReleaseTime: n.GetReleaseTime(),
				Content:     n.GetContent(),
				Status:      n.GetStatus(),
				CreateUser:  snowflake.ID(n.GetCreateUser()),
				CreateDept:  snowflake.ID(n.GetCreateDept()),
				CreateTime:  n.GetCreateTime(),
			}
		}
		response.ResultResponse(ctx, w, vo, nil)
	}
}

// handleSaveNotice 代理新增或修改通知公告。
func handleSaveNotice(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type saveNoticeReq struct {
		ID          snowflake.ID `json:"id"`
		Title       string       `json:"title" validate:"required" label:"公告标题"`
		Category    int32        `json:"category" label:"公告分类"`
		ReleaseTime string       `json:"release_time"`
		Content     string       `json:"content" label:"公告内容"`
		Status      int32        `json:"status"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.DeskClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Desk 工作台服务不可用"))
			return
		}
		var req saveNoticeReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if err := validator.ValidateStruct(ctx, &req); err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		rpcResp, err := svcCtx.DeskClient.SaveNotice(ctx, &deskv1.SaveNoticeRequest{
			Id:          req.ID.Int64(),
			Title:       req.Title,
			Category:    req.Category,
			ReleaseTime: req.ReleaseTime,
			Content:     req.Content,
			Status:      req.Status,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]any{"id": snowflake.ID(rpcResp.GetId())}, nil)
	}
}

// handleDeleteNotices 代理批量逻辑删除通知公告。
func handleDeleteNotices(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.DeskClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 Desk 工作台服务不可用"))
			return
		}
		ids, _ := extractIDList(r)
		rpcResp, err := svcCtx.DeskClient.DeleteNotices(ctx, &deskv1.DeleteNoticesRequest{Ids: ids})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]int64{"affected": rpcResp.GetAffected()}, nil)
	}
}

// handleListAuditLogs 代理分页查询 API 操作审计日志列表。
func handleListAuditLogs(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		current := parseQueryInt64(r, "current", 1)
		size := parseQueryInt64(r, "size", 10)
		account := strings.TrimSpace(r.URL.Query().Get("account"))
		method := strings.TrimSpace(r.URL.Query().Get("method"))
		path := strings.TrimSpace(r.URL.Query().Get("path"))
		statusCode := int32(parseQueryInt64(r, "statusCode", parseQueryInt64(r, "status_code", 0)))
		traceID := strings.TrimSpace(r.URL.Query().Get("traceId"))
		if traceID == "" {
			traceID = strings.TrimSpace(r.URL.Query().Get("trace_id"))
		}

		rpcResp, err := svcCtx.SystemClient.ListAuditLogs(ctx, &systemv1.ListAuditLogsRequest{
			Current:    current,
			Size:       size,
			Account:    account,
			Method:     method,
			Path:       path,
			StatusCode: statusCode,
			TraceId:    traceID,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		records := make([]*AuditLogInfoVO, 0, len(rpcResp.GetRecords()))
		for _, item := range rpcResp.GetRecords() {
			if item == nil {
				continue
			}
			records = append(records, &AuditLogInfoVO{
				ID:          snowflake.ID(item.GetId()),
				TenantID:    item.GetTenantId(),
				ServiceName: item.GetServiceName(),
				UserID:      snowflake.ID(item.GetUserId()),
				Account:     item.GetAccount(),
				Method:      item.GetMethod(),
				Path:        item.GetPath(),
				QueryParams: item.GetQueryParams(),
				RequestBody: item.GetRequestBody(),
				StatusCode:  item.GetStatusCode(),
				DurationMs:  item.GetDurationMs(),
				ClientIP:    item.GetClientIp(),
				TraceID:     item.GetTraceId(),
				CreatedAt:   item.GetCreatedAt(),
			})
		}

		page := response.NewIPage(records, rpcResp.GetTotal(), rpcResp.GetSize(), rpcResp.GetCurrent())
		response.ResultResponse(ctx, w, page, nil)
	}
}

// handleListParams 代理分页查询系统动态参数列表。
func handleListParams(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		current := parseQueryInt64(r, "current", 1)
		size := parseQueryInt64(r, "size", 10)
		paramName := strings.TrimSpace(r.URL.Query().Get("paramName"))
		if paramName == "" {
			paramName = strings.TrimSpace(r.URL.Query().Get("param_name"))
		}
		paramKey := strings.TrimSpace(r.URL.Query().Get("paramKey"))
		if paramKey == "" {
			paramKey = strings.TrimSpace(r.URL.Query().Get("param_key"))
		}

		rpcResp, err := svcCtx.SystemClient.ListParams(ctx, &systemv1.ListParamsRequest{
			Current:   current,
			Size:      size,
			ParamName: paramName,
			ParamKey:  paramKey,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}

		records := make([]*ParamInfoVO, 0, len(rpcResp.GetRecords()))
		for _, p := range rpcResp.GetRecords() {
			if p == nil {
				continue
			}
			records = append(records, &ParamInfoVO{
				ID:         snowflake.ID(p.GetId()),
				TenantID:   p.GetTenantId(),
				ParamName:  p.GetParamName(),
				ParamKey:   p.GetParamKey(),
				ParamValue: p.GetParamValue(),
				Remark:     p.GetRemark(),
			})
		}

		page := response.NewIPage(records, rpcResp.GetTotal(), rpcResp.GetSize(), rpcResp.GetCurrent())
		response.ResultResponse(ctx, w, page, nil)
	}
}

// handleSaveParam 代理新增或修改系统动态参数。
func handleSaveParam(svcCtx *svc.ServiceContext) http.HandlerFunc {
	type saveParamReq struct {
		ID         snowflake.ID `json:"id"`
		ParamName  string       `json:"param_name" validate:"required" label:"参数名称"`
		ParamKey   string       `json:"param_key" validate:"required" label:"参数键名"`
		ParamValue string       `json:"param_value" validate:"required" label:"参数键值"`
		Remark     string       `json:"remark"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		var req saveParamReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.RequestParamError, "请求格式错误"))
			return
		}
		if err := validator.ValidateStruct(ctx, &req); err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		rpcResp, err := svcCtx.SystemClient.SaveParam(ctx, &systemv1.SaveParamRequest{
			Id:         req.ID.Int64(),
			ParamName:  req.ParamName,
			ParamKey:   req.ParamKey,
			ParamValue: req.ParamValue,
			Remark:     req.Remark,
		})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]any{"id": snowflake.ID(rpcResp.GetId())}, nil)
	}
}

// handleDeleteParams 代理批量逻辑删除系统动态参数。
func handleDeleteParams(svcCtx *svc.ServiceContext) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if svcCtx.SystemClient == nil {
			response.ResultResponse(ctx, w, nil, xerr.NewCodeError(xerr.ServerCommonError, "下游 System 系统服务不可用"))
			return
		}
		ids, _ := extractIDList(r)
		rpcResp, err := svcCtx.SystemClient.DeleteParams(ctx, &systemv1.DeleteParamsRequest{Ids: ids})
		if err != nil {
			response.ResultResponse(ctx, w, nil, err)
			return
		}
		response.ResultResponse(ctx, w, map[string]int64{"affected": rpcResp.GetAffected()}, nil)
	}
}
