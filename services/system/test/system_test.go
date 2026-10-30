// Package test 提供系统管理与 RBAC 权限微服务的端到端集成测试。
package test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	systemv1 "github.com/go-blade/api/proto/system/v1"
	"github.com/go-blade/pkg/auditlog"
	"github.com/go-blade/pkg/database"
	"github.com/go-blade/pkg/event"
	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/mq"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/services/system/internal/config"
	"github.com/go-blade/services/system/internal/model"
	"github.com/go-blade/services/system/internal/server"
	"github.com/go-blade/services/system/internal/svc"
)

func TestSystemService_E2E_Workflow(t *testing.T) {
	// 1. 初始化 SQLite 内存数据库，挂载多租户隔离、自动审计与行级数据权限插件并迁移全部模型
	db, err := gorm.Open(sqlite.Open("file:system_test?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Use(tenant.NewTenantPlugin()))
	require.NoError(t, db.Use(database.NewAuditPlugin()))
	require.NoError(t, db.Use(database.NewDataScopePlugin()))

	err = db.AutoMigrate(
		&model.Tenant{},
		&model.Menu{},
		&model.User{},
		&model.Role{},
		&model.RoleMenu{},
		&model.Dept{},
		&model.Dict{},
		&model.AuditLog{},
		&model.Param{},
	)
	require.NoError(t, err)

	ctx000000 := tenant.WithTenant(context.Background(), "000000")
	ctx100001 := tenant.WithTenant(context.Background(), "100001")

	// 插入两个租户 ("000000" 和 "100001")
	require.NoError(t, db.WithContext(ctx000000).Create(&model.Tenant{
		ID:            1,
		TenantID:      "000000",
		TenantName:    "顶级管理租户",
		Linkman:       "系统管理员",
		ContactNumber: "18888888888",
		Status:        1,
	}).Error)
	require.NoError(t, db.WithContext(ctx100001).Create(&model.Tenant{
		ID:            2,
		TenantID:      "100001",
		TenantName:    "企业租户A",
		Linkman:       "张经理",
		ContactNumber: "13900000001",
		Status:        1,
	}).Error)

	// 插入角色（管理员角色 1，普通用户角色 2）
	require.NoError(t, db.WithContext(ctx000000).Create(&model.Role{
		ID:        1,
		TenantID:  "000000",
		RoleName:  "超级管理员",
		RoleAlias: "administrator",
		Sort:      1,
	}).Error)
	require.NoError(t, db.WithContext(ctx000000).Create(&model.Role{
		ID:        2,
		TenantID:  "000000",
		RoleName:  "普通员工",
		RoleAlias: "user",
		Sort:      2,
	}).Error)

	// 插入菜单与按钮权限
	// 菜单 1001: 系统管理（仅管理员角色 1 拥有）
	// 按钮 1002: sys:user:add（仅管理员角色 1 拥有）
	// 菜单 2001: 工作台（管理员角色 1 与普通角色 2 均拥有）
	// 按钮 2002: desk:notice:view（普通角色 2 拥有）
	menus := []*model.Menu{
		{
			ID:        1001,
			ParentID:  0,
			Code:      "sys",
			Name:      "系统管理",
			Path:      "/sys",
			Component: "Layout",
			Category:  1,
			Sort:      1,
		},
		{
			ID:       1002,
			ParentID: 1001,
			Code:     "sys_user_add",
			Name:     "新增用户",
			Category: 2,
			Action:   "sys:user:add",
			Sort:     1,
		},
		{
			ID:        2001,
			ParentID:  0,
			Code:      "desk",
			Name:      "工作台",
			Path:      "/desk",
			Component: "views/desk/index",
			Category:  1,
			Sort:      2,
		},
		{
			ID:       2002,
			ParentID: 2001,
			Code:     "desk_notice_view",
			Name:     "查看通知",
			Category: 2,
			Action:   "desk:notice:view",
			Sort:     1,
		},
	}
	for _, m := range menus {
		require.NoError(t, db.Create(m).Error)
	}

	// 插入角色菜单关联
	roleMenus := []*model.RoleMenu{
		{ID: 1, RoleID: 1, MenuID: 1001},
		{ID: 2, RoleID: 1, MenuID: 1002},
		{ID: 3, RoleID: 1, MenuID: 2001},
		{ID: 4, RoleID: 2, MenuID: 2001},
		{ID: 5, RoleID: 2, MenuID: 2002},
	}
	for _, rm := range roleMenus {
		require.NoError(t, db.Create(rm).Error)
	}

	// 插入管理员用户与普通用户（租户 000000）
	adminUserID := int64(1123598821738675200)
	normalUserID := int64(1123598821738675201)

	require.NoError(t, db.WithContext(ctx000000).Create(&model.User{
		ID:       1123598821738675200,
		TenantID: "000000",
		Account:  "admin",
		Password: "hashed_password",
		Name:     "管理员",
		RealName: "系统管理员",
		Email:    "admin@blade.com",
		Phone:    "18888888888",
		DeptID:   100,
		RoleID:   "1",
		Status:   1,
	}).Error)

	require.NoError(t, db.WithContext(ctx000000).Create(&model.User{
		ID:       1123598821738675201,
		TenantID: "000000",
		Account:  "normal_user",
		Password: "hashed_password",
		Name:     "普通员工张三",
		RealName: "张三",
		Email:    "user@blade.com",
		Phone:    "13800000001",
		DeptID:   101,
		RoleID:   "2",
		Status:   1,
	}).Error)

	// 2. 构造 ServiceContext 并拉起 gRPC Server
	svcCtx := svc.NewServiceContext(config.Config{})
	svcCtx.DB = db

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	grpcServer := pkgrpc.NewServer()
	systemServer := server.NewSystemServer(svcCtx)
	systemv1.RegisterSystemServiceServer(grpcServer, systemServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// 3. gRPC Client 拨号连接
	conn, err := pkgrpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := systemv1.NewSystemServiceClient(conn)

	// 4. 验证1：即使在租户 "100001" 上下文中调用 GetTenant("000000") 也能正常查出租户信息（验证 blade_tenant 不会自锁）
	tenantResp, err := client.GetTenant(ctx100001, &systemv1.GetTenantRequest{
		TenantId: "000000",
	})
	require.NoError(t, err)
	require.NotNil(t, tenantResp.Tenant)
	assert.Equal(t, "000000", tenantResp.Tenant.TenantId)
	assert.Equal(t, "顶级管理租户", tenantResp.Tenant.TenantName)

	// 查询不存在的租户返回 NotFound，绝不返回假数据
	_, err = client.GetTenant(ctx000000, &systemv1.GetTenantRequest{
		TenantId: "999999",
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))

	// 5. 验证2：管理员与普通用户调用 GetUser 和 GetUserRoutes 拿到各自真实的角色、菜单和按钮权限
	adminResp, err := client.GetUser(ctx000000, &systemv1.GetUserRequest{
		UserId: adminUserID,
	})
	require.NoError(t, err)
	require.NotNil(t, adminResp.User)
	assert.Equal(t, "admin", adminResp.User.Account)
	assert.Equal(t, []string{"administrator"}, adminResp.Roles)
	assert.Equal(t, []string{"sys:user:add"}, adminResp.Permissions)

	normalResp, err := client.GetUser(ctx000000, &systemv1.GetUserRequest{
		UserId: normalUserID,
	})
	require.NoError(t, err)
	require.NotNil(t, normalResp.User)
	assert.Equal(t, "normal_user", normalResp.User.Account)
	assert.Equal(t, []string{"user"}, normalResp.Roles)
	assert.Equal(t, []string{"desk:notice:view"}, normalResp.Permissions)
	assert.NotContains(t, normalResp.Permissions, "sys:user:add")

	// 普通用户获取路由：只能拿到 "工作台"，拿不到管理员的 "系统管理"
	normalRoutesResp, err := client.GetUserRoutes(ctx000000, &systemv1.GetUserRoutesRequest{
		UserId: normalUserID,
	})
	require.NoError(t, err)
	require.Len(t, normalRoutesResp.Routes, 1)
	assert.Equal(t, "工作台", normalRoutesResp.Routes[0].Name)
	assert.Equal(t, []string{"desk:notice:view"}, normalRoutesResp.Buttons)

	// 6. 验证3：跨租户隔离 —— 租户 "100001" 无法通过 GetUser 查到租户 "000000" 的用户
	_, err = client.GetUser(ctx100001, &systemv1.GetUserRequest{
		UserId: adminUserID,
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))

	// 7. 验证4：不存在的用户或无权限角色返回 NotFound 或空菜单，绝不返回演示数据
	_, err = client.GetUser(ctx000000, &systemv1.GetUserRequest{
		UserId: 999999999,
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))

	emptyRoutesResp, err := client.GetUserRoutes(ctx000000, &systemv1.GetUserRoutesRequest{
		RoleIds: "999",
	})
	require.NoError(t, err)
	assert.Empty(t, emptyRoutesResp.Routes)
	assert.Empty(t, emptyRoutesResp.Buttons)
}

func TestSystemService_CRUD_And_Pagination(t *testing.T) {
	// 1. 初始化 SQLite 内存数据库，挂载 TenantPlugin、AuditPlugin 与 DataScopePlugin
	db, err := gorm.Open(sqlite.Open("file:system_crud_test?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Use(tenant.NewTenantPlugin()))
	require.NoError(t, db.Use(database.NewAuditPlugin()))
	require.NoError(t, db.Use(database.NewDataScopePlugin()))

	err = db.AutoMigrate(
		&model.Tenant{},
		&model.Menu{},
		&model.User{},
		&model.Role{},
		&model.RoleMenu{},
		&model.Dept{},
		&model.Dict{},
		&model.AuditLog{},
		&model.Param{},
	)
	require.NoError(t, err)

	svcCtx := svc.NewServiceContext(config.Config{})
	svcCtx.DB = db

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	grpcServer := pkgrpc.NewServer()
	systemServer := server.NewSystemServer(svcCtx)
	systemv1.RegisterSystemServiceServer(grpcServer, systemServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	conn, err := pkgrpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := systemv1.NewSystemServiceClient(conn)

	// 构造带当前登录管理员身份 Claims 的上下文，验证 gRPC Metadata 透传与 AuditPlugin 自动填充
	adminClaims := &jwt.BladeClaims{
		TenantID: "000000",
		UserID:   888001,
		DeptID:   999001,
		RoleID:   "1",
		RoleName: "administrator",
		Account:  "admin",
		UserName: "管理员",
	}
	ctxAdmin := tenant.WithTenant(context.Background(), "000000")
	ctxAdmin = jwt.WithClaims(ctxAdmin, adminClaims)

	updaterClaims := &jwt.BladeClaims{
		TenantID: "000000",
		UserID:   888002,
		DeptID:   999002,
		Account:  "editor",
	}
	ctxUpdater := tenant.WithTenant(context.Background(), "000000")
	ctxUpdater = jwt.WithClaims(ctxUpdater, updaterClaims)

	ctxOtherTenant := tenant.WithTenant(context.Background(), "100001")

	// ==================== 1. 部门树 & CRUD 测试 ====================
	rootDeptResp, err := client.SaveDept(ctxAdmin, &systemv1.SaveDeptRequest{
		ParentId: 0,
		DeptName: "研发中心",
		FullName: "集团研发中心",
		Sort:     1,
		Remark:   "一级研发部门",
	})
	require.NoError(t, err)
	require.Greater(t, rootDeptResp.Id, int64(0))

	childDeptResp, err := client.SaveDept(ctxAdmin, &systemv1.SaveDeptRequest{
		ParentId: rootDeptResp.Id,
		DeptName: "后端架构组",
		FullName: "Go 微服务架构组",
		Sort:     1,
		Remark:   "二级部门",
	})
	require.NoError(t, err)
	require.Greater(t, childDeptResp.Id, int64(0))

	// 验证部门审计字段 CreateUser / CreateDept 自动填充
	var createdDept model.Dept
	require.NoError(t, db.WithContext(ctxAdmin).First(&createdDept, rootDeptResp.Id).Error)
	assert.Equal(t, snowflake.ID(888001), createdDept.CreateUser)
	assert.Equal(t, snowflake.ID(999001), createdDept.CreateDept)
	assert.Equal(t, snowflake.ID(888001), createdDept.UpdateUser)

	// 获取部门树，验证父子 Children 自动组装
	deptTreeResp, err := client.GetDeptTree(ctxAdmin, &systemv1.GetDeptTreeRequest{})
	require.NoError(t, err)
	require.Len(t, deptTreeResp.Tree, 1)
	assert.Equal(t, "研发中心", deptTreeResp.Tree[0].DeptName)
	require.Len(t, deptTreeResp.Tree[0].Children, 1)
	assert.Equal(t, "后端架构组", deptTreeResp.Tree[0].Children[0].DeptName)

	// 修改子部门名称并验证 UpdateUser 刷新
	_, err = client.SaveDept(ctxUpdater, &systemv1.SaveDeptRequest{
		Id:       childDeptResp.Id,
		ParentId: rootDeptResp.Id,
		DeptName: "云原生架构组",
		FullName: "云原生平台组",
		Sort:     2,
	})
	require.NoError(t, err)

	var updatedDept model.Dept
	require.NoError(t, db.WithContext(ctxAdmin).First(&updatedDept, childDeptResp.Id).Error)
	assert.Equal(t, "云原生架构组", updatedDept.DeptName)
	assert.Equal(t, snowflake.ID(888002), updatedDept.UpdateUser)

	// 软删除子部门
	delDeptResp, err := client.DeleteDepts(ctxAdmin, &systemv1.DeleteDeptsRequest{
		Ids: []int64{childDeptResp.Id},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), delDeptResp.Affected)

	deptTreeAfterDel, err := client.GetDeptTree(ctxAdmin, &systemv1.GetDeptTreeRequest{})
	require.NoError(t, err)
	require.Len(t, deptTreeAfterDel.Tree, 1)
	assert.Empty(t, deptTreeAfterDel.Tree[0].Children)

	// ==================== 2. 角色、菜单 CRUD & 菜单权限分配闭环 ====================
	roleResp, err := client.SaveRole(ctxAdmin, &systemv1.SaveRoleRequest{
		ParentId:  0,
		RoleName:  "研发工程师",
		RoleAlias: "developer",
		Sort:      1,
	})
	require.NoError(t, err)
	require.Greater(t, roleResp.Id, int64(0))

	listRolesResp, err := client.ListRoles(ctxAdmin, &systemv1.ListRolesRequest{
		Current:  1,
		Size:     10,
		RoleName: "研发",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), listRolesResp.Total)
	require.Len(t, listRolesResp.Records, 1)
	assert.Equal(t, "developer", listRolesResp.Records[0].RoleAlias)

	// 新增菜单与按钮
	menuRouteResp, err := client.SaveMenu(ctxAdmin, &systemv1.SaveMenuRequest{
		ParentId:  0,
		Code:      "devops",
		Name:      "运维中心",
		Path:      "/devops",
		Component: "Layout",
		Sort:      1,
		Category:  1,
	})
	require.NoError(t, err)
	require.Greater(t, menuRouteResp.Id, int64(0))

	menuBtnResp, err := client.SaveMenu(ctxAdmin, &systemv1.SaveMenuRequest{
		ParentId: menuRouteResp.Id,
		Code:     "devops_deploy",
		Name:     "发布服务",
		Sort:     1,
		Category: 2,
		Action:   "devops:deploy:exec",
	})
	require.NoError(t, err)
	require.Greater(t, menuBtnResp.Id, int64(0))

	menuTreeResp, err := client.GetMenuTree(ctxAdmin, &systemv1.GetMenuTreeRequest{
		Name: "运维",
	})
	require.NoError(t, err)
	require.Len(t, menuTreeResp.List, 1)
	assert.Equal(t, "devops", menuTreeResp.List[0].Code)

	// ==================== 3. 用户 CRUD、分页、密码 Bcrypt & 角色授权即时生效 ====================
	saveUserResp, err := client.SaveUser(ctxAdmin, &systemv1.SaveUserRequest{
		Account:  "dev_lisi",
		Password: "MySecretPassword123",
		Name:     "李四",
		RealName: "李四工程师",
		Email:    "lisi@blade.com",
		Phone:    "13700001111",
		DeptId:   rootDeptResp.Id,
		RoleId:   snowflake.ID(roleResp.Id).String(),
		Status:   1,
	})
	require.NoError(t, err)
	require.Greater(t, saveUserResp.Id, int64(0))

	// 验证数据库中的密码已使用 bcrypt 哈希且审计字段已自动填充
	var dbUser model.User
	require.NoError(t, db.WithContext(ctxAdmin).First(&dbUser, saveUserResp.Id).Error)
	assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(dbUser.Password), []byte("MySecretPassword123")))
	assert.Equal(t, snowflake.ID(888001), dbUser.CreateUser)
	assert.Equal(t, snowflake.ID(999001), dbUser.CreateDept)

	// 分页查询用户
	listUsersResp, err := client.ListUsers(ctxAdmin, &systemv1.ListUsersRequest{
		Current:  1,
		Size:     10,
		Account:  "dev_",
		RealName: "李四",
		DeptId:   rootDeptResp.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), listUsersResp.Total)
	require.Len(t, listUsersResp.Records, 1)
	assert.Equal(t, "dev_lisi", listUsersResp.Records[0].Account)

	// 跨租户分页查询用户应为空
	otherTenantUsers, err := client.ListUsers(ctxOtherTenant, &systemv1.ListUsersRequest{
		Current: 1,
		Size:    10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), otherTenantUsers.Total)

	// 授权前调用 GetUserRoutes 应为空
	routesBeforeGrant, err := client.GetUserRoutes(ctxAdmin, &systemv1.GetUserRoutesRequest{
		UserId: saveUserResp.Id,
	})
	require.NoError(t, err)
	assert.Empty(t, routesBeforeGrant.Routes)
	assert.Empty(t, routesBeforeGrant.Buttons)

	// 执行 GrantRoleMenus 授权后，调用 GetUserRoutes 立即生效
	grantResp, err := client.GrantRoleMenus(ctxAdmin, &systemv1.GrantRoleMenusRequest{
		RoleIds: []int64{roleResp.Id},
		MenuIds: []int64{menuRouteResp.Id, menuBtnResp.Id},
	})
	require.NoError(t, err)
	assert.True(t, grantResp.Success)

	routesAfterGrant, err := client.GetUserRoutes(ctxAdmin, &systemv1.GetUserRoutesRequest{
		UserId: saveUserResp.Id,
	})
	require.NoError(t, err)
	require.Len(t, routesAfterGrant.Routes, 1)
	assert.Equal(t, "运维中心", routesAfterGrant.Routes[0].Name)
	assert.Equal(t, []string{"devops:deploy:exec"}, routesAfterGrant.Buttons)

	// 修改用户信息
	_, err = client.SaveUser(ctxUpdater, &systemv1.SaveUserRequest{
		Id:       saveUserResp.Id,
		Account:  "dev_lisi",
		Name:     "李四(高级)",
		RealName: "李四架构师",
		Email:    "lisi_sr@blade.com",
		Phone:    "13700002222",
		DeptId:   rootDeptResp.Id,
		RoleId:   snowflake.ID(roleResp.Id).String(),
		Status:   1,
	})
	require.NoError(t, err)

	// 软删除用户
	delUserResp, err := client.DeleteUsers(ctxAdmin, &systemv1.DeleteUsersRequest{
		Ids: []int64{saveUserResp.Id},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), delUserResp.Affected)

	listUsersAfterDel, err := client.ListUsers(ctxAdmin, &systemv1.ListUsersRequest{
		Current: 1,
		Size:    10,
		Account: "dev_lisi",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), listUsersAfterDel.Total)

	// ==================== 4. 字典 CRUD & 分页查询测试 ====================
	parentDictResp, err := client.SaveDict(ctxAdmin, &systemv1.SaveDictRequest{
		ParentId:  0,
		Code:      "user_sex",
		DictKey:   "-1",
		DictValue: "性别",
		Sort:      1,
		Remark:    "用户性别字典",
	})
	require.NoError(t, err)
	require.Greater(t, parentDictResp.Id, int64(0))

	childDictResp, err := client.SaveDict(ctxAdmin, &systemv1.SaveDictRequest{
		ParentId:  parentDictResp.Id,
		Code:      "user_sex",
		DictKey:   "1",
		DictValue: "男",
		Sort:      1,
	})
	require.NoError(t, err)
	require.Greater(t, childDictResp.Id, int64(0))

	listDictsResp, err := client.ListDicts(ctxAdmin, &systemv1.ListDictsRequest{
		Current:  1,
		Size:     10,
		Code:     "user_sex",
		ParentId: parentDictResp.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), listDictsResp.Total)
	require.Len(t, listDictsResp.Records, 1)
	assert.Equal(t, "男", listDictsResp.Records[0].DictValue)

	// 软删除字典
	delDictResp, err := client.DeleteDicts(ctxAdmin, &systemv1.DeleteDictsRequest{
		Ids: []int64{childDictResp.Id},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), delDictResp.Affected)

	listDictsAfterDel, err := client.ListDicts(ctxAdmin, &systemv1.ListDictsRequest{
		Current:  1,
		Size:     10,
		Code:     "user_sex",
		ParentId: parentDictResp.Id,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), listDictsAfterDel.Total)

	// 软删除菜单和角色验证
	delMenuResp, err := client.DeleteMenus(ctxAdmin, &systemv1.DeleteMenusRequest{
		Ids: []int64{menuBtnResp.Id},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), delMenuResp.Affected)

	delRoleResp, err := client.DeleteRoles(ctxAdmin, &systemv1.DeleteRolesRequest{
		Ids: []int64{roleResp.Id},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), delRoleResp.Affected)

	// ==================== 5. 角色 DataScope 行级数据权限自动解析与过滤测试 ====================
	t.Run("Role_DataScope_RowLevel_Filtering", func(t *testing.T) {
		// 创建测试部门树：总部(DeptA) -> 研发部(DeptB) -> 后端组(DeptC)，以及独立的财务部(DeptD)
		deptA, err := client.SaveDept(ctxAdmin, &systemv1.SaveDeptRequest{ParentId: 0, DeptName: "总部"})
		require.NoError(t, err)
		deptB, err := client.SaveDept(ctxAdmin, &systemv1.SaveDeptRequest{ParentId: deptA.Id, DeptName: "研发部"})
		require.NoError(t, err)
		deptC, err := client.SaveDept(ctxAdmin, &systemv1.SaveDeptRequest{ParentId: deptB.Id, DeptName: "后端组"})
		require.NoError(t, err)
		deptD, err := client.SaveDept(ctxAdmin, &systemv1.SaveDeptRequest{ParentId: deptA.Id, DeptName: "财务部"})
		require.NoError(t, err)

		// 创建三种不同 DataScopeType 的角色：
		// Role 3001: 本级部门 (DataScopeDept = 3)
		// Role 3002: 本级及子部门 (DataScopeDeptAndChild = 4)
		// Role 3003: 仅本人数据 (DataScopeSelf = 5)
		roleDeptOnly := &model.Role{
			ID:            3001,
			TenantID:      "000000",
			RoleName:      "部门主管",
			RoleAlias:     "dept_manager",
			DataScopeType: int(database.DataScopeDept),
		}
		roleDeptChild := &model.Role{
			ID:            3002,
			TenantID:      "000000",
			RoleName:      "大区总监",
			RoleAlias:     "director",
			DataScopeType: int(database.DataScopeDeptAndChild),
		}
		roleSelfOnly := &model.Role{
			ID:            3003,
			TenantID:      "000000",
			RoleName:      "普通专员",
			RoleAlias:     "specialist",
			DataScopeType: int(database.DataScopeSelf),
		}
		require.NoError(t, db.WithContext(ctxAdmin).Create(roleDeptOnly).Error)
		require.NoError(t, db.WithContext(ctxAdmin).Create(roleDeptChild).Error)
		require.NoError(t, db.WithContext(ctxAdmin).Create(roleSelfOnly).Error)

		// 构造普通专员上下文（UserID: 990005, DeptID: deptC.Id, RoleID: "3003"）用于创建本人归属的用户
		ctxSelfUser := tenant.WithTenant(context.Background(), "000000")
		ctxSelfUser = jwt.WithClaims(ctxSelfUser, &jwt.BladeClaims{
			TenantID: "000000",
			UserID:   990005,
			DeptID:   deptC.Id,
			RoleID:   "3003",
			RoleName: "specialist",
			Account:  "spec_caller",
		})

		// 分别在不同部门下创建测试用户（统一前缀 "ds_user_" 便于断言）
		_, err = client.SaveUser(ctxAdmin, &systemv1.SaveUserRequest{
			Account: "ds_user_deptB", Name: "研发部员工", DeptId: deptB.Id, RoleId: "3001", Status: 1,
		})
		require.NoError(t, err)
		_, err = client.SaveUser(ctxSelfUser, &systemv1.SaveUserRequest{
			Account: "ds_user_deptC_self", Name: "后端组员工(专员创建)", DeptId: deptC.Id, RoleId: "3003", Status: 1,
		})
		require.NoError(t, err)
		_, err = client.SaveUser(ctxAdmin, &systemv1.SaveUserRequest{
			Account: "ds_user_deptD", Name: "财务部员工", DeptId: deptD.Id, RoleId: "3003", Status: 1,
		})
		require.NoError(t, err)

		// 5.1 超级管理员查询 "ds_user_"：可查看全部 3 名用户
		allList, err := client.ListUsers(ctxAdmin, &systemv1.ListUsersRequest{Current: 1, Size: 10, Account: "ds_user_"})
		require.NoError(t, err)
		assert.Equal(t, int64(3), allList.Total)

		// 5.2 本级部门权限角色 (Role 3001, 所属部门 deptB) 查询 "ds_user_"：仅能看到 deptB 的 1 名用户
		ctxDeptManager := tenant.WithTenant(context.Background(), "000000")
		ctxDeptManager = jwt.WithClaims(ctxDeptManager, &jwt.BladeClaims{
			TenantID: "000000",
			UserID:   990001,
			DeptID:   deptB.Id,
			RoleID:   "3001",
			RoleName: "dept_manager",
			Account:  "mgr_b",
		})
		deptOnlyList, err := client.ListUsers(ctxDeptManager, &systemv1.ListUsersRequest{Current: 1, Size: 10, Account: "ds_user_"})
		require.NoError(t, err)
		assert.Equal(t, int64(1), deptOnlyList.Total)
		require.Len(t, deptOnlyList.Records, 1)
		assert.Equal(t, "ds_user_deptB", deptOnlyList.Records[0].Account)

		// 5.3 本级及子部门权限角色 (Role 3002, 所属部门 deptB，子部门包含 deptC) 查询 "ds_user_"：能看到 deptB 与 deptC 共 2 名用户，看不到财务部 deptD
		ctxDirector := tenant.WithTenant(context.Background(), "000000")
		ctxDirector = jwt.WithClaims(ctxDirector, &jwt.BladeClaims{
			TenantID: "000000",
			UserID:   990002,
			DeptID:   deptB.Id,
			RoleID:   "3002",
			RoleName: "director",
			Account:  "dir_b",
		})
		deptAndChildList, err := client.ListUsers(ctxDirector, &systemv1.ListUsersRequest{Current: 1, Size: 10, Account: "ds_user_"})
		require.NoError(t, err)
		assert.Equal(t, int64(2), deptAndChildList.Total)

		// 5.4 仅本人权限角色 (Role 3003, UserID: 990005) 查询 "ds_user_"：仅能看到本人 (CreateUser = 990005) 创建的 1 名用户
		selfOnlyList, err := client.ListUsers(ctxSelfUser, &systemv1.ListUsersRequest{Current: 1, Size: 10, Account: "ds_user_"})
		require.NoError(t, err)
		assert.Equal(t, int64(1), selfOnlyList.Total)
		require.Len(t, selfOnlyList.Records, 1)
		assert.Equal(t, "ds_user_deptC_self", selfOnlyList.Records[0].Account)

		// 5.5 自定义部门数据权限角色 (DataScopeCustom = 2)：通过 GrantRoleDataScope 授权该角色仅可见 deptC 与 deptD
		roleCustom := &model.Role{
			ID:            3004,
			TenantID:      "000000",
			RoleName:      "跨部门协调员",
			RoleAlias:     "custom_coord",
			DataScopeType: int(database.DataScopeSelf),
		}
		require.NoError(t, db.WithContext(ctxAdmin).Create(roleCustom).Error)

		grantDSResp, err := client.GrantRoleDataScope(ctxAdmin, &systemv1.GrantRoleDataScopeRequest{
			RoleId:        3004,
			DataScopeType: int32(database.DataScopeCustom),
			DeptIds:       []int64{deptC.Id, deptD.Id},
		})
		require.NoError(t, err)
		assert.True(t, grantDSResp.Success)

		ctxCustomUser := tenant.WithTenant(context.Background(), "000000")
		ctxCustomUser = jwt.WithClaims(ctxCustomUser, &jwt.BladeClaims{
			TenantID: "000000",
			UserID:   990009,
			DeptID:   deptA.Id,
			RoleID:   "3004",
			RoleName: "custom_coord",
			Account:  "coord_caller",
		})
		customList, err := client.ListUsers(ctxCustomUser, &systemv1.ListUsersRequest{Current: 1, Size: 10, Account: "ds_user_"})
		require.NoError(t, err)
		assert.Equal(t, int64(2), customList.Total, "自定义部门权限角色应仅能查出 deptC 与 deptD 共 2 名用户")
	})

	// ==================== 5.6 系统动态参数 (blade_param) CRUD 与缓存联动测试 ====================
	t.Run("System_Param_CRUD_And_Cache", func(t *testing.T) {
		saveParamResp, err := client.SaveParam(ctxAdmin, &systemv1.SaveParamRequest{
			ParamName:  "初始密码配置",
			ParamKey:   "account.initPassword",
			ParamValue: "Blade@2026",
			Remark:     "新用户默认初始密码",
		})
		require.NoError(t, err)
		require.Greater(t, saveParamResp.Id, int64(0))

		// 验证 ParamCache 已同步写入
		cachedVal, hit, err := svcCtx.ParamCache.Get(ctxAdmin, "000000:account.initPassword")
		require.NoError(t, err)
		assert.True(t, hit)
		assert.Equal(t, "Blade@2026", cachedVal)

		listParamResp, err := client.ListParams(ctxAdmin, &systemv1.ListParamsRequest{
			Current:  1,
			Size:     10,
			ParamKey: "account.init",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), listParamResp.Total)
		require.Len(t, listParamResp.Records, 1)
		assert.Equal(t, "Blade@2026", listParamResp.Records[0].ParamValue)

		// 删除参数并验证缓存已清理
		delParamResp, err := client.DeleteParams(ctxAdmin, &systemv1.DeleteParamsRequest{
			Ids: []int64{saveParamResp.Id},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), delParamResp.Affected)

		_, hitAfterDel, _ := svcCtx.ParamCache.Get(ctxAdmin, "000000:account.initPassword")
		assert.False(t, hitAfterDel)
	})

	// ==================== 5.7 租户生命周期 CRUD 与顶级管理租户保护测试 ====================
	t.Run("Tenant_Lifecycle_CRUD_And_TopTenant_Protection", func(t *testing.T) {
		// 先植入顶级租户 000000
		require.NoError(t, db.WithContext(ctxAdmin).Create(&model.Tenant{
			ID:         1,
			TenantID:   "000000",
			TenantName: "管理组",
			Status:     1,
		}).Error)

		saveTenantResp, err := client.SaveTenant(ctxAdmin, &systemv1.SaveTenantRequest{
			TenantName:    "新创科技租户",
			Linkman:       "王总",
			ContactNumber: "13600009999",
			Address:       "深圳市南山区",
			Status:        1,
		})
		require.NoError(t, err)
		require.Greater(t, saveTenantResp.Id, int64(0))
		require.Len(t, saveTenantResp.TenantId, 6)

		listTenantsResp, err := client.ListTenants(ctxAdmin, &systemv1.ListTenantsRequest{
			Current:    1,
			Size:       10,
			TenantName: "新创科技",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), listTenantsResp.Total)

		// 尝试删除顶级管理租户 (ID=1, TenantID="000000") 应被豁免保护 (Affected=0)
		delTopResp, err := client.DeleteTenants(ctxAdmin, &systemv1.DeleteTenantsRequest{
			Ids: []int64{1},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(0), delTopResp.Affected)

		// 删除普通租户正常生效
		delNormalResp, err := client.DeleteTenants(ctxAdmin, &systemv1.DeleteTenantsRequest{
			Ids: []int64{saveTenantResp.Id},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), delNormalResp.Affected)
	})

	// ==================== 6. MQ 审计日志异步落库与分页查询 & RBAC 变更广播事件测试 ====================
	t.Run("AuditLog_MQ_Persistence_And_RBAC_Broadcast", func(t *testing.T) {
		rbacEventCh := make(chan event.RBACChangedEvent, 4)
		require.NoError(t, svcCtx.MQBroker.Subscribe(event.TopicRBACChanged, func(ctx context.Context, msg mq.Message) error {
			var ev event.RBACChangedEvent
			if err := msg.DecodeJSON(&ev); err == nil {
				rbacEventCh <- ev
			}
			return nil
		}, mq.WithBroadcast()))

		// 触发一次角色保存，验证 SystemServer 自动向 MQ 发布 TopicRBACChanged 广播事件
		newRoleResp, err := client.SaveRole(ctxAdmin, &systemv1.SaveRoleRequest{
			RoleName:  "审计管理员",
			RoleAlias: "auditor",
			Sort:      5,
		})
		require.NoError(t, err)
		require.Greater(t, newRoleResp.Id, int64(0))

		select {
		case ev := <-rbacEventCh:
			assert.Equal(t, "000000", ev.TenantID)
			assert.Equal(t, "save_role", ev.Action)
			assert.Contains(t, ev.RoleIDs, newRoleResp.Id)
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for RBACChangedEvent broadcast")
		}

		// 模拟网关通过 MQRecorder 投递两条属于不同租户的脱敏审计日志到 "blade.audit.api"
		mqRec := auditlog.NewMQRecorder(svcCtx.MQBroker)
		require.NoError(t, mqRec.Record(ctxAdmin, auditlog.Entry{
			ID:          snowflake.NextID(),
			TenantID:    "000000",
			ServiceName: "service-gateway",
			UserID:      888001,
			Account:     "admin",
			Method:      "POST",
			Path:        "/blade-system/user/submit",
			RequestBody: `{"account":"new_u","password":"******"}`,
			StatusCode:  200,
			DurationMs:  15,
			ClientIP:    "127.0.0.1",
			TraceID:     "trace-audit-001",
		}))

		require.NoError(t, mqRec.Record(ctxOtherTenant, auditlog.Entry{
			ID:          snowflake.NextID(),
			TenantID:    "100001",
			ServiceName: "service-gateway",
			UserID:      777001,
			Account:     "tenant_admin",
			Method:      "POST",
			Path:        "/blade-system/role/submit",
			RequestBody: `{"role_name":"r1"}`,
			StatusCode:  200,
			DurationMs:  8,
			ClientIP:    "127.0.0.1",
			TraceID:     "trace-audit-002",
		}))

		// 等待 System 异步消费者落库并验证 ListAuditLogs 的多租户隔离与条件过滤
		require.Eventually(t, func() bool {
			resp, qErr := client.ListAuditLogs(ctxAdmin, &systemv1.ListAuditLogsRequest{
				Current: 1,
				Size:    10,
				TraceId: "trace-audit-001",
			})
			return qErr == nil && resp.GetTotal() == 1
		}, 2*time.Second, 15*time.Millisecond)

		auditPage, err := client.ListAuditLogs(ctxAdmin, &systemv1.ListAuditLogsRequest{
			Current: 1,
			Size:    10,
			Account: "admin",
			Method:  "POST",
			Path:    "/blade-system/user",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(1), auditPage.Total)
		require.Len(t, auditPage.Records, 1)
		assert.Equal(t, "trace-audit-001", auditPage.Records[0].TraceId)
		assert.Contains(t, auditPage.Records[0].RequestBody, "******")

		// 验证租户 000000 看不到租户 100001 的审计日志
		crossTenantAudit, err := client.ListAuditLogs(ctxAdmin, &systemv1.ListAuditLogsRequest{
			Current: 1,
			Size:    10,
			TraceId: "trace-audit-002",
		})
		require.NoError(t, err)
		assert.Equal(t, int64(0), crossTenantAudit.Total)
	})
}
