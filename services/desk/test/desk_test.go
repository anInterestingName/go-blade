// Package test 提供协同工作台微服务 DeskService 的端到端集成测试。
package test

import (
	"context"
	"net"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"

	deskv1 "github.com/go-blade/api/proto/desk/v1"
	"github.com/go-blade/pkg/database"
	"github.com/go-blade/pkg/jwt"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/services/desk/internal/config"
	"github.com/go-blade/services/desk/internal/model"
	"github.com/go-blade/services/desk/internal/server"
	"github.com/go-blade/services/desk/internal/svc"
)

// TestDeskService_E2E_Workflow 验证协同工作台通知公告的全链路 CRUD、自动审计与多租户隔离。
func TestDeskService_E2E_Workflow(t *testing.T) {
	// 1. 初始化 SQLite 内存数据库，挂载多租户隔离、自动审计与行级数据权限插件
	db, err := gorm.Open(sqlite.Open("file:desk_test?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Use(tenant.NewTenantPlugin()))
	require.NoError(t, db.Use(database.NewAuditPlugin()))
	require.NoError(t, db.Use(database.NewDataScopePlugin()))

	err = db.AutoMigrate(&model.Notice{})
	require.NoError(t, err)

	// 2. 构造 ServiceContext 并拉起真实 gRPC Server
	svcCtx := svc.NewServiceContext(config.Config{})
	svcCtx.DB = db

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	grpcServer := pkgrpc.NewServer()
	deskServer := server.NewDeskServer(svcCtx)
	deskv1.RegisterDeskServiceServer(grpcServer, deskServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()
	defer grpcServer.Stop()

	// 3. gRPC Client 拨号连接
	conn, err := pkgrpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := deskv1.NewDeskServiceClient(conn)

	// 构造租户 000000 下的登录用户上下文（UserID=1001, DeptID=2001）
	ctx000000 := tenant.WithTenant(context.Background(), "000000")
	ctx000000 = jwt.WithClaims(ctx000000, &jwt.BladeClaims{
		TenantID: "000000",
		UserID:   1001,
		DeptID:   2001,
		RoleID:   "1",
		RoleName: "administrator",
		Account:  "admin",
		UserName: "管理员",
	})

	// 构造租户 100001 下的登录用户上下文（UserID=3001, DeptID=4001）
	ctx100001 := tenant.WithTenant(context.Background(), "100001")
	ctx100001 = jwt.WithClaims(ctx100001, &jwt.BladeClaims{
		TenantID: "100001",
		UserID:   3001,
		DeptID:   4001,
		RoleID:   "2",
		RoleName: "user",
		Account:  "tenant_a_user",
		UserName: "租户A员工",
	})

	// 4. 验证1：携带 jwt.WithClaims 上下文调用 SaveNotice 新增公告时，自动注入 TenantID、CreateUser、CreateDept
	saveResp1, err := client.SaveNotice(ctx000000, &deskv1.SaveNoticeRequest{
		Title:       "五一劳动节放假通知",
		Category:    1,
		ReleaseTime: "2026-04-30 10:00:00",
		Content:     "五一放假五天，请提前做好工作安排。",
		Status:      1,
	})
	require.NoError(t, err)
	require.Greater(t, saveResp1.Id, int64(0))

	saveResp2, err := client.SaveNotice(ctx000000, &deskv1.SaveNoticeRequest{
		Title:       "系统版本 v2.0 升级公告",
		Category:    2,
		ReleaseTime: "2026-05-01 00:00:00",
		Content:     "系统将于凌晨进行停机维护升级。",
		Status:      1,
	})
	require.NoError(t, err)
	require.Greater(t, saveResp2.Id, int64(0))

	// 查询详情验证自动审计字段与租户标识
	getResp1, err := client.GetNotice(ctx000000, &deskv1.GetNoticeRequest{Id: saveResp1.Id})
	require.NoError(t, err)
	require.NotNil(t, getResp1.Notice)
	assert.Equal(t, saveResp1.Id, getResp1.Notice.Id)
	assert.Equal(t, "000000", getResp1.Notice.TenantId)
	assert.Equal(t, "五一劳动节放假通知", getResp1.Notice.Title)
	assert.Equal(t, int32(1), getResp1.Notice.Category)
	assert.Equal(t, int64(1001), getResp1.Notice.CreateUser)
	assert.Equal(t, int64(2001), getResp1.Notice.CreateDept)
	assert.NotEmpty(t, getResp1.Notice.CreateTime)

	// 验证修改公告
	updateResp, err := client.SaveNotice(ctx000000, &deskv1.SaveNoticeRequest{
		Id:          saveResp1.Id,
		Title:       "五一劳动节放假及值班通知",
		Category:    1,
		ReleaseTime: "2026-04-30 12:00:00",
		Content:     "五一放假五天，附上值班表。",
		Status:      1,
	})
	require.NoError(t, err)
	assert.Equal(t, saveResp1.Id, updateResp.Id)

	getUpdated, err := client.GetNotice(ctx000000, &deskv1.GetNoticeRequest{Id: saveResp1.Id})
	require.NoError(t, err)
	assert.Equal(t, "五一劳动节放假及值班通知", getUpdated.Notice.Title)
	assert.Equal(t, "五一放假五天，附上值班表。", getUpdated.Notice.Content)

	// 5. 验证2：调用 ListNotices 分页查询与条件过滤（按标题模糊搜索 + 按分类过滤）
	listAll, err := client.ListNotices(ctx000000, &deskv1.ListNoticesRequest{
		Current: 1,
		Size:    10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), listAll.Total)
	assert.Len(t, listAll.Records, 2)

	// 按标题模糊搜索
	listByTitle, err := client.ListNotices(ctx000000, &deskv1.ListNoticesRequest{
		Current: 1,
		Size:    10,
		Title:   "版本 v2.0",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), listByTitle.Total)
	require.Len(t, listByTitle.Records, 1)
	assert.Equal(t, saveResp2.Id, listByTitle.Records[0].Id)

	// 按分类过滤
	listByCategory, err := client.ListNotices(ctx000000, &deskv1.ListNoticesRequest{
		Current:  1,
		Size:     10,
		Category: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), listByCategory.Total)
	require.Len(t, listByCategory.Records, 1)
	assert.Equal(t, saveResp1.Id, listByCategory.Records[0].Id)

	// 6. 验证3：跨租户隔离（租户 100001 无法查看或删除租户 000000 的公告）
	listTenantA, err := client.ListNotices(ctx100001, &deskv1.ListNoticesRequest{
		Current: 1,
		Size:    10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), listTenantA.Total)
	assert.Empty(t, listTenantA.Records)

	_, err = client.GetNotice(ctx100001, &deskv1.GetNoticeRequest{Id: saveResp1.Id})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))

	delCrossTenant, err := client.DeleteNotices(ctx100001, &deskv1.DeleteNoticesRequest{
		Ids: []int64{saveResp1.Id, saveResp2.Id},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), delCrossTenant.Affected)

	// 确认租户 000000 的公告未被跨租户删除
	stillExists, err := client.GetNotice(ctx000000, &deskv1.GetNoticeRequest{Id: saveResp1.Id})
	require.NoError(t, err)
	assert.Equal(t, saveResp1.Id, stillExists.Notice.Id)

	// 7. 验证4：调用 DeleteNotices 软删除后不再出现在列表中
	delResp, err := client.DeleteNotices(ctx000000, &deskv1.DeleteNoticesRequest{
		Ids: []int64{saveResp1.Id},
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), delResp.Affected)

	// 详情查询返回 NotFound
	_, err = client.GetNotice(ctx000000, &deskv1.GetNoticeRequest{Id: saveResp1.Id})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))

	// 列表查询仅剩未删除的 1 条
	listAfterDel, err := client.ListNotices(ctx000000, &deskv1.ListNoticesRequest{
		Current: 1,
		Size:    10,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), listAfterDel.Total)
	require.Len(t, listAfterDel.Records, 1)
	assert.Equal(t, saveResp2.Id, listAfterDel.Records[0].Id)
}
