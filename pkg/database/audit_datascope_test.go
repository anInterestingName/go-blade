package database

import (
	"context"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"gorm.io/gorm"
)

// auditBusinessRecord 测试标准业务实体（使用 snowflake.ID 与 int64 混合测试）
type auditBusinessRecord struct {
	ID         int64        `gorm:"primaryKey;autoIncrement;column:id"`
	TenantID   string       `gorm:"column:tenant_id"`
	Title      string       `gorm:"column:title"`
	DeptID     int64        `gorm:"column:dept_id"`
	CreateUser snowflake.ID `gorm:"column:create_user"`
	CreateDept snowflake.ID `gorm:"column:create_dept"`
	CreateTime time.Time    `gorm:"column:create_time"`
	UpdateUser int64        `gorm:"column:update_user"`
	UpdateTime time.Time    `gorm:"column:update_time"`
	Status     int          `gorm:"column:status"`
	IsDeleted  int          `gorm:"column:is_deleted"`
}

func (auditBusinessRecord) TableName() string {
	return "blade_audit_record"
}

func setupAuditAndDataScopeDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite memory db: %v", err)
	}

	if err := db.Use(tenant.NewTenantPlugin()); err != nil {
		t.Fatalf("failed to register TenantPlugin: %v", err)
	}
	if err := db.Use(NewAuditPlugin()); err != nil {
		t.Fatalf("failed to register AuditPlugin: %v", err)
	}
	if err := db.Use(NewDataScopePlugin()); err != nil {
		t.Fatalf("failed to register DataScopePlugin: %v", err)
	}

	if err := db.AutoMigrate(&auditBusinessRecord{}); err != nil {
		t.Fatalf("failed to auto migrate: %v", err)
	}
	return db
}

func TestAuditPlugin_CreateAndUpdate(t *testing.T) {
	db := setupAuditAndDataScopeDB(t)

	claimsCreator := &jwt.BladeClaims{
		TenantID: "000000",
		UserID:   1001,
		DeptID:   2001,
		Account:  "creator",
	}
	ctxCreate := tenant.WithTenant(context.Background(), "000000")
	ctxCreate = jwt.WithClaims(ctxCreate, claimsCreator)

	// 1. 单条插入自动填充 create_user, create_dept, update_user, create_time, update_time, status
	single := auditBusinessRecord{
		Title:  "Single Doc",
		DeptID: 2001,
	}
	if err := db.WithContext(ctxCreate).Create(&single).Error; err != nil {
		t.Fatalf("single create failed: %v", err)
	}
	if single.CreateUser != snowflake.ID(1001) {
		t.Fatalf("expected create_user=1001, got %d", single.CreateUser)
	}
	if single.CreateDept != snowflake.ID(2001) {
		t.Fatalf("expected create_dept=2001, got %d", single.CreateDept)
	}
	if single.UpdateUser != 1001 {
		t.Fatalf("expected update_user=1001, got %d", single.UpdateUser)
	}
	if single.CreateTime.IsZero() || single.UpdateTime.IsZero() {
		t.Fatalf("expected non-zero create_time and update_time")
	}
	if single.Status != 1 {
		t.Fatalf("expected default status=1, got %d", single.Status)
	}
	if single.IsDeleted != 0 {
		t.Fatalf("expected default is_deleted=0, got %d", single.IsDeleted)
	}

	// 2. 批量切片插入 & 显式赋值不覆盖验证
	batch := []auditBusinessRecord{
		{Title: "Batch Item 1", DeptID: 2001},
		{Title: "Batch Item 2", DeptID: 2002, CreateUser: snowflake.ID(9999), CreateDept: snowflake.ID(8888)},
	}
	if err := db.WithContext(ctxCreate).Create(&batch).Error; err != nil {
		t.Fatalf("batch create failed: %v", err)
	}
	if batch[0].CreateUser != snowflake.ID(1001) || batch[0].CreateDept != snowflake.ID(2001) || batch[0].UpdateUser != 1001 {
		t.Fatalf("batch[0] audit fields not auto-filled properly: %+v", batch[0])
	}
	if batch[0].CreateTime.IsZero() {
		t.Fatalf("batch[0] create_time is zero")
	}
	// 验证 batch[1] 显式指定的 CreateUser / CreateDept 未被覆盖
	if batch[1].CreateUser != snowflake.ID(9999) || batch[1].CreateDept != snowflake.ID(8888) {
		t.Fatalf("batch[1] explicit audit fields should not be overwritten, got: %+v", batch[1])
	}
	if batch[1].UpdateUser != 1001 {
		t.Fatalf("batch[1] update_user expected 1001, got %d", batch[1].UpdateUser)
	}

	// 3. 更新时自动刷新 update_user 与 update_time
	claimsUpdater := &jwt.BladeClaims{
		TenantID: "000000",
		UserID:   3003,
		DeptID:   4004,
		Account:  "updater",
	}
	ctxUpdate := tenant.WithTenant(context.Background(), "000000")
	ctxUpdate = jwt.WithClaims(ctxUpdate, claimsUpdater)

	if err := db.WithContext(ctxUpdate).
		Model(&auditBusinessRecord{}).
		Where("id = ?", single.ID).
		Update("title", "Updated Single Doc").Error; err != nil {
		t.Fatalf("update failed: %v", err)
	}

	var updated auditBusinessRecord
	if err := db.WithContext(ctxUpdate).First(&updated, single.ID).Error; err != nil {
		t.Fatalf("query updated record failed: %v", err)
	}
	if updated.Title != "Updated Single Doc" {
		t.Fatalf("expected updated title, got %s", updated.Title)
	}
	if updated.UpdateUser != 3003 {
		t.Fatalf("expected update_user=3003 after update, got %d", updated.UpdateUser)
	}
	if updated.CreateUser != snowflake.ID(1001) {
		t.Fatalf("expected create_user to remain 1001, got %d", updated.CreateUser)
	}
}

func TestDataScopePlugin_FiveLevelsAndTenantOverlay(t *testing.T) {
	db := setupAuditAndDataScopeDB(t)

	// 准备测试数据：
	// 租户 000000 下：
	// - 记录 1: dept_id=10, create_user=101
	// - 记录 2: dept_id=11 (10 的子部门), create_user=102
	// - 记录 3: dept_id=20 (其他部门), create_user=103
	// 租户 999999 下：
	// - 记录 4: dept_id=10, create_user=101 (跨租户同部门同用户，用于验证与 TenantPlugin 叠加隔离)
	seedTenant0 := tenant.WithTenant(context.Background(), "000000")
	recordsT0 := []auditBusinessRecord{
		{Title: "Dept10-User101", DeptID: 10, CreateUser: 101, CreateDept: 10},
		{Title: "Dept11-User102", DeptID: 11, CreateUser: 102, CreateDept: 11},
		{Title: "Dept20-User103", DeptID: 20, CreateUser: 103, CreateDept: 20},
	}
	if err := db.WithContext(seedTenant0).Create(&recordsT0).Error; err != nil {
		t.Fatalf("seed tenant 000000 failed: %v", err)
	}

	seedTenant9 := tenant.WithTenant(context.Background(), "999999")
	recordT9 := auditBusinessRecord{Title: "OtherTenant-Dept10", DeptID: 10, CreateUser: 101, CreateDept: 10}
	if err := db.WithContext(seedTenant9).Create(&recordT9).Error; err != nil {
		t.Fatalf("seed tenant 999999 failed: %v", err)
	}

	baseCtx := tenant.WithTenant(context.Background(), "000000")
	baseCtx = jwt.WithClaims(baseCtx, &jwt.BladeClaims{
		TenantID: "000000",
		UserID:   101,
		DeptID:   10,
	})

	// 1. DataScopeAll (全部数据权限：可见当前租户下全部 3 条，不含其他租户)
	t.Run("DataScopeAll", func(t *testing.T) {
		ctx := WithDataScope(baseCtx, DataScope{Type: DataScopeAll})
		var list []auditBusinessRecord
		if err := db.WithContext(ctx).Find(&list).Error; err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(list) != 3 {
			t.Fatalf("expected 3 records for DataScopeAll, got %d", len(list))
		}
	})

	// 2. DataScopeCustom (自定义部门数据权限：指定 dept_id IN (10, 20)，可见 2 条)
	t.Run("DataScopeCustom", func(t *testing.T) {
		ctx := WithDataScope(baseCtx, DataScope{
			Type:    DataScopeCustom,
			DeptIDs: []int64{10, 20},
		})
		var list []auditBusinessRecord
		if err := db.WithContext(ctx).Find(&list).Error; err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("expected 2 records for DataScopeCustom [10, 20], got %d", len(list))
		}
	})

	// 3. DataScopeDept (本级部门数据权限：自动从 BladeClaims.DeptID=10 回退，可见 1 条)
	t.Run("DataScopeDept", func(t *testing.T) {
		ctx := WithDataScope(baseCtx, DataScope{
			Type: DataScopeDept,
		})
		var list []auditBusinessRecord
		if err := db.WithContext(ctx).Find(&list).Error; err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(list) != 1 || list[0].DeptID != 10 {
			t.Fatalf("expected 1 record with dept_id=10 for DataScopeDept, got %+v", list)
		}
	})

	// 4. DataScopeDeptAndChild (本级及子部门数据权限：dept_id IN (10, 11)，可见 2 条)
	t.Run("DataScopeDeptAndChild", func(t *testing.T) {
		ctx := WithDataScope(baseCtx, DataScope{
			Type:    DataScopeDeptAndChild,
			DeptIDs: []int64{10, 11},
		})
		var list []auditBusinessRecord
		if err := db.WithContext(ctx).Find(&list).Error; err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(list) != 2 {
			t.Fatalf("expected 2 records for DataScopeDeptAndChild [10, 11], got %d", len(list))
		}
	})

	// 5. DataScopeSelf (仅本人数据权限：自动从 BladeClaims.UserID=101 回退，匹配 create_user=101，可见 1 条)
	t.Run("DataScopeSelf", func(t *testing.T) {
		ctx := WithDataScope(baseCtx, DataScope{
			Type: DataScopeSelf,
		})
		var list []auditBusinessRecord
		if err := db.WithContext(ctx).Find(&list).Error; err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(list) != 1 || list[0].CreateUser != snowflake.ID(101) {
			t.Fatalf("expected 1 record with create_user=101 for DataScopeSelf, got %+v", list)
		}
	})

	// 6. 复杂 OR 查询条件与 DataScope + TenantPlugin 叠加（防止 OR 运算符优先级导致跨租户/跨部门越权泄露）
	t.Run("ComplexOrCondition_NoCrossTenantOrCrossScopeLeak", func(t *testing.T) {
		ctx := WithDataScope(baseCtx, DataScope{
			Type: DataScopeDept, // 仅限本部门 dept_id=10
		})
		var list []auditBusinessRecord
		// 业务层传入含 OR 的条件，尝试命中 dept_id=20 或其他租户的标题
		err := db.WithContext(ctx).
			Where("title = ? OR title = ? OR title = ?", "Dept10-User101", "Dept20-User103", "OtherTenant-Dept10").
			Find(&list).Error
		if err != nil {
			t.Fatalf("complex OR query failed: %v", err)
		}
		if len(list) != 1 || list[0].TenantID != "000000" || list[0].DeptID != 10 {
			t.Fatalf("OR condition bypassed Tenant or DataScope isolation! got: %+v", list)
		}
	})

	// 7. DataScopeCustom 空部门列表防越权兜底（未分配任何部门时返回 0 条，绝不能退化为查全表）
	t.Run("DataScopeCustom_EmptyDeptIDs_ReturnsZero", func(t *testing.T) {
		ctx := WithDataScope(baseCtx, DataScope{
			Type:    DataScopeCustom,
			DeptIDs: []int64{},
		})
		var list []auditBusinessRecord
		if err := db.WithContext(ctx).Find(&list).Error; err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(list) != 0 {
			t.Fatalf("expected 0 records when DataScopeCustom has empty DeptIDs, got %d", len(list))
		}
	})
}
