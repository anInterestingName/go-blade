package tenant

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type User struct {
	ID       uint   `gorm:"primaryKey"`
	TenantID string `gorm:"column:tenant_id"`
	Name     string
}

type SysConfig struct {
	ID    uint `gorm:"primaryKey"`
	Key   string
	Value string
}

func setupTestDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	plugin := NewTenantPlugin("sys_config")
	if err := db.Use(plugin); err != nil {
		t.Fatalf("failed to use tenant plugin: %v", err)
	}

	if err := db.AutoMigrate(&User{}, &SysConfig{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}

	return db
}

func TestTenantContext(t *testing.T) {
	ctx := context.Background()
	if FromTenant(ctx) != DefaultTenantID {
		t.Fatalf("expected default tenant %s, got %s", DefaultTenantID, FromTenant(ctx))
	}

	customTenant := "100001"
	ctx = WithTenant(ctx, customTenant)
	if FromTenant(ctx) != customTenant {
		t.Fatalf("expected custom tenant %s, got %s", customTenant, FromTenant(ctx))
	}

	// 测试 HTTP 中间件
	handler := TenantMiddleware(func(w http.ResponseWriter, r *http.Request) {
		tenant := FromTenant(r.Context())
		w.Write([]byte(tenant))
	})

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set(HeaderTenantID, "tenant_888")
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Body.String() != "tenant_888" {
		t.Fatalf("expected tenant_888 from header, got %s", rec.Body.String())
	}
}

func TestTenantPlugin_CRUD(t *testing.T) {
	db := setupTestDB(t)

	ctxTenantA := WithTenant(context.Background(), "TENANT_A")
	ctxTenantB := WithTenant(context.Background(), "TENANT_B")

	// 1. 测试单条插入自动注入租户 ID
	userA1 := User{Name: "Alice"}
	if err := db.WithContext(ctxTenantA).Create(&userA1).Error; err != nil {
		t.Fatalf("create userA1 failed: %v", err)
	}
	if userA1.TenantID != "TENANT_A" {
		t.Fatalf("expected tenant_id TENANT_A, got %s", userA1.TenantID)
	}

	// 2. 测试批量插入自动注入租户 ID
	usersA := []User{
		{Name: "Bob"},
		{Name: "Charlie"},
	}
	if err := db.WithContext(ctxTenantA).Create(&usersA).Error; err != nil {
		t.Fatalf("batch create usersA failed: %v", err)
	}
	for _, u := range usersA {
		if u.TenantID != "TENANT_A" {
			t.Fatalf("batch item expected tenant_id TENANT_A, got %s", u.TenantID)
		}
	}

	// 3. 租户 B 插入单条
	userB1 := User{Name: "David"}
	if err := db.WithContext(ctxTenantB).Create(&userB1).Error; err != nil {
		t.Fatalf("create userB1 failed: %v", err)
	}

	// 4. 查询隔离验证
	var countA int64
	if err := db.WithContext(ctxTenantA).Model(&User{}).Count(&countA).Error; err != nil {
		t.Fatalf("count A failed: %v", err)
	}
	if countA != 3 {
		t.Fatalf("expected 3 users for TENANT_A, got %d", countA)
	}

	var countB int64
	if err := db.WithContext(ctxTenantB).Model(&User{}).Count(&countB).Error; err != nil {
		t.Fatalf("count B failed: %v", err)
	}
	if countB != 1 {
		t.Fatalf("expected 1 user for TENANT_B, got %d", countB)
	}

	// 5. 更新隔离验证：租户 B 尝试更新租户 A 的用户
	res := db.WithContext(ctxTenantB).Model(&User{}).Where("id = ?", userA1.ID).Update("name", "HackedName")
	if res.Error != nil {
		t.Fatalf("update failed: %v", res.Error)
	}
	if res.RowsAffected != 0 {
		t.Fatalf("expected 0 rows affected when tenant B updates tenant A, got %d", res.RowsAffected)
	}

	// 验证 Alice 名字未被篡改
	var checkA User
	db.WithContext(ctxTenantA).First(&checkA, userA1.ID)
	if checkA.Name != "Alice" {
		t.Fatalf("user was unexpectedly modified to %s", checkA.Name)
	}

	// 6. 删除隔离验证：租户 B 尝试删除租户 A 的用户
	delRes := db.WithContext(ctxTenantB).Where("id = ?", userA1.ID).Delete(&User{})
	if delRes.Error != nil {
		t.Fatalf("delete failed: %v", delRes.Error)
	}
	if delRes.RowsAffected != 0 {
		t.Fatalf("expected 0 rows affected when tenant B deletes tenant A, got %d", delRes.RowsAffected)
	}

	// 7. 白名单表忽略租户过滤验证
	cfg := SysConfig{Key: "site_name", Value: "GoBlade"}
	if err := db.WithContext(ctxTenantA).Create(&cfg).Error; err != nil {
		t.Fatalf("create sys_config failed: %v", err)
	}
	var readCfg SysConfig
	if err := db.WithContext(ctxTenantB).Where("key = ?", "site_name").First(&readCfg).Error; err != nil {
		t.Fatalf("failed to query ignored table across tenant: %v", err)
	}
	if readCfg.Value != "GoBlade" {
		t.Fatalf("unexpected config value: %s", readCfg.Value)
	}
}
