// Package tenant 提供多租户上下文传递与基于 GORM 的无感数据库租户物理隔离能力。
package tenant

import (
	"context"
	"net/http"
	"reflect"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

type tenantKeyType struct{}

var tenantCtxKey = tenantKeyType{}

const (
	// DefaultTenantID SpringBlade 默认系统顶级租户编号 ("000000")
	DefaultTenantID = "000000"
	// HeaderTenantID HTTP 请求头中携带租户标识的键名 ("Blade-Tenant-Id")
	HeaderTenantID = "Blade-Tenant-Id"
	// TenantColumnName 数据库表中存储租户编号的统一列名 ("tenant_id")
	TenantColumnName = "tenant_id"
)

// WithTenant 将指定的租户编号注入到 context.Context 中并返回衍生上下文。
func WithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantCtxKey, tenantID)
}

// FromTenant 从 context.Context 中提取租户编号。
// 如果上下文未设置租户信息，则兜底返回 DefaultTenantID ("000000")。
func FromTenant(ctx context.Context) string {
	if ctx == nil {
		return DefaultTenantID
	}
	if v, ok := ctx.Value(tenantCtxKey).(string); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return DefaultTenantID
}

// TenantMiddleware HTTP 中间件：从请求头 Blade-Tenant-Id 读取租户并写入当前请求的上下文。
func TenantMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := r.Header.Get(HeaderTenantID)
		if strings.TrimSpace(tenantID) == "" {
			tenantID = DefaultTenantID
		}
		ctx := WithTenant(r.Context(), tenantID)
		next(w, r.WithContext(ctx))
	}
}

// TenantEntity 租户实体通用契约接口，标记支持多租户隔离的模型实体。
type TenantEntity interface {
	// GetTenantID 获取当前实体的租户标识
	GetTenantID() string
	// SetTenantID 为当前实体赋予租户标识
	SetTenantID(tenantID string)
}

// TenantPlugin GORM 多租户隔离插件：自动在 CRUD 阶段附加 tenant_id 过滤或赋值条件。
type TenantPlugin struct {
	// IgnoreTables 声明跳过多租户隔离判定的白名单表名集合（全部以小写比对）
	IgnoreTables map[string]struct{}
}

// NewTenantPlugin 构造并返回一个 TenantPlugin 实例。
// ignoreTables: 可选的免租户隔离白名单数据表名（系统默认已忽略租户主表 blade_tenant）。
func NewTenantPlugin(ignoreTables ...string) *TenantPlugin {
	ignores := map[string]struct{}{
		"blade_tenant":    {},
		"tenant":          {},
		"blade_mq_outbox": {},
		"mq_outbox":       {},
	}
	for _, table := range ignoreTables {
		ignores[strings.ToLower(table)] = struct{}{}
	}
	return &TenantPlugin{IgnoreTables: ignores}
}

// Name 返回当前插件在 GORM 中的唯一注册标识。
func (p *TenantPlugin) Name() string {
	return "blade_tenant_plugin"
}

// Initialize 向 GORM 注册 Query、Create、Update、Delete 四大生命周期的租户回调钩子。
func (p *TenantPlugin) Initialize(db *gorm.DB) error {
	// 注册 Query 回调
	_ = db.Callback().Query().Before("gorm:query").Register("blade:tenant_query", p.queryCallback)
	// 注册 Create 回调
	_ = db.Callback().Create().Before("gorm:create").Register("blade:tenant_create", p.createCallback)
	// 注册 Update 回调
	_ = db.Callback().Update().Before("gorm:update").Register("blade:tenant_update", p.updateCallback)
	// 注册 Delete 回调
	_ = db.Callback().Delete().Before("gorm:delete").Register("blade:tenant_delete", p.deleteCallback)
	return nil
}

// isTableIgnored 判断指定表名是否在忽略租户过滤的白名单中。
func (p *TenantPlugin) isTableIgnored(tableName string) bool {
	if p.IgnoreTables == nil {
		return false
	}
	_, ok := p.IgnoreTables[strings.ToLower(tableName)]
	return ok
}

// hasTenantColumn 检查当前模型对应的表结构是否包含 tenant_id 列。
func hasTenantColumn(stmt *gorm.Statement) bool {
	if stmt == nil || stmt.Schema == nil {
		return false
	}
	if _, ok := stmt.Schema.FieldsByName["TenantID"]; ok {
		return true
	}
	if _, ok := stmt.Schema.FieldsByName["TenantId"]; ok {
		return true
	}
	return stmt.Schema.LookUpField(TenantColumnName) != nil
}

// applyTenantWhere 向 Statement 中附加 tenant_id 隔离条件。
func (p *TenantPlugin) applyTenantWhere(stmt *gorm.Statement) {
	if stmt == nil || stmt.Schema == nil || p.isTableIgnored(stmt.Table) {
		return
	}
	if !hasTenantColumn(stmt) {
		return
	}

	tenantID := FromTenant(stmt.Context)
	stmt.AddClause(clause.Where{
		Exprs: []clause.Expression{
			clause.Eq{
				Column: clause.Column{Table: stmt.Table, Name: TenantColumnName},
				Value:  tenantID,
			},
		},
	})
}

// queryCallback 查询钩子：当模型包含 tenant_id 时自动追加租户过滤条件。
func (p *TenantPlugin) queryCallback(db *gorm.DB) {
	p.applyTenantWhere(db.Statement)
}

// createCallback 插入钩子：当插入单条或批量记录包含 tenant_id 时，若未显式赋值则自动注入上下文中的租户 ID。
func (p *TenantPlugin) createCallback(db *gorm.DB) {
	stmt := db.Statement
	if stmt == nil || stmt.Schema == nil || p.isTableIgnored(stmt.Table) {
		return
	}

	field := stmt.Schema.LookUpField(TenantColumnName)
	if field == nil {
		return
	}

	tenantID := FromTenant(stmt.Context)
	val := stmt.ReflectValue

	for val.Kind() == reflect.Pointer || val.Kind() == reflect.Interface {
		if val.IsNil() {
			return
		}
		val = val.Elem()
	}

	switch val.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < val.Len(); i++ {
			elem := val.Index(i)
			p.setTenantIfEmpty(stmt.Context, field, elem, tenantID)
		}
	case reflect.Struct:
		p.setTenantIfEmpty(stmt.Context, field, val, tenantID)
	}
}

// setTenantIfEmpty 若指定结构体实例中的 tenant_id 为空，则自动写入 tenantID。
func (p *TenantPlugin) setTenantIfEmpty(ctx context.Context, field *schema.Field, val reflect.Value, tenantID string) {
	for val.Kind() == reflect.Pointer || val.Kind() == reflect.Interface {
		if val.IsNil() {
			return
		}
		val = val.Elem()
	}
	if !val.IsValid() || val.Kind() != reflect.Struct {
		return
	}

	currentVal, isZero := field.ValueOf(ctx, val)
	if isZero || currentVal == nil || currentVal == "" {
		_ = field.Set(ctx, val, tenantID)
	}
}

// updateCallback 更新钩子：更新记录时限制仅能更新属于当前租户的数据。
func (p *TenantPlugin) updateCallback(db *gorm.DB) {
	p.applyTenantWhere(db.Statement)
}

// deleteCallback 删除钩子：删除记录时限制仅能删除属于当前租户的数据。
func (p *TenantPlugin) deleteCallback(db *gorm.DB) {
	p.applyTenantWhere(db.Statement)
}
