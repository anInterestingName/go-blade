// Package database 提供 GORM 数据库连接配置、多租户隔离、自动审计与行级数据权限驱动支持。
package database

import (
	"context"
	"reflect"
	"time"

	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/snowflake"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

const (
	// AuditColumnCreateUser 创建人 ID 标准列名 ("create_user")
	AuditColumnCreateUser = "create_user"
	// AuditColumnCreateDept 创建部门 ID 标准列名 ("create_dept")
	AuditColumnCreateDept = "create_dept"
	// AuditColumnCreateTime 创建时间标准列名 ("create_time")
	AuditColumnCreateTime = "create_time"
	// AuditColumnUpdateUser 更新人 ID 标准列名 ("update_user")
	AuditColumnUpdateUser = "update_user"
	// AuditColumnUpdateTime 更新时间标准列名 ("update_time")
	AuditColumnUpdateTime = "update_time"
	// AuditColumnIsDeleted 逻辑删除标准列名 ("is_deleted")
	AuditColumnIsDeleted = "is_deleted"
	// AuditColumnStatus 业务状态标准列名 ("status")
	AuditColumnStatus = "status"
)

// AuditPlugin GORM 自动审计插件：在数据创建与更新阶段，自动从上下文 BladeClaims 提取当前登录用户身份，
// 填充 create_user、create_dept、update_user、create_time、update_time、is_deleted 与 status 等标准审计字段。
type AuditPlugin struct{}

// NewAuditPlugin 构造并返回一个 AuditPlugin 自动审计插件实例。
func NewAuditPlugin() *AuditPlugin {
	return &AuditPlugin{}
}

// Name 返回当前审计插件在 GORM 中的唯一注册名称。
func (p *AuditPlugin) Name() string {
	return "blade_audit_plugin"
}

// Initialize 向 GORM 注册 Create 与 Update 生命周期的前置审计钩子。
func (p *AuditPlugin) Initialize(db *gorm.DB) error {
	_ = db.Callback().Create().Before("gorm:create").Register("blade:audit_create", p.createCallback)
	_ = db.Callback().Update().Before("gorm:update").Register("blade:audit_update", p.updateCallback)
	return nil
}

// createCallback 插入前置钩子：支持单条结构体与批量切片/数组模型插入的审计字段自动填充。
func (p *AuditPlugin) createCallback(db *gorm.DB) {
	stmt := db.Statement
	if stmt == nil || stmt.Schema == nil {
		return
	}

	claims := jwt.FromClaims(stmt.Context)
	now := time.Now()

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
			p.applyCreateAuditToStruct(stmt.Context, stmt.Schema, val.Index(i), claims, now)
		}
	case reflect.Struct:
		p.applyCreateAuditToStruct(stmt.Context, stmt.Schema, val, claims, now)
	}
}

// applyCreateAuditToStruct 对单个结构体实例按需填充创建期审计字段（仅在字段为零值时注入，不覆盖显式赋值）。
func (p *AuditPlugin) applyCreateAuditToStruct(
	ctx context.Context,
	s *schema.Schema,
	val reflect.Value,
	claims *jwt.BladeClaims,
	now time.Time,
) {
	for val.Kind() == reflect.Pointer || val.Kind() == reflect.Interface {
		if val.IsNil() {
			return
		}
		val = val.Elem()
	}
	if !val.IsValid() || val.Kind() != reflect.Struct {
		return
	}

	if claims != nil {
		if claims.UserID > 0 {
			setIntFieldIfZero(ctx, s.LookUpField(AuditColumnCreateUser), val, claims.UserID)
			setIntFieldIfZero(ctx, s.LookUpField(AuditColumnUpdateUser), val, claims.UserID)
		}
		if claims.DeptID > 0 {
			setIntFieldIfZero(ctx, s.LookUpField(AuditColumnCreateDept), val, claims.DeptID)
		}
	}

	setTimeFieldIfZero(ctx, s.LookUpField(AuditColumnCreateTime), val, now)
	setTimeFieldIfZero(ctx, s.LookUpField(AuditColumnUpdateTime), val, now)

	// 初始化逻辑删除标记 is_deleted = 0 与默认状态 status = 1（若为零值）
	setIntFieldIfZero(ctx, s.LookUpField(AuditColumnIsDeleted), val, 0)
	setIntFieldIfZero(ctx, s.LookUpField(AuditColumnStatus), val, 1)
}

// updateCallback 更新前置钩子：自动注入或刷新 update_user 与 update_time。
func (p *AuditPlugin) updateCallback(db *gorm.DB) {
	stmt := db.Statement
	if stmt == nil || stmt.Schema == nil {
		return
	}

	claims := jwt.FromClaims(stmt.Context)
	now := time.Now()

	// 1. 若更新目标结构体中包含对应字段且为零值，先尝试写入反射结构
	val := stmt.ReflectValue
	for val.Kind() == reflect.Pointer || val.Kind() == reflect.Interface {
		if val.IsNil() {
			break
		}
		val = val.Elem()
	}

	if fUpdateUser := stmt.Schema.LookUpField(AuditColumnUpdateUser); fUpdateUser != nil && claims != nil && claims.UserID > 0 {
		shouldSet := true
		if val.IsValid() && val.Kind() == reflect.Struct {
			if _, isZero := fUpdateUser.ValueOf(stmt.Context, val); !isZero {
				shouldSet = false
			} else {
				setIntFieldIfZero(stmt.Context, fUpdateUser, val, claims.UserID)
			}
		}
		if shouldSet && !hasDestUpdateColumn(stmt.Dest, AuditColumnUpdateUser, fUpdateUser.Name) {
			stmt.SetColumn(AuditColumnUpdateUser, convertIntValueForField(fUpdateUser, claims.UserID), true)
		}
	}

	if fUpdateTime := stmt.Schema.LookUpField(AuditColumnUpdateTime); fUpdateTime != nil {
		shouldSet := true
		if val.IsValid() && val.Kind() == reflect.Struct {
			if _, isZero := fUpdateTime.ValueOf(stmt.Context, val); !isZero {
				shouldSet = false
			} else {
				setTimeFieldIfZero(stmt.Context, fUpdateTime, val, now)
			}
		}
		if shouldSet && !hasDestUpdateColumn(stmt.Dest, AuditColumnUpdateTime, fUpdateTime.Name) {
			stmt.SetColumn(AuditColumnUpdateTime, now, true)
		}
	}
}

// hasDestUpdateColumn 检查 stmt.Dest（如 map[string]any 或 struct）是否已显式指定了目标更新列。
func hasDestUpdateColumn(dest any, colName, fieldName string) bool {
	if dest == nil {
		return false
	}
	switch d := dest.(type) {
	case map[string]any:
		if v, ok := d[colName]; ok && !isZeroAny(v) {
			return true
		}
		if v, ok := d[fieldName]; ok && !isZeroAny(v) {
			return true
		}
		return false
	default:
		rv := reflect.ValueOf(dest)
		for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
			if rv.IsNil() {
				return false
			}
			rv = rv.Elem()
		}
		if rv.Kind() == reflect.Struct {
			f := rv.FieldByName(fieldName)
			if f.IsValid() && !f.IsZero() {
				return true
			}
		}
	}
	return false
}

func isZeroAny(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return true
	}
	return rv.IsZero()
}

// convertIntValueForField 将 int64 审计值转换为目标字段匹配的底层 Go 类型（兼容 snowflake.ID、int64、int 等）。
func convertIntValueForField(field *schema.Field, v int64) any {
	if field == nil || field.FieldType == nil {
		return v
	}
	ft := field.FieldType
	for ft.Kind() == reflect.Pointer {
		ft = ft.Elem()
	}
	if ft == reflect.TypeOf(snowflake.ID(0)) {
		return snowflake.ID(v)
	}
	switch ft.Kind() {
	case reflect.Int64:
		return reflect.ValueOf(v).Convert(ft).Interface()
	case reflect.Int:
		return int(v)
	case reflect.Int32:
		return int32(v)
	case reflect.Uint64:
		return uint64(v)
	case reflect.Uint:
		return uint(v)
	default:
		return v
	}
}

// setIntFieldIfZero 当目标整型字段存在且当前为零值时，自动注入指定整型值（兼容 int64、snowflake.ID、int、int32 等）。
func setIntFieldIfZero(ctx context.Context, field *schema.Field, structVal reflect.Value, target int64) {
	if field == nil || !structVal.IsValid() {
		return
	}
	_, isZero := field.ValueOf(ctx, structVal)
	if !isZero {
		return
	}
	_ = field.Set(ctx, structVal, convertIntValueForField(field, target))
}

// setTimeFieldIfZero 当目标时间字段存在且当前为零值时，自动注入指定时间。
func setTimeFieldIfZero(ctx context.Context, field *schema.Field, structVal reflect.Value, now time.Time) {
	if field == nil || !structVal.IsValid() {
		return
	}
	_, isZero := field.ValueOf(ctx, structVal)
	if !isZero {
		return
	}
	_ = field.Set(ctx, structVal, now)
}
