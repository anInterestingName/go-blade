// Package database 提供 GORM 数据库连接配置、多租户隔离、自动审计与行级数据权限驱动支持。
package database

import (
	"context"
	"strings"

	"github.com/go-blade/pkg/jwt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DataScopeType 定义对标 SpringBlade 的五级行级数据权限隔离类型。
type DataScopeType int

const (
	// DataScopeAll 全部数据权限（不附加任何行级过滤条件）
	DataScopeAll DataScopeType = 1
	// DataScopeCustom 自定义部门数据权限（过滤条件：dept_id IN (?)）
	DataScopeCustom DataScopeType = 2
	// DataScopeDept 本级部门数据权限（过滤条件：dept_id = ?）
	DataScopeDept DataScopeType = 3
	// DataScopeDeptAndChild 本级及子部门数据权限（过滤条件：dept_id IN (?)）
	DataScopeDeptAndChild DataScopeType = 4
	// DataScopeSelf 仅本人数据权限（过滤条件：create_user = ?）
	DataScopeSelf DataScopeType = 5
)

const (
	// DefaultDataScopeDeptColumn 默认部门数据权限过滤列名 ("dept_id")
	DefaultDataScopeDeptColumn = "dept_id"
	// DefaultDataScopeUserColumn 默认个人数据权限过滤列名 ("create_user")
	DefaultDataScopeUserColumn = "create_user"
)

// DataScope 承载当前请求上下文的行级数据权限过滤规则。
type DataScope struct {
	// Type 数据权限级别类型（1:全部, 2:自定义部门, 3:本级部门, 4:本级及子部门, 5:仅本人）
	Type DataScopeType
	// DeptID 当前用户所属本级部门主键（未设置时自动从 BladeClaims.DeptID 回退读取）
	DeptID int64
	// DeptIDs 自定义部门或本级及子部门 ID 列表
	DeptIDs []int64
	// UserID 当前用户主键（未设置时自动从 BladeClaims.UserID 回退读取）
	UserID int64
	// DeptColumn 自定义部门过滤列名（为空时默认使用 "dept_id"，若不存在则尝试回退 "create_dept"）
	DeptColumn string
	// UserColumn 自定义本人数据过滤列名（为空时默认使用 "create_user"）
	UserColumn string
}

type dataScopeCtxKeyType struct{}

var dataScopeCtxKey = dataScopeCtxKeyType{}

// WithDataScope 将指定的行级数据权限规则 DataScope 注入到 context.Context 中。
func WithDataScope(ctx context.Context, scope DataScope) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, dataScopeCtxKey, scope)
}

// FromDataScope 从 context.Context 中提取当前设置的行级数据权限规则 DataScope。
func FromDataScope(ctx context.Context) (DataScope, bool) {
	if ctx == nil {
		return DataScope{}, false
	}
	scope, ok := ctx.Value(dataScopeCtxKey).(DataScope)
	return scope, ok
}

// DataScopePlugin GORM 行级数据权限隔离插件：在查询阶段根据上下文 DataScope 自动追加部门或本人 WHERE 过滤条件，
// 并与 TenantPlugin 多租户物理隔离条件无缝叠加。
type DataScopePlugin struct{}

// NewDataScopePlugin 构造并返回一个 DataScopePlugin 行级数据权限插件实例。
func NewDataScopePlugin() *DataScopePlugin {
	return &DataScopePlugin{}
}

// Name 返回当前数据权限插件在 GORM 中的唯一注册名称。
func (p *DataScopePlugin) Name() string {
	return "blade_datascope_plugin"
}

// Initialize 向 GORM 注册 Query 生命周期的前置行级数据权限过滤钩子。
func (p *DataScopePlugin) Initialize(db *gorm.DB) error {
	_ = db.Callback().Query().Before("gorm:query").Register("blade:datascope_query", p.queryCallback)
	return nil
}

// queryCallback 查询前置钩子：检测上下文 DataScope 与目标表 Schema，自动追加对应的 WHERE 表达式。
func (p *DataScopePlugin) queryCallback(db *gorm.DB) {
	stmt := db.Statement
	if stmt == nil || stmt.Schema == nil || stmt.Context == nil {
		return
	}

	scope, ok := FromDataScope(stmt.Context)
	if !ok || scope.Type == 0 || scope.Type == DataScopeAll {
		return
	}

	claims := jwt.FromClaims(stmt.Context)
	if scope.DeptID <= 0 && claims != nil {
		scope.DeptID = claims.DeptID
	}
	if scope.UserID <= 0 && claims != nil {
		scope.UserID = claims.UserID
	}

	switch scope.Type {
	case DataScopeDept:
		col, exists := resolveDeptColumn(stmt, scope.DeptColumn)
		if !exists {
			return
		}
		stmt.AddClause(clause.Where{
			Exprs: []clause.Expression{
				clause.Eq{
					Column: clause.Column{Table: stmt.Table, Name: col},
					Value:  scope.DeptID,
				},
			},
		})

	case DataScopeCustom, DataScopeDeptAndChild:
		col, exists := resolveDeptColumn(stmt, scope.DeptColumn)
		if !exists {
			return
		}
		deptIDs := scope.DeptIDs
		if len(deptIDs) == 0 && scope.Type == DataScopeDeptAndChild && scope.DeptID > 0 {
			deptIDs = []int64{scope.DeptID}
		}
		if len(deptIDs) == 0 {
			// 若未分配任何可见部门，则附加不可能命中的条件防止越权泄露
			stmt.AddClause(clause.Where{
				Exprs: []clause.Expression{
					clause.Eq{
						Column: clause.Column{Table: stmt.Table, Name: col},
						Value:  int64(-1),
					},
				},
			})
			return
		}
		values := make([]any, len(deptIDs))
		for i, id := range deptIDs {
			values[i] = id
		}
		stmt.AddClause(clause.Where{
			Exprs: []clause.Expression{
				clause.IN{
					Column: clause.Column{Table: stmt.Table, Name: col},
					Values: values,
				},
			},
		})

	case DataScopeSelf:
		col, exists := resolveUserColumn(stmt, scope.UserColumn)
		if !exists {
			return
		}
		stmt.AddClause(clause.Where{
			Exprs: []clause.Expression{
				clause.Eq{
					Column: clause.Column{Table: stmt.Table, Name: col},
					Value:  scope.UserID,
				},
			},
		})
	}
}

// resolveDeptColumn 解析并检查当前模型表结构是否存在部门过滤列。
func resolveDeptColumn(stmt *gorm.Statement, customCol string) (string, bool) {
	col := strings.TrimSpace(customCol)
	if col != "" {
		if stmt.Schema.LookUpField(col) != nil {
			return col, true
		}
		return "", false
	}
	if stmt.Schema.LookUpField(DefaultDataScopeDeptColumn) != nil {
		return DefaultDataScopeDeptColumn, true
	}
	if stmt.Schema.LookUpField(AuditColumnCreateDept) != nil {
		return AuditColumnCreateDept, true
	}
	return "", false
}

// resolveUserColumn 解析并检查当前模型表结构是否存在个人数据过滤列。
func resolveUserColumn(stmt *gorm.Statement, customCol string) (string, bool) {
	col := strings.TrimSpace(customCol)
	if col == "" {
		col = DefaultDataScopeUserColumn
	}
	if stmt.Schema.LookUpField(col) != nil {
		return col, true
	}
	return "", false
}
