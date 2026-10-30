// Package model 定义系统服务的数据持久化模型（租户、用户、部门、角色、菜单、字典）。
package model

import (
	"time"

	"github.com/go-blade/pkg/snowflake"
)

// Tenant 系统租户实体，映射数据库表 `blade_tenant`。
type Tenant struct {
	// ID 雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// TenantID 租户编号 (如 "000000")
	TenantID string `gorm:"column:tenant_id;type:varchar(12);not null;uniqueIndex" json:"tenant_id"`
	// TenantName 租户名称
	TenantName string `gorm:"column:tenant_name;type:varchar(50);not null" json:"tenant_name"`
	// Linkman 联系人
	Linkman string `gorm:"column:linkman;type:varchar(20)" json:"linkman"`
	// ContactNumber 联系电话
	ContactNumber string `gorm:"column:contact_number;type:varchar(20)" json:"contact_number"`
	// Address 联系地址
	Address string `gorm:"column:address;type:varchar(255)" json:"address"`
	// Status 状态 (1: 正常, 2: 冻结)
	Status int `gorm:"column:status;type:smallint;default:1" json:"status"`
	// IsDeleted 逻辑删除 (0: 正常, 1: 已删除)
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0" json:"is_deleted"`
	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;autoUpdateTime" json:"update_time"`
}

// TableName 指定租户数据表名。
func (Tenant) TableName() string {
	return "blade_tenant"
}

// Menu 菜单与权限按钮实体，映射数据库表 `blade_menu`。
type Menu struct {
	// ID 雪花算法主键
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// ParentID 父级菜单 ID (顶级为 0)
	ParentID snowflake.ID `gorm:"column:parent_id;default:0;index" json:"parent_id"`
	// Code 路由/菜单编码
	Code string `gorm:"column:code;type:varchar(255);not null" json:"code"`
	// Name 菜单显示名称
	Name string `gorm:"column:name;type:varchar(50);not null" json:"name"`
	// Path 路由路径
	Path string `gorm:"column:path;type:varchar(255)" json:"path"`
	// Component 前端组件名/路径
	Component string `gorm:"column:component;type:varchar(255)" json:"component"`
	// Icon 图标类名
	Icon string `gorm:"column:icon;type:varchar(50)" json:"icon"`
	// Sort 排序权重
	Sort int `gorm:"column:sort;type:int;default:0" json:"sort"`
	// Category 类别 (1: 菜单, 2: 按钮权限)
	Category int `gorm:"column:category;type:smallint;default:1" json:"category"`
	// Action 权限操作标识 (如 "user:add")
	Action string `gorm:"column:action;type:varchar(50)" json:"action"`
	// IsDeleted 逻辑删除标记
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0" json:"is_deleted"`
	// CreateUser 创建人 ID
	CreateUser snowflake.ID `gorm:"column:create_user" json:"create_user"`
	// CreateDept 创建部门 ID
	CreateDept snowflake.ID `gorm:"column:create_dept" json:"create_dept"`
	// UpdateUser 更新人 ID
	UpdateUser snowflake.ID `gorm:"column:update_user" json:"update_user"`
	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;autoUpdateTime" json:"update_time"`
}

// TableName 指定菜单数据表名。
func (Menu) TableName() string {
	return "blade_menu"
}

// User 系统用户实体，映射数据库表 `blade_user`。
type User struct {
	// ID 雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// TenantID 所属租户编号
	TenantID string `gorm:"column:tenant_id;type:varchar(12);not null;default:'000000'" json:"tenant_id"`
	// Account 登录账号
	Account string `gorm:"column:account;type:varchar(45);not null;index" json:"account"`
	// Password 加密密码
	Password string `gorm:"column:password;type:varchar(100);not null" json:"-"`
	// Name 用户姓名
	Name string `gorm:"column:name;type:varchar(20);not null" json:"name"`
	// RealName 真实别名
	RealName string `gorm:"column:real_name;type:varchar(20)" json:"real_name"`
	// Email 电子邮箱
	Email string `gorm:"column:email;type:varchar(45)" json:"email"`
	// Phone 联系手机号
	Phone string `gorm:"column:phone;type:varchar(45)" json:"phone"`
	// DeptID 所属部门主键
	DeptID snowflake.ID `gorm:"column:dept_id" json:"dept_id"`
	// RoleID 拥有角色主键（多个以逗号拼接）
	RoleID string `gorm:"column:role_id;type:varchar(255)" json:"role_id"`
	// Status 账号状态 (1: 正常, 2: 冻结)
	Status int `gorm:"column:status;type:smallint;default:1" json:"status"`
	// IsDeleted 逻辑删除 (0: 正常, 1: 已删除)
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0;index" json:"is_deleted"`
	// CreateUser 创建人 ID
	CreateUser snowflake.ID `gorm:"column:create_user" json:"create_user"`
	// CreateDept 创建部门 ID
	CreateDept snowflake.ID `gorm:"column:create_dept" json:"create_dept"`
	// UpdateUser 更新人 ID
	UpdateUser snowflake.ID `gorm:"column:update_user" json:"update_user"`
	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;autoUpdateTime" json:"update_time"`
}

// TableName 指定用户数据表名。
func (User) TableName() string {
	return "blade_user"
}

// Role 系统角色实体，映射数据库表 `blade_role`。
type Role struct {
	// ID 雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// TenantID 所属租户编号
	TenantID string `gorm:"column:tenant_id;type:varchar(12);not null;default:'000000'" json:"tenant_id"`
	// ParentID 父角色主键 ID
	ParentID snowflake.ID `gorm:"column:parent_id;default:0" json:"parent_id"`
	// RoleName 角色名称
	RoleName string `gorm:"column:role_name;type:varchar(50);not null" json:"role_name"`
	// RoleAlias 角色别名
	RoleAlias string `gorm:"column:role_alias;type:varchar(50);not null" json:"role_alias"`
	// DataScopeType 数据权限类型 (1:全部数据, 2:自定义部门, 3:本级部门, 4:本级及子部门, 5:仅本人数据)
	DataScopeType int `gorm:"column:data_scope_type;default:1" json:"data_scope_type"`
	// DataScopeDeptIDs 自定义数据权限部门主键列表 (逗号分隔，当 DataScopeType=2 时生效)
	DataScopeDeptIDs string `gorm:"column:data_scope_dept_ids;type:varchar(1000);default:''" json:"data_scope_dept_ids"`
	// Sort 排序权重
	Sort int `gorm:"column:sort;type:int;default:0" json:"sort"`
	// IsDeleted 逻辑删除 (0: 正常, 1: 已删除)
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0;index" json:"is_deleted"`
	// CreateUser 创建人 ID
	CreateUser snowflake.ID `gorm:"column:create_user" json:"create_user"`
	// CreateDept 创建部门 ID
	CreateDept snowflake.ID `gorm:"column:create_dept" json:"create_dept"`
	// UpdateUser 更新人 ID
	UpdateUser snowflake.ID `gorm:"column:update_user" json:"update_user"`
	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;autoUpdateTime" json:"update_time"`
}

// TableName 指定角色数据表名。
func (Role) TableName() string {
	return "blade_role"
}

// RoleMenu 角色与菜单权限关联实体，映射数据库表 `blade_role_menu`。
type RoleMenu struct {
	// ID 雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// RoleID 角色主键 ID
	RoleID snowflake.ID `gorm:"column:role_id;not null;index" json:"role_id"`
	// MenuID 菜单或按钮主键 ID
	MenuID snowflake.ID `gorm:"column:menu_id;not null;index" json:"menu_id"`
}

// TableName 指定角色菜单关联数据表名。
func (RoleMenu) TableName() string {
	return "blade_role_menu"
}

// Dept 系统部门实体，映射数据库表 `blade_dept`。
type Dept struct {
	// ID 雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// TenantID 所属租户编号
	TenantID string `gorm:"column:tenant_id;type:varchar(12);not null;default:'000000'" json:"tenant_id"`
	// ParentID 父部门主键 ID (顶级为 0)
	ParentID snowflake.ID `gorm:"column:parent_id;default:0;index" json:"parent_id"`
	// DeptName 部门简称
	DeptName string `gorm:"column:dept_name;type:varchar(45);not null" json:"dept_name"`
	// FullName 部门全称
	FullName string `gorm:"column:full_name;type:varchar(45)" json:"full_name"`
	// Sort 排序权重
	Sort int `gorm:"column:sort;type:int;default:0" json:"sort"`
	// Remark 备注
	Remark string `gorm:"column:remark;type:varchar(255)" json:"remark"`
	// IsDeleted 逻辑删除 (0: 正常, 1: 已删除)
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0;index" json:"is_deleted"`
	// CreateUser 创建人 ID
	CreateUser snowflake.ID `gorm:"column:create_user" json:"create_user"`
	// CreateDept 创建部门 ID
	CreateDept snowflake.ID `gorm:"column:create_dept" json:"create_dept"`
	// UpdateUser 更新人 ID
	UpdateUser snowflake.ID `gorm:"column:update_user" json:"update_user"`
	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;autoUpdateTime" json:"update_time"`
}

// TableName 指定部门数据表名。
func (Dept) TableName() string {
	return "blade_dept"
}

// Dict 系统字典实体，映射数据库表 `blade_dict`。
type Dict struct {
	// ID 雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// ParentID 父字典主键 ID (顶级分类为 0)
	ParentID snowflake.ID `gorm:"column:parent_id;default:0;index" json:"parent_id"`
	// Code 字典编码
	Code string `gorm:"column:code;type:varchar(255);not null;index" json:"code"`
	// DictKey 字典键值 (顶级分类一般为 "-1")
	DictKey string `gorm:"column:dict_key;type:varchar(255)" json:"dict_key"`
	// DictValue 字典名称/显示值
	DictValue string `gorm:"column:dict_value;type:varchar(255);not null" json:"dict_value"`
	// Sort 排序权重
	Sort int `gorm:"column:sort;type:int;default:0" json:"sort"`
	// Remark 备注
	Remark string `gorm:"column:remark;type:varchar(255)" json:"remark"`
	// IsDeleted 逻辑删除 (0: 正常, 1: 已删除)
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0;index" json:"is_deleted"`
	// CreateUser 创建人 ID
	CreateUser snowflake.ID `gorm:"column:create_user" json:"create_user"`
	// UpdateUser 更新人 ID
	UpdateUser snowflake.ID `gorm:"column:update_user" json:"update_user"`
	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;autoUpdateTime" json:"update_time"`
}

// TableName 指定字典数据表名。
func (Dict) TableName() string {
	return "blade_dict"
}

// AuditLog API 操作审计日志持久化实体，映射数据库表 `blade_log_api`。
type AuditLog struct {
	// ID 审计日志雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// TenantID 租户编号
	TenantID string `gorm:"column:tenant_id;type:varchar(12);not null;default:'000000';index:idx_log_api_tenant_created" json:"tenant_id"`
	// ServiceName 产生审计日志的服务名称
	ServiceName string `gorm:"column:service_name;type:varchar(64);not null;default:''" json:"service_name"`
	// UserID 操作用户主键 ID
	UserID snowflake.ID `gorm:"column:user_id;default:0;index:idx_log_api_user_id" json:"user_id"`
	// Account 操作人登录账号
	Account string `gorm:"column:account;type:varchar(45);default:''" json:"account"`
	// Method HTTP 请求方法 (POST/PUT/DELETE/PATCH)
	Method string `gorm:"column:method;type:varchar(16);not null;default:''" json:"method"`
	// Path HTTP 请求路径
	Path string `gorm:"column:path;type:varchar(255);not null;default:''" json:"path"`
	// QueryParams URL 查询参数字符串
	QueryParams string `gorm:"column:query_params;type:varchar(1024);default:''" json:"query_params"`
	// RequestBody 敏感字段掩码脱敏后的请求体摘要
	RequestBody string `gorm:"column:request_body;type:text" json:"request_body"`
	// StatusCode HTTP 响应状态码
	StatusCode int `gorm:"column:status_code;default:200" json:"status_code"`
	// DurationMs 请求处理耗时 (毫秒)
	DurationMs int64 `gorm:"column:duration_ms;default:0" json:"duration_ms"`
	// ClientIP 客户端真实 IP 地址
	ClientIP string `gorm:"column:client_ip;type:varchar(64);default:''" json:"client_ip"`
	// TraceID 分布式链路追踪 TraceID
	TraceID string `gorm:"column:trace_id;type:varchar(64);default:'';index:idx_log_api_trace_id" json:"trace_id"`
	// CreatedAt 审计记录生成时间
	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime;index:idx_log_api_tenant_created" json:"created_at"`
}

// TableName 指定 API 操作审计日志数据表名。
func (AuditLog) TableName() string {
	return "blade_log_api"
}

// Param 系统动态参数实体，映射数据库表 `blade_param`。
type Param struct {
	// ID 参数雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// TenantID 所属租户编号
	TenantID string `gorm:"column:tenant_id;type:varchar(12);not null;default:'000000';index:idx_param_tenant_key" json:"tenant_id"`
	// ParamName 参数名称
	ParamName string `gorm:"column:param_name;type:varchar(100);not null" json:"param_name"`
	// ParamKey 参数键名
	ParamKey string `gorm:"column:param_key;type:varchar(100);not null;index:idx_param_tenant_key" json:"param_key"`
	// ParamValue 参数键值
	ParamValue string `gorm:"column:param_value;type:varchar(500);not null;default:''" json:"param_value"`
	// Remark 参数备注
	Remark string `gorm:"column:remark;type:varchar(255);default:''" json:"remark"`
	// Status 状态 (1: 正常, 2: 停用)
	Status int `gorm:"column:status;type:smallint;default:1" json:"status"`
	// IsDeleted 逻辑删除 (0: 正常, 1: 已删除)
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0;index" json:"is_deleted"`
	// CreateUser 创建人 ID
	CreateUser snowflake.ID `gorm:"column:create_user" json:"create_user"`
	// CreateDept 创建部门 ID
	CreateDept snowflake.ID `gorm:"column:create_dept" json:"create_dept"`
	// UpdateUser 更新人 ID
	UpdateUser snowflake.ID `gorm:"column:update_user" json:"update_user"`
	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;autoUpdateTime" json:"update_time"`
}

// TableName 指定系统动态参数数据表名。
func (Param) TableName() string {
	return "blade_param"
}
