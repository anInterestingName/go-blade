// Package model 定义认证服务的数据持久化模型。
package model

import (
	"time"

	"github.com/go-blade/pkg/snowflake"
	"golang.org/x/crypto/bcrypt"
)

// User 系统用户实体，映射数据库表 `blade_user`。
// 主键与关键外键采用 snowflake.ID，序列化至 JSON 时自动转换为字符串，防止前端精度溢出。
type User struct {
	// ID 雪花算法主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// TenantID 所属租户编号，配合 pkg/tenant 插件进行自动物理隔离
	TenantID string `gorm:"column:tenant_id;type:varchar(12);not null;default:'000000'" json:"tenant_id"`
	// Account 登录账号
	Account string `gorm:"column:account;type:varchar(45);not null;index" json:"account"`
	// Password 加密密码（bcrypt 密文）
	Password string `gorm:"column:password;type:varchar(100);not null" json:"-"`
	// Name 用户姓名
	Name string `gorm:"column:name;type:varchar(20);not null" json:"name"`
	// RealName 真实别名
	RealName string `gorm:"column:real_name;type:varchar(20)" json:"real_name"`
	// Avatar 用户头像 URL
	Avatar string `gorm:"column:avatar;type:varchar(500)" json:"avatar"`
	// Email 电子邮箱
	Email string `gorm:"column:email;type:varchar(45)" json:"email"`
	// Phone 联系手机号
	Phone string `gorm:"column:phone;type:varchar(45)" json:"phone"`
	// DeptID 所属部门主键
	DeptID snowflake.ID `gorm:"column:dept_id" json:"dept_id"`
	// RoleID 拥有角色主键（多个以逗号拼接）
	RoleID string `gorm:"column:role_id;type:varchar(255)" json:"role_id"`
	// Status 账号状态（1: 正常, 2: 冻结/禁用）
	Status int `gorm:"column:status;type:smallint;default:1" json:"status"`
	// IsDeleted 逻辑删除标记（0: 正常, 1: 已删除）
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0;index" json:"is_deleted"`
	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	// UpdateTime 最后更新时间
	UpdateTime time.Time `gorm:"column:update_time;autoUpdateTime" json:"update_time"`
}

// TableName 显式指定表名，对标 SpringBlade 默认表名。
func (User) TableName() string {
	return "blade_user"
}

// CheckPassword 使用 bcrypt 比对明文密码与库中哈希值是否匹配。
func (u *User) CheckPassword(plainPassword string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(plainPassword))
	return err == nil
}

// HashPassword 生成 bcrypt 哈希密文。
func HashPassword(plainPassword string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(plainPassword), bcrypt.DefaultCost)
	return string(bytes), err
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
	// RoleAlias 角色别名（如 administrator, user）
	RoleAlias string `gorm:"column:role_alias;type:varchar(50);not null" json:"role_alias"`
	// Sort 排序权重
	Sort int `gorm:"column:sort;type:int;default:0" json:"sort"`
	// IsDeleted 逻辑删除标记（0: 正常, 1: 已删除）
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0;index" json:"is_deleted"`
}

// TableName 显式指定角色表名。
func (Role) TableName() string {
	return "blade_role"
}
