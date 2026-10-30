// Package model 定义协同工作台服务的数据持久化模型（通知公告等）。
package model

import (
	"time"

	"github.com/go-blade/pkg/snowflake"
)

// Notice 通知公告实体，映射数据库表 `blade_notice`。
type Notice struct {
	// ID 雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// TenantID 所属租户编号
	TenantID string `gorm:"column:tenant_id;type:varchar(12);not null;default:'000000';index" json:"tenant_id"`
	// Title 公告标题
	Title string `gorm:"column:title;type:varchar(255);not null" json:"title"`
	// Category 公告分类
	Category int `gorm:"column:category;type:int;default:1;index" json:"category"`
	// ReleaseTime 发布时间
	ReleaseTime string `gorm:"column:release_time;type:varchar(64)" json:"release_time"`
	// Content 公告正文内容
	Content string `gorm:"column:content;type:text" json:"content"`
	// Status 业务状态 (1: 正常, 2: 停用)
	Status int `gorm:"column:status;type:smallint;default:1" json:"status"`
	// IsDeleted 逻辑删除标记 (0: 正常, 1: 已删除)
	IsDeleted int `gorm:"column:is_deleted;type:smallint;default:0;index" json:"is_deleted"`
	// CreateUser 创建人主键 ID
	CreateUser snowflake.ID `gorm:"column:create_user" json:"create_user"`
	// CreateDept 创建部门主键 ID
	CreateDept snowflake.ID `gorm:"column:create_dept;index" json:"create_dept"`
	// UpdateUser 更新人主键 ID
	UpdateUser snowflake.ID `gorm:"column:update_user" json:"update_user"`
	// CreateTime 创建时间
	CreateTime time.Time `gorm:"column:create_time;autoCreateTime" json:"create_time"`
	// UpdateTime 更新时间
	UpdateTime time.Time `gorm:"column:update_time;autoUpdateTime" json:"update_time"`
}

// TableName 指定通知公告数据表名。
func (Notice) TableName() string {
	return "blade_notice"
}
