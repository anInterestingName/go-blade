// Package testkit 提供系统管理微服务 (service-system) 的进程内嵌入式测试装配器，供跨服务全栈 E2E 测试复用。
package testkit

import (
	systemv1 "github.com/go-blade/api/proto/system/v1"
	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/services/system/internal/config"
	"github.com/go-blade/services/system/internal/model"
	"github.com/go-blade/services/system/internal/server"
	"github.com/go-blade/services/system/internal/svc"
	"gorm.io/gorm"
)

// NewEmbeddedSystemServer 使用指定的共享 GORM 数据库与 MQ Broker 构造真实的 SystemServiceServer 实例，
// 自动迁移全部系统管理表模型并挂载 "blade.audit.api" 异步审计日志落库消费者。
func NewEmbeddedSystemServer(db *gorm.DB, broker mq.Broker) (systemv1.SystemServiceServer, error) {
	if db != nil {
		err := db.AutoMigrate(
			&model.Tenant{},
			&model.Role{},
			&model.Dept{},
			&model.User{},
			&model.Menu{},
			&model.RoleMenu{},
			&model.Dict{},
			&model.AuditLog{},
			&model.Param{},
		)
		if err != nil {
			return nil, err
		}
	}
	ctx := svc.NewServiceContext(config.Config{}, broker)
	ctx.DB = db
	return server.NewSystemServer(ctx), nil
}
