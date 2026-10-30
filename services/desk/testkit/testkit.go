// Package testkit 提供协同工作台微服务 (service-desk) 的进程内嵌入式测试装配器，供跨服务全栈 E2E 测试复用。
package testkit

import (
	deskv1 "github.com/go-blade/api/proto/desk/v1"
	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/services/desk/internal/config"
	"github.com/go-blade/services/desk/internal/model"
	"github.com/go-blade/services/desk/internal/server"
	"github.com/go-blade/services/desk/internal/svc"
	"gorm.io/gorm"
)

// NewEmbeddedDeskServer 使用指定的共享 GORM 数据库与 MQ Broker 构造真实的 DeskServiceServer 实例并自动迁移公告表模型。
func NewEmbeddedDeskServer(db *gorm.DB, broker mq.Broker) (deskv1.DeskServiceServer, error) {
	if db != nil {
		if err := db.AutoMigrate(&model.Notice{}); err != nil {
			return nil, err
		}
	}
	ctx := svc.NewServiceContext(config.Config{}, broker)
	ctx.DB = db
	return server.NewDeskServer(ctx), nil
}
