// Package svc 维护协同工作台微服务的全局依赖与上下文。
package svc

import (
	"fmt"

	"github.com/go-blade/pkg/database"
	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/pkg/redis"
	"github.com/go-blade/services/desk/internal/config"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// ServiceContext 存放协同工作台微服务的 GORM 数据库、Redis 与消息队列 Broker 依赖。
type ServiceContext struct {
	Config   config.Config
	DB       *gorm.DB
	MQBroker mq.Broker
}

// NewServiceContext 初始化 ServiceContext 实例。
func NewServiceContext(c config.Config, customBroker ...mq.Broker) *ServiceContext {
	var db *gorm.DB
	if c.DB.Host != "" || c.DB.DSN != "" {
		var err error
		db, err = database.InitGorm(c.DB)
		if err != nil {
			panic(fmt.Sprintf("init database failed: %v", err))
		}
	}

	if c.Redis.Host != "" {
		_, _ = redis.InitRedis(c.Redis)
	}

	var broker mq.Broker
	if len(customBroker) > 0 && customBroker[0] != nil {
		broker = customBroker[0]
	} else {
		mqCfg := c.MQ
		if mqCfg.ServiceName == "" {
			mqCfg.ServiceName = "service-desk"
		}
		var err error
		broker, err = mq.NewBroker(mqCfg)
		if err != nil {
			logx.Errorf("[desk] init mq broker failed, fallback to default broker: %v", err)
			broker = mq.DefaultBroker()
		}
	}

	return &ServiceContext{
		Config:   c,
		DB:       db,
		MQBroker: broker,
	}
}
