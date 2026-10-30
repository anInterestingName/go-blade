// Package svc 维护认证微服务的运行时上下文与依赖组件。
package svc

import (
	"fmt"
	"time"

	"github.com/go-blade/pkg/database"
	"github.com/go-blade/pkg/jwt"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/services/auth/internal/config"
	"github.com/go-blade/services/auth/internal/hydra"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/gorm"
)

// ServiceContext 认证服务的上下文依赖结构体。
type ServiceContext struct {
	Config      config.Config
	JWTHandler  *jwt.JWTHandler
	Redis       *redis.Redis
	DB          *gorm.DB
	HydraClient hydra.AdminClient
}

// NewServiceContext 根据配置初始化 ServiceContext 实例。
func NewServiceContext(c config.Config) *ServiceContext {
	accessExpire := time.Duration(c.Auth.AccessExpire) * time.Second
	if accessExpire <= 0 {
		accessExpire = 24 * time.Hour
	}
	refreshExpire := time.Duration(c.Auth.RefreshExpire) * time.Second
	if refreshExpire <= 0 {
		refreshExpire = 7 * 24 * time.Hour
	}

	jwtHandler, err := jwt.NewJWTHandler(c.Auth.AccessSecret, accessExpire, refreshExpire, c.Auth.Issuer)
	if err != nil {
		panic(err)
	}

	var rds *redis.Redis
	if c.Redis.Host != "" {
		rds, _ = pkgredis.GetRedis()
	}

	var db *gorm.DB
	if c.DB.Host != "" || c.DB.DSN != "" {
		var err error
		db, err = database.InitGorm(c.DB)
		if err != nil {
			panic(fmt.Sprintf("init database failed: %v", err))
		}
	}

	var hydraClient hydra.AdminClient
	if c.Auth.HydraAdminURL != "" {
		hydraClient = hydra.NewHTTPAdminClient(c.Auth.HydraAdminURL)
	}

	return &ServiceContext{
		Config:      c,
		JWTHandler:  jwtHandler,
		Redis:       rds,
		DB:          db,
		HydraClient: hydraClient,
	}
}
