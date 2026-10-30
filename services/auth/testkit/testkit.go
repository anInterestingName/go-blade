// Package testkit 提供认证微服务 (service-auth) 的进程内嵌入式测试装配器，供跨服务全栈 E2E 测试复用。
package testkit

import (
	authv1 "github.com/go-blade/api/proto/auth/v1"
	"github.com/go-blade/services/auth/internal/config"
	"github.com/go-blade/services/auth/internal/model"
	"github.com/go-blade/services/auth/internal/server"
	"github.com/go-blade/services/auth/internal/svc"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"gorm.io/gorm"
)

// NewEmbeddedAuthServer 使用指定的共享 GORM 数据库、Redis 与 JWT 密钥构造真实的 AuthServiceServer 实例并自动迁移表结构。
func NewEmbeddedAuthServer(db *gorm.DB, rds *redis.Redis, jwtSecret string) (authv1.AuthServiceServer, error) {
	if db != nil {
		if err := db.AutoMigrate(&model.User{}, &model.Role{}); err != nil {
			return nil, err
		}
	}
	ctx := svc.NewServiceContext(config.Config{
		Auth: config.AuthConfig{
			AccessSecret:  jwtSecret,
			AccessExpire:  7200,
			RefreshExpire: 86400,
			Issuer:        "blade",
		},
	})
	ctx.DB = db
	ctx.Redis = rds
	return server.NewAuthServer(ctx), nil
}
