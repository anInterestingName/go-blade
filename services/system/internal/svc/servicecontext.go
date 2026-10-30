// Package svc 维护系统微服务的全局依赖与上下文。
package svc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-blade/pkg/auditlog"
	"github.com/go-blade/pkg/cache"
	"github.com/go-blade/pkg/database"
	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/services/system/internal/config"
	"github.com/go-blade/services/system/internal/model"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

// ServiceContext 存放系统微服务的 GORM 数据库、Redis、参数缓存与消息队列 Broker 依赖。
type ServiceContext struct {
	Config     config.Config
	DB         *gorm.DB
	MQBroker   mq.Broker
	ParamCache *cache.Cache[string]
}

// NewServiceContext 初始化 ServiceContext 实例并挂载审计日志 MQ 消费者。
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
			mqCfg.ServiceName = "service-system"
		}
		var err error
		broker, err = mq.NewBroker(mqCfg)
		if err != nil {
			logx.Errorf("[system] init mq broker failed, fallback to default broker: %v", err)
			broker = mq.DefaultBroker()
		}
	}

	paramCache := cache.NewCache[string](cache.Options{
		Prefix:     "blade:system:param:",
		DefaultTTL: 10 * time.Minute,
	})

	svcCtx := &ServiceContext{
		Config:     c,
		DB:         db,
		MQBroker:   broker,
		ParamCache: paramCache,
	}

	svcCtx.RegisterAuditLogConsumer()
	return svcCtx
}

// RegisterAuditLogConsumer 在当前 ServiceContext 的 MQBroker 上注册 `"blade.audit.api"` 集群消费者组，
// 异步消费网关及各服务产生的脱敏操作审计日志并持久化落盘至 `blade_log_api` 表。
func (s *ServiceContext) RegisterAuditLogConsumer() {
	if s == nil || s.MQBroker == nil {
		return
	}
	_ = s.MQBroker.Subscribe(auditlog.AuditEventTopic, func(ctx context.Context, msg mq.Message) error {
		if s.DB == nil {
			return nil
		}
		var entry auditlog.Entry
		if err := msg.DecodeJSON(&entry); err != nil {
			logx.WithContext(ctx).Errorf("[system-audit] decode audit log message failed: %v", err)
			return nil
		}

		id := entry.ID
		if id == 0 {
			id = snowflake.NextID()
		}
		tenantID := strings.TrimSpace(entry.TenantID)
		if tenantID == "" {
			tenantID = tenant.FromTenant(ctx)
		}
		createdAt := entry.CreatedAt
		if createdAt.IsZero() {
			createdAt = time.Now()
		}

		record := model.AuditLog{
			ID:          id,
			TenantID:    tenantID,
			ServiceName: entry.ServiceName,
			UserID:      entry.UserID,
			Account:     entry.Account,
			Method:      entry.Method,
			Path:        entry.Path,
			QueryParams: entry.Query,
			RequestBody: entry.RequestBody,
			StatusCode:  entry.StatusCode,
			DurationMs:  entry.DurationMs,
			ClientIP:    entry.ClientIP,
			TraceID:     entry.TraceID,
			CreatedAt:   createdAt,
		}

		writeCtx := tenant.WithTenant(ctx, tenantID)
		if err := s.DB.WithContext(writeCtx).Create(&record).Error; err != nil {
			logx.WithContext(ctx).Errorf("[system-audit] persist audit log %d failed: %v", record.ID, err)
			return err
		}
		return nil
	}, mq.WithGroup("service-system-audit"))
}
