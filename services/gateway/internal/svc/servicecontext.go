// Package svc 提供网关服务的全局依赖上下文。
package svc

import (
	"context"
	"fmt"
	"strings"
	"time"

	authv1 "github.com/go-blade/api/proto/auth/v1"
	deskv1 "github.com/go-blade/api/proto/desk/v1"
	systemv1 "github.com/go-blade/api/proto/system/v1"
	"github.com/go-blade/pkg/auditlog"
	"github.com/go-blade/pkg/cache"
	"github.com/go-blade/pkg/discovery"
	"github.com/go-blade/pkg/event"
	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/pkg/oss"
	"github.com/go-blade/pkg/ratelimit"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/sse"
	"github.com/go-blade/services/gateway/internal/config"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ServiceContext 维护网关运行时所需的 gRPC Client、JWT 校验器、限流器、MQ Broker 及 OSS 客户端。
type ServiceContext struct {
	// Config 网关核心配置
	Config config.Config
	// JWTHandler JWT 令牌签发与解析处理器（对称 HMAC）
	JWTHandler *jwt.JWTHandler
	// JWKSValidator RSA JWKS 非对称公钥验签器（可选）
	JWKSValidator *jwt.JWKSValidator
	// TokenValidator 统一双模令牌验签器（组合 JWKS 与 HMAC）
	TokenValidator jwt.TokenValidator
	// RateLimiter 网关限流器（可选，当 RateLimit.Rate > 0 时启用）
	RateLimiter *ratelimit.Limiter
	// PermCache 用户按钮权限码泛型缓存（支持 Redis 与内存双模 + Singleflight 防击穿）
	PermCache *cache.Cache[[]string]
	// OSSProvider 对象存储驱动实例
	OSSProvider oss.Provider
	// MQBroker 消息队列 Broker 实例
	MQBroker mq.Broker
	// SSEHub 网关多租户 Server-Sent Events 实时消息推送中心
	SSEHub *sse.Hub
	// AuditRecorder 网关内存审计日志记录器（便于进程内快速诊断与测试断言）
	AuditRecorder *auditlog.MemoryRecorder
	// AuditPipeline 网关组合审计记录器（同时写入内存 AuditRecorder 与 MQ "blade.audit.api" 主题）
	AuditPipeline auditlog.Recorder
	// AuthClient 下游 Auth 认证微服务 gRPC 客户端
	AuthClient authv1.AuthServiceClient
	authConn   *grpc.ClientConn
	// SystemClient 下游 System 系统微服务 gRPC 客户端
	SystemClient systemv1.SystemServiceClient
	systemConn   *grpc.ClientConn
	// DeskClient 下游 Desk 协同工作台微服务 gRPC 客户端
	DeskClient deskv1.DeskServiceClient
	deskConn   *grpc.ClientConn
}

// NewServiceContext 根据网关配置初始化各服务依赖（支持可选注入自定义 mq.Broker）。
func NewServiceContext(c config.Config, customBroker ...mq.Broker) *ServiceContext {
	jwtHandler, err := jwt.NewJWTHandler(c.Auth.AccessSecret, 2*time.Hour, 24*time.Hour, "blade")
	if err != nil {
		panic(fmt.Sprintf("init jwt handler failed: %v", err))
	}

	var jwksValidator *jwt.JWKSValidator
	var validators []jwt.TokenValidator
	if c.Auth.JWKSURL != "" {
		jwksValidator = jwt.NewJWKSValidator(c.Auth.JWKSURL, time.Hour)
		validators = append(validators, jwksValidator)
	}
	validators = append(validators, jwtHandler)
	tokenValidator := jwt.NewCompositeValidator(validators...)

	// 若配置了 Nacos 注册中心，初始化 Nacos naming client 并注册 gRPC nacos:/// 解析器
	if len(c.Nacos.ServerConfigs) > 0 {
		nacosReg, err := discovery.NewNacosRegistry(c.Nacos)
		if err != nil {
			logx.Errorf("init nacos naming client for resolver failed: %v", err)
		} else if nacosReg.Client() != nil {
			discovery.RegisterNacosResolver(nacosReg.Client(), c.Nacos.Group)
		}
	}

	var limiter *ratelimit.Limiter
	if c.RateLimit.Rate > 0 {
		limiter = ratelimit.NewLimiter(c.RateLimit)
	}

	ossCfg := c.OSS
	if ossCfg.Driver == "" && ossCfg.BaseDir == "" {
		ossCfg.Driver = oss.DriverMemory
	}
	ossProvider, err := oss.NewProvider(ossCfg)
	if err != nil {
		logx.Errorf("init oss provider failed, fallback to memory provider: %v", err)
		ossProvider, _ = oss.NewProvider(oss.Config{Driver: oss.DriverMemory})
	}

	var broker mq.Broker
	if len(customBroker) > 0 && customBroker[0] != nil {
		broker = customBroker[0]
	} else {
		mqCfg := c.MQ
		if mqCfg.ServiceName == "" {
			mqCfg.ServiceName = "service-gateway"
		}
		b, bErr := mq.NewBroker(mqCfg)
		if bErr != nil {
			logx.Errorf("init gateway mq broker failed, fallback to default broker: %v", bErr)
			broker = mq.DefaultBroker()
		} else {
			broker = b
		}
	}

	permCache := cache.NewCache[[]string](cache.Options{
		Prefix:     "blade:gateway:perms:",
		DefaultTTL: 5 * time.Minute,
	})

	memRecorder := auditlog.NewMemoryRecorder()
	mqRecorder := auditlog.NewMQRecorder(broker, auditlog.AuditEventTopic)
	auditPipeline := auditlog.NewMultiRecorder(memRecorder, mqRecorder)
	sseHub := sse.NewHub()

	svcCtx := &ServiceContext{
		Config:         c,
		JWTHandler:     jwtHandler,
		JWKSValidator:  jwksValidator,
		TokenValidator: tokenValidator,
		RateLimiter:    limiter,
		PermCache:      permCache,
		OSSProvider:    ossProvider,
		MQBroker:       broker,
		SSEHub:         sseHub,
		AuditRecorder:  memRecorder,
		AuditPipeline:  auditPipeline,
	}

	svcCtx.RegisterRBACCacheInvalidationSubscriber()
	svcCtx.RegisterNoticeRealtimePushSubscriber()

	// 初始化 Auth 微服务的 gRPC 客户端连接
	if c.Upstreams.Auth.Target != "" {
		conn, err := pkgrpc.DialWithConfig(
			c.Upstreams.Auth,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			panic(fmt.Sprintf("dial auth service upstream %s failed: %v", c.Upstreams.Auth.Target, err))
		}
		svcCtx.authConn = conn
		svcCtx.AuthClient = authv1.NewAuthServiceClient(conn)
	}

	// 初始化 System 微服务的 gRPC 客户端连接
	if c.Upstreams.System.Target != "" {
		conn, err := pkgrpc.DialWithConfig(
			c.Upstreams.System,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			panic(fmt.Sprintf("dial system service upstream %s failed: %v", c.Upstreams.System.Target, err))
		}
		svcCtx.systemConn = conn
		svcCtx.SystemClient = systemv1.NewSystemServiceClient(conn)
	}

	// 初始化 Desk 微服务的 gRPC 客户端连接
	if c.Upstreams.Desk.Target != "" {
		conn, err := pkgrpc.DialWithConfig(
			c.Upstreams.Desk,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			panic(fmt.Sprintf("dial desk service upstream %s failed: %v", c.Upstreams.Desk.Target, err))
		}
		svcCtx.deskConn = conn
		svcCtx.DeskClient = deskv1.NewDeskServiceClient(conn)
	}

	return svcCtx
}

// RegisterRBACCacheInvalidationSubscriber 以全实例广播模式 (mq.WithBroadcast) 订阅 `"blade.system.rbac.changed"` 主题。
// 当 System 服务发生角色授权、角色/菜单修改或用户权限变更时，当前网关实例立即精准失效对应租户（或全量）的 PermCache 缓存。
func (s *ServiceContext) RegisterRBACCacheInvalidationSubscriber() {
	if s == nil || s.MQBroker == nil || s.PermCache == nil {
		return
	}
	_ = s.MQBroker.Subscribe(event.TopicRBACChanged, func(ctx context.Context, msg mq.Message) error {
		var ev event.RBACChangedEvent
		_ = msg.DecodeJSON(&ev)
		prefix := strings.TrimSpace(ev.TenantID)
		if prefix != "" {
			return s.PermCache.DeleteByPrefix(ctx, prefix+":")
		}
		return s.PermCache.DeleteByPrefix(ctx, "")
	}, mq.WithGroup("service-gateway-rbac"), mq.WithBroadcast())
}

// RegisterNoticeRealtimePushSubscriber 以全实例广播模式 (mq.WithBroadcast) 订阅 `"blade.desk.notice.published"` 主题，
// 收到 Desk 服务发布的公告事件后立即通过网关 SSEHub 定向实时推送给对应租户的在线客户端。
func (s *ServiceContext) RegisterNoticeRealtimePushSubscriber() {
	if s == nil || s.MQBroker == nil || s.SSEHub == nil {
		return
	}
	_ = s.MQBroker.Subscribe(event.TopicNoticePublished, func(ctx context.Context, msg mq.Message) error {
		var ev event.NoticePublishedEvent
		if err := msg.DecodeJSON(&ev); err != nil {
			return nil
		}
		s.SSEHub.BroadcastToTenant(ev.TenantID, sse.Event{
			ID:    ev.ID.String(),
			Event: "notice.published",
			Data:  ev,
		})
		return nil
	}, mq.WithGroup("service-gateway-notice"), mq.WithBroadcast())
}

// Close 优雅释放网关持有的外部连接与消息队列资源。
func (s *ServiceContext) Close() {
	if s.MQBroker != nil {
		_ = s.MQBroker.Close(context.Background())
	}
	if s.authConn != nil {
		_ = s.authConn.Close()
	}
	if s.systemConn != nil {
		_ = s.systemConn.Close()
	}
	if s.deskConn != nil {
		_ = s.deskConn.Close()
	}
}
