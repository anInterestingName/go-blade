// Package rpc 提供统一的 gRPC 客户端与服务端封装，包含上下文 Metadata 传递、错误拦截与连接池管理。
package rpc

import (
	"context"
	"net/url"
	"strconv"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"go.opentelemetry.io/otel"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/metrics"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
)

// 常量定义，与 HTTP Header 及 RPC Metadata 对齐
const (
	// MetadataTenantID 租户标识 Metadata Key
	MetadataTenantID = "blade-tenant-id"
	// MetadataUserID 用户主键 Metadata Key
	MetadataUserID = "blade-user-id"
	// MetadataDeptID 部门主键 Metadata Key
	MetadataDeptID = "blade-dept-id"
	// MetadataRoleID 角色主键列表 Metadata Key
	MetadataRoleID = "blade-role-id"
	// MetadataRoleName 角色名称 Metadata Key
	MetadataRoleName = "blade-role-name"
	// MetadataAccount 账号名称 Metadata Key
	MetadataAccount = "blade-account"
	// MetadataUserName 用户真实姓名 Metadata Key
	MetadataUserName = "blade-user-name"
	// MetadataNickName 用户昵称 Metadata Key
	MetadataNickName = "blade-nick-name"
	// MetadataRequestID 链路追踪请求标识 Key
	MetadataRequestID = "x-request-id"
	// DefaultRoundRobinServiceConfig 默认轮询负载均衡 ServiceConfig JSON 字符串
	DefaultRoundRobinServiceConfig = `{"loadBalancingConfig": [{"round_robin":{}}]}`
)

// ServerConfig gRPC 服务端配置结构体
type ServerConfig struct {
	// ListenOn 监听地址，形如 0.0.0.0:9001
	ListenOn string `json:"listenOn,optional"`
	// Timeout 超时时间 (毫秒)，默认 5000ms
	Timeout int64 `json:"timeout,optional"`
}

// ClientConfig gRPC 客户端配置结构体
type ClientConfig struct {
	// Target 服务目标地址或服务发现 URI，如 127.0.0.1:9001 或 nacos:///service-auth
	Target string `json:"target"`
	// Timeout 超时时间 (毫秒)，默认 5000ms
	Timeout int64 `json:"timeout,optional"`
	// NonBlock 是否非阻塞建立连接
	NonBlock bool `json:"nonBlock,optional"`
	// MaxRetries 瞬态网络故障 (codes.Unavailable) 最大重试次数，默认 0（不重试）
	MaxRetries int `json:"maxRetries,optional"`
	// DisableBreaker 是否禁用默认熔断器（在 DialWithConfig 中默认启用）
	DisableBreaker bool `json:"disableBreaker,optional"`
	// BreakerConfig 自定义熔断器阈值与冷却配置
	BreakerConfig BreakerConfig `json:"breaker,optional"`
	// Breaker 可选注入的自定义熔断器实例
	Breaker *CircuitBreaker `json:"-"`
}

// ServerUnaryInterceptor 返回统一的服务端一元拦截器链：提取 OpenTelemetry TraceContext、租户与用户身份 Claims、异常捕获、Prometheus 指标采集与耗时日志记录。
func ServerUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		startTime := time.Now()

		// 1. 从 Incoming Metadata 提取上游 Trace 上下文、租户信息与用户身份 Claims 并写入 context
		md, ok := metadata.FromIncomingContext(ctx)
		if ok {
			ctx = trace.ExtractGRPCMetadata(ctx, md)
		}

		var span oteltrace.Span
		ctx, span = otel.Tracer("go-blade-rpc").Start(ctx, info.FullMethod, oteltrace.WithSpanKind(oteltrace.SpanKindServer))
		defer span.End()

		if ok {
			resolvedTenant := tenant.DefaultTenantID
			if tenantValues := md.Get(MetadataTenantID); len(tenantValues) > 0 && tenantValues[0] != "" {
				resolvedTenant = tenantValues[0]
			}
			ctx = tenant.WithTenant(ctx, resolvedTenant)

			if reqIDs := md.Get(MetadataRequestID); len(reqIDs) > 0 && reqIDs[0] != "" {
				ctx = context.WithValue(ctx, MetadataRequestID, reqIDs[0])
			} else if traceID := trace.TraceIDFromContext(ctx); traceID != "" {
				ctx = context.WithValue(ctx, MetadataRequestID, traceID)
			}

			if claims := extractClaimsFromMetadata(md, resolvedTenant); claims != nil {
				ctx = jwt.WithClaims(ctx, claims)
			}
		} else {
			ctx = tenant.WithTenant(ctx, tenant.DefaultTenantID)
			if traceID := trace.TraceIDFromContext(ctx); traceID != "" {
				ctx = context.WithValue(ctx, MetadataRequestID, traceID)
			}
		}

		// 2. 异常捕获 (Panic Recover) 与指标/日志打点
		defer func() {
			if r := recover(); r != nil {
				logx.WithContext(ctx).Errorf("[gRPC-PANIC] method: %s, panic: %v", info.FullMethod, r)
				err = status.Errorf(codes.Internal, "internal server error: %v", r)
			}
			duration := time.Since(startTime)
			metrics.RecordGRPC("go-blade-grpc", info.FullMethod, status.Code(err).String(), duration)
			if err != nil {
				logx.WithContext(ctx).Errorf("[gRPC-ERR] %s (%v) -> error: %v", info.FullMethod, duration, err)
			} else {
				logx.WithContext(ctx).Infof("[gRPC-OK] %s (%v)", info.FullMethod, duration)
			}
		}()

		// 3. 执行业务 Handler
		resp, err = handler(ctx, req)

		return resp, err
	}
}

func firstMDValue(md metadata.MD, key string) string {
	if vals := md.Get(key); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func extractClaimsFromMetadata(md metadata.MD, fallbackTenant string) *jwt.BladeClaims {
	userIDStr := firstMDValue(md, MetadataUserID)
	account, _ := url.QueryUnescape(firstMDValue(md, MetadataAccount))
	roleID, _ := url.QueryUnescape(firstMDValue(md, MetadataRoleID))
	roleName, _ := url.QueryUnescape(firstMDValue(md, MetadataRoleName))
	deptIDStr := firstMDValue(md, MetadataDeptID)
	userName, _ := url.QueryUnescape(firstMDValue(md, MetadataUserName))
	nickName, _ := url.QueryUnescape(firstMDValue(md, MetadataNickName))

	var userID int64
	if userIDStr != "" {
		userID, _ = strconv.ParseInt(userIDStr, 10, 64)
	}
	var deptID int64
	if deptIDStr != "" {
		deptID, _ = strconv.ParseInt(deptIDStr, 10, 64)
	}

	if userID <= 0 && account == "" && roleID == "" && roleName == "" {
		return nil
	}

	return &jwt.BladeClaims{
		TenantID: fallbackTenant,
		UserID:   userID,
		DeptID:   deptID,
		RoleID:   roleID,
		RoleName: roleName,
		Account:  account,
		UserName: userName,
		NickName: nickName,
		Category: jwt.TokenTypeAccess,
	}
}

// ClientUnaryInterceptor 返回统一的客户端一元拦截器：开启 Client Span，向 Outgoing Metadata 注入 OpenTelemetry TraceContext、租户标识、BladeClaims 身份与 TraceID。
func ClientUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var span oteltrace.Span
		ctx, span = otel.Tracer("go-blade-rpc").Start(ctx, method, oteltrace.WithSpanKind(oteltrace.SpanKindClient))
		defer span.End()

		claims := jwt.FromClaims(ctx)

		// 1. 获取当前上下文的租户（优先使用显式租户，其次使用 Claims 中的租户）
		tenantID := tenant.FromTenant(ctx)
		if (tenantID == "" || tenantID == tenant.DefaultTenantID) && claims != nil && claims.TenantID != "" {
			tenantID = claims.TenantID
		}
		if tenantID == "" {
			tenantID = tenant.DefaultTenantID
		}

		// 2. 注入 Outgoing Metadata (租户、用户 Claims 与 TraceID，对可能包含中文的字符串做 URL 编码满足 HTTP/2 ASCII 头规范)
		kvPairs := []string{MetadataTenantID, tenantID}
		if claims != nil {
			if claims.UserID > 0 {
				kvPairs = append(kvPairs, MetadataUserID, strconv.FormatInt(claims.UserID, 10))
			}
			if claims.DeptID > 0 {
				kvPairs = append(kvPairs, MetadataDeptID, strconv.FormatInt(claims.DeptID, 10))
			}
			if claims.RoleID != "" {
				kvPairs = append(kvPairs, MetadataRoleID, url.QueryEscape(claims.RoleID))
			}
			if claims.RoleName != "" {
				kvPairs = append(kvPairs, MetadataRoleName, url.QueryEscape(claims.RoleName))
			}
			if claims.Account != "" {
				kvPairs = append(kvPairs, MetadataAccount, url.QueryEscape(claims.Account))
			}
			if claims.UserName != "" {
				kvPairs = append(kvPairs, MetadataUserName, url.QueryEscape(claims.UserName))
			}
			if claims.NickName != "" {
				kvPairs = append(kvPairs, MetadataNickName, url.QueryEscape(claims.NickName))
			}
		}

		if reqID, ok := ctx.Value(MetadataRequestID).(string); ok && reqID != "" {
			kvPairs = append(kvPairs, MetadataRequestID, reqID)
		} else if traceID := trace.TraceIDFromContext(ctx); traceID != "" {
			kvPairs = append(kvPairs, MetadataRequestID, traceID)
		}

		// 注入 W3C TraceContext (traceparent, baggage 等)
		carrierMD := metadata.New(nil)
		trace.InjectGRPCMetadata(ctx, carrierMD)
		for k, vals := range carrierMD {
			for _, v := range vals {
				kvPairs = append(kvPairs, k, v)
			}
		}

		ctx = metadata.AppendToOutgoingContext(ctx, kvPairs...)

		// 3. 调用下游
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// ClientResilienceInterceptor 返回客户端高可用弹性一元拦截器：集成熔断器快速失败、单次调用超时控制与 codes.Unavailable 瞬态重试。
func ClientResilienceInterceptor(cfg ClientConfig) grpc.UnaryClientInterceptor {
	breaker := cfg.Breaker
	timeout := time.Duration(cfg.Timeout) * time.Millisecond
	maxRetries := cfg.MaxRetries
	if maxRetries < 0 {
		maxRetries = 0
	}

	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if breaker != nil {
			if err := breaker.Allow(); err != nil {
				return err
			}
		}

		var lastErr error
		maxAttempts := maxRetries + 1
		for attempt := 0; attempt < maxAttempts; attempt++ {
			callCtx := ctx
			var cancel context.CancelFunc
			if timeout > 0 {
				if _, hasDeadline := ctx.Deadline(); !hasDeadline {
					callCtx, cancel = context.WithTimeout(ctx, timeout)
				}
			}

			lastErr = invoker(callCtx, method, req, reply, cc, opts...)
			if cancel != nil {
				cancel()
			}

			if lastErr == nil {
				break
			}

			// 仅当错误为瞬态 Unavailable、仍有剩余重试次数且父级 Context 未取消时执行重试
			if status.Code(lastErr) != codes.Unavailable || attempt+1 >= maxAttempts || ctx.Err() != nil {
				break
			}
		}

		if breaker != nil {
			breaker.RecordResult(lastErr)
		}
		return lastErr
	}
}

// NewServer 创建并装配默认拦截器的 gRPC Server 实例。
func NewServer(opt ...grpc.ServerOption) *grpc.Server {
	defaultOpts := []grpc.ServerOption{
		grpc.UnaryInterceptor(ServerUnaryInterceptor()),
	}
	defaultOpts = append(defaultOpts, opt...)
	return grpc.NewServer(defaultOpts...)
}

// Dial 创建具有自动注入 Metadata 拦截器与默认 round_robin 负载均衡策略的 gRPC ClientConn 客户端连接。
func Dial(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	defaultOpts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(DefaultRoundRobinServiceConfig),
		grpc.WithChainUnaryInterceptor(ClientUnaryInterceptor()),
	}
	defaultOpts = append(defaultOpts, opts...)
	return grpc.NewClient(target, defaultOpts...)
}

// DialWithConfig 根据 ClientConfig 创建具备 Metadata 注入、round_robin 负载均衡、超时控制、瞬态重试与熔断保护的 gRPC ClientConn 客户端连接。
func DialWithConfig(cfg ClientConfig, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if cfg.Breaker == nil && !cfg.DisableBreaker {
		cfg.Breaker = NewCircuitBreaker(cfg.BreakerConfig)
	}
	defaultOpts := []grpc.DialOption{
		grpc.WithDefaultServiceConfig(DefaultRoundRobinServiceConfig),
		grpc.WithChainUnaryInterceptor(
			ClientUnaryInterceptor(),
			ClientResilienceInterceptor(cfg),
		),
	}
	defaultOpts = append(defaultOpts, opts...)
	return grpc.NewClient(cfg.Target, defaultOpts...)
}
