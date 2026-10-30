// Package main 是认证微服务的启动入口。
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"sync"

	authv1 "github.com/go-blade/api/proto/auth/v1"
	pkgconfig "github.com/go-blade/pkg/config"
	"github.com/go-blade/pkg/discovery"
	"github.com/go-blade/pkg/health"
	"github.com/go-blade/pkg/lifecycle"
	"github.com/go-blade/pkg/metrics"
	"github.com/go-blade/pkg/redis"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/services/auth/internal/config"
	"github.com/go-blade/services/auth/internal/handler"
	"github.com/go-blade/services/auth/internal/server"
	"github.com/go-blade/services/auth/internal/svc"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/auth.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	// 使用 pkg/config 双模加载配置（支持本地 YAML 与 Nacos 远程覆盖）
	if err := pkgconfig.LoadConfig(context.Background(), *configFile, &c); err != nil {
		panic(fmt.Sprintf("load config failed: %v", err))
	}

	// 1. 初始化 Redis 组件与动态雪花算法节点
	if c.Redis.Host != "" {
		if _, err := redis.InitRedis(c.Redis); err != nil {
			panic(fmt.Sprintf("init redis failed: %v", err))
		}
	}
	if _, err := snowflake.InitAutoNode(context.Background(), "service-auth"); err != nil {
		logx.Errorf("init snowflake auto node failed: %v", err)
	}

	// 2. 初始化核心 ServiceContext
	svcCtx := svc.NewServiceContext(c)

	// 3. 构建健康检查探针与 HTTP Rest Server
	healthChecker := health.NewChecker()
	if svcCtx.DB != nil {
		healthChecker.Register("mysql", func(ctx context.Context) error {
			sqlDB, err := svcCtx.DB.DB()
			if err != nil {
				return err
			}
			return sqlDB.PingContext(ctx)
		})
	}

	httpServer := rest.MustNewServer(c.RestConf)
	handler.RegisterHandlers(httpServer, svcCtx)
	httpServer.AddRoutes([]rest.Route{
		{
			Method:  http.MethodGet,
			Path:    "/healthz/live",
			Handler: healthChecker.LiveHandler(),
		},
		{
			Method:  http.MethodGet,
			Path:    "/healthz/ready",
			Handler: healthChecker.ReadyHandler(),
		},
		{
			Method:  http.MethodGet,
			Path:    "/metrics",
			Handler: metrics.MetricsHandler(),
		},
	})

	// 4. 构建 gRPC Server 并注册健康检查服务
	grpcServer := pkgrpc.NewServer()
	authGrpcServer := server.NewAuthServer(svcCtx)
	authv1.RegisterAuthServiceServer(grpcServer, authGrpcServer)
	health.RegisterGRPCHealthServer(grpcServer, healthChecker)

	// 5. 初始化服务注册中心（若配置了 Nacos）
	var nacosRegistry *discovery.NacosRegistry
	var rpcInstance discovery.Instance
	hasRegistry := len(c.Nacos.ServerConfigs) > 0
	if hasRegistry {
		var err error
		nacosRegistry, err = discovery.NewNacosRegistry(c.Nacos)
		if err != nil {
			logx.Errorf("init nacos registry failed: %v", err)
		}
	}

	// 6. 并发启动 HTTP 与 gRPC 服务
	var wg sync.WaitGroup

	// 启动 HTTP Server
	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("Starting auth http server at %s:%d...\n", c.Host, c.Port)
		httpServer.Start()
	}()

	// 启动 gRPC Server (默认 0.0.0.0:9001)
	rpcListenOn := c.RpcServer.ListenOn
	if rpcListenOn == "" {
		rpcListenOn = "0.0.0.0:9001"
	}

	lis, err := net.Listen("tcp", rpcListenOn)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on gRPC address %s: %v", rpcListenOn, err))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("Starting auth gRPC server at %s...\n", rpcListenOn)

		// 注册 gRPC 实例至 Nacos 注册中心
		if nacosRegistry != nil {
			ip, port, parseErr := discovery.ParseAddressAndPort(rpcListenOn)
			if parseErr == nil {
				rpcInstance = discovery.Instance{
					ServiceName: "service-auth",
					IP:          ip,
					Port:        port,
					Group:       c.Nacos.Group,
				}
				if regErr := nacosRegistry.Register(context.Background(), rpcInstance); regErr != nil {
					logx.Errorf("register service-auth to nacos failed: %v", regErr)
				}
			}
		}

		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			logx.Errorf("gRPC server serve error: %v", serveErr)
		}
	}()

	// 7. 使用统一生命周期编排器优雅关机
	lc := lifecycle.NewManager()
	lc.Register("nacos-deregister", lifecycle.PhaseTrafficDrain, func(ctx context.Context) error {
		if nacosRegistry != nil && rpcInstance.ServiceName != "" {
			return nacosRegistry.Deregister(ctx, rpcInstance)
		}
		return nil
	})
	lc.Register("servers-stop", lifecycle.PhaseServerStop, func(ctx context.Context) error {
		grpcServer.GracefulStop()
		httpServer.Stop()
		return nil
	})

	_ = lc.WaitForSignal(context.Background())
	wg.Wait()
	fmt.Println("Auth servers stopped cleanly.")
}
