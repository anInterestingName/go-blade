// Package main 是协同工作台微服务的启动入口。
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"sync"

	deskv1 "github.com/go-blade/api/proto/desk/v1"
	pkgconfig "github.com/go-blade/pkg/config"
	"github.com/go-blade/pkg/discovery"
	"github.com/go-blade/pkg/health"
	"github.com/go-blade/pkg/lifecycle"
	"github.com/go-blade/pkg/metrics"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/services/desk/internal/config"
	"github.com/go-blade/services/desk/internal/server"
	"github.com/go-blade/services/desk/internal/svc"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/desk.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	if err := pkgconfig.LoadConfig(context.Background(), *configFile, &c); err != nil {
		panic(fmt.Sprintf("load desk config failed: %v", err))
	}

	// 1. 初始化 ServiceContext 与动态雪花算法节点
	svcCtx := svc.NewServiceContext(c)
	if _, err := snowflake.InitAutoNode(context.Background(), "service-desk"); err != nil {
		logx.Errorf("init snowflake auto node failed: %v", err)
	}

	// 2. 初始化健康检查探针与 HTTP Rest Server
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
	defer httpServer.Stop()
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

	// 3. 初始化 gRPC Server 并注册健康检查服务
	grpcServer := pkgrpc.NewServer()
	deskGrpcServer := server.NewDeskServer(svcCtx)
	deskv1.RegisterDeskServiceServer(grpcServer, deskGrpcServer)
	health.RegisterGRPCHealthServer(grpcServer, healthChecker)

	// 4. 服务注册至 Nacos（若配置）
	var nacosRegistry *discovery.NacosRegistry
	var rpcInstance discovery.Instance
	if len(c.Nacos.ServerConfigs) > 0 {
		var err error
		nacosRegistry, err = discovery.NewNacosRegistry(c.Nacos)
		if err != nil {
			logx.Errorf("init nacos registry failed: %v", err)
		}
	}

	// 5. 并发启动 HTTP 与 gRPC 服务
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("Starting desk http server at %s:%d...\n", c.Host, c.Port)
		httpServer.Start()
	}()

	rpcListenOn := c.RpcServer.ListenOn
	if rpcListenOn == "" {
		rpcListenOn = "0.0.0.0:9003"
	}

	lis, err := net.Listen("tcp", rpcListenOn)
	if err != nil {
		panic(fmt.Sprintf("failed to listen on gRPC address %s: %v", rpcListenOn, err))
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("Starting desk gRPC server at %s...\n", rpcListenOn)

		if nacosRegistry != nil {
			ip, port, parseErr := discovery.ParseAddressAndPort(rpcListenOn)
			if parseErr == nil {
				rpcInstance = discovery.Instance{
					ServiceName: "service-desk",
					IP:          ip,
					Port:        port,
					Group:       c.Nacos.Group,
				}
				_ = nacosRegistry.Register(context.Background(), rpcInstance)
			}
		}

		if serveErr := grpcServer.Serve(lis); serveErr != nil {
			logx.Errorf("gRPC server serve error: %v", serveErr)
		}
	}()

	// 6. 使用统一生命周期编排器优雅退出
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
	fmt.Println("Desk servers stopped cleanly.")
}
