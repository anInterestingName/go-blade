// Package main 是统一微服务网关服务的启动入口。
package main

import (
	"context"
	"flag"
	"fmt"

	pkgconfig "github.com/go-blade/pkg/config"
	"github.com/go-blade/pkg/discovery"
	"github.com/go-blade/pkg/lifecycle"
	"github.com/go-blade/services/gateway/internal/config"
	"github.com/go-blade/services/gateway/internal/handler"
	"github.com/go-blade/services/gateway/internal/svc"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/rest"
)

var configFile = flag.String("f", "etc/gateway.yaml", "the config file")

func main() {
	flag.Parse()

	var c config.Config
	if err := pkgconfig.LoadConfig(context.Background(), *configFile, &c); err != nil {
		panic(fmt.Sprintf("load gateway config failed: %v", err))
	}

	// 1. 初始化服务上下文
	svcCtx := svc.NewServiceContext(c)
	defer svcCtx.Close()

	// 2. 初始化网关 HTTP Server
	server := rest.MustNewServer(c.RestConf)
	defer server.Stop()

	// 3. 注册统一路由
	handler.RegisterHandlers(server, svcCtx)

	// 4. 服务注册与实例注销（若配置了 Nacos）
	var nacosRegistry *discovery.NacosRegistry
	var gwInstance discovery.Instance
	if len(c.Nacos.ServerConfigs) > 0 {
		var err error
		nacosRegistry, err = discovery.NewNacosRegistry(c.Nacos)
		if err != nil {
			logx.Errorf("init nacos registry failed: %v", err)
		} else {
			ip, port, _ := discovery.ParseAddressAndPort(fmt.Sprintf("%s:%d", c.Host, c.Port))
			gwInstance = discovery.Instance{
				ServiceName: "service-gateway",
				IP:          ip,
				Port:        port,
				Group:       c.Nacos.Group,
			}
			if regErr := nacosRegistry.Register(context.Background(), gwInstance); regErr != nil {
				logx.Errorf("register gateway to nacos failed: %v", regErr)
			}
		}
	}

	// 5. 注册多阶段优雅停机编排器并监听退出信号
	lc := lifecycle.NewManager()
	lc.Register("nacos-deregister", lifecycle.PhaseTrafficDrain, func(ctx context.Context) error {
		if nacosRegistry != nil && gwInstance.ServiceName != "" {
			return nacosRegistry.Deregister(ctx, gwInstance)
		}
		return nil
	})
	lc.Register("http-server-stop", lifecycle.PhaseServerStop, func(ctx context.Context) error {
		server.Stop()
		return nil
	})
	lc.Register("gateway-svc-close", lifecycle.PhaseObservability, func(ctx context.Context) error {
		svcCtx.Close()
		return nil
	})

	go func() {
		_ = lc.WaitForSignal(context.Background())
	}()

	fmt.Printf("Starting gateway server at %s:%d...\n", c.Host, c.Port)
	server.Start()
}
