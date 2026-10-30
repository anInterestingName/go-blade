// Package discovery 提供微服务实例的注册、注销与服务发现功能，基于 Nacos 注册中心实现。
package discovery

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/go-blade/pkg/config"
)

// Registry 定义服务注册与注销的标准行为接口。
type Registry interface {
	// Register 注册服务实例到注册中心。
	Register(ctx context.Context, instance Instance) error
	// Deregister 从注册中心注销服务实例。
	Deregister(ctx context.Context, instance Instance) error
}

// Instance 描述一个微服务实例的元数据。
type Instance struct {
	// ServiceName 服务名称 (如 service-auth)
	ServiceName string `json:"serviceName"`
	// IP 实例网络 IP 地址
	IP string `json:"ip"`
	// Port 实例提供服务的端口号
	Port uint64 `json:"port"`
	// Group 分组名称 (默认为 DEFAULT_GROUP)
	Group string `json:"group,optional"`
	// Cluster 集群名称 (默认为 DEFAULT)
	Cluster string `json:"cluster,optional"`
	// Weight 权重 (默认 1.0)
	Weight float64 `json:"weight,optional"`
	// Metadata 实例额外扩展元数据
	Metadata map[string]string `json:"metadata,optional"`
}

// NacosRegistry 基于 Nacos naming_client 的服务注册中心实现。
type NacosRegistry struct {
	client naming_client.INamingClient
}

// NewNacosRegistry 根据 Nacos 配置初始化并返回 NacosRegistry 实例。
func NewNacosRegistry(nc config.NacosConfig) (*NacosRegistry, error) {
	var serverConfigs []constant.ServerConfig
	for _, sc := range nc.ServerConfigs {
		scheme := sc.Scheme
		if scheme == "" {
			scheme = "http"
		}
		contextPath := sc.ContextPath
		if contextPath == "" {
			contextPath = "/nacos"
		}
		serverConfigs = append(serverConfigs, *constant.NewServerConfig(
			sc.IPAddr,
			sc.Port,
			constant.WithScheme(scheme),
			constant.WithContextPath(contextPath),
		))
	}

	timeoutMs := nc.ClientConfig.TimeoutMs
	if timeoutMs == 0 {
		timeoutMs = 5000
	}
	logLevel := nc.ClientConfig.LogLevel
	if logLevel == "" {
		logLevel = "warn"
	}

	var clientCustomOpts []constant.ClientOption
	if nc.ClientConfig.NamespaceID != "" {
		clientCustomOpts = append(clientCustomOpts, constant.WithNamespaceId(nc.ClientConfig.NamespaceID))
	}
	clientCustomOpts = append(clientCustomOpts, constant.WithTimeoutMs(timeoutMs))
	clientCustomOpts = append(clientCustomOpts, constant.WithLogLevel(logLevel))
	if nc.ClientConfig.Username != "" {
		clientCustomOpts = append(clientCustomOpts, constant.WithUsername(nc.ClientConfig.Username))
		clientCustomOpts = append(clientCustomOpts, constant.WithPassword(nc.ClientConfig.Password))
	}
	if nc.ClientConfig.LogDir != "" {
		clientCustomOpts = append(clientCustomOpts, constant.WithLogDir(nc.ClientConfig.LogDir))
	}
	if nc.ClientConfig.CacheDir != "" {
		clientCustomOpts = append(clientCustomOpts, constant.WithCacheDir(nc.ClientConfig.CacheDir))
	}

	clientConfig := constant.NewClientConfig(clientCustomOpts...)

	namingClient, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("create nacos naming client failed: %w", err)
	}

	return &NacosRegistry{client: namingClient}, nil
}

// NewNacosRegistryWithClient 允许直接注入现有的 naming_client.INamingClient，便于单元测试与 Mock。
func NewNacosRegistryWithClient(cli naming_client.INamingClient) *NacosRegistry {
	return &NacosRegistry{client: cli}
}

// Client 返回底层 Nacos naming_client.INamingClient 实例，供 gRPC Resolver 等组件复用。
func (r *NacosRegistry) Client() naming_client.INamingClient {
	if r == nil {
		return nil
	}
	return r.client
}

// Register 将微服务实例注册到 Nacos 注册中心。
func (r *NacosRegistry) Register(ctx context.Context, inst Instance) error {
	group := inst.Group
	if group == "" {
		group = config.DefaultGroup
	}
	cluster := inst.Cluster
	if cluster == "" {
		cluster = "DEFAULT"
	}
	weight := inst.Weight
	if weight <= 0 {
		weight = 1.0
	}

	success, err := r.client.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          inst.IP,
		Port:        inst.Port,
		ServiceName: inst.ServiceName,
		GroupName:   group,
		ClusterName: cluster,
		Weight:      weight,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,
		Metadata:    inst.Metadata,
	})
	if err != nil {
		return fmt.Errorf("register instance to nacos failed: %w", err)
	}
	if !success {
		return fmt.Errorf("register instance to nacos returned false")
	}

	logx.WithContext(ctx).Infof("[NacosRegistry] successfully registered instance: %s (%s:%d)", inst.ServiceName, inst.IP, inst.Port)
	return nil
}

// Deregister 从 Nacos 注册中心注销微服务实例。
func (r *NacosRegistry) Deregister(ctx context.Context, inst Instance) error {
	group := inst.Group
	if group == "" {
		group = config.DefaultGroup
	}
	cluster := inst.Cluster
	if cluster == "" {
		cluster = "DEFAULT"
	}

	success, err := r.client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          inst.IP,
		Port:        inst.Port,
		ServiceName: inst.ServiceName,
		GroupName:   group,
		Cluster:     cluster,
		Ephemeral:   true,
	})
	if err != nil {
		return fmt.Errorf("deregister instance from nacos failed: %w", err)
	}
	if !success {
		return fmt.Errorf("deregister instance from nacos returned false")
	}

	logx.WithContext(ctx).Infof("[NacosRegistry] successfully deregistered instance: %s (%s:%d)", inst.ServiceName, inst.IP, inst.Port)
	return nil
}

// ParseAddressAndPort 辅助工具：将形如 "0.0.0.0:9001" 或 ":9001" 或 "192.168.1.10:9001" 解析为 (ip, port)。
// 当 IP 为空或通配地址 0.0.0.0 时，自动尝试查找本地非环回网络 IP。
func ParseAddressAndPort(listenOn string) (string, uint64, error) {
	host, portStr, err := net.SplitHostPort(listenOn)
	if err != nil {
		return "", 0, fmt.Errorf("invalid listen address format %q: %w", listenOn, err)
	}

	port, err := strconv.ParseUint(portStr, 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port number %q: %w", portStr, err)
	}

	if host == "" || host == "0.0.0.0" || strings.HasPrefix(host, "::") {
		localIP := getOutboundIP()
		if localIP != "" {
			host = localIP
		} else {
			host = "127.0.0.1"
		}
	}

	return host, port, nil
}

// getOutboundIP 获取本机优先使用的对外 IP
func getOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}
