// Package discovery 提供微服务实例的注册、注销与服务发现功能，基于 Nacos 注册中心实现。
package discovery

import (
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/resolver"

	"github.com/go-blade/pkg/config"
)

// SchemeNacos 定义 Nacos Resolver 的 URI Schema。
const SchemeNacos = "nacos"

// nacosResolverBuilder 实现 gRPC 标准 resolver.Builder 接口。
type nacosResolverBuilder struct {
	client naming_client.INamingClient
	group  string
}

// NewNacosResolverBuilder 创建并返回一个 Nacos Resolver Builder。
func NewNacosResolverBuilder(client naming_client.INamingClient, group string) resolver.Builder {
	if group == "" {
		group = config.DefaultGroup
	}
	return &nacosResolverBuilder{
		client: client,
		group:  group,
	}
}

// Build 构建 Nacos 动态服务发现 Resolver。
func (b *nacosResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	serviceName := target.Endpoint()
	if serviceName == "" {
		serviceName = target.URL.Path
		serviceName = strings.TrimPrefix(serviceName, "/")
	}

	r := &nacosResolver{
		target:      target,
		cc:          cc,
		client:      b.client,
		serviceName: serviceName,
		group:       b.group,
	}

	// 立即解析一次实例列表
	r.resolve()

	// 订阅 Nacos 服务实例变化事件
	err := b.client.Subscribe(&vo.SubscribeParam{
		ServiceName: serviceName,
		GroupName:   b.group,
		SubscribeCallback: func(services []model.Instance, err error) {
			if err != nil {
				logx.Errorf("[NacosResolver] callback error for %s: %v", serviceName, err)
				return
			}
			r.updateInstances(services)
		},
	})
	if err != nil {
		logx.Errorf("[NacosResolver] subscribe failed for %s: %v", serviceName, err)
	}

	return r, nil
}

// Scheme 返回 "nacos"
func (b *nacosResolverBuilder) Scheme() string {
	return SchemeNacos
}

// nacosResolver 实现 gRPC resolver.Resolver 接口。
type nacosResolver struct {
	target      resolver.Target
	cc          resolver.ClientConn
	client      naming_client.INamingClient
	serviceName string
	group       string
	mu          sync.Mutex
}

// ResolveNow 主动触发一次地址解析。
func (r *nacosResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.resolve()
}

// Close 关闭 Resolver 并取消事件订阅。
func (r *nacosResolver) Close() {
	_ = r.client.Unsubscribe(&vo.SubscribeParam{
		ServiceName: r.serviceName,
		GroupName:   r.group,
	})
}

// resolve 同步拉取健康实例列表并推送到 gRPC ClientConn
func (r *nacosResolver) resolve() {
	instances, err := r.client.SelectInstances(vo.SelectInstancesParam{
		ServiceName: r.serviceName,
		GroupName:   r.group,
		HealthyOnly: true,
	})
	if err != nil {
		logx.Errorf("[NacosResolver] select instances for %s failed: %v", r.serviceName, err)
		return
	}
	r.updateInstances(instances)
}

// updateInstances 更新地址列表并通知 ClientConn
func (r *nacosResolver) updateInstances(instances []model.Instance) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var addrs []resolver.Address
	for _, inst := range instances {
		if !inst.Healthy || !inst.Enable || inst.Weight <= 0 {
			continue
		}
		addr := net.JoinHostPort(inst.Ip, strconv.FormatUint(inst.Port, 10))
		addrs = append(addrs, resolver.Address{
			Addr:       addr,
			ServerName: r.serviceName,
		})
	}

	if len(addrs) > 0 {
		err := r.cc.UpdateState(resolver.State{Addresses: addrs})
		if err != nil {
			logx.Errorf("[NacosResolver] cc.UpdateState failed for %s: %v", r.serviceName, err)
		}
	} else {
		logx.Errorf("[NacosResolver] no healthy instances available for %s", r.serviceName)
	}
}

// RegisterNacosResolver 全局注册 Nacos Resolver Builder，使得 grpc.Dial("nacos:///service-auth") 可以直接被解析。
func RegisterNacosResolver(client naming_client.INamingClient, group string) {
	resolver.Register(NewNacosResolverBuilder(client, group))
}
