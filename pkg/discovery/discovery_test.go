package discovery

import (
	"context"
	"net/url"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// mockNamingClient 模拟 Nacos Naming 客户端用于单元测试
type mockNamingClient struct {
	naming_client.INamingClient
	registered   []vo.RegisterInstanceParam
	deregistered []vo.DeregisterInstanceParam
	instances    []model.Instance
	subscribed   *vo.SubscribeParam
}

func (m *mockNamingClient) RegisterInstance(param vo.RegisterInstanceParam) (bool, error) {
	m.registered = append(m.registered, param)
	return true, nil
}

func (m *mockNamingClient) DeregisterInstance(param vo.DeregisterInstanceParam) (bool, error) {
	m.deregistered = append(m.deregistered, param)
	return true, nil
}

func (m *mockNamingClient) SelectInstances(param vo.SelectInstancesParam) ([]model.Instance, error) {
	return m.instances, nil
}

func (m *mockNamingClient) Subscribe(param *vo.SubscribeParam) error {
	m.subscribed = param
	return nil
}

func (m *mockNamingClient) Unsubscribe(param *vo.SubscribeParam) error {
	m.subscribed = nil
	return nil
}

func TestNacosRegistry_RegisterAndDeregister(t *testing.T) {
	mockCli := &mockNamingClient{}
	reg := NewNacosRegistryWithClient(mockCli)

	inst := Instance{
		ServiceName: "service-auth",
		IP:          "127.0.0.1",
		Port:        9001,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	// 1. 注册实例
	err := reg.Register(context.Background(), inst)
	require.NoError(t, err)
	require.Len(t, mockCli.registered, 1)
	assert.Equal(t, "service-auth", mockCli.registered[0].ServiceName)
	assert.Equal(t, "127.0.0.1", mockCli.registered[0].Ip)
	assert.Equal(t, uint64(9001), mockCli.registered[0].Port)
	assert.Equal(t, "DEFAULT_GROUP", mockCli.registered[0].GroupName)

	// 2. 注销实例
	err = reg.Deregister(context.Background(), inst)
	require.NoError(t, err)
	require.Len(t, mockCli.deregistered, 1)
	assert.Equal(t, "service-auth", mockCli.deregistered[0].ServiceName)
	assert.Equal(t, "127.0.0.1", mockCli.deregistered[0].Ip)
	assert.Equal(t, uint64(9001), mockCli.deregistered[0].Port)
}

func TestParseAddressAndPort(t *testing.T) {
	// 指定具体 IP 和端口
	ip, port, err := ParseAddressAndPort("192.168.1.100:9090")
	require.NoError(t, err)
	assert.Equal(t, "192.168.1.100", ip)
	assert.Equal(t, uint64(9090), port)

	// 0.0.0.0 自动转换为本地非空 IP
	ip2, port2, err := ParseAddressAndPort("0.0.0.0:8080")
	require.NoError(t, err)
	assert.NotEmpty(t, ip2)
	assert.Equal(t, uint64(8080), port2)

	// 错误格式
	_, _, err = ParseAddressAndPort("invalid_address")
	assert.Error(t, err)
}

// mockClientConn 模拟 gRPC resolver.ClientConn
type mockClientConn struct {
	state resolver.State
}

func (m *mockClientConn) UpdateState(state resolver.State) error {
	m.state = state
	return nil
}

func (m *mockClientConn) ReportError(err error) {}

func (m *mockClientConn) NewAddress(addresses []resolver.Address) {}

func (m *mockClientConn) ParseServiceConfig(serviceConfigJSON string) *serviceconfig.ParseResult {
	return &serviceconfig.ParseResult{}
}

func TestNacosResolver_ResolveAndSubscribe(t *testing.T) {
	mockCli := &mockNamingClient{
		instances: []model.Instance{
			{
				Ip:      "10.0.0.1",
				Port:    9001,
				Healthy: true,
				Enable:  true,
				Weight:  1.0,
			},
			{
				Ip:      "10.0.0.2",
				Port:    9002,
				Healthy: false, // 不健康实例应被自动过滤
				Enable:  true,
				Weight:  1.0,
			},
		},
	}

	builder := NewNacosResolverBuilder(mockCli, "DEFAULT_GROUP")
	assert.Equal(t, SchemeNacos, builder.Scheme())

	mockCC := &mockClientConn{}
	parsedURL, err := url.Parse("nacos:///service-auth")
	require.NoError(t, err)
	target := resolver.Target{
		URL: *parsedURL,
	}

	r, err := builder.Build(target, mockCC, resolver.BuildOptions{})
	require.NoError(t, err)
	defer r.Close()

	// 校验过滤后的健康地址只有 10.0.0.1:9001
	require.Len(t, mockCC.state.Addresses, 1)
	assert.Equal(t, "10.0.0.1:9001", mockCC.state.Addresses[0].Addr)

	// 模拟触发 Nacos 实例变动回调
	assert.NotNil(t, mockCli.subscribed)
	mockCli.subscribed.SubscribeCallback([]model.Instance{
		{
			Ip:      "10.0.0.3",
			Port:    9003,
			Healthy: true,
			Enable:  true,
			Weight:  1.0,
		},
	}, nil)

	// 校验地址变更为 10.0.0.3:9003
	require.Len(t, mockCC.state.Addresses, 1)
	assert.Equal(t, "10.0.0.3:9003", mockCC.state.Addresses[0].Addr)
}
