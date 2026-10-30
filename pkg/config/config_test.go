package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// mockNacosClient 是用于测试的内存 NacosClient 实现。
type mockNacosClient struct {
	configs   map[string]string
	listeners map[string]func(namespace, group, dataId, data string)
}

func newMockNacosClient() *mockNacosClient {
	return &mockNacosClient{
		configs:   make(map[string]string),
		listeners: make(map[string]func(namespace, group, dataId, data string)),
	}
}

func (m *mockNacosClient) GetConfig(param vo.ConfigParam) (string, error) {
	key := fmt.Sprintf("%s:%s", param.Group, param.DataId)
	val, ok := m.configs[key]
	if !ok {
		return "", fmt.Errorf("config not found for key: %s", key)
	}
	return val, nil
}

func (m *mockNacosClient) ListenConfig(param vo.ConfigParam) error {
	key := fmt.Sprintf("%s:%s", param.Group, param.DataId)
	m.listeners[key] = param.OnChange
	return nil
}

func (m *mockNacosClient) CancelListenConfig(param vo.ConfigParam) error {
	key := fmt.Sprintf("%s:%s", param.Group, param.DataId)
	delete(m.listeners, key)
	return nil
}

type TestAppConfig struct {
	Name  string      `json:"name"`
	Port  int         `json:"port"`
	Nacos NacosConfig `json:"nacos,optional"`
}

func (c TestAppConfig) GetNacos() NacosConfig {
	return c.Nacos
}

func TestLoadConfig_LocalOnly(t *testing.T) {
	tmpDir := t.TempDir()
	yamlFile := filepath.Join(tmpDir, "app.yaml")
	content := "name: local-service\nport: 8080\n"
	if err := os.WriteFile(yamlFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write local yaml: %v", err)
	}

	var cfg TestAppConfig
	if err := LoadConfig(context.Background(), yamlFile, &cfg); err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.Name != "local-service" || cfg.Port != 8080 {
		t.Fatalf("unexpected config values: %+v", cfg)
	}
}

func TestLoadConfig_NacosOverride(t *testing.T) {
	tmpDir := t.TempDir()
	yamlFile := filepath.Join(tmpDir, "app.yaml")
	// 本地配置仅包含 nacos 连接信息与默认 name
	localYaml := `
name: local-service
port: 8080
nacos:
  dataId: test-service.yaml
  group: DEFAULT_GROUP
  serverConfigs:
    - ipAddr: 127.0.0.1
      port: 8848
`
	if err := os.WriteFile(yamlFile, []byte(localYaml), 0644); err != nil {
		t.Fatalf("failed to write local yaml: %v", err)
	}

	mockClient := newMockNacosClient()
	mockClient.configs["DEFAULT_GROUP:test-service.yaml"] = `
name: nacos-remote-service
port: 9090
`

	var cfg TestAppConfig
	err := LoadConfig(context.Background(), yamlFile, &cfg, WithClient(mockClient))
	if err != nil {
		t.Fatalf("LoadConfig with nacos override failed: %v", err)
	}

	if cfg.Name != "nacos-remote-service" || cfg.Port != 9090 {
		t.Fatalf("expected remote values, got: %+v", cfg)
	}
}

func TestListenConfig(t *testing.T) {
	mockClient := newMockNacosClient()
	loader := NewLoader(WithClient(mockClient))

	nc := NacosConfig{
		DataID: "dynamic.yaml",
		Group:  "DEFAULT_GROUP",
	}

	var triggeredData string
	err := loader.ListenConfig(nc, func(namespace, group, dataId, data string) {
		triggeredData = data
	})
	if err != nil {
		t.Fatalf("ListenConfig failed: %v", err)
	}

	listenerKey := "DEFAULT_GROUP:dynamic.yaml"
	fn, ok := mockClient.listeners[listenerKey]
	if !ok {
		t.Fatalf("listener was not registered")
	}

	// 模拟 Nacos 推送新配置
	newContent := "logLevel: debug"
	fn("public", "DEFAULT_GROUP", "dynamic.yaml", newContent)

	if triggeredData != newContent {
		t.Fatalf("listener callback did not receive expected data, got: %s", triggeredData)
	}
}

func TestLoadConfig_DetectNacosFromEnv(t *testing.T) {
	// 设置测试环境变量
	t.Setenv(EnvNacosAddr, "127.0.0.1:8848")
	t.Setenv(EnvNacosNamespace, "test-ns")
	t.Setenv(EnvNacosGroup, "TEST_GROUP")
	t.Setenv(EnvNacosDataID, "custom-app.yaml")

	mockClient := newMockNacosClient()
	mockClient.configs["TEST_GROUP:custom-app.yaml"] = `
name: env-loaded-service
port: 7070
`

	var cfg TestAppConfig
	// 即使没有本地文件，也可以直接从环境变量探测到的 Nacos 拉取
	err := LoadConfig(context.Background(), "", &cfg, WithClient(mockClient))
	if err != nil {
		t.Fatalf("LoadConfig from env nacos failed: %v", err)
	}

	if cfg.Name != "env-loaded-service" || cfg.Port != 7070 {
		t.Fatalf("unexpected values from env nacos: %+v", cfg)
	}
}

func TestLoadConfig_DetectNacosFromEnv_InferDataID(t *testing.T) {
	// 未指定 NACOS_DATA_ID 时，根据目标结构体或文件名自动推导
	t.Setenv(EnvNacosHost, "127.0.0.1")
	t.Setenv(EnvNacosPort, "8848")

	mockClient := newMockNacosClient()
	mockClient.configs["DEFAULT_GROUP:auth-service.yaml"] = `
name: inferred-auth-service
port: 6060
`

	var cfg TestAppConfig
	// 传入虚拟文件路径 etc/auth-service.yaml，即使文件不存在，也能推导出 auth-service.yaml 并成功拉取 Nacos 配置
	err := LoadConfig(context.Background(), "etc/auth-service.yaml", &cfg, WithClient(mockClient))
	if err != nil {
		t.Fatalf("LoadConfig with inferred dataId failed: %v", err)
	}

	if cfg.Name != "inferred-auth-service" || cfg.Port != 6060 {
		t.Fatalf("unexpected values from inferred dataId: %+v", cfg)
	}
}

func TestLoadConfig_DetectNacos_FallbackToLocal(t *testing.T) {
	// 当设置了 Nacos 环境变量但 Nacos 中不存在该配置时，平滑回退到本地已有的 yaml 文件
	t.Setenv(EnvNacosHost, "127.0.0.1")
	t.Setenv(EnvNacosDataID, "not-exist.yaml")

	mockClient := newMockNacosClient()

	tmpDir := t.TempDir()
	yamlFile := filepath.Join(tmpDir, "local-fallback.yaml")
	content := "name: local-fallback-service\nport: 5050\n"
	if err := os.WriteFile(yamlFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write local yaml: %v", err)
	}

	var cfg TestAppConfig
	err := LoadConfig(context.Background(), yamlFile, &cfg, WithClient(mockClient))
	if err != nil {
		t.Fatalf("LoadConfig fallback failed: %v", err)
	}

	if cfg.Name != "local-fallback-service" || cfg.Port != 5050 {
		t.Fatalf("expected fallback to local config, got: %+v", cfg)
	}
}
