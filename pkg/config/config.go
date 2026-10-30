// Package config 提供微服务统一配置加载与配置中心能力。
// 支持本地 YAML 配置文件与 Nacos 远程动态配置中心的无缝适配。
package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/zeromicro/go-zero/core/conf"
)

// DefaultGroup 是 Nacos 默认配置分组。
const DefaultGroup = "DEFAULT_GROUP"

// 环境变量名称常量，用于零配置/环境变量启动时自动探测 Nacos。
const (
	// EnvNacosAddr Nacos 主机或完整地址 (例如 "127.0.0.1:8848" 或 "127.0.0.1")
	EnvNacosAddr = "NACOS_ADDR"
	// EnvNacosHost Nacos 主机 IP 或域名 (例如 "127.0.0.1")
	EnvNacosHost = "NACOS_HOST"
	// EnvNacosPort Nacos 端口号 (例如 "8848")
	EnvNacosPort = "NACOS_PORT"
	// EnvNacosNamespace Nacos 命名空间 ID (如 "public", "dev", "test", "prod")
	EnvNacosNamespace = "NACOS_NAMESPACE"
	// EnvNacosGroup Nacos 配置分组 (默认为 DEFAULT_GROUP)
	EnvNacosGroup = "NACOS_GROUP"
	// EnvNacosDataID Nacos 配置集 ID (如 "auth-api.yaml")
	EnvNacosDataID = "NACOS_DATA_ID"
	// EnvNacosUsername Nacos 认证用户名
	EnvNacosUsername = "NACOS_USERNAME"
	// EnvNacosPassword Nacos 认证密码
	EnvNacosPassword = "NACOS_PASSWORD"
	// EnvAppEnv 当前微服务运行环境模式 (如 "dev", "test", "prod")
	EnvAppEnv = "APP_ENV"
)

// NacosClient 是配置中心客户端接口，便于单元测试与 Mock。
type NacosClient interface {
	GetConfig(param vo.ConfigParam) (string, error)
	ListenConfig(param vo.ConfigParam) error
	CancelListenConfig(param vo.ConfigParam) error
}

// NacosConfig 包含连接 Nacos 配置中心所需的基础参数。
type NacosConfig struct {
	// ServerConfigs Nacos 服务端节点配置
	ServerConfigs []ServerConfig `json:"serverConfigs,optional"`
	// ClientConfig Nacos 客户端通用配置
	ClientConfig ClientConfig `json:"clientConfig,optional"`
	// DataID 配置集 ID (如 auth-api.yaml)
	DataID string `json:"dataId,optional"`
	// Group 配置分组 (默认为 DEFAULT_GROUP)
	Group string `json:"group,optional"`
}

// ServerConfig 定义 Nacos 单个节点连接地址。
type ServerConfig struct {
	// IPAddr 节点 IP 或域名
	IPAddr string `json:"ipAddr"`
	// Port 节点端口号
	Port uint64 `json:"port"`
	// Scheme 协议 (http 或 https，默认为 http)
	Scheme string `json:"scheme,optional"`
	// ContextPath 请求上下文路径 (默认为 /nacos)
	ContextPath string `json:"contextPath,optional"`
}

// ClientConfig 定义 Nacos 客户端的命名空间、凭证与本地缓存设置。
type ClientConfig struct {
	// NamespaceID 命名空间 ID (如 dev, test, prod 或具体 UUID)
	NamespaceID string `json:"namespaceId,optional"`
	// TimeoutMs 请求超时时间 (毫秒，默认为 5000)
	TimeoutMs uint64 `json:"timeoutMs,optional"`
	// Username 鉴权用户名
	Username string `json:"username,optional"`
	// Password 鉴权密码
	Password string `json:"password,optional"`
	// LogDir 日志输出目录
	LogDir string `json:"logDir,optional"`
	// CacheDir 本地快照缓存目录
	CacheDir string `json:"cacheDir,optional"`
	// LogLevel 日志级别 (debug, info, warn, error，默认为 warn)
	LogLevel string `json:"logLevel,optional"`
}

// Loader 是配置加载器，支持本地与远程配置拉取与监听。
type Loader struct {
	client NacosClient
	mu     sync.RWMutex
}

// Option 定义 Loader 初始化函数选项。
type Option func(*Loader)

// WithClient 注入自定义 NacosClient (主要用于单元测试与 Mock)。
func WithClient(client NacosClient) Option {
	return func(l *Loader) {
		l.client = client
	}
}

// NewLoader 创建一个新的配置加载器。
func NewLoader(opts ...Option) *Loader {
	l := &Loader{}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

// CreateNacosClient 根据 NacosConfig 构建标准的 nacos-sdk-go/v2 配置客户端。
func CreateNacosClient(nc NacosConfig) (config_client.IConfigClient, error) {
	var scs []constant.ServerConfig
	for _, sc := range nc.ServerConfigs {
		scheme := sc.Scheme
		if scheme == "" {
			scheme = "http"
		}
		contextPath := sc.ContextPath
		if contextPath == "" {
			contextPath = "/nacos"
		}
		scs = append(scs, constant.ServerConfig{
			IpAddr:      sc.IPAddr,
			Port:        sc.Port,
			Scheme:      scheme,
			ContextPath: contextPath,
		})
	}

	timeout := nc.ClientConfig.TimeoutMs
	if timeout == 0 {
		timeout = 5000
	}
	logLevel := nc.ClientConfig.LogLevel
	if logLevel == "" {
		logLevel = "warn"
	}
	logDir := nc.ClientConfig.LogDir
	if logDir == "" {
		logDir = filepath.Join(os.TempDir(), "nacos", "log")
	}
	cacheDir := nc.ClientConfig.CacheDir
	if cacheDir == "" {
		cacheDir = filepath.Join(os.TempDir(), "nacos", "cache")
	}

	cc := constant.ClientConfig{
		NamespaceId:         nc.ClientConfig.NamespaceID,
		TimeoutMs:           timeout,
		NotLoadCacheAtStart: true,
		LogDir:              logDir,
		CacheDir:            cacheDir,
		LogLevel:            logLevel,
		Username:            nc.ClientConfig.Username,
		Password:            nc.ClientConfig.Password,
	}

	return clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  &cc,
			ServerConfigs: scs,
		},
	)
}

// DetectNacosConfigFromEnv 尝试从系统环境变量中探测 Nacos 配置。
// 支持 NACOS_ADDR / NACOS_HOST / NACOS_PORT / NACOS_NAMESPACE / NACOS_GROUP / NACOS_DATA_ID 等。
// 若未提供 DataID，可通过 fallbackServiceName 或 target 结构体中提取的服务名推导为 "${serviceName}.yaml"。
func DetectNacosConfigFromEnv(fallbackServiceName string) (NacosConfig, bool) {
	addr := strings.TrimSpace(os.Getenv(EnvNacosAddr))
	host := strings.TrimSpace(os.Getenv(EnvNacosHost))
	portStr := strings.TrimSpace(os.Getenv(EnvNacosPort))

	if addr == "" && host == "" {
		return NacosConfig{}, false
	}

	var ip string
	var port uint64 = 8848

	if addr != "" {
		if strings.Contains(addr, ":") {
			parts := strings.Split(addr, ":")
			ip = parts[0]
			if p, err := strconv.ParseUint(parts[1], 10, 64); err == nil && p > 0 {
				port = p
			}
		} else {
			ip = addr
		}
	} else {
		ip = host
	}

	if portStr != "" {
		if p, err := strconv.ParseUint(portStr, 10, 64); err == nil && p > 0 {
			port = p
		}
	}

	ns := strings.TrimSpace(os.Getenv(EnvNacosNamespace))
	if ns == "" {
		ns = strings.TrimSpace(os.Getenv(EnvAppEnv))
	}

	group := strings.TrimSpace(os.Getenv(EnvNacosGroup))
	if group == "" {
		group = DefaultGroup
	}

	dataID := strings.TrimSpace(os.Getenv(EnvNacosDataID))
	if dataID == "" && fallbackServiceName != "" {
		dataID = fallbackServiceName
		if !strings.HasSuffix(dataID, ".yaml") && !strings.HasSuffix(dataID, ".yml") {
			dataID += ".yaml"
		}
	}

	nc := NacosConfig{
		ServerConfigs: []ServerConfig{
			{
				IPAddr: ip,
				Port:   port,
			},
		},
		ClientConfig: ClientConfig{
			NamespaceID: ns,
			Username:    strings.TrimSpace(os.Getenv(EnvNacosUsername)),
			Password:    strings.TrimSpace(os.Getenv(EnvNacosPassword)),
		},
		DataID: dataID,
		Group:  group,
	}

	return nc, true
}

// inferServiceNameFromTarget 尝试反射获取 target 中 Name 字段的值，便于自动推导 DataID。
func inferServiceNameFromTarget(target any) string {
	if target == nil {
		return ""
	}
	val := reflect.ValueOf(target)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return ""
	}
	nameField := val.FieldByName("Name")
	if nameField.IsValid() && nameField.Kind() == reflect.String {
		return nameField.String()
	}
	return ""
}

// LoadConfig 通用配置加载入口：
// 1. 优先尝试从环境变量探测 Nacos 连接 (NACOS_HOST / NACOS_ADDR)，若探测成功直接向 Nacos 尝试拉取配置；
// 2. 若未配置环境变量探测或从 Nacos 拉取失败，且 file 路径存在，则作为本地 YAML 配置加载；
// 3. 若本地配置中声明了有效的 NacosConfig，则自动以 Nacos 远程配置无缝覆盖本地配置；
// 4. 若 Nacos 探测成功且加载成功，将配置回填并直接返回。
func LoadConfig(ctx context.Context, file string, target any, opts ...Option) error {
	loader := NewLoader(opts...)

	// 1. 优先检查环境变量中是否有全局 Nacos 启动指引
	var serviceNameHint string
	if file != "" {
		baseName := filepath.Base(file)
		serviceNameHint = strings.TrimSuffix(baseName, filepath.Ext(baseName))
	}

	envNC, hasEnvNacos := DetectNacosConfigFromEnv(serviceNameHint)
	if hasEnvNacos {
		// 如果环境变量里没指定 DataID，且未能从文件名推导出，尝试根据 target 推导
		if envNC.DataID == "" {
			name := inferServiceNameFromTarget(target)
			if name != "" {
				envNC.DataID = name + ".yaml"
			}
		}

		if envNC.DataID != "" {
			if err := loader.LoadFromNacos(ctx, envNC, target); err == nil {
				return nil
			}
			// 若环境变量 Nacos 拉取失败，记录后继续尝试本地回退
		}
	}

	// 2. 本地文件优先解析 / 回退解析
	var localLoaded bool
	if file != "" {
		if _, err := os.Stat(file); err == nil {
			if err := conf.Load(file, target); err != nil {
				return fmt.Errorf("load local config file %s failed: %w", file, err)
			}
			localLoaded = true
		}
	}

	// 3. 检查是否有内嵌 Nacos 配置项（本地 YAML 声明了 Nacos）
	type nacosHolder interface {
		GetNacos() NacosConfig
	}

	if holder, ok := target.(nacosHolder); ok {
		nc := holder.GetNacos()
		if len(nc.ServerConfigs) > 0 {
			// 如果本地未指定 dataID，自动根据 target.Name 或服务名推导
			if nc.DataID == "" {
				name := inferServiceNameFromTarget(target)
				if name != "" {
					nc.DataID = name + ".yaml"
				} else if serviceNameHint != "" {
					nc.DataID = serviceNameHint + ".yaml"
				}
			}
			if nc.DataID != "" {
				if err := loader.LoadFromNacos(ctx, nc, target); err != nil {
					// 若本地有配置且成功加载过，Nacos 失败时可选择抛出或打印，此处严格遵循原有逻辑报错
					return fmt.Errorf("load from embedded nacos config failed: %w", err)
				}
				return nil
			}
		}
	}

	if localLoaded {
		return nil
	}

	if hasEnvNacos {
		return fmt.Errorf("load config failed: failed to load from nacos (%s, dataId: %s) and local file %q not found",
			envNC.ServerConfigs[0].IPAddr, envNC.DataID, file)
	}

	if file == "" {
		return fmt.Errorf("load config failed: no config file specified and no nacos environment found")
	}

	return fmt.Errorf("load config failed: config file %s not found", file)
}

// LoadFromNacos 从 Nacos 配置中心拉取配置内容并反序列化至 target 结构体中。
func (l *Loader) LoadFromNacos(ctx context.Context, nc NacosConfig, target any) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.client == nil {
		client, err := CreateNacosClient(nc)
		if err != nil {
			return fmt.Errorf("create nacos client failed: %w", err)
		}
		l.client = client
	}

	group := nc.Group
	if group == "" {
		group = DefaultGroup
	}

	content, err := l.client.GetConfig(vo.ConfigParam{
		DataId: nc.DataID,
		Group:  group,
	})
	if err != nil {
		return fmt.Errorf("get config from nacos failed (dataId: %s, group: %s): %w", nc.DataID, group, err)
	}

	if strings.TrimSpace(content) == "" {
		return fmt.Errorf("empty config content received from nacos (dataId: %s, group: %s)", nc.DataID, group)
	}

	return conf.LoadConfigFromYamlBytes([]byte(content), target)
}

// ListenConfig 注册配置热变更监听器。
// 当 Nacos 对应 DataID 和 Group 配置发生变动时，回调 onChange 函数。
func (l *Loader) ListenConfig(nc NacosConfig, onChange func(namespace, group, dataId, data string)) error {
	l.mu.RLock()
	client := l.client
	l.mu.RUnlock()

	if client == nil {
		var err error
		client, err = CreateNacosClient(nc)
		if err != nil {
			return fmt.Errorf("create nacos client for listening failed: %w", err)
		}
		l.mu.Lock()
		l.client = client
		l.mu.Unlock()
	}

	group := nc.Group
	if group == "" {
		group = DefaultGroup
	}

	return client.ListenConfig(vo.ConfigParam{
		DataId:   nc.DataID,
		Group:    group,
		OnChange: onChange,
	})
}
