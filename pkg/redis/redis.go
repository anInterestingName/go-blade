// Package redis 提供基于 go-zero 的 Redis 客户端单例封装与防重放验证码存储能力。
package redis

import (
	"errors"
	"sync"
	"time"

	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

var (
	// ErrRedisNotInitialized 表示 Redis 客户端未被初始化。
	ErrRedisNotInitialized = errors.New("redis client is not initialized")
)

var (
	defaultRedis *zeroredis.Redis
	mu           sync.RWMutex
)

// Config Redis 连接配置结构体。
type Config struct {
	// Host Redis 节点或集群连接地址，默认 "127.0.0.1:6379"。
	Host string `json:",default=127.0.0.1:6379"`
	// Type 部署架构类型，可选 "node" 或 "cluster"，默认 "node"。
	Type string `json:",default=node,options=node|cluster"`
	// Pass Redis 认证密码，可选。
	Pass string `json:",optional"`
	// TLS 是否开启 TLS 加密传输，可选。
	TLS bool `json:",optional"`
	// PingTimeout Redis Ping 心跳超时时间，默认 1s。
	PingTimeout time.Duration `json:",default=1s"`
}

// applyDefaults 补全 Config 的零值，提供默认配置项。
func (c *Config) applyDefaults() {
	if c.Host == "" {
		c.Host = "127.0.0.1:6379"
	}
	if c.Type == "" {
		c.Type = zeroredis.NodeType
	}
	if c.PingTimeout == 0 {
		c.PingTimeout = time.Second
	}
}

// InitRedis 初始化 Redis 全局单例实例。
// 支持在初始化失败后重试，避免死锁；线程安全。
func InitRedis(cfg Config) (*zeroredis.Redis, error) {
	cfg.applyDefaults()

	mu.Lock()
	defer mu.Unlock()

	conf := zeroredis.RedisConf{
		Host:        cfg.Host,
		Type:        cfg.Type,
		Pass:        cfg.Pass,
		Tls:         cfg.TLS,
		PingTimeout: cfg.PingTimeout,
	}

	client, err := zeroredis.NewRedis(conf)
	if err != nil {
		return nil, err
	}

	defaultRedis = client
	return defaultRedis, nil
}

// GetRedis 获取全局 Redis 客户端单例。
// 若尚未初始化，则返回 ErrRedisNotInitialized。
func GetRedis() (*zeroredis.Redis, error) {
	mu.RLock()
	defer mu.RUnlock()

	if defaultRedis == nil {
		return nil, ErrRedisNotInitialized
	}
	return defaultRedis, nil
}

// SetRedis 用于在单元测试中注入自定义或 mock 的 Redis 客户端实例。
func SetRedis(r *zeroredis.Redis) {
	mu.Lock()
	defer mu.Unlock()
	defaultRedis = r
}
