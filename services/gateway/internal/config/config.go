// Package config 定义微服务统一网关的配置模型。
package config

import (
	pkgconfig "github.com/go-blade/pkg/config"
	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/pkg/oss"
	"github.com/go-blade/pkg/ratelimit"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/zeromicro/go-zero/rest"
)

// Config 定义网关的核心配置项。
type Config struct {
	rest.RestConf
	// Nacos 服务注册与动态发现配置
	Nacos pkgconfig.NacosConfig `json:"nacos,optional"`
	// Auth JWT 鉴权配置
	Auth AuthConfig `json:"auth"`
	// Upstreams 下游服务 gRPC 连接配置列表
	Upstreams UpstreamConfig `json:"upstreams"`
	// RateLimit 网关限流配置（可选）
	RateLimit ratelimit.Config `json:"rateLimit,optional"`
	// OSS 对象存储配置（可选）
	OSS oss.Config `json:"oss,optional"`
	// MQ 消息队列配置（可选，默认采用 Redis Streams + Memory 双模自适应驱动）
	MQ mq.Config `json:"mq,optional"`
}

// GetNacos 返回网关的 Nacos 配置信息，用于实现 NacosConfigProvider 接口以支持远程配置加载。
func (c Config) GetNacos() pkgconfig.NacosConfig {
	return c.Nacos
}

// AuthConfig 包含 JWT 校验密钥、JWKS 远程公钥端点与鉴权白名单配置。
type AuthConfig struct {
	// AccessSecret JWT 校验签名私钥（对称 HMAC 验签）
	AccessSecret string `json:"accessSecret"`
	// JWKSURL Ory Hydra 等 OIDC 提供方的 JWKS 公钥集合 URL（如 http://localhost:4444/.well-known/jwks.json，可选）
	JWKSURL string `json:"jwksUrl,optional"`
	// Whitelist 鉴权白名单路由列表（支持前缀匹配）
	Whitelist []string `json:"whitelist,optional"`
}

// UpstreamConfig 记录各下游微服务的 gRPC 寻址目标。
type UpstreamConfig struct {
	// Auth auth 认证微服务的 gRPC 目标地址（如 127.0.0.1:9001 或 nacos:///service-auth）
	Auth pkgrpc.ClientConfig `json:"auth"`
	// System system 系统微服务的 gRPC 目标地址
	System pkgrpc.ClientConfig `json:"system,optional"`
	// Desk desk 协同工作台微服务的 gRPC 目标地址
	Desk pkgrpc.ClientConfig `json:"desk,optional"`
}
