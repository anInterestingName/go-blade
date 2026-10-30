// Package config 定义认证微服务的配置结构。
package config

import (
	pkgconfig "github.com/go-blade/pkg/config"
	"github.com/go-blade/pkg/database"
	pkgredis "github.com/go-blade/pkg/redis"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/zeromicro/go-zero/rest"
)

// Config 包含 auth 微服务的所有配置项。
type Config struct {
	rest.RestConf
	RpcServer pkgrpc.ServerConfig   `json:"rpcServer,optional"`
	Nacos     pkgconfig.NacosConfig `json:"nacos,optional"`
	Auth      AuthConfig
	Redis     pkgredis.Config
	DB        database.Config
}

// AuthConfig 包含 JWT 签发与鉴权相关配置。
type AuthConfig struct {
	AccessSecret  string
	AccessExpire  int64
	RefreshExpire int64
	Issuer        string
	HydraAdminURL string `json:",optional"`
}

// GetNacos 返回 Nacos 配置中心与服务注册配置，实现 pkgconfig 的 nacosHolder 接口。
func (c Config) GetNacos() pkgconfig.NacosConfig {
	return c.Nacos
}
