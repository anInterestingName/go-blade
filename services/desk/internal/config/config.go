// Package config 定义协同工作台微服务的配置模型。
package config

import (
	pkgconfig "github.com/go-blade/pkg/config"
	"github.com/go-blade/pkg/database"
	"github.com/go-blade/pkg/mq"
	pkgredis "github.com/go-blade/pkg/redis"
	pkgrpc "github.com/go-blade/pkg/rpc"
	"github.com/zeromicro/go-zero/rest"
)

// Config 包含 desk 微服务运行所需的全部配置。
type Config struct {
	rest.RestConf
	RpcServer pkgrpc.ServerConfig   `json:"rpcServer,optional"`
	Nacos     pkgconfig.NacosConfig `json:"nacos,optional"`
	Redis     pkgredis.Config       `json:"redis,optional"`
	DB        database.Config       `json:"db,optional"`
	MQ        mq.Config             `json:"mq,optional"`
}

// GetNacos 返回 Nacos 配置中心与服务注册配置，实现 pkgconfig 的 nacosHolder 接口。
func (c Config) GetNacos() pkgconfig.NacosConfig {
	return c.Nacos
}
