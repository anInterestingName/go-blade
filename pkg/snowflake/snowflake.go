// Package snowflake 提供具备防前端 JavaScript 精度溢出特性的雪花算法生成器与序列化类型。
package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/zeromicro/go-zero/core/logx"

	pkgredis "github.com/go-blade/pkg/redis"
)

const (
	// MaxNodeID 雪花算法允许的最大节点编号（10 位节点标识，取值范围 0~1023）。
	MaxNodeID int64 = 1023
	// NodeIDModulus 雪花算法节点编号取模基数（1024）。
	NodeIDModulus int64 = MaxNodeID + 1
	// RedisNodeKeyPrefix Redis 动态分配雪花节点编号的计数器键前缀。
	RedisNodeKeyPrefix = "blade:snowflake:node:"
)

var (
	// mu 保护 node 节点的并发读写与安全初始化
	mu sync.RWMutex
	// node 维护全局雪花算法节点单例
	node *snowflake.Node
)

// InitSnowflake 初始化雪花算法生成节点。
// nodeID 取值范围为 0-1023，用于微服务多节点分布式区分。
// 当传入合法 nodeID 时成功创建全局单例节点；若失败则返回错误且不会阻碍后续合法入参的重试初始化。
func InitSnowflake(nodeID int64) error {
	mu.Lock()
	defer mu.Unlock()

	newNode, err := snowflake.NewNode(nodeID)
	if err != nil {
		return err
	}
	node = newNode
	return nil
}

// InitDefaultNode 初始化全局默认雪花算法生成节点（InitSnowflake 的语义别名）。
func InitDefaultNode(nodeID int64) error {
	return InitSnowflake(nodeID)
}

// AllocateNodeID 为指定服务动态分配 0~1023 范围内的雪花算法 NodeID。
// 当全局 Redis 客户端已初始化且可用时，通过 Redis INCR 原子递增取模分配；
// 当 Redis 未初始化或调用出错时，自动降级为基于本机指纹（Hostname + 网卡 IP + 进程 PID + serviceName）的 FNV-1a 哈希取模值。
func AllocateNodeID(ctx context.Context, serviceName string) int64 {
	if ctx == nil {
		ctx = context.Background()
	}
	cleanServiceName := strings.TrimSpace(serviceName)
	if cleanServiceName == "" {
		cleanServiceName = "default"
	}

	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		seq, incrErr := rds.IncrCtx(ctx, RedisNodeKeyPrefix+cleanServiceName)
		if incrErr == nil && seq > 0 {
			return (seq - 1) % NodeIDModulus
		}
		logx.WithContext(ctx).Errorf("[snowflake] redis incr node id for %s failed, fallback to local fingerprint: %v", cleanServiceName, incrErr)
	}

	return fallbackFingerprintNodeID(cleanServiceName)
}

// InitAutoNode 动态分配雪花算法 NodeID 并初始化全局默认节点。
// 优先使用 Redis 原子自增分配；若 Redis 不可用则自动降级为本机进程指纹哈希，绝不阻塞微服务启动。
func InitAutoNode(ctx context.Context, serviceName string) (int64, error) {
	nodeID := AllocateNodeID(ctx, serviceName)
	if err := InitDefaultNode(nodeID); err != nil {
		return 0, err
	}
	return nodeID, nil
}

// fallbackFingerprintNodeID 基于本机 Hostname、网卡 IPv4、进程 PID 与服务名计算 FNV-1a 哈希并对 1024 取模。
func fallbackFingerprintNodeID(serviceName string) int64 {
	hostname, _ := os.Hostname()
	ip := localIPv4()
	pid := os.Getpid()
	raw := fmt.Sprintf("%s|%s|%d|%s", hostname, ip, pid, serviceName)

	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(raw))
	return int64(hasher.Sum32()) % NodeIDModulus
}

// localIPv4 获取本机首个非回环 IPv4 地址；若获取失败则返回 127.0.0.1。
func localIPv4() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "127.0.0.1"
	}
	for _, addr := range addrs {
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ip4 := ipNet.IP.To4(); ip4 != nil {
				return ip4.String()
			}
		}
	}
	return "127.0.0.1"
}

// getNode 安全获取当前 snowflake.Node 节点实例。
// 若未初始化或初始化失败，则自动以默认节点编号 1 兜底初始化。
func getNode() *snowflake.Node {
	mu.RLock()
	n := node
	mu.RUnlock()
	if n != nil {
		return n
	}

	mu.Lock()
	defer mu.Unlock()
	if node != nil {
		return node
	}

	var err error
	node, err = snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize default snowflake node: %v", err))
	}
	return node
}

// GenID 生成 64 位原生 int64 雪花 ID。若节点未初始化，将自动使用默认节点 1。
func GenID() int64 {
	return getNode().Generate().Int64()
}

// GenIDString 生成字符串格式的唯一雪花 ID，便于直接传递给外部客户端。
func GenIDString() string {
	return getNode().Generate().String()
}

// ID 自定义 64 位整型类型，用于解决 JavaScript 前端处理大于 2^53-1 (9007199254740991) 溢出截断的问题。
// 在执行 JSON 序列化时自动转为字符串格式，反序列化时无缝兼容数字与字符串输入。
type ID int64

// MarshalJSON 将 ID 序列化为符合 JSON 规范的字符串格式（如 "1782390182301"）。
func (i ID) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatInt(int64(i), 10))
}

// UnmarshalJSON 实现 json.Unmarshaler 接口，支持从数值或字符串还原为 ID。
func (i *ID) UnmarshalJSON(b []byte) error {
	// 如果是带引号的字符串格式
	if len(b) >= 2 && b[0] == '"' && b[len(b)-1] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		if s == "" {
			*i = 0
			return nil
		}
		val, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		*i = ID(val)
		return nil
	}

	// 如果直接是数值
	var val int64
	if err := json.Unmarshal(b, &val); err != nil {
		return fmt.Errorf("ID must be int64 or string, got %s", string(b))
	}
	*i = ID(val)
	return nil
}

// Int64 将自定义 ID 转换为 Go 基础 int64 类型。
func (i ID) Int64() int64 {
	return int64(i)
}

// String 将自定义 ID 转换为十进制字符串表达。
func (i ID) String() string {
	return strconv.FormatInt(int64(i), 10)
}

// NextID 生成带有防精度丢失特性的下一个雪花 ID 实例。
func NextID() ID {
	return ID(GenID())
}
