// Package ratelimit 提供分布式 Redis Lua 与进程内并发安全双模限流器及 HTTP 429 限流中间件。
package ratelimit

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/tenant"
)

const (
	// DefaultRate 默认每个时间窗口允许的请求上限。
	DefaultRate = 100
	// DefaultWindow 默认限流时间窗口大小（1秒）。
	DefaultWindow = time.Second
	// DefaultKeyPrefix 限流键在 Redis 与内存中的默认统一前缀。
	DefaultKeyPrefix = "blade:ratelimit:"
	// TooManyRequestsMessage 触发限流时返回的默认中文提示语。
	TooManyRequestsMessage = "请求过于频繁，请稍后再试"
)

// rateLimitLuaScript 基于 Redis 的原子固定窗口计数限流脚本（毫秒级过期）。
// KEYS[1]: 限流键名
// ARGV[1]: 允许的最大请求次数 Rate
// ARGV[2]: 窗口毫秒数 WindowMs
// 返回值：1 表示允许放行，0 表示超出阈值被限流。
const rateLimitLuaScript = `
local current = redis.call("INCR", KEYS[1])
if current == 1 then
    redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))
else
    local ttl = redis.call("PTTL", KEYS[1])
    if ttl < 0 then
        redis.call("PEXPIRE", KEYS[1], tonumber(ARGV[2]))
    end
end
if current > tonumber(ARGV[1]) then
    return 0
end
return 1
`

// Config 限流器配置结构体。
type Config struct {
	// Rate 每个时间窗口内允许的最大请求数（默认 100）。
	Rate int `json:"rate,optional"`
	// Window 限流统计时间窗口长度（默认 1s）。
	Window time.Duration `json:"window,optional"`
	// KeyPrefix 限流键名前缀（默认 "blade:ratelimit:"）。
	KeyPrefix string `json:"keyPrefix,optional"`
}

// applyDefaults 补全 Config 的零值默认项。
func (c *Config) applyDefaults() {
	if c.Rate <= 0 {
		c.Rate = DefaultRate
	}
	if c.Window <= 0 {
		c.Window = DefaultWindow
	}
	if strings.TrimSpace(c.KeyPrefix) == "" {
		c.KeyPrefix = DefaultKeyPrefix
	}
}

type windowCounter struct {
	count    int
	expireAt time.Time
}

// Limiter 支持分布式 Redis Lua 脚本与进程内内存回退的双模限流器。
type Limiter struct {
	cfg      Config
	mu       sync.Mutex
	counters map[string]windowCounter
}

// NewLimiter 根据指定配置构造并返回一个双模限流器实例。
func NewLimiter(cfg Config) *Limiter {
	cfg.applyDefaults()
	return &Limiter{
		cfg:      cfg,
		counters: make(map[string]windowCounter),
	}
}

// Config 返回当前限流器生效的配置副本。
func (l *Limiter) Config() Config {
	if l == nil {
		cfg := Config{}
		cfg.applyDefaults()
		return cfg
	}
	return l.cfg
}

// Allow 判断指定业务键 key 在当前窗口内是否允许放行。
// 当全局 pkg/redis 已初始化时使用 Redis Lua 脚本执行原子计数窗口限流；
// 当 Redis 未初始化时自动回退至进程内并发安全计数器，确保单机或测试环境零外部依赖即可精确限流。
func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	if l == nil {
		l = NewLimiter(Config{})
	}
	if ctx == nil {
		ctx = context.Background()
	}

	fullKey := l.buildKey(key)
	windowMs := l.cfg.Window.Milliseconds()
	if windowMs <= 0 {
		windowMs = 1
	}

	if r, err := pkgredis.GetRedis(); err == nil && r != nil {
		res, err := r.EvalCtx(
			ctx,
			rateLimitLuaScript,
			[]string{fullKey},
			strconv.Itoa(l.cfg.Rate),
			strconv.FormatInt(windowMs, 10),
		)
		if err != nil {
			return false, fmt.Errorf("redis rate limit eval failed: %w", err)
		}
		if code, ok := res.(int64); ok {
			return code == 1, nil
		}
		return false, nil
	}

	return l.allowMemory(fullKey), nil
}

func (l *Limiter) buildKey(key string) string {
	clean := strings.TrimSpace(key)
	if clean == "" {
		clean = "default"
	}
	if strings.HasPrefix(clean, l.cfg.KeyPrefix) {
		return clean
	}
	return l.cfg.KeyPrefix + clean
}

func (l *Limiter) allowMemory(fullKey string) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.counters == nil {
		l.counters = make(map[string]windowCounter)
	}

	entry, exists := l.counters[fullKey]
	if !exists || !now.Before(entry.expireAt) {
		l.counters[fullKey] = windowCounter{
			count:    1,
			expireAt: now.Add(l.cfg.Window),
		}
		return l.cfg.Rate >= 1
	}

	entry.count++
	l.counters[fullKey] = entry
	return entry.count <= l.cfg.Rate
}

// DefaultKeyExtractor 默认的 HTTP 请求限流维度提取函数：
// 提取格式为 "<tenantID>:<path>:<clientIP>"。
func DefaultKeyExtractor(r *http.Request) string {
	tenantID := tenant.FromTenant(r.Context())
	if hdrTenant := strings.TrimSpace(r.Header.Get(tenant.HeaderTenantID)); hdrTenant != "" && tenantID == tenant.DefaultTenantID {
		tenantID = hdrTenant
	}
	clientIP := extractClientIP(r)
	return tenantID + ":" + r.URL.Path + ":" + clientIP
}

func extractClientIP(r *http.Request) string {
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := strings.TrimSpace(parts[0]); ip != "" {
			return ip
		}
	}
	if xri := strings.TrimSpace(r.Header.Get("X-Real-Ip")); xri != "" {
		return xri
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil && host != "" {
		return host
	}
	if strings.TrimSpace(r.RemoteAddr) != "" {
		return strings.TrimSpace(r.RemoteAddr)
	}
	return "unknown"
}

// RateLimitMiddleware 构造 HTTP 限流保护中间件。
// 当 keyExtractor 为 nil 时默认按 "租户 + 路由路径 + 客户端 IP" 维度限流；
// 当超出窗口限流阈值时返回 HTTP 429 TooManyRequests 及统一 Response JSON 结构。
func RateLimitMiddleware(limiter *Limiter, keyExtractor func(r *http.Request) string) func(http.HandlerFunc) http.HandlerFunc {
	if limiter == nil {
		limiter = NewLimiter(Config{})
	}
	if keyExtractor == nil {
		keyExtractor = DefaultKeyExtractor
	}

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			key := keyExtractor(r)
			allowed, err := limiter.Allow(r.Context(), key)
			if err != nil || !allowed {
				response.FailJsonWithStatus(
					r.Context(),
					w,
					http.StatusTooManyRequests,
					http.StatusTooManyRequests,
					TooManyRequestsMessage,
				)
				return
			}
			next(w, r)
		}
	}
}
