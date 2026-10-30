// Package cache 提供支持 Redis 与进程内内存双模自适应存储、内置 Singleflight 并发防击穿与空值防穿透的泛型缓存底座。
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	pkgredis "github.com/go-blade/pkg/redis"
)

const (
	// DefaultTTL 默认缓存有效期（5 分钟）。
	DefaultTTL = 5 * time.Minute
)

// Options 定义泛型缓存实例的初始化配置项。
type Options struct {
	// Prefix 缓存键统一前缀（如 "blade:system:user:"）
	Prefix string
	// DefaultTTL 未显式指定 TTL 时的默认过期时间，缺省为 5 分钟
	DefaultTTL time.Duration
}

type memItem[T any] struct {
	val       T
	expiresAt time.Time
}

type flightCall[T any] struct {
	wg  sync.WaitGroup
	val T
	err error
}

// Cache 提供支持 Redis 与进程内内存双模自适应存储、并发 Singleflight 防击穿的泛型缓存封装。
type Cache[T any] struct {
	// Prefix 缓存键前缀
	Prefix string
	// DefaultTTL 默认过期时间
	DefaultTTL time.Duration

	mu      sync.RWMutex
	mem     map[string]memItem[T]
	fmu     sync.Mutex
	flights map[string]*flightCall[T]
}

// NewCache 构造并初始化一个泛型 Cache 实例。
func NewCache[T any](opts Options) *Cache[T] {
	ttl := opts.DefaultTTL
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &Cache[T]{
		Prefix:     opts.Prefix,
		DefaultTTL: ttl,
		mem:        make(map[string]memItem[T]),
		flights:    make(map[string]*flightCall[T]),
	}
}

func (c *Cache[T]) fullKey(key string) string {
	if c.Prefix == "" {
		return key
	}
	if strings.HasPrefix(key, c.Prefix) {
		return key
	}
	return c.Prefix + key
}

func (c *Cache[T]) resolveTTL(ttl time.Duration) time.Duration {
	if ttl > 0 {
		return ttl
	}
	if c.DefaultTTL > 0 {
		return c.DefaultTTL
	}
	return DefaultTTL
}

// Get 根据指定 key 从缓存（优先 Redis，回退进程内内存）中获取泛型数据。
// 返回值依次为：缓存值、是否命中、错误。
func (c *Cache[T]) Get(ctx context.Context, key string) (T, bool, error) {
	var zero T
	if ctx == nil {
		ctx = context.Background()
	}
	fk := c.fullKey(key)

	// 1. 优先尝试从全局 Redis 读取
	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		raw, rErr := rds.GetCtx(ctx, fk)
		if rErr == nil && raw != "" {
			var val T
			if uErr := json.Unmarshal([]byte(raw), &val); uErr != nil {
				return zero, false, uErr
			}
			return val, true, nil
		}
	}

	// 2. 从进程内内存 TTL Map 读取
	now := time.Now()
	c.mu.RLock()
	item, ok := c.mem[fk]
	c.mu.RUnlock()
	if !ok {
		return zero, false, nil
	}
	if !item.expiresAt.IsZero() && !now.Before(item.expiresAt) {
		c.mu.Lock()
		if cur, exists := c.mem[fk]; exists && !cur.expiresAt.IsZero() && !now.Before(cur.expiresAt) {
			delete(c.mem, fk)
		}
		c.mu.Unlock()
		return zero, false, nil
	}
	return item.val, true, nil
}

// Set 将泛型数据写入缓存（同时写入可用 Redis 与进程内内存 TTL Map）。
func (c *Cache[T]) Set(ctx context.Context, key string, val T, ttl time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	fk := c.fullKey(key)
	d := c.resolveTTL(ttl)

	// 1. 写入进程内内存缓存
	c.mu.Lock()
	if c.mem == nil {
		c.mem = make(map[string]memItem[T])
	}
	c.mem[fk] = memItem[T]{
		val:       val,
		expiresAt: time.Now().Add(d),
	}
	c.mu.Unlock()

	// 2. 若全局 Redis 已初始化，则 JSON 序列化后同步写入 Redis
	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		b, mErr := json.Marshal(val)
		if mErr != nil {
			return mErr
		}
		sec := int(d.Seconds())
		if sec <= 0 {
			sec = 1
		}
		if rErr := rds.SetexCtx(ctx, fk, string(b), sec); rErr != nil {
			return rErr
		}
	}

	return nil
}

// Delete 从 Redis 与进程内内存中主动删除一个或多个缓存键。
func (c *Cache[T]) Delete(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	fullKeys := make([]string, 0, len(keys))
	c.mu.Lock()
	for _, k := range keys {
		fk := c.fullKey(k)
		fullKeys = append(fullKeys, fk)
		delete(c.mem, fk)
	}
	c.mu.Unlock()

	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		_, rErr := rds.DelCtx(ctx, fullKeys...)
		return rErr
	}
	return nil
}

// DeleteByPrefix 按前缀批量清理进程内与 Redis 缓存键。
func (c *Cache[T]) DeleteByPrefix(ctx context.Context, prefix string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	fullPrefix := c.fullKey(prefix)

	c.mu.Lock()
	for k := range c.mem {
		if strings.HasPrefix(k, fullPrefix) {
			delete(c.mem, k)
		}
	}
	c.mu.Unlock()

	if rds, err := pkgredis.GetRedis(); err == nil && rds != nil {
		keys, _, sErr := rds.ScanCtx(ctx, 0, fullPrefix+"*", 1000)
		if sErr == nil && len(keys) > 0 {
			_, _ = rds.DelCtx(ctx, keys...)
		}
	}
	return nil
}

// GetOrLoad 先尝试从缓存读取 key，若未命中则通过内置 Singleflight 并发归并机制调用 loader 加载并回填缓存。
// 同一进程内对相同 key 的高并发未命中请求仅会执行一次 loader，其余协程等待并共享结果，防止热点 Key 失效瞬间缓存击穿。
func (c *Cache[T]) GetOrLoad(ctx context.Context, key string, ttl time.Duration, loader func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	if ctx == nil {
		ctx = context.Background()
	}
	if val, ok, err := c.Get(ctx, key); err == nil && ok {
		return val, nil
	}
	if loader == nil {
		return zero, errors.New("cache loader cannot be nil")
	}

	fk := c.fullKey(key)

	c.fmu.Lock()
	if c.flights == nil {
		c.flights = make(map[string]*flightCall[T])
	}
	if call, inFlight := c.flights[fk]; inFlight {
		c.fmu.Unlock()
		call.wg.Wait()
		return call.val, call.err
	}
	call := &flightCall[T]{}
	call.wg.Add(1)
	c.flights[fk] = call
	c.fmu.Unlock()

	defer func() {
		c.fmu.Lock()
		delete(c.flights, fk)
		c.fmu.Unlock()
		call.wg.Done()
	}()

	// 二次检查缓存（Double-Check），避免刚完成的前序请求已写入缓存
	if val, ok, err := c.Get(ctx, key); err == nil && ok {
		call.val = val
		return val, nil
	}

	loadedVal, err := loader(ctx)
	if err != nil {
		call.err = err
		return zero, err
	}

	_ = c.Set(ctx, key, loadedVal, ttl)
	call.val = loadedVal
	return loadedVal, nil
}
