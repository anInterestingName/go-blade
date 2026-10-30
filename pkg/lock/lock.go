// Package lock 提供基于 Redis Lua 原子脚本的分布式互斥锁组件。
// 支持唯一持有者标识（Token）防误删、原子锁续期（Refresh），
// 并在 Redis 未初始化时自动无缝回退至进程内并发安全的内存锁机制。
package lock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	pkgredis "github.com/go-blade/pkg/redis"
)

const (
	// DefaultLockKeyPrefix 分布式锁在 Redis 中的默认统一键前缀。
	DefaultLockKeyPrefix = "blade:lock:"
	// DefaultLockTTL 分布式锁未显式指定有效期时的默认过期时间（10秒）。
	DefaultLockTTL = 10 * time.Second
)

var (
	// ErrEmptyLockKey 表示加锁的业务键名不能为空。
	ErrEmptyLockKey = errors.New("lock key must not be empty")
	// ErrLockNotAcquired 表示目标资源已被其他协程或节点持有，当前未能获取锁。
	ErrLockNotAcquired = errors.New("lock is already held by another holder")
	// ErrLockNotHeld 表示释放或续期锁时，当前实例并非该锁的持有者（或锁已过期）。
	ErrLockNotHeld = errors.New("lock is not held by current instance or has expired")
)

// unlockLuaScript 原子比对持有者 Token 并删除锁，防止锁超时后误删其他协程/节点的锁。
// 返回值：1 表示匹配并成功删除，0 表示锁不存在或 Token 不匹配。
const unlockLuaScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`

// refreshLuaScript 原子比对持有者 Token 并延长锁过期时间（单位：秒）。
// 返回值：1 表示续期成功，0 表示锁不存在或 Token 不匹配。
const refreshLuaScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("EXPIRE", KEYS[1], tonumber(ARGV[2]))
else
    return 0
end
`

// Locker 定义分布式互斥锁的标准操作契约。
type Locker interface {
	// TryLock 尝试立即获取分布式锁（非阻塞），返回是否获取成功。
	TryLock(ctx context.Context) (bool, error)
	// Refresh 对当前持有的锁执行原子续期。
	Refresh(ctx context.Context, ttl time.Duration) (bool, error)
	// Unlock 原子校验持有者身份并释放锁；若锁已过期或被其他持有者抢占则返回 ErrLockNotHeld。
	Unlock(ctx context.Context) error
	// Key 返回完整的锁键名。
	Key() string
	// Token 返回当前锁实例生成的唯一持有者标识。
	Token() string
}

// memoryEntry 表示进程内内存回退锁的持有记录。
type memoryEntry struct {
	token    string
	expireAt time.Time
}

var (
	memMu    sync.Mutex
	memLocks = make(map[string]memoryEntry)
)

// Lock 分布式锁实例（同时作为 RedisLock 的别名主体）。
// 当全局 pkg/redis 已初始化时使用 Redis SetnxEx + Lua 脚本实现；
// 当 Redis 未初始化时自动使用进程内内存锁回退，确保单测与降级场景下的并发安全。
type Lock struct {
	key   string
	token string
	ttl   time.Duration
}

// RedisLock 是 Lock 的类型别名，对齐分布式锁命名约定。
type RedisLock = Lock

// NewLock 构造一个针对指定业务 key 的分布式锁实例（尚未执行加锁）。
// 若 key 未包含 `blade:` 前缀，会自动补全 `blade:lock:` 前缀；若已包含 `blade:` 前缀则保持原样。
func NewLock(key string, ttl time.Duration) (*Lock, error) {
	cleanKey := strings.TrimSpace(key)
	if cleanKey == "" {
		return nil, ErrEmptyLockKey
	}
	if !strings.HasPrefix(cleanKey, "blade:") {
		cleanKey = DefaultLockKeyPrefix + cleanKey
	}
	if ttl <= 0 {
		ttl = DefaultLockTTL
	}
	token, err := generateToken()
	if err != nil {
		return nil, fmt.Errorf("failed to generate lock token: %w", err)
	}
	return &Lock{
		key:   cleanKey,
		token: token,
		ttl:   ttl,
	}, nil
}

// NewRedisLock 构造一个分布式锁实例（NewLock 的别名函数）。
func NewRedisLock(key string, ttl time.Duration) (*RedisLock, error) {
	return NewLock(key, ttl)
}

// Acquire 构造并立即尝试获取指定 key 的分布式锁。
// 若获取成功则返回 *Lock 实例；若锁已被占用则返回 ErrLockNotAcquired。
func Acquire(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	l, err := NewLock(key, ttl)
	if err != nil {
		return nil, err
	}
	ok, err := l.TryLock(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrLockNotAcquired
	}
	return l, nil
}

// Key 返回当前锁实例的完整键名。
func (l *Lock) Key() string {
	return l.key
}

// Token 返回当前锁实例的唯一持有者随机标识。
func (l *Lock) Token() string {
	return l.token
}

// TryLock 尝试非阻塞获取分布式锁。
// 返回 true 表示成功获取锁；返回 false 表示锁已被其他协程或节点持有。
func (l *Lock) TryLock(ctx context.Context) (bool, error) {
	if l == nil || l.key == "" {
		return false, ErrEmptyLockKey
	}
	seconds := durationToSeconds(l.ttl)

	if r, err := pkgredis.GetRedis(); err == nil && r != nil {
		ok, err := r.SetnxExCtx(ctx, l.key, l.token, seconds)
		if err != nil {
			return false, fmt.Errorf("redis setnxex failed: %w", err)
		}
		return ok, nil
	}

	return tryLockMemory(l.key, l.token, l.ttl), nil
}

// Refresh 原子校验持有者 Token 并延长锁的有效期。
// 返回 true 表示续期成功；返回 false 表示锁已过期或并非由当前实例持有。
func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) (bool, error) {
	if l == nil || l.key == "" {
		return false, ErrEmptyLockKey
	}
	if ttl <= 0 {
		ttl = l.ttl
	}
	seconds := durationToSeconds(ttl)

	if r, err := pkgredis.GetRedis(); err == nil && r != nil {
		res, err := r.EvalCtx(ctx, refreshLuaScript, []string{l.key}, l.token, strconv.Itoa(seconds))
		if err != nil {
			return false, fmt.Errorf("redis eval refresh script failed: %w", err)
		}
		if code, ok := res.(int64); ok && code == 1 {
			l.ttl = ttl
			return true, nil
		}
		return false, nil
	}

	ok := refreshMemory(l.key, l.token, ttl)
	if ok {
		l.ttl = ttl
	}
	return ok, nil
}

// Unlock 原子比对持有者 Token 并释放锁。
// 若当前实例并非该锁持有者（例如锁已超时自动释放或被其他协程持有），返回 ErrLockNotHeld，绝不误删他人的锁。
func (l *Lock) Unlock(ctx context.Context) error {
	if l == nil || l.key == "" {
		return ErrEmptyLockKey
	}

	if r, err := pkgredis.GetRedis(); err == nil && r != nil {
		res, err := r.EvalCtx(ctx, unlockLuaScript, []string{l.key}, l.token)
		if err != nil {
			return fmt.Errorf("redis eval unlock script failed: %w", err)
		}
		if code, ok := res.(int64); ok && code == 1 {
			return nil
		}
		return ErrLockNotHeld
	}

	if !unlockMemory(l.key, l.token) {
		return ErrLockNotHeld
	}
	return nil
}

func durationToSeconds(d time.Duration) int {
	sec := int(d.Seconds())
	if sec <= 0 {
		return 1
	}
	return sec
}

func generateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func tryLockMemory(key, token string, ttl time.Duration) bool {
	now := time.Now()
	memMu.Lock()
	defer memMu.Unlock()

	if entry, exists := memLocks[key]; exists {
		if now.Before(entry.expireAt) {
			return false
		}
	}

	memLocks[key] = memoryEntry{
		token:    token,
		expireAt: now.Add(ttl),
	}
	return true
}

func refreshMemory(key, token string, ttl time.Duration) bool {
	now := time.Now()
	memMu.Lock()
	defer memMu.Unlock()

	entry, exists := memLocks[key]
	if !exists || !now.Before(entry.expireAt) || entry.token != token {
		return false
	}

	entry.expireAt = now.Add(ttl)
	memLocks[key] = entry
	return true
}

func unlockMemory(key, token string) bool {
	now := time.Now()
	memMu.Lock()
	defer memMu.Unlock()

	entry, exists := memLocks[key]
	if !exists {
		return false
	}
	if !now.Before(entry.expireAt) {
		delete(memLocks, key)
		return false
	}
	if entry.token != token {
		return false
	}

	delete(memLocks, key)
	return true
}
