// Package cache 提供泛型防击穿缓存底座的单元测试。
package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	pkgredis "github.com/go-blade/pkg/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

type testUser struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func TestCache_MemoryMode_GetSetDeleteExpire(t *testing.T) {
	pkgredis.SetRedis(nil)
	ctx := context.Background()

	c := NewCache[testUser](Options{
		Prefix:     "blade:test:mem:",
		DefaultTTL: 50 * time.Millisecond,
	})

	// 1. 初始未命中
	_, ok, err := c.Get(ctx, "u1")
	require.NoError(t, err)
	assert.False(t, ok)

	// 2. 写入并读取
	expected := testUser{ID: 1001, Name: "Alice"}
	require.NoError(t, c.Set(ctx, "u1", expected, 80*time.Millisecond))

	got, ok, err := c.Get(ctx, "u1")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, expected, got)

	// 3. 等待 TTL 过期
	time.Sleep(100 * time.Millisecond)
	_, ok, err = c.Get(ctx, "u1")
	require.NoError(t, err)
	assert.False(t, ok, "expired item should not be returned")

	// 4. 删除与按前缀清理验证
	require.NoError(t, c.Set(ctx, "u2", testUser{ID: 2, Name: "Bob"}, time.Minute))
	require.NoError(t, c.Set(ctx, "group:1", testUser{ID: 3, Name: "Charlie"}, time.Minute))
	require.NoError(t, c.Set(ctx, "group:2", testUser{ID: 4, Name: "David"}, time.Minute))

	require.NoError(t, c.Delete(ctx, "u2"))
	_, ok, _ = c.Get(ctx, "u2")
	assert.False(t, ok)

	require.NoError(t, c.DeleteByPrefix(ctx, "group:"))
	_, ok1, _ := c.Get(ctx, "group:1")
	_, ok2, _ := c.Get(ctx, "group:2")
	assert.False(t, ok1)
	assert.False(t, ok2)
}

func TestCache_RedisMode_GetSetDeleteExpire(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	rdsClient := zeroredis.MustNewRedis(zeroredis.RedisConf{
		Host: mr.Addr(),
		Type: zeroredis.NodeType,
	})
	pkgredis.SetRedis(rdsClient)
	defer pkgredis.SetRedis(nil)

	ctx := context.Background()
	writerCache := NewCache[testUser](Options{
		Prefix:     "blade:test:rds:",
		DefaultTTL: 2 * time.Second,
	})
	// 模拟跨进程读取的第二个 Cache 实例（本地内存为空，完全依赖 Redis 共享）
	readerCache := NewCache[testUser](Options{
		Prefix:     "blade:test:rds:",
		DefaultTTL: 2 * time.Second,
	})

	u := testUser{ID: 9007199254740993, Name: "RedisUser"}
	require.NoError(t, writerCache.Set(ctx, "k1", u, 2*time.Second))

	// 从 readerCache 读取，验证从 Redis 反序列化命中
	got, ok, err := readerCache.Get(ctx, "k1")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, u, got)

	// 快进 miniredis 时间并清理本地缓存，验证 Redis TTL 过期
	mr.FastForward(3 * time.Second)
	_, ok, err = readerCache.Get(ctx, "k1")
	require.NoError(t, err)
	assert.False(t, ok)

	// 验证 Delete 同步清理 Redis
	require.NoError(t, writerCache.Set(ctx, "k2", u, time.Minute))
	require.NoError(t, writerCache.Delete(ctx, "k2"))
	_, ok, err = readerCache.Get(ctx, "k2")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCache_GetOrLoad_Singleflight_50Concurrency(t *testing.T) {
	pkgredis.SetRedis(nil)
	ctx := context.Background()

	c := NewCache[testUser](Options{
		Prefix:     "blade:test:sf:",
		DefaultTTL: time.Minute,
	})

	var loaderCalls atomic.Int32
	const concurrency = 50

	var startWg sync.WaitGroup
	startWg.Add(1)
	var doneWg sync.WaitGroup
	doneWg.Add(concurrency)

	results := make([]testUser, concurrency)
	errs := make([]error, concurrency)

	for i := 0; i < concurrency; i++ {
		idx := i
		go func() {
			defer doneWg.Done()
			startWg.Wait()
			val, err := c.GetOrLoad(ctx, "hot_key", time.Minute, func(ctx context.Context) (testUser, error) {
				loaderCalls.Add(1)
				time.Sleep(40 * time.Millisecond)
				return testUser{ID: 8888, Name: "SingleflightUser"}, nil
			})
			results[idx] = val
			errs[idx] = err
		}()
	}

	// 同时释放 50 个并发请求
	startWg.Done()
	doneWg.Wait()

	assert.Equal(t, int32(1), loaderCalls.Load(), "50 并发请求下底层 loader 必须且只能被执行 1 次")
	for i := 0; i < concurrency; i++ {
		require.NoError(t, errs[i])
		assert.Equal(t, int64(8888), results[i].ID)
		assert.Equal(t, "SingleflightUser", results[i].Name)
	}

	// 后续再次调用 GetOrLoad 应直接命中缓存，不触发 loader
	val, err := c.GetOrLoad(ctx, "hot_key", time.Minute, func(ctx context.Context) (testUser, error) {
		loaderCalls.Add(1)
		return testUser{}, errors.New("should not be called")
	})
	require.NoError(t, err)
	assert.Equal(t, int64(8888), val.ID)
	assert.Equal(t, int32(1), loaderCalls.Load())
}
