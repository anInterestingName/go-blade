// Package lock 提供基于 Redis Lua 原子脚本与内存回退的分布式互斥锁单元测试。
package lock

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	pkgredis "github.com/go-blade/pkg/redis"
	zeroredis "github.com/zeromicro/go-zero/core/stores/redis"
)

func setupMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := zeroredis.MustNewRedis(zeroredis.RedisConf{
		Host: mr.Addr(),
		Type: zeroredis.NodeType,
	})
	pkgredis.SetRedis(client)

	t.Cleanup(func() {
		mr.Close()
		pkgredis.SetRedis(nil)
	})
	return mr
}

func TestRedisLock_AcquireAndUnlock(t *testing.T) {
	mr := setupMiniredis(t)
	ctx := context.Background()

	// 1. 协程 A 获取锁成功
	lockA, err := Acquire(ctx, "order:pay:1001", 5*time.Second)
	if err != nil {
		t.Fatalf("expected Acquire to succeed, got: %v", err)
	}
	if lockA.Key() != "blade:lock:order:pay:1001" {
		t.Errorf("unexpected key: %s", lockA.Key())
	}
	if lockA.Token() == "" {
		t.Error("expected non-empty token")
	}

	// 2. 协程 B 尝试获取同一把锁应当失败
	_, err = Acquire(ctx, "order:pay:1001", 5*time.Second)
	if !errors.Is(err, ErrLockNotAcquired) {
		t.Fatalf("expected ErrLockNotAcquired, got: %v", err)
	}

	// 3. 伪造另一个锁实例尝试释放 A 的锁（验证防误删）
	lockFake, _ := NewRedisLock("order:pay:1001", 5*time.Second)
	if err := lockFake.Unlock(ctx); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected fake unlock to return ErrLockNotHeld, got: %v", err)
	}

	// 确认 Redis 中锁仍然属于 A
	val, err := mr.Get(lockA.Key())
	if err != nil || val != lockA.Token() {
		t.Fatalf("expected lock token %s in redis, got %s (err=%v)", lockA.Token(), val, err)
	}

	// 4. 续期测试
	refreshed, err := lockA.Refresh(ctx, 10*time.Second)
	if err != nil || !refreshed {
		t.Fatalf("expected Refresh to succeed, got refreshed=%v, err=%v", refreshed, err)
	}
	fakeRefreshed, err := lockFake.Refresh(ctx, 10*time.Second)
	if err != nil || fakeRefreshed {
		t.Fatalf("expected fake Refresh to return false, got %v, err=%v", fakeRefreshed, err)
	}

	// 5. 协程 A 正常释放锁
	if err := lockA.Unlock(ctx); err != nil {
		t.Fatalf("expected lockA Unlock to succeed, got: %v", err)
	}

	// 6. 释放后协程 B 可以再次获取
	lockB, err := Acquire(ctx, "order:pay:1001", 5*time.Second)
	if err != nil {
		t.Fatalf("expected lockB Acquire after unlock to succeed, got: %v", err)
	}
	_ = lockB.Unlock(ctx)
}

func TestRedisLock_ExpirationAndAntiMisdelete(t *testing.T) {
	mr := setupMiniredis(t)
	ctx := context.Background()

	lockA, err := Acquire(ctx, "stock:deduct:2002", 2*time.Second)
	if err != nil {
		t.Fatalf("Acquire A failed: %v", err)
	}

	// 模拟锁 A 超时过期
	mr.FastForward(3 * time.Second)

	// 锁 B 成功获取同名锁
	lockB, err := Acquire(ctx, "stock:deduct:2002", 5*time.Second)
	if err != nil {
		t.Fatalf("Acquire B after A expired failed: %v", err)
	}

	// 锁 A 此时苏醒尝试 Unlock，绝对不能误删锁 B！
	if err := lockA.Unlock(ctx); !errors.Is(err, ErrLockNotHeld) {
		t.Fatalf("expected expired lockA Unlock to fail with ErrLockNotHeld, got: %v", err)
	}

	// 锁 B 正常释放应当成功
	if err := lockB.Unlock(ctx); err != nil {
		t.Fatalf("expected lockB Unlock to succeed, got: %v", err)
	}
}

func TestMemoryFallbackLock_Concurrency(t *testing.T) {
	// 确保全局 Redis 为未初始化状态，触发内置内存锁回退
	pkgredis.SetRedis(nil)
	ctx := context.Background()

	const workers = 20
	var successCount int32
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			l, err := Acquire(ctx, "mem:concurrent:key", 2*time.Second)
			if err == nil && l != nil {
				atomic.AddInt32(&successCount, 1)
				// 保持持有直到所有协程都尝试过一次非阻塞获取
				time.Sleep(50 * time.Millisecond)
				_ = l.Unlock(ctx)
			}
		}()
	}

	wg.Wait()
	if successCount != 1 {
		t.Fatalf("expected exactly 1 goroutine to acquire non-blocking lock, got %d", successCount)
	}

	// 释放后再次获取与续期测试
	l, err := Acquire(ctx, "mem:concurrent:key", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Acquire after release failed: %v", err)
	}
	ok, err := l.Refresh(ctx, 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("memory lock refresh failed: ok=%v, err=%v", ok, err)
	}
	if err := l.Unlock(ctx); err != nil {
		t.Fatalf("memory lock unlock failed: %v", err)
	}
}
