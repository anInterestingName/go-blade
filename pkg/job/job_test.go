// Package job 提供分布式定时任务调度器的单元测试，验证多副本互斥执行、Panic 捕获恢复与超时取消。
package job

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduler_DistributedLock_MultiInstanceMutualExclusion(t *testing.T) {
	const instanceCount = 5
	var executedCount atomic.Int32

	schedulers := make([]*Scheduler, instanceCount)
	for i := 0; i < instanceCount; i++ {
		s := NewScheduler()
		err := s.RegisterInterval(
			"sync-tenant-quota",
			100*time.Millisecond,
			func(ctx context.Context) error {
				executedCount.Add(1)
				return nil
			},
			WithDistributedLock(true, 2*time.Second),
		)
		require.NoError(t, err)
		schedulers[i] = s
	}

	// 模拟 5 个 K8s 副本同时在同一窗口触发定时任务
	var wg sync.WaitGroup
	wg.Add(instanceCount)
	for i := 0; i < instanceCount; i++ {
		go func(idx int) {
			defer wg.Done()
			err := schedulers[idx].RunOnce(context.Background(), "sync-tenant-quota")
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int32(1), executedCount.Load(), "多副本并发触发同一分布式锁任务时，同一锁周期窗口内只能有 1 个副本实际执行")
}

func TestScheduler_PanicRecovery_And_TimeoutCancellation(t *testing.T) {
	s := NewScheduler()

	// 1. 注册一个会触发 Panic 的任务，验证调度器不会崩溃
	err := s.RegisterInterval(
		"panic-cleanup-job",
		time.Minute,
		func(ctx context.Context) error {
			panic("unexpected nil pointer in cleanup job")
		},
		WithDistributedLock(false),
	)
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		runErr := s.RunOnce(context.Background(), "panic-cleanup-job")
		require.Error(t, runErr)
		assert.Contains(t, runErr.Error(), "panicked")
	})

	// 2. 注册一个超时任务，验证 WithTimeout 控制生效
	var timeoutObserved atomic.Bool
	err = s.RegisterInterval(
		"slow-report-job",
		time.Minute,
		func(ctx context.Context) error {
			select {
			case <-time.After(500 * time.Millisecond):
				return nil
			case <-ctx.Done():
				timeoutObserved.Store(true)
				return ctx.Err()
			}
		},
		WithDistributedLock(false),
		WithTimeout(40*time.Millisecond),
	)
	require.NoError(t, err)

	runErr := s.RunOnce(context.Background(), "slow-report-job")
	require.ErrorIs(t, runErr, context.DeadlineExceeded)
	assert.True(t, timeoutObserved.Load(), "任务上下文应当在超过 WithTimeout 时自动取消")
}

func TestScheduler_StartAndStop_PeriodicExecution(t *testing.T) {
	var ticks atomic.Int32
	s1 := NewScheduler()
	s2 := NewScheduler()

	taskFn := func(ctx context.Context) error {
		ticks.Add(1)
		return nil
	}

	require.NoError(t, s1.RegisterInterval("periodic-heartbeat", 35*time.Millisecond, taskFn, WithDistributedLock(true, time.Second)))
	require.NoError(t, s2.RegisterInterval("periodic-heartbeat", 35*time.Millisecond, taskFn, WithDistributedLock(true, time.Second)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s1.Start(ctx)
	s2.Start(ctx)

	time.Sleep(90 * time.Millisecond)
	s1.Stop()
	s2.Stop()

	// 因为锁 TTL 为 1s，在 90ms 窗口内两个实例即便经过多次 Ticker，也只会成功执行 1 次
	assert.Equal(t, int32(1), ticks.Load())
}

func TestParseCron_StandardAndExtendedSpecs(t *testing.T) {
	base := time.Date(2026, 10, 27, 14, 30, 15, 0, time.UTC)

	// 1. 5 段标准表达式：每天凌晨 02:30
	schedDaily, err := ParseCron("30 2 * * *")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 28, 2, 30, 0, 0, time.UTC), schedDaily.Next(base))

	// 2. 6 段含秒表达式：每 15 秒触发一次 ("*/15 * * * * *")
	schedSec, err := ParseCron("*/15 * * * * *")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 27, 14, 30, 30, 0, time.UTC), schedSec.Next(base))

	// 3. 宏描述符：@hourly 与 @every 40ms
	schedHourly, err := ParseCron("@hourly")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 27, 15, 0, 0, 0, time.UTC), schedHourly.Next(base))

	schedEvery, err := ParseCron("@every 40ms")
	require.NoError(t, err)
	assert.Equal(t, base.Add(40*time.Millisecond), schedEvery.Next(base))

	// 4. 非法表达式应报错
	_, err = ParseCron("invalid cron")
	require.ErrorIs(t, err, ErrInvalidCronSpec)
	_, err = ParseCron("99 * * * *")
	require.ErrorIs(t, err, ErrInvalidCronSpec)
}

func TestScheduler_RegisterCron_Execution(t *testing.T) {
	var count atomic.Int32
	s := NewScheduler()

	err := s.RegisterCron("cron-fast-task", "@every 30ms", func(ctx context.Context) error {
		count.Add(1)
		return nil
	}, WithDistributedLock(false))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	require.Eventually(t, func() bool {
		return count.Load() >= 2
	}, time.Second, 10*time.Millisecond)
	s.Stop()
}

func TestParseCron_AllMacros_And_ImpossibleSchedule(t *testing.T) {
	base := time.Date(2026, 5, 15, 10, 20, 30, 0, time.UTC)

	macros := []string{"@yearly", "@annually", "@monthly", "@weekly", "@daily", "@midnight", "@hourly", "@every 100ms"}
	for _, m := range macros {
		sched, err := ParseCron(m)
		require.NoError(t, err, "macro %s should parse", m)
		next := sched.Next(base)
		assert.True(t, next.After(base), "macro %s next time should be after base", m)
	}

	// 周日 7 映射到 0
	schedSun, err := ParseCron("0 0 * * 7")
	require.NoError(t, err)
	nextSun := schedSun.Next(base)
	assert.Equal(t, time.Sunday, nextSun.Weekday())

	// 永远不可能命中的日期（2月31日）应在 5 年上限后安全返回零值，绝不死循环
	schedImpossible, err := ParseCron("0 0 31 2 *")
	require.NoError(t, err)
	assert.True(t, schedImpossible.Next(base).IsZero(), "impossible date 2/31 must return zero time without infinite loop")
}

func FuzzParseCron(f *testing.F) {
	f.Add("*/15 0-23 1,15 1-12 0-6")
	f.Add("0 30 2 31 2 *")
	f.Add("@every 5s")
	f.Add("@daily")
	f.Add("* * * * * *")
	f.Add("invalid/0 99 -1 * *")

	base := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, spec string) {
		assert.NotPanics(t, func() {
			sched, err := ParseCron(spec)
			if err == nil && sched != nil {
				_ = sched.Next(base)
			}
		})
	})
}
