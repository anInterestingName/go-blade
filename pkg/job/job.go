// Package job 提供基于时间间隔调度的分布式定时任务底座组件。
// 默认结合 pkg/lock 分布式互斥锁（键前缀 `blade:job:lock:`）防止多副本集群在同一周期内并发重复执行，
// 并内置任务级超时控制与 Panic 自动恢复保护。
package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-blade/pkg/lock"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// JobLockKeyPrefix 分布式定时任务互斥锁在 Redis/内存锁中的统一键前缀。
	JobLockKeyPrefix = "blade:job:lock:"
	// DefaultJobLockTTL 分布式定时任务未显式指定锁过期时间时的默认锁有效期（10秒）。
	DefaultJobLockTTL = 10 * time.Second
)

var (
	// ErrEmptyTaskName 表示注册或触发的定时任务名称不能为空。
	ErrEmptyTaskName = errors.New("job task name must not be empty")
	// ErrInvalidInterval 表示周期定时任务的时间间隔必须大于 0。
	ErrInvalidInterval = errors.New("job interval must be greater than zero")
	// ErrNilTaskFunc 表示定时任务的执行函数不能为空。
	ErrNilTaskFunc = errors.New("job task function must not be nil")
	// ErrDuplicateTask 表示同名定时任务已存在。
	ErrDuplicateTask = errors.New("job task already registered")
	// ErrTaskNotFound 表示指定的定时任务尚未注册。
	ErrTaskNotFound = errors.New("job task not found")
)

// TaskFunc 定义定时任务的业务执行函数签名。
type TaskFunc func(ctx context.Context) error

// TaskConfig 保存单个定时任务的运行期策略配置。
type TaskConfig struct {
	// DistributedLock 是否开启多副本分布式互斥锁（默认 true）
	DistributedLock bool
	// LockTTL 分布式锁持有时间（防止多实例在同一时间窗口重复触发）
	LockTTL time.Duration
	// Timeout 单次任务执行的最大超时时间（0 表示不限制）
	Timeout time.Duration
	// UnlockOnFinish 任务执行结束后是否立即主动释放锁（默认 false，保持锁至 LockTTL 结束以防时钟微偏导致其他副本在同一窗口二次执行）
	UnlockOnFinish bool
}

// TaskOption 定义定时任务配置的函数式选项。
type TaskOption func(*TaskConfig)

// WithDistributedLock 配置定时任务是否开启分布式互斥锁以及锁的过期时长。
// 若传入了可选的 lockTTL 且大于 0，则设置对应锁过期时间；否则沿用默认值。
func WithDistributedLock(enable bool, lockTTL ...time.Duration) TaskOption {
	return func(c *TaskConfig) {
		c.DistributedLock = enable
		if len(lockTTL) > 0 && lockTTL[0] > 0 {
			c.LockTTL = lockTTL[0]
		}
	}
}

// WithTimeout 配置单次定时任务执行的最大超时时间。
func WithTimeout(timeout time.Duration) TaskOption {
	return func(c *TaskConfig) {
		if timeout > 0 {
			c.Timeout = timeout
		}
	}
}

// WithUnlockOnFinish 配置任务执行完成后是否立即释放分布式锁。
func WithUnlockOnFinish(unlock bool) TaskOption {
	return func(c *TaskConfig) {
		c.UnlockOnFinish = unlock
	}
}

type taskEntry struct {
	name     string
	interval time.Duration
	schedule CronSchedule
	fn       TaskFunc
	cfg      TaskConfig
}

// Scheduler 分布式周期定时任务调度器。
type Scheduler struct {
	mu      sync.RWMutex
	tasks   map[string]*taskEntry
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewScheduler 创建并初始化一个分布式定时任务调度器实例。
func NewScheduler() *Scheduler {
	return &Scheduler{
		tasks: make(map[string]*taskEntry),
	}
}

// RegisterInterval 注册一个按固定时间间隔触发的周期性定时任务。
// 默认开启分布式互斥锁（锁键为 `blade:job:lock:<name>`）。
func (s *Scheduler) RegisterInterval(name string, interval time.Duration, fn TaskFunc, opts ...TaskOption) error {
	cleanName := strings.TrimSpace(name)
	if cleanName == "" {
		return ErrEmptyTaskName
	}
	if interval <= 0 {
		return ErrInvalidInterval
	}
	if fn == nil {
		return ErrNilTaskFunc
	}

	defaultLockTTL := interval
	if defaultLockTTL <= 0 {
		defaultLockTTL = DefaultJobLockTTL
	}

	cfg := TaskConfig{
		DistributedLock: true,
		LockTTL:         defaultLockTTL,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = DefaultJobLockTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[cleanName]; exists {
		return ErrDuplicateTask
	}

	entry := &taskEntry{
		name:     cleanName,
		interval: interval,
		fn:       fn,
		cfg:      cfg,
	}
	s.tasks[cleanName] = entry

	if s.running {
		s.wg.Add(1)
		go s.runTickerLoop(s.stopCh, entry)
	}

	return nil
}

// RegisterCron 注册一个基于标准 Cron 表达式（支持 5 段/6 段及 @daily/@every 等描述符）触发的分布式定时任务。
// 默认开启分布式互斥锁（锁键为 `blade:job:lock:<name>`）。
func (s *Scheduler) RegisterCron(name string, spec string, fn TaskFunc, opts ...TaskOption) error {
	cleanName := strings.TrimSpace(name)
	if cleanName == "" {
		return ErrEmptyTaskName
	}
	if fn == nil {
		return ErrNilTaskFunc
	}
	schedule, err := ParseCron(spec)
	if err != nil {
		return err
	}

	cfg := TaskConfig{
		DistributedLock: true,
		LockTTL:         DefaultJobLockTTL,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.LockTTL <= 0 {
		cfg.LockTTL = DefaultJobLockTTL
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[cleanName]; exists {
		return ErrDuplicateTask
	}

	entry := &taskEntry{
		name:     cleanName,
		schedule: schedule,
		fn:       fn,
		cfg:      cfg,
	}
	s.tasks[cleanName] = entry

	if s.running {
		s.wg.Add(1)
		go s.runTickerLoop(s.stopCh, entry)
	}

	return nil
}

// RunOnce 立即在当前上下文触发一次指定名称的定时任务。
// 若该任务开启了分布式锁且锁已被集群中其他副本持有，则静默跳过本次执行并返回 nil。
func (s *Scheduler) RunOnce(ctx context.Context, name string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	cleanName := strings.TrimSpace(name)
	if cleanName == "" {
		return ErrEmptyTaskName
	}

	s.mu.RLock()
	entry, exists := s.tasks[cleanName]
	s.mu.RUnlock()

	if !exists {
		return ErrTaskNotFound
	}

	return s.executeTask(ctx, entry)
}

// Start 启动调度器，为所有已注册的周期任务开启后台 Ticker 调度协程。
// 当传入的 ctx 被取消或调用 Stop() 时，所有后台调度协程将安全退出。
func (s *Scheduler) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.stopCh = make(chan struct{})
	stopCh := s.stopCh

	entries := make([]*taskEntry, 0, len(s.tasks))
	for _, e := range s.tasks {
		entries = append(entries, e)
	}

	for _, entry := range entries {
		s.wg.Add(1)
		go s.runTickerLoop(stopCh, entry)
	}
	s.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
			s.Stop()
		case <-stopCh:
		}
	}()
}

// Stop 停止定时任务调度器并等待所有正在执行的定时任务协程退出。
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	close(s.stopCh)
	s.mu.Unlock()

	s.wg.Wait()
}

func (s *Scheduler) runTickerLoop(stopCh <-chan struct{}, entry *taskEntry) {
	defer s.wg.Done()

	if entry.schedule != nil {
		s.runCronLoop(stopCh, entry)
		return
	}

	ticker := time.NewTicker(entry.interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			_ = s.executeTask(context.Background(), entry)
		}
	}
}

func (s *Scheduler) runCronLoop(stopCh <-chan struct{}, entry *taskEntry) {
	for {
		now := time.Now()
		next := entry.schedule.Next(now)
		if next.IsZero() {
			return
		}
		wait := time.Until(next)
		if wait <= 0 {
			wait = time.Millisecond
		}

		timer := time.NewTimer(wait)
		select {
		case <-stopCh:
			timer.Stop()
			return
		case <-timer.C:
			_ = s.executeTask(context.Background(), entry)
		}
	}
}

func (s *Scheduler) executeTask(ctx context.Context, entry *taskEntry) (err error) {
	execCtx := ctx
	var cancel context.CancelFunc
	if entry.cfg.Timeout > 0 {
		execCtx, cancel = context.WithTimeout(ctx, entry.cfg.Timeout)
		defer cancel()
	}

	if entry.cfg.DistributedLock {
		lockKey := JobLockKeyPrefix + entry.name
		acquiredLock, lockErr := lock.Acquire(execCtx, lockKey, entry.cfg.LockTTL)
		if lockErr != nil {
			if errors.Is(lockErr, lock.ErrLockNotAcquired) {
				// 未抢到锁的副本静默跳过本次执行
				return nil
			}
			return lockErr
		}
		if entry.cfg.UnlockOnFinish && acquiredLock != nil {
			defer func() {
				_ = acquiredLock.Unlock(context.Background())
			}()
		}
	}

	defer func() {
		if r := recover(); r != nil {
			logx.WithContext(execCtx).Errorf("[job] panic recovered in task %s: %v", entry.name, r)
			err = fmt.Errorf("job task %s panicked: %v", entry.name, r)
		}
	}()

	if runErr := entry.fn(execCtx); runErr != nil {
		logx.WithContext(execCtx).Errorf("[job] task %s failed: %v", entry.name, runErr)
		return runErr
	}
	return nil
}
