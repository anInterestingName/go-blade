// Package lifecycle 提供云原生微服务统一生命周期管理与多阶段 K8s 优雅停机编排器。
// 支持按标准下线顺序（1.流量摘除与健康探针下线 -> 2.停止 HTTP/gRPC 接收请求 -> 3.停止后台调度与 Outbox -> 4.排空 MQ/Event 队列 -> 5.刷盘 Tracer 与关闭底层连接）
// 依次执行停机钩子，并内置单钩子 Panic 捕获隔离与总停机超时控制。
package lifecycle

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
)

// Phase 定义优雅停机流水线的执行阶段优先级（数值越小越先执行）。
type Phase int

const (
	// PhaseTrafficDrain 阶段 1：流量摘除（将健康探针置为 NOT_SERVING、从 Nacos 注销服务实例）。
	PhaseTrafficDrain Phase = 10
	// PhaseServerStop 阶段 2：停止传输层服务（停止接收新的 HTTP / gRPC 请求并等待在途请求完成）。
	PhaseServerStop Phase = 20
	// PhaseBackgroundWorker 阶段 3：停止后台周期任务（停止 pkg/job 定时调度器与 OutboxRelay 转发器）。
	PhaseBackgroundWorker Phase = 30
	// PhaseMessageQueue 阶段 4：排空异步消息与事件队列（关闭 pkg/mq.Broker 与 pkg/event.Bus）。
	PhaseMessageQueue Phase = 40
	// PhaseObservability 阶段 5：可观测性与基础设施收尾（刷盘 OpenTelemetry Tracer、关闭下游连接池）。
	PhaseObservability Phase = 50

	// DefaultShutdownTimeout 默认优雅停机总超时时间（15秒）。
	DefaultShutdownTimeout = 15 * time.Second
)

// HookFunc 定义优雅停机阶段执行的清理函数签名。
type HookFunc func(ctx context.Context) error

// Hook 表示注册到生命周期管理器的单个优雅停机钩子。
type Hook struct {
	// Name 钩子名称（用于日志追踪与排障）
	Name string
	// Phase 所属停机阶段优先级（默认 PhaseServerStop）
	Phase Phase
	// Stop 停机时执行的清理回调
	Stop HookFunc
	seq  int
}

// Manager 维护微服务全生命周期停机钩子并编排按序优雅退出。
type Manager struct {
	mu              sync.Mutex
	hooks           []Hook
	seqCounter      int
	shutdownTimeout time.Duration
	shutdownOnce    sync.Once
	lastErr         error
}

// NewManager 创建并初始化一个生命周期编排器实例（可选指定优雅停机总超时时间）。
func NewManager(shutdownTimeout ...time.Duration) *Manager {
	timeout := DefaultShutdownTimeout
	if len(shutdownTimeout) > 0 && shutdownTimeout[0] > 0 {
		timeout = shutdownTimeout[0]
	}
	return &Manager{
		shutdownTimeout: timeout,
	}
}

// Register 注册一个指定阶段的优雅停机钩子。
// 同一 Phase 阶段内的多个钩子按“后进先出 (LIFO)”逆序执行；不同 Phase 按阶段数值从小到大顺序执行。
func (m *Manager) Register(name string, phase Phase, stopFn HookFunc) {
	if m == nil || stopFn == nil {
		return
	}
	cleanName := strings.TrimSpace(name)
	if cleanName == "" {
		cleanName = "unnamed-hook"
	}
	if phase <= 0 {
		phase = PhaseServerStop
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.seqCounter++
	m.hooks = append(m.hooks, Hook{
		Name:  cleanName,
		Phase: phase,
		Stop:  stopFn,
		seq:   m.seqCounter,
	})
}

// OnShutdown 是 Register 的快捷方法，默认注册在 PhaseServerStop 阶段。
func (m *Manager) OnShutdown(name string, stopFn HookFunc) {
	m.Register(name, PhaseServerStop, stopFn)
}

// Shutdown 按阶段优先级（Phase 小的先执行，同 Phase 内后注册的先执行）安全执行所有已注册的停机钩子。
// 幂等安全：多次调用仅执行一次停机流水线。
func (m *Manager) Shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), m.shutdownTimeout)
		defer cancel()
	} else if _, hasDeadline := ctx.Deadline(); !hasDeadline && m.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, m.shutdownTimeout)
		defer cancel()
	}

	m.shutdownOnce.Do(func() {
		m.mu.Lock()
		ordered := make([]Hook, len(m.hooks))
		copy(ordered, m.hooks)
		m.mu.Unlock()

		sort.SliceStable(ordered, func(i, j int) bool {
			if ordered[i].Phase != ordered[j].Phase {
				return ordered[i].Phase < ordered[j].Phase
			}
			// 同一阶段内采用 LIFO 逆序释放
			return ordered[i].seq > ordered[j].seq
		})

		var firstErr error
		for _, h := range ordered {
			if err := ctx.Err(); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				break
			}
			if err := runHookSafely(ctx, h); err != nil {
				logx.Errorf("[lifecycle] shutdown hook %q (phase=%d) failed: %v", h.Name, h.Phase, err)
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		m.lastErr = firstErr
	})

	return m.lastErr
}

// WaitForSignal 阻塞等待操作系统退出信号（SIGINT / SIGTERM）或传入的 ctx 取消，随后自动触发 Shutdown。
func (m *Manager) WaitForSignal(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case <-ctx.Done():
	case <-sigCh:
	}
	return m.Shutdown(context.Background())
}

func runHookSafely(ctx context.Context, h Hook) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("shutdown hook %s panicked: %v", h.Name, r)
		}
	}()
	return h.Stop(ctx)
}
