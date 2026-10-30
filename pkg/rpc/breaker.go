// Package rpc 提供统一的 gRPC 客户端与服务端封装，包含上下文 Metadata 传递、错误拦截、熔断重试与连接池管理。
package rpc

import (
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// BreakerState 表示熔断器的运行状态。
type BreakerState int32

const (
	// StateClosed 闭合状态：正常放行所有请求，统计连续系统故障次数。
	StateClosed BreakerState = iota
	// StateOpen 开启状态：下游故障达到阈值，直接快速失败拒绝请求；冷却期满后进入半开状态。
	StateOpen
	// StateHalfOpen 半开状态：放行有限探测流量，若探测成功达到阈值则恢复闭合，若失败则立即重新开启。
	StateHalfOpen
)

// String 返回 BreakerState 的可读字符串名称。
func (s BreakerState) String() string {
	switch s {
	case StateClosed:
		return "Closed"
	case StateOpen:
		return "Open"
	case StateHalfOpen:
		return "HalfOpen"
	default:
		return "Unknown"
	}
}

// BreakerConfig 熔断器配置参数结构体。
type BreakerConfig struct {
	// FailureThreshold 连续系统故障触发熔断的阈值，默认 5。
	FailureThreshold int `json:"failureThreshold,optional"`
	// OpenTimeout 熔断开启后的冷却等待时间，默认 10s。
	OpenTimeout time.Duration `json:"openTimeout,optional"`
	// HalfOpenMaxCalls 半开状态下允许探测并连续成功恢复闭合的请求数，默认 1。
	HalfOpenMaxCalls int `json:"halfOpenMaxCalls,optional"`
}

// applyDefaults 为 BreakerConfig 补全默认安全阈值。
func (c *BreakerConfig) applyDefaults() {
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 5
	}
	if c.OpenTimeout <= 0 {
		c.OpenTimeout = 10 * time.Second
	}
	if c.HalfOpenMaxCalls <= 0 {
		c.HalfOpenMaxCalls = 1
	}
}

// CircuitBreaker 并发安全的轻量级三态熔断器，用于保护上游免受下游雪崩影响。
type CircuitBreaker struct {
	mu                  sync.Mutex
	cfg                 BreakerConfig
	state               BreakerState
	consecutiveFailures int
	halfOpenSuccesses   int
	halfOpenInFlight    int
	openedAt            time.Time
}

// NewCircuitBreaker 根据配置创建并初始化 CircuitBreaker 实例。
func NewCircuitBreaker(cfg BreakerConfig) *CircuitBreaker {
	cfg.applyDefaults()
	return &CircuitBreaker{
		cfg:   cfg,
		state: StateClosed,
	}
}

// State 获取熔断器当前的实时状态（若处于 StateOpen 且已超过冷却期，自动演进至 StateHalfOpen）。
func (cb *CircuitBreaker) State() BreakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateOpen && time.Since(cb.openedAt) >= cb.cfg.OpenTimeout {
		cb.state = StateHalfOpen
		cb.halfOpenSuccesses = 0
		cb.halfOpenInFlight = 0
	}
	return cb.state
}

// Allow 判断当前请求是否允许放行；若熔断器开启且未过冷却期，返回 codes.Unavailable 错误。
func (cb *CircuitBreaker) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return nil
	case StateOpen:
		if time.Since(cb.openedAt) >= cb.cfg.OpenTimeout {
			cb.state = StateHalfOpen
			cb.halfOpenSuccesses = 0
			cb.halfOpenInFlight = 1
			return nil
		}
		return status.Error(codes.Unavailable, "circuit breaker is open")
	case StateHalfOpen:
		if cb.halfOpenInFlight >= cb.cfg.HalfOpenMaxCalls {
			return status.Error(codes.Unavailable, "circuit breaker is open")
		}
		cb.halfOpenInFlight++
		return nil
	default:
		return nil
	}
}

// RecordResult 根据本次 gRPC 调用返回的 error 更新熔断器状态机。
// 仅当 IsSystemFailure(err) 为 true 时计入系统故障；业务异常（如 NotFound、InvalidArgument 等）视为下游存活，按成功处理。
func (cb *CircuitBreaker) RecordResult(err error) {
	if IsSystemFailure(err) {
		cb.RecordFailure()
		return
	}
	cb.RecordSuccess()
}

// RecordSuccess 记录一次调用成功并推进状态机恢复。
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.consecutiveFailures = 0
	case StateHalfOpen:
		if cb.halfOpenInFlight > 0 {
			cb.halfOpenInFlight--
		}
		cb.halfOpenSuccesses++
		if cb.halfOpenSuccesses >= cb.cfg.HalfOpenMaxCalls {
			cb.state = StateClosed
			cb.consecutiveFailures = 0
			cb.halfOpenSuccesses = 0
			cb.halfOpenInFlight = 0
		}
	case StateOpen:
		// 若在 Open 状态下收到滞后成功响应，保持重置连续失败计数并回到 Closed
		cb.state = StateClosed
		cb.consecutiveFailures = 0
		cb.halfOpenSuccesses = 0
		cb.halfOpenInFlight = 0
	}
}

// RecordFailure 记录一次下游系统级故障并按阈值触发熔断。
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.consecutiveFailures++
		if cb.consecutiveFailures >= cb.cfg.FailureThreshold {
			cb.state = StateOpen
			cb.openedAt = time.Now()
		}
	case StateHalfOpen:
		if cb.halfOpenInFlight > 0 {
			cb.halfOpenInFlight--
		}
		cb.state = StateOpen
		cb.openedAt = time.Now()
		cb.halfOpenSuccesses = 0
	case StateOpen:
		cb.openedAt = time.Now()
	}
}

// IsSystemFailure 判断 gRPC 错误是否属于计入熔断器的下游系统级故障。
// 仅 codes.Unavailable、codes.DeadlineExceeded、codes.Internal、codes.ResourceExhausted 视为系统故障；
// 其余业务状态码（如 InvalidArgument、NotFound、PermissionDenied、Unauthenticated、AlreadyExists 等）返回 false。
func IsSystemFailure(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	switch code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.ResourceExhausted:
		return true
	default:
		return false
	}
}
