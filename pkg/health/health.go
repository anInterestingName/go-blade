// Package health 提供标准的微服务存活探针（Liveness）与就绪探针（Readiness）管理底座，
// 同时支持 HTTP（/healthz/live、/healthz/ready）与标准 gRPC（grpc.health.v1.Health）协议。
package health

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/go-blade/pkg/response"
	"github.com/go-blade/pkg/trace"
	"github.com/zeromicro/go-zero/rest/httpx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	// StatusUp 表示服务或依赖组件处于健康就绪状态。
	StatusUp = "UP"
	// StatusDown 表示服务或依赖组件处于故障不可用状态。
	StatusDown = "DOWN"
)

// CheckFunc 定义单个依赖组件的健康探测函数签名。
type CheckFunc func(ctx context.Context) error

// NamedChecker 定义具备名称标识的可插拔健康检查器接口。
type NamedChecker interface {
	// Name 返回当前检查项的组件名称（如 "mysql", "redis"）。
	Name() string
	// Check 执行健康探测，返回 nil 表示健康，返回非 nil error 表示故障。
	Check(ctx context.Context) error
}

type pingChecker struct {
	name string
	ping CheckFunc
}

func (p *pingChecker) Name() string {
	return p.name
}

func (p *pingChecker) Check(ctx context.Context) error {
	if p.ping == nil {
		return nil
	}
	return p.ping(ctx)
}

// PingChecker 将自定义的探测函数包装为 NamedChecker 实例（如数据库或 Redis Ping 检查）。
func PingChecker(name string, ping func(ctx context.Context) error) NamedChecker {
	if strings.TrimSpace(name) == "" {
		name = "unnamed"
	}
	return &pingChecker{
		name: name,
		ping: ping,
	}
}

// StatusReport 表示整体健康探测的状态报告。
type StatusReport struct {
	// Status 总体健康状态（"UP" 或 "DOWN"）。
	Status string `json:"status"`
	// Components 各已注册依赖组件的子状态明细（键为组件名，值为 "UP" 或具体失败原因）。
	Components map[string]string `json:"components,omitempty"`
}

// Checker 管理多个命名依赖的健康检查项，并提供 HTTP 与 gRPC 探针适配能力。
type Checker struct {
	mu     sync.RWMutex
	order  []string
	checks map[string]CheckFunc
}

// Manager 是 Checker 的类型别名，便于语义化管理多组件健康探针。
type Manager = Checker

var (
	defaultOnce    sync.Once
	defaultChecker *Checker
)

// NewChecker 创建并初始化一个新的健康检查管理器实例。
func NewChecker(checkers ...NamedChecker) *Checker {
	c := &Checker{
		order:  make([]string, 0, len(checkers)),
		checks: make(map[string]CheckFunc, len(checkers)),
	}
	for _, item := range checkers {
		c.AddChecker(item)
	}
	return c
}

// NewManager 创建并初始化一个新的健康检查管理器实例（NewChecker 的语义化别名）。
func NewManager(checkers ...NamedChecker) *Manager {
	return NewChecker(checkers...)
}

// DefaultChecker 获取全局默认的健康检查管理器单例。
func DefaultChecker() *Checker {
	defaultOnce.Do(func() {
		defaultChecker = NewChecker()
	})
	return defaultChecker
}

// Register 注册一个命名的健康探测函数。
func (c *Checker) Register(name string, check CheckFunc) {
	if c == nil || check == nil {
		return
	}
	name = strings.TrimSpace(name)
	if name == "" {
		name = "unnamed"
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.checks == nil {
		c.checks = make(map[string]CheckFunc)
	}
	if _, exists := c.checks[name]; !exists {
		c.order = append(c.order, name)
	}
	c.checks[name] = check
}

// AddChecker 注册一个实现了 NamedChecker 接口的组件检查器。
func (c *Checker) AddChecker(checker NamedChecker) {
	if c == nil || checker == nil {
		return
	}
	c.Register(checker.Name(), checker.Check)
}

// Check 执行所有已注册的依赖检查项，返回整体 StatusReport 及聚合错误。
// 若全部检查通过，返回 Status="UP" 且 err=nil；若任一检查失败，返回 Status="DOWN" 及首个/聚合错误。
func (c *Checker) Check(ctx context.Context) (StatusReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	report := StatusReport{
		Status:     StatusUp,
		Components: make(map[string]string),
	}
	if c == nil {
		return report, nil
	}

	c.mu.RLock()
	names := make([]string, len(c.order))
	copy(names, c.order)
	checks := make(map[string]CheckFunc, len(c.checks))
	for k, v := range c.checks {
		checks[k] = v
	}
	c.mu.RUnlock()

	var errs []error
	for _, name := range names {
		fn := checks[name]
		if fn == nil {
			continue
		}
		if err := fn(ctx); err != nil {
			report.Status = StatusDown
			report.Components[name] = "DOWN: " + err.Error()
			errs = append(errs, errors.New(name+": "+err.Error()))
		} else {
			report.Components[name] = StatusUp
		}
	}

	if len(errs) > 0 {
		return report, errors.Join(errs...)
	}
	return report, nil
}

// LiveHandler 返回当前 Checker 的存活探针 HTTP 处理器（始终返回 HTTP 200 与 status: "UP"）。
func (c *Checker) LiveHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response.ResultResponse(r.Context(), w, StatusReport{
			Status: StatusUp,
		}, nil)
	}
}

// ReadyHandler 返回当前 Checker 的就绪探针 HTTP 处理器：
// 当所有已注册 CheckFunc 成功时返回 HTTP 200 及 status: "UP"；
// 当任一检查失败时返回 HTTP 503 ServiceUnavailable 及失败详情。
func (c *Checker) ReadyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if traceID := trace.TraceIDFromContext(ctx); traceID != "" {
			w.Header().Set(trace.HeaderTraceID, traceID)
		}

		report, err := c.Check(ctx)
		if err == nil {
			response.ResultResponse(ctx, w, report, nil)
			return
		}

		httpx.WriteJsonCtx(ctx, w, http.StatusServiceUnavailable, &response.Response[StatusReport]{
			Code:    http.StatusServiceUnavailable,
			Success: false,
			Msg:     err.Error(),
			Data:    report,
		})
	}
}

// LiveHandler 返回全局默认的存活探针 HTTP 处理器（/healthz/live）。
func LiveHandler() http.HandlerFunc {
	return DefaultChecker().LiveHandler()
}

// ReadyHandler 返回全局默认的就绪探针 HTTP 处理器（/healthz/ready）。
func ReadyHandler(checkers ...NamedChecker) http.HandlerFunc {
	if len(checkers) > 0 {
		return NewChecker(checkers...).ReadyHandler()
	}
	return DefaultChecker().ReadyHandler()
}

// GRPCHealthServer 实现标准 google.golang.org/grpc/health/grpc_health_v1.HealthServer 协议。
type GRPCHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	checker *Checker
}

// NewGRPCHealthServer 基于指定的 Checker 创建标准 gRPC 健康检查服务端实例。
func NewGRPCHealthServer(checker *Checker) *GRPCHealthServer {
	if checker == nil {
		checker = DefaultChecker()
	}
	return &GRPCHealthServer{
		checker: checker,
	}
}

// Check 执行一元 gRPC 健康状态探测，当 checker 检查通过时返回 SERVING，否则返回 NOT_SERVING。
func (s *GRPCHealthServer) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	if s == nil || s.checker == nil {
		return &grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_SERVING,
		}, nil
	}
	if _, err := s.checker.Check(ctx); err != nil {
		return &grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
		}, nil
	}
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	}, nil
}

// Watch 实现流式 gRPC 健康状态监听，发送当前健康状态后等待上下文结束。
func (s *GRPCHealthServer) Watch(req *grpc_health_v1.HealthCheckRequest, stream grpc_health_v1.Health_WatchServer) error {
	if stream == nil {
		return status.Error(codes.InvalidArgument, "nil watch stream")
	}
	resp, err := s.Check(stream.Context(), req)
	if err != nil {
		return err
	}
	if err := stream.Send(resp); err != nil {
		return err
	}
	<-stream.Context().Done()
	return status.FromContextError(stream.Context().Err()).Err()
}

// RegisterGRPCHealthServer 将标准 grpc.health.v1.Health 服务注册到指定的 grpc.Server 实例。
func RegisterGRPCHealthServer(s *grpc.Server, checker *Checker) {
	if s == nil {
		return
	}
	grpc_health_v1.RegisterHealthServer(s, NewGRPCHealthServer(checker))
}
