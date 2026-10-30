// Package httpclient 提供集成 OpenTelemetry W3C 链路追踪透传、多租户标识注入、Prometheus 指标采集、
// 自适应三态熔断器与指数退避重试的统一出站 HTTP 客户端治理底座。
package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-blade/pkg/metrics"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var (
	// ErrCircuitOpen 表示出站 HTTP 客户端熔断器处于 Open 打开状态，拒绝放行请求。
	ErrCircuitOpen = errors.New("outbound http circuit breaker is open")
)

// BreakerState 定义出站 HTTP 熔断器的三态枚举。
type BreakerState int

const (
	// StateClosed 闭合正常态
	StateClosed BreakerState = iota
	// StateOpen 熔断打开态（快速失败）
	StateOpen
	// StateHalfOpen 半开探测态
	StateHalfOpen
)

// Config 定义出站 HTTP 客户端的超时、重试与熔断治理配置。
type Config struct {
	// ServiceName 目标外部服务名称（用于 Metrics 与 Trace 标识，默认 "outbound-http"）
	ServiceName string `json:"serviceName,optional"`
	// Timeout 单次 HTTP 请求超时时间（默认 10s）
	Timeout time.Duration `json:"timeout,optional"`
	// MaxRetries 瞬态故障或 5xx 响应时的最大重试次数（默认 2 次，0 表示不重试）
	MaxRetries int `json:"maxRetries,optional"`
	// RetryBackoff 首次重试的基础退避时间（默认 50ms，按指数退避增长）
	RetryBackoff time.Duration `json:"retryBackoff,optional"`
	// DisableBreaker 是否禁用熔断器（默认 false，即启用熔断器）
	DisableBreaker bool `json:"disableBreaker,optional"`
	// FailureThreshold 连续系统级故障触发熔断的阈值（默认 5 次）
	FailureThreshold int `json:"failureThreshold,optional"`
	// Cooldown 熔断器从 Open 进入 HalfOpen 的冷却时间（默认 10s）
	Cooldown time.Duration `json:"cooldown,optional"`
}

func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.ServiceName) == "" {
		c.ServiceName = "outbound-http"
	}
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Second
	}
	if c.MaxRetries < 0 {
		c.MaxRetries = 0
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 50 * time.Millisecond
	}
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 5
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 10 * time.Second
	}
}

// httpBreaker 并发安全的出站 HTTP 三态熔断器（仅对网络错误与 5xx 状态码计数，豁免 4xx 业务状态码）。
type httpBreaker struct {
	mu               sync.Mutex
	state            BreakerState
	failures         int
	failureThreshold int
	cooldown         time.Duration
	openedAt         time.Time
	halfOpenInFlight bool
}

func newHTTPBreaker(threshold int, cooldown time.Duration) *httpBreaker {
	if threshold <= 0 {
		threshold = 5
	}
	if cooldown <= 0 {
		cooldown = 10 * time.Second
	}
	return &httpBreaker{
		state:            StateClosed,
		failureThreshold: threshold,
		cooldown:         cooldown,
	}
}

func (b *httpBreaker) Allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return nil
	case StateOpen:
		if time.Since(b.openedAt) >= b.cooldown {
			b.state = StateHalfOpen
			b.halfOpenInFlight = true
			return nil
		}
		return ErrCircuitOpen
	case StateHalfOpen:
		if b.halfOpenInFlight {
			return ErrCircuitOpen
		}
		b.halfOpenInFlight = true
		return nil
	default:
		return nil
	}
}

func (b *httpBreaker) Record(statusCode int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 仅网络故障、超时或 HTTP >= 500 视为系统故障；2xx/3xx/4xx 视为下游服务存活
	isSystemFailure := err != nil || statusCode >= http.StatusInternalServerError
	if !isSystemFailure {
		b.failures = 0
		b.state = StateClosed
		b.halfOpenInFlight = false
		return
	}

	if b.state == StateHalfOpen {
		b.state = StateOpen
		b.openedAt = time.Now()
		b.halfOpenInFlight = false
		return
	}

	b.failures++
	if b.failures >= b.failureThreshold {
		b.state = StateOpen
		b.openedAt = time.Now()
	}
}

// State 返回当前熔断器状态（若处于 StateOpen 且冷却时间已满，则自动演进为 StateHalfOpen）。
func (c *Client) State() BreakerState {
	if c == nil || c.breaker == nil {
		return StateClosed
	}
	c.breaker.mu.Lock()
	defer c.breaker.mu.Unlock()
	if c.breaker.state == StateOpen && time.Since(c.breaker.openedAt) >= c.breaker.cooldown {
		c.breaker.state = StateHalfOpen
		c.breaker.halfOpenInFlight = false
	}
	return c.breaker.state
}

// Client 封装支持全链路追踪、Prometheus 指标、重试与熔断治理的出站 HTTP 客户端。
type Client struct {
	cfg        Config
	httpClient *http.Client
	breaker    *httpBreaker
}

// NewClient 根据 Config（及可选自定义底层 *http.Client）创建治理型 HTTP Client。
func NewClient(cfg Config, rawClient ...*http.Client) *Client {
	cfg.applyDefaults()
	var hc *http.Client
	if len(rawClient) > 0 && rawClient[0] != nil {
		hc = rawClient[0]
	} else {
		hc = &http.Client{Timeout: cfg.Timeout}
	}

	var brk *httpBreaker
	if !cfg.DisableBreaker {
		brk = newHTTPBreaker(cfg.FailureThreshold, cfg.Cooldown)
	}

	return &Client{
		cfg:        cfg,
		httpClient: hc,
		breaker:    brk,
	}
}

// Do 执行出站 HTTP 请求，自动注入 W3C traceparent 与 Blade-Tenant-Id，支持瞬态重试与熔断保护。
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("http request cannot be nil")
	}
	if c == nil {
		c = NewClient(Config{})
	}
	if c.breaker != nil {
		if err := c.breaker.Allow(); err != nil {
			return nil, err
		}
	}

	ctx := req.Context()
	if ctx == nil {
		ctx = context.Background()
	}

	// 预先缓存请求体字节，以便在发生瞬态重试时能够重建 Body
	var bodyBytes []byte
	if req.Body != nil && req.Body != http.NoBody {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read outbound request body failed: %w", err)
		}
	}

	spanName := "http.client/" + req.Method + " " + metrics.NormalizePath(req.URL.Path)
	ctx, span := otel.Tracer("go-blade-httpclient").Start(ctx, spanName, oteltrace.WithSpanKind(oteltrace.SpanKindClient))
	defer span.End()

	maxAttempts := c.cfg.MaxRetries + 1
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastResp *http.Response
	var lastErr error
	startTime := time.Now()

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			wait := c.cfg.RetryBackoff * time.Duration(1<<(attempt-1))
			if wait > 2*time.Second {
				wait = 2 * time.Second
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				lastErr = ctx.Err()
				break
			}
		}

		clonedReq := req.Clone(ctx)
		if bodyBytes != nil {
			clonedReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			clonedReq.ContentLength = int64(len(bodyBytes))
		}

		// 自动注入 OpenTelemetry W3C TraceContext (traceparent)、X-Trace-Id 与租户头
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(clonedReq.Header))
		if traceID := trace.TraceIDFromContext(ctx); traceID != "" && clonedReq.Header.Get(trace.HeaderTraceID) == "" {
			clonedReq.Header.Set(trace.HeaderTraceID, traceID)
		}
		if tID := tenant.FromTenant(ctx); tID != "" && clonedReq.Header.Get(tenant.HeaderTenantID) == "" {
			clonedReq.Header.Set(tenant.HeaderTenantID, tID)
		}

		lastResp, lastErr = c.httpClient.Do(clonedReq)
		if lastErr == nil && !shouldRetryStatus(lastResp.StatusCode) {
			break
		}

		// 若还有后续重试机会，关闭本次失败响应的 Body 防止连接泄漏
		if attempt+1 < maxAttempts && ctx.Err() == nil {
			if lastResp != nil && lastResp.Body != nil {
				_, _ = io.Copy(io.Discard, lastResp.Body)
				_ = lastResp.Body.Close()
			}
			continue
		}
		break
	}

	duration := time.Since(startTime)
	statusCode := 0
	statusStr := "ERR"
	if lastResp != nil {
		statusCode = lastResp.StatusCode
		statusStr = strconv.Itoa(statusCode)
	}

	metrics.RecordHTTP(c.cfg.ServiceName, req.Method, metrics.NormalizePath(req.URL.Path), statusStr, duration)

	if c.breaker != nil {
		c.breaker.Record(statusCode, lastErr)
	}

	span.SetAttributes(
		attribute.String("http.method", req.Method),
		attribute.String("http.url", req.URL.String()),
		attribute.Int("http.status_code", statusCode),
	)
	if lastErr != nil {
		span.RecordError(lastErr)
		span.SetStatus(codes.Error, lastErr.Error())
		return nil, lastErr
	}
	if statusCode >= http.StatusInternalServerError {
		span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", statusCode))
	} else {
		span.SetStatus(codes.Ok, "")
	}

	return lastResp, nil
}

// DoJSON 发送 JSON 请求并将 2xx 响应体反序列化到 respOut 中。
func (c *Client) DoJSON(ctx context.Context, method, endpoint string, reqBody any, respOut any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var bodyReader io.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request json failed: %w", err)
		}
		bodyReader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bodyReader)
	if err != nil {
		return fmt.Errorf("create http request failed: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read http response body failed: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("http request returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}

	if respOut != nil && len(respBytes) > 0 {
		if err := json.Unmarshal(respBytes, respOut); err != nil {
			return fmt.Errorf("unmarshal response json failed: %w", err)
		}
	}
	return nil
}

func shouldRetryStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
