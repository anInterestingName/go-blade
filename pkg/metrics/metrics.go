// Package metrics 提供基于 Prometheus 的 HTTP 与 gRPC 核心监控指标采集、中间件打点与 /metrics 暴露能力。
package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Collector 封装统一的 HTTP、gRPC 与数据库 Prometheus 监控指标收集器。
type Collector struct {
	// Registry 当前收集器绑定的 Prometheus 注册表。
	Registry *prometheus.Registry
	// HTTPRequestsTotal HTTP 请求总数计数器（标签：service, method, path, status）。
	HTTPRequestsTotal *prometheus.CounterVec
	// HTTPRequestDurationSeconds HTTP 请求耗时直方图（标签：service, method, path）。
	HTTPRequestDurationSeconds *prometheus.HistogramVec
	// GRPCRequestsTotal gRPC 调用总数计数器（标签：service, method, code）。
	GRPCRequestsTotal *prometheus.CounterVec
	// GRPCRequestDurationSeconds gRPC 调用耗时直方图（标签：service, method）。
	GRPCRequestDurationSeconds *prometheus.HistogramVec
	// DBQueriesTotal 数据库 SQL 操作总数计数器（标签：table, operation, status）。
	DBQueriesTotal *prometheus.CounterVec
	// DBQueryDurationSeconds 数据库 SQL 操作耗时直方图（标签：table, operation）。
	DBQueryDurationSeconds *prometheus.HistogramVec
}

var (
	defaultOnce      sync.Once
	defaultCollector *Collector
)

// NewCollector 创建并注册一个独立的 Prometheus 指标收集器实例。
func NewCollector() *Collector {
	reg := prometheus.NewRegistry()

	httpReqs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blade_http_requests_total",
			Help: "Total number of HTTP requests processed by go-blade services.",
		},
		[]string{"service", "method", "path", "status"},
	)

	httpDur := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "blade_http_request_duration_seconds",
			Help:    "HTTP request latency distributions in seconds for go-blade services.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"service", "method", "path"},
	)

	grpcReqs := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blade_grpc_requests_total",
			Help: "Total number of gRPC requests processed by go-blade services.",
		},
		[]string{"service", "method", "code"},
	)

	grpcDur := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "blade_grpc_request_duration_seconds",
			Help:    "gRPC request latency distributions in seconds for go-blade services.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"service", "method"},
	)

	dbQueries := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blade_db_queries_total",
			Help: "Total number of database SQL operations executed by go-blade services.",
		},
		[]string{"table", "operation", "status"},
	)

	dbDur := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "blade_db_query_duration_seconds",
			Help:    "Database SQL query latency distributions in seconds for go-blade services.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"table", "operation"},
	)

	reg.MustRegister(httpReqs, httpDur, grpcReqs, grpcDur, dbQueries, dbDur)

	return &Collector{
		Registry:                   reg,
		HTTPRequestsTotal:          httpReqs,
		HTTPRequestDurationSeconds: httpDur,
		GRPCRequestsTotal:          grpcReqs,
		GRPCRequestDurationSeconds: grpcDur,
		DBQueriesTotal:             dbQueries,
		DBQueryDurationSeconds:     dbDur,
	}
}

// DefaultCollector 获取全局默认的 Prometheus 指标收集器单例。
func DefaultCollector() *Collector {
	defaultOnce.Do(func() {
		defaultCollector = NewCollector()
	})
	return defaultCollector
}

// RecordHTTP 在当前 Collector 中记录一次 HTTP 请求的状态码计数与耗时。
func (c *Collector) RecordHTTP(service, method, path, status string, duration time.Duration) {
	if c == nil {
		return
	}
	if strings.TrimSpace(service) == "" {
		service = "go-blade-http"
	}
	if strings.TrimSpace(method) == "" {
		method = "UNKNOWN"
	}
	if strings.TrimSpace(path) == "" {
		path = "/"
	}
	if strings.TrimSpace(status) == "" {
		status = "200"
	}
	c.HTTPRequestsTotal.WithLabelValues(service, method, path, status).Inc()
	c.HTTPRequestDurationSeconds.WithLabelValues(service, method, path).Observe(duration.Seconds())
}

// RecordGRPC 在当前 Collector 中记录一次 gRPC 调用的状态码计数与耗时。
func (c *Collector) RecordGRPC(service, method, code string, duration time.Duration) {
	if c == nil {
		return
	}
	if strings.TrimSpace(service) == "" {
		service = "go-blade-grpc"
	}
	if strings.TrimSpace(method) == "" {
		method = "unknown"
	}
	if strings.TrimSpace(code) == "" {
		code = "OK"
	}
	c.GRPCRequestsTotal.WithLabelValues(service, method, code).Inc()
	c.GRPCRequestDurationSeconds.WithLabelValues(service, method).Observe(duration.Seconds())
}

// Handler 返回当前 Collector 绑定的 Prometheus HTTP 导出处理器。
func (c *Collector) Handler() http.HandlerFunc {
	if c == nil || c.Registry == nil {
		return promhttp.Handler().ServeHTTP
	}
	return promhttp.HandlerFor(c.Registry, promhttp.HandlerOpts{}).ServeHTTP
}

// MetricsHandler 暴露全局默认 Collector 的标准 Prometheus /metrics HTTP 端点。
func MetricsHandler() http.HandlerFunc {
	return DefaultCollector().Handler()
}

// RecordGRPC 向全局默认 Collector 记录一次 gRPC 请求的调用次数、状态码与耗时指标。
func RecordGRPC(service, method, code string, duration time.Duration) {
	DefaultCollector().RecordGRPC(service, method, code, duration)
}

// RecordHTTP 向全局默认 Collector 记录一次 HTTP 请求的调用次数、状态码与耗时指标。
func RecordHTTP(service, method, path, status string, duration time.Duration) {
	DefaultCollector().RecordHTTP(service, method, path, status, duration)
}

// RecordDB 在当前 Collector 中记录一次数据库 SQL 操作的状态计数与耗时直方图。
func (c *Collector) RecordDB(table, operation, status string, duration time.Duration) {
	if c == nil || c.DBQueriesTotal == nil || c.DBQueryDurationSeconds == nil {
		return
	}
	if strings.TrimSpace(table) == "" {
		table = "unknown"
	}
	if strings.TrimSpace(operation) == "" {
		operation = "query"
	}
	if strings.TrimSpace(status) == "" {
		status = "OK"
	}
	c.DBQueriesTotal.WithLabelValues(table, operation, status).Inc()
	c.DBQueryDurationSeconds.WithLabelValues(table, operation).Observe(duration.Seconds())
}

// RecordDB 向全局默认 Collector 记录一次数据库 SQL 操作的调用次数、状态与耗时指标。
func RecordDB(table, operation, status string, duration time.Duration) {
	DefaultCollector().RecordDB(table, operation, status, duration)
}

// NormalizePath 对 URL Path 执行防高基数归一化处理：
// 将纯数字 ID（如雪花 ID）、UUID 或超长哈希段替换为统一的 ":id" 占位符，防止动态路径撑爆 Prometheus 时间序列内存。
func NormalizePath(rawPath string) string {
	trimmed := strings.TrimSpace(rawPath)
	if trimmed == "" || trimmed == "/" {
		return "/"
	}
	parts := strings.Split(trimmed, "/")
	for i, seg := range parts {
		if seg == "" {
			continue
		}
		if isDynamicPathSegment(seg) {
			parts[i] = ":id"
		}
	}
	normalized := strings.Join(parts, "/")
	if !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	return normalized
}

func isDynamicPathSegment(seg string) bool {
	// 1. 纯数字段（如 "/users/1782390182301"）
	allDigits := true
	for i := 0; i < len(seg); i++ {
		if seg[i] < '0' || seg[i] > '9' {
			allDigits = false
			break
		}
	}
	if allDigits && len(seg) > 0 {
		return true
	}

	// 2. 标准 36 位 UUID 或 32 位 Hex 字符串
	if len(seg) == 36 && seg[8] == '-' && seg[13] == '-' && seg[18] == '-' && seg[23] == '-' {
		return true
	}
	if len(seg) >= 24 {
		allHex := true
		for i := 0; i < len(seg); i++ {
			ch := seg[i]
			if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') || ch == '-') {
				allHex = false
				break
			}
		}
		if allHex {
			return true
		}
	}
	return false
}

// statusRecorder 包装 http.ResponseWriter 以捕获响应 HTTP 状态码。
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.written {
		r.statusCode = code
		r.written = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.statusCode = http.StatusOK
		r.written = true
	}
	return r.ResponseWriter.Write(b)
}

// MetricsMiddleware 构造 HTTP 监控指标采集中间件，自动统计 HTTP 请求次数、响应状态码与耗时直方图。
func MetricsMiddleware(serviceName string) func(http.HandlerFunc) http.HandlerFunc {
	if strings.TrimSpace(serviceName) == "" {
		serviceName = "go-blade-http"
	}
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			next(rec, r)

			duration := time.Since(start)
			statusStr := strconv.Itoa(rec.statusCode)
			normalizedPath := NormalizePath(r.URL.Path)
			DefaultCollector().RecordHTTP(serviceName, r.Method, normalizedPath, statusStr, duration)
		}
	}
}
