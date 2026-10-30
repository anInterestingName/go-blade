// Package auditlog 提供敏感字段自动脱敏工具与 API 操作审计日志采集底座（对标 SpringBlade `blade_log_api`）。
// 支持对密码、令牌、验证码、手机号、身份证号等敏感键值自动掩码，并提供内存环形存储与基于 pkg/event 的异步审计分发。
package auditlog

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-blade/pkg/event"
	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
	"github.com/go-blade/pkg/trace"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	// AuditEventTopic API 操作审计日志投递至领域事件总线的默认主题名称。
	AuditEventTopic = "blade.audit.api"
	// MaskedPlaceholder 密码、密钥与令牌等高度敏感字段的统一掩码占位符。
	MaskedPlaceholder = "******"
	// DefaultMaxMemoryEntries MemoryRecorder 默认保留的最大审计日志条数。
	DefaultMaxMemoryEntries = 1000
	// MaxRawBodyFallbackLen 非 JSON 请求体或超长文本截断的安全最大长度（字节）。
	MaxRawBodyFallbackLen = 512
)

var secretNormalizedKeys = map[string]struct{}{
	"password":     {},
	"oldpassword":  {},
	"newpassword":  {},
	"secret":       {},
	"accesstoken":  {},
	"refreshtoken": {},
	"captchacode":  {},
}

var phoneNormalizedKeys = map[string]struct{}{
	"phone":         {},
	"mobile":        {},
	"contactnumber": {},
}

var idCardNormalizedKeys = map[string]struct{}{
	"idcard":   {},
	"idnumber": {},
}

// MaskPhone 对手机号码字符串执行掩码脱敏（例如 `"13812345678"` -> `"138****5678"`）。
// 若长度不足以按标准手机号保留前 3 后 4，则按安全降级规则掩码中间字符。
func MaskPhone(phone string) string {
	trimmed := strings.TrimSpace(phone)
	if trimmed == "" {
		return ""
	}
	runes := []rune(trimmed)
	n := len(runes)
	if n >= 7 {
		return string(runes[:3]) + "****" + string(runes[n-4:])
	}
	if n <= 2 {
		return MaskedPlaceholder
	}
	return string(runes[:1]) + "****" + string(runes[n-1:])
}

// MaskIDCard 对身份证号字符串执行掩码脱敏（保留前 6 位与后 4 位，中间替换为 `"********"`）。
func MaskIDCard(idCard string) string {
	trimmed := strings.TrimSpace(idCard)
	if trimmed == "" {
		return ""
	}
	runes := []rune(trimmed)
	n := len(runes)
	if n >= 15 {
		return string(runes[:6]) + "********" + string(runes[n-4:])
	}
	return MaskPhone(trimmed)
}

// MaskString 根据字段键名对单个字符串值执行敏感脱敏处理。
func MaskString(key, val string) string {
	norm := normalizeKey(key)
	if _, ok := secretNormalizedKeys[norm]; ok {
		return MaskedPlaceholder
	}
	if _, ok := phoneNormalizedKeys[norm]; ok {
		return MaskPhone(val)
	}
	if _, ok := idCardNormalizedKeys[norm]; ok {
		return MaskIDCard(val)
	}
	return val
}

// MaskJSON 解析 JSON 字节流（支持嵌套对象与数组），对敏感字段递归执行掩码脱敏：
// - 键名（忽略大小写、下划线与连字符）匹配 `password`、`old_password`、`new_password`、`secret`、`access_token`、`refresh_token`、`captcha_code` 的值统一替换为 `"******"`；
// - 匹配 `phone`、`mobile` 的字符串字段执行 `MaskPhone` 脱敏；
// - 匹配 `id_card` 的字符串字段执行 `MaskIDCard` 脱敏；
// - 若输入不是合法 JSON，则做安全截断并返回字符串。
func MaskJSON(raw []byte) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}

	var data any
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		return truncateSafeString(string(trimmed))
	}

	// 若输入不是合法 JSON，或仅是单段非 JSON 词法单元（如 "123 abc" 被 Decoder 读走首个 token），则做安全截断并返回字符串
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return truncateSafeString(string(trimmed))
	}

	masked := maskValue(data)
	out, err := json.Marshal(masked)
	if err != nil {
		return truncateSafeString(string(trimmed))
	}
	return string(out)
}

func normalizeKey(key string) string {
	var b strings.Builder
	b.Grow(len(key))
	for _, r := range strings.ToLower(strings.TrimSpace(key)) {
		if r == '_' || r == '-' || r == ' ' {
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func maskValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		res := make(map[string]any, len(val))
		for k, item := range val {
			norm := normalizeKey(k)
			if _, isSecret := secretNormalizedKeys[norm]; isSecret {
				res[k] = MaskedPlaceholder
				continue
			}
			if _, isPhone := phoneNormalizedKeys[norm]; isPhone {
				if strVal, ok := item.(string); ok {
					res[k] = MaskPhone(strVal)
					continue
				}
			}
			if _, isIDCard := idCardNormalizedKeys[norm]; isIDCard {
				if strVal, ok := item.(string); ok {
					res[k] = MaskIDCard(strVal)
					continue
				}
			}
			res[k] = maskValue(item)
		}
		return res
	case []any:
		res := make([]any, len(val))
		for i, item := range val {
			res[i] = maskValue(item)
		}
		return res
	default:
		return v
	}
}

func truncateSafeString(s string) string {
	runes := []rune(s)
	if len(runes) <= MaxRawBodyFallbackLen {
		return s
	}
	return string(runes[:MaxRawBodyFallbackLen]) + "...(truncated)"
}

// Entry 对标 SpringBlade `blade_log_api` 的 API 操作审计日志实体。
type Entry struct {
	// ID 审计记录唯一雪花主键
	ID snowflake.ID `json:"id"`
	// TenantID 租户编号
	TenantID string `json:"tenant_id"`
	// ServiceName 产生审计日志的服务名称（如 "service-gateway"）
	ServiceName string `json:"service_name"`
	// UserID 操作用户主键 ID
	UserID snowflake.ID `json:"user_id"`
	// Account 操作人登录账号
	Account string `json:"account"`
	// Method HTTP 请求方法（POST / PUT / DELETE / PATCH 等）
	Method string `json:"method"`
	// Path HTTP 请求路径
	Path string `json:"path"`
	// Query URL 查询参数字符串
	Query string `json:"query,omitempty"`
	// RequestBody 经过敏感字段掩码脱敏后的请求体摘要
	RequestBody string `json:"request_body,omitempty"`
	// StatusCode HTTP 响应状态码
	StatusCode int `json:"status_code"`
	// DurationMs 请求处理耗时（毫秒）
	DurationMs int64 `json:"duration_ms"`
	// ClientIP 客户端真实 IP 地址
	ClientIP string `json:"client_ip"`
	// TraceID 分布式链路追踪 TraceID
	TraceID string `json:"trace_id"`
	// CreatedAt 审计记录生成时间
	CreatedAt time.Time `json:"created_at"`
}

// Recorder 定义 API 操作审计日志记录器接口。
type Recorder interface {
	// Record 记录一条已脱敏的 API 操作审计日志。
	Record(ctx context.Context, entry Entry) error
}

// MemoryRecorder 并发安全的内存环形/切片审计日志记录器，支持查询近期审计条目与测试断言。
type MemoryRecorder struct {
	mu         sync.RWMutex
	maxEntries int
	entries    []Entry
}

// MemoryStore 是 MemoryRecorder 的语义类型别名。
type MemoryStore = MemoryRecorder

// NewMemoryRecorder 创建并初始化一个指定最大容量的内存审计日志记录器。
func NewMemoryRecorder(maxEntries ...int) *MemoryRecorder {
	limit := DefaultMaxMemoryEntries
	if len(maxEntries) > 0 && maxEntries[0] > 0 {
		limit = maxEntries[0]
	}
	return &MemoryRecorder{
		maxEntries: limit,
		entries:    make([]Entry, 0, 64),
	}
}

// Record 将审计条目追加至并发安全的内存切片（超出容量上限时自动淘汰最早记录）。
func (m *MemoryRecorder) Record(ctx context.Context, entry Entry) error {
	if m == nil {
		return nil
	}
	if entry.ID == 0 {
		entry.ID = snowflake.NextID()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.entries) >= m.maxEntries {
		copy(m.entries, m.entries[1:])
		m.entries[len(m.entries)-1] = entry
	} else {
		m.entries = append(m.entries, entry)
	}
	return nil
}

// Entries 返回当前已记录的所有审计日志副本切片（并发安全）。
func (m *MemoryRecorder) Entries() []Entry {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]Entry, len(m.entries))
	copy(out, m.entries)
	return out
}

// EventRecorder 将 API 操作审计日志异步发布至 pkg/event 领域事件总线的记录器实现。
type EventRecorder struct {
	bus   event.Bus
	topic string
}

// EventStore 是 EventRecorder 的语义类型别名。
type EventStore = EventRecorder

// NewEventRecorder 创建一个基于领域事件总线的异步审计记录器。
// 若传入的 bus 为 nil，则默认使用 event.DefaultBus()；默认发布主题为 AuditEventTopic ("blade.audit.api")。
func NewEventRecorder(bus event.Bus, topic ...string) *EventRecorder {
	if bus == nil {
		bus = event.DefaultBus()
	}
	t := AuditEventTopic
	if len(topic) > 0 && strings.TrimSpace(topic[0]) != "" {
		t = strings.TrimSpace(topic[0])
	}
	return &EventRecorder{
		bus:   bus,
		topic: t,
	}
}

// Record 将审计日志条目发布至领域事件总线的 `"blade.audit.api"` 主题。
func (e *EventRecorder) Record(ctx context.Context, entry Entry) error {
	if e == nil {
		return nil
	}
	if entry.ID == 0 {
		entry.ID = snowflake.NextID()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	b := e.bus
	if b == nil {
		b = event.DefaultBus()
	}
	return b.Publish(ctx, e.topic, entry)
}

// MQRecorder 将 API 操作审计日志通过 pkg/mq 分布式消息队列异步投递的记录器实现。
type MQRecorder struct {
	broker mq.Broker
	topic  string
}

// NewMQRecorder 创建一个基于 pkg/mq.Broker 的分布式审计日志记录器。
// 若传入的 broker 为 nil，则默认使用 mq.DefaultBroker()；默认主题为 AuditEventTopic ("blade.audit.api")。
func NewMQRecorder(broker mq.Broker, topic ...string) *MQRecorder {
	if broker == nil {
		broker = mq.DefaultBroker()
	}
	t := AuditEventTopic
	if len(topic) > 0 && strings.TrimSpace(topic[0]) != "" {
		t = strings.TrimSpace(topic[0])
	}
	return &MQRecorder{
		broker: broker,
		topic:  t,
	}
}

// Record 将审计日志条目封装并投递至分布式消息队列的 `"blade.audit.api"` 主题。
func (r *MQRecorder) Record(ctx context.Context, entry Entry) error {
	if r == nil {
		return nil
	}
	if entry.ID == 0 {
		entry.ID = snowflake.NextID()
	}
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	b := r.broker
	if b == nil {
		b = mq.DefaultBroker()
	}
	return b.Publish(ctx, r.topic, entry, mq.WithKey(entry.TenantID))
}

// MultiRecorder 组合多个 Recorder，支持同时写入内存审计缓存并投递异步领域事件。
type MultiRecorder struct {
	recorders []Recorder
}

// NewMultiRecorder 构造一个多路审计记录器。
func NewMultiRecorder(recorders ...Recorder) *MultiRecorder {
	filtered := make([]Recorder, 0, len(recorders))
	for _, r := range recorders {
		if r != nil {
			filtered = append(filtered, r)
		}
	}
	return &MultiRecorder{recorders: filtered}
}

// Record 依次向内部所有 Recorder 写入审计条目。
func (m *MultiRecorder) Record(ctx context.Context, entry Entry) error {
	if m == nil {
		return nil
	}
	var firstErr error
	for _, r := range m.recorders {
		if err := r.Record(ctx, entry); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// responseRecorder 包装 http.ResponseWriter 以捕获响应状态码与最后更新的请求上下文。
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func (r *responseRecorder) WriteHeader(code int) {
	if !r.written {
		r.statusCode = code
		r.written = true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.statusCode = http.StatusOK
		r.written = true
	}
	return r.ResponseWriter.Write(b)
}

// AuditMiddleware 构造 HTTP API 操作审计日志中间件：
// 拦截写请求（POST / PUT / DELETE / PATCH），无损读取请求体并重新回填 `r.Body`，
// 自动对请求体执行 `MaskJSON` 敏感字段脱敏，并在请求结束时采集状态码、耗时、租户、操作人及 TraceID 写入 Recorder。
func AuditMiddleware(serviceName string, recorder Recorder) func(http.HandlerFunc) http.HandlerFunc {
	if strings.TrimSpace(serviceName) == "" {
		serviceName = "go-blade-http"
	}

	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !isAuditableMethod(r.Method) {
				next(w, r)
				return
			}

			start := time.Now()

			var bodyBytes []byte
			contentType := strings.ToLower(r.Header.Get("Content-Type"))
			isMultipart := strings.HasPrefix(contentType, "multipart/")

			if r.Body != nil && !isMultipart {
				readBytes, err := io.ReadAll(r.Body)
				if err == nil {
					bodyBytes = readBytes
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}

			// 捕获下游中间件或 Handler 更新后的最新 Context
			capturedCtx := r.Context()
			rec := &responseRecorder{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			next(rec, r)

			// 如果下游在 r 上调用了 WithContext，这里我们也尝试从原始请求与解析信息中提取
			ctx := capturedCtx
			tenantID := tenant.FromTenant(ctx)
			claims := jwt.FromClaims(ctx)
			traceID := trace.TraceIDFromContext(ctx)
			if traceID == "" {
				traceID = strings.TrimSpace(rec.Header().Get(trace.HeaderTraceID))
			}

			var userID int64
			var account string
			if claims != nil {
				userID = claims.UserID
				account = claims.Account
				if claims.TenantID != "" && (tenantID == "" || tenantID == tenant.DefaultTenantID) {
					tenantID = claims.TenantID
				}
			}

			maskedBody := ""
			if isMultipart {
				maskedBody = "[multipart/form-data]"
			} else if len(bodyBytes) > 0 {
				maskedBody = MaskJSON(bodyBytes)
				// 若未登录（例如登录接口 /blade-auth/oauth/token），尝试从 JSON 请求体补充 account 与 tenant_id
				if account == "" || tenantID == "" || tenantID == tenant.DefaultTenantID {
					bodyTenant, bodyAcc := extractIdentityFromJSON(bodyBytes)
					if account == "" && bodyAcc != "" {
						account = bodyAcc
					}
					if (tenantID == "" || tenantID == tenant.DefaultTenantID) && bodyTenant != "" {
						tenantID = bodyTenant
					}
				}
			}

			entry := Entry{
				ID:          snowflake.NextID(),
				TenantID:    tenantID,
				ServiceName: serviceName,
				UserID:      snowflake.ID(userID),
				Account:     account,
				Method:      strings.ToUpper(r.Method),
				Path:        r.URL.Path,
				Query:       r.URL.RawQuery,
				RequestBody: maskedBody,
				StatusCode:  rec.statusCode,
				DurationMs:  time.Since(start).Milliseconds(),
				ClientIP:    extractClientIP(r),
				TraceID:     traceID,
				CreatedAt:   time.Now(),
			}

			if recorder != nil {
				if err := recorder.Record(ctx, entry); err != nil {
					logx.WithContext(ctx).Errorf("[auditlog] record audit entry failed: %v", err)
				}
			} else {
				_ = event.DefaultBus().Publish(ctx, AuditEventTopic, entry)
			}
		}
	}
}

// AuditLogMiddleware 是 AuditMiddleware 的语义别名函数。
func AuditLogMiddleware(serviceName string, recorder Recorder) func(http.HandlerFunc) http.HandlerFunc {
	return AuditMiddleware(serviceName, recorder)
}

func isAuditableMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch:
		return true
	default:
		return false
	}
}

func extractIdentityFromJSON(raw []byte) (tenantID string, account string) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", ""
	}
	if v, ok := m["account"].(string); ok {
		account = strings.TrimSpace(v)
	}
	if v, ok := m["tenant_id"].(string); ok && strings.TrimSpace(v) != "" {
		tenantID = strings.TrimSpace(v)
	} else if v, ok := m["tenantId"].(string); ok && strings.TrimSpace(v) != "" {
		tenantID = strings.TrimSpace(v)
	}
	return tenantID, account
}

func extractClientIP(r *http.Request) string {
	if xff := strings.TrimSpace(r.Header.Get("X-Forwarded-For")); xff != "" {
		parts := strings.Split(xff, ",")
		if ip := strings.TrimSpace(parts[0]); ip != "" {
			return ip
		}
	}
	if xrip := strings.TrimSpace(r.Header.Get("X-Real-Ip")); xrip != "" {
		return xrip
	}
	host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr))
	if err == nil && host != "" {
		return host
	}
	return strings.TrimSpace(r.RemoteAddr)
}
