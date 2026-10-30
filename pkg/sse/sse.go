// Package sse 提供支持多租户隔离与用户定向推送的 Server-Sent Events (SSE) 实时消息流底座。
package sse

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/go-blade/pkg/jwt"
	"github.com/go-blade/pkg/snowflake"
	"github.com/go-blade/pkg/tenant"
)

// Event 定义通过 SSE 推送给客户端的标准事件帧。
type Event struct {
	// ID 事件唯一标识
	ID string `json:"id,omitempty"`
	// Event 事件类型名称（如 "notice.published"）
	Event string `json:"event,omitempty"`
	// Data 事件业务数据载荷
	Data any `json:"data"`
}

// Format 将 Event 编码为符合 W3C Server-Sent Events 协议规范的文本字节流。
func (e Event) Format() []byte {
	var b strings.Builder
	if e.ID != "" {
		b.WriteString("id: ")
		b.WriteString(strings.ReplaceAll(e.ID, "\n", ""))
		b.WriteString("\n")
	}
	if e.Event != "" {
		b.WriteString("event: ")
		b.WriteString(strings.ReplaceAll(e.Event, "\n", ""))
		b.WriteString("\n")
	}

	var payloadStr string
	switch v := e.Data.(type) {
	case string:
		payloadStr = v
	case []byte:
		payloadStr = string(v)
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			payloadStr = fmt.Sprintf("%v", v)
		} else {
			payloadStr = string(raw)
		}
	}

	for _, line := range strings.Split(payloadStr, "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteString("\n")
	}
	b.WriteString("\n")
	return []byte(b.String())
}

// Client 表示一个活跃的 SSE 客户端订阅连接。
type Client struct {
	// ID 客户端连接唯一标识
	ID string
	// TenantID 客户端所属租户编号
	TenantID string
	// UserID 客户端所属用户主键 ID
	UserID int64
	// Events 接收服务端推送事件的缓冲通道
	Events chan Event
}

// Hub 维护所有在线 SSE 客户端连接，支持按租户广播、按用户单播与全局广播。
type Hub struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

// NewHub 创建并初始化一个并发安全的 SSE 推送中心实例。
func NewHub() *Hub {
	return &Hub{
		clients: make(map[string]*Client),
	}
}

// Register 注册一个新的 SSE 客户端连接并返回 Client 实例与注销回调。
func (h *Hub) Register(tenantID string, userID int64) (*Client, func()) {
	cleanTenant := strings.TrimSpace(tenantID)
	if cleanTenant == "" {
		cleanTenant = tenant.DefaultTenantID
	}
	client := &Client{
		ID:       snowflake.GenIDString(),
		TenantID: cleanTenant,
		UserID:   userID,
		Events:   make(chan Event, 32),
	}

	h.mu.Lock()
	h.clients[client.ID] = client
	h.mu.Unlock()

	unregister := func() {
		h.mu.Lock()
		if _, exists := h.clients[client.ID]; exists {
			delete(h.clients, client.ID)
			close(client.Events)
		}
		h.mu.Unlock()
	}
	return client, unregister
}

// BroadcastToTenant 向指定租户下的所有在线 SSE 客户端推送一条实时事件，返回成功投递的客户端数量。
func (h *Hub) BroadcastToTenant(tenantID string, ev Event) int {
	if h == nil {
		return 0
	}
	cleanTenant := strings.TrimSpace(tenantID)
	if cleanTenant == "" {
		cleanTenant = tenant.DefaultTenantID
	}
	if ev.ID == "" {
		ev.ID = snowflake.GenIDString()
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	delivered := 0
	for _, c := range h.clients {
		if c.TenantID == cleanTenant {
			select {
			case c.Events <- ev:
				delivered++
			default:
			}
		}
	}
	return delivered
}

// SendToUser 向指定租户与指定 UserID 的在线 SSE 客户端定向推送一条事件。
func (h *Hub) SendToUser(tenantID string, userID int64, ev Event) int {
	if h == nil || userID <= 0 {
		return 0
	}
	cleanTenant := strings.TrimSpace(tenantID)
	if cleanTenant == "" {
		cleanTenant = tenant.DefaultTenantID
	}
	if ev.ID == "" {
		ev.ID = snowflake.GenIDString()
	}

	h.mu.RLock()
	defer h.mu.RUnlock()

	delivered := 0
	for _, c := range h.clients {
		if c.TenantID == cleanTenant && c.UserID == userID {
			select {
			case c.Events <- ev:
				delivered++
			default:
			}
		}
	}
	return delivered
}

// ClientCount 返回当前在线的 SSE 客户端连接总数。
func (h *Hub) ClientCount() int {
	if h == nil {
		return 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// StreamHandler 构造标准 HTTP SSE 流式订阅处理器。
// 自动从请求上下文提取 TenantID 与 BladeClaims.UserID，建立 `text/event-stream` 长连接并实时推送事件帧。
func (h *Hub) StreamHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		ctx := r.Context()
		tenantID := tenant.FromTenant(ctx)
		var userID int64
		if claims := jwt.FromClaims(ctx); claims != nil {
			userID = claims.UserID
			if claims.TenantID != "" && (tenantID == "" || tenantID == tenant.DefaultTenantID) {
				tenantID = claims.TenantID
			}
		}

		w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("X-Accel-Buffering", "no")

		client, unregister := h.Register(tenantID, userID)
		defer unregister()

		// 发送初始连接就绪事件帧
		connectedFrame := Event{
			ID:    client.ID,
			Event: "connected",
			Data: map[string]any{
				"client_id": client.ID,
				"tenant_id": client.TenantID,
			},
		}
		_, _ = w.Write(connectedFrame.Format())
		flusher.Flush()

		for {
			select {
			case <-ctx.Done():
				return
			case ev, open := <-client.Events:
				if !open {
					return
				}
				_, _ = w.Write(ev.Format())
				flusher.Flush()
			}
		}
	}
}
