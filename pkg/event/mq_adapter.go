// Package event 提供支持并发 Worker 池、上下文租户/链路透传、Panic 隔离恢复与优雅关机排空的领域事件总线底座，
// 并提供与 pkg/mq 分布式消息队列 Broker 的双向桥接适配器。
package event

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/go-blade/pkg/mq"
	"github.com/go-blade/pkg/snowflake"
)

// TopicRBACChanged RBAC 角色菜单或用户权限发生变更时的标准跨服务广播主题名称。
const TopicRBACChanged = "blade.system.rbac.changed"

// TopicNoticePublished 协同工作台发布或更新通知公告时的标准跨服务实时广播主题名称。
const TopicNoticePublished = "blade.desk.notice.published"

// RBACChangedEvent 定义当系统角色、菜单授权或用户角色发生变更时发布的广播事件载荷。
type RBACChangedEvent struct {
	// TenantID 发生权限变更的租户编号
	TenantID string `json:"tenant_id"`
	// Action 触发变更的操作类型（如 "grant_role_menus", "save_role", "delete_roles", "save_menu", "delete_menus", "save_user", "delete_users"）
	Action string `json:"action"`
	// RoleIDs 受影响的角色主键 ID 列表（可选）
	RoleIDs []int64 `json:"role_ids,omitempty"`
	// UserIDs 受影响的用户主键 ID 列表（可选）
	UserIDs []int64 `json:"user_ids,omitempty"`
}

// NoticePublishedEvent 定义当协同工作台发布或更新通知公告时触发的实时广播事件载荷。
type NoticePublishedEvent struct {
	// ID 公告雪花主键 ID
	ID snowflake.ID `json:"id"`
	// TenantID 所属租户编号
	TenantID string `json:"tenant_id"`
	// Title 公告标题
	Title string `json:"title"`
	// Category 公告分类
	Category int32 `json:"category"`
	// ReleaseTime 发布时间
	ReleaseTime string `json:"release_time"`
}

// MQBusAdapter 将 pkg/mq.Broker 适配为满足 event.Bus 接口的分布式领域事件总线实现。
type MQBusAdapter struct {
	broker    mq.Broker
	broadcast bool
	group     string
}

// NewMQBusAdapter 基于给定的 mq.Broker 构造一个实现 event.Bus 接口的适配器。
// 当 broadcast 为 true 时，Subscribe 默认采用全实例广播模式（WithBroadcast）；否则采用集群竞争消费组模式。
func NewMQBusAdapter(broker mq.Broker, broadcast bool, group ...string) *MQBusAdapter {
	if broker == nil {
		broker = mq.DefaultBroker()
	}
	g := ""
	if len(group) > 0 {
		g = strings.TrimSpace(group[0])
	}
	return &MQBusAdapter{
		broker:    broker,
		broadcast: broadcast,
		group:     g,
	}
}

// Subscribe 在底层 mq.Broker 上注册领域事件消费者处理器。
func (a *MQBusAdapter) Subscribe(topic string, handler HandlerFunc) {
	if a == nil || a.broker == nil || handler == nil {
		return
	}
	var opts []mq.SubscribeOption
	if a.group != "" {
		opts = append(opts, mq.WithGroup(a.group))
	}
	if a.broadcast {
		opts = append(opts, mq.WithBroadcast())
	}

	_ = a.broker.Subscribe(topic, func(ctx context.Context, m mq.Message) error {
		var payload any = m.Payload
		var decoded any
		if len(m.Payload) > 0 && json.Unmarshal(m.Payload, &decoded) == nil {
			payload = decoded
		}
		evMsg := Message{
			ID:         m.ID,
			Topic:      m.Topic,
			TenantID:   m.TenantID,
			TraceID:    m.TraceID,
			Payload:    payload,
			OccurredAt: m.OccurredAt,
		}
		return handler(ctx, evMsg)
	}, opts...)
}

// Publish 将领域事件通过底层 mq.Broker 投递至指定主题。
func (a *MQBusAdapter) Publish(ctx context.Context, topic string, payload any) error {
	if a == nil || a.broker == nil {
		return ErrBusClosed
	}
	return a.broker.Publish(ctx, topic, payload)
}

// Close 优雅关闭底层绑定的 mq.Broker。
func (a *MQBusAdapter) Close(ctx context.Context) error {
	if a == nil || a.broker == nil {
		return nil
	}
	return a.broker.Close(ctx)
}
