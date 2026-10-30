// Package mq 提供基于 GORM 的本地事务发件箱 (Transactional Outbox) 模式支持。
// 确保业务数据库修改与消息发布在同一本地事务中原子落盘，并由后台 Relay 协程结合分布式锁可靠异步投递至任意 mq.Broker。
package mq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/go-blade/pkg/lock"
	"github.com/go-blade/pkg/snowflake"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

const (
	// OutboxStatusPending 待投递状态
	OutboxStatusPending = 0
	// OutboxStatusSent 已成功投递状态
	OutboxStatusSent = 1
	// OutboxStatusFailed 超过最大重试次数投递失败状态
	OutboxStatusFailed = 2

	// OutboxRelayLockKey 多实例抢占执行 Outbox 批量扫描投递的分布式锁键名。
	OutboxRelayLockKey = "blade:mq:outbox:relay:lock"
	// DefaultOutboxMaxRetries 默认 Outbox 消息最大重试投递次数。
	DefaultOutboxMaxRetries = 5
)

// OutboxRecord 对应数据库 `blade_mq_outbox` 本地事务发件箱表的持久化实体。
type OutboxRecord struct {
	// ID 发件箱记录唯一雪花主键 ID
	ID snowflake.ID `gorm:"primaryKey;column:id" json:"id"`
	// TenantID 所属租户编号
	TenantID string `gorm:"column:tenant_id;size:12;not null;default:'000000';index:idx_outbox_tenant_status" json:"tenant_id"`
	// Topic 目标消息主题
	Topic string `gorm:"column:topic;size:128;not null" json:"topic"`
	// MessageKey 消息路由分区键
	MessageKey string `gorm:"column:message_key;size:128;default:''" json:"message_key"`
	// Payload 序列化后的消息载荷文本
	Payload string `gorm:"column:payload;type:text" json:"payload"`
	// Headers 序列化后的消息头 JSON 文本
	Headers string `gorm:"column:headers;type:text" json:"headers"`
	// TraceID 分布式链路追踪 TraceID
	TraceID string `gorm:"column:trace_id;size:64;default:''" json:"trace_id"`
	// Status 投递状态 (0:待投递, 1:已投递, 2:失败)
	Status int `gorm:"column:status;default:0;index:idx_outbox_status_next" json:"status"`
	// RetryCount 已重试投递次数
	RetryCount int `gorm:"column:retry_count;default:0" json:"retry_count"`
	// MaxRetries 允许的最大重试次数
	MaxRetries int `gorm:"column:max_retries;default:5" json:"max_retries"`
	// NextRetryAt 下次允许重试投递的时间戳
	NextRetryAt time.Time `gorm:"column:next_retry_at;index:idx_outbox_status_next" json:"next_retry_at"`
	// ErrorMsg 最后一次投递失败的错误信息
	ErrorMsg string `gorm:"column:error_msg;size:512;default:''" json:"error_msg"`
	// CreatedAt 创建时间
	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	// UpdatedAt 更新时间
	UpdatedAt time.Time `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

// TableName 指定 OutboxRecord 映射的数据库表名（豁免 GORM TenantPlugin 自动注入，由 Relay 跨租户统一扫描投递并在 Headers 中还原租户）。
func (OutboxRecord) TableName() string {
	return "blade_mq_outbox"
}

// PublishInTx 在给定的 GORM 事务句柄 `tx` 中将待发布消息写入 `blade_mq_outbox` 本地消息表。
// 当外层业务事务 Commit 时该消息原子落盘；当外层业务事务 Rollback 时该消息自动回滚，绝不对外发出脏消息。
func PublishInTx(ctx context.Context, tx *gorm.DB, topic string, payload any, opts ...PublishOption) error {
	if tx == nil {
		return errors.New("mq outbox transaction db cannot be nil")
	}
	msg, pubOpts, err := BuildMessage(ctx, topic, payload, opts...)
	if err != nil {
		return err
	}

	headersBytes, _ := json.Marshal(msg.Headers)
	now := time.Now().UTC()
	nextAt := now
	if pubOpts.Delay > 0 {
		nextAt = now.Add(pubOpts.Delay)
	}

	tenantID := strings.TrimSpace(msg.TenantID)
	if tenantID == "" {
		tenantID = "000000"
	}

	rec := OutboxRecord{
		ID:          snowflake.NextID(),
		TenantID:    tenantID,
		Topic:       msg.Topic,
		MessageKey:  msg.Key,
		Payload:     string(msg.Payload),
		Headers:     string(headersBytes),
		TraceID:     msg.TraceID,
		Status:      OutboxStatusPending,
		RetryCount:  0,
		MaxRetries:  DefaultOutboxMaxRetries,
		NextRetryAt: nextAt,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// 使用 Session(&gorm.Session{NewDB: true}) 旁路可能干扰写入的查询条件，直接在同一底层 SQL 事务内插入
	return tx.WithContext(ctx).Create(&rec).Error
}

// ExecuteInTx 在单个 GORM 本地数据库事务中顺序执行业务函数 `fn` 并原子写入一条 Outbox 事务消息。
// 任意一步失败均自动回滚整个数据库事务。
func ExecuteInTx(
	ctx context.Context,
	db *gorm.DB,
	topic string,
	payload any,
	fn func(tx *gorm.DB) error,
	opts ...PublishOption,
) error {
	if db == nil {
		return errors.New("mq outbox gorm db cannot be nil")
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if fn != nil {
			if err := fn(tx); err != nil {
				return err
			}
		}
		return PublishInTx(ctx, tx, topic, payload, opts...)
	})
}

// OutboxRelayOptions 定义后台 Outbox 轮询转发器的运行参数。
type OutboxRelayOptions struct {
	// PollInterval 轮询扫描间隔（默认 100ms）
	PollInterval time.Duration
	// BatchSize 单批拉取待投递消息的最大条数（默认 50）
	BatchSize int
	// LockTTL 分布式扫描锁持有时长（默认 3s）
	LockTTL time.Duration
}

// OutboxRelay 负责从 `blade_mq_outbox` 表扫描到期未发送的事务消息并可靠投递至目标 mq.Broker。
type OutboxRelay struct {
	db     *gorm.DB
	broker Broker
	opts   OutboxRelayOptions

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewOutboxRelay 创建并初始化一个 GORM 本地事务发件箱转发器实例。
func NewOutboxRelay(db *gorm.DB, broker Broker, opts ...OutboxRelayOptions) *OutboxRelay {
	cfg := OutboxRelayOptions{
		PollInterval: 100 * time.Millisecond,
		BatchSize:    50,
		LockTTL:      3 * time.Second,
	}
	if len(opts) > 0 {
		if opts[0].PollInterval > 0 {
			cfg.PollInterval = opts[0].PollInterval
		}
		if opts[0].BatchSize > 0 {
			cfg.BatchSize = opts[0].BatchSize
		}
		if opts[0].LockTTL > 0 {
			cfg.LockTTL = opts[0].LockTTL
		}
	}
	if broker == nil {
		broker = DefaultBroker()
	}
	return &OutboxRelay{
		db:     db,
		broker: broker,
		opts:   cfg,
	}
}

// ProcessPendingOnce 立即扫描并投递一批已到期的 Outbox 待发送记录，返回成功投递的条数。
func (r *OutboxRelay) ProcessPendingOnce(ctx context.Context) (int, error) {
	if r == nil || r.db == nil || r.broker == nil {
		return 0, errors.New("outbox relay db or broker is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	acquired, lockErr := lock.Acquire(ctx, OutboxRelayLockKey, r.opts.LockTTL)
	if lockErr != nil {
		if errors.Is(lockErr, lock.ErrLockNotAcquired) {
			return 0, nil
		}
		return 0, lockErr
	}
	defer func() {
		_ = acquired.Unlock(context.Background())
	}()

	now := time.Now().UTC()
	var records []OutboxRecord
	// 使用 Unscoped 并在查询时忽略单租户过滤（通过 Table 显式扫描全租户待发送发件箱）
	err := r.db.WithContext(ctx).
		Table(OutboxRecord{}.TableName()).
		Where("status = ? AND next_retry_at <= ?", OutboxStatusPending, now).
		Order("id asc").
		Limit(r.opts.BatchSize).
		Find(&records).Error
	if err != nil {
		return 0, err
	}

	sentCount := 0
	for _, rec := range records {
		var headers map[string]string
		if rec.Headers != "" {
			_ = json.Unmarshal([]byte(rec.Headers), &headers)
		}

		pubErr := r.broker.Publish(
			ctx,
			rec.Topic,
			[]byte(rec.Payload),
			WithKey(rec.MessageKey),
			WithHeaders(headers),
		)

		if pubErr == nil {
			updateErr := r.db.WithContext(ctx).
				Table(OutboxRecord{}.TableName()).
				Where("id = ?", rec.ID).
				Updates(map[string]any{
					"status":     OutboxStatusSent,
					"updated_at": time.Now().UTC(),
				}).Error
			if updateErr == nil {
				sentCount++
			}
			continue
		}

		nextRetry := rec.RetryCount + 1
		newStatus := OutboxStatusPending
		maxRetries := rec.MaxRetries
		if maxRetries <= 0 {
			maxRetries = DefaultOutboxMaxRetries
		}
		if nextRetry >= maxRetries {
			newStatus = OutboxStatusFailed
		}
		backoff := time.Duration(1<<nextRetry) * 100 * time.Millisecond
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}

		errMsg := pubErr.Error()
		if len(errMsg) > 500 {
			errMsg = errMsg[:500]
		}

		_ = r.db.WithContext(ctx).
			Table(OutboxRecord{}.TableName()).
			Where("id = ?", rec.ID).
			Updates(map[string]any{
				"status":        newStatus,
				"retry_count":   nextRetry,
				"next_retry_at": time.Now().UTC().Add(backoff),
				"error_msg":     errMsg,
				"updated_at":    time.Now().UTC(),
			}).Error
	}

	return sentCount, nil
}

// Start 启动后台轮询协程，定期扫描并投递 `blade_mq_outbox` 中的待发送消息。
func (r *OutboxRelay) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.stopCh = make(chan struct{})
	stopCh := r.stopCh
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(r.opts.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := r.ProcessPendingOnce(ctx); err != nil {
					logx.Errorf("[mq-outbox] process pending outbox failed: %v", err)
				}
			}
		}
	}()
}

// Stop 优雅停止后台 OutboxRelay 轮询协程。
func (r *OutboxRelay) Stop() {
	r.mu.Lock()
	if !r.running {
		r.mu.Unlock()
		return
	}
	r.running = false
	close(r.stopCh)
	r.mu.Unlock()
	r.wg.Wait()
}
