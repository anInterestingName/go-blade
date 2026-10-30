// Package database 提供基于 GORM 的数据库连接池管理与日志适配。
// 将 GORM 执行的 SQL 日志与慢查询无缝接入 go-zero 的 logx 链路追踪体系。
package database

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	gormlogger "gorm.io/gorm/logger"
)

// GormLogger 适配器结构体：实现 GORM 的 logger.Interface 接口。
// 负责将 SQL 语句、受影响行数以及慢查询转接到 go-zero 的分布式日志追踪器中。
type GormLogger struct {
	// SlowThreshold 慢查询耗时阈值（超过该时长判定为慢 SQL 并以 Slow 级别打印）
	SlowThreshold time.Duration
	// LogLevel 日志打印级别阈值 (Silent, Error, Warn, Info)
	LogLevel gormlogger.LogLevel
}

// NewGormLogger 构造并初始化一个适配 go-zero logx 的 GormLogger 实例。
// slowThreshold: 慢查询阈值（缺省默认 200ms）；level: 日志过滤级别。
func NewGormLogger(slowThreshold time.Duration, level gormlogger.LogLevel) *GormLogger {
	if slowThreshold <= 0 {
		slowThreshold = 200 * time.Millisecond
	}
	return &GormLogger{
		SlowThreshold: slowThreshold,
		LogLevel:      level,
	}
}

// LogMode 调整日志记录器的级别并返回新的 logger 接口实例。
func (l *GormLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	newLogger := *l
	newLogger.LogLevel = level
	return &newLogger
}

// Info 输出普通业务信息日志至 logx。
func (l *GormLogger) Info(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= gormlogger.Info {
		logx.WithContext(ctx).Infof(msg, data...)
	}
}

// Warn 输出警告级别日志至 logx 慢日志/提示输出。
func (l *GormLogger) Warn(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= gormlogger.Warn {
		logx.WithContext(ctx).Infof("[WARN] "+msg, data...)
	}
}

// Error 输出异常与错误日志至 logx。
func (l *GormLogger) Error(ctx context.Context, msg string, data ...interface{}) {
	if l.LogLevel >= gormlogger.Error {
		logx.WithContext(ctx).Errorf(msg, data...)
	}
}

// Trace 捕获并记录单条 SQL 执行追踪信息。
// 自动根据执行耗时判定是否为慢 SQL，并将 SQL 文本及 rows 统计输出至 logx 上下文中。
func (l *GormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if l.LogLevel <= gormlogger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()

	logger := logx.WithContext(ctx)

	switch {
	case err != nil && l.LogLevel >= gormlogger.Error:
		logger.Errorf("[GORM-ERR] %s | %s | rows:%d | err: %v", elapsed, sql, rows, err)
	case elapsed > l.SlowThreshold && l.SlowThreshold != 0 && l.LogLevel >= gormlogger.Warn:
		logger.Slowf("[GORM-SLOW] %s (threshold: %s) | %s | rows:%d", elapsed, l.SlowThreshold, sql, rows)
	case l.LogLevel >= gormlogger.Info:
		logger.Infof("[GORM-SQL] %s | %s | rows:%d", elapsed, sql, rows)
	}
}

var _ gormlogger.Interface = (*GormLogger)(nil)
