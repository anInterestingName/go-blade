// Package database 提供 GORM OpenTelemetry 全链路追踪 Span 插件与 Prometheus SQL 指标打点支持。
package database

import (
	"errors"
	"time"

	"github.com/go-blade/pkg/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

const (
	gormSpanKey      = "blade:gorm:otel_span"
	gormStartTimeKey = "blade:gorm:start_time"
)

// TracingPlugin GORM 全链路可观测插件：
// 在每次数据库操作（Create / Query / Update / Delete / Row / Raw）前后自动创建 OpenTelemetry Client Span，
// 记录目标表名、影响行数与异常信息，并向 pkg/metrics 采集 SQL 操作次数与耗时直方图。
type TracingPlugin struct{}

// NewTracingPlugin 构造并返回一个 GORM 全链路追踪与指标监控插件实例。
func NewTracingPlugin() *TracingPlugin {
	return &TracingPlugin{}
}

// Name 返回插件在 GORM 中的唯一注册名称。
func (p *TracingPlugin) Name() string {
	return "blade_tracing_plugin"
}

// Initialize 向 GORM 六大操作生命周期注册 Before 与 After 追踪钩子。
func (p *TracingPlugin) Initialize(db *gorm.DB) error {
	_ = db.Callback().Create().Before("gorm:create").Register("blade:trace_before_create", p.before("create"))
	_ = db.Callback().Create().After("gorm:create").Register("blade:trace_after_create", p.after("create"))

	_ = db.Callback().Query().Before("gorm:query").Register("blade:trace_before_query", p.before("query"))
	_ = db.Callback().Query().After("gorm:query").Register("blade:trace_after_query", p.after("query"))

	_ = db.Callback().Update().Before("gorm:update").Register("blade:trace_before_update", p.before("update"))
	_ = db.Callback().Update().After("gorm:update").Register("blade:trace_after_update", p.after("update"))

	_ = db.Callback().Delete().Before("gorm:delete").Register("blade:trace_before_delete", p.before("delete"))
	_ = db.Callback().Delete().After("gorm:delete").Register("blade:trace_after_delete", p.after("delete"))

	_ = db.Callback().Row().Before("gorm:row").Register("blade:trace_before_row", p.before("row"))
	_ = db.Callback().Row().After("gorm:row").Register("blade:trace_after_row", p.after("row"))

	_ = db.Callback().Raw().Before("gorm:raw").Register("blade:trace_before_raw", p.before("raw"))
	_ = db.Callback().Raw().After("gorm:raw").Register("blade:trace_after_raw", p.after("raw"))

	return nil
}

func (p *TracingPlugin) before(operation string) func(*gorm.DB) {
	return func(db *gorm.DB) {
		if db == nil || db.Statement == nil || db.Statement.Context == nil {
			return
		}
		db.InstanceSet(gormStartTimeKey, time.Now())

		spanName := "gorm:" + operation
		if db.Statement.Table != "" {
			spanName = spanName + "/" + db.Statement.Table
		}
		ctx, span := otel.Tracer("go-blade-gorm").Start(
			db.Statement.Context,
			spanName,
			oteltrace.WithSpanKind(oteltrace.SpanKindClient),
		)
		db.Statement.Context = ctx
		db.InstanceSet(gormSpanKey, span)
	}
}

func (p *TracingPlugin) after(operation string) func(*gorm.DB) {
	return func(db *gorm.DB) {
		if db == nil || db.Statement == nil {
			return
		}

		table := db.Statement.Table
		if table == "" && db.Statement.Schema != nil {
			table = db.Statement.Schema.Table
		}
		if table == "" {
			table = "unknown"
		}

		var duration time.Duration
		if v, ok := db.InstanceGet(gormStartTimeKey); ok {
			if startTime, valid := v.(time.Time); valid {
				duration = time.Since(startTime)
			}
		}

		statusStr := "OK"
		if db.Error != nil && !errors.Is(db.Error, gorm.ErrRecordNotFound) {
			statusStr = "ERROR"
		}
		metrics.RecordDB(table, operation, statusStr, duration)

		if v, ok := db.InstanceGet(gormSpanKey); ok {
			if span, valid := v.(oteltrace.Span); valid && span != nil {
				span.SetAttributes(
					attribute.String("db.system", "gorm"),
					attribute.String("db.operation", operation),
					attribute.String("db.sql.table", table),
					attribute.Int64("db.rows_affected", db.RowsAffected),
				)
				if db.Error != nil && !errors.Is(db.Error, gorm.ErrRecordNotFound) {
					span.RecordError(db.Error)
					span.SetStatus(codes.Error, db.Error.Error())
				} else {
					span.SetStatus(codes.Ok, "")
				}
				span.End()
			}
		}
	}
}
