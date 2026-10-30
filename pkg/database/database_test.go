package database

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/go-blade/pkg/metrics"
	"github.com/go-blade/pkg/trace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

func TestConfigDefaults(t *testing.T) {
	cfg := Config{}
	cfg.applyDefaults()

	if cfg.Host != "127.0.0.1" || cfg.Port != 3306 || cfg.User != "root" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.TablePrefix != "blade_" {
		t.Fatalf("expected TablePrefix to be 'blade_', got '%s'", cfg.TablePrefix)
	}
	if cfg.EnableTenant == nil || !*cfg.EnableTenant {
		t.Fatalf("expected EnableTenant default to be true")
	}
	if cfg.EnableAudit == nil || !*cfg.EnableAudit {
		t.Fatalf("expected EnableAudit default to be true")
	}
	if cfg.SlowThreshold != 200*time.Millisecond {
		t.Fatalf("expected SlowThreshold to be 200ms, got %v", cfg.SlowThreshold)
	}
}

func TestGormLogger(t *testing.T) {
	l := NewGormLogger(100*time.Millisecond, gormlogger.Info)
	if l.SlowThreshold != 100*time.Millisecond {
		t.Fatalf("unexpected slowThreshold: %v", l.SlowThreshold)
	}

	l2 := l.LogMode(gormlogger.Warn)
	if l2.(*GormLogger).LogLevel != gormlogger.Warn {
		t.Fatalf("unexpected LogLevel: %v", l2.(*GormLogger).LogLevel)
	}

	ctx := context.Background()
	// 验证日志打印无 panic
	l.Info(ctx, "test info: %s", "arg")
	l.Warn(ctx, "test warn: %s", "arg")
	l.Error(ctx, "test error: %s", "arg")
	l.Trace(ctx, time.Now().Add(-50*time.Millisecond), func() (string, int64) {
		return "SELECT 1", 1
	}, nil)
}

type sampleTracedEntity struct {
	ID   int64  `gorm:"primaryKey"`
	Name string `gorm:"size:64"`
}

func (sampleTracedEntity) TableName() string {
	return "blade_traced_entity"
}

func TestTracingPlugin_OTelSpanAndMetrics(t *testing.T) {
	tp, err := trace.InitTracer(trace.Config{ServiceName: "test-gorm-tracing"})
	require.NoError(t, err)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Use(NewTracingPlugin()))
	require.NoError(t, db.AutoMigrate(&sampleTracedEntity{}))

	parentCtx, parentSpan := trace.StartSpan(context.Background(), "http.handler")
	defer parentSpan.End()

	require.NoError(t, db.WithContext(parentCtx).Create(&sampleTracedEntity{ID: 101, Name: "trace-item"}).Error)

	var found sampleTracedEntity
	require.NoError(t, db.WithContext(parentCtx).First(&found, 101).Error)
	assert.Equal(t, "trace-item", found.Name)

	// 验证 Prometheus 已采集到 blade_traced_entity 的 create 与 query 指标
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.MetricsHandler()(rec, req)
	body := rec.Body.String()
	assert.Contains(t, body, `blade_db_queries_total{operation="create",status="OK",table="blade_traced_entity"} 1`)
	assert.Contains(t, body, `blade_db_queries_total{operation="query",status="OK",table="blade_traced_entity"} 1`)
}
