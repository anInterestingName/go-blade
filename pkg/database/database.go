// Package database 提供 GORM 数据库连接配置与多租户驱动支持。
package database

import (
	"fmt"
	"time"

	"github.com/go-blade/pkg/tenant"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

// Config 数据库数据源配置结构体。
type Config struct {
	// DSN 自定义数据源连接串（若配置则优先使用）
	DSN string `json:",optional"`
	// Host 数据库主机地址
	Host string `json:",default=127.0.0.1"`
	// Port 数据库监听端口
	Port int `json:",default=3306"`
	// User 数据库认证用户名
	User string `json:",default=root"`
	// Password 数据库认证密码
	Password string `json:",optional"`
	// Database 目标数据库名称
	Database string `json:",optional"`
	// TablePrefix 统一数据表前缀（默认 "blade_"）
	TablePrefix string `json:",default=blade_"`
	// MaxIdleConns 连接池最大空闲连接数
	MaxIdleConns int `json:",default=10"`
	// MaxOpenConns 连接池最大允许打开的连接总数
	MaxOpenConns int `json:",default=100"`
	// ConnMaxLifetime 连接存活的最长时间
	ConnMaxLifetime time.Duration `json:",default=1h"`
	// SlowThreshold 慢查询记录阈值（超过该时长判定为慢 SQL）
	SlowThreshold time.Duration `json:",default=200ms"`
	// LogLevel 日志输出级别 ("silent", "error", "warn", "info")
	LogLevel string `json:",default=info"`
	// EnableTenant 是否启用多租户数据自动物理隔离插件
	// 注意：为保证 Config 零值与默认规范一致，当为 nil 时默认视为 true。
	EnableTenant *bool `json:",default=true"`
	// EnableAudit 是否启用 GORM 自动审计与行级数据权限插件
	// 注意：为保证 Config 零值与默认规范一致，当为 nil 时默认视为 true。
	EnableAudit *bool `json:",default=true"`
}

// applyDefaults 补全 Config 的默认值，确保无论是通过反序列化还是直接代码实例化均保持行为一致。
func (c *Config) applyDefaults() {
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.Port == 0 {
		c.Port = 3306
	}
	if c.User == "" {
		c.User = "root"
	}
	if c.TablePrefix == "" {
		c.TablePrefix = "blade_"
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 10
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 100
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = time.Hour
	}
	if c.SlowThreshold == 0 {
		c.SlowThreshold = 200 * time.Millisecond
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.EnableTenant == nil {
		defaultTenant := true
		c.EnableTenant = &defaultTenant
	}
	if c.EnableAudit == nil {
		defaultAudit := true
		c.EnableAudit = &defaultAudit
	}
}

// GetDSN 解析并生成符合 MySQL 规范的 DSN 数据库连接字符串。
func (c *Config) GetDSN() string {
	if c.DSN != "" {
		return c.DSN
	}
	c.applyDefaults()
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		c.User, c.Password, c.Host, c.Port, c.Database)
}

// InitGorm 初始化 GORM 数据库连接实例，挂载链路日志适配器并按需挂载租户隔离插件。
func InitGorm(c Config) (*gorm.DB, error) {
	c.applyDefaults()

	logLevel := gormlogger.Info
	switch c.LogLevel {
	case "silent":
		logLevel = gormlogger.Silent
	case "error":
		logLevel = gormlogger.Error
	case "warn":
		logLevel = gormlogger.Warn
	}

	customLogger := NewGormLogger(c.SlowThreshold, logLevel)

	db, err := gorm.Open(mysql.Open(c.GetDSN()), &gorm.Config{
		Logger: customLogger,
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   c.TablePrefix,
			SingularTable: true, // 不自动添加复数 s
		},
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	sqlDB.SetMaxIdleConns(c.MaxIdleConns)
	sqlDB.SetMaxOpenConns(c.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(c.ConnMaxLifetime)

	// 如果开启多租户隔离，挂载 TenantPlugin
	if c.EnableTenant != nil && *c.EnableTenant {
		if err := db.Use(tenant.NewTenantPlugin()); err != nil {
			return nil, fmt.Errorf("failed to register tenant plugin: %w", err)
		}
	}

	// 如果开启自动审计与数据权限，挂载 AuditPlugin 与 DataScopePlugin
	if c.EnableAudit != nil && *c.EnableAudit {
		if err := db.Use(NewAuditPlugin()); err != nil {
			return nil, fmt.Errorf("failed to register audit plugin: %w", err)
		}
		if err := db.Use(NewDataScopePlugin()); err != nil {
			return nil, fmt.Errorf("failed to register datascope plugin: %w", err)
		}
	}

	// 默认挂载 OpenTelemetry 全链路追踪与 SQL Prometheus 指标采集插件
	if err := db.Use(NewTracingPlugin()); err != nil {
		return nil, fmt.Errorf("failed to register tracing plugin: %w", err)
	}

	return db, nil
}
