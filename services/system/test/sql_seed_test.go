// Package test 提供系统管理微服务与部署 SQL 脚本（Schema 与 Seed Data）的自动化校验测试。
package test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// TestDeploySQL_SchemaAndSeedValidation 校验 deploy/sql/01_schema.sql 与 deploy/sql/02_seed_data.sql 的表结构、种子数据完整性及 admin 账号 bcrypt 密码哈希。
func TestDeploySQL_SchemaAndSeedValidation(t *testing.T) {
	schemaPath := filepath.Join("..", "..", "..", "deploy", "sql", "01_schema.sql")
	seedPath := filepath.Join("..", "..", "..", "deploy", "sql", "02_seed_data.sql")

	schemaBytes, err := os.ReadFile(schemaPath)
	require.NoError(t, err, "读取 01_schema.sql 失败")
	schemaSQL := string(schemaBytes)

	seedBytes, err := os.ReadFile(seedPath)
	require.NoError(t, err, "读取 02_seed_data.sql 失败")
	seedSQL := string(seedBytes)

	// 1. 校验 01_schema.sql 中 9 张核心表的建表语句及关键列定义
	expectedTablesAndColumns := map[string][]string{
		"blade_tenant": {
			"id", "tenant_id", "tenant_name", "linkman", "contact_number", "address",
			"create_user", "create_dept", "create_time", "update_user", "update_time", "status", "is_deleted",
		},
		"blade_user": {
			"id", "tenant_id", "account", "password", "name", "real_name", "avatar", "email", "phone",
			"dept_id", "role_id", "create_user", "create_dept", "create_time", "update_user", "update_time", "status", "is_deleted",
		},
		"blade_dept": {
			"id", "tenant_id", "parent_id", "dept_name", "full_name", "sort", "remark",
			"create_user", "create_dept", "create_time", "update_user", "update_time", "status", "is_deleted",
		},
		"blade_role": {
			"id", "tenant_id", "parent_id", "role_name", "role_alias", "data_scope_type", "data_scope_dept_ids", "sort",
			"create_user", "create_dept", "create_time", "update_user", "update_time", "status", "is_deleted",
		},
		"blade_menu": {
			"id", "parent_id", "code", "name", "alias", "path", "component", "icon", "sort", "category", "action",
			"create_user", "create_dept", "create_time", "update_user", "update_time", "status", "is_deleted",
		},
		"blade_role_menu": {
			"id", "role_id", "menu_id",
		},
		"blade_dict": {
			"id", "parent_id", "code", "dict_key", "dict_value", "sort", "remark",
			"create_user", "create_dept", "create_time", "update_user", "update_time", "status", "is_deleted",
		},
		"blade_notice": {
			"id", "tenant_id", "title", "category", "release_time", "content",
			"create_user", "create_dept", "create_time", "update_user", "update_time", "status", "is_deleted",
		},
		"blade_log_api": {
			"id", "tenant_id", "service_name", "user_id", "account", "method", "path",
			"query_params", "request_body", "status_code", "duration_ms", "client_ip", "trace_id", "created_at",
		},
		"blade_mq_outbox": {
			"id", "tenant_id", "topic", "message_key", "payload", "headers", "trace_id",
			"status", "retry_count", "max_retries", "next_retry_at", "error_msg", "created_at", "updated_at",
		},
		"blade_param": {
			"id", "tenant_id", "param_name", "param_key", "param_value", "remark",
			"create_user", "create_dept", "create_time", "update_user", "update_time", "status", "is_deleted",
		},
	}

	for table, cols := range expectedTablesAndColumns {
		createStmt := "CREATE TABLE IF NOT EXISTS `" + table + "`"
		assert.Contains(t, schemaSQL, createStmt, "01_schema.sql 缺少表定义: %s", table)
		for _, col := range cols {
			assert.Contains(t, schemaSQL, "`"+col+"`", "表 %s 缺少列定义: %s", table, col)
		}
	}

	// 2. 校验 02_seed_data.sql 中核心业务表的 INSERT 语句完整性
	seedTables := []string{
		"blade_tenant",
		"blade_role",
		"blade_dept",
		"blade_user",
		"blade_menu",
		"blade_role_menu",
		"blade_dict",
		"blade_notice",
		"blade_param",
	}
	for _, table := range seedTables {
		assert.Contains(t, seedSQL, "INSERT IGNORE INTO `"+table+"`", "02_seed_data.sql 缺少表 %s 的种子数据 INSERT 语句", table)
	}

	// 3. 校验网关 requireWritePerm 所需的权限码及规范权限码在 02_seed_data.sql 中均已覆盖
	requiredActions := []string{
		"user:add", "user:delete",
		"dept:add", "dept:delete",
		"role:add", "role:delete", "role:grant",
		"menu:add", "menu:delete",
		"dict:add", "dict:delete",
		"tenant:add", "tenant:delete",
		"param:add", "param:delete",
		"notice:add", "notice:delete",
		"system:user:add", "system:user:edit", "system:user:remove", "system:role:grant",
		"desk:notice:view",
	}
	for _, act := range requiredActions {
		assert.Contains(t, seedSQL, "'"+act+"'", "02_seed_data.sql 缺少按钮权限码: %s", act)
	}

	// 4. 提取 02_seed_data.sql 中 admin 用户的 bcrypt 哈希字符串，使用 bcrypt.CompareHashAndPassword 校验明文 "admin123"
	reBcrypt := regexp.MustCompile(`\$2[aby]\$\d{2}\$[./A-Za-z0-9]{53}`)
	matches := reBcrypt.FindAllString(seedSQL, -1)
	require.NotEmpty(t, matches, "未能在 02_seed_data.sql 中提取到合法的 bcrypt 哈希字符串")

	adminHash := strings.TrimSpace(matches[0])
	err = bcrypt.CompareHashAndPassword([]byte(adminHash), []byte("admin123"))
	if err != nil {
		genHash, _ := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
		t.Fatalf("02_seed_data.sql 中 admin 密码哈希 (%s) 与 'admin123' 不匹配: %v (可用哈希示例: %s)", adminHash, err, string(genHash))
	}
}
