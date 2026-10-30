-- =============================================================================
-- go-blade 生产级数据库 Schema 初始化脚本 (MySQL 8.0+)
-- 字符集：utf8mb4，排序规则：utf8mb4_general_ci
-- =============================================================================

CREATE DATABASE IF NOT EXISTS blade DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE blade;

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- -----------------------------------------------------------------------------
-- 1. 租户表 (blade_tenant)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_tenant` (
  `id` bigint NOT NULL COMMENT '雪花主键 ID',
  `tenant_id` varchar(12) NOT NULL COMMENT '租户编号 (如 000000)',
  `tenant_name` varchar(50) NOT NULL COMMENT '租户名称',
  `linkman` varchar(20) DEFAULT '' COMMENT '联系人',
  `contact_number` varchar(20) DEFAULT '' COMMENT '联系电话',
  `address` varchar(255) DEFAULT '' COMMENT '联系地址',
  `create_user` bigint DEFAULT 0 COMMENT '创建人 ID',
  `create_dept` bigint DEFAULT 0 COMMENT '创建部门 ID',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` bigint DEFAULT 0 COMMENT '修改人 ID',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `status` smallint DEFAULT 1 COMMENT '状态 (1:正常, 2:冻结)',
  `is_deleted` smallint DEFAULT 0 COMMENT '是否已删除 (0:正常, 1:已删除)',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_tenant_id` (`tenant_id`),
  KEY `idx_tenant_is_deleted` (`is_deleted`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='系统租户表';

-- -----------------------------------------------------------------------------
-- 2. 用户表 (blade_user)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_user` (
  `id` bigint NOT NULL COMMENT '雪花主键 ID',
  `tenant_id` varchar(12) NOT NULL DEFAULT '000000' COMMENT '所属租户编号',
  `account` varchar(45) NOT NULL COMMENT '登录账号',
  `password` varchar(100) NOT NULL COMMENT '加密密码 (bcrypt)',
  `name` varchar(20) NOT NULL COMMENT '用户姓名/昵称',
  `real_name` varchar(20) DEFAULT '' COMMENT '真实姓名',
  `avatar` varchar(500) DEFAULT '' COMMENT '用户头像 URL',
  `email` varchar(45) DEFAULT '' COMMENT '电子邮箱',
  `phone` varchar(45) DEFAULT '' COMMENT '联系手机号',
  `dept_id` bigint DEFAULT 0 COMMENT '所属部门主键 ID',
  `role_id` varchar(255) DEFAULT '' COMMENT '所属角色主键列表 (多个以逗号分隔)',
  `create_user` bigint DEFAULT 0 COMMENT '创建人 ID',
  `create_dept` bigint DEFAULT 0 COMMENT '创建部门 ID',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` bigint DEFAULT 0 COMMENT '修改人 ID',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `status` smallint DEFAULT 1 COMMENT '账号状态 (1:正常, 2:冻结)',
  `is_deleted` smallint DEFAULT 0 COMMENT '是否已删除 (0:正常, 1:已删除)',
  PRIMARY KEY (`id`),
  KEY `idx_user_tenant_account` (`tenant_id`, `account`),
  KEY `idx_user_dept_id` (`dept_id`),
  KEY `idx_user_is_deleted` (`is_deleted`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='系统用户表';

-- -----------------------------------------------------------------------------
-- 3. 部门表 (blade_dept)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_dept` (
  `id` bigint NOT NULL COMMENT '雪花主键 ID',
  `tenant_id` varchar(12) NOT NULL DEFAULT '000000' COMMENT '所属租户编号',
  `parent_id` bigint DEFAULT 0 COMMENT '父级部门主键 ID (顶级为 0)',
  `dept_name` varchar(45) NOT NULL COMMENT '部门简称',
  `full_name` varchar(45) DEFAULT '' COMMENT '部门全称',
  `sort` int DEFAULT 0 COMMENT '排序权重',
  `remark` varchar(255) DEFAULT '' COMMENT '备注',
  `create_user` bigint DEFAULT 0 COMMENT '创建人 ID',
  `create_dept` bigint DEFAULT 0 COMMENT '创建部门 ID',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` bigint DEFAULT 0 COMMENT '修改人 ID',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `status` smallint DEFAULT 1 COMMENT '状态 (1:正常, 2:停用)',
  `is_deleted` smallint DEFAULT 0 COMMENT '是否已删除 (0:正常, 1:已删除)',
  PRIMARY KEY (`id`),
  KEY `idx_dept_tenant_parent` (`tenant_id`, `parent_id`),
  KEY `idx_dept_is_deleted` (`is_deleted`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='系统部门表';

-- -----------------------------------------------------------------------------
-- 4. 角色表 (blade_role)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_role` (
  `id` bigint NOT NULL COMMENT '雪花主键 ID',
  `tenant_id` varchar(12) NOT NULL DEFAULT '000000' COMMENT '所属租户编号',
  `parent_id` bigint DEFAULT 0 COMMENT '父级角色主键 ID (顶级为 0)',
  `role_name` varchar(50) NOT NULL COMMENT '角色名称',
  `role_alias` varchar(50) NOT NULL COMMENT '角色别名 (如 administrator, user)',
  `data_scope_type` int DEFAULT 1 COMMENT '数据权限类型 (1:全部数据, 2:自定义部门, 3:本级部门, 4:本级及子部门, 5:仅本人数据)',
  `data_scope_dept_ids` varchar(1000) DEFAULT '' COMMENT '自定义数据权限部门主键列表 (多个以逗号分隔，当 data_scope_type=2 时生效)',
  `sort` int DEFAULT 0 COMMENT '排序权重',
  `create_user` bigint DEFAULT 0 COMMENT '创建人 ID',
  `create_dept` bigint DEFAULT 0 COMMENT '创建部门 ID',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` bigint DEFAULT 0 COMMENT '修改人 ID',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `status` smallint DEFAULT 1 COMMENT '状态 (1:正常, 2:停用)',
  `is_deleted` smallint DEFAULT 0 COMMENT '是否已删除 (0:正常, 1:已删除)',
  PRIMARY KEY (`id`),
  KEY `idx_role_tenant_alias` (`tenant_id`, `role_alias`),
  KEY `idx_role_is_deleted` (`is_deleted`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='系统角色表';

-- -----------------------------------------------------------------------------
-- 5. 菜单与按钮权限表 (blade_menu)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_menu` (
  `id` bigint NOT NULL COMMENT '雪花主键 ID',
  `parent_id` bigint DEFAULT 0 COMMENT '父级菜单主键 ID (顶级为 0)',
  `code` varchar(255) NOT NULL COMMENT '菜单/按钮编码',
  `name` varchar(50) NOT NULL COMMENT '菜单/按钮名称',
  `alias` varchar(255) DEFAULT '' COMMENT '菜单别名',
  `path` varchar(255) DEFAULT '' COMMENT '前端路由路径',
  `component` varchar(255) DEFAULT '' COMMENT '前端组件路径',
  `icon` varchar(50) DEFAULT '' COMMENT '菜单图标',
  `sort` int DEFAULT 0 COMMENT '排序权重',
  `category` smallint DEFAULT 1 COMMENT '菜单类别 (1:菜单路由, 2:按钮权限)',
  `action` varchar(50) DEFAULT '' COMMENT '按钮权限操作标识 (如 system:user:add)',
  `create_user` bigint DEFAULT 0 COMMENT '创建人 ID',
  `create_dept` bigint DEFAULT 0 COMMENT '创建部门 ID',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` bigint DEFAULT 0 COMMENT '修改人 ID',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `status` smallint DEFAULT 1 COMMENT '状态 (1:正常, 2:停用)',
  `is_deleted` smallint DEFAULT 0 COMMENT '是否已删除 (0:正常, 1:已删除)',
  PRIMARY KEY (`id`),
  KEY `idx_menu_parent_id` (`parent_id`),
  KEY `idx_menu_code` (`code`),
  KEY `idx_menu_is_deleted` (`is_deleted`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='系统菜单与权限按钮表';

-- -----------------------------------------------------------------------------
-- 6. 角色菜单关联表 (blade_role_menu)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_role_menu` (
  `id` bigint NOT NULL COMMENT '雪花主键 ID',
  `role_id` bigint NOT NULL COMMENT '角色主键 ID',
  `menu_id` bigint NOT NULL COMMENT '菜单或按钮主键 ID',
  PRIMARY KEY (`id`),
  KEY `idx_role_menu_role_id` (`role_id`),
  KEY `idx_role_menu_menu_id` (`menu_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='角色与菜单权限关联表';

-- -----------------------------------------------------------------------------
-- 7. 系统字典表 (blade_dict)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_dict` (
  `id` bigint NOT NULL COMMENT '雪花主键 ID',
  `parent_id` bigint DEFAULT 0 COMMENT '父级字典主键 ID (顶级分类为 0)',
  `code` varchar(255) NOT NULL COMMENT '字典编码',
  `dict_key` varchar(255) DEFAULT '' COMMENT '字典键值 (顶级分类一般为 -1)',
  `dict_value` varchar(255) NOT NULL COMMENT '字典名称/显示值',
  `sort` int DEFAULT 0 COMMENT '排序权重',
  `remark` varchar(255) DEFAULT '' COMMENT '字典备注',
  `create_user` bigint DEFAULT 0 COMMENT '创建人 ID',
  `create_dept` bigint DEFAULT 0 COMMENT '创建部门 ID',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` bigint DEFAULT 0 COMMENT '修改人 ID',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `status` smallint DEFAULT 1 COMMENT '状态 (1:正常, 2:停用)',
  `is_deleted` smallint DEFAULT 0 COMMENT '是否已删除 (0:正常, 1:已删除)',
  PRIMARY KEY (`id`),
  KEY `idx_dict_parent_id` (`parent_id`),
  KEY `idx_dict_code` (`code`),
  KEY `idx_dict_is_deleted` (`is_deleted`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='系统数据字典表';

-- -----------------------------------------------------------------------------
-- 8. 通知公告表 (blade_notice)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_notice` (
  `id` bigint NOT NULL COMMENT '雪花主键 ID',
  `tenant_id` varchar(12) NOT NULL DEFAULT '000000' COMMENT '所属租户编号',
  `title` varchar(255) NOT NULL COMMENT '公告标题',
  `category` int DEFAULT 1 COMMENT '公告分类 (1:发布通知, 2:批转通知, 3:系统维护)',
  `release_time` varchar(64) DEFAULT '' COMMENT '发布时间',
  `content` text COMMENT '公告正文内容',
  `create_user` bigint DEFAULT 0 COMMENT '创建人 ID',
  `create_dept` bigint DEFAULT 0 COMMENT '创建部门 ID',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` bigint DEFAULT 0 COMMENT '修改人 ID',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `status` smallint DEFAULT 1 COMMENT '业务状态 (1:正常, 2:停用)',
  `is_deleted` smallint DEFAULT 0 COMMENT '是否已删除 (0:正常, 1:已删除)',
  PRIMARY KEY (`id`),
  KEY `idx_notice_tenant_category` (`tenant_id`, `category`),
  KEY `idx_notice_create_dept` (`create_dept`),
  KEY `idx_notice_is_deleted` (`is_deleted`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='协同工作台通知公告表';

-- -----------------------------------------------------------------------------
-- 9. API 操作审计日志表 (blade_log_api)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_log_api` (
  `id` bigint NOT NULL COMMENT '审计日志雪花主键 ID',
  `tenant_id` varchar(12) NOT NULL DEFAULT '000000' COMMENT '租户编号',
  `service_name` varchar(64) NOT NULL DEFAULT '' COMMENT '产生审计日志的服务名称',
  `user_id` bigint DEFAULT 0 COMMENT '操作用户主键 ID',
  `account` varchar(45) DEFAULT '' COMMENT '操作人登录账号',
  `method` varchar(16) NOT NULL DEFAULT '' COMMENT 'HTTP 请求方法 (POST/PUT/DELETE/PATCH)',
  `path` varchar(255) NOT NULL DEFAULT '' COMMENT 'HTTP 请求路径',
  `query_params` varchar(1024) DEFAULT '' COMMENT 'URL 查询参数字符串',
  `request_body` text COMMENT '敏感字段掩码脱敏后的请求体摘要',
  `status_code` int DEFAULT 200 COMMENT 'HTTP 响应状态码',
  `duration_ms` bigint DEFAULT 0 COMMENT '请求处理耗时 (毫秒)',
  `client_ip` varchar(64) DEFAULT '' COMMENT '客户端真实 IP 地址',
  `trace_id` varchar(64) DEFAULT '' COMMENT '分布式链路追踪 TraceID',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '审计记录生成时间',
  PRIMARY KEY (`id`),
  KEY `idx_log_api_tenant_created` (`tenant_id`, `created_at`),
  KEY `idx_log_api_user_id` (`user_id`),
  KEY `idx_log_api_trace_id` (`trace_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='API 操作审计日志表';

-- -----------------------------------------------------------------------------
-- 10. 分布式消息队列本地事务发件箱表 (blade_mq_outbox)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_mq_outbox` (
  `id` bigint NOT NULL COMMENT '发件箱记录唯一雪花主键 ID',
  `tenant_id` varchar(12) NOT NULL DEFAULT '000000' COMMENT '所属租户编号',
  `topic` varchar(128) NOT NULL COMMENT '目标消息主题名称',
  `message_key` varchar(128) DEFAULT '' COMMENT '消息路由/分区键',
  `payload` text COMMENT '消息载荷 JSON 文本',
  `headers` text COMMENT '跨进程透传消息头 JSON 文本 (含 traceparent 与 tenant_id)',
  `trace_id` varchar(64) DEFAULT '' COMMENT '分布式链路追踪 TraceID',
  `status` smallint DEFAULT 0 COMMENT '投递状态 (0:待投递, 1:已投递, 2:投递失败)',
  `retry_count` int DEFAULT 0 COMMENT '已重试次数',
  `max_retries` int DEFAULT 5 COMMENT '最大允许重试次数',
  `next_retry_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '下次重试或延迟到期投递时间',
  `error_msg` varchar(512) DEFAULT '' COMMENT '最后一次投递失败原因',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_outbox_status_next` (`status`, `next_retry_at`),
  KEY `idx_outbox_tenant_status` (`tenant_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='分布式消息队列本地事务发件箱表';

-- -----------------------------------------------------------------------------
-- 11. 系统动态参数表 (blade_param)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `blade_param` (
  `id` bigint NOT NULL COMMENT '参数雪花主键 ID',
  `tenant_id` varchar(12) NOT NULL DEFAULT '000000' COMMENT '所属租户编号',
  `param_name` varchar(100) NOT NULL COMMENT '参数名称',
  `param_key` varchar(100) NOT NULL COMMENT '参数键名 (如 account.initPassword)',
  `param_value` varchar(500) NOT NULL DEFAULT '' COMMENT '参数键值',
  `remark` varchar(255) DEFAULT '' COMMENT '参数备注',
  `create_user` bigint DEFAULT 0 COMMENT '创建人 ID',
  `create_dept` bigint DEFAULT 0 COMMENT '创建部门 ID',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_user` bigint DEFAULT 0 COMMENT '修改人 ID',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `status` smallint DEFAULT 1 COMMENT '状态 (1:正常, 2:停用)',
  `is_deleted` smallint DEFAULT 0 COMMENT '是否已删除 (0:正常, 1:已删除)',
  PRIMARY KEY (`id`),
  KEY `idx_param_tenant_key` (`tenant_id`, `param_key`),
  KEY `idx_param_is_deleted` (`is_deleted`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='系统动态参数表';

SET FOREIGN_KEY_CHECKS = 1;
