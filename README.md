# go-blade — 云原生企业级 Go 微服务底座

`go-blade` 是对标 **SpringBlade / Saber** 企业级规范的云原生 Go 微服务开发平台（基于 **Go Workspace + gRPC/Protobuf 契约优先 + GORM + Nacos + OpenTelemetry** 构建）。

全栈内置**多租户物理隔离、五级 DataScope 行级数据权限、JWKS/HMAC 双模认证与 Token 实时黑名单吊销、可插拔多驱动消息队列（Redis Streams + Memory 双模 & Kafka）与事务发件箱（Transactional Outbox）、全链路追踪（HTTP -> gRPC -> GORM/MQ）、SSE 实时推送及敏感数据透明加解密/脱敏**。

完整的开发、联调、组件使用和生产部署说明见 [《go-blade 框架使用手册》](docs/manual/USER_MANUAL.md)。

---

## 1. 工程模块结构 (`go.work`)

| 模块路径 | 职责说明 |
| :--- | :--- |
| `pkg/` | **全局通用技术底座**：包含 23 个纯技术无业务耦合的基础组件，支持 Redis/外部中间件与进程内内存双模自适应降级 |
| `api/` | **契约中心**：统一管理 `auth/v1`、`system/v1`、`desk/v1` Protobuf 与生成的 gRPC 代码 |
| `services/gateway/` | **统一 API 网关**：负责 CORS、OpenTelemetry 追踪、防高基数 Metrics、双模限流、防重放幂等、JWT/JWKS 鉴权、RBAC 权限守卫、操作审计脱敏采集、SSE 实时推送及 gRPC 协议转换 |
| `services/auth/` | **认证授权中心**：支持账号密码 + 图形验证码登录、Refresh Token Lua 原子单次轮换防重放、Token 黑名单吊销及 Ory Hydra OAuth 2.1 / OIDC 登录授权挑战流 |
| `services/system/` | **系统管理与权限中心**：提供租户 (`blade_tenant`)、用户 (`blade_user`)、部门树 (`blade_dept`)、角色与自定义数据权限 (`blade_role`)、菜单按钮 (`blade_menu`)、字典 (`blade_dict`)、系统动态参数 (`blade_param`) 与审计日志 (`blade_log_api`) 管理 |
| `services/desk/` | **协同工作台中心**：提供多租户通知公告 (`blade_notice`) 管理及基于 MQ + SSE 的跨网关实时公告推送 |
| `deploy/` | **生产交付清单**：包含 `sql/`（11 张核心表 Schema 与种子数据）、`docker-compose/`（MySQL/Redis/Nacos/Jaeger/Prometheus/MinIO/Kafka/Hydra）及 `k8s/` 生产编排清单 |

---

## 2. `pkg/` 核心底座组件矩阵

| 组件包 | 核心特性 |
| :--- | :--- |
| `pkg/response` & `pkg/xerr` | 对标 SpringBlade `Response[T]` 与 `IPage[T]` 分页结构；内部异常服务端打点、前端模糊化脱敏防泄露 |
| `pkg/snowflake` | `snowflake.ID` 自动 JSON 字符串序列化防前端 JS 精度丢失；`InitAutoNode` 支持 Redis 原子分配 + 本机指纹哈希双模分配 NodeID (0~1023) |
| `pkg/tenant` | `TenantPlugin` 在 GORM CRUD 阶段自动注入并过滤 `tenant_id`（`blade_tenant` 与 `blade_mq_outbox` 自动豁免） |
| `pkg/database` | 自动挂载 `TenantPlugin`、`AuditPlugin`（自动填充 `create_user`/`create_dept` 等）、`DataScopePlugin`（五级行级数据权限：`1:全部`/`2:自定义部门`/`3:本级部门`/`4:本级及子部门`/`5:仅本人`）与 `TracingPlugin`（GORM OpenTelemetry Span + SQL 指标） |
| `pkg/jwt` | 双 Token 类型隔离、Refresh Token Redis Lua 单次消费轮换、Ory Hydra JWKS RSA 动态公钥验签、`RevokeToken` 实时黑名单吊销 |
| `pkg/rpc` & `pkg/httpclient` | gRPC 与出站 HTTP 全链路自动透传 W3C `traceparent`、租户与用户身份；内置三态自适应熔断器 (`CircuitBreaker`) 与瞬态指数退避重试 |
| `pkg/mq` & `pkg/event` | 可插拔多驱动消息队列（默认 `Redis Streams + Memory` 双模，内置 `Kafka` 驱动）；支持 `WithGroup` 集群竞争消费与 `WithBroadcast` 全实例广播消费、`<topic>.dlq` 死信队列、`WithDelay` 延迟投递及 GORM `Transactional Outbox` (`blade_mq_outbox`) |
| `pkg/cache` | 泛型双模缓存 `Cache[T]`，内置进程内 `Singleflight` 并发请求归并防止热点 Key 失效击穿 |
| `pkg/lock` & `pkg/ratelimit` | Redis Lua 原子持有者校验锁/续期 + 内存锁回退；Redis 毫秒级固定窗口 + 内存双模限流器 |
| `pkg/job` | 分布式定时任务调度器，支持 `RegisterInterval` 与标准 5/6 段 Cron 表达式 (`RegisterCron`)，默认结合分布式锁防多副本重跑 |
| `pkg/auditlog` & `pkg/crypto` | `MaskJSON` 写请求敏感字段递归脱敏并经 MQ 异步落盘 `blade_log_api`；`AES-256-GCM` GORM `EncryptedString` 透明落盘加密、HMAC 盲索引与 `MaskStruct` 响应标签脱敏 |
| `pkg/sse` | 多租户与用户级路由隔离的 Server-Sent Events 实时流推送中心 |
| `pkg/health` & `pkg/lifecycle` | HTTP (`/healthz/live`, `/healthz/ready`) + gRPC 标准健康探针；5 阶段有序 K8s 优雅停机编排器 |

---

## 3. 快速验证与启动

### 3.1 运行全量静态检查与单元测试（无需外部容器依赖）
```powershell
# Windows PowerShell
.\scripts\check.ps1

# Linux / macOS
./scripts/check.sh
```

### 3.2 启动本地中间件栈与容器构建
```bash
# 启动 MySQL 8.0, Redis 7.0, Nacos, Jaeger, Prometheus, MinIO, Kafka KRaft
docker compose -f deploy/docker-compose/docker-compose.yml up -d

# 构建微服务镜像
docker build --build-arg SERVICE_NAME=gateway -t go-blade/service-gateway:latest .

# 部署至 Kubernetes 集群
kubectl apply -f deploy/k8s/
```
