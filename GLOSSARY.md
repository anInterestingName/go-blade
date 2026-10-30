# go-blade 领域术语表 (Glossary)

本文档统一定义 `go-blade` 微服务底座工程中的核心领域概念，供开发者与 Agent 参考对齐。

---

## 1. 认证与安全 (Auth & JWT)
- **BladeUser / BladeClaims**：对齐 SpringBlade 的 JWT 载荷声明实体，包含 `tenant_id`、`user_id`、`dept_id`、`role_id`、`role_name`、`account`、`user_name` 等元数据。
- **Access Token**：短期访问凭证（默认有效 2 小时），类别标记为 `access_token`，用于携带用户上下文访问业务 API。
- **Refresh Token**：长期刷新凭证（默认有效 24 小时），类别标记为 `refresh_token`，仅用于在换发接口获取新的一组双 Token。
- **Token Blacklist (令牌黑名单吊销)**：基于令牌 SHA-256 摘要（`blade:jwt:blacklist:<sha256>`）存储于 Redis 与进程内内存的即时吊销机制，用于支撑主动登出（`/blade-auth/oauth/logout`）或强制下线场景下的令牌实时失效。
- **Ory Hydra**：独立的无头 (Headless) OAuth 2.1 与 OpenID Connect (OIDC) 授权服务器基础设施，仅负责协议流、客户端与非对称密钥管理。
- **Login / Consent Challenge**：Hydra 登录与授权流程的核心挑战机制。Hydra 生成唯一 challenge 串重定向至业务服务，经业务校验并注入 `BladeClaims` 后回调 Hydra Admin API 完成换票。
- **JWKS (JSON Web Key Set)**：存储公钥集合的标准 JSON 格式端点（如 `/.well-known/jwks.json`），供网关和资源服务器动态拉取以校验 Access Token 签名。

## 2. 多租户与数据权限 (Multi-Tenancy & DataScope)
- **TenantID (租户标识)**：系统多租户隔离主键，顶级管理租户编号固定为 `"000000"`。
- **Blade-Tenant-Id**：网关及 HTTP 请求头中传递租户编号的标准键名。
- **TenantPlugin**：GORM 插件，自动对包含 `tenant_id` 字段的数据表执行单条/批量赋值及 CRUD 作用域过滤。
- **AuditPlugin (自动审计插件)**：GORM 插件，在 Create/Update 阶段自动从上下文 `BladeClaims` 提取身份，填充 `create_user`、`create_dept`、`update_user`、`create_time`、`update_time`、`status` 与 `is_deleted`。
- **DataScope (行级数据权限)**：对标 SpringBlade 的五级行级数据可见性规则（`1:全部数据`、`2:自定义部门`、`3:本级部门`、`4:本级及子部门`、`5:仅本人数据`），由 `DataScopePlugin` 在查询阶段自动追加 `dept_id` 或 `create_user` 过滤条件。

## 3. 分布式与精度 (Snowflake & ID)
- **NodeID (节点编号)**：雪花算法分布式机器节点编号（取值范围 0-1023），支持通过 `InitAutoNode` 基于 Redis 原子自增取模或主机进程指纹哈希动态分配，避免多副本扩容时的 ID 冲突。
- **snowflake.ID**：自定义 64 位整型类型，JSON 序列化为 string（如 `"1782390182301"`），避免前端 JS `Number.MAX_SAFE_INTEGER` 截断。

## 4. 传输协议、高可用治理与响应 (Response, RPC & Resilience)
- **Response[T] (统一响应)**：对标 SpringBlade `R<T>`，包含 `code`, `success`, `msg`, `data` 四大核心字段。
- **IPage[T] (分页结构)**：包含 `records`, `total`, `size`, `current`, `pages`，无缝对接 Saber 前端表格。
- **ResultCode / CodeError**：对标 SpringBlade 的统一业务错误码体系。
- **CircuitBreaker (熔断器)**：gRPC 客户端三态自适应熔断器（`Closed` / `Open` / `HalfOpen`），仅对系统级故障（如 `Unavailable`、`DeadlineExceeded`、`Internal`）计数熔断，对业务错误码豁免。
- **Singleflight (并发请求归并)**：`pkg/cache` 内置的防缓存击穿机制，当热点缓存 Key 失效时，同一进程内的高并发未命中请求仅执行一次底层回源加载并共享结果。

## 5. 可观测性、事件驱动与运维治理 (Observability, Event & Audit)
- **Health Probe (健康检查探针)**：`pkg/health` 提供的存活（`/healthz/live`）与就绪（`/healthz/ready`）HTTP 探针及 `grpc.health.v1.Health` 标准协议实现。
- **EventBus (异步领域事件总线)**：`pkg/event` 提供的并发 Worker 池事件发布/订阅组件，支持跨协程自动保持 `TenantID` 与 `TraceID` 上下文、优雅关机排空（Graceful Drain）及与 `pkg/mq` 的桥接适配。
- **MQ Broker (可插拔多驱动消息队列)**：`pkg/mq` 提供的统一消息队列契约，默认内置 `Redis Streams + Memory` 双模自适应驱动并支持 `Kafka` 等可插拔驱动；原生支持 `WithGroup` 集群竞争消费、`WithBroadcast` 全实例广播消费、`<topic>.dlq` 死信队列、`WithDelay` 延迟投递及 GORM 本地事务发件箱（`Transactional Outbox`）。
- **Distributed Scheduler (分布式定时任务调度)**：`pkg/job` 提供的结合 `pkg/lock` 分布式互斥锁的周期与标准 Cron 表达式（5/6 段及 `@daily`/`@every` 宏）任务调度器，保障多实例集群下同一周期仅单节点执行。
- **Outbound HTTP Client (`pkg/httpclient`)**：统一出站 HTTP 治理客户端，内置 OpenTelemetry `traceparent` 与 `Blade-Tenant-Id` 自动注入、Prometheus 指标采集、三态熔断器与指数退避重试。
- **Lifecycle Manager (`pkg/lifecycle`)**：多阶段优雅停机编排器，按 `PhaseTrafficDrain` -> `PhaseServerStop` -> `PhaseBackgroundWorker` -> `PhaseMessageQueue` -> `PhaseObservability` 顺序编排资源安全释放。
- **AuditLog (操作审计日志)**：`pkg/auditlog` 对标 `blade_log_api` 的写操作审计组件，内置 `MaskJSON` 对密码、密钥、Token 及手机号进行自动掩码脱敏，并支持通过 `MQRecorder` 异步投递至 `services/system` 持久化落库。
- **EncryptedString & MaskStruct (`pkg/crypto`)**：基于 `AES-256-GCM` 的 GORM 透明字段加解密类型、HMAC-SHA256 盲索引（`BlindIndex`）与基于结构体 `mask` 标签的响应脱敏工具。
- **SSE Hub (`pkg/sse`)**：多租户隔离的 Server-Sent Events 实时消息流中心，支撑通知公告等实时事件跨网关推送到前端。
