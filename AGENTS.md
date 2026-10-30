# AGENTS.md - go-blade 工程规范与 Agent 协作指南

本文档定义 `go-blade` 微服务底座工程的架构演进阶段、代码规范与 Agent 协作准则。Agent 在此工程中执行设计、编码、测试与重构时，必须遵守本规范。

---

## 1. 工程现状与演进基线 (Current vs Target)

### 1.1 当前就绪基线 (Current Phase)
- **全局公共底座 (`pkg/`)**：已就绪独立模块 (`github.com/go-blade/pkg`)，包含统一响应 (`pkg/response`)、统一错误码 (`pkg/xerr`)、JWT 鉴权与令牌黑名单吊销 (`pkg/jwt`)、分布式动态雪花 ID (`pkg/snowflake`)、多租户 GORM 隔离插件 (`pkg/tenant`)、数据库连接池与自动审计/DataScope 行级数据权限/OpenTelemetry Tracing 插件 (`pkg/database`)、gRPC 拦截器与负载均衡/熔断/重试治理 (`pkg/rpc`)、出站 HTTP 客户端治理 (`pkg/httpclient`)、Nacos 服务发现与配置中心 (`pkg/discovery`, `pkg/config`)、全链路追踪与防高基数指标监控 (`pkg/trace`, `pkg/metrics`)、标准健康检查探针 (`pkg/health`)、统一生命周期与优雅停机编排 (`pkg/lifecycle`)、泛型防击穿缓存 (`pkg/cache`)、结构化参数校验 (`pkg/validator`)、分布式互斥锁 (`pkg/lock`)、双模限流器 (`pkg/ratelimit`)、多租户对象存储 (`pkg/oss`)、可插拔多驱动分布式消息队列与事务发件箱 (`pkg/mq`)、异步领域事件总线 (`pkg/event`)、支持标准 Cron 表达式的分布式定时任务调度 (`pkg/job`) 及敏感字段脱敏操作审计日志 (`pkg/auditlog`)。
- **契约中心 (`api/`)**：已就绪独立模块 (`github.com/go-blade/api`)，定义并生成 `api/proto/auth/v1`、`api/proto/system/v1` 与 `api/proto/desk/v1` 等 gRPC/Protobuf 契约代码。
- **核心微服务 (`services/`)**：已落地 `services/auth`（认证中心）、`services/gateway`（统一 HTTP/gRPC 网关）、`services/system`（系统管理与 RBAC/DataScope 权限服务）、`services/desk`（协同工作台与通知公告服务）。
- **根目录工作区 (`go.work`)**：当前已统一纳入 `./api`、`./pkg`、`./services/auth`、`./services/desk`、`./services/gateway`、`./services/system` 六大模块。
- **本地检查脚本**：提供 `scripts/check.ps1` 与 `scripts/check.sh`，统一对全部模块执行 `gofmt`、`go vet` 与 `go test`。

### 1.2 目标演进架构 (Target Architecture)
- **契约优先 (API-First)**：在 `api/proto/<domain>/v1` (Protobuf/gRPC) 与 `api/openapi/` 下持续演进接口契约，通过自动化工具生成传输代码。
- **微服务集合 (`services/`)**：在已落地的 `auth`、`system`、`desk`、`gateway` 基础上，持续演进更多业务域服务。服务间交互严格通过契约 RPC，服务内部模型与持久化强制收敛于 `internal/`。

---

## 2. 目录规范与职责边界

参考社区流行的工程组织范式 (Standard Go Project Layout) 与 go-zero 最佳实践：

| 目录路径 | 职责边界 | 规范与约束 |
| :--- | :--- | :--- |
| `pkg/` | 全局基础通用底座公共库 | 纯通用技术库，严禁包含特定业务服务的业务模型或私有逻辑 |
| `scripts/` | 构建、静态检查与代码生成脚本 | 跨平台统一命令入口 |
| `deploy/` | 本地与线上编排部署清单 | `docker-compose/` 用于中间件联调，`k8s/` 用于生产部署 |
| `api/` *(演进中)* | 接口契约中心 | Protocol Buffers (gRPC) 契约与 OpenAPI 规范 |
| `services/<service>/` *(演进中)* | 微服务业务工程 | 业务逻辑强制收敛于 `internal/`，`cmd/` 保持薄层入口 |

---

## 3. 核心底座组件与编码约定

在开发任何底层或业务逻辑时，必须复用 `pkg/` 底座，禁止另起炉灶：

### 3.1 统一响应与异常收敛 (`pkg/response`, `pkg/xerr`)
- **HTTP 响应格式**：所有接口返回值必须符合 `pkg/response.Response[T]` 与 `pkg/response.IPage[T]` 分页结构。
- **异常收敛**：Handler 层统一使用 `pkg/response.ResultResponse(ctx, w, resp, err)` 处理返回。
- **安全防护**：未捕获的内部系统异常由 `ResultResponse` 在服务端打点追踪日志，向客户端仅输出模糊化安全提示，禁止将原始堆栈或 `err.Error()` 直出前端。
- **业务错误码**：在 `pkg/xerr` 中声明，抛出业务异常必须使用 `xerr.NewCodeError(code, msg)`。

### 3.2 整数精度与分布式动态雪花算法 ID (`pkg/snowflake`)
- **防精度截断**：凡需向前端 JavaScript 交互的 int64 雪花 ID，实体字段必须使用 `pkg/snowflake.ID`。
- **序列化规范**：`snowflake.ID` 自动在 JSON 序列化为 string（如 `"1782390182301"`），反序列化兼容 string 与 number。
- **动态节点分配**：支持通过 `snowflake.InitAutoNode(ctx, serviceName)` 或 `snowflake.AllocateNodeID(ctx, serviceName)` 动态分配 0~1023 节点编号（优先基于 Redis `INCR` 原子取模分配，Redis 不可用时自动降级为本机 Hostname + IP + PID + 服务名的 FNV-1a 哈希取模），消除多副本硬编码 `NodeID` 碰撞隐患。

### 3.3 多租户隔离 (`pkg/tenant`)
- **请求头规范**：网关及上游 HTTP 请求头传递 `Blade-Tenant-Id`（缺省默认为 `"000000"`）。
- **上下文字段**：使用 `pkg/tenant.FromTenant(ctx)` 与 `pkg/tenant.WithTenant(ctx, tenantID)` 读写租户。
- **GORM 插件机制**：凡包含 `tenant_id` 列的业务表，由 `TenantPlugin` 自动注入与过滤（`blade_tenant` 租户元数据表自动豁免，支持跨租户查询）。插件支持单条与批量切片模型插入。

### 3.4 登录态、JWKS 双模鉴权与 Token 黑名单吊销 (`pkg/jwt`)
- **Claims 载荷**：使用 `pkg/jwt.BladeClaims` 对齐 SpringBlade 字段（`tenant_id`, `user_id`, `dept_id`, `role_id`, `role_name`, `account`, `user_name`）。
- **命名规范**：遵循 Go 惯用规范，缩写保持全大写（`TenantID`、`UserID`、`DeptID`、`RoleID`）。
- **双 Token 与类型隔离**：支持 `access_token` 与 `refresh_token`；`ParseToken` 强制校验令牌类型为访问令牌，刷新令牌通过 `RefreshTokenCtx` 基于 Redis Lua 脚本实现单次消费轮换防重放。
- **Token 黑名单主动吊销**：提供 `jwt.RevokeToken`、`jwt.RevokeTokenWithClaims` 与 `jwt.IsTokenRevoked`（以令牌 SHA-256 摘要作为键，支持 Redis + 进程内内存双模兜底）；`ParseTokenCtx` 与 `CompositeValidator` 自动拦截已吊销令牌并返回 `ErrTokenRevoked`，网关与认证服务在 `/blade-auth/oauth/logout` 登出时实时吊销令牌。

### 3.5 数据库、自动审计与 DataScope 行级数据权限 (`pkg/database`)
- **连接初始化**：通过 `pkg/database.InitGorm(cfg)` 初始化连接，`Config` 具备零值安全补全机制，并自动挂载 `TenantPlugin`、`AuditPlugin` 与 `DataScopePlugin`。
- **GORM 自动审计 (`AuditPlugin`)**：在 Create / Update 阶段自动从上下文 `BladeClaims` 提取身份，填充 `create_user`、`create_dept`、`update_user`、`create_time`、`update_time`、`status` 与 `is_deleted` 等标准审计字段（不覆盖显式非零赋值）。
- **GORM 行级数据权限 (`DataScopePlugin`)**：对标 SpringBlade 五级数据权限（`1:全部数据`、`2:自定义部门`、`3:本级部门`、`4:本级及子部门`、`5:仅本人数据`），通过 `database.WithDataScope(ctx, scope)` 注入上下文，在查询阶段自动追加部门（`dept_id` / `create_dept`）或本人（`create_user`）行级过滤条件，并与多租户条件叠加。
- **日志适配**：定制 `GormLogger` 接入 `go-zero` 的 `logx` 链路追踪系统。

### 3.6 统一配置与 Nacos 配置中心 (`pkg/config`, `pkg/discovery`)
- **双模加载机制**：`pkg/config.LoadConfig(ctx, file, target)` 默认优先加载本地 YAML 文件；若结构体包含 `NacosConfig`，则自动从 Nacos 配置中心拉取并无缝覆盖。
- **动态监听与热更新**：通过 `pkg/config.NewLoader().ListenConfig(nc, onChange)` 监听动态配置推送。
- **隔离规范**：遵循 Nacos Namespace（多环境隔离）与 Group（业务域/服务分组隔离）。

### 3.7 gRPC 全链路追踪、负载均衡与熔断重试治理 (`pkg/rpc`, `pkg/trace`, `pkg/metrics`)
- **上下文与可观测透传**：`ClientUnaryInterceptor` 与 `ServerUnaryInterceptor` 自动透传 W3C `traceparent`、租户标识与 `BladeClaims` 用户身份 Metadata，自动创建 OpenTelemetry Client/Server Span 并调用 `metrics.RecordGRPC` 采集指标。
- **负载均衡与高可用弹性 (`DialWithConfig`)**：默认启用 `round_robin` 轮询负载均衡策略；内置并发安全的三态自适应熔断器 (`CircuitBreaker`：`Closed` / `Open` / `HalfOpen`，仅统计 `Unavailable`、`DeadlineExceeded`、`Internal`、`ResourceExhausted` 等系统级故障，忽略业务状态码) 与瞬态 `codes.Unavailable` 自动重试。

### 3.8 标准健康检查探针 (`pkg/health`)
- **双协议探针**：提供 `health.Checker` / `health.Manager`，同时支持 HTTP 存活与就绪探针（`/healthz/live`、`/healthz/ready`）及标准 gRPC 健康检查协议（`grpc.health.v1.Health`）。支持注册 MySQL、Redis 等依赖的 `PingChecker`，任一依赖故障时就绪探针返回 HTTP `503 ServiceUnavailable` 或 gRPC `NOT_SERVING`。

### 3.9 泛型防击穿缓存 (`pkg/cache`)
- **双模自适应与防击穿**：`cache.Cache[T]` 支持 Redis 与进程内内存 TTL Map 双模自动回退；`GetOrLoad` 内置进程内 Singleflight 并发请求归并机制，防止热点 Key 失效瞬间高并发击穿底层数据库。

### 3.10 结构化参数校验、分布式锁、双模限流与对象存储 (`pkg/validator`, `pkg/lock`, `pkg/ratelimit`, `pkg/oss`)
- **参数校验 (`pkg/validator`)**：零外部依赖结构体标签校验器（支持 `required`, `min`, `max`, `len`, `email`, `phone`, `oneof` 及嵌套结构体校验），结合 `label` 标签返回 `*xerr.CodeError(400)` 友好中文提示。
- **分布式互斥锁 (`pkg/lock`)**：基于 Redis `SetnxEx` + Lua 原子脚本实现唯一持有者 Token 校验解锁与原子续期 (`Refresh`)，Redis 未初始化时自动降级为进程内并发安全内存锁。
- **双模限流器 (`pkg/ratelimit`)**：支持 Redis 毫秒级固定窗口 Lua 脚本与进程内内存回退限流，`RateLimitMiddleware` 默认按 `<tenantID>:<path>:<clientIP>` 维度限流，超限返回 HTTP `429 TooManyRequests`。
- **多租户对象存储 (`pkg/oss`)**：提供统一 `Provider` 接口（支持 `local`、`memory`、`s3`、`minio` 驱动），通过 `BuildTenantObjectKey` 强制按 `<tenant_id>/<YYYYMMDD>/<snowflake_id>_<filename>` 隔离存储并校验扩展名白名单与路径穿越。

### 3.11 异步领域事件总线、可插拔多驱动消息队列、分布式定时任务与脱敏操作审计日志 (`pkg/event`, `pkg/mq`, `pkg/job`, `pkg/auditlog`)
- **可插拔多驱动消息队列 (`pkg/mq`)**：定义统一 `mq.Broker` 契约与 `RegisterDriver` 扩展机制，默认内置 **Redis Streams + Memory 双模自适应驱动** 并提供 **Kafka (`DriverKafka`)** 驱动实现；消息信封自动跨进程透传 `TenantID` 与 W3C `traceparent`；支持集群消费组（`WithGroup`）与全实例广播消费（`WithBroadcast`，用于多副本网关 RBAC 权限缓存实时失效 `blade.system.rbac.changed`）；内置指数退避重试、`<topic>.dlq` 死信转发、`WithDelay` 分布式延迟消息及 GORM 本地事务发件箱（`ExecuteInTx` / `OutboxRelay` -> `blade_mq_outbox`）。
- **异步领域事件总线 (`pkg/event`)**：`AsyncBus` 基于缓冲队列与并发 Worker 池实现，发布时自动提取并跨协程还原 `TenantID`、`TraceID` 与 OpenTelemetry `SpanContext`，内置单消费者 Panic 恢复隔离与 `Close(ctx)` 优雅排空（Graceful Drain），并提供 `MQBusAdapter` 与 `pkg/mq` 无缝桥接。
- **分布式定时任务调度 (`pkg/job`)**：`Scheduler` 同时支持按固定周期（`RegisterInterval`）与标准 5/6 段 Cron 表达式及 `@daily`/`@hourly`/`@every` 宏（`RegisterCron`）调度任务，默认结合 `pkg/lock`（键前缀 `blade:job:lock:<name>`）防止多副本集群在同一周期重复执行，内置超时控制与 Panic 恢复。
- **出站 HTTP 客户端与优雅停机编排 (`pkg/httpclient`, `pkg/lifecycle`)**：`httpclient.Client` 自动注入 W3C `traceparent` 与 `Blade-Tenant-Id`、采集 Prometheus 指标并内置三态熔断器与退避重试；`lifecycle.Manager` 按流量摘除、服务停机、后台 Worker 停止、MQ 排空与可观测刷盘五阶段编排 K8s 优雅退出。
- **字段级透明加密、结构体脱敏与 SSE 实时推送 (`pkg/crypto`, `pkg/sse`)**：`crypto.EncryptedString` 提供 GORM `AES-256-GCM` 透明落盘加密与 `BlindIndex` 盲索引，`crypto.MaskStruct` 支持按 `mask` 标签脱敏响应结构体；`sse.Hub` 提供多租户隔离的 Server-Sent Events 实时推送（对接 `/blade-desk/notice/stream`）。
- **敏感字段脱敏操作审计日志 (`pkg/auditlog`)**：对标 SpringBlade `blade_log_api`，`MaskJSON` 自动递归脱敏密码、密钥、Token、验证码（替换为 `"******"`）及手机号、身份证号；`AuditMiddleware` 无损采集写请求并通过 `MQRecorder` 异步投递至 `"blade.audit.api"` 主题，由 `services/system` 消费持久化至 `blade_log_api` 并支持 `/blade-system/log/api/list` 分页查询。


---

## 4. 开发工作流与静态检查红线

### 4.1 本地测试与质量检查
```powershell
# Windows 环境运行统一检查 (gofmt, go vet, go test)
.\scripts\check.ps1

# Linux / macOS 环境
./scripts/check.sh
```

### 4.2 质量红线
1. 任何新增或修改代码必须通过 `gofmt -l`、`go vet ./...` 与 `go test -v ./...` 检查。
2. 业务函数首参数必须显式传递 `context.Context`。
3. **注释规范 (Godoc Standard)**：
   - 每个包的 package 语句上方必须提供**包级注释 (Package Comment)**。
   - 所有首字母大写的**导出标识符（struct, interface, func, const, var）**必须编写完整中文注释，且以该标识符名称开头。

---

## 5. Agent 编码行为准则

1. **查证驱动**：任何假设或对现有架构的引用必须查验实际代码与文件，区分“已实现”和“规划中”。
2. **测试保障**：对 `pkg/` 底座或核心业务提交修改时，必须配套编写 `*_test.go` 并执行通过。
3. **渐进式重构**：修改应原子化，优先修复底层安全隐患与核心库并发隐患。

---

## Agent skills

### Issue tracker

Issues are tracked as local markdown files under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Domain docs

Single-context layout (`GLOSSARY.md` at root, ADRs in `docs/adr/`). See `docs/agents/domain.md`.

