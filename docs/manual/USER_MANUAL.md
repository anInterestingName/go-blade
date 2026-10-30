# go-blade 框架使用手册

> 版本基线：仓库当前实现（Go Workspace、Go 1.26、gRPC/Protobuf、GORM、Nacos、OpenTelemetry）。
> 本手册中的命令均从仓库根目录执行。接口、配置和部署清单以当前代码为准；新增业务能力时，请同步更新 api/proto/、对应服务和本手册。

## 目录

1. [架构总览与快速上手](#1-架构总览与快速上手)
2. [从零开发一个微服务或业务模块](#2-从零开发一个微服务或业务模块)
3. [pkg 核心底座组件使用指南](#3-pkg-核心底座组件使用指南)
4. [内置核心微服务详解](#4-内置核心微服务详解)
5. [生产部署、运维与工程规范](#5-生产部署运维与工程规范)
6. [常见问题](#6-常见问题)

---

## 1. 架构总览与快速上手

### 1.1 框架定位

go-blade 是一个契约优先的 Go 微服务底座，设计目标是让服务拥有 SpringBlade/Saber 类似的企业级约定，同时保留 Go 的单体部署效率和显式依赖边界。

| 层次 | 当前实现 | 作用 |
| --- | --- | --- |
| 工作区 | go.work | 将 api、pkg 和四个服务模块放在同一工作区内开发和检查 |
| 契约 | Protobuf + gRPC | api/proto/<domain>/v1 统一定义服务间接口 |
| HTTP | go-zero REST | 网关负责鉴权、协议转换、统一响应和流量治理 |
| 持久化 | GORM + MySQL | 租户隔离、审计字段、DataScope 和 SQL tracing 通过插件注入 |
| 配置与发现 | 本地 YAML + Nacos | 本地配置可直接启动，配置了 Nacos 时支持远程覆盖和服务注册 |
| 可观测性 | OpenTelemetry + Jaeger + Prometheus | HTTP、gRPC、GORM 和消息链路共享 TraceID，并暴露指标 |
| 弹性 | Redis/外部中间件 + 内存回退 | 缓存、锁、限流、消息、雪花节点等组件在依赖不可用时可降级到进程内实现 |

双模降级适合本地开发、单元测试和短时依赖故障。生产环境仍应使用 Redis、MySQL、消息队列和对象存储，并通过健康检查和监控确认依赖恢复，避免把内存回退误当成跨实例一致性方案。

### 1.2 工程目录

~~~text
go-blade/
├── api/                         # 独立契约模块 github.com/go-blade/api
│   └── proto/{auth,system,desk}/v1/
├── pkg/                         # 独立公共底座模块 github.com/go-blade/pkg
│   ├── response/ xerr/ validator/ tenant/
│   ├── database/ jwt/ middleware/ ratelimit/
│   ├── rpc/ discovery/ config/ trace/ metrics/
│   ├── cache/ lock/ oss/ mq/ event/ job/ sse/
│   └── auditlog/ crypto/ health/ lifecycle/ httpclient/
├── services/
│   ├── gateway/                 # HTTP 统一网关
│   ├── auth/                    # 认证、Token、Hydra challenge
│   ├── system/                  # 租户、用户、RBAC、DataScope、审计日志
│   └── desk/                    # 通知公告和 SSE 事件源
├── deploy/
│   ├── sql/                     # 01_schema.sql、02_seed_data.sql
│   ├── docker-compose/          # 本地中间件编排
│   └── k8s/                     # Namespace、Deployment、Service、HPA
├── scripts/                     # check、Protobuf 生成脚本
├── docs/                        # ADR、Agent 文档和本使用手册
├── go.work
└── Dockerfile
~~~

服务内部遵循 cmd/ 薄入口、internal/config 配置、internal/handler HTTP 接入、internal/server gRPC 实现、internal/logic 业务逻辑、internal/model 持久化模型、internal/svc 依赖上下文的边界。服务之间只通过 api 中的 RPC 契约通信，禁止跨服务引用对方的 internal 包。

### 1.3 5 分钟快速启动：零依赖模式

安装 Go 1.26 或更高版本并进入仓库根目录，然后运行：

~~~powershell
.\scripts\check.ps1
~~~

Linux/macOS：

~~~bash
./scripts/check.sh
~~~

脚本按 api、pkg、auth、gateway、system、desk 顺序执行 gofmt、go vet 和 go test；api 只有契约同步检查，不运行测试。

不依赖 Redis、MySQL 或 Nacos 时，可直接启动网关验证探针：

~~~powershell
go run ./services/gateway/cmd/api -f ./services/gateway/etc/gateway.yaml
Invoke-RestMethod http://127.0.0.1:8080/ping
Invoke-WebRequest http://127.0.0.1:8080/healthz/live
Invoke-WebRequest http://127.0.0.1:8080/healthz/ready
~~~

pkg 中的缓存、锁、限流、MQ、雪花节点和事件总线会在外部客户端未初始化时使用内存实现。该模式用于开发和测试，不提供跨进程数据共享。

### 1.4 完整中间件联调

~~~bash
docker compose -f deploy/docker-compose/docker-compose.yml up -d
~~~

基础编排包含 MySQL 8.0（3306）、Redis 7（6379）、Nacos（8848/9848）、Jaeger（16686、4317、4318）、Prometheus（9090）、MinIO（9000/9001）和 Kafka KRaft（9092）。MySQL 会自动执行 deploy/sql/ 目录中的 01_schema.sql 与 02_seed_data.sql。

如需 Ory Hydra OAuth2/OIDC：

~~~bash
docker compose -f deploy/docker-compose/docker-compose.yml -f deploy/docker-compose/docker-compose.hydra.yaml up -d
~~~

Hydra 使用 PostgreSQL 5432，公共 API 为 4444，管理 API 为 4445。开发编排使用 DEV=true 和 HTTP 地址；上线前必须替换密钥、启用 TLS，并重新配置 issuer、login、consent 和 logout URL。

服务默认端口：

~~~powershell
go run ./services/auth/cmd/api -f ./services/auth/etc/auth.yaml       # HTTP 8888，gRPC 9001
go run ./services/system/cmd/api -f ./services/system/etc/system.yaml # HTTP 8889，gRPC 9002
go run ./services/desk/cmd/api -f ./services/desk/etc/desk.yaml       # HTTP 8890，gRPC 9003
go run ./services/gateway/cmd/api -f ./services/gateway/etc/gateway.yaml # HTTP 8080
~~~

默认种子账号为 admin，密码为 admin123，租户为 000000。验证码是动态图片，自动化测试应使用测试夹具或替换验证码依赖，不能把固定验证码写入生产脚本。

### 1.5 请求约定

- 租户请求头为 Blade-Tenant-Id，缺省值为 000000。网关会把租户上下文透传到 gRPC Metadata；服务内部使用 tenant.FromTenant(ctx) 读取。
- 访问令牌可放在 Blade-Auth: <access_token>，也兼容 Authorization: Bearer <access_token>。
- JSON 字段采用 snake_case。登录、刷新等少数入口同时兼容 camelCase，客户端统一使用 snake_case 可减少差异。
- 所有业务 HTTP 返回统一为 {"code":200,"success":true,"msg":"操作成功","data":...}；分页数据位于 data.records、data.total、data.size、data.current、data.pages。
- 雪花 ID 在网关 JSON 中序列化为字符串，前端必须按字符串保存和传递，不要转换成 JavaScript Number。
- 响应头可能包含 X-Trace-Id，排查问题时将它与日志、Jaeger 查询条件一起提供。

---

## 2. 从零开发一个微服务或业务模块

下面以新增 report 领域为例，名称可替换为实际业务域。

### Step 1：契约先行

在 api/proto/report/v1/report.proto 定义消息和服务。字段优先使用稳定的基础类型，ID 使用 int64，HTTP 输出由网关视图对象转换成字符串。

~~~protobuf
syntax = "proto3";
package report.v1;
option go_package = "github.com/go-blade/api/proto/report/v1;reportv1";

message GetReportRequest { int64 id = 1; }
message Report { int64 id = 1; string tenant_id = 2; string title = 3; }
message GetReportResponse { Report report = 1; }

service ReportService {
  rpc GetReport(GetReportRequest) returns (GetReportResponse);
}
~~~

生成代码：

~~~powershell
.\scripts\gen_proto.ps1
~~~

~~~bash
./scripts/gen_proto.sh
~~~

脚本需要 protoc、protoc-gen-go 和 protoc-gen-go-grpc。生成后检查 report.pb.go 和 report_grpc.pb.go 是否与 .proto 在同一目录，并运行 gofmt。不要直接修改生成文件。

### Step 2：模型、租户列和仓储

业务表默认包含 tenant_id、标准审计字段、status 和 is_deleted。主键使用 snowflake.ID。模型的 GORM 标签应声明主键、列名、租户索引和审计列，并提供 TableName 返回 blade_report。

~~~go
type Report struct {
    ID         snowflake.ID
    TenantID   string
    Title      string
    CreateUser snowflake.ID
    CreateDept snowflake.ID
    CreateTime time.Time
    UpdateUser snowflake.ID
    UpdateTime time.Time
    Status     int16
    IsDeleted  int16
}

func (Report) TableName() string { return "blade_report" }
~~~

使用 database.InitGorm(database.Config{...}) 初始化数据库。默认会挂载租户、审计和 DataScope 插件；只有明确理解跨租户用途时才关闭插件。

敏感字段使用 crypto.EncryptedString；需要等值查询时另外保存 crypto.BlindIndex(明文) 的结果到索引列。生产启动时调用 crypto.SetGlobalKey 注入来自 Secret 的主密钥。更换密钥需要规划密文迁移，盲索引也会随密钥变化。

### Step 3：业务逻辑和 gRPC 服务

每个业务函数第一个参数显式接收 context.Context，并从上下文取得租户、Claims 和 Trace。业务可预期错误使用 xerr.NewCodeError：

~~~go
func (l *ReportLogic) Get(ctx context.Context, id int64) (*Report, error) {
    tenantID := tenant.FromTenant(ctx)
    var report Report
    err := l.db.WithContext(ctx).
        Where("tenant_id = ? AND id = ?", tenantID, id).
        First(&report).Error
    if errors.Is(err, gorm.ErrRecordNotFound) {
        return nil, xerr.NewCodeError(xerr.DataNotExist, "报告不存在")
    }
    if err != nil {
        return nil, err
    }
    return &report, nil
}
~~~

gRPC 服务实现应把业务错误转换为 xerr.ToGrpcError(err)，网关再由 response.ResultResponse 还原业务码；未知错误只向客户端返回模糊化的 500 文案。

服务入口应复用 rpc.NewServer、health.NewChecker、health.RegisterGRPCHealthServer 和 lifecycle.NewManager。退出时先摘流量，再停止 HTTP/gRPC，最后关闭 MQ 和观测资源。

### Step 4：网关路由与中间件

网关新增路由时同步完成四件事：定义 HTTP 路由和请求/响应 VO；解析 JSON 或 query 参数并调用 validator.ValidateStruct；调用下游 gRPC 客户端并用 response.ResultResponse 输出；对写请求配置 RBAC 权限码，并确认审计中间件能够读取和恢复请求体。

当前中间件执行顺序为：CORS → Trace → Metrics → RateLimit（启用时）→ Idempotent → JWT/JWKS Auth → 权限缓存加载 → Audit。自定义中间件要明确放置位置，避免在鉴权之前读取用户 Claims。

~~~go
type SaveReportRequest struct {
    Title string
}

if err := validator.ValidateStruct(r.Context(), &req); err != nil {
    response.ResultResponse(r.Context(), w, nil, err)
    return
}
~~~

输出前使用 crypto.MaskStruct 或 mask 标签，禁止将密码、密钥、Token、验证码和未脱敏身份证号写入响应或审计日志。

---

## 3. pkg 核心底座组件使用指南

### 3.1 响应、错误和参数校验

~~~go
page := response.NewIPage(records, total, current, size)
response.ResultResponse(ctx, w, page, nil)
return xerr.NewCodeError(xerr.DataAlreadyExist, "报告标题已存在")
~~~

常用错误码：200 成功、400 参数错误、401 未授权、403 无权限、404 不存在、500 内部错误；业务码包括 40001 Token 过期、40002 Token 无效、40003 租户不存在、40004 用户不存在、40005 密码错误、40006 验证码错误、40007 数据不存在、40008 数据已存在。不要在 Handler 中直接返回 err.Error()，ResultResponse 会记录未知错误并只输出安全文案。

validator 支持 required、min、max、len、email、phone、oneof，并递归校验嵌套结构体和切片。label 优先于 json 作为错误提示字段名：

~~~go
type Login struct {
    Account string
    Email   string
    Phone   string
    Status  int
}
err := validator.ValidateStruct(ctx, &Login{Account: ""})
~~~

### 3.2 租户、数据库和数据安全

请求头：

~~~http
Blade-Tenant-Id: 100001
~~~

服务内部：

~~~go
ctx = tenant.WithTenant(ctx, "100001")
tenantID := tenant.FromTenant(ctx) // 未设置时为 "000000"
~~~

GORM TenantPlugin 会在带 tenant_id 的模型查询中追加过滤条件，在创建和批量创建时填充租户；blade_tenant 和 blade_mq_outbox 由组件设计为跨租户管理表。跨租户后台任务必须显式设计，不能通过拼接 SQL 绕过插件。

~~~go
db, err := database.InitGorm(database.Config{
    Host: "127.0.0.1", Port: 3306,
    User: "root", Password: "root", Database: "blade",
    MaxOpenConns: 100, SlowThreshold: 200 * time.Millisecond,
})
~~~

AuditPlugin 在 Create/Update 阶段从 jwt.FromClaims(ctx) 填充创建人、创建部门、更新人和时间字段，仅填充零值，不覆盖显式值。DataScopePlugin 支持：

| 值 | 语义 |
| --- | --- |
| 1 | 全部数据 |
| 2 | 自定义部门，使用部门 ID 列表 |
| 3 | 本级部门 |
| 4 | 本级及子部门 |
| 5 | 仅本人（create_user） |

需要下推数据权限时使用 database.WithDataScope(ctx, scope)。模型应保留 dept_id 或 create_dept、create_user，否则对应范围无法生成有效过滤条件。TracingPlugin/GormLogger 会记录 SQL span 和慢查询，敏感参数不要写进日志。

~~~go
if _, err := snowflake.InitAutoNode(ctx, "service-report"); err != nil {
    return err
}
id := snowflake.NextID()
~~~

有 Redis 时通过 blade:snowflake:node:<service> 的 INCR 分配 0~1023 节点；Redis 不可用时使用主机名、IP、PID 和服务名的 FNV-1a 指纹。进程启动时尽早初始化节点。

~~~go
_ = crypto.SetGlobalKey(os.Getenv("BLADE_CRYPTO_MASTER_KEY"))
ciphertext, _ := crypto.Encrypt("secret")
plaintext, _ := crypto.Decrypt(ciphertext)
index := crypto.BlindIndex("13800138000")
~~~

EncryptedString 实现 GORM driver.Valuer/sql.Scanner，落库时 AES-256-GCM 加密，读取时透明解密；BlindIndex 是 HMAC-SHA256 等值索引。mask 标签可配合 MaskStruct 做响应脱敏。加密列不能直接用于普通 SQL 等值查询，应查询盲索引列。

### 3.3 认证、鉴权和流量治理

令牌载荷使用 jwt.BladeClaims，字段包括 TenantID、UserID、DeptID、RoleID、RoleName、Account、UserName 和 Token category。登录签发 access_token 与 refresh_token；刷新由 Redis Lua 脚本单次消费，旧 Refresh Token 不能重复使用；登出调用 jwt.RevokeToken，网关后续解析会立即拒绝黑名单令牌。

Hydra 模式使用 JWKSValidator 拉取 /.well-known/jwks.json，HMAC 本地模式使用 JWTHandler。生产网关推荐 CompositeValidator，先验签 JWKS，再按策略回退 HMAC；两种密钥的 issuer、过期时间和 Token 类型都必须校验。

~~~http
GET /blade-system/user/info HTTP/1.1
Host: 127.0.0.1:8080
Blade-Tenant-Id: 000000
Blade-Auth: eyJ...
~~~

网关内置 CORS、Auth、RBAC 和 Idempotent 中间件。写请求必须有匹配按钮权限，例如 user:add、role:grant；重复请求可由幂等中间件按请求键短时间拦截。不要把幂等键设计为全局固定值。

~~~go
limiter := ratelimit.NewLimiter(ratelimit.Config{
    Rate: 100, Window: time.Minute,
})
server.Use(ratelimit.RateLimitMiddleware(limiter, nil))
~~~

超限响应为 HTTP 429。多副本生产环境必须确认 Redis 已连接，否则每个副本的内存窗口彼此独立。

### 3.4 配置、服务发现和通信治理

~~~go
var cfg Config
if err := config.LoadConfig(ctx, "etc/service.yaml", &cfg); err != nil {
    return err
}
~~~

配置结构体实现 GetNacos() config.NacosConfig 且包含有效 Nacos 配置时，LoadConfig 先读取本地 YAML，再使用 Nacos DataID/Group/Namespace 内容覆盖。通过 NewLoader().ListenConfig 注册热更新回调。动态回调只更新可变配置，连接池、监听端口和密钥等不可热切换项应重启服务。

discovery.NacosRegistry 负责注册/注销实例，RegisterNacosResolver 注册 gRPC Resolver，客户端目标可使用 nacos:///service-auth。服务关闭时先注销 Nacos，再停止服务器。

~~~go
conn, err := rpc.DialWithConfig(rpc.ClientConfig{
    Target: "127.0.0.1:9001", Timeout: 5000,
    MaxRetries: 2,
})
~~~

默认 round_robin；三态熔断器只统计 Unavailable、DeadlineExceeded、Internal、ResourceExhausted 等系统级故障，业务错误不会打开熔断。重试只针对瞬态 Unavailable，业务写操作仍需自行保证幂等。

~~~go
client := httpclient.NewClient(httpclient.Config{
    Timeout: 5 * time.Second, MaxRetries: 2,
})
req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
resp, err := client.Do(req)
~~~

出站请求自动注入 traceparent 和 Blade-Tenant-Id。对不可重试的 POST、支付和扣款请求应关闭自动重试或设置幂等键。

### 3.5 缓存、锁、对象存储

#### pkg/cache

~~~go
cache := cache.NewCache[User](cache.Options{DefaultTTL: 10 * time.Minute})
user, err := cache.GetOrLoad(ctx, "user:1001", 10*time.Minute, func(ctx context.Context) (User, error) {
    return loadUser(ctx, 1001)
})
~~~

GetOrLoad 使用进程内 singleflight 合并同一进程的并发 miss；Redis 存在时不同实例共享数据，Redis 不可用时只保存在本机。读取空值和删除键时同时清理两种存储。

#### pkg/lock

~~~go
lock, err := lock.Acquire(ctx, "report:generate:1001", 30*time.Second)
if err != nil {
    return err
}
defer lock.Unlock(context.Background())
~~~

Redis 模式使用 SETNX EX 和 Lua 持有者校验解锁/续期；无 Redis 时使用进程内锁。必须保留持有者校验，禁止直接 DEL 他人锁；长任务要定期 Refresh 或设置足够 TTL。

#### pkg/oss

~~~go
provider, err := oss.NewProvider(oss.Config{
    Driver: "minio", Endpoint: "http://127.0.0.1:9000",
    AccessKey: "minioadmin", SecretKey: "minioadmin", BucketName: "blade",
})
key, err := oss.BuildTenantObjectKey(tenant.WithTenant(ctx, "100001"), "avatar.png")
~~~

支持 local、memory、s3、minio。对象键固定为 <tenant_id>/<YYYYMMDD>/<snowflake_id>_<filename>；组件会校验扩展名白名单、控制字符和路径穿越。不要接受客户端传入完整对象键，也不要把本地 BaseDir 直接暴露为静态目录。

### 3.6 事件、MQ、Outbox、任务和 SSE

#### pkg/mq

~~~go
broker, err := mq.NewBroker(mq.Config{Driver: mq.DriverAuto, ServiceName: "service-report"})
if err != nil {
    return err
}
defer broker.Close(context.Background())

_ = broker.Subscribe("report.created", func(ctx context.Context, msg mq.Message) error {
    return handleReportCreated(ctx, msg)
}, mq.WithGroup("report-workers"))

_ = broker.Publish(ctx, "report.created", payload, mq.WithDelay(5*time.Second))
~~~

DriverAuto 优先 Redis Streams，无 Redis 时回退 Memory；也可选择 memory、redis 或 kafka。WithGroup 是集群竞争消费，WithBroadcast 是每个实例都收到消息，适合网关权限缓存失效；失败消息进入 <topic>.dlq。消息信封自动透传 TenantID、TraceID 和 traceparent。

#### 本地事务发件箱

~~~go
err := mq.ExecuteInTx(ctx, db, "report.created", payload, func(tx *gorm.DB) error {
    if err := tx.Create(&report).Error; err != nil {
        return err
    }
    return nil
})
~~~

事务提交后消息写入 blade_mq_outbox，OutboxRelay 扫描并投递；事务回滚时不会产生脏消息。Relay 是最终一致性机制，消费者仍要幂等，并监控 pending、failed 和 retry_count。

#### pkg/event

~~~go
bus := event.NewAsyncBus(event.WithWorkers(4), event.WithBufferSize(256))
bus.Subscribe("report.created", func(ctx context.Context, msg event.Message) error {
    return notify(ctx, msg.Payload)
})
_ = bus.Publish(ctx, "report.created", report)
defer bus.Close(context.Background())
~~~

AsyncBus 会跨协程恢复租户、TraceID 和 SpanContext；单个消费者 panic 会被隔离并记录。服务关闭时使用带超时的 Close(ctx) 排空队列。需要跨进程时使用 MQBusAdapter 接入 pkg/mq。

#### pkg/job

~~~go
scheduler := job.NewScheduler()
_ = scheduler.RegisterCron("report-cleanup", "0 */5 * * * *", func(ctx context.Context) error {
    return cleanup(ctx)
}, job.WithTimeout(30*time.Second))
scheduler.Start(ctx)
defer scheduler.Stop()
~~~

支持固定间隔、5/6 段 Cron、@daily、@hourly 和 @every。默认使用 blade:job:lock:<name> 分布式锁，多个副本只有一个执行；任务 panic 会被恢复并记录。需要立即执行可调用 RunOnce。

#### pkg/sse

网关公告流为 GET /blade-desk/notice/stream，需要有效访问令牌。标准事件包含 id、event 和多行 data，连接建立后先发送 connected 事件。Hub 按租户隔离，也可按用户单播：

~~~go
hub.BroadcastToTenant("100001", sse.Event{
    Event: "notice.published",
    Data:  map[string]any{"id": noticeID},
})
~~~

SSE 连接必须处理断线重连和 Last-Event-ID 业务策略；Hub 本身只负责当前进程在线连接管理，跨网关广播应通过 MQ。

### 3.7 可观测性、审计和生命周期

- pkg/trace：HTTP、gRPC、出站 HTTP 和 MQ 统一使用 W3C traceparent；日志通过上下文关联 TraceID。
- pkg/metrics：暴露 /metrics，指标标签禁止使用用户 ID、完整 URL 或其他高基数字段。
- pkg/auditlog：写请求自动记录方法、路径、用户、租户、状态码、耗时和 TraceID；MaskJSON 递归处理 password、secret、token、captcha、手机号和身份证号。默认主题为 blade.audit.api，system 服务消费后落到 blade_log_api。
- pkg/health：/healthz/live 表示进程存活；/healthz/ready 会检查注册的 MySQL/Redis 等依赖，故障返回 503；同时提供 gRPC Health 协议。
- pkg/lifecycle：建议按 TrafficDrain → ServerStop → WorkerStop → MessageQueue → Observability 注册关闭动作，使用统一超时，避免直接 os.Exit 丢失消息和 span。

---

## 4. 内置核心微服务详解

### 4.1 services/gateway

网关默认端口 8080，负责 HTTP API、鉴权、RBAC、限流、幂等、审计、SSE 和 gRPC 协议转换。完整路由如下：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | /ping | 返回 pong |
| GET | /healthz/live、/healthz/ready | 存活/就绪探针 |
| GET | /metrics | Prometheus 指标 |
| GET | /blade-auth/oauth/captcha | 获取验证码 |
| POST | /blade-auth/oauth/token | 账密登录，签发双 Token |
| POST | /blade-auth/oauth/refresh-token | 单次消费 Refresh Token |
| GET | /blade-auth/oauth/validate-token | 校验当前 Access Token |
| GET/POST | /blade-auth/oauth/login | Hydra 登录 challenge 查询/接受 |
| POST | /blade-auth/oauth/consent | Hydra consent challenge |
| POST | /blade-auth/oauth/logout | Hydra 登出或本地 Token 吊销 |
| GET/POST | /blade-system/tenant/{info,list}、/submit、/remove | 租户管理 |
| GET/POST | /blade-system/user/{info,list}、/submit、/remove | 用户管理 |
| GET/POST | /blade-system/dept/tree、/submit、/remove | 部门管理 |
| GET/POST | /blade-system/role/list、/submit、/remove、/grant、/grant-datascope | 角色、菜单和 DataScope 授权 |
| GET/POST | /blade-system/menu/{routes,tree}、/submit、/remove | 动态路由和菜单 |
| GET/POST | /blade-system/dict/list、/submit、/remove | 字典 |
| GET | /blade-system/log/api/list | API 审计日志分页查询 |
| GET/POST | /blade-system/param/list、/submit、/remove | 系统参数 |
| GET/POST | /blade-desk/notice/{list,detail}、/submit、/remove | 通知公告 |
| GET | /blade-desk/notice/stream | 多租户 SSE 实时流 |
| POST/GET | /blade-resource/oss/endpoint/put-file、/presigned-url | 对象存储上传和预签名 URL |

写接口通过 requireWritePerm 校验权限，例如 user:add、dept:add、role:grant、notice:add。管理员更新角色菜单后，网关通过 blade.system.rbac.changed 广播让各副本清理权限缓存。

### 4.2 services/auth

认证服务 HTTP 默认 8888、gRPC 9001。登录顺序为：读取租户 → 校验验证码 → 查询用户 → bcrypt 校验密码 → 签发 Access/Refresh Token。/blade-auth/oauth/logout 可处理 Hydra logout challenge，也可直接吊销当前 Access Token。

Hydra 授权流：

1. Hydra 将 login_challenge 重定向到 /blade-auth/oauth/login；
2. auth 查询 challenge，若 skip=true 可直接接受；
3. 用户提交租户、账号、密码和验证码到 POST /blade-auth/oauth/login；
4. consent 页面提交 scope 到 POST /blade-auth/oauth/consent；
5. Hydra 按 issuer 和 JWKS 签发令牌，网关使用 JWKSValidator 验签。

### 4.3 services/system

system 服务 HTTP 默认 8889、gRPC 9002，负责 blade_tenant、blade_user、blade_dept、blade_role、blade_menu、blade_dict、blade_param 和 blade_log_api。角色数据权限类型与 pkg/database 的五级 DataScope 对齐；菜单按钮的 action 同时供前端按钮显隐和网关写权限校验使用。

### 4.4 services/desk

desk 服务 HTTP 默认 8890、gRPC 9003，负责 blade_notice 通知公告 CRUD。公告提交后通过 MQ 发布领域事件，网关订阅并按租户向 SSE 客户端发送 notice.published。同一事件不能跨租户投递，消费者应从消息信封恢复 TenantID 后再调用 BroadcastToTenant。

---

## 5. 生产部署、运维与工程规范

### 5.1 Docker 构建

根目录 Dockerfile 使用多阶段构建，SERVICE_NAME 决定编译哪个服务：

~~~bash
docker build --build-arg SERVICE_NAME=gateway -t go-blade/service-gateway:latest .
docker build --build-arg SERVICE_NAME=auth    -t go-blade/service-auth:latest .
docker build --build-arg SERVICE_NAME=system  -t go-blade/service-system:latest .
docker build --build-arg SERVICE_NAME=desk    -t go-blade/service-desk:latest .
~~~

运行镜像通过 /app/server -f /app/etc/<service>.yaml 启动。生产镜像应使用不可变 tag、非 root 用户、只读文件系统和外部 Secret；示例文件中的默认密钥只适用于本地联调。

### 5.2 Kubernetes

~~~bash
kubectl apply -f deploy/k8s/00-namespace-config.yaml
kubectl apply -f deploy/k8s/01-services-deployment.yaml
kubectl apply -f deploy/k8s/02-hpa.yaml
kubectl -n go-blade get pods,svc,hpa
~~~

清单包含 go-blade Namespace、公共 ConfigMap、四个服务 Deployment/Service 和 gateway/system HPA。Liveness 使用 /healthz/live，Readiness 使用 /healthz/ready，终止宽限期为 30 秒。上线前确认：

- 镜像中的 YAML 端口与 Deployment containerPort、Service targetPort 一致；仓库默认本地配置是 gateway 8080、auth 8888、system 8889、desk 8890，而清单示例使用 8000、8081、8082、8083，部署时必须通过镜像配置或清单统一；
- blade-common-env 的 Nacos、MySQL、Redis、Kafka、OTLP 地址能在集群 DNS 中解析；
- Prometheus scrape 端口与实际 metrics 监听端口一致；
- readiness 依赖的 MySQL/Redis 已准备好，避免 Pod 在依赖未就绪时接收流量；
- HPA 已安装 Metrics Server，并为每个容器设置 requests/limits；
- 优雅停机期间先摘除流量，再停止 gRPC/HTTP，最后排空 MQ 和刷新观测数据。

### 5.3 数据库初始化和迁移

01_schema.sql 创建核心表和索引，02_seed_data.sql 写入管理租户、角色、组织、管理员和菜单按钮。生产部署不要重复执行含种子数据的脚本来代替迁移工具；应将后续表结构变更写成带版本号的迁移，并在备份后执行。

生产环境至少修改：MySQL 密码、Redis 密码/TLS、Nacos 账号和 Namespace、Hydra DSN/密钥、JWT/HMAC Secret、加密主密钥、MinIO/S3 凭证、Kafka ACL 和 OTLP 认证。

### 5.4 质量红线

提交前运行：

~~~powershell
.\scripts\check.ps1
~~~

必须满足：

1. gofmt -l 无输出，所有模块 go vet ./... 和 go test -v ./... 通过；
2. 业务函数首参数为 context.Context，并向下游传递；
3. 每个包有包级注释，每个导出标识符有以名称开头的完整中文 Godoc 注释；
4. pkg/ 或核心服务的修改配套单元测试，涉及并发、租户、权限、重试、加密或降级时补充失败路径测试；
5. 不在日志、响应和审计中输出密码、密钥、Token、验证码或原始内部堆栈；
6. 新增 RPC 先修改 api/proto，再生成代码和实现服务；
7. 任何跨服务数据访问都通过契约 RPC 或消息，不直接引用其他服务的 internal 包。

---

## 6. 常见问题

### Q1：没有 Redis 能否启动？

可以。缓存、锁、限流、MQ、雪花节点和事件总线支持内存回退，但它们只在当前进程有效。多副本部署或需要可靠消息时必须修复 Redis/消息队列连接，而不是长期依赖回退。

### Q2：live 正常但 ready 返回 503 是什么原因？

存活探针只表示进程存在；就绪探针会执行已注册的依赖检查。查看服务日志和依赖连接配置，重点检查 MySQL、Redis、Nacos 地址、账号、TLS 和容器网络。

### Q3：为什么前端收到的 ID 是字符串？

雪花 ID 可能超过 JavaScript 安全整数上限。snowflake.ID 故意序列化为字符串，前端状态、路由参数和请求体都应保持字符串。

### Q4：gRPC 返回业务错误后网关变成 500？

服务端必须返回 xerr.ToGrpcError(err)，不要直接返回普通 fmt.Errorf；网关必须使用 response.ResultResponse。普通未知错误会被安全映射为 500。

### Q5：Refresh Token 第二次使用失败是否是异常？

不是。刷新采用 Redis Lua 单次消费轮换，第一次成功后旧 Refresh Token 会失效，用旧值重放应返回未授权或 Token 无效。

### Q6：多租户查询返回了其他租户数据怎么办？

确认请求头拼写为 Blade-Tenant-Id，服务没有用 context.Background() 丢失原上下文，模型包含 tenant_id，GORM 使用 InitGorm 并挂载了 TenantPlugin。跨租户管理表必须明确豁免并增加权限控制。

### Q7：Kubernetes Pod 一直重启或被标记为不就绪？

先检查 Deployment 端口和 YAML 端口是否一致。仓库本地配置与 K8s 示例端口不同，若未覆盖配置，探针会访问错误端口。再检查 ConfigMap 中依赖地址和服务日志。

### Q8：审计日志没有落库怎么办？

先确认网关已启用 AuditMiddleware，MQ broker 已初始化，blade.audit.api 消费者正在运行；然后检查 blade_mq_outbox、死信主题和 blade_log_api。审计是异步链路，查询时要允许短暂延迟，并监控失败重试次数。

