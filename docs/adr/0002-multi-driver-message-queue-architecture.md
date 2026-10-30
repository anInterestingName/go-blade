# ADR 0002: 采用可插拔多驱动消息队列架构（Redis Streams + Memory 默认双模 & Kafka 扩展）

- **状态**: 已采纳 (Accepted)
- **日期**: 2026-10-27

---

## 1. 背景与上下文 (Context)

在 `go-blade` 微服务底座演进过程中，跨微服务的异步解耦与事件驱动存在两大核心场景：
1. **点对点队列削峰消费（Queue / Consumer Group 模式）**：例如 `services/gateway` 采集并脱敏的 API 操作审计日志（`blade.audit.api`），需要异步削峰投递给 `services/system` 集群消费落库，同一条审计记录只能被消费入库一次。
2. **全实例广播消费（Broadcast 模式）**：例如当管理员在 `services/system` 修改角色菜单权限（`GrantRoleMenus`）或用户信息时，需要通过 `"blade.system.rbac.changed"` 事件实时通知所有 `services/gateway` 副本清空本地/二级权限缓存（`PermCache`）。

同时，工程底座面临以下约束：
- **开发与单测轻量性**：`scripts/check.ps1` 要求随时在不依赖重型外部容器的条件下秒级完成全部单元测试。
- **生产级高吞吐与可插拔扩展**：中小规模部署希望直接复用已有的 `Redis 7.0`（零额外中间件运维成本），而大规模高吞吐日志/事件流场景需要无缝切换至 `Apache Kafka`（或未来扩展 `RocketMQ` / `NATS`）。
- **事务最终一致性与延迟投递**：Redis Streams 与 Kafka 均不支持绑定外部关系型数据库（MySQL）的原地两阶段半消息事务，需要通用机制保证业务 DB 写入与消息发送的原子性。

---

## 2. 决策 (Decision)

我们在 `pkg/mq` 中设计并实现 **面向契约的可插拔多驱动消息队列底座**：

1. **统一 `mq.Broker` 契约与驱动注册表 (`RegisterDriver` / `NewBroker`)**：
   - 统一封装 `Publish(ctx, topic, payload, opts...)` 与 `Subscribe(topic, handler, opts...)`。
   - 消息信封 (`mq.Message`) 原生在 Headers 中自动提取并跨进程还原 `TenantID`（多租户标识）、`TraceID` 与 OpenTelemetry W3C `traceparent`。
2. **默认 `Redis Streams + Memory` 双模自适应驱动 (`driver: "auto"` / `"redis"` / `"memory"`)**：
   - 当全局 Redis 已初始化时，采用 Redis Streams（`XADD` / `XGROUP CREATE ... MKSTREAM` / `XREADGROUP` / `XACK`）实现持久化流处理与消费组负载均衡。
   - 当未配置 Redis 或处于轻量单测时，自动无缝降级为进程内并发安全 `MemoryBroker`。
3. **内置 `Kafka` 可插拔驱动 (`driver: "kafka"`) 与 KRaft 容器编排**：
   - 提供支持按 Key FNV-1a 一致性哈希分区、Record Headers 透传、集群消费组与广播动态消费组的 `KafkaBroker`。
   - 在 `deploy/docker-compose/docker-compose.yml` 中采用免 ZooKeeper 的 **Apache Kafka 3.8 KRaft 单节点模式**。
4. **内置消费弹性治理（重试 + `<topic>.dlq` 死信队列）**：
   - 消费端统一由 `ExecuteHandlerWithRetry` 守护，捕获单消费者 Panic，按指数退避执行重试；超过 `MaxRetries` 后自动将消息连同原始 Topic 和错误原因投递至 `<topic>.dlq`。
5. **GORM 本地事务发件箱 (`Transactional Outbox`) 与双模延迟调度 (`WithDelay`)**：
   - 提供 `mq.ExecuteInTx` / `mq.PublishInTx` 将消息写入同库 `blade_mq_outbox` 表，由后台 `OutboxRelay` 结合 `pkg/lock` 分布式锁轮询可靠投递。
   - 提供基于 Redis ZSet（回退内存队列）的 `mq.WithDelay(duration)` 延迟投递支持。

---

## 3. 影响与后果 (Consequences)

- **正面影响**：
  - 业务代码（`services/gateway`、`services/system`、`services/desk`）仅依赖 `pkg/mq.Broker` 接口，切换 `memory` / `redis` / `kafka` 只需修改一行配置。
  - 单元测试无需启动外部 Kafka/RocketMQ 容器即可 100% 验证队列竞争、全节点广播、失败死信、延迟消息与事务发件箱回滚语义。
- **权衡约束**：
  - 使用 `WithBroadcast()` 广播模式时，底层通过为每个实例生成唯一消费组后缀（`<group>-broadcast-<snowflake_id>`）实现，适用于实例数量可控的微服务集群广播场景。
