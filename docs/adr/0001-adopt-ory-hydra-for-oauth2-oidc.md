# ADR 0001: 采纳 Ory Hydra 作为独立的 OAuth 2.1 / OIDC 基础设施服务

- **状态**: 提议中 (Proposed)
- **日期**: 2026-10-23
- **决策人**: 架构团队、go-blade 核心维护者

---

## 1. 背景与上下文 (Context)

`go-blade` 作为现代化 Go 微服务底座，目标是支撑多租户、高并发的分布式企业级业务架构，并全面兼容现有前端交互与网关鉴权规范。

在安全与认证授权体系演进过程中，我们面临以下诉求与挑战：
1. **标准化协议诉求**：现代客户端架构（SPA 前端、移动端 App、小程序、第三方开放平台等）要求严格支持 OAuth 2.1（强制 PKCE、无隐式流、严格重定向校验）与 OpenID Connect (OIDC) 规范。
2. **避免重复造轮子**：手写完整的 OAuth 2.1 协议服务器（涵盖 Token 撤销 RFC 7009、Token 内省 RFC 7662、JWKS 密钥动态轮转、Discovery 端点 RFC 8414 等）复杂度高、漏洞风险大。
3. **数据与协议分离 (Headless 架构)**：通用的全家桶 IAM（如 Keycloak、Zitadel、Casdoor）通常强行捆绑特定的用户实体表结构、组织架构或其自带的 UI 前端，而 `go-blade` 已规划成熟的 `BladeClaims` 多租户用户数据体系（`pkg/tenant`、`pkg/jwt`），需要高度自主可控的账号与权限管理。

---

## 2. 方案选型与决策 (Decision)

我们决定采纳 **Ory Hydra** 作为 `go-blade` 的独立 OAuth 2.1 / OIDC 授权基础设施内核，并由微服务体系中的 `services/auth` 作为身份提供方（Identity Provider, IdP）与其协同配合。

### 核心决策要点：
1. **无头协议网关 (Headless Engine)**：
   Ory Hydra 纯粹负责 OAuth 2.1 / OIDC 协议解析、客户端注册、授权码管理、Token 签发与 JWKS 密钥对维护；Hydra 数据库中不保存任何用户登录凭证、密码或多租户组织数据。
2. **登录与授权挑战流程 (Login & Consent Flow)**：
   所有交互式登录均由 Hydra 生成 `login_challenge` 并委托给 `services/auth` 服务。`services/auth` 负责多租户上下文识别、用户名/密码/验证码校验；通过后通过 Admin API 回调告知 Hydra 登录成功，并在 Consent 阶段将 `BladeClaims`（`tenant_id`、`dept_id`、`role_id` 等）注入至 Access Token 与 ID Token。
3. **网关无状态验签 (Stateless Verification via JWKS)**：
   API 网关（Gateway）不直接依赖 Hydra 数据库，仅需定时拉取或监听 Hydra 的 `/.well-known/jwks.json` 端点缓存公钥，即可实现微秒级的本地非对称验签与多租户 Claims 解密。

---

## 3. 架构影响与后果 (Consequences)

### 正向影响 (Positive)
- **极高的安全性与合规性**：Hydra 获得 OIDC 官方合规认证，原生支持严格的 OAuth 2.1 最佳安全实践，避免自研协议带来的安全隐患。
- **业务数据 100% 自主掌控**：用户表、部门表、租户隔离规则（`pkg/tenant`）留在 `services/auth` 及底层业务数据库中，无任何第三方数据侵入。
- **高性能无状态水平伸缩**：网关及下游业务服务只做本地 JWKS 验签，授权服务器压力仅存在于用户首次登录与换发 Token 阶段。
- **技术栈纯度**：Hydra 为纯 Go 编写并原生提供 Go SDK，与 `go-blade` 整体技术栈天然一致。

### 负向影响与妥协 (Negative / Trade-offs)
- **系统组件增加**：本地开发编排与线上 K8s 部署需新增 Hydra 服务及对应的一张轻量数据库 schema。
- **开发集成复杂度**：`services/auth` 必须实现标准的 Login & Consent Challenge 接口，开发团队需对 OAuth 2.1 授权码 + PKCE 模式有清晰理解。

---

## 4. 实施指导规范 (Implementation Guidelines)

1. **部署配置**：在 `deploy/docker-compose/hydra.yaml` 中固化本地调试编排，配置统一的 Public（4444）与 Admin（4445）端口。
2. **底座增强 (`pkg/jwt`)**：在保持现有轻量对称加解密向后兼容的基础上，新增基于 JWKS 的公钥轮转验签支持。
3. **服务实现 (`services/auth`)**：遵循 go-zero 项目结构，基于 Hydra 官方 Go SDK (`github.com/ory/hydra-client-go/v2`) 实现 Challenge 闭环。
