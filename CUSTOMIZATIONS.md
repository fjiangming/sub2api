# 个性化代码维护台账

日期：2026-10-23。Fork：`fjiangming/sub2api`，分支：`dev`。
基线：`dba770d56a0ee0ae5bad1b3dc5e11614d323163c`。
原作者仓库：`Wei-Shaw/sub2api`，本地 remote `upstream`。

## 给后续 AI 的要求

每次修改源码、配置和测试，必须同步更新本台账，记录文件、原因、验证和兼容性。
合并原作者更新须先建立集成分支，保留以下功能语义，不要求逐行保留旧实现。
禁止用 upstream 文件整体覆盖个性化实现。已执行的迁移不能改写，只能新增迁移。
核心只保留持久交接、计费事务和少量运输钩子；运营编排继续放在已有运营中心。

## 不能破坏的约定

1. 实际付费 dispatch 前持久化意图；存储失败不得发出该次请求。
2. 已发出 HTTP 请求在客户端断开后有独立、有限期限；已断开的客户端不能启动新的尝试。
3. 计费任务同步移交持久存储，不依靠内存队列的 drop/sample 策略保证资金正确。
4. 带历史价格的使用记录与命令先持久化；usage_logs、原生 dedup、资金效果同事务提交。
5. 恢复只能调用原生 UsageBillingRepository.Apply；禁止用用户管理 API 直接减余额。
6. 重复恢复不重复扣费；不同实际 dispatch 使用不同资金事件 ID，防止复用客户端请求 ID 少扣。
7. 人工补记须经过 review → prepared → settled；归属、最终用量、历史价格和证据必须核对。
8. 已结算记录保留原账，额外用量生成独立差额记录；后续差额基于最后已结算子节点继续。
9. 保留原作者图片/视频/搜索/缓存等强制稳定资金事件 ID 的幂等语义。
10. 更新来源固定本 fork；更新缓存、回滚命令和安装脚本不能切回原作者发行版。

## 源码清单

以下路径除特别说明均相对于仓库根。

| 文件 | 个性化用途与合并注意事项 |
| --- | --- |
| `AGENTS.md`、`.gitignore` | 提醒后续 AI 维护台账，仅解除根 AGENTS 的忽略。 |
| `backend/migrations/custom_20261023_billing_recovery.sql` | 独立 sub2api_custom schema：意图、命令、补记链、版本、重试、缓存待办、审核事件；避开数字迁移号。 |
| `backend/internal/service/billing_recovery.go` | 持久交接契约、期限、dispatch ID、恢复、缓存失效。 |
| `backend/internal/service/billing_recovery_usage.go` | 历史使用记录校验、归属固定、累计用量转差额；不接受裸余额差额。 |
| `backend/internal/repository/billing_recovery_repo.go` | PostgreSQL 存储、审核 CAS、原生 usage_logs 事务内落库、退避与 12 次失败后转核查。 |
| `backend/internal/repository/billing_recovery_settlement.go` | 补记链锁、缓存完成标记、按原时间归属的历史限额更新；订阅复用原生日历日/一次性/周月窗口算法。 |
| `backend/internal/repository/billing_recovery_transport.go` | 有界 HTTP/SSE 数值 usage 采集，不存 prompt/响应正文/密钥；保留中断/截断标志。 |
| `backend/internal/repository/http_upstream.go` | 普通 HTTP 和 TLS 两个实际 dispatch 点保存意图及观察响应。新增运输路径须重新核查。 |
| `backend/internal/repository/usage_billing_repo.go` | 配置恢复仓库；Apply 同事务写记录、标记结算、原生 auth invalidation outbox；去重分支禁止重复资金效果。 |
| `backend/internal/service/usage_billing.go` | 四个仅运行时的 recovery 字段，不参与原生指纹和 JSON。 |
| `backend/internal/service/billing_service.go`、`backend/internal/service/openai_gateway_usage.go` | CostBreakdown 运行时定价失败标志；两个计费主干明确的定价失败留 review，禁止当作成功的零元账单；显式免费价格保持原语义。 |
| `backend/internal/service/gateway_usage_billing.go` | Gateway/OpenAI 共用计费入口先持久化；dispatch ID；独立期限；缓存用重复失效代替重复减算。 |
| `backend/internal/service/usage_record_worker_pool.go` | 开启保护时同步执行，最低 30 秒任务预算；关停/队列溢出不丢资金任务。 |
| `backend/internal/server/middleware/api_key_auth.go`、`api_key_auth_google.go` | 标准认证通过后加恢复上下文，Google 原生路由也接入；保留 simple mode 不扣余额的语义。 |
| `backend/internal/handler/openai_gateway_handler.go` | 共用 usage task wrapper 保留恢复上下文。 |
| `backend/internal/service/openai_ws_forwarder_v2.go`、`openai_ws_forwarder_ingress.go`、`grok_audio.go` | WS turn / Grok realtime 发出前保存意图；保留 WS 控制生命周期。 |
| `backend/internal/handler/billing_recovery_handler.go`、`backend/internal/server/routes/admin.go` | 现有 admin 边界内列表、详情、审核、补记、结算 API；无匿名资金接口。 |
| `backend/internal/service/update_service.go` | fjiangming/sub2api 更新源；缓存绑定 repository，拒绝原作者旧缓存。 |
| `frontend/src/components/common/VersionBadge.vue`、`deploy/install.sh` | 版本按钮/回滚/安装指向 fork，镜像 ghcr.io/fjiangming/sub2api。 |
| `.github/workflows/custom-dev-release.yml`、`Dockerfile.custom-release` | dev push 测试、嵌入前端、五平台归档、校验和、双架构镜像、Release；保留 pg_dump/psql 和健康检查。 |
| `backend/internal/service/billing_recovery*_test.go`、`backend/internal/repository/billing_recovery_test.go`、`.github/billing-recovery-sql.test.cjs` | 取消/期限、同步移交、差额、隐私、事务失败、重复恢复、更新缓存、真实 PostgreSQL 历史窗口测试。 |

## 运营中心契约

调度、查单、数值核对、核查确认、补记编排位于已有 `sub2api-extra/operations-center` 的“计费核查”tab，另见该工程台账。运营中心不直接写资金表。
API 为 `/api/v1/admin/billing-recovery`；案件字段 snake_case，usage_record 内部字段沿用原生 Go UsageLog 的 PascalCase。
变更携带 version；冲突后重新读取。同一已结算节点最多有一个未排除的补记子节点；进一步补记应对最后已结算子节点提交最终累计用量，服务端减去整条父链。
每 30 秒读取核心 `state=pending`，按到期顺序最多 100 条，包含 prepared 和 settled 但 cache_pending 的记录；重启后继续读取。只重试结算/缓存，不重发推理。
查单复用 provider-monitor 同步业务样本；只有唯一 request ID、历史账号映射和未截断查询才能确定匹配。样本可能没有价格，必须补充历史售卖价格及倍率证据；不能用时间相近自动扣费。

## 配置与发行

- 核心默认开启；`BILLING_RECOVERY_ENABLED=false` 可退回原行为，不推荐关闭。
- `BILLING_RECOVERY_TIMEOUT_SECONDS` 为 30–7200 秒，默认 1800；到期仍未知进入核查。未知请求超过期限加 60 秒、最近一分钟未更新才能补记。
- 运营中心 `OPERATIONS_CENTER_BILLING_RECOVERY_ENABLED=true` 默认开启；后台需要已有的持久管理员认证。只有浏览器 SSO 时可人工执行，不用短期浏览器 token 运行后台。
- dev 每次 push 构建该次 HEAD；本地 commit 不触发 GitHub。fork 需启用 Actions，允许工作流创建 Release/GHCR 包。
- 版本：`major.minor.(upstreamPatch*1000000+runNumber*100+runAttempt)`，三段数字兼容原更新器，重跑有不同版本。
- 构建分别发布版本；latest 提升单独串行，防止慢的旧构建回退 latest。归档名和 checksums.txt 保持原生约定。
- 保留原作者 tag release 工作流；GITHUB_TOKEN 自动创建 tag 不递归触发它。
- Docker 长期部署应修改 compose 为 fork 镜像；网页更新运行中程序，重建容器仍取 compose 中的镜像。

## 可靠性边界

不静默丢失待计费事项，不等于任何物理故障下立即得到最终账单。断电、SIGKILL、上游不返回最终 usage 时，保留已提交意图/已观察用量，等待查单与核查，不能编造 token 或估算扣费。
大于 2 MiB 的单 JSON / SSE 行标记截断；图片/音视频继续用原生解析及持久任务。新协议必须验证发出前意图和统一计费入口，不能仅按平台名称推定覆盖。
历史订阅/Key/账号滚动限额只加到原请求所属的现存窗口；旧周期的使用记录保留，不消耗新周期。
user×platform 的 Redis/flusher 辅助限额沿用原作者体系，没有改成资金账本；恢复资金后如运营依赖该限额展示，还需核对其展示，不能把它当扣费证明。
数据库不可用时禁止新 dispatch；已发出且尚未持久化最终用量的请求保留原意图。明确的定价失败保留用量并标记 PRICING_UNAVAILABLE 待核查，运营补充历史价格后才能按记录扣费。同步计费增加数据库写入及延迟，上线前需观察真实并发容量。

## 验证与后续集成

核心：`go test -tags=unit ./internal/service ./internal/repository ./internal/handler ./internal/server/middleware`。
前端：`pnpm run build`。运营中心：`npm test`、`npm run check`，桌面/390px 手机验证保存→确认→结算。
SQL：安装临时 `@electric-sql/pglite@0.5.6`，`PGLITE_MODULE_PATH` 指定绝对模块路径，运行 `node --test .github/billing-recovery-sql.test.cjs`。
Windows 全量单测中原作者 `TestOllamaProbeCallback_StaleLongDoesNotOverrideNewShort`、`TestCommandCodeGatewayPassesThroughCatalogProtocols` 出现时间精度边界失败，未改其代码/测试；备份测试需要 Git usr/bin 的 sh 在 PATH。CI 为 Linux，保留测试失败阻断发行。全量 Go 测试没有被标为全部通过。
本次新增计费回归及现有定价/计费相关测试、2 项 PostgreSQL 历史窗口/缺价核查测试、运营中心 158 项测试及语法检查、前端构建、Linux amd64 嵌入前端二进制构建、桌面/手机交互检查通过；增加 SSE 最后一行无换行符的 usage 采集回归。本地 Docker 守护进程未启动，未进行容器构建、真实上游端到端请求和实际 GitHub 发布；首次 push 须检查 CI。未连接或更新服务器。
合并上游逐项核查统一计费调用点、HTTP/WS dispatch、usage_logs 列、dedup/归档、限额窗口、缓存 outbox、后台任务、版本解析、归档名和迁移加载器。

## 2026-10-23：首次推送与 CI 适配

首次 GitHub Linux CI 的 lint 发现恢复仓库及测试中 8 个 cleanup 调用未显式处理返回值。`billing_recovery_repo.go`、`billing_recovery_settlement.go`、`billing_recovery_test.go` 改为与原生仓库一致的 deferred cleanup 写法；不改变事务或资金行为。服务器只读核实实际二进制为 0.2.17，冻结镜像仍带 0.1.160 标签；现有 Compose 含 pull_policy: never，首次切换须调整目标服务的镜像及拉取策略。详细命令见相邻 sub2api-extra 的 `operations-center/docs/billing-recovery-update.md`。本轮未更新服务器。

GitHub 安全检查发现原分支锁定的 DOMPurify 3.4.14 命中 GHSA-cg2g-xh64-v4q7、GHSA-mrv2-3pqr-j5jr。`frontend/package.json` 和 `frontend/pnpm-lock.yaml` 将直接及受 override 约束的间接依赖最低版本提升到 3.4.17；保留安全扫描，不添加漏洞豁免。后续合并可采用上游更新的已修复版本。验证：前端 frozen-lockfile 安装、构建及 10 项 URL/Logo 清理回归通过；生产依赖审计按仓库现有豁免规则校验通过。

## 2026-10-23：发行检查门禁与镜像追溯

排查确认早期自定义发行流程与原生 CI / Security Scan 独立，可能在它们失败时仍发布镜像。`.github/workflows/custom-dev-release.yml` 新增 checks job；`.github/release-tools/require_fork_checks.py` 仅接受同一完整 SHA、dev 分支、push 事件下原生两个工作流的最新成功记录。缺失/排队/运行中继续等待，失败/取消/跳过/API 异常/超时阻止 publish 和 latest 提升。保留原生工作流，不侵入原作者 CI，继续为每次合格 push 发布独立版本。

`.github/release-tools/test_fork_release_gate.py` 覆盖提交/分支/事件/工作流隔离、失败/取消/跳过、较新失败不能被旧成功掩盖、重跑未完成、等待双门禁、缺失超时及 API 异常。发布 Release 前运行 amd64/arm64 镜像中的二进制，断言实际版本和完整 commit，并检查 amd64 的 pg_dump/psql 能运行；失败不提升 latest。镜像仍包含 OCI source / revision / version；latest 表示最新成功发行，不能把正在构建或失败的 dev HEAD 当作已发布。服务器只需拉取 GHCR 镜像，步骤及首次冻结镜像切换见 extra 的更新文档；本轮不部署生产。

验证：7 项门禁测试、工作流 YAML / Bash 语法检查通过；真实 GitHub API 对 d3a9911 的完整 CI 与安全检查放行，对 8740f14 的安全失败阻断。额外运行原生 release helpers 全套时，Windows 的 GBK 默认解码及 Bash 路径导致既有测试失败，未修改其源码；同一版本的这些原生测试已在 GitHub Linux CI 通过。
