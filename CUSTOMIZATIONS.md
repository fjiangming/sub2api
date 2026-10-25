# 个性化代码维护台账

日期：2026-10-23。Fork：`fjiangming/sub2api`，分支：`dev`。
初始基线：`dba770d56a0ee0ae5bad1b3dc5e11614d323163c`。
当前上游集成基线：`v0.2.18`（`ac1b8f1271be0d674fa6f074403c1410f30a3a81`）及其仅同步 VERSION 的后续提交 `5c185c5c5d4f5e409e2139806e0964ae944e03e8`。
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
7. 人工补记须经过 review → prepared → settled；归属、最终用量、价格来源和证据必须核对。历史价格快照不可覆盖；CSV 旧请求缺快照时允许明确标注当前主站报价并经管理员确认，不将当前价冒充历史价。
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
- `BILLING_RECOVERY_SYNC_STREAM=true` 默认将支持的同步文本请求在内部转为上游 SSE，再还原同步 JSON；`false` 只关闭桥接，不关闭持久计费。
- `BILLING_RECOVERY_TIMEOUT_SECONDS` 为 30–7200 秒，默认 1800；到期仍未知进入核查。未知请求超过期限加 60 秒、最近一分钟未更新才能补记。
- 运营中心 `OPERATIONS_CENTER_BILLING_RECOVERY_ENABLED=true` 默认开启；后台需要已有的持久管理员认证。只有浏览器 SSO 时可人工执行，不用短期浏览器 token 运行后台。
- dev 每次 push 构建该次 HEAD；本地 commit 不触发 GitHub。fork 需启用 Actions，允许工作流创建 Release/GHCR 包。
- 版本：`major.minor.(upstreamPatch*1000000+runNumber*100+runAttempt)`，三段数字兼容原更新器，重跑有不同版本。
- 构建分别发布版本；latest 提升单独串行，防止慢的旧构建回退 latest。归档名和 checksums.txt 保持原生约定。
- 保留原作者 tag release 工作流；GITHUB_TOKEN 自动创建 tag 不递归触发它。
- Docker 长期部署应修改 compose 为 fork 镜像；网页更新运行中程序，重建容器仍取 compose 中的镜像。

## 可靠性边界

不静默丢失待计费事项，不等于任何物理故障下立即得到最终账单。断电、SIGKILL、上游不返回最终 usage 时，保留已提交意图/已观察用量，等待查单与核查，不能编造 token 或估算扣费。
数值观察器大于 2 MiB 的单 JSON / SSE 行标记截断；同步桥接的可重读 JSON 请求上限 4 MiB、SSE 聚合上限 64 MiB，越界不能假装收到完整 usage。图片/音视频继续用原生解析及持久任务。新协议必须验证发出前意图和统一计费入口，不能仅按平台名称推定覆盖。
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

## 2026-10-23：修复旧提交创建 Release 的 403

发行 run 39864676105 已推送版本镜像，但 `gh release create --target SHA` 返回 HTTP 403 / Resource not accessible by integration；job 实际已有 contents:write。构建期间 dev 已推进且工作流变更，旧 SHA 尚无 tag，符合 GitHub 对无既有引用的历史提交发行所施加的 workflows 权限限制（GitHub 2023-11-02 changelog）。不扩大长期 Token 权限，不用新 HEAD 冒充旧产物来源。

自定义工作流的 version job 在长时间构建前，用该 job 的短期 GITHUB_TOKEN 预先推送完整 SHA 对应的不可变轻量 tag；已有 tag 必须同 SHA，禁止强推。publish 再核对远端 tag 的 SHA，并用 --verify-tag 创建 Release，不再让 Release API 隐式创建历史 tag。失败的检查/构建可能留下 tag，但不会产生正式 Release 或提升 latest；GITHUB_TOKEN 推 tag 不递归触发原生 tag release 流程。YAML、所有 Bash 块及 tag / publish 的依赖关系需持续核验。

## 2026-10-23：生产改动清单归档

新增 `docs/server-changes-2026-10-23.md`，根据本次服务器部署前文件/数据库备份和部署后实况记录两份 Compose 的 image / pull_policy、运营中心加密持久认证、计费恢复 schema、容器替换、备份和演练目录及临时维护标记；`.gitignore` 仅为此文档增加精确白名单，避免被原有 docs/* 规则忽略。服务器副本放在 `/root/sub2api-deploy/backups/billing-fix-20261023072212/SERVER-CHANGES.md`，追加脱敏文件/数据库差异核对 JSON；本次整理不改变运行配置或触发重启。

额外核实原作者 `246_enable_gemini_image_generation_groups.sql` 同计费迁移一起执行，将生产分组 60、62、68、71 的 allow_image_generation 从 false 改为 true，已在清单列明。这是本次镜像启动时的既有迁移效果，未人工回退。原有 504809 条使用记录在上线验收时完整保留；原 90 天策略启动时过期的 5 条零元记录已原样恢复，策略仍有效。验证：两份 Compose 完整 YAML 结构比较仅各 2 项不同；env/Nginx/主站配置和价格文件逐字节比较；卷内原密钥与成本台账一致；升级前 dump 对比确认新增 2 项迁移及仅 sub2api_custom 的关系对象，原关系对象无删除。此次仅文档和台账，未推送，也未触发新构建。

## 2026-10-23：修复计费核查列表 / 详情读取 SQL NULL

生产 unknown / 全部状态列表返回主站 HTTP 500，经运营中心转为 502。日志定位到 scanRecovery 将 observed_usage 的 SQL NULL 直接扫描到 *json.RawMessage（Go 1.27 中为 *jsontext.Value）失败；等待上游响应的正常意图和未经过 HTTP 观察器的 prepared 记录均可能包含该空值。此前只验收空的 pending 列表，未覆盖含这类真实记录的读取。

`backend/internal/repository/billing_recovery_repo.go` 仅将 observed_usage 和 request_snapshot 先读到可接受 SQL NULL 的 []byte，再赋给 API 的 JSON 字段，保留 SQL NULL、JSON null 和已有用量的区别。列表、详情和结算恢复读取共用此修复；不改迁移、历史记录、定价、资金事务或上游请求。`backend/internal/repository/billing_recovery_test.go` 新增使用真实 database/sql Scan 转换的 8 个回归子场景，覆盖 unknown、无 HTTP observation 的 prepared / pending、数值用量和 JSON null，分别验证 Get / List、历史快照和已准备的使用记录 / 命令保持原值。

验证：修复前 4 个 SQL NULL 子场景复现与生产完全相同的错误；修复后全部计费恢复回归通过，完整 repository 单元测试通过。Windows 完整测试需将实际 Git 安装目录 E:/Git/usr/bin 加入本次进程 PATH，未改动原作者备份测试。生产只读核对确认 SQL NULL 意图仍保留、已结算事项均对应 usage_logs；发布继续经过 dev 自动工作流的完整 CI / Security 门禁，服务器通过镜像更新，备份仍统一置于 /root/sub2api-deploy/backups/ 的本次专用子目录。

## 2026-10-23：生产等待上游响应超时调整

用户授权后，仅调整服务器 `/root/sub2api-deploy/docker-compose.yml` 中 sub2api 的环境变量：新增 `GATEWAY_RESPONSE_HEADER_TIMEOUT=1800` 和 `BILLING_RECOVERY_TIMEOUT_SECONDS=1800`。前者覆盖原生默认的 600 秒响应头等待上限，后者显式固定 fork 原有的 1800 秒独立处理期限。不能仅提高 Nginx 超时而保留应用的 600 秒上限。后续合并、Compose 替换或镜像更新应保留这两个部署覆盖；它们不修改镜像内的默认值。

`/etc/nginx/conf.d/migrated-services.conf` 中 epoint2api.fjmooo.cn、aihub.fo2.us.ci、aihubdirect.fo2.us.ci 三个域名的 `location /`（均代理到 127.0.0.1:9000），将 `proxy_send_timeout`、`proxy_read_timeout`、`send_timeout` 统一为 1860s。原值分别为 600s、1200s、600s；连接超时、监控路径及其他站点配置保持原值。这些是 Nginx 对应读写间隔的超时，不代表 Cloudflare 或客户端的限制也随之延长。

切换使用原维护开关阻止新请求，并连续 25 秒确认在途和等待队列均为零；备份完整 PostgreSQL 数据库、角色、应用 data 及原配置后，只重建 sub2api，使用 `--no-deps --pull never --force-recreate`。未拉取或部署其他代码，镜像 ID、提交 050d5b96a3ebb1c8cd9e81be2179848f7b3e0449 和运行版本 0.2.17000701 均保持不变。Nginx 校验成功后平滑重载；北京时间 11:10:19 恢复接入。PostgreSQL、Redis、运营中心的容器 ID 和启动时间保持不变。

备份与回滚材料统一存放 `/root/sub2api-deploy/backups/timeout-20261023110218/`，包含私有部署脚本、候选配置、精确差异、原配置、完整 dump、角色、应用 data、SHA-256 清单及核验 JSON。现有 90 天清理任务在启动时删除的 105 条过期使用记录已按切换前的原 ID、原字段恢复；没有再次扣费，原保留策略保持生效。恢复接入前核对用户余额、业务身份、使用记录总数 506670、使用记录费用摘要及迁移记录完全一致。

验证：两份 Compose 的解析结构仅增加上述 2 项环境变量；候选/实际 Nginx 配置均通过 `nginx -t`。主站、运营中心、三个域名的本地回源健康检查成功；从本地电脑访问主站首页/健康接口及运营中心公网健康接口均为 200。VPS 自身经 Cloudflare 访问公网域名返回 403，来源为边缘层，没有变更其规则。生产运营中心使用持久认证读取计费核查各状态列表和 unknown 详情均为 200；恢复后首轮核验的 9 笔已结算真实请求均有原生使用记录和扣费去重记录，已结算缓存待办为 0。未额外发起收费推理、补扣或重发历史推理；未进行超过 10 分钟的受控真实上游端到端验证，不能据此保证上游无最终用量时也能自动扣费。

`docs/server-changes-2026-10-23.md` 追加本次配置、备份、数据保护及验证清单；服务器副本位于本次备份目录的 `SERVER-CHANGES.md`。本轮没有修改核心源代码、推送 GitHub 或发布新镜像，现有其他未提交源码改动未部署。

## 2026-10-23：所有上游的同步超时保护和逐请求核查证据

此节为本地源码修改，未提交、推送、发布或部署；与上一节独立的生产超时配置记录分开。已定位的供应商侧 CDN 约 125 秒 524 会丢失源站最终 usage；单独延长本站期限不能解决该链路。改动不按供应商域名或账号过滤。

| 文件 | 本轮个性化实现与集成约束 |
| --- | --- |
| `backend/internal/repository/billing_recovery_stream.go`（新增） | 标准 Anthropic Messages、OpenAI Chat / Responses、Gemini generateContent 同步请求内部使用 SSE，返回原同步 JSON，支持原生工具 / thinking 等输出；复用 apicompat 累加器并保留 Responses 终态扩展字段，长 Token / 工具参数用 Builder 线性累加。不改变签名请求、原生流式、background、媒体或非标准协议；不新增推理重试。4 MiB 请求 / 64 MiB 聚合边界及关闭开关须保留。 |
| `backend/internal/repository/http_upstream.go` | 普通 HTTP / TLS 两个实际 dispatch 先转换，持久化意图后发送，原始 SSE 先经过观察器，再聚合 JSON；必须保留观察顺序。 |
| `backend/internal/repository/billing_recovery_transport.go` | 持久化最终 / 部分状态、终止事件、多 choice 完结、响应 Header / body ID、断开及耗时；深度合并嵌套数值 usage。无效计数、初始输出 0、截断、缺少终态不能冒充完整用量；不保存文本或认证数据。 |
| `backend/internal/service/billing_recovery.go` | 请求归属、关联标识和期限快照；每个真实 dispatch 生成不同关联 ID，保留调用方 Header；AWS 签名不改写。提供列表过滤和原始客户端断开查询，按 Gemini 路径识别原生流式，避免错误记录同步类型。 |
| `backend/internal/service/billing_recovery_pricing.go`（新增） | 请求入口只捕获模型 / stream / 定价元数据，发出前冻结历史售价、区间、倍率与缓存策略；只读 quote 重建标准 Token 使用记录，不取上游采购成本或今天的售价。媒体 / 工具 / tier / effort / response 模型及缺价拒绝自动报价，转运营核查。 |
| `backend/internal/server/middleware/api_key_auth.go`、`api_key_auth_google.go` | 在已有恢复上下文中包裹入口请求体，读取完即丢弃正文，仅保留定价元数据。 |
| `backend/internal/repository/billing_recovery_repo.go` | Begin 原子记录用户邮箱 / 名称、Key / 上游账号 / 分组名称、历史账号倍率及缓存覆盖；列表支持用户 / Key / 上游账号 / 请求 ID / 超时条件，新增 incomplete 查询已结算但最终观察不完整的记录。 |
| `backend/internal/handler/billing_recovery_handler.go` | 原 admin 边界下增加过滤和版本化只读 `POST /:id/quote`，报价不扣费；补记仍走 review → prepared → native Apply。 |
| `backend/migrations/custom_20261023_billing_recovery_lookup.sql`（新增） | 仅新增 user / account 查询索引，不改已经执行的恢复迁移，不删除业务数据。 |
| `backend/internal/repository/billing_recovery_stream_test.go`、`backend/internal/service/billing_recovery_pricing_test.go`（新增）、`backend/internal/repository/billing_recovery_test.go`、`.github/billing-recovery-sql.test.cjs` | 四协议真实 HTTP 断开后收齐 usage、仅一次 dispatch、工具 / thinking / Responses 扩展、2000 片长输出、无效最终计数、历史售价 / 归属、Gemini 原生流式、可空 SQL、新过滤 / 不完整账单 / 新索引的回归。 |
| `deploy/.env.example` | 说明默认开关、期限和内存边界；Docker 覆盖须显式传入 service environment，单写 .env 不自动注入容器。 |
| `CUSTOMIZATIONS.md` | 本节登记每项源码 / 配置 / 测试变动，保留前轮独立的生产改动记录。 |

运营编排、持久查单队列、历史 Key 定向日志刷新、证据副本、核查草稿和差额维护位于相邻 extra 工程，不移入核心。运营中心会发现 unknown / review / incomplete；最终标准 Token 和历史价格均完整才生成 review 草稿，不自动批准，不直接扣余额。客户端自带且可重复的 ID（包括上游回显）不授权唯一资金匹配。旧版本没有历史价格快照的意图需人工补全价格凭据。

标准桥接通过上游心跳 / 分片降低供应商 CDN 同步等待超时，但不能要求不配合的上游提供最终账单。未知用量、独立期限耗尽、强制退出、数据库暂不可写时仍保留已提交意图 / 已知数值待核查；不能声称所有物理故障下已准确自动扣费。上游不支持流式或日志时不做估算。发布后需配套更新主站、运营中心及供应商监控三个镜像，原 fork Release / GHCR 更新通道保持不变。

验证：本轮计费相关 Go 回归通过，Windows server 二进制可构建；完整 repository / handler / middleware 单元测试通过，service 全量仍触发原有 `TestOllamaProbeCallback_StaleLongDoesNotOverrideNewShort` 的 Windows 时间精度失败，未修改该测试。真实 PostgreSQL 3 项回归通过。extra 的完整运营中心 172 项与供应商监控 287 项测试、80 个 JS 源文件语法检查、运营中心 check 及桌面 / 390px 手机核查交互通过。CI YAML 依赖与 8 个更新 Bash 块通过解析 / 语法验证。未进行 Docker 构建、GitHub 发行、Linux 执行或真实上游收费请求验证。本地改动尚未应用于服务器。

最终静态检查使用与 GitHub CI 相同的 golangci-lint 2.14.0，在本机对 service / repository / handler / middleware 四个完整包运行得到 `0 issues`。修正了 SSE 测试的未经检查类型断言，改用带错误校验的标准 ResponseController Flush；观察器 finish 用显式 bodyComplete 条件表达同一终态语义，所有不可失败的 Buffer / Builder 写入按仓库 errcheck 规则显式接收返回值。修改后计费回归及最终 server 构建再次通过；两仓库 diff --check 均通过。

## 2026-10-23：授权清理部署临时材料

用户审核清单后授权清理。只删除 `/root/sub2api-deploy/backups/` 下三次专用部署目录内已逐项盘点的 46 个演练/失败/临时条目，约 11.721 GiB；删除前确认没有容器挂载、部署进程或活动锁引用目标。保留正式 dump、角色、应用/运营中心/Redis 备份、files/ 原配置、三份 deploy.py 依赖链及核验材料，约 0.783 GiB。后续 AI 不应再依赖 rehearsal-* 展开副本或 *.first-attempt / *.first-switch-attempt 等已清理材料，应以保留的正式备份及执行回执为准。

清理前后逐文件核对保留备份 SHA-256 和生产配置散列，四个相关服务的容器身份、启动时间及健康状态不变，公网主站/运营中心健康为 200；未修改业务数据或触发重启。回执位于 `/root/sub2api-deploy/backups/timeout-20261023110218/cleanup-20261023.json`。`docs/server-changes-2026-10-23.md` 追加清理结果。本轮仅维护文件和文档，没有修改计费源码或发布镜像。

本地临时文件和生成产物的 48 个待删条目已验证路径范围，但自动安全审核拒绝批量及单文件删除命令，未执行本地删除。Downloads 下清理清单已标明服务器完成/本地未完成，并提供尚未执行的本地 PowerShell 清理脚本；没有删除原始上游 CSV、初次核查证据或未提交的个性化源码。

## 2026-10-23：CSV 历史补记的最小核心接入

此轮根据用户要求仅本地实现，仍在 `fjiangming/sub2api` 的 `dev`；没有提交、推送、发布、部署或操作真实资金。保留此前所有未提交的运输保护、定价快照、查单和文档改动。CSV 解析、时间匹配、价格输入、预览、确认编排及中断恢复继续放在 `sub2api-extra/operations-center`，核心不增加供应商适配器、CSV 解析器、定时任务或另一套扣费实现。

| 文件 | 本轮增量与未来集成要求 |
| --- | --- |
| `backend/internal/service/billing_recovery_import.go`（新增） | 独立的导入预留 DTO / 可选 store 接口，限定原生来源、上游账单摘要、预览摘要、用户 / Key / 账号、历史时间和证据；不扩大原通用 store 接口，保留原有测试替身兼容。 |
| `backend/internal/repository/billing_recovery_import.go`（新增） | 按账号的事务 advisory lock；同账单与同本地来源双重唯一预留，重复同摘要返回原案件。已有案件检查版本 / 归属 / 状态 / 无使用记录 / 静默期。原版错误来源检查原生用户 / Key / 账号 / 分组 / 模型 / 时间及上游尝试序号，排除已有使用记录、持久意图及原生 dedup / 长期归档；缺本地关联 ID、已删除实体或历史订阅不唯一则拒绝。仅创建 review 案件与审计事件，不写 usage_logs 或余额。数组参数必须用 pq.Array。 |
| `backend/migrations/custom_20261023_billing_recovery_csv_import.sql`（新增） | 新增 `sub2api_custom.billing_import_claims`；`(account_id,bill_identity)` 与 `local_reference` 唯一，绑定 payload_digest、原案件、操作者和时间。文件名在基础 custom 迁移后排序；以后不得改写已经执行的迁移。 |
| `backend/internal/handler/billing_recovery_handler.go` | 既有管理员边界内新增 `POST /api/v1/admin/billing-recovery/imports/reserve`；列表新增 RFC3339Nano 的 from / to。预留不代表批准或扣费，仍必须按原有 usage → review → settle 接口执行。 |
| `backend/internal/service/billing_recovery.go`、`backend/internal/repository/billing_recovery_repo.go` | 列表增加历史起止时间过滤，保留原有调用参数与动态分页占位符，不改变普通列表或自动恢复条件。 |
| `backend/internal/service/billing_recovery_usage.go` | 仅新建、未填写使用记录的 historical-error review 案件豁免新 staging 时间造成的额外一分钟等待；原请求仍须超过独立期限加 60 秒。普通意图的静默期不变。继续复用原生 Restore / Apply 和资金同事务语义。 |
| `backend/internal/repository/billing_recovery_import_test.go`（新增） | 预留幂等 / 摘要冲突、版本和归属、原生来源 / 订阅、已有账单、删除实体、无资金写入、无效身份、活跃请求及日期分页回归。 |
| `backend/internal/handler/billing_recovery_handler_test.go`（新增）、`backend/internal/service/billing_recovery_test.go` | HTTP 输入在预留前校验、纳秒日期过滤，以及历史 staging 豁免不能绕过原请求期限。 |
| `.github/billing-recovery-sql.test.cjs` | 新增真实 PostgreSQL 迁移、历史归属 staging、删除 Key 阻断、原生防重归档及两类唯一约束测试；保留此前 3 项 SQL 回归。 |
| `CUSTOMIZATIONS.md` | 本节记录本轮全部核心增量；配套操作说明为 extra 的 `operations-center/docs/billing-csv-import.md`。 |

新的人工 CSV 契约：自动生成候选不等于自动批准扣费。没有请求 ID 的上游 CSV 可以按账号 / 模型 / 开始时间匹配本地意图或仍保留的原生错误，但时间归属须确认；多个候选或多行争用同一请求须明确选择并提供依据。CSV 自带的 ID 不可冒充原生日志里缺失的关联证据。已有使用记录不能再按 CSV 收整笔费用，原有差额链约定保持；只剩原版使用记录而没有持久案件的差额不自动建账。上游采购成本不当用户售价；旧请求没有历史快照时由运营人员核定有效售价和原倍率，不能自动套今天的配置。

预留绑定上游账单身份、本地来源和冻结的预览摘要，不允许结算中改用户 / 改价格。原生使用记录、dedup 和资金仍由已有 Apply 事务提交；供应商 CSV、运营中心 SQLite 的丢失或重启不应变成第二次扣费的理由。若原日志、关联证据或最终用量已丢失，保留待核查，不编造归属或 Token。

验证：核心 BillingRecovery 相关 service / repository / handler 回归、4 项真实 PostgreSQL SQL 测试及 Windows server 构建通过；最终 golangci-lint 2.14.0 对上述三个完整包为 0 issues，两仓库 diff --check 通过。运营中心 197 项全量测试（含 25 项 CSV 专项）、实际导出文件 15100 行解析和 1440px / 390px 完整模拟流程通过，详见 extra 台账。未运行 Docker / Linux CI、真实上游推理或生产补扣验证。

## 2026-10-23：授权提交与推送计费修复

用户授权将上述超时观察、同步桥接、历史定价与 CSV 补记改动提交并推送 GitHub。核心沿用 `dev`，配套 extra 沿用 `main`，推送前两仓库均与远端同分支同步。保留自动 CI / Security 门禁及 fork 发行镜像渠道；推送只触发既有 GitHub 构建，不执行服务器更新、生产迁移或真实补扣。服务器须等待用户后续明确通知后再更新。

## 2026-10-23：完整 CI 的空 Gateway 路由兼容修复

首次推送 `d656e96bd` 的 Linux 完整 CI 发现原生 `TestClaudeResetRedeemRouteMatchesCodexResetProtection` 只构造管理员路由、不提供 Gateway；新增价格初始化在注册路由阶段解引用空处理器。此前计费专项和完整 handler 测试未运行 server/routes 包，未覆盖该场景；CI 门禁按设计阻止失败提交发行。

`backend/internal/handler/billing_recovery_handler.go` 仅在提供 GatewayHandler 时初始化价格服务，保留空依赖时的原生路由注册行为。`billing_recovery_handler_test.go` 增加空 Gateway 注册不 panic、恢复未配置时列表返回 503 的回归，不修改原作者测试、认证中间件、计费或发行门禁。修改前本地复现原生路由测试同一 panic；修改后完整 handler / routes 单元测试及 Windows server 构建通过，两包 golangci-lint 为 0 issues，复核提交 diff --check。后续仍由完整 Linux CI 和安全检查决定是否发行。配套 extra 同一提交的运营中心 ARM64 QEMU 依赖安装曾触发 Illegal instruction，重跑后全部镜像构建成功，无需改动业务源码。

## 2026-10-23：Nginx 保持客户端断开后的回源请求

用户明确授权配置 `proxy_ignore_client_abort on;`，北京时间 20:45:02 完成。服务器 `/etc/nginx/conf.d/migrated-services.conf` 仅在 `epoint2api.fjmooo.cn`、`aihub.fo2.us.ci`、`aihubdirect.fo2.us.ci` 三个域名代理到 `http://127.0.0.1:9000` 的 `location /` 中各新增一行该指令。保留原有 1860s 超时及其他路由；后续生成或替换 Nginx 配置须保留此项部署覆盖。

先核对远端配置与候选差异只有三行，备份整个 `/etc/nginx` 及原文件，再通过独立入口验证候选完整配置。原子替换后再次执行 `nginx -t` 和 `systemctl reload nginx`；主进程 PID 1006 保持不变，确认新 worker 启动，三个域名的本机 Host 回源 `/health` 均为 200；从本地访问 epoint2api 和 aihub 的公网 HTTPS `/health` 也均为 200。其余 Nginx 被包含文件和主站 Compose / .env / 应用配置 / 价格文件散列保持一致；主站、PostgreSQL、Redis、运营中心的容器身份、镜像、启动时间及重启次数保持一致，没有操作业务数据或部署新镜像。

所有服务器备份、候选、差异、前后配置展开、校验回执和 `rollback.sh` 统一放在 `/root/sub2api-deploy/backups/nginx-client-abort-20261023204501/`，目录权限 0700。配置过程使用 `/run/lock/sub2api-nginx-client-abort.lock` 防并发，不增加常驻服务。`docs/server-changes-2026-10-23.md` 同步追加此项，服务器副本放在本次备份目录的 `SERVER-CHANGES.md`。本轮仅记录配置维护，不提交或推送 GitHub。

此项使新请求在客户端或 CF 断开后由 Nginx 继续保持到应用的回源连接，不延长 CF 自身超时，也不改变上游供应商线路。未发起收费推理或生产补扣，未进行真实 125 秒断开后的长请求计费验证；不能将健康检查视为所有漏费场景均已修复。

## 2026-10-23：集成原作者 v0.2.18

用户授权评估并合并原作者最新版。先在现有 dev HEAD `899e5f98e07fdd08806294393aa12de5de97e776` 上创建本地集成分支 `codex/integrate-upstream-v0.2.18`，再合并上游 `5c185c5c5d4f5e409e2139806e0964ae944e03e8`；该提交仅比正式发布 tag 多将 VERSION 从 0.2.17 同步到 0.2.18。完成验证后回到 dev，以 fast-forward 保留可追溯的上游合并历史。本轮不推送 GitHub、不触发发行、不更新服务器或生产数据。

建议集成的原因是 OpenAI/Codex 的出站身份和空响应失败恢复得到修复。DOMPurify 3.4.17 安全升级已在 fork 中提前完成，自动合并没有重复变更。新增上游内容不替代计费恢复机制，也没有数据库迁移。先前未提交的 Nginx 维护台账和服务器改动清单随本次本地提交保留。

| 上游文件 | 本轮集成内容及个性化兼容性 |
| --- | --- |
| `backend/internal/pkg/openai/request.go`、`request_codex_identity_normalization_test.go` | 默认使用 CLI 身份，成对归一化历史 TUI 的 originator / UA，并验证配对及幂等；不改变计费请求归属。 |
| `backend/internal/service/openai_codex_identity.go`、`openai_gateway_service.go`、`setting_gateway_runtime.go`、`settings_view.go` | 同步规范 Codex 身份及说明，保留版本来源和账号自定义 OS / 终端指纹。 |
| `backend/internal/service/openai_gateway_passthrough.go`、`openai_gateway_upstream_errors.go`、`openai_capacity_shed_test.go` | 空文本 / 空片段不再误判已输出，保留实际内容和工具调用的重试边界；尊重请求级 Retry-After，最多等待 8 秒。沿用原生 failover；每次实际 dispatch 的持久意图及客户端已断开禁止新 dispatch 的 fork 保护保持原样。 |
| `backend/internal/service/openai_codex_identity_test.go`、`openai_codex_version_sync_service_test.go`、`openai_oauth_passthrough_test.go`、`openai_setup_token_compat_test.go`、`openai_ws_forwarder_success_test.go` | 原作者相应身份回归更新，未删除或降低 fork 计费测试。 |
| `frontend/src/i18n/locales/en/admin/settings.ts`、`frontend/src/i18n/locales/zh/admin/settings.ts` | 更新 Codex 配置示例与说明；不改版本按钮和核查入口。 |
| `backend/cmd/server/VERSION` | 基础版本改为 0.2.18；后续 dev push 仍按既有 fork 公式生成 0.2.18 系列独立版本和 GHCR 镜像。 |
| `README.md`、`README_CN.md`、`README_JA.md`、`assets/partners/logos/ooko.ai.png` | 接收原作者的合作伙伴资料更新。 |
| `CUSTOMIZATIONS.md`、`docs/server-changes-2026-10-23.md` | 更新当前上游基线和本节，并保留前一轮已完成的 Nginx 配置记录。 |

冲突检查：自动合并没有 unmerged 文件；21 个上游增量文件与 fork 的 49 个既有改动文件没有源代码交集。另有本地台账和服务器清单两份文档待提交。47 个既有个性化代码 / 配置 / 测试文件的 Git 内容与合并前一致，包含三项 custom 迁移、原生 Apply 事务挂接、价格快照、独立请求期限、同步 SSE 桥接、CSV 防重、fork 更新源、dev 发行门禁及镜像地址。运营中心工程本轮无改动。

验证：`internal/pkg/openai`、repository、handler、middleware、routes 五个完整单元测试包通过；service 中 `BillingRecovery|Codex|CapacityShed|OpenAIStream|OpenAIOAuth|OpenAISetup|OpenAIWSForward|UsageRecordWorkerPool` 专项通过，覆盖新增空流 / Retry-After / 身份回归及原计费保护。4 项真实 PostgreSQL/PGlite 回归、7 项 fork 发行门禁回归、前端完整 build（含 3 项 i18n 检查和 TypeScript）、Linux amd64 嵌入前端二进制构建通过。golangci-lint 2.14.0 检查上述六个完整 Go 包为 0 issues；diff --check 通过。

Windows service 全包测试仍只有此前台账记录的 `TestCommandCodeGatewayPassesThroughCatalogProtocols`（模型目录测试基于 UnixNano 产生的地址重复）和 `TestOllamaProbeCallback_StaleLongDoesNotOverrideNewShort`（短时间窗口 / CAS）失败；这两个测试及其对应实现的 Git 内容与合并前一致，没有为通过本次合并删除测试或放宽断言。不能将本轮结果描述为全量测试通过。本轮未运行 Linux 全量单元 / Docker 集成测试、真实上游付费请求或 GitHub CI；后续发行继续由未改动的完整 CI / Security 门禁决定。

## 2026-10-24：CSV 自动读取主站价格与历史日志来源

用户同意简化 CSV 补记流程，要求尽量不手配售卖规则。此轮核心在 `dev` 本地实现，配套运营编排和界面仍放在 extra。保留此前独立的生产镜像通道台账及服务器改动清单；没有推送、发行、部署、连接生产或补扣真实用户。

| 文件 | 本轮增量与后续集成要求 |
| --- | --- |
| `backend/internal/service/billing_recovery_import.go` | 新增管理员核对的结构化 `log_source` DTO 和可选只读来源接口，不扩大已有通用 store 接口；CSV 解析、匹配和原文件读取仍在运营中心。 |
| `backend/internal/repository/billing_recovery_import.go` | 提取历史来源检查，新增只读 ImportSource；完整失败日志可创建 `historical-log:` review 案件，不向 ops_error_logs 造记录。校验服务端 ID、用户 / Key / 账号 / 分组、模型、结束时间 / 耗时 / 失败状态及原请求期限；排除已有原生用量、持久意图和 dedup / 归档，包含原生 client: 前缀。预留仍复用账号锁、同账单 / 同来源双重唯一和原事务，来源固定为 log:服务端ID，无新迁移。 |
| `backend/internal/service/billing_recovery_import_quote.go`（新增） | 只读导入报价：优先保持历史快照；缺失时复用原生模型 / 渠道定价、区间 / 时间策略、用户 / 原分组倍率和账号成本规则。Key 后来迁移分组仍读取原分组。不支持的已有快照、未知模型 / 价格或归属不一致拒绝报价，不能用当前价掩盖已知维度。返回 immutable_dispatch_snapshot / current_sub2api_prices 和报价时间；当前报价只改临时对象，不写快照、案件、预留或资金。 |
| `backend/internal/service/billing_recovery_pricing.go` | 提取现有价格计划及报价共用函数，运输前快照和原异常报价继续使用相同原生计算；自动恢复依旧不代入当前价。 |
| `backend/internal/handler/billing_recovery_handler.go` | 原管理员边界下新增 POST /api/v1/admin/billing-recovery/imports/quote，接受已核对来源与 Token；复用原生 Key / Gateway 服务，无依赖时 503，不在注册路由时解引用空 Gateway。 |
| `backend/internal/service/billing_recovery_usage.go` | 首次历史日志 staging 与历史错误享有相同的一分钟豁免，仍强制原请求超过独立期限加一分钟；资金继续走 Restore / review / 原生 Apply。 |
| `backend/internal/service/billing_recovery_import_quote_test.go`（新增） | Claude / OpenAI / Gemini 原生报价、分组 0.9 / 用户 0.6 覆盖、账号 0.79 不叠乘、历史快照保持、已知不支持维度 / 归属阻断、Key 迁组后原分组报价及不修改原快照。 |
| `backend/internal/repository/billing_recovery_import_test.go` | 只读日志检查、日志 staging、防重归档及 ID / 摘要 / 状态 / 时间 / 模型 / 混合来源校验；预览不能创建案件或资金效果。 |
| `backend/internal/handler/billing_recovery_handler_test.go`、`backend/internal/service/billing_recovery_test.go` | 新报价接口输入 / 空依赖返回及不预留来源，历史日志 staging 豁免不能绕过原请求期限。 |
| `CUSTOMIZATIONS.md` | 更新当前价格例外和本节，配套说明见 extra 的 operations-center/docs/billing-csv-import.md。 |

本节取代此前“缺历史快照必须手填历史价格”的 CSV 默认操作要求，但不会更改过去记录、已有快照或后台核查草稿策略。当前价不能证明请求发生时的售价；运营中心必须显式展示来源和报价时间，最终人工确认包含这一项。原有人工历史规则仍是可选覆盖，不能覆盖明确不支持标准 Token 的快照。用户扣费按主站标准费乘用户 / 分组倍率，账号倍率仅参与账号成本 / 限额，不能把上游支出换算直接当售价。

日志摘要为完整关联事件的 SHA-256，属于受管理员认证保护的来源声明，不是签名。核心不独立读取或认证原文件；运营中心须从本机只读挂载提取完整关联、核对原生实体，并在首次确认前重读证据。核心在确认时再次校验和原生去重，后续仍由同一 Apply 事务写使用记录、dedup 和资金。价格缺失、来源 / 订阅有歧义或未知 Token 仍待核查。主站与运营中心需配套升级；此次无数据库迁移或发行通道改动。

验证：service / repository / handler / routes 的 BillingRecovery、UsageBilling、RecordUsage 专项通过（routes 无命中用例）；新报价接口及 Key 迁组回归另行通过。4 项真实 PostgreSQL/PGlite 回归、Windows server 构建通过。golangci-lint 2.14.0 以原 CI 默认配置检查三个完整包，结果 0 issues；额外启用 unit 标签的 lint 曾暴露原生未改动测试文件中的依赖 / 返回值 / staticcheck 等告警，未因此改动它们，本轮新增报价文件的一处无效赋值已修复并复测。不能将这些结果表述为 Linux 全量测试或所有标签 lint 均通过。配套运营中心 225 项全量测试、check、四组桌面 / 手机模拟流程及离线回放通过，详细数量见 extra 台账；两仓库 diff --check 通过。尚未进行实际服务器定价读取、真实资金结算、Docker / Linux CI 或 GitHub 发布，新增与修改文件均保留本地供后续审核。

用户随后授权验证后推送 GitHub。提交本节的 CSV 自动报价、完整日志来源及配套测试 / 台账到 dev，extra 配套提交到 main；沿用现有 CI / Security 门禁及 fork Release / GHCR 通道，不修改发行配置。推送前 fetch 确认两仓库均与目标分支同步，复核改动及验证记录。此前独立的生产镜像通道维护台账和服务器改动清单继续保留本地未提交，不纳入本次功能提交；不部署服务器或操作真实资金。推送成功只代表代码已上传，构建 / 发布结果以本次完整 SHA 的 Actions 为准。

## 2026-10-25：CSV 整组核查的原子认领和历史防重接口

用户要求实现自动候选核查和去重。本轮主站保持 `dev`，编排 / 匹配 / 界面仍在 extra 的 `operations-center`；仅本地代码和隔离测试，不提交、推送、发布、部署、连接生产或补扣真实用户。保留此前独立生产镜像通道台账及 `docs/server-changes-2026-10-23.md` 的既有未提交改动。原 fork Release / GHCR 通道不变。

| 文件 | 本轮增量与后续集成须保留的约定 |
| --- | --- |
| `backend/internal/service/billing_recovery_import_group.go`（新增） | 可选整组 store / DTO 和历史认领 DTO，不扩大原通用恢复接口；2–20 成员，冻结原生使用记录全部计费维度，Restore 仅允许改变原生生成的 ID / 请求 ID / 入账时间及加载关系。不把稳定记账分配称为历史逐笔确定关联。 |
| `backend/internal/repository/billing_recovery_import_group.go`（新增） | 复用按上游账号的事务 advisory lock，先检查所有历史来源，再在同一事务中认领所有账单 / 来源、冻结 usage 并保存原组回执。任一冲突全组回滚；同组同摘要重试返回原案件，不重新分配。只允许相同用户 / Key / 分组 / 账号的余额历史来源，无持久 CaseID；配置 Key 滚动限额或账号配额则阻断。新 ImportClaims 按账号及 CSV 历史请求时间返回认领与实时案件状态，最长 34 天 / 20,000 条，超限失败，不能返回部分结果冒充完整防重。 |
| `backend/internal/repository/billing_recovery_import.go` | 提取输入验证、稳定本地引用及事务内 reserveImport，单条 API 保持原有行为；组复用相同的历史来源检查、双唯一约束、原生用量 / 持久意图 / dedup 热表和归档保护。多种来源描述同一请求时，后续 staging 检查可阻断并回滚，不允许按别名重复计费。 |
| `backend/migrations/custom_20261025_billing_import_groups.sql`（新增） | 只新增 `sub2api_custom.billing_import_groups`，按账号 + 组身份唯一，保存摘要和原案件回执；不改写已执行迁移，不新增余额或资金表。必须随主站升级应用，不能仅升级运营中心。 |
| `backend/internal/handler/billing_recovery_handler.go` | 在原管理员边界内新增 POST `/api/v1/admin/billing-recovery/imports/reserve-group`、GET `/api/v1/admin/billing-recovery/imports/claims`；验证输入，缺可选 store 返回 503。仅认领，不创建 usage_logs 或扣费。 |
| `backend/internal/service/billing_recovery_import_quote.go` | 原只读报价附加可选 group_audit_blocked，Key 滚动限额 / 账号配额不满足组内时间分配等价时解释阻断；普通单条报价和历史快照 / 当前价语义不变。 |
| `backend/internal/service/billing_recovery_usage.go` | 原 Restore 流程校验整组冻结用量与金额，并拒绝认领后新启用的 Key 滚动限额 / 账号配额；不修改原生 Apply、订阅或差额资金事务。仍先保存核定使用记录，再 review → prepared → settle。 |
| `backend/internal/repository/billing_recovery_import_group_test.go`、`backend/internal/service/billing_recovery_import_group_test.go`（新增） | 整组全成 / 全回滚、原回执幂等、摘要冲突、重复账单 / 来源、归属 / 订阅 / 金额 / 人数边界、原生重复踪迹与配额阻断，所有用量 / 价格维度冻结、非法冻结快照及认领后改限额的回归。 |
| `backend/internal/handler/billing_recovery_handler_test.go` | 新接口输入校验、可选 store 缺失及按历史时间读取认领的回归。 |
| `.github/billing-recovery-sql.test.cjs` | 执行实际新迁移和仓库 SQL，验证末成员唯一冲突使全部新案件 / 认领回滚、冻结用量、历史时间 / 账号过滤及 Key 限额阻断；认领期间 usage 和余额均不变。保留原 4 项 PostgreSQL 回归。 |
| `CUSTOMIZATIONS.md` | 记录本轮所有核心改动。完整操作和 extra 约束见配套 `operations-center/docs/billing-csv-import.md`。 |

主站只负责原子认领、不可变金额和原生计费，整组覆盖 / 可能对应方式报价等价的证明在运营中心。完整日志仍是受管理员认证保护的来源声明，核心不认证原文件；管理员须核对最终 CSV 和归属。历史报价没有快照时明确使用当前主站价，不将采购支出换算为用户售价。

整组认领是原子的，逐笔资金结算不是全组同一事务；部分成功后必须沿原组 / 原案件恢复，已经结算的成员保持幂等。无法证明完整覆盖、仍有组外缺耗时请求、订阅、滚动额度、跨用户或历史关联缺失都继续待核查，不能为了增加可补记条数放宽。历史快照回放未新增可补记组，不代表实现失效或已经确认了全部亏损。

验证：BillingRecovery / UsageBilling 的 repository / service / handler 专项，以及最终冻结快照 / 新 HTTP 接口专项通过；5 项实际 PostgreSQL/PGlite 迁移和 SQL 回归通过。以原 CI 默认配置运行 golangci-lint 2.14.0 检查上述三个完整包为 0 issues，最终 Windows server 构建和两仓库 diff --check 通过。配套运营中心完整 252 项测试、npm run check、六组桌面 / 手机 Playwright 模拟和两种离线回放通过；回放保护结果见 extra 同日台账。未运行 Docker / Linux 全量 CI、生产原生报价或实际扣费，本地演示只有模拟数据。新增源文件和迁移仍未提交或推送。

用户随后明确授权提交并推送 GitHub。本轮将整组认领、历史防重接口、迁移和回归提交到主站 dev，配套匹配版本 4 / 5 / 6 与运营编排提交到 extra main。推送前 fetch 确认两仓库均与远端同分支同步；独立生产镜像通道台账、服务器维护清单和其他项目文件继续保留本地。沿用现有 CI / Security 和镜像发布流程，构建与发布结果以此次提交 SHA 的 Actions 为准；本次不部署服务器或操作真实资金。完整功能升级需主站和运营中心同时更新。
