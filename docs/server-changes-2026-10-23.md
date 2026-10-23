# 2026-10-23 服务器改动清单

服务器：`154.36.173.220`。时间均为北京时间（UTC+8）。

本清单对应本次计费修复镜像上线，以部署前文件备份、数据库备份、部署事件和上线后实际状态为依据。清单及核对结果不包含密码、管理员 API Key、数据库口令或加密配置明文。源码层面的个性化改动另见两个工程的 `CUSTOMIZATIONS.md`。

## 1. 修改的服务器配置文件

### 1.1 主站 Compose

文件：`/root/sub2api-deploy/docker-compose.yml`。

| 配置项 | 修改前 | 修改后 |
| --- | --- | --- |
| `services.sub2api.image` | `migration20261021/sub2api:frozen` | `ghcr.io/fjiangming/sub2api:latest` |
| `services.sub2api.pull_policy` | `never` | `missing` |

对备份和现文件进行完整 YAML 结构比较，仅以上两个配置项不同。文件经过 YAML 重新序列化，格式、注释和引号可能与原件不同。

原文件备份：`backups/billing-fix-20261023072212/files/root/sub2api-deploy/docker-compose.yml`，相对于 `/root/sub2api-deploy/`。

### 1.2 运营中心 Compose

文件：`/root/sub2api-extra/operations-center/compose.yaml`。

| 配置项 | 修改前 | 修改后 |
| --- | --- | --- |
| `services.operations-center.image` | `migration20261021/sub2api-operations-center:frozen` | `ghcr.io/fjiangming/sub2api-extra:operations-center-latest` |
| `services.operations-center.pull_policy` | `never` | `missing` |

完整 YAML 结构比较也只有这两个配置项不同。原文件位于本次备份目录的 `files/root/sub2api-extra/operations-center/compose.yaml`。

`pull_policy: missing` 允许容器首次缺少镜像时拉取。服务器已有同名 latest 时，后续更新仍须显式执行 `docker compose pull --policy always <服务名>`，然后 `up -d --no-deps --wait <服务名>`，才能获得刚发布的镜像；单独执行 `up` 不保证更新 latest。

### 1.3 运营中心加密运行配置

Docker 卷：`sub2api-extra_operations-center-data`，容器内目录：`/app/data`。

宿主机文件：`/var/lib/docker/volumes/sub2api-extra_operations-center-data/_data/settings.enc.json`。

在已有加密配置中增加 `sub2api` 配置块，配置后台持久认证：

| 字段 | 本次配置 |
| --- | --- |
| `sub2api.adminToken` | 复用服务器已有的原生管理员集成 Key，加密存储；不在清单展示值 |
| `sub2api.adminEmail` | 空字符串 |
| `sub2api.adminPassword` | 空字符串 |
| `sub2api.clearPersistentCredentials` | `false` |

原因：原有环境变量中的管理员邮箱密码对应开启了 2FA 的账号，后台任务无法无人值守完成登录。新版运营中心支持原生 `admin-` Key，通过 `x-api-key` 认证，从而恢复后台每 30 秒的待办读取。

`settings.key` 与部署前备份逐字节一致，继续使用原加密密钥。既有全局管理员集成 Key 未轮换，管理员 2FA 未关闭。运营中心 `.env` 原件未修改，运行时加密配置提供新的认证方式。

修改前卷备份：`operations-center-before-admin-key.tgz`；最初部署前卷备份：`operations-center-data.tgz`。恢复加密配置时应成对保留 `settings.enc.json` 和 `settings.key`。

## 2. 新增或由启动自动生成的文件

### 2.1 运营中心数据卷

新增文件：`/var/lib/docker/volumes/sub2api-extra_operations-center-data/_data/user-messages.json`，容器内为 `/app/data/user-messages.json`。

这是新镜像启动时自动初始化的用户消息存储文件，创建时为空列表，并非计费补扣数据。已有 `cost-ledger.json` 与部署前备份逐字节一致。

### 2.2 本次专用备份及执行记录目录

新增目录：`/root/sub2api-deploy/backups/billing-fix-20261023072212/`。核对时约占用 **8.2 GiB**（`du -sh` 显示 `8.2G`），包含完整恢复演练副本和失败尝试记录。目录权限为 `0700`，供 root 管理；备份内包含原配置和业务数据。

下表所有相对路径均位于该目录内：

| 新增文件或目录 | 用途 |
| --- | --- |
| `files/` | 按服务器绝对路径层级保存的原 Compose、env 和 Nginx 配置，共 8 份原文件 |
| `sub2api-before-update.dump` | 正式切换前 PostgreSQL 全库逻辑备份 |
| `postgres-roles-before-update.sql` | 正式切换前数据库角色及全局权限备份 |
| `sub2api-data.tgz` | 主站 `/root/sub2api-deploy/data` 备份 |
| `operations-center-data.tgz` | 运营中心原数据卷备份 |
| `operations-center-before-admin-key.tgz` | 切换后台认证前追加的运营中心数据卷备份 |
| `redis-data.tgz` | Redis 持久化数据备份 |
| `backup-manifest.json`、`backup-manifest-final.json` | 备份大小和 SHA256；最终清单包含认证切换前追加备份 |
| `deploy.py` | 本次部署专用脚本，含备份、演练、排空、切换和验证逻辑；不是常驻服务 |
| `state.json`、`status.json`、`events.jsonl`、`commands.log` | 原容器与挂载身份、最终阶段、部署事件和命令执行记录 |
| `prepare.log`、`prepare.pid`、`rehearse.log`、`rehearse.pid`、`switch.log`、`switch.pid`、`finish-operations-auth.log`、`finish-operations-auth.pid` | 各阶段的执行日志和历史任务 PID 文件；PID 文件存在不表示任务仍在运行 |
| `maintenance-owned.json`、`switch-started.json`、`backup-complete.json`、`switch-success.json` | 维护标记归属和阶段完成凭据 |
| `business-before.json`、`business-after.json` | 暂停流量期间业务数量、余额摘要和计费总额核对结果 |
| `operations-before-auth-update.json`、`operations-auth-update-success.json`、`billing-probe.json` | 运营中心认证切换及 API 探测结果 |
| `final-deployment-verification.json` | 最终镜像身份、挂载、数据库/Redis 状态、实际后台轮询和真实请求结算验收报告 |
| `missing-usage-records.copy.sql`、`usage-history-comparison.json`、`expired-records-restored.json`、`usage-diff-export.log` | 5 条零元历史记录的备份定位、比较和原样恢复记录 |
| `rehearsal.dump`、`postgres-roles.sql` | 完整数据库恢复演练的备份输入 |
| `rehearsal-app-data/`、`rehearsal-operations-data/`、`rehearsal-postgres/` | 演练时的独立应用、运营中心和数据库副本；保留在备份目录，不挂载给生产服务 |
| `rehearsal-core.env`、`rehearsal-operations.env`、`rehearsal-postgres.env` | 演练容器专用环境文件，包含私有配置，权限 `0600` |
| `rehearsal-success.json`、`rehearsal-restore.log`、`rehearsal.dump.log`、`postgres-roles.sql.log`、`postgres-roles-before-update.sql.log`、`sub2api-before-update.dump.log` | 恢复演练及备份执行结果，部分成功日志为空文件 |
| `*.first-switch-attempt`、`*.first-attempt`、`rehearsal-postgres.first-attempt/` | 失败尝试留下的原始备份、恢复日志和独立数据库副本，已保留 |
| `__pycache__/` | 导入本次 Python 部署脚本时自动生成的字节码缓存 |
| `server-schema-change-audit.json` | 本次新增数据库对象、迁移及分组生图开关差异 |
| `server-files-change-audit.json` | 配置逐字节/结构比较、卷文件比较和备份目录一级文件清单 |
| `SERVER-CHANGES.md` | 本清单的服务器副本 |

演练中的主站、运营中心、PostgreSQL、Redis 临时容器及 `sub2api-billing-upgrade-rehearsal` 内部网络均已删除；演练产生的目录和备份继续保留。

## 3. 数据库新增对象和数据配置变化

### 3.1 计费恢复迁移

应用了 `custom_20261023_billing_recovery.sql`，新增 schema：`sub2api_custom`。

| 对象 | 用途 |
| --- | --- |
| `sub2api_custom.billing_recovery` | 持久保存实际 dispatch 意图、观察到的用量、使用记录、历史定价命令、状态、重试及补记链 |
| `sub2api_custom.billing_recovery_events` | 保存核查和结算审计事件 |
| `custom_billing_recovery_pending` | 待处理事项的检索索引 |
| `billing_recovery_api_key_id_native_request_id_key` | API Key 与原生资金事件的唯一约束/索引 |
| `billing_recovery_pkey`、`billing_recovery_events_pkey` | 两张新表的主键索引 |
| `billing_recovery_events_id_seq` | 审计事件 ID 序列 |

新表还包含迁移定义的状态校验和外键约束。`public.schema_migrations` 记录本次迁移文件名和 checksum。对升级前备份目录与现有数据库的表、序列和索引比较，新增关系对象都位于 `sub2api_custom`，原有关系对象无删除。

### 3.2 镜像携带的原作者迁移

同时应用了原作者已有的 `246_enable_gemini_image_generation_groups.sql`，将 Gemini/Antigravity 分组的 `allow_image_generation=false` 统一置为 `true`。

从部署前全库备份提取原值，与上线后实际数据库比较，本服务器有以下 **4 个分组开关改变**：

| 分组 ID | 分组名称 | 平台 | 修改前 | 修改后 |
| --- | --- | --- | --- | --- |
| 60 | gemini逆向 | gemini | 关闭生图 | 开启生图 |
| 62 | Gemini 反重力 | gemini | 关闭生图 | 开启生图 |
| 68 | Gemini Ultra | gemini | 关闭生图 | 开启生图 |
| 71 | Gemini Vertex | gemini | 关闭生图 | 开启生图 |

这项改动由新镜像的原生数据库迁移在启动时执行。上述两个迁移在北京时间 2026-10-23 07:36:18 写入 `schema_migrations`；迁移记录由 296 项增加至 298 项。

### 3.3 历史记录和正常业务写入

主站启动时原有的 90 天日志保留任务清理了 5 条刚过期零元使用记录（ID `36170` 至 `36174`）。已从正式切换前备份按原 ID、原字段恢复，没有重新扣费，90 天保留策略继续生效。

最终验收时原有 **504809 条使用记录**完整保留。暂停流量期间用户余额摘要、累计已计费金额及业务身份核对通过。恢复接单后的余额、使用记录、去重记录、恢复表、缓存及审计日志会随真实请求继续变化。

## 4. 镜像和容器变化

| 服务 | 上线后的镜像和身份 | 操作 |
| --- | --- | --- |
| 主站 `sub2api` | `ghcr.io/fjiangming/sub2api:latest`；版本 `0.2.17000501`；提交 `ad94ed860b6f20ef0827cc1943534fa6c37bf0d2` | 替换应用容器，沿用原数据挂载和网络 |
| 运营中心 `sub2api-operations-center` | `ghcr.io/fjiangming/sub2api-extra:operations-center-latest`；提交 `642ad0892d14e5b625f579f284ec25bf24e4203b` | 先上线计费核查镜像，再切换到支持原生管理员 Key 的新镜像，沿用原卷 |
| `sub2api-postgres` | 原冻结镜像、原容器、原启动时间 | 未重建、未重启 |
| `sub2api-redis` | 原冻结镜像、原容器、原启动时间 | 未重建、未重启 |

主站更新前实际运行程序为 `0.2.17`；旧冻结镜像中保留的 `0.1.160` 元数据不代表当时运行程序的版本。服务器本轮通过拉取镜像更新应用，未使用 `git pull` 加现场源码构建来部署。

镜像内已带有 fork 更新源、计费持久交接和运营中心“计费核查”界面。相关源文件位于镜像内部，未逐个替换服务器宿主机源码文件。旧冻结镜像与下载的镜像缓存继续保留，本轮未执行镜像清理。

## 5. 临时改动及运行时文件

临时创建了 `/etc/nginx/aihub-maintenance.flag`，以启用服务器已有的维护开关，暂停新请求并等待在途请求排空；完成后已删除。现有 Nginx 配置文件及维护规则原件均未改动。

为备份执行过 Redis `SAVE`，原路径 `/root/sub2api-deploy/redis_data/dump.rdb` 更新为当时的持久化快照，缓存数据未清空。主站 `/root/sub2api-deploy/data/logs/` 和 Docker 日志正常继续写入及轮转。

未增加常驻独立计费服务、systemd 服务、系统定时任务或生产监听端口。计费恢复后台调度位于既有运营中心进程内。

## 6. 与部署前备份核对一致的配置

以下服务器文件均已逐字节与部署前备份核对一致：

- `/root/sub2api-deploy/.env`
- `/root/sub2api-extra/operations-center/.env`
- `/root/sub2api-extra/compose.services.env`
- `/root/sub2api-extra/docker-compose.yml`
- `/etc/nginx/conf.d/migrated-services.conf`
- `/etc/nginx/snippets/aihub-maintenance-switch.conf`
- `/root/sub2api-deploy/data/config.yaml`
- `/root/sub2api-deploy/data/.installed`
- `/root/sub2api-deploy/data/model_pricing.json`
- `/root/sub2api-deploy/data/model_pricing.sha256`
- 运营中心卷内的 `settings.key`、`cost-ledger.json`

本轮未向生产环境变量追加 `BILLING_RECOVERY_*` 或 `OPERATIONS_CENTER_BILLING_RECOVERY_*`，生效的是新程序默认值：核心计费保护开启、独立处理期限 1800 秒，运营中心后台恢复开启、每 30 秒轮询一次。

## 7. 验收与后续更新依据

最终上线验收时间为北京时间 2026-10-23 07:55:35，主站与运营中心内外网健康检查均通过。真实流量已有 42 笔结算，已结算事项全部对应使用记录和原生去重记录，已结算缓存待办为 0；实际运营中心进程最近 3 分钟连续 6 次读取待办成功。详细数值属于当时快照，之后会随业务变化。

验收时发现的上游 524 超时已保留核查事项。后台自动恢复仅处理已准备结算或缓存待办，不会在缺少上游用量证据时直接补扣余额。

后续服务器更新参考相邻工程的 `operations-center/docs/billing-recovery-update.md`。以两份 Compose 的 fork 镜像地址为准，显式拉取新镜像并仅重建对应应用服务，继续保留原业务数据挂载、数据库和 Redis。

本清单、`server-files-change-audit.json` 和 `server-schema-change-audit.json` 是部署完成后为整理改动清单追加的文件，不会触发应用重启或更改业务配置。

## 8. 2026-10-23 追加：延长上游同步请求等待时间

本节为后续经用户授权的独立配置调整，不改变前文对首次部署的历史记录。北京时间 11:10:19 完成切换并恢复接入，沿用主站版本 `0.2.17000701`、提交 `050d5b96a3ebb1c8cd9e81be2179848f7b3e0449`。

### 8.1 修改的文件

`/root/sub2api-deploy/docker-compose.yml` 仅在 sub2api 服务的 environment 列表新增：

```yaml
    - GATEWAY_RESPONSE_HEADER_TIMEOUT=1800
    - BILLING_RECOVERY_TIMEOUT_SECONDS=1800
```

第一项将应用等待上游响应头的原生默认上限由 600 秒延长到 1800 秒，解决已核实的 11 分钟同步请求在 600 秒处被本机提前中止的问题。第二项把原本默认 1800 秒的计费恢复独立处理期限显式写入部署配置。两项配置在后续仅拉取镜像并重建容器时继续生效。

`/etc/nginx/conf.d/migrated-services.conf` 仅调整以下三个域名中代理到 `http://127.0.0.1:9000` 的 `location /`：

| 域名 | 三项超时的原值 | 三项超时的新值 | 连接超时 |
| --- | --- | --- | --- |
| `epoint2api.fjmooo.cn` | 600s | 1860s | 保持 30s |
| `aihub.fo2.us.ci` | 1200s | 1860s | 保持 60s |
| `aihubdirect.fo2.us.ci` | 600s | 1860s | 保持 30s |

三项指令为：

```nginx
proxy_send_timeout 1860s;
proxy_read_timeout 1860s;
send_timeout 1860s;
```

这些指令约束相应的读写间隔；不延长 Cloudflare 或下游客户端自身的超时。主站 `/monitor/` 和其他站点的代理规则、DNS、证书、端口及访问控制均未调整。Nginx 文件尾部分混合换行随写入统一为 LF，无规则变化。

### 8.2 稳定性和数据保护

- 先备份、校验候选 Compose 和 Nginx 配置，再启用现有维护标记阻止新请求进入；保持原容器处理在途请求，连续 25 秒确认在途和等待队列均为零后才进入切换。
- 排空后完成 PostgreSQL 自定义格式全库备份、角色备份和应用 data 归档，校验备份目录并记录 SHA-256。
- 仅执行 `docker compose up -d --no-deps --pull never --force-recreate sub2api`；没有拉取镜像、现场构建源码、停止整个 Compose 或重建依赖服务。
- 主站镜像 ID 保持 `sha256:41b4c7b29537fe4beb9657315f8f4b6e9d0c2a256442741ed25e32039947736e`，挂载保持不变；PostgreSQL、Redis、运营中心的容器 ID、启动时间均保持不变。
- 主站启动时既有 90 天清理任务删除的 105 条过期使用记录（ID 36170–36274）已按切换前原字段恢复，保留策略未改，没有再次扣费。恢复接入前原有 506670 条使用记录、费用摘要、用户余额摘要、业务身份和迁移记录核对一致。
- `nginx -t` 成功后执行平滑 reload；主站健康和数据核对通过后删除本次维护标记。没有新增常驻服务、定时任务或生产端口。

### 8.3 新增文件与备份位置

本次所有备份、部署及核验材料统一放在：

```text
/root/sub2api-deploy/backups/timeout-20261023110218/
```

| 文件/目录 | 用途 |
| --- | --- |
| `files/` | 两份原配置及原 .env、Nginx 主配置/维护规则、应用配置/价格、运营中心部署配置的私有备份 |
| `docker-compose.candidate.yml`、`migrated-services.candidate.conf`、`nginx-validation.conf` | 经过校验的候选配置及独立校验入口 |
| `docker-compose.yml.diff`、`migrated-services.conf.diff`、`plan.json`、`original-files.json` | 配置差异、允许变更、原文件散列及权限 |
| `sub2api-before-update.dump`、`postgres-roles-before-update.sql`、`sub2api-data.tgz`、`backup-manifest.json`、`database-backup-contents.txt` | 完整数据库/角色/应用文件备份及校验依据 |
| `retention-candidates.copy`、`retention-candidates.json`、`expired-history-restored.json` | 过期历史记录的原始导出和原样恢复记录 |
| `before.json`、`business-before.json`、`business-after.json`、`schema-before.json`、`verification.json` | 容器、业务数据、迁移及切换后配置核验 |
| `live-verification.json`、`authenticated-api-verification.json`、`nginx-verification.json`、`external-verification.json` | 健康、真实结算、认证 API、实际 Nginx 和公网探测结果 |
| `deploy.py`、`events.jsonl`、`status.json`、`switch.log`、`commands.log`、`switch.pid`、`execution.lock`、`switch-started.json`、备份日志及 `__pycache__/` | 本次专用部署/失败回滚脚本、过程和状态材料；不是常驻任务 |
| `SERVER-CHANGES.md` | 本清单的服务器副本 |

备份目录权限为 0700，备份配置和日志受 root 权限保护；其中包含私有环境配置和业务数据，不应公开上传。临时 `/etc/nginx/aihub-maintenance.flag` 已删除，两个目标配置文件的原权限和属主保持不变。

### 8.4 验证和边界

两份 Compose 的完整解析结果仅增加 2 个环境变量；实际容器中两项均为 1800。实际 Nginx 三个域名的 9 条目标指令均为 1860s，候选与生效配置语法检查通过。主站及运营中心健康检查、三个域名的本机回源检查均为 200。

从本地电脑访问公网主站首页、主站健康接口、运营中心健康接口均为 200。VPS 自身通过 Cloudflare 访问这几个公网地址返回边缘层 403，未修改 Cloudflare 规则；该探测来源与本地电脑及实际接入流量的结果不同。

运营中心的持久管理员认证正常：unknown、prepared、settled、review、pending 和全部状态列表，以及 unknown 详情读取均为 200。恢复接入后的首轮核验中，9 笔已结算真实请求全部对应原生使用记录及扣费去重记录，已结算缓存待办为 0；未额外发起收费推理或补扣来做测试。

本次修复的是已确认的应用 600 秒等待上限和三处代理的较短等待时间，没有部署本地其他未提交的源码改动，也没有发布新镜像。没有执行受控的超过 10 分钟真实上游推理，因此不能把健康和短请求验收等同于所有长请求场景已验证。上游始终不返回最终用量、超过独立处理期限或连接被上游终止时，仍需保留核查事项并凭证据处理；历史遗漏记录不会由这次配置变更自动补扣。

## 9. 2026-10-23 追加：清理部署演练与失败材料

用户审核文件清理清单后授权删除。复核所有容器挂载和部署进程后，仅删除三个专用备份目录内清单明确列出的 46 个条目，释放约 11.721 GiB；主要为三套独立 rehearsal-postgres 副本、演练应用/运营中心副本、演练 dump、失败切换副本、旧脚本草稿和历史 PID/缓存。上述第 2、8 节列出的相关演练/临时材料是清理前的历史记录，现不再全部存在。

三个备份父目录和正式备份、files/ 原配置、deploy.py 依赖链、核验报告仍保留，合计约 0.783 GiB。清理前后保留条目逐文件 SHA-256 及生产配置散列核对一致；主站、PostgreSQL、Redis、运营中心的容器 ID、镜像、启动时间、重启次数及健康状态保持一致，未重启任何服务或修改业务数据。主站和运营中心本机及公网健康接口均为 200。

逐项执行回执保存在 `/root/sub2api-deploy/backups/timeout-20261023110218/cleanup-20261023.json`，包含删除路径、占用、保留文件散列及前后服务状态。没有清理 Docker 镜像、生产数据卷、运行日志或其他既有备份目录。
