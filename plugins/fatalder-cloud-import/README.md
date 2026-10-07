# Fatalder 云导入 · neomega 原生插件

管理员在游戏内选择建筑、查看 Worker 报价并确认，插件协调 service_center 机器人 OP 授权、建筑导入与离服握手，默认保留授予的 OP。业务运行于 neomega Python 插件宿主；建筑解析、计费、建造仍由 Fatalder Worker 的共享执行核心负责。

## 安装与配置

需要包含 managed_operator 握手的新版 Worker、已连接目标租赁服且具备授予权限能力的 neomega Agent，以及支持配置与权限管理的 **Host API 1.8**（worker wire 仍为 **1.0**）。Worker 目标配置必须允许 service_center，并允许所配置服号。插件在 manifest 中申请玩家列表、原子玩家观察、命令、聊天事件及 **185（RequestPermissions）** 发包权限；服主通过管理向导审阅并明确保存，无需猜测安装项 ENV 授权名单。宿主必须具备相应来源且允许世界写入；来源缺失时，先按诊断配置/升级并重启宿主。权限包按实体唯一 ID 定位，不使用玩家名字发送 op/deop。

同一租赁服只安装一个本插件实例；该实例同时只占用一个导入槽。

1. 在仓库根目录运行 `python3 plugins/fatalder-cloud-import/tools/package.py /your/output/cloud-import.zip`。
2. 使用 neomega 提供的管理脚本安装：`python3 runtime-admin.py --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" install importer /your/output/cloud-import.zip`。
3. 运行 `python3 runtime-admin.py --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" configure importer`，按向导填写 HTTPS Worker 根地址、API Key、Worker 目标配置 ID、租赁服号与管理员 UUID 数组。
4. 运行 `python3 runtime-admin.py --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" permissions importer`，审阅用途、申请与缺口，输入“同意”保存权限；在后续启动提问处回车，保持停止。再执行同一管理脚本的 `doctor importer`，检查通过后独立执行 `start importer`。如需宿主重启后自动加载，再明确执行 `enable importer`（该命令也会启动尚未运行的插件）。API Key 存在宿主管理的私有密钥文件，普通配置只保存 secret 引用；不把真实 Key 写进本目录或安装包。
5. 将建筑文件放入插件**数据目录**的 `imports/`，不是安装目录。文件名含空格时用双引号包裹。也支持 `名称.ref.json`：内容为 Worker 已存对象的完整 ObjectRef（object_id、sha256、size_bytes、display_name），sha256 必须有 `sha256:` 前缀。

插件要求普通 HTTPS 证书验证；不跟随重定向、不读取系统代理。数据目录内 `downloads/` 保存已校验的 Worker 转换产物。首次启动后可由服主管理数据目录文件，不提供任意网络 URL 导入。

有进服密码时，将 `rental_access` 配为 `{"passcode":"secret:rental_password"}`，通过管理脚本 `secret-set` 私有写入该密钥；无密码时保持 `null`。密码仅在启动和恢复请求发出时读取，不写入准备请求或持久化任务回执。Worker 状态采用大写协议值，插件将 `READY` 保持为待确认报价，并兼容 Go 的 RFC3339Nano 截止时间。

## 游戏命令

- `!导入 列表`：显示最多 40 个本地文件。
- `!导入 "房屋.bdx" 100 64 100 overworld`：上传/引用建筑并获取报价。维度可用 overworld、nether、the_end。
- `!导入 确认 <token>`：仅在报价有效期内启动；未确认不会开始建造。
- `!导入 状态`：查看任务、恢复标志与权限租约状态。
- `!导入 暂停`、`!导入 继续`、`!导入 取消`：控制当前任务；取消仍需等待权限会话结束确认。
- `!导入 恢复`：明确恢复 Worker 标为 recovery_required 的任务。若准备/启动响应丢失，使用持久化的原请求和原幂等键核对；不会创建新的扣费请求。

只处理当前玩家名能唯一对应到 UUID 白名单的聊天消息。报价金额和币种直接来自 Worker，不自行换算计费单位。默认导入参数由 `shared/flow` 导出，不另设第二套产品默认值。

## 状态与边界

准备/启动请求先保存再发送；进程重启仅恢复观察，不自动开始建造。聊天请求按宿主事务确认接收；如果进程恰在确认接收后、保存具体任务意图前停止，该命令不会自动重放，需要管理员检查状态后重新操作。网络失败不等于动作失败，应先查询状态。普通 SSE 断线不会取消任务，重连从已保存游标继续。

Worker 提供真实连接 UUID、名字、session/attempt 与目标服绑定。插件用 Agent 新玩家观察核对身份和权限后才授予 OP；已有 OP 原样保留。权限动作使用持久化确定性回执，只在操作成功且权限读回成功后 ACK。未知结果保留待核查租约，不重新发授权动作、不猜测成功。`revoke_operator_on_completion` 默认 `false`：新任务在 Prepare 冻结 retain，完成实际身份验证与授权后，Worker 在结束、取消或失败时持久记录 retained 并保留 OP，无需插件回应清理 ACK；重连核对原 job/session 的可信记录后释放租约，不把未知授权结果标为成功。

如果服主主动配置 `revoke_operator_on_completion: true`，仅对确认由插件授予的 OP 恢复授权前的权限等级与可请求的低 8 位能力标志，读回确认后才结束会话。策略随授权租约保存；修改配置作用于新会话，丢失 ACK 后恢复仍沿用原策略，防止意外撤权。最近一次保留记录在持久状态的 `last_operator_session` 中。

Worker 授权最多等待 120 秒；撤权策略的会话结束确认也最多等待 120 秒，默认 retain 结束不等待插件清理 ACK。开启撤权策略时，插件离线、宿主观察 epoch 改变、机器人已离服、回执未知或撤权超时，均可能留下待人工核查的 OP；此时不能声称已恢复原权限。请核对实际机器人 UUID/实体 ID/权限和任务回执，处理完成前保持导入槽锁定，不清空状态绕过检查。默认保留策略在核对原会话的可信 retained 记录后释放权限租约，任务终态后可继续下一个导入。已有任务绑定 Worker 地址、密钥引用和目标服；处理中不要更换这些配置。密钥文件并非对同系统用户恶意插件的安全沙箱。

本版没有实服验收记录；单元测试与 Go 构建只证明源码层行为。实服需验证管理员/非管理员、报价确认、实际导入、原有 OP 保留、默认授予后保留、可选撤权、取消、断线与重启恢复。

## 开发验证

在仓库根目录：

```sh
PYTHONPATH=plugins/fatalder-cloud-import python3 -m unittest discover -s plugins/fatalder-cloud-import/tests
```

业务默认值已作为版本化 JSON 随包发布，打包和测试不依赖 Fatalder 源码。需要同步默认值时，在已授权的 Fatalder checkout 中运行 `GOTOOLCHAIN=go1.26.0 go run "$PLUGIN_CHECKOUT/plugins/fatalder-cloud-import/tools/export-defaults.go"`，审核输出后更新 `fatalder_plugin/build_defaults.json`；当前来源为 Fatalder `1b3d3849fbaf82c7a7cd05ce958846f142e0b50f`。

修改 Settings 后，用 neomega SDK 的 `schema_for_config(Settings)` 重新导出 config.schema.json。本仓库运行插件测试与 ZIP 制品检查；修改 Worker 公共 API 时须另在 Fatalder 运行其门禁。安装包白名单排除测试、缓存、配置与密钥。

## 从 Fatalder 仓内版本迁移

独立版本 1.0.1 保持 `fatalder.cloud-import`、`state_version: 1`、配置声明和持久状态键不变，仅改变源码/制品归属及可复现打包。当前安全更新要求维护协议；旧版 1.0.1/1.0.3 无法提供可信的空闲证明，暂不支持无缝迁移。不能通过停进程、清空回执、卸载重装或改名绕过进行中任务和未知结果检查。宿主状态、事件游标和数据目录不搬入新仓库或 ZIP；任何迁移都不会自动撤销世界写入。

迁移不改变原 Worker 的 120 秒结束 ACK 行为；插件离线完成的改进由后续独立版本实现。

### 1.0.2：离线完成与旧失败核查

新建任务将 `revoke_operator_on_completion` 转为 Prepare 中冻结的 `operator_cleanup_policy`。默认保留 OP 时，Worker 验证真实会话身份并完成授权后，结束直接记录 `retained`；插件离线超过两分钟也不因缺清理 ACK 改写建造结果。重连仅查询原 job/session/目标服/终态，不重发启动；收到重复通知仍只释放一次租约。任务内部更换连接时，也消费原 session 的持久 retained 事件关闭该连接租约，槽位仍保持进行中；下一连接重新核验实际身份，旧事件不能清掉新连接租约。撤权策略继续走原清理 ACK 与权限收据。

`!导入 状态` 分开显示任务状态与内容缺项/未知；成功不代表 NBT 完整。旧 `cleanup_failed` 必须由管理员输入 `!导入 核查`，阅读原任务失败、各任务可信结果、内容缺项及保留 OP 的影响，再输入五分钟内有效的 `!导入 确认核查 token`。确认再次 GET 核对同份 job/session/结果证据，记录本地显式保留 OP 收据并释放槽位；Worker 历史 FAILED 保留，不重建、不发权限命令。缺证据/需恢复时保持占用。原版 Worker 不提供可信 task_outcomes 时不能绕过核查；先升级兼容 Worker。

状态继续使用 `fatalder.controller` / state_version 1，新增 worker_cleanup_policy、task_outcomes、review、last_operator_session 字段。新任务准入仍检查终态且无 lease；未知 prepare/start、在途任务及未核查 lease 均不可清空绕过。所有消息、轮询与事件状态更新在同一 Controller.lock 下；部署更新守卫应复用该锁。代码与离线测试不代表真服验收。

### 1.0.3：声明权限与显式授权

最低 Host API 提升至 1.1，worker wire 保持 1.0；配置与 SQLite state_version 仍为 1。升级后按 `configure → permissions → doctor → start` 检查配置、明确保存新增授权并独立启动。旧 catalog 没有确认记录时必须重新确认，不自动补齐权限；仅保存权限不会启动。先核查旧任务和权限租约，再按前述迁移边界更新。使用新的 1.0.3 制品名，不覆盖既有 1.0.1 发布包。

### 1.0.4：维护检查与安全更新

最低 Host API 为 1.2，worker wire 仍为 1.0，state_version 保持 1。插件通过 `on_maintenance` 在同一业务锁内核对任务、租约、控制请求及所有状态写者，空闲时封闭新任务；busy 无损返回，release 按同一 token 解除。维护等待不停止后台任务，也不取消远端导入。

使用运行中且支持维护协议的实例执行显式固定版本更新；先查看计划并确认候选版本、哈希与权限。未应用的配置或密钥必须先处理，停止状态不代表远端业务空闲。备份或验证失败发生在 Drain 前，激活失败需按宿主诊断处理，不能把已保存当作已生效。详细命令、备份及旧版本限制见 [安全更新合同](https://github.com/OmineDev/neomega-agent/blob/main/docs/runtime/plugin-updates.md)。本版本不提供世界回滚，不自动重放未知动作；自动测试不代表实服验收。


### 1.1.0：可信前置服务接口

新增 `plugin.fatalder.cloud-import.<method>`、major 1 的七个方法。全部要求 Host 注入的真实调用安装 ID，同时必须位于配置 `service_installation_ids` 数组；默认空数组拒绝所有插件调用，既有游戏管理员配置保持兼容。每个任务归属创建它的安装，不信任请求体里的玩家名/owner。服务与聊天共用一个 Controller 和权限租约；不得再安装第二个实例绕过同服唯一槽位。服务任务由原调用安装控制，游戏管理员仍可查看状态，槽位结束后可创建后续聊天任务。

| 方法 | 参数 | 结果和行为 |
|---|---|---|
| prepare | request_key、source_name、position=[x,y,z]、dimension | 仅上传/引用本插件 imports 内文件并获取报价，不扣费或建造；不接受任意 URL |
| status | request_key | 当前或已保留完成任务读回 |
| confirm | request_key、job_id、confirm_token、confirmation_key | token 与原报价一致、未过期才将原确认 key 保存并提交 start；重复相同确认仅读回 |
| pause / resume / cancel | request_key、job_id | 原任务控制；不是世界回滚，结果以随后 status 观察为准 |
| recover | request_key、job_id、confirmation_key | 原任务明确恢复；未确认 prepare 丢回复时 key 必须为空字符串，已确认任务必须用返回的原 key；不生成新扣费 key |

prepare/status 返回 `request_key, job_id, phase, quote, confirm_token, confirmation_key, accepted_start_key, recovery_required, lease_pending, task_outcomes`。报价金额、币种、到期时间原样来自 Worker。`job_id` 在准备回复未知时为 null。网络未知不重新 prepare；查询 status 后，使用明确 recover 恢复持久化的原 prepare/start 请求。服务超时也不表示后台动作没发生。配置 allowlist 是消费付费能力的明确授权，消费者必须向用户呈现报价并在用户明确确认后调用 confirm；prepare 的成功不能替代确认。

同一 owner/request_key 的同参数调用读回，参数不同拒绝；保存最多 256 个历史服务任务防止旧 key 被当成新任务，达到容量拒绝新任务而不删除账本。Controller 状态提交前将 commit_id 写入数据目录 `fatalder-state-pending.json` 并 fsync；提交结果未知时封闭后续状态写入，重启只核对原 Host receipt 后读取持久状态，不重发或换 CAS。权限动作继续使用原有 reservation 与确定性回执协议。

P06 世界工具可将本插件作为显式可选后端：服主配置同一 Worker/服号、数据源并将 P06 的实际安装 ID 加入 allowlist，P06 声明七个方法权限。没有配置、没有授权或没有 Worker 时，报告后端未配置，不假装完成导入。版本为独立新制品 1.1.0，不覆盖 1.0.4；维护、配置、原任务和原权限租约的安全语义不变。

本次实现使用现有 64 项离线测试、配置 schema 检查和 Host ZIP checker 验证；未联系真实 Worker、未扣费、未进行游戏世界写入。真服或人工观察不是本地交付门槛，运行状态仍仅按实际回执记录。

`accepted_start_key` 仅在原确认已持久保存为 start 请求后返回，未确认时为 null；它证明对应确认请求已接纳，不代表付费任务或游戏导入完成。消费者可通过只读 status 按原 request_key、job_id 和确认 key 核对丢失回执。
