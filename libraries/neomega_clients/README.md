# neomega_clients — 前置生态类型化客户端

P01–P13 的 Python 异步客户端。安装它不会安装或启动任何 provider；没有后台线程、单例连接或跨进程对象引用。P14 展示继续使用 `neomega_display`。

## 接入

开发环境使用匹配 Host 的 `neomega_runtime` SDK；生产 ZIP 通过 `release.json` 的 `libraries: ["neomega_clients"]` 固定打包。SDK 由 Host 提供，不随库复制。

```python
from neomega_clients import PlayersClient, EconomyClient
players = PlayersClient(ctx.services)
resolved = await players.resolve({'name': '玩家名'})
# 不存在的身份会拒绝为 not_found，不生成假的 XUID。
if resolved['status'] == 'resolved':
    player_id = resolved['player']['player_id']
    fields = await players.get({'player_id': player_id})
    await players.patch({'player_id': player_id, 'fields': {'language': 'zh-CN'},
                         'expected_revision': fields['fields_revision'],
                         'request_id': business_request_id})
```

`request_id` 属于消费者的持久业务意图；每次新的业务动作生成一个，查询/恢复原动作保留它。`expected_revision` 从相应服务读取，不从其他服务复制。economy 转账同时提交来源账户 `expected_revision` 和目标账户 `target_expected_revision`。namespace 默认消费者安装身份，不能把字符串指定的 namespace 当作授权。

## 全部客户端

每个方法接收一个对应 `TypedDict`，返回 JSON 类型字典。Python 方法名与公开 export 一致，仅 `WorldToolsClient.prepare_task` 对应远程 `prepare`、`EconomyClient.receipt_request` 对应远程 `receipt`、`PlayersClient.import_fields` 对应远程 `import`，避免与通用 intent/回执工具重名。

| 客户端 | 公开方法 | 请求模型 |
|---|---|---|
| InteractionClient | register/open/respond/cancel/status/unregister | InteractionRequest |
| PlayersClient | resolve/history/get/patch/query/export/register_schema/get_schema/import_fields | PlayerRequest |
| ObservationsClient | subscribe/snapshot/refresh/status/poll/unsubscribe/read | ObservationRequest |
| CbBridgeClient | register/unregister/subscribe/poll/status | BridgeRequest |
| ChunksClient | request/read/subscribe/cancel/poll | ChunkRequest |
| WorldToolsClient | inspect/prepare_task/execute/status/cancel/resume/snapshot/fatalder_prepare/fatalder_status/fatalder_confirm/fatalder_pause/fatalder_resume/fatalder_cancel/fatalder_recover | WorldRequest |
| MusicClient | play/status/stop | MusicRequest |
| ScoresClient | objective/mutate/query/rank/sync | ScoreRequest |
| RolesClient | define/assign/revoke/check/list | RoleRequest |
| ModerationClient | ban/unban/check/list | ModerationRequest |
| EconomyClient | balance/post/transfer/hold/release/ledger/receipt_request | EconomyRequest |
| SchedulerClient | schedule/cancel/query/claim/ack | SchedulerRequest |
| MessagingClient | send/routes/status/subscribe | MessagingRequest |

完整 provider 业务约束见 `plugins/<id>/README.md` 和各 `manifest.json` input/output schema。模型中可选字段表示不同方法复用，**不表示每个方法都可以省略所有字段**。provider 权限及 schema 是运行时权威；客户端不吞掉拒绝、不构造默认成功回执。

## 公共服务名称、权限与版本

每个方法调用 `plugin.neomega.<provider>.<method>`，major 为 1。消费玩家档案查询的 manifest 片段：

```json
{
  "permissions": {
    "operations": [],
    "services": ["plugin.neomega.players.query"],
    "purposes": {"service:plugin.neomega.players.query": "读取玩家档案列表"}
  },
  "dependencies": [{"id": "neomega.players", "major": 1, "min_version": "1.0.0"}]
}
```

服务授权与业务权限独立。经济发行、治理和世界修改还受 provider 的消费者配置约束。不要仅为方便声明所有服务；生产插件只声明实际调用方法及必需 provider。

## 保留未知结果，不自动重放

便捷方法只提交一次。SDK 的拒绝、超时、失败和取消异常保持原类型；异常携带 `.intent`，已经收到 call ID 时还携带 `.call_id`。网络失败不是业务失败。

```python
from neomega_runtime.services import ServiceIntent
client = EconomyClient(ctx.services)
intent = client.prepare('transfer', {
    'request_id': business_request_id, 'currency': 'coin', 'account': source,
    'target': destination, 'amount': 100,
    'expected_revision': source_revision, 'target_expected_revision': target_revision,
}, idempotency_key=admission_id, deadline=utc_deadline)
await ctx.storage.set('pending-transfer', intent.payload())
# 上一行持久化成功后才提交，避免进程退出丢失恢复依据。
call_id = await ctx.services.submit(intent)
await ctx.storage.set('pending-transfer-call', call_id)
receipt = await client.receipt(call_id)
# pending: 继续读同一回执；失败/unknown: 读取 provider 请求结果，不能重新生成业务 ID。
result = await client.receipt_request({'request_id': business_request_id})
```

`prepare` 是纯构造；`receipt(call_id)` 仅查询 Host。持久意图还可通过 `ServiceIntent(json.dumps(saved_payload))` 恢复以核对字段，但该操作不是重放授权。Host 在线服务回执不保证跨重启持久，经济等 provider 的业务回执才是相应持久域证据。对于沒有持久业务回执的临时会话/播放，重启后报告失效，不承诺恢复继续。

## 使用组合

玩家档案用 `player_id`；位置/背包用 `ObservationsClient.subscribe` 后 `poll`，保留 `cursor`、`epoch` 和 `gap`。区块坐标 `pos` 是子区块坐标三元组，不是任意世界方块坐标；读取单元 `cell` 是子区块局部坐标。音乐 `play` 提交 `midi_base64`、玩家名数组 `targets`，可选 `speed`、`volume`、`sound`，之后以 `playback_id` 查询/停止。

持久调度先 `schedule`，消费者定时 `claim`。把 `trigger_id` 用作业务幂等标识，再 `ack` 原 `token`。未知业务结果传 `outcome: "unknown"`，调度进入暂停未知状态，不能当成功或自动补做。封禁到期判断不依赖调度通知及时到达。

`examples/prerequisite-suite` 是可以直接打包的最小服务消费者，只读取档案及自身调度任务。它无需游戏会话、不改变经济或世界数据。

## 逐方法输入参考

下列 `{}` 表示没有必填输入，`?` 表示可选；修改方法中的 ID 均由业务调用方保存。`W` 表示 `request_id,expected_revision`，不是实际 JSON 字段。每个命名空间的 revision 只属于该服务。下列参数通过对应请求 TypedDict 原样发送。

| 服务 | 方法与输入 |
|---|---|
| interaction | `register(request_id,menu_id,menu,commands?,args?,ttl_seconds?)`; `open(request_id,menu_id,player)`; `respond(request_id,session_id,expected_revision,value:string)`; `cancel(request_id,session_id)`; `status(session_id?)`; `unregister(request_id,menu_id)` |
| players | `resolve(player_id? 或 uuid?/xuid?/name?)`; `get(player_id)`; `history(player_id)`; `patch(W,player_id,fields,schema_version?)`; `query(cursor?,limit?)`; `export(cursor?,limit?)`; `get_schema({})`; `register_schema(W,schema_version,schema)`; `import_fields(W,schema_version,namespace,item)` |
| observations | `subscribe(kinds?)`; `snapshot(kinds?,player?)`; `refresh(kinds?,player?)`; `status({})`; `poll(subscription_id,cursor)`; `unsubscribe(subscription_id)`; `read(kind,sequence,offset?,limit?)` |
| cbbridge | `register(prefix,priority?,consume?)`; `subscribe(route_id)`; `poll(route_id,cursor)`; `unregister(route_id)`; `status(offset?,limit?)` |
| chunks | `request(dimension,pos,max_age?)`; `read(request_id,cell?,offset?,length?,nbt_offset?)`; `subscribe(dimension,pos,ttl?,max_age?)`; `poll(subscription_id,cursor)`; `cancel(request_id)` |
| worldtools | `prepare_task(request_key,kind,args)`; `inspect(request_key,pos)`; `execute(task_id,expected_revision)`; `status(task_id,offset?,limit?)`; `cancel(task_id,expected_revision)`; `resume(task_id,expected_revision)`; `snapshot(task_id,index?,offset?)`; `fatalder_prepare(request_key,source_name,position,dimension)`; `fatalder_status(task_id)`; `fatalder_confirm(task_id,confirm_token,confirmation_key)`; `fatalder_pause/fatalder_resume/fatalder_cancel(task_id,control_id)`; `fatalder_recover(task_id)` |
| music | `play(targets,midi_base64,speed?,volume?,sound?)`; `status(playback_id)`; `stop(playback_id)` |
| scores | `objective(W,objective,action:create/rename/remove,name?)`; `mutate(W,objective,player_id,action:set/add/sub/reset,value?)`; `query(objective,player_id?)`; `rank(objective,offset?,limit?,expected_revision?)`; `sync(W,objective,player_id)` 或 `sync(action:receipt,request_id)`；均可指定 `namespace` |
| roles | `define(W,role,action:set/remove,capabilities?)`; `assign(W,role,player_id,scope?,expires_at?)`; `revoke(W,role,player_id,scope?)`; `check(player_id,capability,scope?)`; `list(player_id?,offset?,limit?,expected_revision?)`；均可指定 `namespace` |
| moderation | `ban(W,player_id,reason?,expires_at?)`; `unban(W,player_id)`; `check(player_id)`; `list(offset?,limit?)` |
| economy | `balance(currency,account)`; `post(W,currency,account,amount)`; `transfer(W,currency,account,target,target_expected_revision,amount)`; `hold(W,currency,account,hold_id,amount)`; `release(W,currency,account,hold_id,capture?)`; `ledger(currency,account,after?,limit?)`; `receipt_request(request_id)` |
| scheduler | `schedule(request_id,schedule_id,due_at,expected_revision?,interval_seconds?,timezone?,missed?,payload?)`; `cancel(request_id,schedule_id)`; `query(schedule_id?,offset?)`; `claim(request_id,limit?)`; `ack(request_id,schedule_id,trigger_id,token,outcome)` |
| messaging | `routes({})`; `send(route_id,text,request_id)`; `status(delivery_id?)`; `subscribe(after?,limit?)` |

角色与封禁的 `expires_at` 为带时区 ISO 时间字符串或 `null`；调度 `due_at` 同样必须有时区。玩家自定义字段 revision 为 `fields_revision`；封禁 revision 从 `check.revision` 或 `ban.revision` 读取。

菜单示例：`{'title':'商店','steps':[{'id':'confirm','kind':'confirm','prompt':'确认购买？'}]}`。选择项为 `{label,value}`。权限菜单可添加 `{capability,scope,namespace}`。会话返回 `session_id,state,step,page,answers,answer_sources,revision`，消费者回应须基于最新 revision。

世界任务 `args`：备份 `dimension,pos,size,mode?`（默认 `snapshot`）；复制 `pos,size,destination`；恢复 `backup_id,destination?,dimension?,mode?`（默认 `snapshot`）；容器 `pos,slots,anvil_pos,workspace_pos,second_pos?`。单动作参数沿用 SDK `ctx.world`。`status` 的 `limit` 最多为 2，按 `step_count/region_count` 分页；大参数只返回摘要，应保留原请求。备份输出会明确标注 entities、tick、可携带 BDX 的 NBT/空气等保真损失。

大观察数据通过 `read(kind,sequence,offset,limit)` 获取 `fragment,next_offset,done,length,sha256`；拼接文本、按 UTF-8 核对 SHA256 后解析 JSON。采样 sequence 改变会拒绝，不混合不同快照。区块单元 NBT 使用 `nbt_offset` 分页，`nbt.data_base64` 是该页字节。以上分页都由调用方显式推进，客户端不隐式循环读取或重试。


世界工具的 snapshot 模式要求 Host API 1.9 的维度绑定读取，备份页 `snapshot(task_id,index,offset)` 返回 16KiB `data_base64`、总 `length` 与 SHA256；按原字节拼接校验再解析 JSON，保留未知/不支持字段与损失说明。传统结构模式须显式 `mode: "structure"`。容器恢复需提供真实 `anvil_pos/workspace_pos`。

Fatalder 云导入由世界工具调用独立 `fatalder.cloud-import` 前置。消费者只授予所需 `plugin.neomega.worldtools.fatalder_*` 服务权限；`fatalder_prepare` 返回远端报价与任务，`fatalder_confirm` 提交返回的 `confirm_token` 和调用方持久 `confirmation_key`。`dimension` 使用 `overworld/nether/the_end`。未知结果通过原 `task_id` 查询/恢复，不能生成新请求隐式再次付费。命令方块路由 `status` 分页默认 64，按 `next_offset/done` 继续。


## 1.1 合同修订

本库同时适配玩家字段 schema/导入与世界任务 CAS。世界工具早期本地候选的 `execute/cancel/resume(task_id)` 调用不再适用；必须读当前任务返回的 `revision`，以 `WorldMutationRequest` 提交 `expected_revision`。后台推进也会更新版本，冲突后重新读取状态并决定业务意图，不自动重发。任务持久格式是 `schema_version: 2`；它与服务 major、库版本是不同概念。

玩家命名空间由可信消费者安装身份确定。`get_schema({})` 返回自己的 `namespace,revision,schema_version,schema`；新 namespace 无注册时 version/schema 为 null。注册后 patch 必须带匹配的 schema_version。`register_schema` 使用 schema 注册自己的 revision，不是玩家字段 revision。`import_fields` 输入某个 export 页中的单个 item，并使用目的玩家当前 `fields_revision` 作为 expected_revision；namespace 必须是自己的安装身份。它只替换自己的字段，核对既有玩家稳定身份，不创建玩家、不导入他人的 namespace。不同字段格式升级须先设计迁移，不盲目提高 schema_version。

```python
schema = await players.get_schema({})
await players.register_schema({
    'request_id': schema_request_id, 'expected_revision': schema['revision'],
    'schema_version': 1,
    'schema': {'type': 'object', 'properties': {'language': {'type': 'string'}},
               'additionalProperties': False},
})
profile = await players.get({'player_id': player_id})
await players.patch({'player_id': player_id, 'fields': {'language': 'zh-CN'},
    'expected_revision': profile['fields_revision'], 'schema_version': profile['schema_version'],
    'request_id': business_request_id})
```

CbBridge 的专用返回类型保证作者能接通完整链路：

```python
from neomega_clients import CbBridgeClient
bridge = CbBridgeClient(ctx.services)
route = await bridge.register({'prefix': '[shop]', 'priority': 0, 'consume': True})
subscription = await bridge.subscribe({'route_id': route['id']})
page = await bridge.poll({'route_id': subscription['id'], 'cursor': subscription['cursor']})
# 保存 page['cursor']；gap/dropped 表明丢失，不能假定队列完整。
await bridge.unregister({'route_id': route['id']})
```

`register → BridgeRoute`、`subscribe → BridgeSubscription`、`poll → BridgePoll`、`status → BridgeStatus`、`unregister → BridgeRemoved` 与真实字段一一对应。事件的 `authenticated_player` 固定为 null，命令方块文本不是玩家身份凭据。路由 lease 到期需明确重新注册，不由客户端偷偷重建。

观察的 `last_success_at/last_success_value` 与 `last_attempt_at/last_error` 分开；失败不会使旧数据变新。区块 `action_state` 与 `observation` 分开，缓存可读不能结算动作 unknown；`baseline_revision` 和 `metadata` 用于辨别新观察。音乐客户端返回 `MusicResult`，包括 position/progress、发出/丢弃数和原操作回执。


控制远端导入时，`fatalder_pause/fatalder_resume/fatalder_cancel` 使用 `WorldControlRequest`，必须提交提前保存的 control_id。通过 `fatalder_status` 读回 `remote.control/remote.controls` 原控制记录。`action_state: accepted/unknown/completed/rejected` 与 `observed_state/desired_state_observed` 分开，当前远端状态符合目标不证明丢回包的某次控制已经完成。`rejected` 是有派发前拒绝证据的终态，返回 `evidence: pre_dispatch_rejection` 和 `error_code`；它不表示执行成功。通用失败、超时或断线仍保留 unknown，不能当作 rejected。同一控制 ID 查询原记录，不生成新 ID 重放未知操作。

逐组返回合同如下（对应所有 79 个方法，字段缺省由具体操作决定）：

| 方法组 | 返回模型及关键数据 |
|---|---|
| interaction 全部 | MenuResult：注册 ttl_seconds；会话 session_id/revision/answers；删除 removed；列表 sessions |
| players resolve/get/history/patch/query/export/import_fields | PlayerResult：稳定 PlayerRecord、fields_revision/schema_version、导出 items/namespace/consistency/next_cursor |
| players get_schema/register_schema | PlayerSchemaResult / PlayerSchemaSaved：namespace/revision/schema_version/schema，保存另含 status |
| observations 除 read / read | ObservationResult / ObservationPage：订阅 cursor/epoch/gap，成功与失败时间分离，分片 offset/next_offset/sha256 |
| cbbridge register/subscribe/poll/status/unregister | BridgeRoute / BridgeSubscription / BridgePoll / BridgeStatus / BridgeRemoved |
| chunks 全部 | ChunkResult：request_id、动作与观察状态、独立 subscription_id、原始字节/单元读取、updates |
| worldtools 除 snapshot / snapshot | WorldResult / SnapshotPage：任务 revision/schema_version、步骤/区域分页、远端控制及快照 task_id/index/offset/sha256 |
| music 全部 | MusicResult：播放状态、duration/position/progress、emitted/dropped、原操作意图/回执 |
| scores 全部 | ScoreResult：目标名称/数量、player_id/value、rank items、sync_state/operation_ids/operations |
| roles 全部 | RoleResult：命名空间 revision、player_id、allowed/roles/scope、分页 items/total |
| moderation 全部 | ModerationResult：ban/bans、active、revision、next_offset |
| economy balance/post/transfer/hold/release；ledger；receipt_request | Balance；LedgerResult；BusinessReceipt（unknown 的 result 为 null） |
| scheduler 全部 | SchedulerResult：schedule/schedules/schedule_id、triggers、revision/next_offset |
| messaging 全部 | MessagingResult：delivery_id、路由/投递列表、事件 cursor/gap、remote_id/updated_at |


群桥出站请求必须使用 `MessagingClient.new_request_id()` 明确创建的 `v2:<issued_at_ms>:<random>` ID，并在 `send` 之前随业务意图持久化。客户端不会自动给 send 注入 ID，也不会遇到异常就生成新 ID。保留窗口内重复使用原 ID 读原结果；过期拒绝为 `request_expired`，不自动重新发送。首次使用旧格式的请求拒绝为 `request_id_upgrade_required`；旧投递结果继续通过 `status(delivery_id)` 查询。窗口由 provider 配置决定，入站外部平台事件去重不使用这个 ID 格式。

```python
from neomega_clients import MessagingClient
messages = MessagingClient(ctx.services)
request = {'route_id': 'staff', 'text': '服务器维护通知',
           'request_id': messages.new_request_id()}
await ctx.storage.set('pending-notice', request)
result = await messages.send(request)
# 保留 request/result.delivery_id；不因超时或 request_expired 自动新建通知。
```
