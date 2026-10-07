# 消息桥

把玩家聊天发送到 Polaris 现有群桥、OneBot v11 群或 Telegram 群，把群消息显示到游戏；业务插件可向自己获准的路由投递消息并查询结果。支持文本、按路由限速、重复消息去重、断线重新轮询和持久投递记录。

## 配置和连接

所有路由在 Host 配置中定义，服务调用不能更改外部地址或收件群。默认无路由，因此安装后不会建立外部连接。`config.example.json` 默认接入已有 Polaris `ws://localhost:18766/` 群桥，替换群号和消费者 installation ID 后使用，不要求另建 HTTP 网关。最多 20 条路由；每路由最多 64 条待发送记录，保护 Host 256 KiB 单次提交预算。

- **Polaris WebSocket**：`adapter=polaris_ws`，沿用 `ws://localhost:18766/`；协议为 OneBot v11 正向 WS，发送 `{action:"send_group_msg",params:{group_id,message,auto_escape},echo}`，按 echo 匹配 `status/retcode/data.message_id` 回执，接收 `post_type=message,message_type=group,group_id,message_id,user_id,self_id,sender,message`。支持文本段数组和作为纯文本显示的 CQ 字符串。协议字段对照 FlyLink `internal/onebot/client.go` 的 `SendGroupMessage/call`；不使用管理面的 Unix HMAC API。连接需要 token 时配置安装 secret 引用；本机无认证网关可留空，远端必须 WSS。断线每 5 秒重连，已进入发送阶段的消息不重发。
- **OneBot v11 HTTP**：开启正向 HTTP API，把 `endpoint` 配为对应根地址。开启反向 HTTP 上报，地址为 `http://127.0.0.1:18767/group`（最后一段是路由 ID），设置上报 secret，消息格式设为 array。`webhook_port=0` 禁用入站监听。配置 `webhook_secret` 引用 HMAC-SHA1 签名密钥，`secret` 引用 HTTP access token。发送使用 `send_group_msg` 文本 segment，不执行 CQ 字符串。远端 HTTP API 要使用 HTTPS；反向回调默认只监听本机，跨主机使用 TLS 反向代理。
- **Telegram**：将路由 `adapter` 改为 `telegram`、`endpoint` 设为 `https://api.telegram.org`，`secret` 引用 Bot token，`group_id` 为目标 chat ID。Bot 需有群消息读取权限；使用 `getUpdates`，不要同时为此 Bot 配置 webhook 或运行其他轮询消费者。每个 bot token 只配置一个路由。发送使用 `sendMessage`。
- 非空凭据使用 `ctx.secrets` 安装私有引用，如 `secret:bridge-token`，真实文件放安装 data_dir 的 `secrets/bridge-token`；目录 0700、文件 0600。不得将 token 直接写入配置或提交到包。轮换后下一次请求读取新值。
- `game_to_group`/`group_to_game` 控制两方向，`prefix` 过滤游戏聊天；每个方向独立受 `messages_per_minute` 限速。群消息仅显示为文本；自身机器人消息不回传，游戏侧只接受带 XUID 且匹配 Host 当前玩家列表的真实聊天。
- `allowed_installations` 是可调用该路由的消费者安装 ID，不是插件 ID；为空时不开放给任何消费者。每条路由可使用不同名单。

首次连接 Telegram 会读取尚未确认的队列消息。接收后游标持久化；临时网络失败在下一轮继续读原游标。OneBot 接收失败返回非 200，是否重投由对应实现决定。保留窗口内的重复事件 ID 不会再次生成游戏动作；过期与无时间来源的具体边界见下文。

## 管理命令

默认禁用。需安装并授权 `neomega.roles`，配置 `roles_namespace` 为明确的共享权限空间，并在角色插件 `shared_namespaces` 给消息桥安装 ID 授予 reader 权限。通过 `identities` 将外部账号 ID 显式绑定到玩家档案的 canonical `player_id`；显示名不作鉴权。

路由配置示例片段：

```json
{
  "roles_namespace": "server-admin",
  "identities": [{"external_id": "123456", "player_id": "xuid:1234567890"}],
  "admin_commands": [{"name": "day", "command": "time set day", "capability": "server.time"}]
}
```

只有精确文本 `/admin day` 才会执行配置的固定命令。检查能力 `server.time`、scope 为路由 ID。角色服务不存在、超时、账号未绑定或能力不满足均拒绝；不能通过消息拼接额外命令、参数或玩家名。`Host grants` 仍须允许对应游戏动作。

## 服务 API（major 1）

所有调用以 `plugin.neomega.messaging.<方法>` 命名，使用 Host 注入的 installation 调用身份。

| 方法 | 输入 | 输出 |
| --- | --- | --- |
| `send` | `route_id,text,request_id` | `delivery_id,request_id,route_id,owner,state,created_at,direction` |
| `routes` | `{}` | `routes`：当前消费者可用路由和连接状态，不含地址/secret |
| `status` | 可选 `delivery_id` | 当前消费者最近最多 100 条投递及 `sealed`；指定 ID 不存在或不归自己返回 not_found |
| `subscribe` | `after=0,limit=100`（最多 200） | `events,cursor,gap`；轮询有界历史，游标按返回值前进 |

消息体上限默认 3000 UTF-8 字节。每个 installation 的 request_id 独立，必须使用 `v2:<issued_at_ms>:<random>`：毫秒 Unix 时间，random 为 8 至 128 个 ASCII 字母、数字、下划线或连字符。窗口内同 ID 同参数返回原记录，异参返回 conflict。推荐使用 neomega_clients 的消息 ID helper，先持久保存 ID 再调用；不确定时复用原 ID，不能自动换 ID 重发。send 返回 queued 只说明已持久接收，不代表渠道收到；连接错误记 unknown，明确的远端拒绝记 rejected，成功回执记 delivered（外部 API 确认，不代表群成员阅读）。

`subscribe` 是有界游标轮询，不创建永久远端订阅。只返回调用者自己的出站回执及获准路由入站事件；发生历史截断时 gap=true。撤销路由 ACL 后不再读取该路由历史。

## 持久化、维护与恢复

状态 schema_version=3（需要 Host API 1.10 的分页键名扫描），由安装独有 Host storage 分键存储 outbox、去重记录、Telegram 游标及有界事件历史。每条投递/去重/事件独立，按路由维护有界队列、按调用者维护最近 100 条索引；查询不会读取全账本。所有写入使用选择键事务，每次提交之前把原 commit ID 写入安装数据目录的屏障文件并 fsync 文件与目录。超时、取消或进程重启后只查询原 Host receipt；receipt 尚不可得时返回 unknown 并停止新写入，不以新 CAS 重放。此版取代未发布的单键草案；若检测到旧草案 messaging 键，明确拒绝启动要求显式迁移，不静默忽略。发送前持久标记 dispatching；进程重启把该状态转 unknown，不自动重发。queued 可继续发送。保留原 delivery_id 和 request_id，unknown 应结合外部渠道历史核对，重复 send 不会新建投递。入站游戏动作与去重记录在同一个 Host transaction 接纳；它表示 accepted，不声称游戏显示已完成，原 commit ID 保留在 seen 记录用于 Host 读回。

`retention_seconds` 默认 86400 秒，控制主动发送请求有效窗口与终态结果保留；`inbound_retention_seconds` 默认 86400 秒，独立控制来源事件窗口。两者可设 1 至 2592000 秒。`clock_skew_seconds` 默认 60 秒，允许最多 300 秒超前。出站 ID 早于持久 `expired_before` 返回 `request_expired`，未来超出容差返回 `request_in_future`，格式错误返回 `invalid_request_id`。客户端给出的时间是请求有效期字段，并不是不可伪造的业务身份；换 ID 是一个新请求，调用方必须保留原 ID。

服务端持久保存 `last_seen_clock`，按系统时间与已见最大时间的较大值判断有效期；回退在 `status.clock_rollback` 显示。回收前先单独原子保存单调 `expired_before` / `inbound_expired_before`，再分批删除已过期终态并释放对应计数。缩短窗口会让旧请求立即失效；之后扩展窗口、重启或时间回退都不能让已回收的 v2 ID 复活。`queued`、`dispatching`、`unknown` 不删除；已过期未决出站用原 `delivery_id` 查询，不能换新 ID 重放。每次写入及后台周期各扫描至多 32 条出站和 32 条入站键；大账本可能需要多个周期释放容量。没有永久 tombstone 表。达到 `max_records`（每类默认 10000）返回 `capacity_exhausted`，不会丢弃未决记录腾位置。

从 state_version=2 升级时，Host update/update-set 必须显式传入 `migration_from=2`。这是元数据升级，旧分键原地保留，不重放旧不确定动作。旧分键 schema_version=2 在首次启动原子迁移元数据并记录固定 `legacy_cutoff`；全部原记录、游标和计数保留，后台分页扫描覆盖完整旧账本，不依赖每位调用者最近 100 条索引。已知旧格式 ID 仅返回已有记录，不再接受新的无时间 ID，返回 `request_id_upgrade_required`；旧终态至少保留迁移后的兼容窗口，回收后该格式仍拒绝，未知结果一直保留。事件历史仍独立按 `subscription_history` 截断。不能以删除安装数据解决 unknown。

Telegram 使用来源 `message.date`，OneBot 使用来源 `event.time`（均为 Unix 秒）和来源消息 ID；键为 `(adapter, route, message_id)`。这只是经过对应渠道认证的来源时间，不宣称它是不可伪造的消息 ID 创建时间。过期单条事件返回 `event_expired`，缺失或异常时间返回 `event_time_required`，超前返回 `event_in_future`，不转发。Telegram 跳过该条并推进 update 游标，继续处理后续消息；OneBot HTTP 返回 422 与具体 error，WS 在路由 health 显示错误，适配器继续运行。字段依据 [Telegram Message](https://core.telegram.org/bots/api#message) 和 [OneBot v11 群消息事件](https://11.onebot.dev/event/message/#群消息)。

Polaris WS 若来源省略 time，允许有效无时间事件，使用本机首次接收时间做有限窗口去重；游戏聊天/命令方块流保留原 delivery token 或 connection_id/source_sequence 派生 ID，同样只承诺来源保留窗口，不把每次接收时间包装成可信 v2 ID。无时间事件在记录回收后再次到达不享有永久去重保证。所有入站记录回收前读取原 commit receipt 和 operation_ids；仅无动作或所有动作均有明确 succeeded/failed/cancelled 结果且无 partial_effects 时可删除，pending、unknown、缺失回执继续保留。事务被接纳不等于游戏已显示。

seal 在锁内阻止新写入及后台投递，存在网络在途、unknown 或尚未核定提交返回 busy；release 校验维护 token。Worker 管理定时任务与入站连接回调；停止时关闭监听服务器并等待限时 HTTP 请求退出。

## 作者使用

`example.py` 提供可直接从消费者生命周期调用的函数。消费者 manifest 需声明运行依赖 `neomega.messaging` major=1 min_version=1.1.0，并申请所调用服务权限；服主另授 Host grants 和路由 ACL。无需打包 SDK 或在运行时安装 pip 包。

本插件标准库实现：内置有界 RFC6455 客户端，支持掩码、分片、ping/pong 和 echo 回执；不安装运行时 pip 包。OneBot HTTP、Polaris WS、Telegram 可并存。

可选安装 `neomega.cbbridge` 并授权 manifest 列出的四个服务，设置路由 `cb_prefix` 后订阅该前缀的命令方块消息，作为 `[Command block]` 普通文本发群。空前缀禁用此来源；采用非消费注册，不抢占其他业务。轮询续租、重复事件按 connection_id/source_sequence/route 去重，来源始终为未认证系统文本，不获得管理权限。订阅缺失时重新注册；游标缺口在路由 health 显示 `cbbridge_gap`，不伪造丢失消息。此来源是有界实时流，不承诺离线历史补发。

工程检查覆盖导入、配置 schema、发行文件和离线直接调用；未连接真实群和游戏，不将其表述为实服验证。MIT 许可证。
