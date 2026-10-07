# 封禁管理

统一管理永久、临时和离线封禁。封禁后玩家上线时执行踢出；临时封禁按绝对时间到期，不会因调度器停机而延长。

## 安装与设置

插件 ID `neomega.moderation`，版本 1.0.0；依赖 `neomega.players`、`neomega.scheduler` 1.x。要求游戏会话。配置 `writer_installations` 为被授权处罚的插件安装 ID，空列表禁止封禁/解封；`enforce` 控制踢出执行，默认开启；`poll_seconds` 默认5秒；`max_players` 默认256（含保留历史主体）。完整示例见 `config.example.json`。

申请 players:list 以匹配在线 UUID/XUID，申请 framework.command.execute 以发送踢出，申请玩家身份与调度服务以核对身份和处理到期。调用插件另需逐项申请公开服务权限。

## 服务 API（major 1）

| 方法 | 请求 | 返回 |
|---|---|---|
| ban | request_id、player_id、expected_revision；可选 expires_at、reason | status=saved、ban、active |
| unban | request_id、player_id、expected_revision；可选 reason | status=saved、ban、active=false |
| check | player_id | active、ban、revision；未知主体 revision=0 |
| list | 可选 offset | bans（附 active）、next_offset；每页16项 |

服务名为 `plugin.neomega.moderation.<方法>`。稳定 player_id 使用 players 返回的 `uuid:<uuid>` 或 `xuid:<xuid>`；名称不能作为封禁键。永久封禁不传 expires_at；临时封禁传带偏移 ISO 时间。修改前 check 获取 revision；并发修改返回 revision_conflict，重新读取后由业务决定是否提交新 request_id。reason 最多512 bytes，不得含控制字符。

## 持久性与执行

封禁、请求指纹、操作者和原始结果原子保存。相同调用安装的 request_id 同参返回旧结果、异参拒绝。读取和列表都即时比较到期时间，后台扫描也将记录转为 expired。到期任务通过稳定 schedule_id 注册、主动 claim/ack；旧 ban revision 的到期任务不会影响后来更新的封禁。

在线名单与玩家档案双重核对后，在同一个 storage 事务保存 kick 意图及执行记录。记录提供 commit_id、operation_id、执行状态；lost reply/重启先查询原交易和操作回执，不自动重发未知踢出。新的上线观察在之前动作已确定终态时创建新执行。封禁是上线后处理，不是协议登录前拦截。

`enforce=false` 时保留封禁和到期管理但不踢人。后台失败记录错误类型，下轮从持久状态继续；未知副作用保持未知。maintenance seal 等待当前受锁操作结束后封闭写入与后台工作；check/list 保持可读。manifest state_version=1，升级不得清空处罚或请求历史。

示例见 `examples.py`。许可证 AGPL-3.0，见 LICENSE。自动离线检查与真实服务器效果分开记录，不以人工或真服验收阻塞交付。

## 提交结果不明时

写入前先在受管 data_dir 持久化并 fsync commit_id 标记，再提交 SDK 事务。取消、进程中断和回包不明后，重启仅查询原提交 receipt；查不到保持 commit_unresolved，维护封闭返回 busy，不自动重放。查到原 receipt 才清除标记并允许后续写入。此状态属于运行时数据保护，不要求人工或真服验收。

## 调度故障隔离

每个玩家的扫描与每个到期回执独立处理，单个身份服务或调度错误不会跳过其他玩家，也不会阻止到期 claim。调度不可用时仍以绝对时间判断封禁有效性并执行踢出。封禁替换、解封与到期会随封禁记录持久保存旧任务清理项，重启继续 query/cancel；取消使用固定请求 ID，已完成或已取消才清理记录。注册回包不明时只查询原任务，不重新注册；查不到不能证明未写入，因此保留未知清理项。明确拒绝的注册可稍后重试。任何本地事务结果不明仍保留原提交屏障，禁止后续写入。
