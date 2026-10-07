# 命令方块通信

通过系统 Text 消息触发插件路由，提供前缀匹配、优先级、消费/继续传播、短期租约、限速和有界消息队列。无需其他前置，需要 Host API 1.8、Text 包观察授权。

## 使用

安装后配置并授权，启动插件。消费者注册前缀，例如 `[cb:shop]`，然后定期 poll；轮询及 subscribe 为该路由续租。命令方块中执行 `tellraw @a {"rawtext":[{"text":"[cb:shop]open"}]}`，消费者读取 payload=`open`。`setup.mcfunction` 提供布置示例，可按房间需要由管理员执行。建议选择只向机器人发送的目标，避免玩家聊天刷屏。

| 服务 | 参数 | 返回 |
| --- | --- | --- |
| plugin.neomega.cbbridge.register | prefix、priority=0、consume=true | id、expires_at、路由属性 |
| plugin.neomega.cbbridge.subscribe | route_id | 路由属性、起始 cursor |
| plugin.neomega.cbbridge.poll | route_id、cursor | events、cursor、gap、dropped、expires_at |
| plugin.neomega.cbbridge.unregister | route_id | removed |
| plugin.neomega.cbbridge.status | 空对象 | 自己的路由、协议流状态 |

所有方法 major 1；调用者 manifest 需声明对应 service 权限。归属来自 Host call context，别的安装不能读取或撤销路由。优先级从高到低，consume 会停止之后的路由；同前缀同优先级冲突会拒绝，不暗中抢占。路由需在 lease_seconds 到期前轮询续租，重启后重新注册。

## 安全与交付范围

接受系统/原始 TextType 0，不把普通聊天当命令方块输入。但是系统文本也可能由其他命令产生，**前缀和 source_name 均不是认证信息**；返回 authenticated_player=null。不得用文本声称的玩家名/XUID直接授予管理员、扣款或转账。敏感业务须通过独立可信身份入口和权限服务确认。

消息按连接与源序号去重；每路由按秒限速、按容量截断。gap/dropped 明确提示消息可能丢失，poll 不代表消费业务完成；业务副作用仍应保存自己的业务请求回执。观察连接结束时报告流状态，不自动重放历史或冒充成功。

seal 暂停新注册和入站分发，release 恢复；已有队列可读。停止释放流及租约，无外部进程。工程检查包括打包/schema/直接离线调用，未设置实服或人工验收门。

`status(offset=0, limit=64)` 按页返回本调用者路由，并提供 `total/next_offset/done`，避免大路由表超过服务消息上限。

命令方块 `tellraw` 的 JSON `rawtext` 纯 `text` 段会按顺序合并；普通玩家聊天、selector/translate 等需要游戏解释的段不作为前缀路由输入。所有输入仍不代表经过认证的玩家。
