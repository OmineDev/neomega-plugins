# 交互菜单

为业务插件提供统一命令、别名、参数、帮助菜单、分页选择、确认、多步输入和雪球朝向预览。一个玩家同时只有一个会话；业务插件通过服务获取结果后自己执行操作，菜单服务不代为授权、扣款、传送或发货。

## 使用

安装 `neomega.display >= 1.1.0`，授予命令执行、玩家列表、聊天/退出订阅及展示调用权限。配置见 `config.example.json`。从消费者运行 `examples/consumer.py` 的 `install_menu(ctx)`；消费者 manifest 声明对 `neomega.interaction` major 1、最低 1.0.0 的依赖以及 register/status 服务权限。玩家输入 `!menu` 查看已注册入口，示例入口为 `!travel` / `!旅行`。选择题输入当前页编号；`!next` / `!prev` 翻页，`!cancel` 随时取消，确认题输入 yes/no。注册默认保留 300 秒，消费者每 240 秒重新注册，停止后租约自动过期。

## 服务合同（major 1）

服务统一前缀 `plugin.neomega.interaction.`，精确 schema 见 manifest。

| 方法 | 参数 | 结果 |
| --- | --- | --- |
| register | request_id、menu_id、menu；可选 commands、args、ttl_seconds | menu_id、ttl_seconds |
| unregister | menu_id | removed |
| open | request_id、menu_id、player（name/uuid/xuid 查询对象） | 会话 |
| respond | request_id、session_id、expected_revision、value | 会话；source=consumer，不证明玩家输入 |
| cancel | session_id | 会话 |
| status | 可选 session_id | 单会话，或 sessions 列表 |

会话包含 session_id、menu_id、真实玩家快照、state、step、page、answers、revision、source、每题 answer_sources、命令 args、notification。state 为 active/completed/cancelled/expired/disconnected。消费者仅能访问自己 installation + generation 的注册和会话，不接受参数伪造 owner。消费者应在结果保留期内轮询，用 session_id 作为自己业务账本的幂等键；completed 仅表示输入完成。会话与注册不跨重启恢复。多题输入来源不一致时 source=mixed，逐题来源以 answer_sources 为准。

menu = `{title, steps, permission?}`；steps 为 1–32 项，每项 `{id,kind,prompt}`。kind 为 select/confirm/text/integer；select 增加 options: `[{label,value}]`，最多 256 项；integer 可指定 min/max。args 是 `[{name,type}]`，type 支持 text/integer/number，严格要求与命令参数数量一致。命令支持 shell 风格引号，但不运行 shell。

## 角色集成

受限菜单声明 `permission:{capability,scope:'global',namespace?}`。namespace 默认注册消费者安装 ID；菜单每次打开及输入均请求 roles.check，依赖失效或拒绝时禁止推进。角色服务调用者是 interaction 本身，因此服主必须在 roles 配置 shared_namespaces 中显式允许 interaction 安装读取对应 namespace，同时授权其调用 players.resolve 和 roles.check。这既不继承消费者 Host 权限，也不等价授予游戏管理员；最终业务动作仍由消费者复核。

## 雪球与朝向

启用 `snowball_adapter:true`，page_size 设为 4，并授予 Text 包 9 的观察权限（Host API 1.5）。将 `adapters/snowball.mcfunction` 放入行为包函数并按 tick 执行，或把各行部署到重复/连锁命令方块。它为首次看到的雪球打标签，依据附近玩家朝向映射四个选项，不删除雪球。南/西/北/东分别为 1/2/3/4。玩家打开选择页后投雪球，收到选择预览和一次性确认码，再输入 `!confirm <码>` 完成选择。

实现直接订阅 Text 协议包，因 tellraw 不属于 chat.received。命令块输出/最近玩家选择不证明投掷者身份，只能提出预览；15 秒内真实玩家聊天确认才写入答案。多人近距离环境可能选到不同预览对象，不能用最近玩家推断真实投掷者。聊天分页与雪球使用同一会话；翻页后旧确认码失效。原生表单不在本插件范围。

## 生命周期、错误和限额

服务受 Host 可信调用上下文隔离。注册名冲突拒绝；默认同时保留 128 会话、256 注册；输入 120 秒超时，完成结果保留 600 秒，注册租约 10–3600 秒。请求去重在结果保留窗口内有效，同 ID 异参数返回 conflict；新调用超容量返回 busy。升级 seal 遇到活动会话返回 busy，release 恢复准入。停止由 Worker 取消并等待受管计时器/包观察任务，再释放所有内存会话。

每条展示通知保留接纳回执，不把命令接纳当作游戏可见；失败或调用超时标记 unknown，不自动重发。共享 Actionbar 使用 5 秒 TTL 和优先级 30，低优先级可被其他插件覆盖。离线检查不等于真人客户端运行记录，外部布置条件不构成本套件交付门。
