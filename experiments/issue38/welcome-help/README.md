# 欢迎与帮助插件（Issue 38 开发体验）

玩家加入时收到欢迎语；聊天消息严格等于 `!帮助` 时收到三行固定服务器帮助。其他聊天不响应，已离线玩家的事件仅确认。

原生持久订阅 `player.list_added`、`chat.received`；通过当前玩家列表核对收件人。中文命令直接精确匹配，因为当前 `ChatRouter.command` 只允许 ASCII 名称。权限仅为玩家列表读取和 `framework.command.execute`（通知内部使用 tellraw）。无定时器、公告、外部依赖或可变业务配置。

以稳定 `event_id` 的 SHA-256 作为持久去重键；事件 ACK、去重标记和全部通知动作在同一事务提交，锁串行化并发处理。提交后查询动作结果并记录 operation_id；失败、unknown 或等待超时均不自动补发。标记代表动作已受理而非玩家看到了消息。标记不自动清理，适用于本次小型实验；长期运行需另外设计保留策略及有界快照读取。提交不确定时沿 SDK 异常保留原意图，交由操作者核对，插件不重新生成动作。

## 离线复现

Python 3.10+；`AGENT` 指向 neomega-agent checkout。在插件仓根目录执行：

```sh
export AGENT=/path/to/neomega-agent
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$AGENT/sdk/python" python3 experiments/issue38/welcome-help/check_offline.py
mkdir -p dist/tmp
TMPDIR="$PWD/dist/tmp" GOCACHE="$PWD/dist/go-cache" GOWORK=off GOPROXY=off GOTOOLCHAIN=go1.26.0 go -C "$AGENT" build -o "$PWD/dist/neomega-plugin-check" ./cmd/neomega-plugin-check
python3 "$AGENT/scripts/runtime-plugins.py" --checker "$PWD/dist/neomega-plugin-check" pack experiments/issue38/welcome-help dist/welcome-help.zip
python3 "$AGENT/scripts/runtime-plugins.py" --checker "$PWD/dist/neomega-plugin-check" check dist/welcome-help.zip
```

核对脚本使用真实 SDK Event、Storage、Transaction、Operations 与 Notifications，并以内存 IPC 同行替代 Host。验证欢迎语、三行帮助、无关聊天、离线玩家、不同投递令牌、插件对象重建、并发重复事件和 unknown 不重发。它验证插件决策与 SDK 意图，未验证真实 Host 的持久性、游戏发送或玩家画面。打包检查只验证制品结构与 manifest。

## 本次记录

- 开始记录：2026-09-26 19:31:42 UTC；结束与耗时见下方验收记录。
- 框架源码：`415623638df86722059fc9b0cc71c642b53c0b66`；插件仓基线：`21d549010f472c76880e6ec20d9347f717aeddc1`，开始时工作树干净。模型：`gpt-6-astra`，推理强度 `medium`（集成时由父任务会话元数据及未覆盖的继承设置确认）。
- 从 basic 模板生成后实现；直接核对脚本通过。没有新测试框架、全套测试、Host/机器人连接、服务重启、人工协助、提交或推送。
- 修复两处本次工具使用错误：Go 默认临时目录只读，显式设置工作树 `dist/tmp`；核对脚本误取 `arguments.command`，按 SDK 修正为 `arguments.cmd`。业务源码未出现运行失败。
- 建议：快速上手补充 ChatRouter 的 ASCII 名称约束和中文匹配示例；持久事件示例明确区分稳定 event_id 与 delivery_token；为长期去重提供有界读取/保留策略示例。

验收记录：2026-09-26 19:33:38 UTC 完成，约 2 分钟（从首次可核对时间记录计；含入口阅读的总尝试约 3 分钟）。离线核对 PASS；pack/check 均 `ok: true`、`plugin_id: experiment.welcome-help`、5 个发布文件；`git diff --check` 通过。独立核对脚本不随插件发布。

集成检查补入仓库 AGPL-3.0 许可证并加入发布白名单；最终包为 6 个文件。以上 5 文件验收描述保留独立尝试当时结果。
