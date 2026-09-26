# 入门服务插件

玩家加入时收到欢迎消息；发送 `!help` 查看文本帮助；每隔 300 秒向全服公告一次，首次公告在第一个间隔后发送。无需计分板或其他插件。

代码改编自 [native-gameplay](https://github.com/OmineDev/neomega-agent/tree/main/examples/extensions/native-gameplay)，使用当前 `neomega_runtime` 的 `Plugin`、`PluginContext.chat`、`notifications.prepare` 和 `every`。需要包含聊天路由、通知和制品检查工具的新版 neomega-agent 发行包；Host/SDK 继续由该仓库维护，本目录只包含业务示例。

## 配置与修改

`config.example.json` 给出全部默认值：`welcome_text` 中的 `{name}` 替换为玩家名；`help_text` 是私聊帮助；`announcement_text` 是全服公告；`announcement_interval` 单位为秒，最少 10 秒。三条文案必须非空且各不超过 200 UTF-8 字节（中文通常每字 3 字节）。文案长度在插件配置验证时检查，静态 schema 检查类型和公告间隔；改完配置后重载生效。

`main.py` 中 `reply` 生成私聊通知，`announce` 生成全服公告，`on_event` 分流加入和聊天事件。普通聊天和未知命令仅确认事件。欢迎基于 `player.list_added`，不是“首次注册”；Host 重新建立玩家列表时也可能欢迎现有玩家。已离线玩家的延迟加入事件直接确认。

## 打包与安装

从 neomega-agent 源码构建检查器；`AGENT` 指向它的 checkout，`SOURCE` 指向本目录，`OUTPUT` 位于源码目录之外：

```sh
(cd "$AGENT" && GOWORK=off go build -o "$OUTPUT/neomega-plugin-check" ./cmd/neomega-plugin-check)
python3 "$AGENT/scripts/runtime-plugins.py" --checker "$OUTPUT/neomega-plugin-check" pack "$SOURCE" "$OUTPUT/example.starter-service-1.0.0.zip"
python3 "$AGENT/scripts/runtime-plugins.py" --checker "$OUTPUT/neomega-plugin-check" check "$OUTPUT/example.starter-service-1.0.0.zip"
```

发行包已附带 `runtime-plugins.py` 和检查器时可直接使用。`package.files.json` 明确列出包内文件：入口、manifest、schema、配置示例、说明和许可证；不包含测试、真实配置或运行数据。

管理员选定实例后使用现有管理工具（`TARGET_URL` 为机器人实例地址，非发现目录；令牌只从文件读取）：

```sh
python3 "$AGENT/scripts/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" install starter-service "$OUTPUT/example.starter-service-1.0.0.zip"
python3 "$AGENT/scripts/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" configure starter-service
python3 "$AGENT/scripts/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" permissions starter-service
```

权限用途：`framework.command.execute` 发送通知与公告；`players: ["list"]` 核对当前收件人；持久订阅 `chat.received` / `player.list_added` 处理帮助与欢迎。安装后保持停止；`permissions` 按提示确认权限，并在启动检查通过后选择启动，回车可保持停止；无需原始包、背包或计分板权限。

事件 ACK 与通知意图通过一个现有事务提交，随后查询动作结果。失败或 `unknown` 报告原 operation ID 并让 Worker 退出，不自动重发。事件与公告共用一把事务锁，避免并发提交状态版本冲突。公告使用 `ctx.every`，Worker 停止时取消并等待任务，插件不自建线程、调度器或恢复后台。

## 离线验证

```sh
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$AGENT/sdk/python:$SOURCE" python3 -m unittest discover -s "$SOURCE/tests"
```

局部模拟使用真实 SDK 路由与通知生成，检查欢迎、帮助、公告和 ACK；制品另用上述现有检查器验证。未连接游戏服务器，不声称实服或玩家画面验收；这些不属于本示例本轮交付范围。
