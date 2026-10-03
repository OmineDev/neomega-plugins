# 入门服务插件

玩家加入时收到欢迎消息；发送 `!help` 查看文本帮助；每隔 300 秒向全服公告一次，首次公告在第一个间隔后发送。无需计分板或其他插件。

使用随 Neomega 1.0.0 开发工具包提供的 Python SDK，需要 Python 3.10+、Host API 1.1 或以上。Host 必须已连接游戏会话并提供聊天、玩家列表和命令执行来源。无其他插件依赖，不捆绑 SDK 或第三方 Python 依赖。

源码及许可证随 ZIP 提供，沿用本目录的 GNU AGPLv3 许可。作者入口和操作说明见 [Neomega Wiki](https://wiki.wuxie233.com/)。

## 配置与修改

`config.example.json` 给出全部默认值：`welcome_text` 中的 `{name}` 替换为玩家名；`help_text` 是私聊帮助；`announcement_text` 是全服公告；`announcement_interval` 单位为秒，最少 10 秒。三条文案必须非空且各不超过 200 UTF-8 字节（中文通常每字 3 字节）。文案长度在插件配置验证时检查，静态 schema 检查类型和公告间隔；改完配置后重载生效。

`main.py` 中 `reply` 生成私聊通知，`announce` 生成全服公告，`on_event` 分流加入和聊天事件。普通聊天和未知命令仅确认事件。欢迎基于 `player.list_added`，不是“首次注册”；Host 重新建立玩家列表时也可能欢迎现有玩家。已离线玩家的延迟加入事件直接确认。

## 打包与安装

从官方目录选择 `example.starter-service` 的固定版本 `1.0.0`，或下载同版本 ZIP 并核对目录 SHA-256。开发工具包目录为 `$TOOLKIT`，下载文件路径为 `$PACKAGE`；`TARGET_URL` 是实例管理地址，令牌只从文件读取：

```sh
python3 "$TOOLKIT/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" install starter-service "$PACKAGE"
python3 "$TOOLKIT/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" configure starter-service
python3 "$TOOLKIT/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" permissions-plan starter-service
python3 "$TOOLKIT/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" permissions starter-service
```

安装、配置、权限确认和启动是独立动作。安装后保持停止；查看权限后再明确同意，启动提示可回车保持停止。更新使用管理工具 `update-plan` / `update`，核对固定版本和摘要，不删除原安装、订单或回执后重装。

源码打包可在社区仓执行 `python3 tools/package-plugin.py plugins/starter-service --output example.starter-service-1.0.0.zip`。`release.json` 与 `package.files.json` 逐项列出相同文件，不包含测试、真实配置或运行数据。

权限用途：`framework.command.execute` 发送通知与公告；`players: ["list"]` 核对当前收件人；持久订阅 `chat.received` / `player.list_added` 处理帮助与欢迎。安装后保持停止；`permissions` 按提示确认权限，并在启动检查通过后选择启动，回车可保持停止；无需原始包、背包或计分板权限。

事件 ACK 与通知意图通过一个现有事务提交，随后查询动作结果。失败或 `unknown` 报告原 operation ID 并让 Worker 退出，不自动重发。事件与公告共用一把事务锁，避免并发提交状态版本冲突。公告使用 `ctx.every`，Worker 停止时取消并等待任务，插件不自建线程、调度器或恢复后台。

## 状态与故障处理

通过管理工具查看日志与原 operation ID；`accepted` 不等于已经显示消息，`unknown` 需管理员核对原记录。普通停止与重新启动保留安装状态。不要为重试不确定结果生成新业务键。
