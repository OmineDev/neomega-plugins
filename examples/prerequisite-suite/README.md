# 前置生态消费者示例

安装后读取玩家档案列表和本消费者的调度列表，在日志显示连接状态；默认每 60 秒刷新。无需进入游戏，无经济或世界写入。

1. 安装并配置 `neomega.players`、`neomega.scheduler` 1.x；显式启动前置服务。
2. 使用与 Host 匹配的工具包构建此示例，审阅其两个只读服务权限后安装并启动。
3. 查看插件日志：显示档案数和当前消费者的任务数。服务拒绝/离线会返回真实错误，不显示假的连接成功。

```sh
python3 tools/lock-libraries.py examples/prerequisite-suite
python3 tools/package-plugin.py examples/prerequisite-suite --output /tmp/prerequisite-suite.zip
```

源码展示 `PlayersClient(ctx.services)`、`SchedulerClient(ctx.services)` 和 `ctx.every` 的接线。完整 13 个客户端、写入幂等/CAS、回执与未知恢复说明见 `libraries/neomega_clients/README.md`；各插件 README 给出业务参数。此示例不自动安装或授权其他插件。
