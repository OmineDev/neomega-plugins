# 个人传送点

每个账号独立保存地点，支持列出、删除、返回、显式覆盖与原子重命名。安装后保持停止，服主配置并授权后启动。默认最多3个地点，成功传送后冷却30秒，仅主世界；配额调低不删除已有地点。

## 玩家使用

- `!home save 名称` 保存；同名覆盖必须使用 `!home replace 名称`。
- `!home list` 列出；`!home delete 名称` 删除。
- `!home go 名称` 返回；`!home rename 旧名 新名` 重命名。
- `!home status` 查询本人未结清请求。默认也支持 `!家`，有空格的名称用英文引号包住。

地点名1至32字符。保存按**脚部所在方块的水平中心**，不是精确亚格位置。不转换 querytarget 的眼高。请站到完整方块上的空地后保存；脚部格必须为空气，半砖、地毯、流体及特殊姿态可能被拒绝。保存时移动、身份不符、缺少坐标或回执矛盾都保留旧地点。返回地点仍可能有岩浆、高空、领地限制；`check_for_blocks` 仅请求服务器检查空间是否被方块占据，不承诺地形安全。

## 按需配置

`config.example.json` 包含全部默认配置。`enabled=false` 停止接收新请求，保留数据，继续只读核对已提交操作。`aliases` 设置1至8个不同聊天前缀。`max_homes` 支持1至100，`cooldown_seconds` 支持0至86400。`warmup_seconds` 默认0，可设0至30秒；启用后每秒检查本人位置，较倒计时起点移动超过0.5格、换维度、离线或观察失效则取消。

`allowed_dimensions` 只支持 `overworld`、`nether`、`the_end`，默认仅主世界。`cross_dimension` 默认false；启用跨维度前须在目标服验收对应维度语法和落点。`check_for_blocks` 默认true。错误配置拒绝加载。

账户按观察到的UUID存储，要求聊天有可靠XUID并与当前观察名一致；不使用姓名回退。准确名字目标与当前实体ID在提交前核对，但执行窗口并非UUID原子绑定；目标环境不能允许该短窗口内不同账号复用同名。重连/改名UUID稳定性仍须目标服核验。

## 回执与恢复

地点及请求保存在安装级存储中；删除或改名不改变已经提交传送的固定落点。请求预约与事件ACK同事务持久保存；倒计时或查询就绪后，固定命令意图与commit ID另行同事务提交。结果未知时只查询原始commit/operation，不重新发出传送；本人新传送会被阻止。使用status核对，仍未知时由服主核对Host原操作，插件没有危险的自动重试或清除未决按钮。冷却从明确成功的确认时刻开始。重启取消未提交倒计时；保存中途重启不会用旧采样续写地点。

保存探针也是有固定意图和回执的world operation。探针成功不代表地点已保存：还需成功原始三整数坐标、可选JSON DataSet一致、前后唯一实体采样与无移动校验，最后才保存地点。

## 来源与验证边界

功能参考 ToolDelta 市场 [传送点设置 0.0.4，SuperScript](https://github.com/ToolDelta-Basic/PluginMarket/blob/ca5d2870cbaf47c29778c52b984cdd8d9e2dcb8f/%E4%BC%A0%E9%80%81%E7%82%B9%E8%AE%BE%E7%BD%AE/__init__.py)。此插件是独立原生实现，没有复制该插件源码，不需要 ToolDelta 或第三方Python库。代码采用仓库AGPL-3.0许可证。

使用公开 [NeOmega toolkit 1.1.0](https://wiki.wuxie233.com/toolkit/) 的Python SDK，Host API最低1.10；SDK不随ZIP打包。协议依据：[execute](https://learn.microsoft.com/en-us/minecraft/creator/commands/commands/execute?view=minecraft-bedrock-stable)、[teleport](https://learn.microsoft.com/en-us/minecraft/creator/reference/content/commandsreference/examples/commands/teleport?view=minecraft-bedrock-stable)。

离线检查不等于真实服验收。发布启用前须验证：脚部原点与testforblock组合命令原始回执、负坐标、save→move→go反复循环Y不漂移、身份字段、各启用维度和真实玩家反馈。当前源码候选不声称这些路径已在目标服通过。
