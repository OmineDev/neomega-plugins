# Go 状态与事件示例

记录已处理的聊天事件数量，提供 `go_state.count` 只读成员和 `go_state.inspect` 服务。事件状态更新与 ACK 使用同一提交；安装后先配置、授予声明的订阅权限，再由管理员启用和启动。

最低 Host 发行版 1.0.0、Host API 1.9、Worker wire 1.0。不需要 Python；无其他插件依赖。配置字段和默认值见 `config.schema.json` / `config.example.json`。

## 选择平台

目录分别提供 `example.go-state-linux-amd64`、`example.go-state-linux-arm64`、`example.go-state-windows-amd64`、`example.go-state-darwin-amd64`、`example.go-state-darwin-arm64`。版本均为 1.0.0，每个包只包含指定平台的 Worker。

只选择与 Host 一致的一项。各变体导出同名非共享 Bus 成员，不要同时启用多个变体；迁移平台时保留旧安装数据和回执，由管理员处理迁移，不把重新安装当作状态恢复。

```sh
python3 runtime-admin.py catalog-show --plugin example.go-state-linux-amd64 --version 1.0.0
python3 runtime-admin.py --token-file /private/token catalog-install go-state --plugin example.go-state-linux-amd64 --version 1.0.0
python3 runtime-admin.py --token-file /private/token configure go-state
python3 runtime-admin.py --token-file /private/token permissions go-state
```

目录安装不自动授予权限或启动。`permissions` 的交互启动确认可保持停止；未知操作保留原 ID，不自动重放。更新选择同平台相同插件 ID 的固定版本，先查看 `catalog-update-plan` 再按管理工具的确认与修订约束更新；失败保留原安装及恢复标记，不删目录重装。

## Python 插件消费

消费者 manifest 的 `permissions.services` 需声明 `go_state.inspect`，并由管理员独立授予该权限；活动后可调用：

```python
from neomega_runtime import u64
count = await ctx.bus.call('go_state.inspect', result_type=u64,
                           kind='service', major=1, timeout=5)
```

`bus-contract.json` 固定 EmptyRequest → u64 的合同结构与摘要。配置的 `label` 用于 legacy `plugin.<平台插件ID>.count` 服务；原生 Bus 读取和服务返回持久计数。

## 从公开源码构建

源码在本目录 `source/`，独立 SDK 在仓库 `sdk/go/`。`go.mod` 使用本地 replace，不需要克隆私有 Host 仓库或下载未发布的 SDK 模块。仓库根执行：

```sh
python3 tools/package-go-state.py --output /path/to/output
```

此命令仅交叉编译五目标并构建 ZIP，不运行 Worker。包内入口是 `worker` 或 Windows 的 `worker.exe`；打包器按目标生成明确的平台 manifest 和身份后缀。

SDK 与示例未另行授予开源许可证，见 `NOTICE`；不得根据仓库根许可证推断本目录或 `sdk/go/` 的独立许可。
