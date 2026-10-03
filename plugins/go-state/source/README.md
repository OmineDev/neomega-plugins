# Go 状态服务示例

独立 Go Worker 将 `chat.received` 的 durable ACK 与计数状态放在同一个 CAS 提交里；导出 Native Bus `go_state.count`（候选可读 read）及 `go_state.inspect`（活动 service），均为 EmptyRequest → u64。也保留 `count` 在线服务返回 label 和 count。它不发送游戏动作。事件订阅需要活动游戏 session；旧在线服务名为 `plugin.<已安装的manifest.id>.count`，major 1，参数 `{}`。身份由 Host 提供，源码没有固定安装 ID。

Python 消费者在 manifest.permissions.services 声明 `go_state.inspect`，激活后可以调用：

```python
from neomega_runtime import u64

count = await ctx.bus.call('go_state.inspect', result_type=u64,
                           kind='service', major=1, timeout=5)
```

无业务参数时 Python SDK 使用同一 EmptyRequest 合同。`bus-contract.json` 的规范结构和 digest 随示例固定；源码审核核对其标准 token 摘要算法，未运行跨语言 E2E。

在此目录构建：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOWORK=off go build -trimpath -o worker .
```

`go.mod` 的相对 replace 使用随包提供的 `../../../sdk/go`；不需要私有 Host 源码。源 manifest 默认 linux/amd64，发布脚本为每个平台写入正确 target_os、target_arch、entrypoint 和 `example.go-state-<os>-<arch>` ID。Windows 入口为 worker.exe。将可执行文件、manifest.json、config.schema.json、config.example.json 放入同一个插件包；不要用 Linux manifest 安装其他平台产物。

`config.example.json` 可作为安装配置。回调在 validation 阶段检查配置和持久 count，排空由 SDK 负责。revision 冲突或提交回执不确定时会失败并保留日志中的 commit ID；不会自动重试、重复 ACK 或重放 unknown。恢复时先查询原 state.receipt，再决定人工处理。

本示例只完成构建和源码审核，不代表已执行、连服或真实 E2E 验证。
