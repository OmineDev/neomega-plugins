# Managed Go Worker SDK

独立 Go module `github.com/OmineDev/neomega-sdk-go`，只依赖标准库，不导入私有 Host。需要 Go 1.23+；配套 Host API 1.9、Worker wire 1.0。SDK 与 Host 应从同批发行包获取。

## 生命周期

用 `Open(ctx)` 建立 Host 提供的 stdio、Unix socket 或 TCP loopback 连接，再将 Peer 交给 `NewWorker(peer, callbacks).Run(ctx)`。Open 使用单次 10 秒连接与握手预算，不重连。socket 使用 Host 发放的认证 token；插件不自行声明安装身份。

`Validate` 只能检查配置、状态及暂存声明；`Activate` 启动活动工作。`Event` 串行处理投递，必须显式提交 ACK。`Drain` 等待之前已接纳事件和受管调用清理，`Spawn` 的协作后台工作被取消并实际 join；成功排空后恢复接纳，不会再次调用 Activate。需要恢复的计时器应在后续事件中显式创建。未协作退出的 goroutine 需要 Host 的进程终止边界，SDK 不伪造排空成功。

`Maintenance` 接收 seal/release 与原 token；回调分别返回 sealed/released、busy 或 unsupported。未注册时返回 unsupported。不要把 detached goroutine 当作受管任务。stdout 专用于 wire；日志写 stderr。

## 状态、事件及操作

`LoadConfig(&settings)` 从 Host 选择的配置文件加载，调用前可设默认值。`Storage.Snapshot` 提供全局 revision 与原始 JSON 值；`PrepareCommit` 冻结 ID、CAS revision、修改和可选事件投递身份，`Storage.Submit` 只提交一次。订阅写在 manifest 的 subscriptions；不存在 SDK 自造的订阅 RPC。`Storage.Ack` 是显式事件提交，不会因回调正常返回而自动 ACK。

`Operations.Prepare` 零 IO 冻结操作、参数、session、幂等键与期限，`Submit` 返回原 operation_id；`Get` 查询原回执，`Cancel` 显式请求取消。停止等待不等于撤销已接纳动作。`unknown` 永远保留，不自动重放。`Uncertain` 保留原 Intent 和已知 ID；跨进程恢复前自行持久保存 `Intent.Method()` 与 `Intent.Payload()`，使用 `state.receipt`/`operations.get` 对账。`RestoreIntent` 只恢复数据，不提交。

## Bus

`Bus.Catalog` 获取目录。`BusRequest` 显式传入 target、major、schema_digest、tagged args、kind 和 deadline，可绑定 provider。SDK 不把任意 Go struct 猜成一个结构合同。整数宽度、float bit pattern、NBT 等按目录的 tagged-value 格式传递；JSON 数字保留为 json.Number。大值沿用 Host 的有界 bulk 通道，不能把 base64 字符串当作任意协议的二进制包装。

`Bus.Submit/Get/Wait/Cancel` 保留原回执；`Bus.Result` 解码成功结果，`Bus.Call` 完成单次提交、等待和结果解码。等待失败仍返回已知回执；不自动重放。导出通过 Validate 返回 bus_exports（完整 member/kind/major/contract/schema_digest/schema），`Callbacks.Bus` 处理 Host 已鉴权调用；manifest 仍须匹配声明和权限。本 SDK 提供 read/service Worker 接口，流提供者应使用现有 Python SDK 或原生 Go Frame。

## 构建

```sh
GOWORK=off go build ./...
```

可构建示例见 `../../examples/go-worker`。构建和源码审核不能证明游戏服务端接受操作或运行稳定；本轮没有执行 Worker、测试或真实 E2E。
