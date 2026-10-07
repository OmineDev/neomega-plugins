# 共享区块服务

把相同坐标的主动区块请求合并，给多个插件提供经过摘要校验的快照、单格方块和 NBT、分段原始数据与有期限的变化订阅。区块坐标不是方块坐标；Y 范围为 -128..127。

## 使用

安装后授予 `framework.world.subchunk.request`，并在 Host 开启 `NEOMEGA_OBSERVE_CHUNKS`、为本安装设置 `NEOMEGA_SNAPSHOT_INSTALLATIONS`。配置请求/订阅上限、缓存时效和观察超时，启动服务。消费者声明 `neomega.chunks` 依赖及所调用的 `plugin.neomega.chunks.<方法>` service 权限。

| 方法 | 输入 | 返回 |
|---|---|---|
| request | dimension、pos=[x,y,z]、可选 max_age | request_id、state、epoch、原 operation_ids |
| read | request_id；可选 cell=[0..15,0..15,0..15] 及 nbt_offset，或 offset/length | 元数据和单格 layers/NBT，或不超过 16 KiB 的 base64 分段 |
| cancel | request_id | 从合并请求分离本调用方；不取消其他调用方 |
| subscribe | dimension、pos、可选 ttl=60/max_age | subscription_id、request_id、cursor |
| poll | subscription_id、cursor=0 | cursor、updates、gap（消费者未及时读取时明确提示） |

同一安装可以读到自己申请的请求。请求由 Host 提供的调用安装身份绑定，调用参数不能冒充其他安装。订阅是 Worker 生命周期内的有期限资源；重启后重新订阅。每秒处理一个请求，队列和缓存有界。`request` 的新鲜度阈值不超过服务配置。缓存记录包含世界 epoch、原请求回执和快照 revision；重新连接后不把旧世界数据当作当前数据。

## 状态与恢复

`queued → waiting → observed`。`observed` 仅在收到实际快照、验证传输摘要并解析成功后成立。发包成功不会伪装为已观察。超时没有观察为 `unobserved`；换世界为 `epoch_expired`；动作状态为 `unknown` 时只查询原 commit/operation，不发送替代请求。

`read` 的 NBT 保持源 encoding 与字节；不把未观测格子当空气。空 NBT 只有在 `nbt_complete=true` 时才能判定没有 NBT。订阅采用有界最新状态流，`gap=true` 后以返回最新状态重新同步。

## 配置

参见 `config.example.json`。`max_requests`/`max_subscriptions` 为 1..1024；缓存时效 1..3600 秒；观察超时 1..300 秒。缓存仅存已验证的区块字节，不保存临时下载 URL 或凭据。插件不移动机器人，不建立独立游戏连接。

单格 NBT 按 nbt_offset 每次返回最多 16 KiB，length 为完整长度。未知请求不会阻塞后续队列，后台按游标公平轮换。升级 seal 后停止新申请、订阅刷新和后台提交，原缓存仍可读取；release 恢复。

持久请求改用安装 cache 目录逐记录 JSON，写入经过 fsync 与原子替换；Host action 事务选择 `keys=[]`，仅接纳小型 admission 元数据，避免全库快照超过 1 MiB。提交前保存 unknown 与原 commit ID；丢回执只查询原提交，不换 ID，未知 admission 阻止新 action。seal 会读原在途操作，仍未知则返回 busy。公平轮换保证已获得提交回执但区块仍未观察的请求不饿死后续请求。
