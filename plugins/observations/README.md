# 共享观察

为插件共享一次采样结果，避免每个玩法重复查询位置和机器人背包。支持按需采集、短期订阅、缓存新鲜度、协议死亡/重生观察及 TPS 估计。无必需前置；需要 Host API 1.8 和对应操作/数据包观察授权。

## 安装与配置

安装 ZIP 后保持停止；使用 config.example.json 配置后，授权 manifest 所列只读操作及 ActorEvent、Respawn、SetTime 包观察，再启动。interval 是采样完成后的等待秒数，不会积压重复采样任务。没有订阅时不主动采位置/背包；refresh 可直接更新。关闭或重载释放采样和订阅；缓存与租约不跨重启。

## API

所有服务名为 `plugin.neomega.observations.<方法>`，major 1，参数为 JSON；调用者需在 manifest 声明服务权限。订阅归属以 Host 可信 installation_id 判定。

| 方法 | 参数 | 返回 |
| --- | --- | --- |
| subscribe | kinds 数组：position/inventory/death/respawn/tps/gap | subscription_id、cursor、epoch、expires_at |
| snapshot | kinds 可选；player 可选 | observations 映射；每项含来源、时间、complete、stale |
| refresh | 同 snapshot | 先执行一次有界只读采样，再返回快照 |
| poll | subscription_id、cursor | events、cursor、epoch、gap；轮询自动续租 |
| unsubscribe | subscription_id | removed |
| read | kind、sequence、offset=0、limit=8192 | JSON 文本 fragment、next_offset、done、length、sha256 |
| status | 空对象 | 流状态、订阅数、支持范围、维护状态 |

`consumer.py` 是消费者插件可直接调用的用法。调用期间发生 timeout/失败不表示数据不存在；snapshot 未获得该来源时返回 unavailable，失败采样不会变成空背包。源时间与本地读取时间分开保留。消费者检测 stale、gap 或 epoch 变化时应重新读取快照。

## 覆盖范围

位置来自 Host entities.query，有数量上限，保留 complete。背包是**当前机器人自身主背包**的原生缓存证据，原始 slot unknown 状态和操作回执保留；传入 player 请求其他玩家背包返回 unavailable，不推断为空。权限不足按 Host 拒绝处理。

死亡来自机器人能观察到的 ActorEventDeath（EventType 3），并非全服所有玩家；只返回实体 runtime ID，不伪造 XUID。Respawn 返回协议原 state，观察到阶段不等于玩家已经完成重生。二者按同连接事件序号去重，事件缓冲耗尽以 gap 表达。未观察到事件不代表没有死亡。

TPS 是 SetTime 昼夜时间增量除单调时钟耗时的估算，quality=daytime_estimate；停止时间、回退、跳变或样本不足为 unavailable，不能视为权威服务器 tick。显示缓存须检查 stale。协议流停止后不会静默重连或跨代复用缓存。

## 维护与检查

seal 暂停新订阅和主动采集，release 恢复；没有世界写动作或持久账本。工程交付包括静态 schema、导入、ZIP 白名单与离线直接调用；真实游戏表现未经本项检查证明，不设置实服/人工门。

大于服务信封的数据保留在缓存，快照返回 payload_available 和序号。用 read 分页拼接 fragment、核对 SHA-256 后 JSON 解码；offset 按 Unicode 字符计数。采样已更新则 conflict，重新获取快照；不会把截断当完整。多种 kind 合并超过信封时按提示逐 kind 获取。
