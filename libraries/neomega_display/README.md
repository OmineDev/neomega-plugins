# 共享展示服务

同时为多个插件提供物品展示和玩家屏幕消息。任务、菜单和商店可以共享 Actionbar / Title，避免互相覆盖。

## 使用能力

- 物品展示保留 `create / get / update / renew / close` 服务及已有世界展示配置。
- 屏幕展示通过 `channel_acquire` 租用玩家 Actionbar 或 Title；数字越大的优先级越先显示，同级先申请者优先。高优先级结束后低优先级自动恢复。
- `channel_renew` 延长租期，`channel_release` 释放，`channel_get` 查询归属、选择情况及操作证据。
- 调用者必须由 Host 授权服务访问。身份来自可信调用上下文，不能通过请求参数冒充其他插件；租约绑定安装 ID 与运行代次。

## Python 示例

```python
from neomega_display import DisplayClient
client = DisplayClient(ctx)
result = await client.channel_acquire(request_id="quest-step-1", player="玩家名",
    channel="actionbar", text="找到村长领取奖励", priority=50, ttl_seconds=10)
lease = result.payload["lease"]
await client.channel_release(lease["lease_id"], request_id="quest-step-1-close", revision=lease["revision"])
```

消费者声明 `neomega.display >= 1.1.0` 依赖，并逐项声明使用的 `plugin.neomega.display.channel_*` 服务权限。安装后需授予命令执行权限并启动服务。

## 配额与状态

最多 32 个玩家/通道组合、128 个活动租约、每调用者 16 个；文本 UTF-8 最多 2048 字节，优先级 0–100，租期 1–60 秒。每调用者每分钟最多 120 次变更；释放不受该速率限制。每轮最多 8 个命令，持续租约每秒刷新，实际吞吐取决于 `tick_seconds`（默认 0.25 秒）及命令响应速度。

租期从 Host 接受请求时间算起。过期、主动释放或服务重启会结束临时租约；消费者停止后未主动释放的租约在 TTL 后结束，新代次不能接管旧代次租约。跨游戏会话不对新世界重放旧清理命令。

命令意图与状态原子持久化，通过原操作回执核对；结果未知时对应通道冻结，查询保留证据，不自动重发。成功回执仅代表服务端命令结果，`client_visible` 保持未知。

请求回执分键保存，终态租约与回执至少保留 5 分钟，随后自动回收；调用者不得重用已经过期的请求 ID。未知或仍显示中的通道保留证据，不自动回收。最多保留 512 个常规请求索引，状态预算为 160 KiB，达到预算时暂拒新申请，仍允许释放；回收后自动恢复额度。空通道及过期速率记录同时回收。

## 许可证

原始实现遵循随包 LICENSE；世界物品展示 profile 的来源与适用范围见项目完整说明。
