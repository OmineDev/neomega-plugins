# 固定口粮兑换

基于 `plugins/points-exchange` 的独立变体：`!口粮 <request_id>` 用 **12 测试积分换 3 个面包**；`!口粮 状态 <request_id>` 查询原回执。请求号为 1–32 位字母、数字、下划线或连字符，不扩展商品、充值或通用商店。

沿用参考例的 UUID 白名单（默认空）、首次默认 100 积分及 `initial_points` 配置（0–10000）；已有余额不因改配置重置。示例 UUID 必须按实际授权玩家替换。SDK 由 Host 提供，包内不携带 SDK。插件 ID 为 `example.ration-exchange`，权限只包含持久聊天订阅、玩家列表和固定 give/tellraw 命令。

玩家 UUID 与请求号形成稳定业务键；扣分、业务记录和唯一奖励意图通过 SDK `Business.commit` 一起持久化。重复请求读取原业务及 Operation，不再次扣款或发货，改名也不改变身份。状态查询保存原 operation ID 与完整回执。`unknown` 保留待核对，不自动补发、退款或换键；failed/cancelled 同样没有自动补偿。反馈通知与事件 ACK 独立提交，通知不是第二份奖励。成功仅代表原命令成功回执，未证明实际背包物品。

## 离线复现

从插件仓根目录运行，`AGENT` 指向对应 neomega-agent checkout：

```sh
mkdir -p dist
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$AGENT/sdk/python" python3 experiments/issue38/ration-exchange/tools/check-offline.py
(cd "$AGENT" && GOWORK=off GOTOOLCHAIN=go1.26.0 go build -o "$OLDPWD/dist/neomega-plugin-check" ./cmd/neomega-plugin-check)
python3 "$AGENT/scripts/runtime-plugins.py" --checker dist/neomega-plugin-check pack experiments/issue38/ration-exchange dist/example.ration-exchange-1.0.0.zip
python3 "$AGENT/scripts/runtime-plugins.py" --checker dist/neomega-plugin-check check dist/example.ration-exchange-1.0.0.zip
```

受限环境可将 `TMPDIR`、`GOCACHE` 设为本仓 `dist/` 下的绝对路径。检查脚本复用参考例的本地 IPC 模拟和真实 SDK，临时 JSON 状态在 `dist/` 创建并自动清理；验证正常、重启后重复、unknown、确定失败、提交响应丢失、余额不足及聊天身份/ACK。显式核对奖励命令为 3 个面包、每笔扣 12 分。无新测试框架，不连接 Host 或游戏，不启动机器人。

## 本次体验记录

- 开始：2026-09-26 19:31:42 UTC；结束：2026-09-26 19:33:32 UTC；约 1 分 50 秒（含必要离线验证，不含主代理集成）。
- 框架源码：`415623638df86722059fc9b0cc71c642b53c0b66`；插件参考仓：`21d549010f472c76880e6ec20d9347f717aeddc1`。
- 模型：`gpt-6-astra`，推理强度 `medium`（集成时由父任务会话元数据及未覆盖的继承设置确认）。
- 实现完成；轻量离线场景、basic 模板及口粮包 pack/check 均通过。未进行实服 E2E、发布、提交或推送。
- 遇到的错误：初次生成命令使用错误工作目录，修正路径并清理误建空目录；Go 默认临时路径不可写，改为本仓 dist；均自行处理，无需人工帮助。
- 建议：在开发交接直接列出独立插件仓的积分参考例；为受限工作区补充 Go 临时目录和缓存目录配置示例。

许可证沿用参考例 AGPL-3.0。发布白名单不包含离线核对脚本、缓存、状态或私有配置。
