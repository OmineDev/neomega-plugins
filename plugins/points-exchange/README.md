# 最小积分兑换示例

固定 **10 测试积分兑换 1 个苹果**。每个白名单玩家首次兑换时从 `initial_points` 取初始积分（默认 100），以后使用 Host 持久余额；修改配置不会重置已有余额。仅供学习，不对接真实货币或经济插件。

## 安装与权限

使用提供 `Business`、`Storage`、`Operations` 的 neomega-agent Python SDK（Host API 1.2，worker protocol 1.0）。SDK 由 Host 提供，ZIP 不复制 SDK、不安装第三方依赖。插件 ID `example.points-exchange`，版本 `1.0.0`，状态版本 `1`。

```sh
python3 plugins/points-exchange/tools/package.py dist/example.points-exchange-1.0.0.zip
sha256sum dist/example.points-exchange-1.0.0.zip
# 下列路径指向自己的 neomega-agent checkout 和本地 Host 管理令牌
python3 "$AGENT/scripts/runtime-admin.py" --url "$HOST_URL" --token-file "$TOKEN_FILE" install points-demo dist/example.points-exchange-1.0.0.zip
python3 "$AGENT/scripts/runtime-admin.py" --url "$HOST_URL" --token-file "$TOKEN_FILE" configure points-demo
python3 "$AGENT/scripts/runtime-admin.py" --url "$HOST_URL" --token-file "$TOKEN_FILE" permissions points-demo
```

配置参考 `config.example.json`，将占位 UUID 替换为测试玩家的规范小写 UUID；默认白名单为空，不接受兑换。`initial_points` 为 0–10000 的整数。配置使用 Host 现有交互入口，权限交互会展示并确认本包声明的权限：

- `players:list`：按聊天姓名（有 XUID 时一并核对）唯一匹配玩家，再检查 UUID 白名单。
- `event:chat.received`：读取持久聊天订阅。
- `operation:framework.command.execute`：发送固定 `give` 奖励与 `tellraw` 反馈。

安装保持 stopped。仅在自行准备好的测试环境、已有游戏 session 且完成上述配置与授权后，通过现有 `enable points-demo`、`start points-demo` 启用。离线检查无需启动插件。本次交付不包含发布 Release 或实服操作。

## 兑换与查看

```text
!兑换 demo01
!兑换 状态 demo01
```

请求号只允许 1–32 位英文字母、数字、下划线或连字符。`玩家 UUID + 请求号` 哈希得到固定业务标识；不同玩家可以使用相同请求号。重发 `!兑换 demo01` 只读取原交易，不再次扣分或创建发奖意图。真正的下一笔购买才用新请求号；改名不会改变旧交易身份。

扣分、业务记录和唯一奖励意图由 `Business.commit` 在一个 Host 状态事务中持久化。事件 ACK 与反馈另用现有事件事务提交；若在业务提交之后、ACK 之前中断，重投事件会读取原业务回执。反馈命令属于通知，不是额外奖励。默认没有轮询任务，输入状态命令刷新原 Operation 回执。

| 状态 | 含义与处理 |
| --- | --- |
| `pending` | Host 已接纳，尚未获得终态；继续查询同一请求号。 |
| `succeeded` | 原 Operation 的成功回执；表示命令反馈成功，不声称背包实测。 |
| `failed` / `cancelled` | 原回执的确定失败或取消；不自动退款或补发，不能据此推断所有副作用均可回滚。 |
| `unknown` | 保留待人工核对；不换键重发，不自动退款或补奖。 |
| `insufficient_points` / `not_found` | 余额不足的已保存结果，或查询的请求从未提交。 |

Host 保存 `points/<uuid>` 余额、SDK Business 业务记录（含原发奖 intent）、原 commit 的 operation ID；每次状态查询将完整 Operation 回执和当前分类保存到 `exchange_status/<业务标识>`。状态副本只是最近一次查询结果，最终依据仍是原 Operation。余额不足也固定在该请求号，之后重复不会变成另一笔购买。

遇到 `unknown`，记录玩家 UUID、请求号、聊天显示的 operation ID，查询**原请求**并由管理员核对原命令回执、相关游戏记录及实际物品情况。证据不足就维持 unknown；不要用新请求号“重试”，不要删状态重建。本示例没有人工恢复入口或补偿后台，也不把超时当失败。

## 轻量离线检查

在仓库根目录运行；`AGENT` 指向相同版本的 neomega-agent 源码：

```sh
PYTHONPATH="$AGENT/sdk/python" python3 plugins/points-exchange/tools/check-offline.py
python3 plugins/points-exchange/tools/package.py dist/example.points-exchange-1.0.0.zip
```

直接脚本使用真实 SDK 与很小的本地 IPC 模拟，验证正常、重复、failed、unknown、提交回包丢失和余额不足；从临时磁盘状态重建客户端，检查余额、原回执及奖励意图数量。临时状态自动删除，无新增测试框架。它不证明真实 Host 调度或游戏执行；真实 E2E 不在本票验收范围。

打包复用仓库显式文件白名单、固定时间和权限的 ZIP 方式：仅源码、manifest、静态配置 schema/示例、说明与许可证，不含验证脚本、SDK、缓存、密钥或运行数据。许可 AGPL-3.0。
