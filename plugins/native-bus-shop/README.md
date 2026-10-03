# 原生 Bus 发货服务

提供 `shop.buy` v1 服务，将订单、计数和发货意图原子保存，再返回原 operation ID。它只向指定玩家发放石头或面包，不提供价格、余额、扣款、聊天入口或商店界面。

## 安装与授权

需要 Python 3.10+、Neomega 1.0.0 工具包 Python SDK、Host API 1.6 或以上及已连接的游戏会话。插件没有其他插件依赖；Host 必须提供 `command.execute`。先安装并启用此 provider，调用方随后安装且独立申请 `service:shop.buy`，声明依赖 `example.nativebusshop`。仅安装本包不会发货，必须由获授权调用方发起请求。

从官方目录取得 `example.nativebusshop` 的固定版本 `1.0.0`，核对 SHA-256。`$TOOLKIT` 是开发工具包目录，`$PACKAGE` 是下载的 ZIP；管理令牌只从文件读取：

```sh
python3 "$TOOLKIT/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" install native-bus-shop "$PACKAGE"
python3 "$TOOLKIT/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" permissions-plan native-bus-shop
python3 "$TOOLKIT/runtime-admin.py" --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" permissions native-bus-shop
```

配置为 `{}`，没有自定义配置项。安装保持停止，权限预览不授予权限；交互授权后需另行确认启动，回车可保持停止。本插件只申请 `service:command.execute`，用途是发送订单对应的 `give` 发货命令。不申请聊天、玩家列表或原始包权限。授权后任何有权调用 `shop.buy` 的调用方均能申请免费物品，管理员应限制调用方权限。

## 请求与返回

服务成员为 `shop.buy`，major `1`，schema digest 固定为 manifest 的 `bus.exports[0].schema_digest`。调用方使用配套 SDK 的 Bus 服务调用，字段为：

| 字段 | 类型及限制 |
| --- | --- |
| `business_key` | 1–128 个 ASCII 字母、数字、下划线或短横线 |
| `player` | 1–16 个 ASCII 字母、数字或下划线；不支持空格或中文玩家名 |
| `item` | `stone` 或 `bread` |
| `count` | `u32`，1–64 |

返回 `business_key`、`order_number`（`u64`）、`commit_id`、`operation_ids`、`duplicate`、`state` 和 `receipt_json`。`accepted` 表示意图已受理，不能视为物品已送达；服务最多短暂等待原动作回执，超时仍返回原 ID。`receipt_json` 为空表示未取得最终回执。

业务键按 Host 可信调用方安装身份隔离。同一身份和键的相同请求读取原订单和原动作，不重复发货；请求参数不同会冲突。无效玩家名、物品、数量或业务键被拒绝，权限不足、会话缺失及调用期限等错误由 Host/SDK 返回。

订单存于 `shop/orders/<order_number>`，计数存于 `shop/order_count`，幂等记录由 SDK `business.once` 保存。停止、更新和恢复时保留这些状态及历史回执。遇到 `unknown` 先对账原 operation ID，不换键自动重放；不要删数据重新安装。

## 源码与更新

本包包含完整 provider 源码，不包含调用方、不捆绑 SDK 或私有 Host。源码来自 Neomega 作者工具包同一示例；公开使用范围见 NOTICE，未新增开源许可。

社区仓打包：`python3 tools/package-plugin.py plugins/native-bus-shop --output example.nativebusshop-1.0.0.zip`。`release.json` 和 `package.files.json` 是逐文件白名单，不包含真实配置、状态或测试。更新使用管理工具 `update-plan` / `update`，核对版本、权限差异及摘要；失败时保留原安装与恢复标记。作者文档见 [Neomega Wiki](https://wiki.wuxie233.com/)。
