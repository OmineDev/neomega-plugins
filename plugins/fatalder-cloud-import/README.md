# Fatalder 云导入 · neomega 原生插件

管理员在游戏内选择建筑、查看 Worker 报价并确认，插件协调 service_center 机器人 OP 授权、建筑导入与离服握手，默认保留授予的 OP。业务运行于 neomega Python 插件宿主；建筑解析、计费、建造仍由 Fatalder Worker 的共享执行核心负责。

## 安装与配置

需要包含 managed_operator 握手的新版 Worker、已连接目标租赁服且具备授予权限能力 的 neomega Agent，以及支持 config schema/secrets 的插件宿主。Worker 目标配置必须允许 service_center，并允许所配置服号。宿主必须为安装项 `managed:importer` 开启 `NEOMEGA_PLAYER_INSTALLATIONS`、`NEOMEGA_PLAYER_OBSERVATION_INSTALLATIONS` 和 `NEOMEGA_PACKET_SEND_INSTALLATIONS`，发送包类型 `NEOMEGA_PACKET_SEND_IDS` 包含 **185（RequestPermissions）**，并使用允许世界写入的模式。合并到现有授权列表，不覆盖其他安装项；修改宿主启动环境需要按实际部署流程生效。入站订阅授权不能替代发包授权。权限包按实体唯一 ID 定位，不使用玩家名字发送 op/deop。

同一租赁服只安装一个本插件实例；该实例同时只占用一个导入槽。

1. 在仓库根目录运行 `python3 plugins/fatalder-cloud-import/tools/package.py /your/output/cloud-import.zip`。
2. 使用 neomega 提供的管理脚本安装：`python3 runtime-admin.py --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" install importer /your/output/cloud-import.zip`。
3. 运行 `python3 runtime-admin.py --url "$TARGET_URL" --token-file "$TARGET_TOKEN_FILE" configure importer`，按向导填写 HTTPS Worker 根地址、API Key、Worker 目标配置 ID、租赁服号与管理员 UUID 数组。
4. 运行管理脚本的 `doctor importer` 检查，再执行 `enable importer`、`start importer`。API Key 存在宿主管理的私有密钥文件，普通配置只保存 secret 引用；不把真实 Key 写进本目录或安装包。
5. 将建筑文件放入插件**数据目录**的 `imports/`，不是安装目录。文件名含空格时用双引号包裹。也支持 `名称.ref.json`：内容为 Worker 已存对象的完整 ObjectRef（object_id、sha256、size_bytes、display_name），sha256 必须有 `sha256:` 前缀。

插件要求普通 HTTPS 证书验证；不跟随重定向、不读取系统代理。数据目录内 `downloads/` 保存已校验的 Worker 转换产物。首次启动后可由服主管理数据目录文件，不提供任意网络 URL 导入。

有进服密码时，将 `rental_access` 配为 `{"passcode":"secret:rental_password"}`，通过管理脚本 `secret-set` 私有写入该密钥；无密码时保持 `null`。密码仅在启动和恢复请求发出时读取，不写入准备请求或持久化任务回执。Worker 状态采用大写协议值，插件将 `READY` 保持为待确认报价，并兼容 Go 的 RFC3339Nano 截止时间。

## 游戏命令

- `!导入 列表`：显示最多 40 个本地文件。
- `!导入 "房屋.bdx" 100 64 100 overworld`：上传/引用建筑并获取报价。维度可用 overworld、nether、the_end。
- `!导入 确认 <token>`：仅在报价有效期内启动；未确认不会开始建造。
- `!导入 状态`：查看任务、恢复标志与权限租约状态。
- `!导入 暂停`、`!导入 继续`、`!导入 取消`：控制当前任务；取消仍需等待权限会话结束确认。
- `!导入 恢复`：明确恢复 Worker 标为 recovery_required 的任务。若准备/启动响应丢失，使用持久化的原请求和原幂等键核对；不会创建新的扣费请求。

只处理当前玩家名能唯一对应到 UUID 白名单的聊天消息。报价金额和币种直接来自 Worker，不自行换算计费单位。默认导入参数由 `shared/flow` 导出，不另设第二套产品默认值。

## 状态与边界

准备/启动请求先保存再发送；进程重启仅恢复观察，不自动开始建造。聊天请求按宿主事务确认接收；如果进程恰在确认接收后、保存具体任务意图前停止，该命令不会自动重放，需要管理员检查状态后重新操作。网络失败不等于动作失败，应先查询状态。普通 SSE 断线不会取消任务，重连从已保存游标继续。

Worker 提供真实连接 UUID、名字、session/attempt 与目标服绑定。插件用 Agent 新玩家观察核对身份和权限后才授予 OP；已有 OP 原样保留。权限动作使用持久化确定性回执，只在操作成功且权限读回成功后 ACK。未知结果保留待核查租约，不重新发授权动作、不猜测成功。`revoke_operator_on_completion` 默认 `false`：导入结束、取消或失败时保留 OP，先持久保存本次会话的保留策略和权限租约记录，再 ACK 允许 Worker 结束会话、离服；该 ACK 不表示已撤权，也不把未知授权结果标为成功。

如果服主主动配置 `revoke_operator_on_completion: true`，仅对确认由插件授予的 OP 恢复授权前的权限等级与可请求的低 8 位能力标志，读回确认后才结束会话。策略随授权租约保存；修改配置作用于新会话，丢失 ACK 后恢复仍沿用原策略，防止意外撤权。最近一次保留记录在持久状态的 `last_operator_session` 中。

Worker 授权和会话结束确认各最多等待 120 秒。开启撤权策略时，插件离线、宿主观察 epoch 改变、机器人已离服、回执未知或撤权超时，均可能留下待人工核查的 OP；此时不能声称已恢复原权限。请核对实际机器人 UUID/实体 ID/权限和任务回执，处理完成前保持导入槽锁定，不清空状态绕过检查。默认保留策略在 ACK 成功或读回 Worker 已结束会话后释放权限租约，任务终态后可继续下一个导入。已有任务绑定 Worker 地址、密钥引用和目标服；处理中不要更换这些配置。密钥文件并非对同系统用户恶意插件的安全沙箱。

本版没有实服验收记录；单元测试与 Go 构建只证明源码层行为。实服需验证管理员/非管理员、报价确认、实际导入、原有 OP 保留、默认授予后保留、可选撤权、取消、断线与重启恢复。

## 开发验证

在仓库根目录：

```sh
PYTHONPATH=plugins/fatalder-cloud-import python3 -m unittest discover -s plugins/fatalder-cloud-import/tests
```

业务默认值已作为版本化 JSON 随包发布，打包和测试不依赖 Fatalder 源码。需要同步默认值时，在已授权的 Fatalder checkout 中运行 `GOTOOLCHAIN=go1.26.0 go run "$PLUGIN_CHECKOUT/plugins/fatalder-cloud-import/tools/export-defaults.go"`，审核输出后更新 `fatalder_plugin/build_defaults.json`；当前来源为 Fatalder `1b3d3849fbaf82c7a7cd05ce958846f142e0b50f`。

修改 Settings 后，用 neomega SDK 的 `schema_for_config(Settings)` 重新导出 config.schema.json。本仓库运行插件测试与 ZIP 制品检查；修改 Worker 公共 API 时须另在 Fatalder 运行其门禁。安装包白名单排除测试、缓存、配置与密钥。

## 从 Fatalder 仓内版本迁移

独立版本 1.0.1 保持 `fatalder.cloud-import`、`state_version: 1`、配置声明和持久状态键不变；仅改变源码/制品归属及可复现打包。现有安装使用相同名称（如 `importer`）执行 `update`，不要卸载重装或改名。先停止新任务并核对没有进行中的导入、未知请求、权限租约，再停插件，私下备份旧制品、配置、密钥与数据后更新；有未完成任务则继续使用原版，不能清空回执绕过。宿主状态、事件游标和数据目录不搬入新仓库或 ZIP。迁移不会自动启动、重放任务或撤销世界写入。

迁移不改变原 Worker 的 120 秒结束 ACK 行为；插件离线完成的改进由后续独立版本实现。
