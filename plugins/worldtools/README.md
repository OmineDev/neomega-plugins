# 世界工具与区域备份

把世界读取、空气与方块状态恢复、容器、命令方块和大型导入组合为持久任务。安装前可看清覆盖范围和缺项；执行后沿原任务查询、取消或继续，不重复发送未知动作。

## 安装与调用

需要 `neomega.chunks` 和支持 `framework.world.set_block`、维度绑定 `framework.world.region.read` 的 Host 1.9+（旧 minor 8 不保证新增 capability 已安装）。授权 manifest 实际操作后启动。NBT 使用 Host 的受管机器人工作区；传入 `anvil_pos` 与 `workspace_pos`。容器与世界动作受 Host 权限、窗口和世界模式控制。

每个消费者授权 `plugin.neomega.worldtools.<方法>`；可信 installation 隔离任务。`prepare` 的 `request_key` 同键同参数返回原任务，异参拒绝。

| 方法 | 输入 | 结果 |
|---|---|---|
| prepare | request_key, kind, args | task_id、范围、步骤、losses |
| execute / cancel / resume | task_id, expected_revision | 启动、边界取消、沿原任务继续 |
| status | task_id, offset=0, limit=2 | 分页步骤、原 commit 与 operation 回执 |
| snapshot | task_id, index=0, offset=0 | 2³ tile 的持久 JSON，16 KiB base64 分页与摘要 |
| inspect | request_key, pos | 单方块读取计划；随后 execute/status |

## 便携备份与差异恢复

`backup` 默认 `mode=snapshot`，输入 `dimension:0|1|2,pos,size`。每片最多 2³ 方块，实际数据来自维度核验的原生结构读取，保存到安装受管目录 `world_records/<task>.tile.<index>.json`。每片含维度、范围、epoch、所有格子（包括空气）、前景名称/状态、原始 NBT、背景层、逐格 coverage/missing、区域 losses 与 SHA256。未观察格子绝不会当空气。

`restore` 默认 `mode=snapshot`，输入 `backup_id,destination?,dimension?,anvil_pos?,workspace_pos?`，只接受同调用者完整成功的备份。准备时检查所有快照摘要、报告缺项；执行时按 tile 读取目标、比较覆盖字段并展开有界写入步骤，每一步保存原回执。支持跨连接 epoch 的持久恢复；执行仍绑定当次 epoch 和维度。容量由 max_blocks、max_steps、max_state_bytes 限制；差异步骤超额明确失败并保留已完成范围。

| 覆盖项 | 精确恢复范围 |
|---|---|
| 空气和前景方块 | 名称及受支持状态；空气会主动清除目标方块 |
| 普通箱子/陷阱箱/木桶 | 完整且可表达的 Items 列表，基础 Name/Count/Damage/Slot；先清空容器再分批写入，含空槽恢复 |
| 命令方块 | 指令、类型、朝向、条件、红石、延迟、名称、输出开关、首 tick 开关 |
| 不支持字段 | 原始内容仍在快照，missing 明确列出，既不伪称完整也不随意转成近似数据 |

带复杂 item tag 的容器不声明 Items 完整覆盖；背景层、双箱配对拓扑、任意私有 NBT、其他方块实体、实体、计划 tick 不在精确覆盖内。容器带不支持的业务属性逐项报告。恢复不承诺整个存档级原子性。修改同一目标的外部插件仍可能改变最终世界；这里不引入人工或真服验收门。

## 其他世界任务

- `copy`：`pos,size,destination`，先保存所有源 tile，再加载目标，支持重叠；服务器内存结构沿原 epoch 使用。
- `backup mode=structure`：显式旧式内存 structure + BDX artifact；`restore mode=structure|bdx` 恢复旧备份。BDX 不支持空气清除、容器、背景层和完整 NBT，不冒充默认便携快照。
- `container`：`pos,slots,anvil_pos,workspace_pos,dimension?,block?`；`second_pos` 启用双箱 0..26/27..53 槽映射，每动作最多九槽；普通容器任务不自动清未声明槽。
- `command_block/sign/item/enhanced_item/structure_block/pick_block/place_block/held_on_block/item_frame`：args 对应 SDK 同名方法，不传 idempotency_key/deadline。

## 可选 Fatalder 大型导入

额外安装 `fatalder.cloud-import >=1.2.0`、配置独立 Worker 与来源文件，在其 `service_installation_ids` 授权本插件安装。此依赖可选，不安装不影响其他世界能力。费用与同服唯一任务继续由 Fatalder 执行。

| 方法 | 输入与行为 |
|---|---|
| fatalder_prepare | request_key, source_name, position, dimension（overworld/nether/the_end）；返回 task_id 与 remote 报价，尚不扣费 |
| fatalder_status | task_id；读回同一来源 request_key/原 job，不新建导入 |
| fatalder_confirm | task_id, confirm_token, confirmation_key；必须与报价原 token/key 匹配才开始计费执行 |
| fatalder_pause / fatalder_resume / fatalder_cancel | task_id, control_id；沿原 job 操作，相同 ID 仅读原记录 |
| fatalder_recover | task_id；使用原 request_key/job_id/confirmation_key 恢复，不换任务 |

来源为 Fatalder 安装 imports 的文件或已有 `.ref.json`，不是任意 URL。网络丢回执保留原 service intent/call_id，status 可查询，未知写不会自动重试。确认动作是费用业务必需的显式调用，不是真服或人工验收门。

`fatalder_status(task_id)` 是丢失在线 call_id 或旧 generation 回执不可读时的只读恢复入口，按原 request_key 查询服务端持久任务。只有 prepare 的原 request_key + job_id，或 confirm 的原 job_id + confirmation_key 与服务端 accepted_start_key 完全一致，才把对应未知请求记为 observed。阶段名称不作为成功证据；暂停、继续、取消等缺少原调用证据的请求继续保持 unknown，不重发。

## 持久性、并发与维护

业务记录逐任务 fsync + 原子替换，快照独立分片；不把整个任务库写进 Host 单值。每个世界动作仅以 `transaction(keys=[])` 原子接纳小型 admission 记录与动作。先 fsync 原 commit ID 和 unknown 状态再提交，进程断电后只查原 receipt；未确定提交不产生新的世界动作。配置提供任务数、区域、步骤和总业务记录字节预算。记录默认保留，容量满明确拒绝。

区域相交任务串行；容器与工作区步骤独占本插件机器人租约。unknown 保留范围直到核对；取消不回滚已经写入。其他插件竞争仍由 Host 世界窗口控制。

服务输出的步骤和回执分页，大参数和大结果返回摘要，持久快照用 snapshot 接口读取。维护 seal 先核对在途动作，未知返回 busy；封存后不再接纳写操作，相同 token release 恢复。

任务记录使用 schema_version=2 与递增 revision；旧记录只迁移元数据，保留原动作 ID 与 unknown。execute/cancel/resume 必须携带最近读回的 expected_revision；过期返回 revision_conflict，重新读取后再决定操作。后台进度也会改变 revision。每个容器仅首批创建 block，后续批次仅写槽位；指定维度固定传入每批。维护检查完整本地及远端任务生命周期，服务已回复不等于远端已结束。

Fatalder 控制调用必须提供稳定 control_id；接受后查询 fatalder_status，返回 remote.controls 及本地 remote_calls.control_state。accepted/unknown 保留原 ID 并拒绝新控制；只有原控制 completed 回执证明执行接纳；原服务明确的接纳前拒绝单独记为 rejected，相同 ID 返回原拒绝，修正条件后可使用新 ID。普通 failed 或超时仍是 unknown。desired_state_observed 仅说明当前阶段已符合目标，不证明原请求成功。旧无控制 ID 的记录继续保守保持未知，不换 ID 重发。
