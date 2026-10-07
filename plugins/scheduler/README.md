# 持久调度

为封禁到期、定时奖励、过期清理和周期业务提供持久任务。消费插件主动领取自己的任务，停止或重启不会丢失任务；调度器不执行传入的命令或 Python。

## 安装与配置

插件 ID `neomega.scheduler`，版本 1.0.0。无前置，无游戏权限，不要求游戏会话。使用支持可信 service 调用上下文的 Host/SDK。默认每个调用安装最多 128 个活跃任务，租约 60 秒，补跑最多 32 个周期。配置参考 `config.example.json`；调用者需要逐项申请 `plugin.neomega.scheduler.<方法>` 服务权限。

## 服务 API（major 1）

| 方法 | 请求字段 | 返回 |
|---|---|---|
| schedule | request_id、schedule_id、due_at；可选 interval_seconds、timezone、missed、payload、expected_revision | status=scheduled、schedule、revision |
| query | 可选 schedule_id；或 offset | 单项 schedule（含终态）；不指定 ID 时分页活跃 schedules、next_offset、revision |
| cancel | request_id、schedule_id、可选 expected_revision | status、schedule_id、revision |
| claim | request_id、可选 limit（1..32，默认16）、expected_revision | triggers、revision |
| ack | request_id、schedule_id、trigger_id、token、outcome；可选 expected_revision | status=acked、schedule、revision |

`due_at` 必须是带 UTC 偏移的 ISO 时间，`interval_seconds=0` 表示一次性，正整数表示固定秒数周期（最多一年）。`timezone` 记录业务时区；周期按绝对秒推进，不等于本地每天同一时刻，因此 DST 不会重复槽位。payload 为最多 512 UTF-8 bytes 的 JSON 对象。ID 最多 128 bytes。query 每页 16 项。

## 错过、租约与恢复

默认 `coalesce` 把错过周期合并到最近一次应触发槽位；`skip` 跳过完整错过周期到下一次；`catch_up` 保留最近最多 max_catch_up 个槽位，顺序领取。一次性任务到期后保留至领取，不丢弃。

trigger_id 由调用安装、任务 ID 和计划时刻稳定生成。租约失效重新领取只换 token，不换 trigger_id；旧 token 和过期 token 无法确认。消费者以 trigger_id 幂等处理自己的事务，再 ack `succeeded` 或 `failed`。ack `unknown` 会暂停该任务，避免把不明副作用作为成功或自动重放；可以查询并取消旧任务，以新任务 ID 安排未来业务。已领取的动作是否撤销由消费者负责，cancel 只停止后续领取。

所有变更 request_id 在调用安装范围内永久保存业务回执，相同参数返回原结果，异参返回 `idempotency_conflict`。请求回包不明时用 query 读取任务，或使用完全相同的 request_id 和参数读取业务回执，不重新发明任务。receipt、审计、任务更新在同一个 SDK storage CAS 事务提交。已完成/取消任务移入独立归档，不占活跃任务名额；历史 schedule_id 不可复用。

## 隔离与维护

任务归属仅来自 Host 可信安装身份，参数不能伪造 owner。每个调用者只能查询、领取、取消和确认自己的任务。maintenance seal 在互斥锁下关闭变更，查询仍可用；release token 必须匹配。存储 schema_version 由 manifest state_version=1 表示；升级不清空任务和回执。

示例见 `examples.py`。许可证 AGPL-3.0，见 LICENSE。源码/离线检查与实际游戏效果是不同证据，不要求人工或真服验收才能使用。

## 提交结果不明时

写入前先在受管 data_dir 持久化并 fsync commit_id 标记，再提交 SDK 事务。取消、进程中断和回包不明后，重启仅查询原提交 receipt；查不到保持 commit_unresolved，维护封闭返回 busy，不自动重放。查到原 receipt 才清除标记并允许后续写入。此状态属于运行时数据保护，不要求人工或真服验收。
