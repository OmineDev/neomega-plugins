# 玩家档案与身份服务

提供名称、UUID、XUID 的可信解析，保留离线玩家档案、首次和最近出现时间及历史名称。调用插件可保存自己的玩家字段；其他调用插件无法读取或修改这些字段。身份仅从 Host 玩家列表建立，不接受调用者伪造身份。无需 Polaris 在线。

## 安装与配置

安装 `neomega.players` 1.0.0，授予 `players:list` 后启动。需要 Host API 1.8、Python SDK 的可信 ServiceCallContext 和选择性 storage transaction。无必需前置。配置示例见 `config.example.json`，30 秒观察一次；上限 16384 玩家、每人 64 条最近名称记录、每个调用插件/玩家 16 KiB 自定义字段。达到容量时保留已有数据并记录告警。字段写入前先用 `get` 取得修订。

## API（major 1）

服务名为 `plugin.neomega.players.<方法>`。消费者必须声明依赖和调用权限。Host 安装 ID 决定字段命名空间，不采用参数里的自报身份。

| 方法 | 参数 | 返回 |
| --- | --- | --- |
| resolve | `player_id` 或 `uuid`/`xuid`/`name`（组合条件取交集） | `status=resolved, player` |
| get | `player_id` | `player, fields, fields_revision` |
| history | `player_id` | `names, first_seen, last_seen, revision, retained_name_limit` |
| patch | `player_id, request_id, expected_revision, fields` | `status=saved, request_id, revision, fields` |
| query / export | `cursor` 默认 `0:0`，`limit` 默认 50、最多 100 | `items, next_cursor, consistency=live_paginated` |

player 包含 `player_id, uuid, xuid, name, names, first_seen, last_seen, source, generation, revision`。优先以 UUID 建立稳定 ID，无 UUID 才以实际 XUID 建立；以后补齐 UUID 保持既有 ID。未知 XUID 保持空值，不生成假身份。遇到 UUID/XUID 互相冲突拒绝自动合并；名称重用解析为 `conflict`。名称索引保留历史别名，名称详情只保留配置范围内最近记录。

`patch` 为浅层合并，null 删除字段；重复 request_id 同参数返回原回执、异参拒绝。CAS 比较自己的 fields_revision，身份观察不会使自定义字段 CAS 失效。状态和去重回执同一 Host 事务提交；提交不明返回 `unknown`，查询原状态/回执后处理，不重放业务。幂等记录不自动过期。查询/导出在 512 KiB 编码预算内截页并返回续传游标，逐页返回当前状态，并非跨页全局冻结快照。错误区分 `invalid_argument, not_found, conflict, permission_denied, busy, unknown`。

## 使用与恢复

见 `example.py`。重启保留档案、字段和请求回执；服务启动验证 schema_version=1，不支持的未来版本拒绝加载，避免错误降级写入。维护 seal 等待本地事务结束并阻止后续写入；release 必须使用相同 token。观察任务由 Worker 生命周期取消，提交前将原 commit ID 写入安装目录并 fsync；未知和取消跨重启保留，只查原回执，不重放。未确认事务期间 seal 返回 busy。

Host 列表是观察证据，不是原子游戏身份绑定；last_seen 不能证明玩家此刻在线。调用方需要即时在线目标时仍使用 Host 玩家 API。无真服或人工验收要求；当前实现可通过离线接口调用验证。

许可证：AGPL-3.0，见 LICENSE。
