# 虚拟分数

管理独立于游戏计分板的持久分数，可查询玩家分值和排行；需要时将某位玩家的分数显式同步到服主允许的游戏目标。

## API

完整服务名为 plugin.neomega.scores.<方法>，major=1。

| 方法 | 参数 | 结果 |
| --- | --- | --- |
| objective | objective、action=create/rename/remove、name；修改公共参数 | revision |
| mutate | objective、player_id、action=set/add/sub/reset；非 reset 需 value；修改公共参数 | revision、player_id、value |
| query | objective，可选 player_id | found、revision、name、count；指定玩家时 value（未设置为 null） |
| rank | objective，offset=0、limit=50（最多100），可选 expected_revision | revision、total、items(rank/player_id/value) |
| sync | objective、player_id；修改公共参数 | revision、sync_state=queued、operation_ids |

目标 ID 稳定；rename 只修改展示名。remove 清空目标数据但保留 revision 墓碑，防止旧请求创建混淆。分数为有符号32位整数，set/add/sub 溢出拒绝；reset 移除玩家行。分数相同按 player_id 排序，不抖动。分页使用 expected_revision 可识别排行已变。单目标最多512个玩家，超过明确报 objective_capacity，不静默截断。

sync_bindings 每项为 namespace、objective、game_objective；服主将一个游戏目标唯一绑定一个虚拟目标。只有配置了绑定才可同步，游戏目标须已存在。sync 将玩家档案当时的名称及分值固定为 set/reset 操作，与业务回执原子提交；不会自动建立或覆盖其他游戏目标。queued 仅表示入持久队列，调用方再次调用 sync，传 action=receipt 与原 request_id，由提供者返回原 result 及 operations 真实操作回执（此查询不需要 objective/expected_revision），未知结果不得重发。

## 配置、权限与持久性

安装 neomega.players 并启动后再启动本服务。目标 Host API 1.8 必须包含可信调用方上下文扩展；缺少扩展时明确拒绝此服务。调用方还须获得 manifest 声明的 service 权限，权限角色不会改变 Host grants。

所有请求可带 namespace，默认调用方 installation_id。默认命名空间只允许该安装访问。shared_namespaces 配置显式声明 name、owner、readers、writers，writers 同时有读权；未授权调用无法自填 owner 获权。namespace 是插件安装作用域，不是浏览器账号。

修改必带 request_id（1–96 字符字母数字/_.:-）和 expected_revision（初始 0）。同调用方、方法和 request_id 重复且参数相同返回原结果；更改参数报 request_conflict。先读 revision 再修改；冲突只返回 revision_conflict，不自动重放。审计记录包含调用安装、操作时间、方法和结果，并与数据、业务回执同事务提交。原回执不会因重启消失。发生 commit_unknown 时保留原请求编号并读取状态；不可改编号重做。提交前 fsync 原 commit/business ID 标记，重启恢复原 commit 回执查询；取消或未知结果均保留跨重启写入屏障，后续修改尝试和维护请求会先查询原业务回执，只有发现原提交后才解除屏障；未发现时继续返回 commit_unknown / busy。启动检查 schema_version=1；已有无版本数据要求显式迁移，不清空内容。

玩家参数 player_id 必须由玩家档案服务解析，不接受自行创造的身份。服务停止、依赖不可用、维护封存、权限不足都有明确错误。维护 seal 在业务锁下封存；release 仅匹配 token。

Python 示例是消费方插件的一部分，消费方自行声明 service 权限和对应 dependency。未新增测试文件；离线检查与实际服务器表现是不同证据。
