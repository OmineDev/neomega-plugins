# 角色权限

让插件以角色、玩家和资源作用域表达业务权限，支持限时成员；不授予游戏管理员或 Host 操作权限。

## API

完整服务名为 plugin.neomega.roles.<方法>，major=1。revision 属于整个 namespace 策略。

| 方法 | 参数 | 结果 |
| --- | --- | --- |
| define | role、action=set/remove（默认set）、capabilities；修改公共参数 | revision |
| assign | role、player_id、scope=global、expires_at（可选RFC3339）；修改公共参数 | revision、player_id |
| revoke | role、player_id、scope=global；修改公共参数 | revision、player_id |
| check | player_id、capability、scope=global | allowed、roles、scope、revision |
| list | 可选 player_id、offset=0、limit=50（最多100）、expected_revision | revision、total、items |

capabilities 为精确权限节点，无通配符、隐式角色继承或默认管理员。global 授权可满足任意 scope；其他 scope 精确匹配。能力由匹配角色取并集，无角色默认拒绝。到期时间必须带时区且晚于当前时间，每次 check 动态判断，到期数据保留以便查询。define remove 同事务撤销该角色所有成员；不存在角色的 assign 拒绝。相同玩家/角色/作用域 assign 替换到期时间；revoke 幂等。

list 不指定玩家列出角色定义，指定玩家列出其成员记录（含 active 状态）；分页排序稳定，expected_revision 检测页间策略变化。每个 namespace 最多128角色、512成员，单角色最多128个能力；超过显式拒绝。不要将服务返回 allowed 当成 Host 权限委托。sync_bindings 在本插件不使用，保持为空。

## 配置、权限与持久性

安装 neomega.players 并启动后再启动本服务。目标 Host API 1.8 必须包含可信调用方上下文扩展；缺少扩展时明确拒绝此服务。调用方还须获得 manifest 声明的 service 权限，权限角色不会改变 Host grants。

所有请求可带 namespace，默认调用方 installation_id。默认命名空间只允许该安装访问。shared_namespaces 配置显式声明 name、owner、readers、writers，writers 同时有读权；未授权调用无法自填 owner 获权。namespace 是插件安装作用域，不是浏览器账号。

修改必带 request_id（1–96 字符字母数字/_.:-）和 expected_revision（初始 0）。同调用方、方法和 request_id 重复且参数相同返回原结果；更改参数报 request_conflict。先读 revision 再修改；冲突只返回 revision_conflict，不自动重放。审计记录包含调用安装、操作时间、方法和结果，并与数据、业务回执同事务提交。原回执不会因重启消失。发生 commit_unknown 时保留原请求编号并读取状态；不可改编号重做。提交前 fsync 原 commit/business ID 标记，重启恢复原 commit 回执查询；取消或未知结果均保留跨重启写入屏障，后续修改尝试和维护请求会先查询原业务回执，只有发现原提交后才解除屏障；未发现时继续返回 commit_unknown / busy。启动检查 schema_version=1；已有无版本数据要求显式迁移，不清空内容。

玩家参数 player_id 必须由玩家档案服务解析，不接受自行创造的身份。服务停止、依赖不可用、维护封存、权限不足都有明确错误。维护 seal 在业务锁下封存；release 仅匹配 token。

Python 示例是消费方插件的一部分，消费方自行声明 service 权限和对应 dependency。未新增测试文件；离线检查与实际服务器表现是不同证据。
