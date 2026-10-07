# 经济账本

为商店、银行、红包等插件提供独立安装隔离的多货币账户、整数余额、原子转账、冻结和不可变流水。它不修改游戏计分板，也不自动发放初始资金。

安装后配置 `currencies`，把可信消费插件的 **安装 ID** 填入 `allowed_callers`。只有同时位于 `mint_callers` 的安装可以调用增减余额 `post`。默认空白名单拒绝访问；这些插件受托管理账户，玩家是否可以购买/付款由业务入口授权，不能把玩家输入直接透传到账本。依赖 `neomega.players`；玩家账户使用该服务提供的稳定 `player_id`，商户等非玩家账户使用业务稳定 ID。

## 服务 major 1

服务前缀 `plugin.neomega.economy.`。调用者需要声明对应服务权限。全部调用要求 Host 提供可信调用上下文。

| 服务 | 参数 | 结果 |
| --- | --- | --- |
| balance | currency, account | balance, held, available, revision |
| post | currency, account, amount, request_id | 提交后的账户和 sequence；amount 可正可负 |
| transfer | currency, account, target, amount, request_id | 原子扣款与入账，禁止向自身转账 |
| hold | currency, account, amount, hold_id, request_id | 冻结可用额度，余额不变 |
| release | currency, account, hold_id, request_id, capture=false | 解除冻结；capture=true 将冻结额消费 |
| receipt | request_id | committed 和结果，或 unknown |
| ledger | after=0, limit=20 | 本调用者流水、cursor、has_more |

`amount` 为最小货币单位整数，拒绝 bool、小数、零金额与透支；单次金额和余额上限 2^53−1。冻结额不参与可用余额。账户 revision 每次成功变更递增；转账两端、流水序号、去重回执在一个 Host storage CAS 事务内提交。不同账户共享安装账本，调用者身份隔离的是 request_id、hold_id 和流水访问。

同一调用者重复相同 request_id 和参数返回原结果，异参或不同操作复用该 ID 被拒绝。hold_id 不可复用，已释放冻结不能二次释放。流水只追加，分页 cursor 为全局扫描位置，因此页面可能没有当前调用者的记录但 has_more 为 true。业务应继续分页。

未知提交结果先调用 receipt 查询，unknown 表示尚不能确认，不代表失败，禁止换 request_id 自动重放。游戏物品交付与此账本没有跨系统事务，商店应保存自己的业务过程与补偿决策。maintenance seal 在事务锁下关闭业务入口，release 按 token 幂等开放；不提交游戏动作。

`consumer.py` 提供转账调用示例。配置和账本通过 Host 持久存储；无数据库凭据、无外部依赖。许可证 AGPL-3.0-only。实现可做离线检查，未声明真服运行结果。

## 并发控制与恢复

post/transfer/hold/release **必须**提交 `expected_revision`，对应源 account 的当前 revision；新账户为 0。transfer 另可提交 `target_expected_revision`，需要同时保护目标账户状态时使用。版本不符拒绝整笔操作，不部分转账。先读取 balance，再保存含 revision 的完整请求；重试必须使用同一 request_id 和完全相同请求，不能重新查询后改 revision 重用旧 request_id。并发冲突后需由业务决定新操作，并用新 request_id。

启动检查持久 `economy:schema_version=1`；空安装初始化为 1，未知版本不迁移、不覆盖。每次事务提交前在安装数据目录 fsync 保存 commit_id 恢复标记。CommitUncertain 或取消保留标记；后续写入和 maintenance seal 必须先查 Host storage.receipt，未确认则返回 commit_unresolved/busy，不重放。标记跨重启保留。确认已提交后清除标记继续；receipt 未找到仍为未知，需要运维核对，绝不当作未执行自动重放。经济业务 receipt 和 Host commit receipt 为不同层次。
