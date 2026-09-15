# Fatalder 插件迁移

来源：Fatalder 提交 `1b3d3849fbaf82c7a7cd05ce958846f142e0b50f` 的 `integrations/neomega-cloud-import`。源码、静态 schema、51 个测试、默认值快照与打包入口迁至 `plugins/fatalder-cloud-import`。版本从 1.0.0 增至 1.0.1 以区分独立制品，业务 Python 文件、schema 和默认值未改变。保留 AGPL-3.0 许可。

保持 manifest ID `fatalder.cloud-import`、state_version 1、配置字段和 `fatalder.controller` 状态键。Host 安装名称决定 `managed:<name>` 身份和稳定数据目录；使用相同名称更新，禁止卸载重装。旧插件有在途任务、unknown 回执或权限租约时拒绝手工迁移，先在原版本核对并结束。

Worker 身份/HTTP API、前端产品合同与共享核心不随迁移删除。后续保留 OP 完成策略由独立任务改变，不在源码迁移中悄悄修复行为。

自动验证范围：插件单元测试、ZIP 白名单/可复现检查、临时离线 Host 的公开安装与配置入口。没有进行游戏连接、真实 OP 授权、报价或世界写入；完整玩家侧与内容保真验收仍须另行记录。
