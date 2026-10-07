# 社区贡献与发布

任何作者均可通过 PR 投稿；插件不需要 Polaris 账号，也不以精选资格作为收录条件。维护者审核合入后，才可从默认分支手动发布。目录收录与服主授予 Host 权限是两件事。Polaris 精选只引用本目录已有的 `plugin_id`，不另建插件身份或发行包。

## 投稿

1. 新建 `plugins/<目录>/`，提交稳定 ID 的 `manifest.json`、静态 `config.schema.json`、入口源码、README 和 LICENSE。版本使用独立语义版本，状态版本仅随数据合同变化。不要改旧插件安装名或删除任务回执。
2. 添加 `release.json`：`name` 为展示名，`files` 为逐项明确的包内相对路径数组，不支持通配符。参考现有插件；真实配置、凭据、日志、数据库、缓存与建筑文件不得入包。所有申请权限和订阅必须在 manifest 的 `permissions.purposes` 中解释用途；兼容既有制品时可在 `release.json.permission_purposes` 提供相同用途映射，避免改变原包字节。
3. 添加业务测试到插件的 `tests/`，运行下列命令。PR 说明权限变化、许可证来源、迁移影响以及已完成的验证。维护者审核源码、白名单和包内容后合入；自动检查不替代人工审核。

```sh
PYTHONPATH="$NEOMEGA_TOOLKIT/sdk/python" python3 tools/check-plugins.py
python3 -m unittest discover -s tests
python3 tools/package-plugin.py plugins/<目录> --output /tmp/review.zip
```

本地 `NEOMEGA_TOOLKIT` 指向与目标 Host 匹配的公开工具包解包目录。CI 匿名下载 [1.1.0 Linux amd64 工具包](https://wiki.wuxie233.com/downloads/neomega-toolkit-1.1.0-linux-amd64.tar.gz)，固定 SHA-256 `1865efc93815c031cbe8f6a2cd353683868c0837f97c43dbd42c63a2495140ed`，再验证包内 `FILES.sha256`。SDK API 为 1.10，Worker Wire 为 1.0；无需读取私有框架仓库或配置 SDK 凭据。运行各插件 `tests/` 与已有 `tools/check-offline.py`，发布时另用随包原生 checker 校验 ZIP。SDK 仅作开发依赖，不放入插件 ZIP。

通用打包器只校验社区提交约定，不替代 Host 完整 manifest/schema 校验。社区制品上限为 32 MiB；Polaris 托管安装当前上限为 5 MiB，超过该大小的包只能在支持对应大小的独立 Host/CLI 中安装。ZIP 的文件顺序取自白名单，时间和权限固定；同一源和打包运行时可复现。既有 Fatalder 白名单保持旧打包器字节不变。Host 离线检查仍可使用 `tools/check-host.py`；离线安装不证明实服业务运行成功。

Host 配置 schema 使用 SDK 支持的 JSON Schema 子集，不能直接采用通用校验器支持的全部关键字。优先从完整类型声明的配置 dataclass 生成 schema，并用目标 SDK 的 `check_config_schema` 和 `load_config` 核对静态 schema、默认值与示例；例如列表必须声明元素类型。不能由静态 schema 表达的业务约束保留在配置加载校验中。ZIP checker 通过仍须在目标 Host 核验配置描述和校验入口；它不证明配置可以加载或游戏逻辑已经运行。

## 社区服务插件的共用源码

在线时间、签到、玩家互传、定时命令和个人传送点各自独立安装；它们包内的 `community_support.py` 是 `tools/community_support.py` 的相同字节副本，不是插件间运行依赖。只修改 canonical 文件，再运行 `python3 tools/sync-community-support.py --write` 更新副本。`tools/check-plugins.py` 会检查副本一致性；发行白名单仍须明确包含该文件，不得把工具包 SDK 一同打包。

支持层只负责身份观察、目标编码和原操作回执读取，不写业务账本、不确认事件、不提交世界动作。各插件负责自己的事务、配置和生命周期；修复共用源码时必须检查全部使用者，已发行制品仍遵守增版本和不可替换规则。

## 审核后发布

1. 维护者在默认分支运行 **Publish reviewed plugin**，输入已经审核合入的插件目录名。工作流绑定启动时的提交 SHA，运行社区检查和测试，从实际 ZIP 提取运行时、API、依赖和订阅元数据，以启动 SHA 记录源码来源，生成 ZIP、`SHA256SUMS` 与候选索引，并保存 Actions artifact。
2. 工作流拒绝已入目录版本及已有 tag，创建 `<plugin_id>-v<version>` Release；下载原 ZIP 核对字节后，重读默认分支并仅合并本次固定版本条目，再提交目录候选 PR。包名固定为 `<plugin_id>-<version>.zip`，SHA-256 固定。修复必须增版本，禁止替换原包；镜像只能复制相同字节。
3. 维护者审核目录 PR、Release 来源和下载哈希，合入 `catalog/index.json` 后客户端才可发现并安装。目录保留所有历史版本；新版本不会自动安装或启动。

仓库管理员应设置默认分支 PR 审核/检查保护，并允许 Actions 创建 PR；工作流不会更改 GitHub 设置，默认分支限制本身不能证明有人审核过。如果当前设置禁止机器人建 PR，Release 可以已成功而目录 PR 未创建：从 artifact 取 `index.json` 与当前目录核对、保留并发新增条目后人工提交 PR。不要盲目重跑、删除标签或覆盖 Release。任何失败都先核对 tag、Release、制品哈希及候选 PR 的实际状态。

工作流使用仓库 `GITHUB_TOKEN`，不需要新增凭据。发布地址沿用 `OmineDev/neomega-plugins`；分叉仓若自行发布，须显式调整目录 Release 根地址并让消费者选择对应来源，不能冒充官方来源。具体索引合同见 [目录发布说明](docs/catalog-release.md)。

## 前置库与市场介绍

新插件应在 `release.json.description` 给出简短介绍，并在包内 README 说明功能、调用接口、配置、权限和失败语义。安装前文档由索引中的 ZIP/README 哈希共同绑定，不把用户可见介绍塞入严格运行 manifest。前置库随消费者 ZIP 携带；库锁、兼容矩阵和完整本地套件构建见 [目录发布说明](docs/catalog-release.md#前置生态完整制品)。开发任务若明确禁止新增测试，复用现有检查和直接演练即可，无需新增测试文件；本地交付不依赖真人或真服验收。
