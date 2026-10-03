# 社区贡献与发布

任何作者均可通过 PR 投稿；插件不需要 Polaris 账号，也不以精选资格作为收录条件。维护者审核并固定源码提交后发布；常规工作流从默认分支启动，明确授权的手工发行可使用独立开发分支的固定源码提交。目录收录与服主授予 Host 权限是两件事。Polaris 精选只引用本目录已有的 `plugin_id`，不另建插件身份或发行包。

## 投稿

1. 新建 `plugins/<目录>/`，提交稳定 ID 的 `manifest.json`、静态 `config.schema.json`、入口源码、README 和原许可证。未另授开源许可的官方工具包示例保留准确的 `NOTICE`，并在 `release.json` 设置 `license_notice: "NOTICE"`；不得默认为它改授仓库根 AGPL 或其他许可。版本使用独立语义版本，状态版本仅随数据合同变化。不要改旧插件安装名或删除任务回执。
2. 添加 `release.json`：`name` 为展示名，`files` 为逐项明确的包内相对路径数组，不支持通配符。参考现有插件；真实配置、凭据、日志、数据库、缓存与建筑文件不得入包。所有申请权限和订阅必须在 manifest 的 `permissions.purposes` 中解释用途；兼容既有制品时可在 `release.json.permission_purposes` 提供相同用途映射，避免改变原包字节。
3. 添加业务测试到插件的 `tests/`，运行下列命令。PR 说明权限变化、许可证来源、迁移影响以及已完成的验证。维护者审核源码、白名单和包内容后合入；自动检查不替代人工审核。

```sh
PYTHONPATH="$AGENT/sdk/python" python3 tools/check-plugins.py
python3 -m unittest discover -s tests
python3 tools/package-plugin.py plugins/<目录> --output /tmp/review.zip
```

本地 `AGENT` 指向匹配的 neomega-agent checkout；CI 固定 SDK 提交为 `675d7e0e52bfa854a7ab79b19c61db59d1921210`，运行各插件 `tests/` 与已有 `tools/check-offline.py`。

通用打包器只校验社区提交约定，不替代 Host 完整 manifest/schema 校验。社区制品上限为 32 MiB；Polaris 托管安装当前上限为 5 MiB，超过该大小的包只能在支持对应大小的独立 Host/CLI 中安装。ZIP 的文件顺序取自白名单，时间和权限固定；同一源和打包运行时可复现。既有 Fatalder 白名单保持旧打包器字节不变。Host 离线检查仍可使用 `tools/check-host.py`；离线安装不证明实服业务运行成功。

## 审核后发布

1. 维护者在默认分支运行 **Publish reviewed plugin**，输入已经审核合入的插件目录名。工作流绑定启动时的提交 SHA，运行社区检查和测试，生成 ZIP、`SHA256SUMS` 与候选索引，并保存 Actions artifact。
2. 工作流拒绝已入目录版本及已有 tag，创建 `<plugin_id>-v<version>` Release；下载原 ZIP 核对字节后，提交目录候选 PR。包名固定为 `<plugin_id>-<version>.zip`，SHA-256 固定。修复必须增版本，禁止替换原包；镜像只能复制相同字节。
3. 维护者审核目录 PR、Release 来源和下载哈希，合入 `catalog/index.json` 后客户端才可发现并安装。目录保留所有历史版本；新版本不会自动安装或启动。

仓库管理员应设置默认分支 PR 审核/检查保护，并允许 Actions 创建 PR；工作流不会更改 GitHub 设置，默认分支限制本身不能证明有人审核过。如果当前设置禁止机器人建 PR，Release 可以已成功而目录 PR 未创建：从 artifact 取 `index.json` 与当前目录核对、保留并发新增条目后人工提交 PR。不要盲目重跑、删除标签或覆盖 Release。任何失败都先核对 tag、Release、制品哈希及候选 PR 的实际状态。

工作流使用仓库 `GITHUB_TOKEN`，不需要新增凭据。发布地址沿用 `OmineDev/neomega-plugins`；分叉仓若自行发布，须显式调整目录 Release 根地址并让消费者选择对应来源，不能冒充官方来源。具体索引合同见 [目录发布说明](docs/catalog-release.md)。

## 明确授权的手工发行

维护者可在任务明确禁止运行测试、但授权构建和发行时，采用源码独立审核、必要构建与静态核验路线；必须如实记录未运行项，不能把跳过当作测试通过。该例外不更改、关闭工作流或绕过分支保护。执行外部写入前核对 push/tag/release 的全部触发器；`[skip ci]` 仅作用于 GitHub 支持的 push/pull_request 跳过场景，不能假定它覆盖所有事件。

先在独立开发分支固定公开源码及许可文件，再从该 SHA 构建 ZIP。Go 插件的公开源码必须包含独立 SDK 的本地依赖闭包，不能依赖私有 Host；`tools/package-go-state.py` 仅构建五平台样例，不执行生成的 Worker。Release 标签指实际源码 SHA，制品摘要核对后再将候选 `catalog/index.json` 发布至默认分支，保留所有历史版本及并发新增条目。目录主分支可只包含索引与必要公开元数据，源码链接必须指向真实的发行提交。

资产、标签和目录版本均不可覆盖；修复应递增版本。`NOTICE` 表达原有授权边界，不是新开源许可，公共仓库根许可证不替代各包明确的许可说明。
