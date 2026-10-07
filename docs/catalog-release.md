# 发布固定版本审核目录

所有社区作者都可通过 PR 收录；精选只能引用既有 `plugin_id`，插件不依赖 Polaris 账号。目录由维护者审核，Host 权限由服主另外授予。先完成插件独立测试、确定审核提交并构建固定 ZIP；同版本不能替换字节。不要从变动中的源码重打已审定版本。

```sh
python3 tools/catalog-release.py \
  --package dist/fatalder.cloud-import-1.0.1.zip \
  --plugin fatalder.cloud-import --version 1.0.1 --name 'Fatalder 云导入' \
  --index catalog/index.json \
  --source-ref <审核提交的完整40位SHA> --source-path plugins/fatalder-cloud-import
```

工具核对包名和 manifest 身份/版本，计算 SHA-256，从 ZIP 内 manifest 复制 `permissions`、`subscriptions`、runtime、Host API、Worker 协议、依赖和存在的平台/最低 Host 版本字段作为发现元数据。`--source-ref` 与 `--source-path` 必须成对提供，生成固定来源记录；本地旧式调用可省略，正式 workflow 始终提供。未声明的最低 Host 版本不猜测填写。可用 `--permission-purposes /path/to/reviewed-purposes.json` 添加权限用途字符串映射；该说明不改变包内声明或 Host 授权。工具只准备本地索引，不创建 Release，不自动收录目录中的其他 ZIP。已有相同条目可重复运行，不同内容必须增加版本。

先将原始 ZIP 上传至 `OmineDev/neomega-plugins` 的 `<plugin_id>-v<version>` GitHub Release，工件名 `<plugin_id>-<version>.zip`。验证公开工件与索引哈希一致后，通过审核提交 `catalog/index.json`。客户端读取固定官方索引，不接受任意第三方目录。镜像根地址下保持 `<plugin_id>-v<version>/<plugin_id>-<version>.zip`，字节和哈希完全相同。

更新目录只提供发现，新版本不会自动安装或启动。实际安装继续经过 neomega-agent 的 ZIP 私有文件限制和 Host 校验；离线安装和自动测试不证明实服业务成功。

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p test_catalog_release.py
```

通用投稿与手动发布入口见 [CONTRIBUTING](../CONTRIBUTING.md)。`tools/prepare-release.py <目录>` 仅在本地准备未发布版本的 ZIP、校验文件和候选索引；`.github/workflows/release.yml` 在审核合入默认分支后由维护者手动触发，发布及下载校验后创建索引 PR。此流程不自动批准或合入 PR。

## 合并并发目录变化

准备候选后若默认分支目录已有新增条目，只合并候选中指定的 ID/版本，保留当前目录其他内容：

```sh
python3 tools/catalog-release.py --merge-index dist/release/index.json \
  --plugin example.demo --version 1.0.0 --index catalog/index.json
```

相同 ID/版本仅允许完全相同条目；不同内容仍拒绝。此命令不重打包、不替换 Release，也不会从候选快照覆盖其他条目。正式 workflow 在下载核对 Release 后取最新默认分支运行它；PR 合并时仍须处理之后发生的并发变更。若 Release 已存在而 PR 创建失败，只恢复目录步骤，保留原 ZIP 与摘要。

## 插件简介与安装前文档

发行元数据 `release.json.description` 是不超过 2000 字的卡片简介。省略时从同一制品的 `README.md` 首个非标题说明行提取；作者应提供明确摘要。索引不改变严格运行 manifest：新增 `description` 与 `readme: {path, sha256, size}`。README 为 UTF-8，最大 256 KiB；摘要、路径和长度从实际 ZIP 生成，版本与 ZIP SHA-256 一同固定。Polaris 在安装前获取并核对 ZIP 后读取 README，不执行插件、不从任意文档 URL 加载内容。旧目录可继续从包内 README 回退读取。

## 前置生态完整制品

六个能力库位于 `libraries/<import_package>/`，目录本身就是导入包。每个库提交 `library.json`，字段为 `name`、`version`、`license`、明确文件列表 `files` 和库名数组 `dependencies`；文件列表必须含完整 `LICENSE`。运行时由 Host 提供的 `neomega_runtime` 不随包复制。插件 `release.json.libraries` 声明需要的库；库依赖拓扑自动展开，循环依赖拒绝。

```sh
python3 tools/lock-libraries.py plugins/<目录>
python3 tools/package-plugin.py plugins/<目录> --output dist/<ID>-<版本>.zip
python3 tools/bundle-ecosystem.py --output dist/ecosystem
```

锁文件记录每个库的版本、许可证、文件 SHA-256 和 `py3-none-any` ABI。打包时严格比较当前源码与锁，不静默更新；库直接随消费者 ZIP 交付，并附 `library.lock.json`、完整许可证及 `THIRD_PARTY_NOTICES.txt`。插件文件与库文件冲突拒绝。未使用库的既有包保持旧文件顺序和字节构建方式。

全量构建输出每个插件 ZIP、六个独立版本化库 ZIP、`SHA256SUMS`、`compatibility.json` 和 `bundle.json`，包括全部源码插件，允许 `--plugin <目录>` 重复指定有限集合用于诊断。`bundle.json.publication_state=local-only`，使用本地 `artifact` 文件名而非伪造 Release URL；它是交付库存，不能直接当作远端市场索引。正式发布仍使用 `prepare-release.py` 与原不可覆盖 Release 流程，实际上传成功且下载字节一致后才收录。

`compatibility.json` 来自 manifest 的 Host API、Worker 协议、平台、最低 Host 版本及 provider 依赖，库清单来自发行声明，provider 导出 API 名称和 major 来自 manifest.exports。未声明字段保持缺省，不把理论兼容写成实测结果。没有真服或人工验收门，源码检查与已有自动检查可作为本地交付证据；安装和启动继续保留既有安全权限语义。

### 第三方 Python 依赖

当前六库优先标准库，不要求额外运行时 pip。确需第三方库时，先提交精确版本与 SHA-256 的 requirements 锁和审核过的 SPDX 许可证映射，在独立构建目录下载匹配目标的 wheel：

```sh
python3 tools/vendor-wheels.py --requirements requirements.lock \
  --license-map licenses.json --output dist/vendor \
  --platform any --python-version 311 --abi none
```

工具使用 `pip download --require-hashes --only-binary=:all:`，所有间接依赖也须明确列入锁文件；拒绝缺许可证、模块覆盖、路径穿越、符号链接和需要 `.data` 重定位的包，不改全局或运行 Host 的 Python 环境。原生 wheel 必须显式指定 pip 目标平台/Python/ABI，并在插件 manifest 限定相应 OS/架构与运行环境；不同目标构建不同版本制品，不能把原生二进制混入通用库。将 staging 中所需文件按插件白名单纳入 ZIP，保留 wheel 锁和许可证。此工具不支持源码发行包编译，不猜测系统库兼容性。

### Host 服务接口 schema 的闭集

`manifest.exports` 中输入/输出 schema 使用 Host 协议的严格子集，不等同于通用 JSON Schema。允许字段仅为 `type`、`properties`、`required`、`additionalProperties`、`items`、`enum`、`pattern`、`minimum`、`maximum`；`type` 必须是一个字符串，`additionalProperties` 仅为布尔值，数组必须指定单个元素 schema，最大深度为 32。`pattern` 使用 Go 正则。`minLength`、`maxLength`、`minItems`、`maxItems`、联合类型、`anyOf`、`$schema`、`description`、`default` 不在此接口合同内。

不能在 schema 表达的长度、数量、跨字段约束保留在 provider 输入校验中；不要为了通过制品检查删除真实业务校验。可空可选字段优先表达为不在 `required` 中的字段，而不是联合类型。Host API `min_minor` 必须来自真正使用的能力及目标框架常量，不能照抄 SDK 包版本或市场里其他制品的值。

完整构建可追加 `--checker /path/to/neomega-plugin-check`，将当前目标 Host 的逐包真实回执写入 `compatibility.json`，包括失败原因；不会用 SDK 版本替代 Host 判断，也不会为通过检查改写既有发布包。`bundle.json` 保留每项 ZIP 字节数、manifest 摘要及存在时的 library lock 摘要；`SHA256SUMS` 只记录真正生成的制品。历史包与本次修改包分别解释检查结果。

若验证使用源码 SDK，再追加 `--sdk-source /path/to/sdk/python`，兼容矩阵记录 SDK Python 源码内容摘要和 Git revision，不写入本机路径。源码没有独立包版本时 `version` 保持 `null`，不能用 Host API minor 冒充 SDK 发行版本；未指定源码时保持未知。

### Host 与 SDK 的同套交付

指定 `--sdk-source` 时，工具同时生成版本由 Git revision 与内容摘要固定的 SDK ZIP，包含 `neomega_runtime` 的 Python、类型及协议资源、README、`source.lock.json`。不把私有 Host 全仓源码混入 SDK。已存在的许可证通过 `--sdk-license` 纳入；上游未声明许可则保留 `NOTICE.txt` 与 `license_status: not_declared`，不虚构开源授权。

Host 二进制由框架正常构建后用 `--host-binary <binary> --host-target linux-amd64 --host-revision <commit>` 纳入同套制品；`target` 必须是该二进制实际的 GOOS/GOARCH。工具复制二进制并记录摘要，不进行交叉编译，也不自动公开发布。Host/SDK 都记入 `bundle.json` 的 `components` 和同一个 `SHA256SUMS`。

SDK ZIP 使用时先解压，再把解压根目录加入 `PYTHONPATH`。当前协议模型读取相邻 `schema.json`，不支持把未解压的 SDK ZIP 直接加入 Python 导入路径；纯能力库 ZIP 的导入方式不代表 SDK 有相同装载合同。

已提供真实构建过的 MySQL 可选依赖锁：`docs/examples/mysql/requirements.lock`（PyMySQL 1.2.3）及 `licenses.json`（MIT）。MySQL 消费者使用上述命令替换这两个参数，把生成的 `pymysql/`、对应 `.dist-info/`、`wheels.lock.json` 复制到消费者插件根目录，并逐文件加入该插件 `release.json.files`。保留锁中全部文件及许可证；打包工具逐字节核对展开文件 SHA，避免锁与 ZIP 不一致。默认 SQLite 消费者不携带这个可选驱动，所有路线均不在运行时执行 pip。
