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
