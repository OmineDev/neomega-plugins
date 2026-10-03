# 发布固定版本审核目录

所有社区作者都可通过 PR 收录；精选只能引用既有 `plugin_id`，插件不依赖 Polaris 账号。目录由维护者审核，Host 权限由服主另外授予。先完成插件独立测试、确定审核提交并构建固定 ZIP；同版本不能替换字节。不要从变动中的源码重打已审定版本。

```sh
python3 tools/catalog-release.py \
  --package dist/fatalder.cloud-import-1.0.1.zip \
  --plugin fatalder.cloud-import --version 1.0.1 --name 'Fatalder 云导入' \
  --index catalog/index.json
```

工具核对包名和 manifest 身份/版本，计算 SHA-256，复制 `permissions` 作为预览。可用 `--permission-purposes /path/to/reviewed-purposes.json` 添加权限用途字符串映射；该说明不改变包内声明或 Host 授权。工具只准备本地索引，不创建 Release，不自动收录目录中的其他 ZIP。已有相同条目可重复运行，不同内容必须增加版本。

先将原始 ZIP 上传至 `OmineDev/neomega-plugins` 的 `<plugin_id>-v<version>` GitHub Release，工件名 `<plugin_id>-<version>.zip`。验证公开工件与索引哈希一致后，通过审核提交 `catalog/index.json`。客户端默认读取固定官方索引；显式第三方源使用 source_id/index_url/release_root 三元绑定，不改变官方源身份。镜像根地址下保持 `<plugin_id>-v<version>/<plugin_id>-<version>.zip`，字节和哈希完全相同。

更新目录只提供发现，新版本不会自动安装或启动。实际安装继续经过 neomega-agent 的 ZIP 私有文件限制和 Host 校验；离线安装和自动测试不证明实服业务成功。

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p test_catalog_release.py
```

通用投稿与手动发布入口见 [CONTRIBUTING](../CONTRIBUTING.md)。`tools/prepare-release.py <目录>` 仅在本地准备未发布版本的 ZIP、校验文件和候选索引；`.github/workflows/release.yml` 在审核合入默认分支后由维护者手动触发，发布及下载校验后创建索引 PR。此流程不自动批准或合入 PR。

## 固定源码与兼容信息

新精选发行使用 `--source-ref <完整源码提交 SHA> --source-path plugins/<目录> --min-host-version 1.0.0`。`source-ref` 必须是已公开且包含该插件源码及依赖的提交；来源说明不替代审核或包摘要。`--description` 可补充浏览简介。

带源码来源的条目从包内 manifest 复制 `runtime`、`host_api`、`worker_protocol`、`dependencies` 及存在的 `target_os` / `target_arch`。`min_host_version` 表达发行版最低要求，`host_api` 表达接口最低要求；安装仍由 Host 校验。历史条目不修改、不补写推测信息。CLI 下载时将这些已声明字段与包内 manifest 逐项比对。

`prepare-release.py <目录> --source-ref <SHA> --min-host-version 1.0.0 --output <目录>` 生成相同元数据。先固定源码提交，再构建制品和候选目录；上传制品并核对摘要后才发布索引。源码提交与目录提交可不同，Release 必须指向实际制品源码提交，不能使用尚无新源码的默认分支作为来源。

Go Worker 每个版本包只含一个目标平台。目录保持原有 `(plugin_id, version)` 唯一键，平台变体使用明确的 `-linux-amd64` 等身份后缀；只安装当前 Host 对应的一项，不同时启用提供同名非共享服务的不同平台变体。公开源码包含独立 Go SDK 和本地模块引用，构建不需要私有 Host 仓库。
