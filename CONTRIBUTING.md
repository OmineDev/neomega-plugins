# 贡献与发布

社区提交 PR，维护者按插件变更范围审核后合入。每插件位于 `plugins/<目录>/`，manifest ID 是稳定身份，版本是独立语义版本；状态版本只在数据合同改变时调整。不得随迁移改安装名或删除任务回执。配置只发布静态 schema，禁止携带真实配置、密钥、日志、数据库或建筑文件。

```sh
PYTHONPATH=plugins/fatalder-cloud-import python3 -m unittest discover -s plugins/fatalder-cloud-import/tests
python3 -m unittest discover -s tests
python3 plugins/fatalder-cloud-import/tools/package.py dist/fatalder.cloud-import-1.0.4.zip
```

包使用源文件白名单与固定 ZIP 时间/权限。维护者检查 manifest、schema、权限变化、来源许可、测试与制品内容，确认 ID 和状态兼容性。改变权限须记录用途，审核发布不能替代服主授权。默认值按插件 README 的显式来源更新，日常测试与打包无需 Fatalder checkout。

## 发布

1. 在 PR 中更新插件版本与变更说明，通过测试并由维护者审核。
2. 从干净提交构建 ZIP；在现有 Host 的临时离线实例通过公开管理入口安装并验证配置，不使用真实凭据。
3. 生成 `SHA256SUMS`，用 `fatalder.cloud-import-v<version>` 标签关联审核提交；创建同名 GitHub Release，上传 ZIP 与校验文件。不得覆盖已发布版本的 ZIP；修复必须增版本。
4. 目录索引由 neomega-agent 的审核分发合同管理，填入固定版本、Release URL、SHA-256 和权限用途；镜像只能提供完全相同字节。发布插件包不表示目录已收录。
5. Release 说明写清迁移兼容性、自动测试及实服验收界限。禁止把离线安装说成实际导入成功。

```sh
sha256sum dist/fatalder.cloud-import-1.0.4.zip
# 在 dist 内生成仅含文件名的 SHA256SUMS，然后：
gh release create fatalder.cloud-import-v1.0.4 dist/fatalder.cloud-import-1.0.4.zip dist/SHA256SUMS --target <reviewed-commit> --notes-file <release-notes.md>
```

Host 安装测试需要已构建的离线 binary 和相同版本的 `scripts/runtime-admin.py`。插件 `session_required`，离线只验证安装与配置；不能启动成连接游戏的运行实例。

现有公开管理入口的可重复离线检查：

```sh
TMPDIR=/your/temp python3 tools/check-host.py --host /path/to/neomega-runtime --agent /path/to/neomega-agent dist/fatalder.cloud-import-1.0.4.zip
```

检查临时安装保持 stopped、静态配置校验和保存、密钥引用保留，以及无效字段未覆盖已保存配置；临时进程和状态在结束时清理。不验证真实 Worker 或游戏运行。
