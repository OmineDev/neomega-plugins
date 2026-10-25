# neomega-plugins

OmineDev 审核维护的 neomega 业务插件。社区通过 Pull Request 贡献；维护者审核测试与权限用途后发布，各插件独立版本。Host、SDK 和安装管理由 [neomega-agent](https://github.com/OmineDev/neomega-agent) 维护，Fatalder Worker 与通用 API 保留在 Fatalder。

| 插件 | ID | 源码与安装说明 |
| --- | --- | --- |
| 每日签到 | `neomega.daily-signin` | [plugins/daily-signin](plugins/daily-signin/README.md) |
| 在线时长 | `neomega.online-time` | [plugins/online-time](plugins/online-time/README.md) |
| 个人传送点 | `neomega.homes` | [plugins/homes](plugins/homes/README.md) |
| 玩家互传 | `neomega.tpa` | [plugins/tpa](plugins/tpa/README.md) |
| 传送点与主城 | `neomega.warps` | [plugins/warps](plugins/warps/README.md) |
| 定时命令 | `neomega.timed-commands` | [plugins/timed-commands](plugins/timed-commands/README.md) |
| 欢迎与公告 | `neomega.welcome` | [plugins/welcome](plugins/welcome/README.md) |
| Fatalder 云导入 | `fatalder.cloud-import` | [plugins/fatalder-cloud-import](plugins/fatalder-cloud-import/README.md) |

玩法插件各自独立，不依赖其他插件。写新插件时以 [plugins/daily-signin](plugins/daily-signin/) 为范本：一次处理只做一次存储提交，同时写入状态、游戏命令、回复和事件确认。`plugins/` 下其余 `neomega.*` 前置服务正在重新设计，暂不收录到目录。

从本仓库 Releases 获取固定版本 ZIP 与 SHA256SUMS，经校验后通过现有 `runtime-admin.py install` / `update` 安装；版本不会自动升级。镜像必须提供相同版本与相同 SHA-256。公开制品不包含服务器配置、密钥或运行数据。

维护与发布见 [CONTRIBUTING.md](CONTRIBUTING.md)，迁移来源及验证见 [docs/migration.md](docs/migration.md)。每个插件和库的许可证见其目录内 LICENSE：Fatalder 云导入沿用来源代码的 AGPL-3.0，随包库为 MIT。

## 完整前置生态

前置服务与 Python 能力库统一通过明确权限、版本依赖与固定 ZIP 分发。库随消费者打包，不要求服主运行 pip；市场摘要与 README 由实际制品生成，安装前即可阅读。全套构建与调用方库锁约定见 [制品与目录说明](docs/catalog-release.md#前置生态完整制品)。

```sh
python3 tools/bundle-ecosystem.py --output dist/ecosystem
```

输出本地完整制品、SHA-256 清单和兼容矩阵，不自动发布远端，也不覆盖历史 Release。
