# neomega-plugins

OmineDev 审核维护的 neomega 业务插件。社区通过 Pull Request 贡献；维护者审核测试与权限用途后发布，各插件独立版本。Host、SDK 和安装管理由 [neomega-agent](https://github.com/OmineDev/neomega-agent) 维护，Fatalder Worker 与通用 API 保留在 Fatalder。

| 插件 | ID | 源码与安装说明 |
| --- | --- | --- |
| 测试积分兑换示例 | `example.points-exchange` | [plugins/points-exchange](plugins/points-exchange/README.md) |
| 入门服务（欢迎 / 帮助 / 公告） | `example.starter-service` | [plugins/starter-service](plugins/starter-service/README.md) |
| Fatalder 云导入 | `fatalder.cloud-import` | [plugins/fatalder-cloud-import](plugins/fatalder-cloud-import/README.md) |

从本仓库 Releases 获取固定版本 ZIP 与 SHA256SUMS，经校验后通过现有 `runtime-admin.py install` / `update` 安装；版本不会自动升级。镜像必须提供相同版本与相同 SHA-256。公开制品不包含服务器配置、密钥或运行数据。

维护与发布见 [CONTRIBUTING.md](CONTRIBUTING.md)，迁移来源及验证见 [docs/migration.md](docs/migration.md)。许可沿用来源代码 AGPL-3.0，见 [LICENSE](LICENSE)。
