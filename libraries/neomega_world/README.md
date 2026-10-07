# neomega_world 1.0.0

无 I/O 的世界数据模型库。`Region(origin,size)` 校验 int32 坐标、正尺寸并计算体积；`tiles()` 生成不超过 4096 方块的区域；`overlaps()` 检查覆盖冲突。`canonical/digest` 为 JSON NBT 模型提供确定性编码及 SHA-256；原始二进制 NBT 不在此猜测解码。

`container_slots(slots,double_chest=True)` 统一双箱 0..53 槽，前 27 槽为第一箱。`differences(before,after)` 比较按坐标键索引的观测；缺失键明确为 unknown，而非空气。`snapshot(region,cells,dimension,epoch,losses)` 生成带哈希和保真损失清单的 v1 快照，`verify_snapshot()` 校验格式及摘要。

本库不建立连接、不缓存世界数据、不写游戏、不提供服务客户端。消费者通过 release.json `libraries:["neomega_world"]` 固定打包；P06 客户端属于 neomega_clients。

`normalize_block(entry)` 提取原始结构块的 foreground、raw_entity_data、background、coverage、missing 和支持的 nbt 写入形式。基础容器 Items 和命令块配置有明确可恢复字段；未知 NBT 不伪造近似格式。`region_snapshot(dump, dimension=..., epoch=...)` 检查完整格子与重复位置后生成含空气的摘要快照。`restore_difference(saved, current)` 只比较完整覆盖字段。库保持纯数据转换，不做网络或文件操作。
