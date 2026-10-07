# 通用纯计算库

提供安全具名文本模板、Minecraft 消息样式/原始文本、大字映射、物品名称表、三维向量与区域交集、朝向选项和原始操作回执解释。无隐式网络、线程、Host 动作或运行依赖。

```python
from neomega_common import template, styled, big_text, Vec3, Region
message = styled('success', template('欢迎 {name}', {'name': 'Alex'}))
label = big_text('MENU 1')
zone = Region(Vec3(0, 0, 0), Vec3(10, 100, 10))
assert zone.contains(Vec3(1, 64, 1))
```

`big_text` 默认使用全角 ASCII，并非伪称内置游戏大字字形；可传资源包实际支持的 mapping，未知字符原样保留。物品未知名字保留命名空间 ID，不编造本地化。区域边界含端点，不同维度无交集。`operation_result` 保留 unknown/失败/受理等原始状态。
