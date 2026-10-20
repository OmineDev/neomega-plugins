# neomega_menu

在插件进程里运行的聊天栏编号菜单，基于 SDK 的 `ctx.chat_waiters`。随消费者 ZIP 打包，服主不需要额外安装任何插件。

```python
import neomega_menu as menu

async def on_event(self, ctx, event):
    async with self.lock:
        if await menu.feed(ctx, event):      # 菜单回复：已确认事件
            return
        ...                                  # 其余事件照常处理

async def shop(self, ctx, player):           # 用 ctx.spawn(self.shop(ctx, player)) 启动
    index = await menu.choose(ctx, player, '商店', ['面包 ×16（10 金币）', '钻石（100 金币）'])
    if index is not None and await menu.confirm(ctx, player, '确定购买？'):
        async with self.lock:
            ...                              # 扣款、发货，自己的存储提交
```

| 函数 | 返回 |
| --- | --- |
| `feed(ctx, event)` | 这条聊天是菜单回复时确认事件并返回 True |
| `say(ctx, player, text)` | 私聊一句话 |
| `ask(ctx, player, question)` | 玩家的下一句话；超时或输入 0 / 取消时返回 None |
| `choose(ctx, player, title, options)` | 选中项从 0 开始的序号，或 None；输错 3 次关闭 |
| `confirm(ctx, player, question)` | 回复 1 / 是 / 确认时返回 True |

- 菜单要等待之后的聊天事件，所以必须在 `ctx.spawn` 里运行，不能直接在 `on_event` 里等待。
- `feed` 会提交一次存储事务来确认事件。插件自己的提交和它共用同一个全局 revision，所以 `feed` 和菜单流程中的提交要持有同一把锁。
- 同一玩家同时只能有一个菜单；提示消息不进存储，插件重启后打开的菜单自动关闭。
