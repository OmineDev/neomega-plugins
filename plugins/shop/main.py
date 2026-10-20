"""积分商店：玩家发送 !商店，在聊天菜单里挑选商品，确认后扣金币并发货。

金币由经济插件管理。每次购买生成一个随机 request_id 交给经济插件扣款；等待
超时或结果不明时用同一个 request_id 再扣一次，经济插件按它去重，不会扣两次。
扣款成功后，发货命令和回复在一次存储提交里交给 Host 执行。菜单等待玩家回复
期间不持锁，只有存储提交持锁，和 on_event 的确认共用一个全局 revision。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import uuid

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ServiceRejected, player_target)
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from neomega_runtime.storage import CommitUncertain
import neomega_menu as menu

ECONOMY = 'plugin.neomega.economy.'
# 结果不明的扣款可以用同一个 request_id 安全重试；明确的业务拒绝不在其中。
UNSETTLED = (ServiceWaitTimeout, ServiceUncertain, ServiceFailed)


@dataclass(frozen=True)
class Item:
    name: str = field(metadata={'title': '商品名称'})
    price: int = field(metadata={'title': '价格', 'description': '需要的金币数量', 'minimum': 1})
    commands: list[str] = field(metadata={
        'title': '发货命令', 'description': '{player} 会替换为购买的玩家'})


@dataclass(frozen=True)
class Settings:
    title: str = field(default='积分商店', metadata={'title': '商店标题'})
    items: list[Item] = field(default_factory=lambda: [
        Item('面包 ×16', 10, ['give {player} bread 16']),
        Item('钻石', 100, ['give {player} diamond 1']),
    ], metadata={'title': '商品', 'description': '至少一件，名称不能重复'})

    def __post_init__(self):
        if not self.items:
            raise ValueError('shop needs at least one item')
        if len({item.name for item in self.items}) != len(self.items):
            raise ValueError('item names must be unique')
        if any(not command.strip() for item in self.items for command in item.commands):
            raise ValueError('delivery commands must not be empty')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


class Shop(Plugin):
    config_type = Settings

    async def on_start(self, ctx: PluginContext):
        self.lock = asyncio.Lock()
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('商店', aliases=('sd', 'shop'), description='打开积分商店')(
                lambda command: self.open(ctx, command))

    async def on_event(self, ctx: PluginContext, event: Event):
        async with self.lock:
            try:
                # 菜单回复由 feed 确认事件。
                if await menu.feed(ctx, event):
                    return
                if event.kind == 'chat.received':
                    for router in self.routers:
                        if await router.dispatch(event):
                            return
            except (ValueError, PlayerNotFound, AmbiguousPlayer):
                # 引号不配对，或玩家已经下线：不回复，只确认事件。
                pass
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    async def open(self, ctx, command):
        await (await ctx.storage.transaction(event=command.event.payload, keys=[])).save()
        ctx.spawn(self.browse(ctx, command.player))

    async def browse(self, ctx, player):
        items = ctx.config.items
        index = await menu.choose(ctx, player, ctx.config.title,
                                  [f'{item.name} — {item.price} 金币' for item in items])
        if index is None:
            return
        item = items[index]
        if not await menu.confirm(ctx, player, f'购买「{item.name}」需要 {item.price} 金币，确认吗？'):
            return

        account = player_id(player)
        arguments = {'player_id': account, 'amount': item.price,
                     'reason': ('购买 ' + item.name)[:32], 'request_id': uuid.uuid4().hex}
        try:
            try:
                result = await ctx.services.call(ECONOMY + 'pay', arguments, timeout=5)
            except UNSETTLED:
                result = await ctx.services.call(ECONOMY + 'pay', arguments, timeout=5)
        except ServiceRejected as error:
            if error.code != 'insufficient_funds':
                raise
            await menu.say(ctx, player, await self.short_of_funds(ctx, account, item.price))
            return
        except UNSETTLED:
            await menu.say(ctx, player, '扣款结果暂时不确定，请用 !流水 查看；没有扣款可以重新购买。')
            return
        # duplicate=True 表示这笔扣款之前已经成功，同样发货。
        await self.deliver(ctx, player, item, result['balance'], arguments['request_id'])

    async def short_of_funds(self, ctx, account, price):
        try:
            balance = (await ctx.services.call(ECONOMY + 'balance', {'player_id': account},
                                               timeout=5))['balance']
        except (ServiceRejected, *UNSETTLED):
            return '金币不足。'
        return f'金币不足，需要 {price} 金币，你现在有 {balance} 金币。'

    async def deliver(self, ctx, player, item, balance, request_id):
        target = player_target(player['name'])
        async with self.lock:
            tx = await ctx.storage.transaction(keys=[])
            for index, line in enumerate(item.commands):
                tx.action(ctx.commands.prepare(line.strip().removeprefix('/').replace('{player}', target),
                                               idempotency_key=f'{tx.commit_id}_item_{index}',
                                               deadline=deadline()))
            plan = ctx.notifications.prepare(player['name'], f'购买成功，剩余 {balance} 金币。',
                                             idempotency_key=tx.commit_id + '_reply', deadline=deadline())
            for part in plan.parts:
                tx.action(part.intent)
            try:
                await tx.save()
                return
            except CommitUncertain:
                # 已经扣款，不退款；日志里的 request_id 对应经济插件流水，供管理员核对。
                ctx.log.warning('shop delivery uncertain player=%s item=%s request_id=%s',
                                player['name'], item.name, request_id)
        await menu.say(ctx, player, '发货结果不确定，请联系管理员。')


if __name__ == '__main__':
    Shop().run()
