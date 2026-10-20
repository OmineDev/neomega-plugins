"""个人传送点：玩家用 !设家 记下当前位置，之后用 !回家 传送回去。

每个玩家一条记录，保存他所有的家。一次命令只做一次存储提交：记录修改、
传送命令、回复和事件确认一起写入，要么全部生效，要么全部不生效。
设家需要先读玩家位置，这一步在打开存储之前完成，读不到就只回复、只确认。
回家冷却只记在内存里，重启后清空。
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import math
import time

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ProtocolError, player_target)
from neomega_runtime.managed import IPCRejected

DEFAULT_HOME = '家'
MAX_NAME_LENGTH = 16
DIMENSIONS = {0: 'overworld', 1: 'nether', 2: 'the_end'}


@dataclass(frozen=True)
class Settings:
    max_homes: int = field(default=3, metadata={
        'title': '每人最多几个家', 'minimum': 1, 'maximum': 64})
    cooldown_seconds: int = field(default=10, metadata={
        'title': '回家冷却（秒）', 'description': '两次回家之间至少间隔的秒数，填 0 不限制', 'minimum': 0})


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_key(player) -> str:
    return 'player:' + (player['uuid'] or 'xuid-' + player['xuid'])


def home_list(homes) -> str:
    return '、'.join(f'「{name}」' for name in homes)


class Homes(Plugin):
    config_type = Settings

    async def on_start(self, ctx: PluginContext):
        self.last_home = {}  # 玩家记录键 -> 上次回家的时刻
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('设家', aliases=('sethome',), description='把当前位置记为家', max_args=1)(
                lambda command: self.set_home(ctx, command))
            router.command('回家', aliases=('home', 'hj'), description='传送到记下的家', max_args=1)(
                lambda command: self.go_home(ctx, command))
            router.command('删家', aliases=('delhome',), description='删除一个家', min_args=1, max_args=1)(
                lambda command: self.delete_home(ctx, command))
            router.command('家列表', aliases=('homes',), description='查看自己的家', max_args=0)(
                lambda command: self.list_homes(ctx, command))

    async def on_event(self, ctx: PluginContext, event: Event):
        if event.kind == 'chat.received':
            try:
                for router in self.routers:
                    if await router.dispatch(event):
                        return
            except (ValueError, PlayerNotFound, AmbiguousPlayer):
                # 引号不配对、参数个数不对，或玩家已经下线：不回复，只确认事件。
                pass
        await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    def clock(self) -> float:
        return time.monotonic()

    async def set_home(self, ctx, command):
        player, key = command.player, player_key(command.player)
        name = command.args[0] if command.args else DEFAULT_HOME
        if not 1 <= len(name) <= MAX_NAME_LENGTH:
            return await self.answer(ctx, command, f'家的名字最长 {MAX_NAME_LENGTH} 个字。')
        try:
            sample = await ctx.positions.sample(target='players', name=player['name'], limit=1)
        except (ProtocolError, TimeoutError, IPCRejected):
            sample = {}
        if not sample.get('entities'):
            return await self.answer(ctx, command, '暂时读不到你的位置，请稍后再试。')
        entity = sample['entities'][0]
        if entity['dimension'] not in DIMENSIONS:
            return await self.answer(ctx, command, '你所在的维度不能设家。')

        tx = await ctx.storage.transaction(event=command.event.payload, keys=[key])
        homes = tx.get(key, {})
        if name not in homes and len(homes) >= ctx.config.max_homes:
            self.reply(ctx, tx, player, f'你已经有 {len(homes)} 个家（上限 {ctx.config.max_homes} 个）：'
                                        f'{home_list(homes)}。发送 !删家 <名字> 删掉一个再设。')
            return await tx.save()
        replaced = name in homes
        homes[name] = {'dimension': entity['dimension'],
                       **{axis: round(float(entity['position'][axis]), 1) for axis in 'xyz'}}
        tx.set(key, homes)
        self.reply(ctx, tx, player, f'已更新「{name}」的位置。' if replaced else f'已设置家「{name}」。')
        await tx.save()

    async def go_home(self, ctx, command):
        player, key = command.player, player_key(command.player)
        name = command.args[0] if command.args else DEFAULT_HOME
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[key])
        homes = tx.get(key, {})
        home = homes.get(name)
        now = self.clock()
        wait = self.last_home.get(key, -math.inf) + ctx.config.cooldown_seconds - now
        if home is None:
            text = (f'你还没有叫「{name}」的家，已有的家：{home_list(homes)}。' if homes
                    else '你还没有设家，发送 !设家 把当前位置记为家。')
        elif wait > 0:
            text = f'回家还在冷却中，请 {math.ceil(wait)} 秒后再试。'
        else:
            target = player_target(player['name'])
            where = f'{home["x"]} {home["y"]} {home["z"]}'
            tx.action(ctx.commands.prepare(
                f'execute in {DIMENSIONS[home["dimension"]]} run tp {target} {where}',
                idempotency_key=tx.commit_id + '_tp', deadline=deadline()))
            text = f'已传送到「{name}」。'
        self.reply(ctx, tx, player, text)
        await tx.save()
        if home is not None and wait <= 0:
            self.last_home[key] = now

    async def delete_home(self, ctx, command):
        player, key = command.player, player_key(command.player)
        name = command.args[0]
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[key])
        homes = tx.get(key, {})
        if name not in homes:
            text = (f'没有叫「{name}」的家，已有的家：{home_list(homes)}。' if homes else '你还没有设家。')
        else:
            del homes[name]
            if homes:
                tx.set(key, homes)
            else:
                tx.delete(key)
            text = f'已删除家「{name}」。'
        self.reply(ctx, tx, player, text)
        await tx.save()

    async def list_homes(self, ctx, command):
        player, key = command.player, player_key(command.player)
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[key])
        homes = tx.get(key, {})
        text = (f'你的家（{len(homes)}/{ctx.config.max_homes}）：{home_list(homes)}。' if homes
                else '你还没有设家，发送 !设家 把当前位置记为家。')
        self.reply(ctx, tx, player, text)
        await tx.save()

    async def answer(self, ctx, command, text):
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
        self.reply(ctx, tx, command.player, text)
        await tx.save()

    def reply(self, ctx, tx, player, text):
        plan = ctx.notifications.prepare(player['name'], text,
                                         idempotency_key=tx.commit_id + '_reply', deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)


if __name__ == '__main__':
    Homes().run()
