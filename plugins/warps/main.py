"""传送点与主城：公共传送点、主城和设重生点，全部通过聊天命令使用。

传送点有两个来源：服主在插件设置里预设的（只能在设置里改），以及管理员在游戏里
用 !设传送点 记下的（存储键 warps，按创建顺序排列）。主城优先用管理员在游戏里
设的位置（存储键 city），没有时用设置里的坐标。每个玩家一条记录，保存上次传送和
上次设重生点的时刻（UNIX 秒），所以冷却在插件重启后照样有效。

一次命令只做一次存储提交：记录修改、游戏命令、回复和事件确认一起写入，要么全部
生效，要么全部不生效。读玩家位置在打开存储之前完成，读不到就只回复、只确认。

传送收费交给可选的经济插件，在提交前扣款。request_id 由事件决定，事件重新投递时
经济插件按它去重，不会重复扣款。没装经济插件时不收费。
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import math
import time

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ProtocolError, ServiceRejected, player_target)
from neomega_runtime.managed import IPCRejected
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout

WARPS, CITY = 'warps', 'city'
DIMENSIONS = {0: 'overworld', 1: 'nether', 2: 'the_end'}
DIMENSION_NAMES = {0: '主世界', 1: '下界', 2: '末地'}
MAX_NAME_LENGTH = 16
MAX_DESCRIPTION_LENGTH = 32
MAX_WARPS = 64
# 位置查询（querytarget）报的是玩家眼睛的高度，脚下比它低 1.62 格。
EYE_HEIGHT = 1.62
ECONOMY_PAY = 'plugin.neomega.economy.pay'
# 结果不明的扣款可以用同一个 request_id 安全重试；明确的业务拒绝不在其中。
UNSETTLED = (ServiceWaitTimeout, ServiceUncertain, ServiceFailed)
CHARGE_UNSURE = '扣款结果暂时不确定，这次没有传送。可以用 !流水 查看，没有扣款可以再试一次。'


@dataclass(frozen=True, kw_only=True)
class Warp:
    name: str = field(metadata={'title': '名称', 'description': f'最长 {MAX_NAME_LENGTH} 个字，不能是纯数字'})
    dimension: int = field(default=0, metadata={
        'title': '维度', 'description': '0 主世界，1 下界，2 末地', 'minimum': 0, 'maximum': 2})
    x: float = field(metadata={'title': 'X 坐标'})
    y: float = field(metadata={'title': 'Y 坐标'})
    z: float = field(metadata={'title': 'Z 坐标'})
    description: str = field(default='', metadata={
        'title': '说明', 'description': f'显示在传送点列表里，可以不填，最长 {MAX_DESCRIPTION_LENGTH} 个字'})


@dataclass(frozen=True)
class Settings:
    admins: list[str] = field(default_factory=list, metadata={
        'title': '管理员', 'description': '可以在游戏里设置、删除传送点和主城的玩家名字'})
    warps: list[Warp] = field(default_factory=list, metadata={
        'title': '公共传送点', 'description': f'名称不能重复，最多 {MAX_WARPS} 个；管理员也可以在游戏里用 !设传送点 添加'})
    teleport_cooldown_seconds: int = field(default=10, metadata={
        'title': '传送冷却（秒）', 'description': '两次传送（传送点和主城）之间至少间隔的秒数，填 0 不限制',
        'minimum': 0})
    teleport_cost: int = field(default=0, metadata={
        'title': '每次传送花费金币', 'description': '需要安装经济插件，没装时不收费；填 0 免费', 'minimum': 0})
    city_enabled: bool = field(default=False, metadata={
        'title': '启用设置里的主城', 'description': '开启后 !主城 传送到下面的坐标；管理员在游戏里用 !设主城 设的位置优先'})
    city_dimension: int = field(default=0, metadata={
        'title': '主城维度', 'description': '0 主世界，1 下界，2 末地', 'minimum': 0, 'maximum': 2})
    city_x: float = field(default=0.0, metadata={'title': '主城 X 坐标'})
    city_y: float = field(default=64.0, metadata={'title': '主城 Y 坐标'})
    city_z: float = field(default=0.0, metadata={'title': '主城 Z 坐标'})
    spawnpoint_enabled: bool = field(default=True, metadata={
        'title': '允许玩家设重生点', 'description': '开启后玩家可以用 !设重生点 把重生点设在当前位置'})
    spawnpoint_cooldown_seconds: int = field(default=60, metadata={
        'title': '设重生点冷却（秒）', 'description': '两次设重生点之间至少间隔的秒数，填 0 不限制', 'minimum': 0})

    def __post_init__(self):
        if any(not name.strip() for name in self.admins):
            raise ValueError('admin names must not be empty')
        if len(self.warps) > MAX_WARPS:
            raise ValueError('too many warps')
        if any(not valid_name(warp.name) or len(warp.description) > MAX_DESCRIPTION_LENGTH
               or warp.dimension not in DIMENSIONS for warp in self.warps):
            raise ValueError('invalid warp')
        if len({warp.name.casefold() for warp in self.warps}) != len(self.warps):
            raise ValueError('warp names must be unique')
        if self.city_dimension not in DIMENSIONS:
            raise ValueError('invalid city dimension')


def valid_name(name: str) -> bool:
    # 纯数字留给编号，首尾空格和控制字符容易看错。
    return (1 <= len(name) <= MAX_NAME_LENGTH and not name.isdigit()
            and name.isprintable() and name.strip() == name)


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_key(player) -> str:
    return 'player:' + (player['uuid'] or 'xuid-' + player['xuid'])


def coordinate(value) -> str:
    text = f'{float(value):.2f}'.rstrip('0').rstrip('.')
    return '0' if text == '-0' else text


def feet(entity) -> dict:
    """把位置查询结果换成脚的坐标；y 先去掉眼睛高度，再消掉浮点误差。"""
    position = entity['position']
    return {'x': float(position['x']), 'y': round(float(position['y']) - EYE_HEIGHT, 3),
            'z': float(position['z'])}


def charge_request_id(event_id: str) -> str:
    """同一个事件得到同一个 request_id，不同事件不同。

    event_id 形如 game:<64 位会话号>:<序号>，直接截到 64 字会把序号截掉，同一次
    会话里的所有传送就共用一个 request_id，经济插件会当成重复扣款而不再扣钱。
    """
    return 'warp-' + hashlib.sha256(event_id.encode('utf-8')).hexdigest()[:48]


def place_text(place) -> str:
    return DIMENSION_NAMES[place['dimension']] + ' ' + ' '.join(coordinate(place[axis]) for axis in 'xyz')


def find(warps, query):
    """按编号（从 1 开始）或不区分大小写的名称找传送点。"""
    if query.isdigit():
        index = int(query) - 1
        return warps[index] if 0 <= index < len(warps) else None
    return next((warp for warp in warps if warp[0].casefold() == query.casefold()), None)


class Warps(Plugin):
    config_type = Settings

    async def on_start(self, ctx: PluginContext):
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('传送点', aliases=('warps', 'csd'), description='查看公共传送点', max_args=0)(
                lambda command: self.list_warps(ctx, command))
            router.command('传送', aliases=('warp', 'cs'), description='前往公共传送点',
                           usage='<名称或编号>', max_args=1)(
                lambda command: self.go_warp(ctx, command))
            router.command('设传送点', aliases=('setwarp',), description='把当前位置设为公共传送点（管理员）',
                           usage='<名称> [说明]', min_args=1, max_args=2)(
                lambda command: self.set_warp(ctx, command))
            router.command('删传送点', aliases=('delwarp',), description='删除公共传送点（管理员）',
                           usage='<名称或编号>', min_args=1, max_args=1)(
                lambda command: self.delete_warp(ctx, command))
            router.command('主城', aliases=('spawn', 'zc'), description='传送到主城', max_args=0)(
                lambda command: self.go_city(ctx, command))
            router.command('设主城', aliases=('setcity',), description='把当前位置设为主城（管理员）', max_args=0)(
                lambda command: self.set_city(ctx, command))
            router.command('删主城', aliases=('delcity',), description='清除游戏里设的主城（管理员）', max_args=0)(
                lambda command: self.delete_city(ctx, command))
            router.command('设重生点', aliases=('setspawnpoint',), description='把重生点设在当前位置', max_args=0)(
                lambda command: self.set_spawnpoint(ctx, command))

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
        # 冷却跨重启保存，所以用墙上时间而不是 monotonic。
        return time.time()

    def all_warps(self, ctx, tx):
        """[(名称, 位置, 是否设置里预设)]：预设在前，游戏里设的按创建顺序在后。"""
        warps = [(warp.name, {'dimension': warp.dimension, 'x': warp.x, 'y': warp.y, 'z': warp.z,
                              'description': warp.description}, True) for warp in ctx.config.warps]
        taken = {name.casefold() for name, _, _ in warps}
        return warps + [(name, place, False) for name, place in tx.get(WARPS, {}).items()
                        if name.casefold() not in taken]

    def city(self, ctx, tx):
        if tx.get(CITY) is not None:
            return tx.get(CITY)
        if ctx.config.city_enabled:
            return {'dimension': ctx.config.city_dimension,
                    'x': ctx.config.city_x, 'y': ctx.config.city_y, 'z': ctx.config.city_z}
        return None

    def is_admin(self, ctx, player) -> bool:
        return player['name'].casefold() in {name.strip().casefold() for name in ctx.config.admins}

    # ---- 玩家命令 ----

    async def list_warps(self, ctx, command):
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[WARPS])
        warps = self.all_warps(ctx, tx)
        if not warps:
            text = '还没有公共传送点。'
        else:
            lines = ['公共传送点：']
            for number, (name, place, _) in enumerate(warps, 1):
                note = f'：{place["description"]}' if place.get('description') else ''
                lines.append(f'{number}. {name}（{DIMENSION_NAMES[place["dimension"]]}）{note}')
            lines.append('发送 !传送 <编号或名称> 前往。')
            text = '\n'.join(lines)
        self.reply(ctx, tx, command.player, text)
        await tx.save()

    async def go_warp(self, ctx, command):
        if not command.args:
            return await self.list_warps(ctx, command)
        query = command.args[0]
        tx = await ctx.storage.transaction(event=command.event.payload,
                                           keys=[WARPS, player_key(command.player)])
        found = find(self.all_warps(ctx, tx), query)
        if found is None:
            self.reply(ctx, tx, command.player, f'没有找到传送点「{query}」，发送 !传送点 查看全部。')
            return await tx.save()
        await self.teleport(ctx, tx, command, found[1], found[0], f'「{found[0]}」')

    async def go_city(self, ctx, command):
        tx = await ctx.storage.transaction(event=command.event.payload,
                                           keys=[CITY, player_key(command.player)])
        place = self.city(ctx, tx)
        if place is None:
            self.reply(ctx, tx, command.player, '服务器还没有设置主城。')
            return await tx.save()
        await self.teleport(ctx, tx, command, place, '主城', '主城')

    async def teleport(self, ctx, tx, command, place, label, where):
        player, key = command.player, player_key(command.player)
        record = tx.get(key, {})
        now, cooldown = self.clock(), ctx.config.teleport_cooldown_seconds
        # 时钟被调回时也不会比一次冷却更久。
        wait = min(record.get('teleport', -math.inf) + cooldown - now, cooldown)
        if wait > 0:
            self.reply(ctx, tx, player, f'传送还在冷却中，请 {math.ceil(wait)} 秒后再试。')
            return await tx.save()
        paid, note = await self.charge(ctx, command, label)
        if not paid:
            self.reply(ctx, tx, player, note)
            return await tx.save()
        record['teleport'] = now
        tx.set(key, record)
        coordinates = ' '.join(coordinate(place[axis]) for axis in 'xyz')
        tx.action(ctx.commands.prepare(
            f'execute in {DIMENSIONS[place["dimension"]]} run tp {player_target(player["name"])} {coordinates}',
            idempotency_key=tx.commit_id + '_tp', deadline=deadline()))
        self.reply(ctx, tx, player, f'已传送到{where}。' + note)
        await tx.save()

    async def charge(self, ctx, command, label):
        """扣传送费，返回 (可以传送, 附加说明)。结果不明时用同一个 request_id 再试一次。"""
        cost = ctx.config.teleport_cost
        if not cost:
            return True, ''
        arguments = {'player_id': player_key(command.player).removeprefix('player:'), 'amount': cost,
                     'reason': ('传送到' + label)[:32],
                     'request_id': charge_request_id(command.event.payload['event_id'])}
        for retry in (False, True):
            try:
                result = await ctx.services.call(ECONOMY_PAY, arguments, timeout=5)
                break
            except IPCRejected:
                return True, ''  # 没有安装或没有启动经济插件：不收费。
            except ServiceRejected as error:
                if error.code == 'insufficient_funds':
                    return False, f'金币不足，传送一次需要 {cost} 金币。'
                if error.code != 'unavailable':
                    return False, '扣款失败，这次没有传送。'
                if retry:
                    return False, CHARGE_UNSURE
            except UNSETTLED:
                if retry:
                    return False, CHARGE_UNSURE
        coin = result.get('currency', '金币')  # 经济插件 2.0.0 不返回货币名
        return True, f'花费 {cost} {coin}，剩余 {result["balance"]} {coin}。'

    async def set_spawnpoint(self, ctx, command):
        if not ctx.config.spawnpoint_enabled:
            return await self.answer(ctx, command, '这个服务器没有开放 !设重生点。')
        entity = await self.locate(ctx, command)
        if entity is None:
            return
        player, key = command.player, player_key(command.player)
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[key])
        record = tx.get(key, {})
        now, cooldown = self.clock(), ctx.config.spawnpoint_cooldown_seconds
        wait = min(record.get('spawnpoint', -math.inf) + cooldown - now, cooldown)
        if wait > 0:
            self.reply(ctx, tx, player, f'设重生点还在冷却中，请 {math.ceil(wait)} 秒后再试。')
            return await tx.save()
        block = {'dimension': entity['dimension'],
                 **{axis: math.floor(value) for axis, value in feet(entity).items()}}
        record['spawnpoint'] = now
        tx.set(key, record)
        tx.action(ctx.commands.prepare(
            f'execute in {DIMENSIONS[block["dimension"]]} run spawnpoint {player_target(player["name"])} '
            f'{block["x"]} {block["y"]} {block["z"]}',
            idempotency_key=tx.commit_id + '_spawnpoint', deadline=deadline()))
        self.reply(ctx, tx, player, f'已把重生点设在这里（{place_text(block)}）。')
        await tx.save()

    # ---- 管理员命令 ----

    async def set_warp(self, ctx, command):
        if not self.is_admin(ctx, command.player):
            return await self.answer(ctx, command, '只有管理员可以设置传送点。')
        name = command.args[0]
        if not valid_name(name):
            return await self.answer(ctx, command, f'传送点名称最长 {MAX_NAME_LENGTH} 个字，不能是纯数字。')
        if len(command.args) > 1 and len(command.args[1]) > MAX_DESCRIPTION_LENGTH:
            return await self.answer(ctx, command, f'说明最长 {MAX_DESCRIPTION_LENGTH} 个字。')
        preset = next((warp.name for warp in ctx.config.warps if warp.name.casefold() == name.casefold()), None)
        if preset is not None:
            return await self.answer(ctx, command, f'「{preset}」是插件设置里的传送点，请在设置里修改。')
        entity = await self.locate(ctx, command)
        if entity is None:
            return
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[WARPS])
        stored = tx.get(WARPS, {})
        existing = next((key for key in stored if key.casefold() == name.casefold()), None)
        if existing is None and len(ctx.config.warps) + len(stored) >= MAX_WARPS:
            self.reply(ctx, tx, command.player, f'传送点已经有 {MAX_WARPS} 个了，先用 !删传送点 删掉一些。')
            return await tx.save()
        description = command.args[1] if len(command.args) > 1 else \
            stored.get(existing, {}).get('description', '')
        place = {'dimension': entity['dimension'],
                 **{axis: round(value, 1) for axis, value in feet(entity).items()},
                 'description': description}
        stored[existing or name] = place
        tx.set(WARPS, stored)
        self.reply(ctx, tx, command.player,
                   f'已更新传送点「{existing}」（{place_text(place)}）。' if existing
                   else f'已设置传送点「{name}」（{place_text(place)}）。')
        await tx.save()

    async def delete_warp(self, ctx, command):
        if not self.is_admin(ctx, command.player):
            return await self.answer(ctx, command, '只有管理员可以删除传送点。')
        query = command.args[0]
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[WARPS])
        stored = tx.get(WARPS, {})
        # 按名称删时先找游戏里设的，被同名预设遮住的旧传送点也能删掉。
        found = None if query.isdigit() else next(
            ((key, stored[key], False) for key in stored if key.casefold() == query.casefold()), None)
        found = found or find(self.all_warps(ctx, tx), query)
        if found is None:
            text = f'没有找到传送点「{query}」。'
        elif found[2]:
            text = f'「{found[0]}」是插件设置里的传送点，请在设置里删除。'
        else:
            del stored[found[0]]
            if stored:
                tx.set(WARPS, stored)
            else:
                tx.delete(WARPS)
            text = f'已删除传送点「{found[0]}」。'
        self.reply(ctx, tx, command.player, text)
        await tx.save()

    async def set_city(self, ctx, command):
        if not self.is_admin(ctx, command.player):
            return await self.answer(ctx, command, '只有管理员可以设置主城。')
        entity = await self.locate(ctx, command)
        if entity is None:
            return
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[CITY])
        place = {'dimension': entity['dimension'],
                 **{axis: round(value, 1) for axis, value in feet(entity).items()}}
        tx.set(CITY, place)
        self.reply(ctx, tx, command.player, f'已把主城设在这里（{place_text(place)}）。')
        await tx.save()

    async def delete_city(self, ctx, command):
        if not self.is_admin(ctx, command.player):
            return await self.answer(ctx, command, '只有管理员可以清除主城。')
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[CITY])
        fallback = '现在使用插件设置里的主城坐标。' if ctx.config.city_enabled else '现在没有主城。'
        if tx.get(CITY) is None:
            text = '游戏里没有设置过主城，' + fallback
        else:
            tx.delete(CITY)
            text = '已清除游戏里设的主城，' + fallback
        self.reply(ctx, tx, command.player, text)
        await tx.save()

    # ---- 公用 ----

    async def locate(self, ctx, command):
        """读发命令玩家的位置；读不到或维度不支持时回复并确认事件，返回 None。"""
        try:
            sample = await ctx.positions.sample(target='players', name=command.player['name'], limit=1)
        except (ProtocolError, TimeoutError, IPCRejected):
            sample = {}
        if not sample.get('entities'):
            await self.answer(ctx, command, '暂时读不到你的位置，请稍后再试。')
            return None
        entity = sample['entities'][0]
        if entity['dimension'] not in DIMENSIONS:
            await self.answer(ctx, command, '你所在的维度不支持这个操作。')
            return None
        return entity

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
    Warps().run()
