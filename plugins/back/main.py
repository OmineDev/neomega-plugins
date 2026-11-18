"""死亡点返回：后台采样在线玩家坐标并监听死亡消息，死亡后可发送 !返回 传送回死亡地点。

插件维护一份在线真人玩家缓存（启动时读取 players 与 robot 排除机器人自身，并由
player.list_added / player.list_removed 持续更新），后台按配置间隔采样玩家脚下坐标作为
短时回退；同时订阅入站 Text 死亡翻译包（TextType=2, NeedsTranslation, Message 以
death. 开头）。

收到死亡通知后优先即时查询死者位置；若查询失败、位置为空或坐标落入虚空（y < -60 且未开启
允许返回虚空），则回退使用最近 10 秒内采样的有效存活位置。有效死亡点通过独立存储事务写入
death:<player_id> 并可选私聊提醒。玩家发送 !返回 时在单次事件事务中校验记录、有效期、
单次限制、虚空限制与冷却，扣费（如配置且已安装经济插件）后标记已使用、更新冷却、下发跨维度
tp 命令并私聊回复。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import math
import time

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, PacketSubscriptionError,
                             Plugin, PluginContext, PlayerNotFound, ProtocolError,
                             ServiceRejected, player_target)
from neomega_runtime.managed import IPCError, IPCRejected
from neomega_runtime.protocol import StringBytes, packets
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout

DIMENSIONS = {0: 'overworld', 1: 'nether', 2: 'the_end'}
DIMENSION_NAMES = {0: '主世界', 1: '下界', 2: '末地'}
# 位置查询（querytarget）报的是玩家眼睛的高度，脚下比它低 1.62 格。
EYE_HEIGHT = 1.62
VOID_Y = -60.0
FALLBACK_MAX_AGE_SECONDS = 10.0
ECONOMY_PAY = 'plugin.neomega.economy.pay'
# 结果不明的扣款可以用同一个 request_id 安全重试；明确的业务拒绝不在其中。
UNSETTLED = (ServiceWaitTimeout, ServiceUncertain, ServiceFailed)
CHARGE_UNSURE = '扣款结果暂时不确定，这次没有传送。可以用 !流水 查看，没有扣款可以再试一次。'


@dataclass(frozen=True)
class Settings:
    sample_interval_seconds: int = field(default=2, metadata={
        'title': '位置采样间隔（秒）',
        'description': '后台定时记录在线玩家有效位置的时间间隔',
        'minimum': 1, 'maximum': 10})
    expire_seconds: int = field(default=600, metadata={
        'title': '死亡记录有效期（秒）',
        'description': '死亡后多少秒内可以发送 !返回；填 0 表示不过期',
        'minimum': 0})
    one_time: bool = field(default=True, metadata={
        'title': '单次有效',
        'description': '开启后每次死亡记录只能成功返回一次；关闭则在有效期内可多次返回'})
    cooldown_seconds: int = field(default=30, metadata={
        'title': '返回冷却（秒）',
        'description': '两次使用 !返回 之间至少间隔的秒数，填 0 不限制',
        'minimum': 0})
    cost: int = field(default=0, metadata={
        'title': '每次返回花费金币',
        'description': '需要安装经济插件，没装时不收费；填 0 免费',
        'minimum': 0})
    notify_on_death: bool = field(default=True, metadata={
        'title': '死亡后私聊提醒',
        'description': '记录到有效死亡点后私聊提示玩家发送 !返回 回到死亡地点'})
    allow_void_return: bool = field(default=False, metadata={
        'title': '允许返回虚空（y < -60）',
        'description': '关闭时不记录 y < -60 的虚空死亡点，也会阻止传送到此类位置'})


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def death_key(pid: str) -> str:
    return 'death:' + pid


def cooldown_key(pid: str) -> str:
    return 'cd:' + pid


def decode_text(value) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, StringBytes):
        return value.value.decode('utf-8', 'replace')
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode('utf-8', 'replace')
    return str(value)


def coordinate(value) -> str:
    text = f'{float(value):.3f}'.rstrip('0').rstrip('.')
    return '0' if text == '-0' else text


def feet_pos(entity: dict) -> dict | None:
    """将位置查询实体转为脚下坐标与维度；无效坐标返回 None。"""
    dimension = entity.get('dimension')
    position = entity.get('position')
    if type(dimension) is not int or dimension not in DIMENSIONS or not isinstance(position, dict):
        return None
    try:
        x = round(float(position['x']), 3)
        y = round(float(position['y']) - EYE_HEIGHT, 3)
        z = round(float(position['z']), 3)
    except (KeyError, TypeError, ValueError):
        return None
    if not (math.isfinite(x) and math.isfinite(y) and math.isfinite(z)):
        return None
    return {'dimension': dimension, 'x': x, 'y': y, 'z': z}


def place_text(place: dict) -> str:
    return DIMENSION_NAMES[place['dimension']] + ' ' + ' '.join(coordinate(place[axis]) for axis in 'xyz')


def format_duration(seconds: int) -> str:
    minutes, secs = divmod(max(0, int(seconds)), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        if secs:
            return f'{hours} 小时 {minutes} 分 {secs} 秒'
        if minutes:
            return f'{hours} 小时 {minutes} 分钟'
        return f'{hours} 小时'
    if minutes:
        return f'{minutes} 分 {secs} 秒' if secs else f'{minutes} 分钟'
    return f'{secs} 秒'


def charge_request_id(event_id: str) -> str:
    """同一个事件得到同一个 request_id，不同事件不同。"""
    return 'back-' + hashlib.sha256(event_id.encode('utf-8')).hexdigest()[:48]


class Back(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.bot_name: str | None = None
        self.online: dict[str, dict] = {}
        self.online_synced = False
        self.last_pos: dict[str, dict] = {}
        self.routers = ()

    async def on_start(self, ctx: PluginContext):
        self.bot_name = None
        self.online = {}
        self.online_synced = False
        self.last_pos = {}
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('返回', aliases=('back', 'fh'), description='传送回上次死亡地点', max_args=0)(
                lambda command: self.go_back(ctx, command))
            router.command('死亡点', aliases=('deathpos',), description='查看上次死亡位置与剩余有效期', max_args=0)(
                lambda command: self.show_death(ctx, command))
        await self.refresh_bot_name(ctx)
        await self.sync_players(ctx)
        ctx.spawn(self.sample_loop(ctx), name='position-sampler')
        ctx.spawn(self.death_loop(ctx), name='death-listener')

    async def on_event(self, ctx: PluginContext, event: Event):
        if event.kind == 'player.list_added':
            await self.on_player_added(ctx, event)
            return
        if event.kind == 'player.list_removed':
            await self.on_player_removed(ctx, event)
            return
        if event.kind == 'chat.received':
            try:
                for router in self.routers:
                    if await router.dispatch(event):
                        return
            except (ValueError, PlayerNotFound, AmbiguousPlayer):
                # 引号不配对、参数个数不对，或玩家已经下线：不回复，只确认事件。
                pass
        async with self.lock:
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    def clock(self) -> float:
        # 死亡记录有效期和冷却需要跨重启保留，使用墙上时间。
        return time.time()

    # ---- 在线玩家缓存 ----

    async def refresh_bot_name(self, ctx: PluginContext) -> str | None:
        try:
            name = (await ctx.robot()).get('bot_name')
        except (IPCError, ProtocolError, ConnectionError, TimeoutError, OSError) as error:
            ctx.log.warning('robot status unavailable: %s', type(error).__name__)
            return self.bot_name
        if name:
            self.bot_name = name
            for key in [k for k, entry in self.online.items() if entry['name'] == name]:
                removed = self.online.pop(key)
                self.last_pos.pop(removed['player_id'], None)
        return self.bot_name

    async def sync_players(self, ctx: PluginContext):
        try:
            roster = await ctx.players()
        except (IPCError, ProtocolError, ConnectionError, TimeoutError, OSError) as error:
            ctx.log.warning('initial player list unavailable: %s', type(error).__name__)
            return
        for player in roster:
            name = player.get('name', '')
            if not name or (self.bot_name and name == self.bot_name):
                continue
            uuid = player.get('uuid', '')
            xuid = player.get('xuid', '')
            if not uuid and not xuid:
                continue
            pid = player_id({'uuid': uuid, 'xuid': xuid})
            key = uuid or pid
            existing = self.online.get(key)
            self.online[key] = {
                'name': name,
                'uuid': uuid,
                'xuid': xuid or (existing.get('xuid', '') if existing else ''),
                'player_id': pid,
                'entity_unique_id': existing.get('entity_unique_id') if existing else None,
            }
        self.online_synced = True

    async def on_player_added(self, ctx: PluginContext, event: Event):
        if self.bot_name is None:
            await self.refresh_bot_name(ctx)
        async with self.lock:
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()
        body = event.payload.get('payload') or {}
        name = body.get('name')
        uuid = body.get('uuid') or ''
        xuid = body.get('xuid') or ''
        if not isinstance(name, str) or not name or (self.bot_name and name == self.bot_name):
            return
        if not uuid and not xuid:
            return
        pid = player_id({'uuid': uuid, 'xuid': xuid})
        key = uuid or pid
        existing = self.online.get(key, {})
        euid = body.get('entity_unique_id')
        if euid is None:
            euid = existing.get('entity_unique_id')
        self.online[key] = {
            'name': name,
            'uuid': uuid,
            'xuid': xuid or existing.get('xuid', ''),
            'player_id': pid,
            'entity_unique_id': euid,
        }

    async def on_player_removed(self, ctx: PluginContext, event: Event):
        async with self.lock:
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()
        body = event.payload.get('payload') or {}
        uuid = body.get('uuid') or ''
        if uuid:
            removed = self.online.pop(uuid, None)
            if removed is not None:
                ctx.log.debug('player left: %s (%s)', removed['name'], uuid)
                self.last_pos.pop(removed['player_id'], None)

    def find_online_by_name(self, name: str) -> dict | None:
        for entry in self.online.values():
            if entry['name'] == name:
                return entry
        return None

    def match_online_entity(self, entity: dict) -> dict | None:
        uid = str(entity.get('uniqueId', ''))
        if not uid:
            return None
        if uid in self.online:
            return self.online[uid]
        uid_cf = uid.casefold()
        eid = entity.get('id')
        for entry in self.online.values():
            if entry['uuid'] and entry['uuid'].casefold() == uid_cf:
                return entry
            euid = entry.get('entity_unique_id')
            if euid is not None and (str(euid) == uid or (type(eid) is int and eid != 0 and eid == euid)):
                return entry
        return None

    # ---- 后台采样与死亡监听 ----

    async def sample_loop(self, ctx: PluginContext):
        while True:
            try:
                await self.sample_once(ctx)
            except asyncio.CancelledError:
                raise
            except (IPCError, ProtocolError, TimeoutError, ConnectionError, OSError) as error:
                ctx.log.warning('position sampling failed: %s', error)
            await asyncio.sleep(ctx.config.sample_interval_seconds)

    async def sample_once(self, ctx: PluginContext):
        if self.bot_name is None:
            await self.refresh_bot_name(ctx)
        if not self.online_synced:
            await self.sync_players(ctx)
        if not self.online:
            return
        snapshot = await ctx.positions.sample(target='players', limit=64)
        now = self.clock()
        for entity in snapshot.get('entities', []):
            player = self.match_online_entity(entity)
            if player is None:
                continue
            pos = feet_pos(entity)
            if pos is None:
                continue
            if pos['y'] < VOID_Y and not ctx.config.allow_void_return:
                continue
            self.last_pos[player['player_id']] = {**pos, 'at': now}

    async def death_loop(self, ctx: PluginContext):
        while True:
            try:
                async with ctx.packets.typed.subscribe([packets.Text]) as stream:
                    async for event in stream:
                        await self.handle_death_packet(ctx, event.model)
                return
            except asyncio.CancelledError:
                raise
            except PacketSubscriptionError as error:
                if error.reason in ('permission_revoked', 'closed'):
                    ctx.log.info('death packet subscription ended: %s', error.reason)
                    return
                ctx.log.warning('death packet subscription interrupted (%s), retrying in 2s', error.reason)
            except IPCRejected as error:
                if error.code in ('permission_denied', 'permission_revoked'):
                    ctx.log.warning('death packet subscription unauthorized (%s)', error.code)
                    return
                ctx.log.warning('death packet subscription rejected (%s), retrying in 2s', error.code)
            except (IPCError, ProtocolError, TimeoutError, ConnectionError, OSError) as error:
                ctx.log.warning('death packet subscription failed (%s), retrying in 2s', error)
            await asyncio.sleep(2)

    def parse_death_victim(self, model) -> dict | None:
        if getattr(model, 'TextType', None) != 2 or not getattr(model, 'NeedsTranslation', False):
            return None
        message = decode_text(getattr(model, 'Message', ''))
        if not message.startswith('death.'):
            return None
        params = getattr(model, 'Parameters', None)
        if not params:
            return None
        victim_name = decode_text(params[0])
        if not victim_name or (self.bot_name and victim_name == self.bot_name):
            return None
        return self.find_online_by_name(victim_name)

    async def handle_death_packet(self, ctx: PluginContext, model):
        player = self.parse_death_victim(model)
        if player is None:
            return
        try:
            await self.record_death(ctx, player)
        except asyncio.CancelledError:
            raise
        except (IPCError, ProtocolError, TimeoutError, ConnectionError, OSError) as error:
            ctx.log.warning('failed to record death for %s: %s', player['name'], error)

    async def query_player_pos(self, ctx: PluginContext, name: str) -> tuple[dict | None, bool]:
        """即时读取玩家脚下坐标；返回 (有效坐标或 None, 是否因虚空被拒)。"""
        try:
            sample = await ctx.positions.sample(target='players', name=name, limit=1)
        except (IPCError, ProtocolError, TimeoutError, ConnectionError, OSError):
            return None, False
        entities = sample.get('entities') or []
        if not entities:
            return None, False
        pos = feet_pos(entities[0])
        if pos is None:
            return None, False
        if pos['y'] < VOID_Y and not ctx.config.allow_void_return:
            return None, True
        return pos, False

    async def record_death(self, ctx: PluginContext, player: dict):
        pid = player['player_id']
        live_pos, saw_void = await self.query_player_pos(ctx, player['name'])
        now = self.clock()
        chosen = live_pos
        if chosen is None:
            cached = self.last_pos.get(pid)
            if cached is not None and 0 <= now - cached['at'] <= FALLBACK_MAX_AGE_SECONDS:
                if cached['y'] >= VOID_Y or ctx.config.allow_void_return:
                    chosen = {
                        'dimension': cached['dimension'],
                        'x': cached['x'],
                        'y': cached['y'],
                        'z': cached['z'],
                    }
        if chosen is None:
            if ctx.config.notify_on_death:
                text = ('你在虚空中死亡，未能记录可返回的坐标。' if saw_void
                        else '暂时未能读取到你的死亡位置，本次未记录返回点。')
                async with self.lock:
                    tx = await ctx.storage.transaction(event=None, keys=[])
                    if self.attach_reply(ctx, tx, player, text, tag='death'):
                        await tx.save()
            return

        record = {**chosen, 'at': now, 'used': False}
        key = death_key(pid)
        async with self.lock:
            tx = await ctx.storage.transaction(event=None, keys=[key])
            tx.set(key, record)
            if ctx.config.notify_on_death:
                self.attach_reply(ctx, tx, player,
                                  f'已记录你的死亡位置（{place_text(record)}），发送 !返回 可回到死亡地点。',
                                  tag='death')
            await tx.save()

    # ---- 玩家命令 ----

    async def go_back(self, ctx: PluginContext, command):
        player = command.player
        pid = player_id(player)
        d_key, c_key = death_key(pid), cooldown_key(pid)
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=[d_key, c_key])
            record = tx.get(d_key)
            cd_record = tx.get(c_key)
            now = self.clock()
            config = ctx.config

            if record is None:
                self.attach_reply(ctx, tx, player, '没有找到你的死亡记录。')
                return await tx.save()
            if not isinstance(record, dict) or record.get('dimension') not in DIMENSIONS:
                self.attach_reply(ctx, tx, player, '死亡位置所在维度不支持返回。')
                return await tx.save()
            if record.get('used') and config.one_time:
                self.attach_reply(ctx, tx, player, '上次死亡点的返回机会已经用过了。')
                return await tx.save()
            if config.expire_seconds > 0 and now - record['at'] > config.expire_seconds:
                self.attach_reply(ctx, tx, player, '上次的死亡记录已经过期了。')
                return await tx.save()
            if record['y'] < VOID_Y and not config.allow_void_return:
                self.attach_reply(ctx, tx, player, '上次死亡位置在虚空中，不允许返回。')
                return await tx.save()

            if config.cooldown_seconds > 0 and isinstance(cd_record, dict):
                last_cd = cd_record.get('at', -math.inf)
                wait = min(last_cd + config.cooldown_seconds - now, config.cooldown_seconds)
                if wait > 0:
                    self.attach_reply(ctx, tx, player, f'返回还在冷却中，请 {math.ceil(wait)} 秒后再试。')
                    return await tx.save()

            target = player_target(player['name'])
            paid, note = await self.charge(ctx, command)
            if not paid:
                self.attach_reply(ctx, tx, player, note)
                return await tx.save()

            tx.set(d_key, {**record, 'used': True})
            tx.set(c_key, {'at': now})
            coordinates = ' '.join(coordinate(record[axis]) for axis in 'xyz')
            tx.action(ctx.commands.prepare(
                f'execute in {DIMENSIONS[record["dimension"]]} run tp {target} {coordinates}',
                idempotency_key=tx.commit_id + '_tp', deadline=deadline()))
            self.attach_reply(ctx, tx, player, f'已传送回死亡地点（{place_text(record)}）。' + note)
            await tx.save()

    async def show_death(self, ctx: PluginContext, command):
        player = command.player
        pid = player_id(player)
        d_key = death_key(pid)
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=[d_key])
            record = tx.get(d_key)
            now = self.clock()
            config = ctx.config

            if not isinstance(record, dict) or record.get('dimension') not in DIMENSIONS:
                text = '没有找到你的死亡记录。'
            else:
                where = place_text(record)
                reasons = []
                validity = '长期有效（不过期）'
                if record.get('used') and config.one_time:
                    reasons.append('已使用')
                if config.expire_seconds > 0:
                    remaining = record['at'] + config.expire_seconds - now
                    if remaining <= 0:
                        reasons.append('已过期')
                    else:
                        validity = f'剩余有效期 {format_duration(math.ceil(remaining))}'
                if record['y'] < VOID_Y and not config.allow_void_return:
                    reasons.append('虚空位置不可返回')
                if reasons:
                    text = f'最近死亡位置：{where}（{"，".join(reasons)}）。'
                else:
                    text = f'最近死亡位置：{where}，{validity}。发送 !返回 即可回到该位置。'
            self.attach_reply(ctx, tx, player, text)
            await tx.save()

    async def charge(self, ctx: PluginContext, command) -> tuple[bool, str]:
        """扣返回费，返回 (可以传送, 附加说明)。结果不明时用同一个 request_id 再试一次。"""
        cost = ctx.config.cost
        if not cost:
            return True, ''
        arguments = {
            'player_id': player_id(command.player),
            'amount': cost,
            'reason': '返回死亡点',
            'request_id': charge_request_id(command.event.payload['event_id']),
        }
        for retry in (False, True):
            try:
                result = await ctx.services.call(ECONOMY_PAY, arguments, timeout=5)
                break
            except IPCRejected:
                return True, ''  # 没有安装或没有启动经济插件：不收费。
            except ServiceRejected as error:
                if error.code == 'insufficient_funds':
                    return False, f'金币不足，返回一次需要 {cost} 金币。'
                if error.code != 'unavailable':
                    return False, '扣款失败，这次没有传送。'
                if retry:
                    return False, CHARGE_UNSURE
            except UNSETTLED:
                if retry:
                    return False, CHARGE_UNSURE
        coin = result.get('currency', '金币')  # 经济插件 2.0.0 不返回货币名
        return True, f'花费 {cost} {coin}，剩余 {result["balance"]} {coin}。'

    def attach_reply(self, ctx: PluginContext, tx, player: dict, text: str, tag: str = 'reply') -> bool:
        try:
            plan = ctx.notifications.prepare(
                player['name'], text,
                idempotency_key=f'{tx.commit_id}_{tag}', deadline=deadline())
        except ValueError:
            return False
        for part in plan.parts:
            tx.action(part.intent)
        return True


if __name__ == '__main__':
    Back().run()
