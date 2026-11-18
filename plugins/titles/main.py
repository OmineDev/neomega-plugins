"""称号与喇叭：玩家购买、佩戴、卸下称号并发送全服喇叭，支持进服头衔播报与离线发收合并。

普通聊天消息绝不截获或重发，称号只在 !称号 列表、进服播报与 !喇叭 广播中展示。
每次事件只做一次存储提交：玩家数据、命令动作、回复和事件确认一起落盘。
收费购买与收费喇叭交给可选的经济插件，扣款使用由 event_id 派生的 request_id，
在结果不明时仅用同一个 request_id 重试一次，确保事件重投时不重复扣款。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
import time

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ServiceRejected)
from neomega_runtime.http import ProtocolError
from neomega_runtime.managed import IPCError, IPCRejected
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout

ECONOMY_PAY = 'plugin.neomega.economy.pay'
# 结果不明的扣款可以用同一个 request_id 安全重试；明确的业务拒绝不在其中。
UNSETTLED = (ServiceWaitTimeout, ServiceUncertain, ServiceFailed)
# 沿用守卫插件的积压判定：新装或重启后连续到达的历史事件仅确认并合并待生效称号，不刷屏播报。
CATCH_UP_GAP = 1.0
CATCH_UP_LIMIT = 60.0
MAX_HORN_LENGTH = 200
MAX_STORAGE_KEY_BYTES = 256
MAX_BROADCAST_COMMAND_BYTES = 320


@dataclass(frozen=True)
class TitleDef:
    id: str = field(metadata={
        'title': '称号编号',
        'description': '唯一标识，玩家可用 !买称号 或 !佩戴称号 传入，玩家存档也按它识别',
    })
    name: str = field(metadata={
        'title': '称号名称',
        'description': '显示在列表里的名称，不能与其他称号重复',
    })
    display: str = field(metadata={
        'title': '展示效果',
        'description': '支持 Minecraft § 颜色与格式代码，例如 §6[开拓者]',
    })
    price: int = field(default=0, metadata={
        'title': '价格',
        'description': '购买需要的金币数量（需经济插件）；0 表示免费',
        'minimum': 0,
    })
    purchasable: bool = field(default=False, metadata={
        'title': '允许购买',
        'description': '开启后玩家可以用 !买称号 购买；关闭后仅管理员可用 !发称号 发放',
    })
    description: str = field(default='', metadata={
        'title': '说明',
        'description': '显示在 !称号 列表里的介绍，可留空',
    })

    def __post_init__(self):
        if not self.id.strip() or self.id.strip() != self.id:
            raise ValueError('title id must not be empty or have leading/trailing whitespace')
        if not self.name.strip() or self.name.strip() != self.name:
            raise ValueError('title name must not be empty or have leading/trailing whitespace')
        if not self.display.strip():
            raise ValueError('title display must not be empty')
        if any(ord(c) < 32 or ord(c) == 127 for value in (self.id, self.name, self.display, self.description)
               for c in value):
            raise ValueError('title fields must not contain control characters')
        if type(self.price) is not int or self.price < 0:
            raise ValueError('title price must be a non-negative integer')


def _default_titles() -> list[TitleDef]:
    return [
        TitleDef(
            id='pioneer',
            name='开拓者',
            display='§6[开拓者]',
            price=100,
            purchasable=True,
            description='服务器开拓纪念',
        ),
        TitleDef(
            id='vip',
            name='赞助者',
            display='§b[赞助者]',
            price=0,
            purchasable=False,
            description='管理员手动发放',
        ),
    ]


@dataclass(frozen=True)
class Settings:
    admins: list[str] = field(default_factory=list, metadata={
        'title': '管理员',
        'description': '可以使用 !发称号 和 !收称号 的玩家名字，不区分大小写',
    })
    titles: list[TitleDef] = field(default_factory=_default_titles, metadata={
        'title': '称号列表',
        'description': '配置所有可用的称号，编号（id）和名称（name）均不能重复',
    })
    horn_enabled: bool = field(default=True, metadata={
        'title': '启用全服喇叭',
        'description': '开启后玩家可以用 !喇叭 发送带佩戴称号的全服广播',
    })
    horn_cost: int = field(default=10, metadata={
        'title': '喇叭费用',
        'description': '每次发送喇叭扣除的金币（需要经济插件，未安装时跳过扣款）；填 0 免费',
        'minimum': 0,
    })
    horn_cooldown_seconds: int = field(default=15, metadata={
        'title': '喇叭冷却（秒）',
        'description': '同一玩家两次发送喇叭之间至少间隔的秒数，填 0 不限制',
        'minimum': 0,
    })
    horn_sound: bool = field(default=True, metadata={
        'title': '喇叭提示音',
        'description': '全服发喇叭时给全体在线玩家播放 random.orb 提示音',
    })
    join_broadcast: bool = field(default=True, metadata={
        'title': '进服头衔播报',
        'description': '玩家进服时若佩戴有称号，向全服播报其佩戴头衔',
    })

    def __post_init__(self):
        if any(not name.strip() for name in self.admins):
            raise ValueError('admin names must not be empty')
        if len({title.id.casefold() for title in self.titles}) != len(self.titles):
            raise ValueError('title ids must be unique')
        if len({title.name.casefold() for title in self.titles}) != len(self.titles):
            raise ValueError('title names must be unique')
        if type(self.horn_cost) is not int or self.horn_cost < 0:
            raise ValueError('horn cost must be a non-negative integer')
        if type(self.horn_cooldown_seconds) is not int or self.horn_cooldown_seconds < 0:
            raise ValueError('horn cooldown must be a non-negative integer')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def player_key(player) -> str:
    return 'player:' + player_id(player)


def pending_key(name: str) -> str:
    return 'pending_name:' + name.casefold()


def charge_request_id(prefix: str, event_id: str) -> str:
    """由事件 ID 确定性生成长度 64 的 request_id，重投时经济插件可按其去重。"""
    return hashlib.sha256(f'{prefix}:{event_id}'.encode('utf-8')).hexdigest()


def find_title(titles: list[TitleDef], query: str) -> TitleDef | None:
    """按 1 基编号，或不区分大小写的 id / name 查找称号。"""
    cleaned = query.strip()
    if not cleaned:
        return None
    if cleaned.isascii() and cleaned.isdigit():
        index = int(cleaned) - 1
        if 0 <= index < len(titles):
            return titles[index]
    folded = cleaned.casefold()
    return (next((title for title in titles if title.id.casefold() == folded), None)
            or next((title for title in titles if title.name.casefold() == folded), None))


def find_title_by_id(titles: list[TitleDef], title_id: str | None) -> TitleDef | None:
    if not title_id:
        return None
    return next((title for title in titles if title.id == title_id), None)


def sanitize_horn_text(text: str) -> str:
    """去掉控制符、换行和 Minecraft § 格式代码，防止伪造颜色或系统播报。"""
    # 先清除 C0/DEL 控制字符，避免 § 与格式字母之间插入控制符绕过过滤。
    cleaned = re.sub(r'[\x00-\x1f\x7f]+', ' ', text)
    while '§' in cleaned:
        cleaned = re.sub(r'§.?', '', cleaned)
    return ' '.join(cleaned.split())


def load_player_record(tx, key: str) -> dict:
    raw = tx.get(key)
    if not isinstance(raw, dict):
        return {'owned': [], 'equipped': None}
    owned = []
    for item in raw.get('owned', []):
        if isinstance(item, str) and item and item not in owned:
            owned.append(item)
    equipped = raw.get('equipped') if isinstance(raw.get('equipped'), str) else None
    record = {'owned': owned, 'equipped': equipped}
    horn_at = raw.get('horn_at')
    if type(horn_at) in (int, float) and math.isfinite(horn_at):
        record['horn_at'] = float(horn_at)
    return record


def apply_pending_ops(titles: list[TitleDef], record: dict, pending: dict) -> tuple[bool, list[str], list[str]]:
    """把 pending_name 中的授予/收回操作合并到玩家档案，返回 (是否有变更, 授予列表, 收回列表)。"""
    changed = False
    granted, revoked = [], []
    for tid, op in pending.items():
        title_def = find_title_by_id(titles, tid)
        if op == 'grant':
            if title_def is not None and tid not in record['owned']:
                record['owned'].append(tid)
                changed = True
                granted.append(f'「{title_def.name}」（{title_def.display}§r）')
        elif op == 'revoke':
            if tid in record['owned']:
                record['owned'] = [item for item in record['owned'] if item != tid]
                if record['equipped'] == tid:
                    record['equipped'] = None
                changed = True
                label = f'「{title_def.name}」' if title_def is not None else f'「{tid}」'
                revoked.append(label)
    return changed, granted, revoked


def _style_step(style: str, token: str) -> str:
    if len(token) != 2 or token[0] != '§':
        return style
    code = token[1].lower()
    if code == 'r':
        return ''
    if code in '0123456789abcdefghijmnpqstu':
        return token + ''.join('§' + c for c in 'klo' if '§' + c in style.lower())
    if code in 'klo' and token.lower() not in style.lower():
        return style + token
    return style


def chunk_broadcast(text: str, max_command_bytes: int = MAX_BROADCAST_COMMAND_BYTES) -> list[str]:
    """按 320 字节上限切分全服 tellraw，跨段保留当前 § 样式。"""
    def fits(value: str) -> bool:
        raw = json.dumps({'rawtext': [{'text': value}]}, ensure_ascii=False, separators=(',', ':'))
        return len(('tellraw @a ' + raw).encode('utf-8')) <= max_command_bytes

    chunks, chunk, prefix, style = [], '', '', ''
    i = 0
    while i < len(text):
        end = i + (2 if text[i] == '§' and i + 1 < len(text) else 1)
        token = text[i:end]
        i = end
        if not fits(prefix + chunk + token):
            if chunk:
                chunks.append(prefix + chunk)
                chunk, prefix = '', style
            if not fits(prefix + token):
                raise ValueError('broadcast token exceeds command budget')
        chunk += token
        style = _style_step(style, token)
    if chunk or not chunks:
        chunks.append(prefix + chunk)
    return chunks


class Titles(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.bot_name = None
        self._pending_buys: set[tuple[str, str]] = set()
        self._pending_horns: set[str] = set()
        self.started = self.last_event = None
        self.caught_up = False

    async def on_start(self, ctx: PluginContext):
        self.bot_name = None
        self._pending_buys.clear()
        self._pending_horns.clear()
        self.started = self.now()
        self.last_event = None
        self.caught_up = False
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('称号', aliases=('ch', 'titles'), description='查看全部称号与可用指令', max_args=0)(
                lambda command: self.list_titles(ctx, command))
            router.command('买称号', aliases=('mch',), description='购买指定称号', usage='<编号或名称>')(
                lambda command: self.buy_title(ctx, command))
            router.command('佩戴称号', aliases=('pdch',), description='佩戴已拥有的称号', usage='<编号或名称>')(
                lambda command: self.equip_title(ctx, command))
            router.command('卸下称号', aliases=('xxch',), description='卸下当前佩戴的称号', max_args=0)(
                lambda command: self.unequip_title(ctx, command))
            router.command('喇叭', aliases=('lb', 'horn'), description='发送全服广播喇叭', usage='<内容>')(
                lambda command: self.horn(ctx, command))
            router.command('发称号', aliases=('granttitle',), description='授予玩家称号（管理员）',
                           usage='<玩家名> <编号或名称>')(
                lambda command: self.admin_title(ctx, command, grant=True))
            router.command('收称号', aliases=('revoketitle',), description='收回玩家称号（管理员）',
                           usage='<玩家名> <编号或名称>')(
                lambda command: self.admin_title(ctx, command, grant=False))

    async def on_event(self, ctx: PluginContext, event: Event):
        self.watch_backlog()
        try:
            if event.kind == 'player.list_added' and await self.on_join(ctx, event):
                return
            if event.kind == 'chat.received':
                for router in self.routers:
                    if await router.dispatch(event):
                        return
        except (ValueError, PlayerNotFound, AmbiguousPlayer):
            # 引号不配对、参数个数不对，或玩家已经下线：不拦截普通聊天，只确认事件。
            pass
        async with self.lock:
            await (await event.transaction(keys=[])).save()

    async def bot(self, ctx) -> str | None:
        try:
            name = (await ctx.robot()).get('bot_name')
        except (IPCError, ProtocolError, TimeoutError, ConnectionError, OSError):
            return self.bot_name
        if name:
            self.bot_name = name
        return self.bot_name

    def now(self) -> float:
        return time.monotonic()

    def clock(self) -> float:
        # 喇叭冷却跨重启保存，使用墙上时间。
        return time.time()

    def watch_backlog(self):
        """记下事件到达时间，判断启动时积压的旧事件是否已经处理完。"""
        now = self.now()
        if not self.caught_up:
            last = self.last_event if self.last_event is not None else self.started
            if last is None or now - last >= CATCH_UP_GAP or now - self.started >= CATCH_UP_LIMIT:
                self.caught_up = True
        self.last_event = now

    @staticmethod
    async def resolve_online(ctx, name, xuid):
        """按名字和 XUID 确认在线玩家；兼容在线名单不带 XUID 的 Host。"""
        try:
            return await ctx.resolve_player(name=name, xuid=xuid or None)
        except PlayerNotFound:
            if not xuid:
                raise
            player = await ctx.resolve_player(name=name)
            if player.get('xuid'):
                raise
            return player

    @staticmethod
    def is_admin(ctx, player) -> bool:
        return player['name'].casefold() in {name.strip().casefold() for name in ctx.config.admins}

    # ---- 进服合并与头衔播报 ----

    async def on_join(self, ctx, event) -> bool:
        body = event.payload.get('payload')
        if not isinstance(body, dict) or not isinstance(body.get('name'), str) or not body['name']:
            return False
        player = await self.resolve_online(ctx, body['name'], body.get('xuid'))
        bot = await self.bot(ctx)
        if bot and player['name'] == bot:
            return False
        p_key = player_key(player)
        pend_key = pending_key(player['name'])
        keys = [p_key] if len(pend_key.encode('utf-8')) > MAX_STORAGE_KEY_BYTES else [p_key, pend_key]
        async with self.lock:
            tx = await event.transaction(keys=keys)
            record = load_player_record(tx, p_key)

            if pend_key in keys:
                pending = tx.get(pend_key)
                if isinstance(pending, dict) and pending:
                    changed, granted, revoked = apply_pending_ops(ctx.config.titles, record, pending)
                    notices = []
                    if granted:
                        notices.append(f'你上线后收到管理员授予的称号：{"、".join(granted)}，发送 !佩戴称号 <编号或名称> 即可佩戴。')
                    if revoked:
                        notices.append(f'管理员收回了你的称号：{"、".join(revoked)}。')
                    if notices:
                        # 先成功构造私聊提醒，再删除 pending_name；避免提醒准备失败时丢失待生效操作。
                        self.reply(ctx, tx, player, '\n'.join(notices), tag='pending')
                    tx.delete(pend_key)
                    if changed:
                        tx.set(p_key, record)

            live = self.caught_up and event.payload.get('session', ctx.operations.session) == ctx.operations.session
            if ctx.config.join_broadcast and live:
                equipped_id = record['equipped']
                equipped = (find_title_by_id(ctx.config.titles, equipped_id)
                            if equipped_id and equipped_id in record['owned'] else None)
                if equipped is not None:
                    self.broadcast(ctx, tx, f'{equipped.display}§r §f{player["name"]} §e加入了服务器', tag='join')

            await tx.save()
        return True

    # ---- 玩家命令 ----

    async def list_titles(self, ctx, command):
        player, p_key = command.player, player_key(command.player)
        async with self.lock:
            tx = await command.event.transaction(keys=[p_key])
            record = load_player_record(tx, p_key)
            owned = set(record['owned'])
            equipped_id = record['equipped']

            lines = []
            if not ctx.config.titles:
                lines.append('服务器还没有配置任何称号。')
            else:
                lines.append('称号列表：')
                for index, title in enumerate(ctx.config.titles, 1):
                    if equipped_id == title.id and title.id in owned:
                        status = '§a[佩戴中]§r'
                    elif title.id in owned:
                        status = '§a[已拥有]§r'
                    else:
                        status = '§7[未拥有]§r'
                    if title.purchasable:
                        acquire = f'{title.price} 金币' if title.price > 0 else '免费购买'
                    else:
                        acquire = '管理员发放'
                    desc = f' - {title.description}' if title.description else ''
                    lines.append(f'{index}. {title.display}§r {title.name} ({title.id}) | {acquire} | {status}{desc}')

            help_cmds = ['!买称号 <编号或名称>', '!佩戴称号 <编号或名称>', '!卸下称号']
            if ctx.config.horn_enabled:
                help_cmds.append('!喇叭 <内容>')
            lines.append('可用指令：' + '、'.join(help_cmds))
            if self.is_admin(ctx, player):
                lines.append('管理员指令：!发称号 <玩家名> <编号或名称>、!收称号 <玩家名> <编号或名称>')

            self.reply(ctx, tx, player, '\n'.join(lines))
            await tx.save()

    async def buy_title(self, ctx, command):
        query = ' '.join(command.args).strip()
        if not query:
            return await self.answer(ctx, command, '用法：!买称号 <编号或名称>，发送 !称号 查看列表。')
        title = find_title(ctx.config.titles, query)
        if title is None:
            return await self.answer(ctx, command, f'没有找到称号「{query}」，发送 !称号 查看全部称号。')
        if not title.purchasable:
            return await self.answer(ctx, command, f'称号「{title.name}」不可购买，仅限管理员发放。')

        player, p_key = command.player, player_key(command.player)
        pid = player_id(player)
        paid_note = ''
        if title.price == 0:
            async with self.lock:
                tx = await command.event.transaction(keys=[p_key])
                record = load_player_record(tx, p_key)
                if title.id in record['owned']:
                    self.reply(ctx, tx, player, f'你已经拥有称号「{title.name}」了。')
                    return await tx.save()
                record['owned'].append(title.id)
                current_valid = (find_title_by_id(ctx.config.titles, record['equipped']) is not None
                                 and record['equipped'] in record['owned'])
                if not current_valid:
                    record['equipped'] = title.id
                    equip_note = '已自动为你佩戴。'
                else:
                    equip_note = f'发送 !佩戴称号 {title.id} 即可切换佩戴。'
                tx.set(p_key, record)
                self.reply(ctx, tx, player, f'成功购买称号「{title.name}」（{title.display}§r），{equip_note}')
                await tx.save()
            return

        buy_key = (pid, title.id)
        async with self.lock:
            snap = await ctx.storage.transaction(keys=[p_key])
            record = load_player_record(snap, p_key)
            if title.id in record['owned']:
                tx = await command.event.transaction(keys=[])
                self.reply(ctx, tx, player, f'你已经拥有称号「{title.name}」了。')
                return await tx.save()
            if buy_key in self._pending_buys:
                tx = await command.event.transaction(keys=[])
                self.reply(ctx, tx, player, '这笔称号购买正在处理中，请稍候。')
                return await tx.save()
            self._pending_buys.add(buy_key)

        try:
            request_id = charge_request_id('buy', command.event.payload['event_id'])
            status, payload = await self.charge_economy(
                ctx, player, title.price, ('购买称号 ' + title.name)[:32], request_id)
            if status == 'not_installed':
                return await self.answer(ctx, command, '服务器未开启经济系统，暂时无法购买收费称号。')
            if status == 'insufficient_funds':
                return await self.answer(ctx, command, f'金币不足，购买「{title.name}」需要 {title.price} 金币。')
            if status == 'unsettled':
                return await self.answer(ctx, command, '扣款结果暂时不确定，这次没有发放称号。可以用 !流水 查看，没有扣款可以再试一次。')
            if status == 'rejected':
                return await self.answer(ctx, command, '扣款失败，未能购买该称号。')
            coin = payload.get('currency', '金币')
            paid_note = f'花费 {title.price} {coin}，剩余 {payload["balance"]} {coin}。'

            async with self.lock:
                tx = await command.event.transaction(keys=[p_key])
                record = load_player_record(tx, p_key)
                if title.id not in record['owned']:
                    record['owned'].append(title.id)
                current_valid = (find_title_by_id(ctx.config.titles, record['equipped']) is not None
                                 and record['equipped'] in record['owned'])
                if not current_valid:
                    record['equipped'] = title.id
                    equip_note = '已自动为你佩戴。'
                else:
                    equip_note = f'发送 !佩戴称号 {title.id} 即可切换佩戴。'
                tx.set(p_key, record)
                self.reply(ctx, tx, player, f'成功购买称号「{title.name}」（{title.display}§r），{equip_note}{paid_note}')
                await tx.save()
        finally:
            async with self.lock:
                self._pending_buys.discard(buy_key)

    async def equip_title(self, ctx, command):
        query = ' '.join(command.args).strip()
        if not query:
            return await self.answer(ctx, command, '用法：!佩戴称号 <编号或名称>，发送 !称号 查看列表。')
        title = find_title(ctx.config.titles, query)
        if title is None:
            return await self.answer(ctx, command, f'没有找到称号「{query}」，发送 !称号 查看全部称号。')

        player, p_key = command.player, player_key(command.player)
        async with self.lock:
            tx = await command.event.transaction(keys=[p_key])
            record = load_player_record(tx, p_key)
            if title.id not in record['owned']:
                hint = f'可发送 !买称号 {title.id} 购买。' if title.purchasable else '仅限管理员发放。'
                self.reply(ctx, tx, player, f'你还没有拥有称号「{title.name}」，{hint}')
                return await tx.save()
            if record['equipped'] == title.id:
                self.reply(ctx, tx, player, f'你已经在佩戴称号「{title.name}」（{title.display}§r）了。')
                return await tx.save()

            record['equipped'] = title.id
            tx.set(p_key, record)
            self.reply(ctx, tx, player, f'已佩戴称号「{title.name}」（{title.display}§r）。')
            await tx.save()

    async def unequip_title(self, ctx, command):
        player, p_key = command.player, player_key(command.player)
        async with self.lock:
            tx = await command.event.transaction(keys=[p_key])
            record = load_player_record(tx, p_key)
            if not record['equipped']:
                self.reply(ctx, tx, player, '你当前没有佩戴任何称号。')
                return await tx.save()
            record['equipped'] = None
            tx.set(p_key, record)
            self.reply(ctx, tx, player, '已卸下当前佩戴的称号。')
            await tx.save()

    async def horn(self, ctx, command):
        if not ctx.config.horn_enabled:
            return await self.answer(ctx, command, '服务器已关闭全服喇叭功能。')
        if not command.args:
            return await self.answer(ctx, command, '用法：!喇叭 <内容>')
        cleaned = sanitize_horn_text(' '.join(command.args))
        if not cleaned:
            return await self.answer(ctx, command, '喇叭内容不能为空或只包含格式控制符。')
        if len(cleaned) > MAX_HORN_LENGTH:
            return await self.answer(ctx, command, f'喇叭内容太长了，最多 {MAX_HORN_LENGTH} 个字。')

        player, p_key = command.player, player_key(command.player)
        pid = player_id(player)
        cost = ctx.config.horn_cost

        async with self.lock:
            snap = await ctx.storage.transaction(keys=[p_key])
            record = load_player_record(snap, p_key)
            now, cooldown = self.clock(), ctx.config.horn_cooldown_seconds
            wait = min(record.get('horn_at', -math.inf) + cooldown - now, cooldown)
            if wait > 0:
                tx = await command.event.transaction(keys=[])
                self.reply(ctx, tx, player, f'喇叭还在冷却中，请 {math.ceil(wait)} 秒后再试。')
                return await tx.save()
            if cost > 0 and pid in self._pending_horns:
                tx = await command.event.transaction(keys=[])
                self.reply(ctx, tx, player, '上一条喇叭正在扣款处理中，请稍候。')
                return await tx.save()
            if cost > 0:
                self._pending_horns.add(pid)

        try:
            paid_note = ''
            if cost > 0:
                request_id = charge_request_id('horn', command.event.payload['event_id'])
                status, payload = await self.charge_economy(ctx, player, cost, '全服喇叭', request_id)
                if status == 'insufficient_funds':
                    return await self.answer(ctx, command, f'金币不足，发送一次喇叭需要 {cost} 金币。')
                if status == 'unsettled':
                    return await self.answer(ctx, command, '扣款结果暂时不确定，这次没有发送喇叭。可以用 !流水 查看，没有扣款可以再试一次。')
                if status == 'rejected':
                    return await self.answer(ctx, command, '扣款失败，这次没有发送喇叭。')
                if status == 'paid':
                    coin = payload.get('currency', '金币')
                    paid_note = f'花费 {cost} {coin}，剩余 {payload["balance"]} {coin}。'
                # status == 'not_installed'：未装经济插件时视作免费。

            async with self.lock:
                tx = await command.event.transaction(keys=[p_key])
                record = load_player_record(tx, p_key)
                record['horn_at'] = self.clock()
                tx.set(p_key, record)
                equipped_id = record['equipped']
                equipped = (find_title_by_id(ctx.config.titles, equipped_id)
                            if equipped_id and equipped_id in record['owned'] else None)
                if equipped is not None:
                    message = f'§e[喇叭] {equipped.display}§r §f{player["name"]}§e: §f{cleaned}'
                else:
                    message = f'§e[喇叭] §f{player["name"]}§e: §f{cleaned}'
                self.broadcast(ctx, tx, message, tag='horn')
                if ctx.config.horn_sound:
                    tx.action(ctx.commands.prepare(
                        'execute as @a at @s run playsound random.orb @s',
                        idempotency_key=tx.commit_id + '_sound',
                        deadline=deadline(),
                    ))
                if paid_note:
                    self.reply(ctx, tx, player, f'已发送全服喇叭，{paid_note}')
                else:
                    self.reply(ctx, tx, player, '已发送全服喇叭。')
                await tx.save()
        finally:
            if cost > 0:
                async with self.lock:
                    self._pending_horns.discard(pid)

    # ---- 管理员命令 ----

    async def admin_title(self, ctx, command, *, grant: bool):
        verb = '发称号' if grant else '收称号'
        if not self.is_admin(ctx, command.player):
            return await self.answer(ctx, command, '只有管理员可以发放或收回称号。')
        if len(command.args) < 2:
            return await self.answer(ctx, command, f'用法：!{verb} <玩家名> <编号或名称>')
        target_name = command.args[0].strip()
        query = ' '.join(command.args[1:]).strip()
        if not target_name or not query:
            return await self.answer(ctx, command, f'用法：!{verb} <玩家名> <编号或名称>')
        pend_key = pending_key(target_name)
        if len(pend_key.encode('utf-8')) > MAX_STORAGE_KEY_BYTES:
            return await self.answer(ctx, command, '玩家名过长，请检查后重试。')
        title = find_title(ctx.config.titles, query)
        if title is None:
            return await self.answer(ctx, command, f'没有找到称号「{query}」，发送 !称号 查看全部称号。')

        players = await ctx.players()
        bot = await self.bot(ctx)
        candidates = [p for p in players if not (bot and p['name'] == bot)]
        matches = [p for p in candidates if p['name'] == target_name]
        if not matches:
            folded = target_name.casefold()
            matches = [p for p in candidates if p['name'].casefold() == folded]
        if len(matches) > 1:
            return await self.answer(ctx, command, f'当前有多名名为「{target_name}」的在线玩家，无法通过名字操作。')

        if not matches:
            async with self.lock:
                tx = await command.event.transaction(keys=[pend_key])
                pending = tx.get(pend_key)
                pending = dict(pending) if isinstance(pending, dict) else {}
                pending[title.id] = 'grant' if grant else 'revoke'
                tx.set(pend_key, pending)
                if grant:
                    text = f'{target_name} 当前不在线，已记录发放称号「{title.name}」（{title.display}§r），将在其下次进服时自动生效。'
                else:
                    text = f'{target_name} 当前不在线，已记录收回称号「{title.name}」，将在其下次进服时自动生效。'
                self.reply(ctx, tx, command.player, text)
                return await tx.save()

        target = matches[0]
        target_p_key = player_key(target)
        target_pend_key = pending_key(target['name'])
        keys = ([target_p_key, target_pend_key]
                if len(target_pend_key.encode('utf-8')) <= MAX_STORAGE_KEY_BYTES else [target_p_key])
        async with self.lock:
            tx = await command.event.transaction(keys=keys)
            record = load_player_record(tx, target_p_key)
            pending_changed = False
            if target_pend_key in keys:
                pending = tx.get(target_pend_key)
                if isinstance(pending, dict) and pending:
                    tx.delete(target_pend_key)
                    pending_changed, _, _ = apply_pending_ops(ctx.config.titles, record, pending)

            notify_target = None
            if grant:
                if title.id in record['owned']:
                    if pending_changed:
                        tx.set(target_p_key, record)
                    admin_text = f'{target["name"]} 已经拥有称号「{title.name}」了。'
                else:
                    record['owned'].append(title.id)
                    tx.set(target_p_key, record)
                    admin_text = f'已向在线玩家 {target["name"]} 发放称号「{title.name}」（{title.display}§r）。'
                    notify_target = f'管理员授予了你称号「{title.name}」（{title.display}§r），发送 !佩戴称号 {title.id} 即可佩戴。'
            else:
                if title.id not in record['owned']:
                    if pending_changed:
                        tx.set(target_p_key, record)
                    admin_text = f'{target["name"]} 还没有拥有称号「{title.name}」。'
                else:
                    record['owned'] = [item for item in record['owned'] if item != title.id]
                    if record['equipped'] == title.id:
                        record['equipped'] = None
                    tx.set(target_p_key, record)
                    admin_text = f'已收回在线玩家 {target["name"]} 的称号「{title.name}」。'
                    notify_target = f'管理员收回了你的称号「{title.name}」。'

            self.reply(ctx, tx, command.player, admin_text)
            if notify_target and player_id(target) != player_id(command.player):
                self.reply(ctx, tx, target, notify_target, tag='target')
            await tx.save()

    # ---- 公用 ----

    async def charge_economy(self, ctx, player, amount: int, reason: str, request_id: str):
        """调用经济插件扣款；结果不明时用同一个 request_id 再试一次。"""
        arguments = {
            'player_id': player_id(player),
            'amount': amount,
            'reason': reason,
            'request_id': request_id,
        }
        for retry in (False, True):
            try:
                result = await ctx.services.call(ECONOMY_PAY, arguments, timeout=5)
                return 'paid', result
            except IPCRejected:
                return 'not_installed', None
            except ServiceRejected as error:
                if error.code == 'insufficient_funds':
                    return 'insufficient_funds', None
                if error.code != 'unavailable':
                    return 'rejected', error.code
                if retry:
                    return 'unsettled', None
            except UNSETTLED:
                if retry:
                    return 'unsettled', None
        return 'unsettled', None

    async def answer(self, ctx, command, text: str):
        async with self.lock:
            tx = await command.event.transaction(keys=[])
            self.reply(ctx, tx, command.player, text)
            await tx.save()

    def reply(self, ctx, tx, player, text: str, *, tag: str = 'reply'):
        plan = ctx.notifications.prepare(
            player['name'],
            text,
            idempotency_key=f'{tx.commit_id}_{tag}',
            deadline=deadline(),
        )
        for part in plan.parts:
            tx.action(part.intent)

    def broadcast(self, ctx, tx, text: str, *, tag: str):
        chunks = chunk_broadcast(text)
        for index, chunk in enumerate(chunks):
            rawtext = json.dumps({'rawtext': [{'text': chunk}]}, ensure_ascii=False, separators=(',', ':'))
            suffix = '' if len(chunks) == 1 else f'_{index}'
            tx.action(ctx.commands.prepare(
                'tellraw @a ' + rawtext,
                idempotency_key=f'{tx.commit_id}_{tag}{suffix}',
                deadline=deadline(),
            ))


if __name__ == '__main__':
    Titles().run()
