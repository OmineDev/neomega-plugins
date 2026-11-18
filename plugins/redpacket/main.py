"""口令红包：玩家用金币发拼手气或口令红包，全服发送 !抢 领取，过期未领完自动退回。

所有状态保存在 packets（红包记录与自增编号）和 refund_notices（离线退回通知）
两个存储键中，全部命令、进服通知与定时退回共用 self.lock 串行处理。
!发红包 先以事件 ID 决定的幂等键向经济插件扣款，成功后在一次提交里写入红包、
回复发包人并广播全服。!抢 先在锁内读取红包状态并校验资格，再用 (红包编号, 玩家)
决定的幂等键调用经济插件发奖，成功后写入领取记录、私聊回复并在抢完时广播手气最佳。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import random
import time

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ServiceRejected, player_target)
from neomega_runtime.http import ProtocolError
from neomega_runtime.managed import IPCRejected
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from neomega_runtime.storage import CommitUncertain

PACKETS = 'packets'
REFUND_NOTICES = 'refund_notices'
ECONOMY_PAY = 'plugin.neomega.economy.pay'
ECONOMY_GRANT = 'plugin.neomega.economy.grant'
# 结果不明的扣款或发奖可以用同一个 request_id 安全重试；明确的业务拒绝不在其中。
UNSETTLED = (ServiceWaitTimeout, ServiceUncertain, ServiceFailed)

MAX_PASSCODE_LENGTH = 24
SWEEP_INTERVAL_SECONDS = 60
FINISHED_RETENTION_SECONDS = 3600
MAX_FINISHED_KEEP = 20
MAX_REFUND_NOTICES = 20
CATCH_UP_GAP = 1.0
CATCH_UP_LIMIT = 60.0

PAY_UNSURE = '扣款结果暂时不确定，红包暂未发出。可以用 !流水 查看，没有扣款可以再试一次。'
GRANT_UNSURE = '发放红包金币结果暂时不确定，请稍后用 !流水 查看。'


@dataclass(frozen=True)
class Settings:
    admins: list[str] = field(default_factory=list, metadata={
        'title': '管理员', 'description': '可以在游戏里发系统红包的玩家名字，不区分大小写'})
    min_total: int = field(default=10, metadata={
        'title': '最低总金额', 'description': '发一个红包最少需要多少金币', 'minimum': 1})
    max_count: int = field(default=20, metadata={
        'title': '最大份数', 'description': '单个红包最多可以分成多少份', 'minimum': 1, 'maximum': 100})
    expire_minutes: int = field(default=30, metadata={
        'title': '过期时间（分钟）', 'description': '红包超过这么久还没抢完时，剩余金额退回发包人',
        'minimum': 1, 'maximum': 1440})
    max_active_packets: int = field(default=10, metadata={
        'title': '最多同时存在红包数', 'description': '场上未过期且未抢完的红包最多有几个',
        'minimum': 1, 'maximum': 50})

    def __post_init__(self):
        if any(not name.strip() for name in self.admins):
            raise ValueError('admin names must not be empty')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def parse_positive_int(text: str) -> int | None:
    if not text.isascii() or not text.isdecimal():
        return None
    value = int(text)
    return value if value >= 1 else None


def valid_passcode(text: str) -> bool:
    return (1 <= len(text) <= MAX_PASSCODE_LENGTH
            and not text.isdigit() and not text.isdecimal()
            and text.isprintable() and text.strip() == text)


def split_amounts(total: int, count: int) -> list[int]:
    """二倍均值拆分算法：生成 count 个正整数，严格满足 sum(shares) == total。"""
    if type(total) is not int or type(count) is not int or count < 1 or total < count:
        raise ValueError('total must be an integer >= count and count >= 1')
    avail = total - count
    shares = []
    for left in range(count, 1, -1):
        if avail == 0:
            extra = 0
        elif 2 * avail < left:
            extra = 1 if random.random() < (avail / left) else 0
        else:
            upper = min(avail, (2 * avail) // left)
            extra = random.randint(0, upper)
        shares.append(1 + extra)
        avail -= extra
    shares.append(1 + avail)
    random.shuffle(shares)
    return shares


def pay_request_id(event_id: str) -> str:
    """同一个事件得到同一个扣款 request_id，不同事件不同，长度 <= 64。"""
    return 'rp-pay-' + hashlib.sha256(event_id.encode('utf-8')).hexdigest()[:40]


def grab_request_id(pkt_id: int, pid: str) -> str:
    raw = f'rp-grab-{pkt_id}-{pid}'
    if len(raw) <= 64:
        return raw
    return 'rp-grab-' + hashlib.sha256(f'{pkt_id}:{pid}'.encode('utf-8')).hexdigest()[:40]


def refund_request_id(pkt_id: int) -> str:
    raw = f'rp-refund-{pkt_id}'
    if len(raw) <= 64:
        return raw
    return 'rp-refund-' + hashlib.sha256(str(pkt_id).encode('utf-8')).hexdigest()[:40]


def has_claimed(pkt: dict, pid: str) -> bool:
    return any(claim['player_id'] == pid for claim in pkt['claims'])


def is_active(pkt: dict, now: float) -> bool:
    return pkt['expires_at'] > now and len(pkt['claims']) < pkt['count']


def is_finished(pkt: dict, now: float) -> bool:
    return len(pkt['claims']) >= pkt['count'] or (
        pkt['expires_at'] <= now and (pkt['is_system'] or pkt['refunded'])
    )


def prune_items(items: list[dict], now: float) -> list[dict]:
    """保留进行中、待退回和最近终结的红包，清除过久或超出数量上限的已终结旧红包。"""
    finished_keep = [
        pkt for pkt in items
        if is_finished(pkt, now) and pkt['expires_at'] + FINISHED_RETENTION_SECONDS > now
    ]
    keep_ids = {pkt['id'] for pkt in finished_keep[-MAX_FINISHED_KEEP:]}
    return [pkt for pkt in items if not is_finished(pkt, now) or pkt['id'] in keep_ids]


def format_remaining(seconds: float) -> str:
    secs = max(1, math.ceil(seconds))
    if secs >= 60:
        mins, rem = divmod(secs, 60)
        return f'{mins} 分 {rem} 秒' if rem else f'{mins} 分钟'
    return f'{secs} 秒'


def format_grab_hint(passcode: str, pkt_id: int) -> str:
    if passcode:
        return f'!抢 "{passcode}"' if any(ch.isspace() for ch in passcode) else f'!抢 {passcode}'
    return f'!抢 {pkt_id}'


def broadcast_command(text: str) -> str:
    return 'tellraw @a ' + json.dumps({'rawtext': [{'text': text}]}, ensure_ascii=False, separators=(',', ':'))


def find_packet_for_grab(items: list[dict], query: str | None, pid: str, now: float):
    if query is None:
        open_active = [pkt for pkt in items if is_active(pkt, now) and not pkt['passcode']]
        if len(open_active) == 1:
            pkt = open_active[0]
            if has_claimed(pkt, pid):
                return 'msg', f'你已经抢过 #{pkt["id"]} 号红包了。'
            return 'ok', pkt
        if len(open_active) > 1:
            return 'msg', '当前有多个无口令红包，请发送 !抢 <编号>，或发 !红包 查看。'
        if any(is_active(pkt, now) and pkt['passcode'] for pkt in items):
            return 'msg', '当前没有无口令红包，请发送 !抢 <口令>，或发 !红包 查看列表。'
        return 'msg', '当前没有可抢的红包。'

    if query.isdigit():
        pkt_id = int(query) if query.isascii() else -1
        pkt = next((item for item in items if item['id'] == pkt_id), None)
        if pkt is None:
            return 'msg', f'没有找到 #{query} 号红包。'
        if pkt['passcode']:
            return 'msg', f'#{pkt["id"]} 号是口令红包，请发送 !抢 <口令> 领取。'
        if has_claimed(pkt, pid):
            return 'msg', f'你已经抢过 #{pkt["id"]} 号红包了。'
        if len(pkt['claims']) >= pkt['count']:
            return 'msg', f'来晚一步，#{pkt["id"]} 号红包已经被抢完了！'
        if pkt['expires_at'] <= now:
            return 'msg', f'#{pkt["id"]} 号红包已经过期了。'
        return 'ok', pkt

    matches = [pkt for pkt in items if pkt['passcode'] == query]
    if not matches:
        return 'msg', '口令不对，或者没有这个红包。'
    claimable = [pkt for pkt in matches if is_active(pkt, now) and not has_claimed(pkt, pid)]
    if claimable:
        return 'ok', claimable[0]
    if any(is_active(pkt, now) and has_claimed(pkt, pid) for pkt in matches):
        return 'msg', '你已经抢过这个红包了。'
    exhausted = [pkt for pkt in matches if len(pkt['claims']) >= pkt['count']]
    if exhausted:
        if any(has_claimed(pkt, pid) for pkt in exhausted):
            return 'msg', '你已经抢过这个红包了。'
        return 'msg', '来晚一步，这个红包已经被抢完了！'
    return 'msg', '这个红包已经过期了。'


class RedPacket(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.started = None
        self.last_event = None
        self.caught_up = False
        self.reminded_joins: dict[str, set[int]] = {}

    async def on_start(self, ctx: PluginContext):
        self.started = self.monotonic()
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('发红包', aliases=('fhb', 'sendrp'), description='发金币红包（可带口令）',
                           usage='<总金币> <份数> [口令]')(
                lambda command: self.send_packet(ctx, command))
            router.command('抢', aliases=('qhb', 'grab'), description='抢红包',
                           usage='[口令或编号]')(
                lambda command: self.grab_packet(ctx, command))
            router.command('红包', aliases=('hb', 'redpackets'), description='查看当前可抢红包', max_args=0)(
                lambda command: self.list_packets(ctx, command))
            router.command('系统红包', aliases=('xthb', 'sysrp'), description='发系统红包（管理员）',
                           usage='<总金币> <份数> [口令]')(
                lambda command: self.system_packet(ctx, command))
        ctx.every(SWEEP_INTERVAL_SECONDS, lambda: self.sweep(ctx), name='redpacket-refund')

    async def on_event(self, ctx: PluginContext, event: Event):
        self.watch_backlog()
        try:
            if event.kind == 'player.list_added' and await self.handle_join(ctx, event):
                return
            if event.kind == 'chat.received':
                for router in self.routers:
                    if await router.dispatch(event):
                        return
        except (ValueError, PlayerNotFound, AmbiguousPlayer):
            # 引号不配对、参数数量不对，或玩家已经下线：不回复，只确认事件。
            pass
        async with self.lock:
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()
            event.defer_ack()

    def clock(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    def watch_backlog(self):
        now = self.monotonic()
        if not self.caught_up:
            last = self.last_event if self.last_event is not None else self.started
            if last is None or now - last >= CATCH_UP_GAP or now - self.started >= CATCH_UP_LIMIT:
                self.caught_up = True
        self.last_event = now

    @staticmethod
    async def resolve_join_player(ctx, name: str, xuid: str):
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
        wanted = {name.strip().casefold() for name in ctx.config.admins}
        return (player['name'].casefold() in wanted
                or (bool(player.get('uuid')) and player['uuid'].casefold() in wanted))

    # ---- 参数校验与建包 ----

    def validate_packet_args(self, ctx, args: tuple[str, ...], usage: str):
        if len(args) not in (2, 3):
            return None, usage
        total = parse_positive_int(args[0])
        count = parse_positive_int(args[1])
        if total is None or count is None:
            return None, f'总金币和份数都必须是正整数，{usage}'
        if count > ctx.config.max_count:
            return None, f'单个红包最多只能分成 {ctx.config.max_count} 份。'
        if total < ctx.config.min_total:
            return None, f'红包总金额至少需要 {ctx.config.min_total} 金币。'
        if total < count:
            return None, '总金币不能少于份数（每人至少 1 金币）。'
        passcode = ''
        if len(args) == 3:
            raw = args[2].strip()
            if not raw:
                return None, '口令不能为空，如果不设口令请省略第三个参数。'
            if not valid_passcode(raw):
                return None, f'口令不能是纯数字，且长度不能超过 {MAX_PASSCODE_LENGTH} 个字符。'
            passcode = raw
        return (total, count, passcode), None

    def create_packet(self, ctx, packets: dict, player: dict, *,
                      total: int, count: int, passcode: str, is_system: bool, now: float) -> dict:
        pkt_id = packets['next_id']
        packets['next_id'] = pkt_id + 1
        pkt = {
            'id': pkt_id,
            'sender_id': player_id(player),
            'sender_name': player['name'],
            'is_system': is_system,
            'total': total,
            'count': count,
            'shares': split_amounts(total, count),
            'claims': [],
            'passcode': passcode,
            'expires_at': now + ctx.config.expire_minutes * 60,
            'refunded': False,
        }
        packets['items'].append(pkt)
        return pkt

    @staticmethod
    def build_create_broadcast(pkt: dict, coin: str) -> str:
        sender_label = '系统' if pkt['is_system'] else pkt['sender_name']
        hint = format_grab_hint(pkt['passcode'], pkt['id'])
        if pkt['passcode']:
            return (f'[口令红包] {sender_label} 发出了一个 {pkt["total"]} {coin}的口令红包'
                    f'（共 {pkt["count"]} 份，编号 #{pkt["id"]}）！发送 {hint} 领取！')
        return (f'[红包] {sender_label} 发出了一个 {pkt["total"]} {coin}的红包'
                f'（共 {pkt["count"]} 份，编号 #{pkt["id"]}）！发送 {hint} 领取！')

    # ---- 经济插件调用 ----

    async def call_economy(self, ctx, method: str, arguments: dict):
        """调用经济服务；结果不明（含 unavailable）时用同一个 request_id 重试一次。"""
        for retry in (False, True):
            try:
                return await ctx.services.call(method, arguments, timeout=5)
            except ServiceRejected as error:
                if error.code != 'unavailable' or retry:
                    raise
            except UNSETTLED:
                if retry:
                    raise

    async def charge_sender(self, ctx, event_id: str, player: dict, total: int, has_passcode: bool):
        arguments = {
            'player_id': player_id(player),
            'amount': total,
            'reason': '发口令红包' if has_passcode else '发红包',
            'request_id': pay_request_id(event_id),
        }
        try:
            result = await self.call_economy(ctx, ECONOMY_PAY, arguments)
            return True, result, None
        except IPCRejected:
            return False, None, '经济服务当前不可用，红包未能发出。'
        except ServiceRejected as error:
            if error.code == 'insufficient_funds':
                return False, None, f'金币不足，发红包需要 {total} 金币。'
            if error.code == 'player_offline':
                return False, None, '扣款失败，请确认在线后再试。'
            if error.code == 'unavailable':
                return False, None, PAY_UNSURE
            return False, None, '扣款失败，红包未能发出。'
        except UNSETTLED:
            return False, None, PAY_UNSURE

    # ---- 玩家与管理员命令 ----

    async def send_packet(self, ctx: PluginContext, command):
        parsed, err = self.validate_packet_args(ctx, command.args, '用法：!发红包 <总金币> <份数> [口令]')
        if err is not None:
            return await self.answer(ctx, command, err)
        total, count, passcode = parsed
        player = command.player
        player_target(player['name'])
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=[PACKETS])
            packets = tx.get(PACKETS, {'next_id': 1, 'items': []})
            now = self.clock()
            active_count = sum(1 for pkt in packets['items'] if is_active(pkt, now))
            if active_count >= ctx.config.max_active_packets:
                self.reply(ctx, tx, player,
                           f'当前活跃红包已达上限（{ctx.config.max_active_packets} 个），请等已有红包抢完或过期后再发。')
                await tx.save()
                command.event.defer_ack()
                return
            ok, result, err_text = await self.charge_sender(
                ctx, command.event.payload['event_id'], player, total, bool(passcode))
            if not ok:
                self.reply(ctx, tx, player, err_text)
                await tx.save()
                command.event.defer_ack()
                return
            pkt = self.create_packet(ctx, packets, player, total=total, count=count,
                                     passcode=passcode, is_system=False, now=now)
            tx.set(PACKETS, packets)
            coin = result.get('currency', '金币')
            self.reply(ctx, tx, player,
                       f'已发出 #{pkt["id"]} 号红包（{total} {coin}，{count} 份），现有 {result["balance"]} {coin}。')
            self.broadcast(ctx, tx, self.build_create_broadcast(pkt, coin), tag='broadcast')
            await tx.save()
            command.event.defer_ack()

    async def grab_packet(self, ctx: PluginContext, command):
        if len(command.args) > 1:
            return await self.answer(ctx, command, '用法：!抢 [口令或编号]')
        query = None
        if len(command.args) == 1:
            query = command.args[0].strip()
            if not query:
                return await self.answer(ctx, command, '请输入正确的口令或红包编号，或直接发送 !抢。')

        player = command.player
        player_target(player['name'])
        pid = player_id(player)
        async with self.lock:
            packets = await ctx.storage.get(PACKETS, {'next_id': 1, 'items': []})
            now = self.clock()
            status, target = find_packet_for_grab(packets['items'], query, pid, now)
            if status != 'ok':
                tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
                self.reply(ctx, tx, player, target)
                await tx.save()
                command.event.defer_ack()
                return

            pkt_id = target['id']
            amount = target['shares'][len(target['claims'])]
            arguments = {
                'player_id': pid,
                'amount': amount,
                'reason': '抢口令红包' if target['passcode'] else '抢红包',
                'request_id': grab_request_id(pkt_id, pid),
            }
            try:
                result = await self.call_economy(ctx, ECONOMY_GRANT, arguments)
            except IPCRejected:
                err_text = '经济服务当前不可用，领取失败，请稍后再试。'
            except ServiceRejected as error:
                err_text = GRANT_UNSURE if error.code == 'unavailable' else '发放红包金币失败，请稍后再试。'
            except UNSETTLED:
                err_text = GRANT_UNSURE
            else:
                err_text = None

            if err_text is not None:
                tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
                self.reply(ctx, tx, player, err_text)
                await tx.save()
                command.event.defer_ack()
                return

            tx = await ctx.storage.transaction(event=command.event.payload, keys=[PACKETS])
            packets = tx.get(PACKETS, {'next_id': 1, 'items': []})
            pkt = next(item for item in packets['items'] if item['id'] == pkt_id)
            pkt['claims'].append({'player_id': pid, 'name': player['name'], 'amount': amount})
            tx.set(PACKETS, packets)

            coin = result.get('currency', '金币')
            if result.get('pending'):
                reply_text = f'恭喜你抢到了 #{pkt_id} 号红包，获得 {amount} {coin}（将在下次确认在线时入账）。'
            else:
                reply_text = f'恭喜你抢到了 #{pkt_id} 号红包，获得 {amount} {coin}，现有 {result["balance"]} {coin}。'
            self.reply(ctx, tx, player, reply_text)

            if len(pkt['claims']) == pkt['count']:
                best = max(pkt['claims'], key=lambda claim: claim['amount'])
                sender_label = '系统' if pkt['is_system'] else pkt['sender_name']
                finish_text = (f'[红包] {sender_label} 的 #{pkt_id} 号红包（{pkt["total"]} {coin}，共 {pkt["count"]} 份）'
                               f'已被抢完！手气最佳：{best["name"]}（{best["amount"]} {coin}）！')
                self.broadcast(ctx, tx, finish_text, tag='finish')
            await tx.save()
            command.event.defer_ack()

    async def list_packets(self, ctx: PluginContext, command):
        pid = player_id(command.player)
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=[PACKETS])
            packets = tx.get(PACKETS, {'next_id': 1, 'items': []})
            now = self.clock()
            active = [pkt for pkt in packets['items'] if is_active(pkt, now)]
            if not active:
                text = '当前没有可抢的红包。'
            else:
                lines = ['当前可抢的红包：']
                for pkt in active:
                    sender = '系统' if pkt['is_system'] else pkt['sender_name']
                    left_count = pkt['count'] - len(pkt['claims'])
                    need_pass = '需要口令' if pkt['passcode'] else '无口令'
                    rem = format_remaining(pkt['expires_at'] - now)
                    claimed_mark = '（已抢）' if has_claimed(pkt, pid) else ''
                    lines.append(
                        f'#{pkt["id"]} {sender}：{pkt["total"]} 金币，剩 {left_count}/{pkt["count"]} 份，'
                        f'{need_pass}，剩余 {rem}{claimed_mark}'
                    )
                lines.append('发送 !抢 <编号或口令> 领取红包。')
                text = '\n'.join(lines)
            self.reply(ctx, tx, command.player, text)
            await tx.save()
            command.event.defer_ack()

    async def system_packet(self, ctx: PluginContext, command):
        if not self.is_admin(ctx, command.player):
            return await self.answer(ctx, command, '只有管理员可以发系统红包。')
        parsed, err = self.validate_packet_args(ctx, command.args, '用法：!系统红包 <总金币> <份数> [口令]')
        if err is not None:
            return await self.answer(ctx, command, err)
        total, count, passcode = parsed
        player = command.player
        player_target(player['name'])
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=[PACKETS])
            packets = tx.get(PACKETS, {'next_id': 1, 'items': []})
            now = self.clock()
            active_count = sum(1 for pkt in packets['items'] if is_active(pkt, now))
            if active_count >= ctx.config.max_active_packets:
                self.reply(ctx, tx, player,
                           f'当前活跃红包已达上限（{ctx.config.max_active_packets} 个），请等已有红包抢完或过期后再发。')
                await tx.save()
                command.event.defer_ack()
                return
            pkt = self.create_packet(ctx, packets, player, total=total, count=count,
                                     passcode=passcode, is_system=True, now=now)
            tx.set(PACKETS, packets)
            self.reply(ctx, tx, player, f'已发出 #{pkt["id"]} 号系统红包（{total} 金币，{count} 份）。')
            self.broadcast(ctx, tx, self.build_create_broadcast(pkt, '金币'), tag='broadcast')
            await tx.save()
            command.event.defer_ack()

    # ---- 进服通知与过期退回 ----

    async def handle_join(self, ctx: PluginContext, event: Event) -> bool:
        body = event.payload['payload']
        player = await self.resolve_join_player(ctx, body['name'], body.get('xuid', ''))
        pid = player_id(player)
        is_live = self.caught_up and event.payload.get('session', ctx.operations.session) == ctx.operations.session
        async with self.lock:
            tx = await ctx.storage.transaction(event=event.payload, keys=[PACKETS, REFUND_NOTICES])
            notices = tx.get(REFUND_NOTICES, {})
            mine = notices.pop(pid, None)
            if mine is not None:
                if notices:
                    tx.set(REFUND_NOTICES, notices)
                else:
                    tx.delete(REFUND_NOTICES)

            lines = []
            if mine:
                for entry in mine:
                    coin = entry.get('currency', '金币')
                    lines.append(f'你发出的 #{entry["id"]} 号红包已过期，剩余 {entry["amount"]} {coin}已退回账户。')

            if is_live:
                packets = tx.get(PACKETS, {'next_id': 1, 'items': []})
                now = self.clock()
                claimable = [pkt for pkt in packets['items'] if is_active(pkt, now) and not has_claimed(pkt, pid)]
                seen_ids = self.reminded_joins.get(pid, set())
                fresh = [pkt for pkt in claimable if pkt['id'] not in seen_ids]
                if claimable:
                    self.reminded_joins[pid] = {pkt['id'] for pkt in claimable}
                else:
                    self.reminded_joins.pop(pid, None)
                if len(self.reminded_joins) > 1000:
                    self.reminded_joins.clear()
                if fresh:
                    if len(fresh) == 1 and not fresh[0]['passcode']:
                        pkt = fresh[0]
                        lines.append(f'当前有 1 个可抢红包（#{pkt["id"]}，{pkt["total"]} 金币），发送 !抢 {pkt["id"]} 领取。')
                    else:
                        lines.append(f'当前有 {len(fresh)} 个可抢红包，发送 !红包 查看详情。')

            if lines:
                try:
                    self.reply(ctx, tx, player, '\n'.join(lines))
                except ValueError:
                    pass
            await tx.save()
            event.defer_ack()
            return True

    async def online_map(self, ctx: PluginContext) -> dict[str, str]:
        try:
            players = await ctx.players()
        except (IPCRejected, ProtocolError, TimeoutError):
            return {}
        return {player_id(p): p['name'] for p in players}

    async def sweep(self, ctx: PluginContext):
        async with self.lock:
            try:
                packets = await ctx.storage.get(PACKETS, {'next_id': 1, 'items': []})
            except (IPCRejected, ProtocolError, TimeoutError) as error:
                ctx.log.warning('redpacket sweep read failed: %s', type(error).__name__)
                return

            now = self.clock()
            items = packets.get('items', [])
            if not items:
                return

            refunded_list = []
            for pkt in items:
                if (pkt['expires_at'] <= now and not pkt['refunded']
                        and not pkt['is_system'] and len(pkt['claims']) < pkt['count']):
                    remaining = sum(pkt['shares'][len(pkt['claims']):])
                    if remaining <= 0:
                        refunded_list.append((pkt['id'], 0, None))
                        continue
                    arguments = {
                        'player_id': pkt['sender_id'],
                        'amount': remaining,
                        'reason': '红包过期退回',
                        'request_id': refund_request_id(pkt['id']),
                    }
                    try:
                        result = await self.call_economy(ctx, ECONOMY_GRANT, arguments)
                    except (IPCRejected, ServiceRejected, *UNSETTLED) as error:
                        ctx.log.warning('redpacket refund failed id=%s: %s', pkt['id'], type(error).__name__)
                        continue
                    refunded_list.append((pkt['id'], remaining, result))

            refunded_ids = {pkt_id for pkt_id, _, _ in refunded_list}
            simulated = [
                {**pkt, 'refunded': True} if pkt['id'] in refunded_ids else pkt
                for pkt in items
            ]
            pruned_preview = prune_items(simulated, now)
            if not refunded_ids and len(pruned_preview) == len(items):
                return

            online_by_id = await self.online_map(ctx) if any(r is not None for _, _, r in refunded_list) else {}
            try:
                tx = await ctx.storage.transaction(keys=[PACKETS, REFUND_NOTICES])
                packets = tx.get(PACKETS, {'next_id': 1, 'items': []})
                notices = tx.get(REFUND_NOTICES, {})
                had_notices = bool(notices)

                by_id = {pkt['id']: pkt for pkt in packets['items']}
                for pkt_id, remaining, result in refunded_list:
                    pkt = by_id.get(pkt_id)
                    if pkt is None:
                        continue
                    pkt['refunded'] = True
                    if result is None:
                        continue
                    coin = result.get('currency', '金币')
                    sender_pid = pkt['sender_id']
                    online_name = online_by_id.get(sender_pid)
                    sent_online = False
                    if online_name is not None and not result.get('pending', False):
                        try:
                            self.reply_name(
                                ctx, tx, online_name,
                                f'你发出的 #{pkt_id} 号红包已过期，剩余 {remaining} {coin}已退回，'
                                f'现有 {result["balance"]} {coin}。',
                                tag=f'refund_{pkt_id}',
                            )
                            sent_online = True
                        except ValueError:
                            sent_online = False
                    if not sent_online:
                        entries = notices.setdefault(sender_pid, [])
                        entries.append({'id': pkt_id, 'amount': remaining, 'currency': coin})
                        notices[sender_pid] = entries[-MAX_REFUND_NOTICES:]

                packets['items'] = prune_items(packets['items'], now)
                tx.set(PACKETS, packets)
                if notices:
                    tx.set(REFUND_NOTICES, notices)
                elif had_notices:
                    tx.delete(REFUND_NOTICES)
                await tx.save()
            except (CommitUncertain, IPCRejected, ProtocolError, TimeoutError) as error:
                ctx.log.warning('redpacket sweep save failed: %s', type(error).__name__)

    # ---- 回复与广播 ----

    async def answer(self, ctx: PluginContext, command, text: str):
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
            self.reply(ctx, tx, command.player, text)
            await tx.save()
            command.event.defer_ack()

    def reply(self, ctx: PluginContext, tx, player: dict, text: str, *, tag: str = 'reply'):
        self.reply_name(ctx, tx, player['name'], text, tag=tag)

    def reply_name(self, ctx: PluginContext, tx, name: str, text: str, *, tag: str = 'reply'):
        plan = ctx.notifications.prepare(name, text,
                                         idempotency_key=f'{tx.commit_id}_{tag}', deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)

    def broadcast(self, ctx: PluginContext, tx, text: str, *, tag: str = 'broadcast'):
        tx.action(ctx.commands.prepare(broadcast_command(text),
                                       idempotency_key=f'{tx.commit_id}_{tag}', deadline=deadline()))


if __name__ == '__main__':
    RedPacket().run()
