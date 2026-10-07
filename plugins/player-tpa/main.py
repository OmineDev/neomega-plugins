"""Native consent-based player teleportation; uncertain actions are never replayed."""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import math
import shlex
import time
import uuid

from neomega_runtime import Plugin
from neomega_runtime.storage import CommitUncertain
from neomega_runtime.http import ProtocolError
from neomega_runtime.managed import IPCRejected
from community_support import ObservedRoster, IdentityUnavailable, exact_target, recover_actions


def deadline():
    return (datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat()


@dataclass(frozen=True)
class Settings:
    enabled: bool = True
    aliases: list[str] = field(default_factory=lambda: ['tpa'])
    expiry_seconds: int = 60
    cooldown_seconds: int = 30
    incoming_limit: int = 5
    warmup_seconds: int = 0
    same_dimension_only: bool = True
    max_distance: float = 0
    check_for_blocks: bool = True

    def __post_init__(self):
        for name in ('enabled', 'same_dimension_only', 'check_for_blocks'):
            if type(getattr(self, name)) is not bool:
                raise ValueError(name + ' must be boolean')
        for name, low, high in [('expiry_seconds', 5, 600), ('cooldown_seconds', 0, 3600),
                                ('incoming_limit', 1, 20), ('warmup_seconds', 0, 30)]:
            value = getattr(self, name)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(name + ' out of range')
        if (type(self.max_distance) not in (int, float) or not math.isfinite(self.max_distance)
                or not 0 <= self.max_distance <= 30000000):
            raise ValueError('max_distance out of range')
        if self.max_distance and not self.same_dimension_only:
            raise ValueError('max_distance requires same_dimension_only')
        if (not isinstance(self.aliases, list) or not 1 <= len(self.aliases) <= 8
                or len(set(self.aliases)) != len(self.aliases)
                or any(not isinstance(a, str) or not a.isascii() or not a.isalnum()
                       or not 1 <= len(a) <= 24 or a != a.lower() for a in self.aliases)):
            raise ValueError('aliases must be 1..8 unique lowercase alphanumeric words')


@dataclass
class Request:
    id: str
    sender: object
    receiver: object
    direction: str
    expires: float
    state: str = 'pending'


class PlayerTPA(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.roster = ObservedRoster()
        self.requests = {}
        self.cooldowns = {}
        self.unresolved = set()
        self.uncertain = {}
        self.accept_tasks = {}

    async def on_start(self, ctx):
        await self.roster.start(ctx)
        # Recover all persisted action IDs through bounded pages, also when disabled.
        pages = await ctx.storage.get('index:actions:count', 0)
        for page in range(pages):
            for request_id in await ctx.storage.get('index:actions:' + str(page), []):
                record = await ctx.storage.get('action:' + request_id)
                if not record or record['state'] not in ('succeeded', 'failed'):
                    self.unresolved.add(request_id)
        await self.reconcile(ctx)
        ctx.every(5, lambda: self.reconcile(ctx), name='tpa-receipts')
        ctx.every(1, lambda: self.expire(ctx), name='tpa-expiry')

    async def on_stop(self, ctx):
        self.requests.clear()
        await self.roster.close()

    async def expire(self, ctx):
        async with self.lock:
            now = time.monotonic()
            for key, request in tuple(self.requests.items()):
                if not ctx.config.enabled or request.expires <= now:
                    request.state = 'expired' if ctx.config.enabled else 'cancelled'
                    del self.requests[key]
            self.cooldowns = {key: until for key, until in self.cooldowns.items() if until > now}

    async def reply(self, ctx, event, identity, message):
        # Notifications are informational and never retried on an ambiguous receipt.
        async with self.lock:
            self.roster.require_current(identity)
            key = uuid.uuid4().hex
            plan = ctx.notifications.prepare(identity.name, message,
                                             idempotency_key=key, deadline=deadline())
            tx = await ctx.storage.transaction(event=event.payload if event else None, commit_id=key, keys=[])
            for part in plan.parts:
                tx.action(part.intent)
            await tx.save()

    def target(self, name):
        exact_target(name)
        matches = [p for p in self.roster.snapshot() if p.name == name]
        if len(matches) != 1:
            raise ValueError('目标不在线或身份不唯一。')
        return matches[0]

    async def position(self, ctx, identity):
        self.roster.require_current(identity)
        exact_target(identity.name)
        try:
            result = await ctx.positions.sample(target='players', name=identity.name, limit=2)
        except (ProtocolError, TimeoutError, IPCRejected) as exc:
            ctx.log.warning('TPA position query failed: %s', exc)
            raise ValueError('位置查询失败或超时，当前互传已取消。') from exc
        self.roster.require_current(identity)
        rows = result['entities']
        if not result['complete'] or len(rows) != 1:
            raise ValueError('无法确认玩家位置。')
        row = rows[0]
        if int(row['uniqueId']) != identity.entity_unique_id:
            raise ValueError('位置与玩家身份不一致。')
        point = tuple(row['position'][axis] for axis in ('x', 'y', 'z'))
        if type(row['dimension']) is not int or any(type(n) not in (int, float) or not math.isfinite(n) for n in point):
            raise ValueError('位置格式无效。')
        return row['dimension'], point

    async def positions(self, ctx, request):
        a = await self.position(ctx, request.sender)
        b = await self.position(ctx, request.receiver)
        self.roster.require_current(request.sender)
        self.roster.require_current(request.receiver)
        if ctx.config.same_dimension_only and a[0] != b[0]:
            raise ValueError('只允许同维度互传。')
        if ctx.config.max_distance and math.dist(a[1], b[1]) > ctx.config.max_distance:
            raise ValueError('超过允许互传距离。')
        return a, b

    async def reconcile(self, ctx):
        for request_id in tuple(self.unresolved):
            key = 'action:' + request_id
            async with self.lock:
                record = await ctx.storage.get(key)
                record = record or self.uncertain.get(request_id)
            if not record:
                raise RuntimeError('indexed TPA action missing: ' + request_id)
            updated = await recover_actions(ctx, record)
            async with self.lock:
                tx = await ctx.storage.transaction(keys=[key] + ['account:' + u for u in record['participants']])
                # An unconfirmed initial transaction may not exist; never fabricate it.
                current = tx.get(key)
                if not current:
                    self.uncertain[request_id] = updated
                    continue
                if current['commit_id'] != record['commit_id']:
                    raise RuntimeError('TPA action commit changed')
                tx.set(key, updated)
                if updated['state'] in ('succeeded', 'failed'):
                    for user in record['participants']:
                        account_key = 'account:' + user
                        account = tx.get(account_key, {})
                        if account.get('active') == request_id:
                            account.pop('active')
                            tx.set(account_key, account)
                await tx.save()
                if updated['state'] in ('succeeded', 'failed'):
                    self.unresolved.discard(request_id)
                    self.uncertain.pop(request_id, None)
                    ctx.log.info('TPA %s: %s', request_id, updated['state'])

    async def create(self, ctx, event, actor, direction, name):
        target = self.target(name)
        if actor.uuid == target.uuid:
            raise ValueError('不能向自己发送请求。')
        async with self.lock:
            self.roster.require_current(actor)
            self.roster.require_current(target)
            now = time.monotonic()
            if self.cooldowns.get(actor.uuid, 0) > now:
                raise ValueError('发起请求冷却尚未结束。')
            account = await ctx.storage.get('account:' + target.uuid, {})
            if not account.get('receive', True) or actor.uuid in account.get('blocked', {}):
                raise ValueError('对方当前不接收你的请求。')
            incoming = [r for r in self.requests.values() if r.receiver.uuid == target.uuid and r.expires > now]
            outgoing = [r for r in self.requests.values() if r.sender.uuid == actor.uuid and r.expires > now]
            if len(incoming) >= ctx.config.incoming_limit or len(outgoing) >= ctx.config.incoming_limit:
                raise ValueError('待处理请求数量已达上限。')
            if any(r.receiver.uuid == target.uuid and r.direction == direction for r in outgoing):
                raise ValueError('已有相同请求，请等待处理或先取消。')
            request_id = uuid.uuid4().hex
            request = Request(request_id, actor, target, direction, now + ctx.config.expiry_seconds)
            # ACK must be confirmed before exposing the temporary request.
            await event.ack()
            self.requests[request_id] = request
            self.cooldowns[actor.uuid] = now + ctx.config.cooldown_seconds
        await self.reply(ctx, None, actor, '请求已发送，ID：' + request_id)
        action = '传送到你身边' if direction == 'to' else '邀请你传送过去'
        await self.reply(ctx, None, target, actor.name + ' 请求' + action + '。输入 !' + ctx.config.aliases[0]
                         + ' accept ' + request_id + ' 接受，或 deny ' + request_id + ' 拒绝。')

    async def preference(self, ctx, event, actor, args):
        async with self.lock:
            key = 'account:' + actor.uuid
            tx = await ctx.storage.transaction(event=event.payload, keys=[key])
            account = tx.get(key, {})
            blocked = account.setdefault('blocked', {})
            if args[0] == 'receive' and len(args) == 2 and args[1] in ('on', 'off'):
                account['receive'] = args[1] == 'on'
                if args[1] == 'off':
                    for rid, request in tuple(self.requests.items()):
                        if request.receiver.uuid == actor.uuid:
                            request.state = 'cancelled'
                            del self.requests[rid]
            elif args[0] == 'block' and len(args) == 2:
                target = self.target(args[1])
                if target.uuid not in blocked and len(blocked) >= 100:
                    raise ValueError('屏蔽名单最多100人。')
                blocked[target.uuid] = target.name
                for rid, request in tuple(self.requests.items()):
                    if request.receiver.uuid == actor.uuid and request.sender.uuid == target.uuid:
                        request.state = 'cancelled'
                        del self.requests[rid]
            elif args[0] == 'unblock' and len(args) == 2:
                matches = [u for u, name in blocked.items() if args[1] in (u, name)]
                if len(matches) != 1:
                    raise ValueError('请使用 blocks 列表中的准确 UUID 解除屏蔽。')
                del blocked[matches[0]]
            else:
                raise ValueError('用法：receive on|off，block 名字，unblock UUID。')
            tx.set(key, account)
            await tx.save()
        await self.reply(ctx, None, actor, '互传偏好已保存。')

    async def accept(self, ctx, event, actor, request_id):
        async with self.lock:
            request = self.requests.get(request_id)
            if (not request or request.receiver.uuid != actor.uuid or request.state != 'pending'
                    or request.expires <= time.monotonic()):
                raise ValueError('请求不存在、已过期或不属于你。')
            self.roster.require_current(request.sender)
            self.roster.require_current(request.receiver)
            request.state = 'warming'
        try:
            initial = None
            if ctx.config.same_dimension_only or ctx.config.max_distance or ctx.config.warmup_seconds:
                initial = await self.positions(ctx, request)
            if ctx.config.warmup_seconds:
                await self.reply(ctx, None, actor, str(ctx.config.warmup_seconds) + '秒后传送，双方请勿移动。')
                for _ in range(ctx.config.warmup_seconds):
                    await asyncio.sleep(1)
                    current = await self.positions(ctx, request)
                    if any(a[0] != b[0] or math.dist(a[1], b[1]) > .5 for a, b in zip(initial, current)):
                        raise ValueError('玩家移动，互传已取消。')
                await self.positions(ctx, request)
            async with self.lock:
                if not ctx.config.enabled or request.state != 'warming' or request.expires <= time.monotonic():
                    raise ValueError('请求已取消或过期。')
                participants = [request.sender.uuid, request.receiver.uuid]
                keys = ['action:' + request_id, 'index:actions:count'] + ['account:' + u for u in participants]
                count = await ctx.storage.get('index:actions:count', 0)
                page_key = 'index:actions:' + str(max(0, count - 1))
                commit_id = uuid.uuid4().hex
                tx = await ctx.storage.transaction(event=event.payload, commit_id=commit_id, keys=keys + [page_key])
                if tx.get('action:' + request_id):
                    await event.ack()
                    return
                for user in participants:
                    account = tx.get('account:' + user, {})
                    if account.get('active') or any(user in row['participants'] for row in self.uncertain.values()):
                        raise ValueError('玩家有未核对的传送，暂不能再次传送。')
                receiver_preferences = tx.get('account:' + request.receiver.uuid, {})
                if (not receiver_preferences.get('receive', True)
                        or request.sender.uuid in receiver_preferences.get('blocked', {})):
                    raise ValueError('接收偏好已改变，请求取消。')
                self.roster.require_current(request.sender)
                self.roster.require_current(request.receiver)
                source, destination = (request.sender, request.receiver) if request.direction == 'to' else (request.receiver, request.sender)
                intent = ctx.player_actions.teleport_to(source.name, destination.name, idempotency_key=request_id,
                                                        deadline=deadline(), check_for_blocks=ctx.config.check_for_blocks)
                record = dict(business_key=request_id, commit_id=commit_id, participants=participants,
                              actions=[dict(intent=intent.payload(), operation_id=None, receipt=None, state='pending')], state='pending')
                page = tx.get(page_key, [])
                if len(page) == 100:
                    page_key = 'index:actions:' + str(count)
                    page = []
                    count += 1
                tx.set(page_key, page + [request_id])
                tx.set('index:actions:count', max(1, count))
                tx.set('action:' + request_id, record)
                for user in participants:
                    account = tx.get('account:' + user, {})
                    account['active'] = request_id
                    tx.set('account:' + user, account)
                tx.action(intent)
                self.unresolved.add(request_id)
                try:
                    await tx.save()
                except CommitUncertain:
                    self.uncertain[request_id] = record
                except Exception:
                    self.unresolved.discard(request_id)
                    raise
                request.state = 'accepted'
                self.requests.pop(request_id, None)
            await self.reply(ctx, None, actor, '传送已提交，输入 !' + ctx.config.aliases[0] + ' status ' + request_id + ' 查询回执。')
        finally:
            async with self.lock:
                if request.state == 'warming':
                    request.state = 'cancelled'
                    self.requests.pop(request_id, None)

    async def queue_accept(self, ctx, event, actor, request_id):
        async with self.lock:
            request = self.requests.get(request_id)
            if (not request or request.receiver.uuid != actor.uuid or request.state != 'pending'
                    or request.expires <= time.monotonic()):
                raise ValueError('请求不存在、已过期或不属于你。')
            if request_id in self.accept_tasks:
                raise ValueError('该请求正在处理中。')
            # Leave headroom below the SDK's 32 tasks for roster and two timers.
            if len(self.accept_tasks) >= 16:
                raise ValueError('当前互传繁忙，请稍后在请求过期前接受。')
            task = ctx.spawn(self.accept_task(ctx, event, actor, request_id),
                             name='tpa-accept-' + request_id)
            self.accept_tasks[request_id] = task
            task.add_done_callback(lambda done: self.accept_tasks.pop(request_id, None))

    async def accept_task(self, ctx, event, actor, request_id):
        try:
            await self.accept(ctx, event, actor, request_id)
        except IdentityUnavailable:
            async with self.lock:
                await event.ack()
            ctx.log.warning('TPA cancelled because identity changed')
        except ValueError as exc:
            await self.reply(ctx, event, actor, str(exc))

    async def on_event(self, ctx, event):
        if event.kind != 'chat.received':
            async with self.lock:
                await event.ack()
            return
        payload = event.payload['payload']
        message = payload.get('message', '')
        prefix = message.split(maxsplit=1)[0] if message else ''
        if prefix not in ['!' + alias for alias in ctx.config.aliases]:
            async with self.lock:
                await event.ack()
            return
        try:
            actor = self.roster.resolve_chat(payload)
        except ValueError:
            ctx.log.warning('TPA ignored chat without verified identity')
            async with self.lock:
                await event.ack()
            return
        try:
            args = shlex.split(message)[1:]
            if not ctx.config.enabled:
                await self.reply(ctx, event, actor, '互传功能已关闭。')
            elif not args:
                await self.reply(ctx, event, actor, '互传：to|here 名字；list；accept|deny|cancel ID；receive on|off；block|unblock 名字；blocks；status ID。名字含空格请加引号。')
            elif args[0] in ('to', 'here') and len(args) == 2:
                await self.create(ctx, event, actor, args[0], args[1])
            elif args[0] == 'accept' and len(args) == 2:
                await self.queue_accept(ctx, event, actor, args[1])
            elif args[0] in ('deny', 'cancel') and len(args) == 2:
                async with self.lock:
                    request = self.requests.get(args[1])
                    owner = request.receiver if request and args[0] == 'deny' else request.sender if request else None
                    if not owner or owner.uuid != actor.uuid:
                        raise ValueError('请求不存在或不属于你。')
                    await event.ack()
                    request.state = 'denied' if args[0] == 'deny' else 'cancelled'
                    del self.requests[args[1]]
                await self.reply(ctx, None, actor, '请求已' + ('拒绝。' if args[0] == 'deny' else '取消。'))
            elif args[0] == 'list' and len(args) == 1:
                async with self.lock:
                    rows = [r.id + ' ' + r.direction + ' ' + r.sender.name + ' → ' + r.receiver.name
                            for r in self.requests.values() if actor.uuid in (r.sender.uuid, r.receiver.uuid) and r.expires > time.monotonic()]
                await self.reply(ctx, event, actor, '\n'.join(rows) or '没有待处理请求。')
            elif args[0] == 'blocks' and len(args) == 1:
                account = await ctx.storage.get('account:' + actor.uuid, {})
                rows = [u + ' ' + name for u, name in account.get('blocked', {}).items()]
                await self.reply(ctx, event, actor, '\n'.join(rows) or '没有屏蔽玩家。')
            elif args[0] == 'status' and len(args) == 2:
                record = await ctx.storage.get('action:' + args[1])
                record = record or self.uncertain.get(args[1])
                if not record or actor.uuid not in record['participants']:
                    raise ValueError('没有属于你的该请求回执。')
                states = {'pending': '等待回执', 'unknown': '结果未知，请管理员核对，禁止重试', 'partial': '结果不完整，请管理员核对', 'succeeded': '已成功', 'failed': '已失败'}
                await self.reply(ctx, event, actor, states[record['state']])
            elif args[0] in ('receive', 'block', 'unblock'):
                await self.preference(ctx, event, actor, args)
            else:
                raise ValueError('命令格式错误，输入 !' + ctx.config.aliases[0] + ' 查看帮助。')
        except IdentityUnavailable:
            async with self.lock:
                await event.ack()
            ctx.log.warning('TPA identity changed during request')
        except ValueError as exc:
            await self.reply(ctx, event, actor, str(exc))


if __name__ == '__main__':
    PlayerTPA().run()
