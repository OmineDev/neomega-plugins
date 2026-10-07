"""Personal block-centre homes; uncertain game operations are never replayed."""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
import shlex
import time
import unicodedata
import uuid

from neomega_runtime import Plugin
from neomega_runtime.game import CommandResult
from neomega_runtime.http import ProtocolError
from neomega_runtime.legacy_commands import CommandOutcomeError
from neomega_runtime.operations import OperationWaitTimeout, OperationSubmissionError
from neomega_runtime.managed import IPCRejected
from neomega_runtime.storage import CommitUncertain
from community_support import IdentityUnavailable, ObservedRoster, exact_target, recover_actions

DIMENSIONS = {0: 'overworld', 1: 'nether', 2: 'the_end'}


@dataclass(frozen=True)
class Settings:
    enabled: bool = True
    aliases: list[str] = field(default_factory=lambda: ['!home', '!家'], metadata={'minItems': 1, 'maxItems': 8})
    max_homes: int = field(default=3, metadata={'minimum': 1, 'maximum': 100})
    cooldown_seconds: int = field(default=30, metadata={'minimum': 0, 'maximum': 86400})
    warmup_seconds: int = field(default=0, metadata={'minimum': 0, 'maximum': 30})
    allowed_dimensions: list[str] = field(default_factory=lambda: ['overworld'], metadata={'minItems': 1, 'maxItems': 3})
    cross_dimension: bool = False
    check_for_blocks: bool = True

    def __post_init__(self):
        if (not 1 <= len(self.aliases) <= 8 or len(set(self.aliases)) != len(self.aliases)
                or any(not re.fullmatch(r'![^\s!]{1,24}', a) for a in self.aliases)):
            raise ValueError('aliases must be distinct !commands without spaces')
        for value, lo, hi in ((self.max_homes, 1, 100), (self.cooldown_seconds, 0, 86400), (self.warmup_seconds, 0, 30)):
            if type(value) is not int or not lo <= value <= hi:
                raise ValueError('home count or timing outside allowed range')
        if (not self.allowed_dimensions or len(set(self.allowed_dimensions)) != len(self.allowed_dimensions)
                or any(d not in DIMENSIONS.values() for d in self.allowed_dimensions)):
            raise ValueError('invalid dimension allowlist')
        if any(type(x) is not bool for x in (self.enabled, self.cross_dimension, self.check_for_blocks)):
            raise ValueError('feature switches must be booleans')


def home_name(value):
    value = value.strip()
    if not 1 <= len(value) <= 32 or any(unicodedata.category(c).startswith('C') for c in value):
        raise ValueError('地点名需为1至32个字符，不能包含控制字符')
    return value


def moved(a, b, threshold):
    return a['dimension'] != b['dimension'] or math.dist(a['point'], b['point']) > threshold


def feet_block(receipt):
    try:
        result = CommandResult(receipt)
        packet = result.protocol_output()
    except (ProtocolError, CommandOutcomeError):
        raise ValueError('缺少可核对的脚部原始回执，原地点保持不变') from None
    if not result.is_success:
        raise ValueError('未取得脚部空地的成功回执，请站到完整方块上的空地后保存')
    messages = [m for m in packet.get('OutputMessages', [])
                if m.get('Message') == 'commands.testforblock.success' and m.get('Success') is True]
    if len(messages) != 1:
        raise ValueError('脚部坐标回执不唯一，原地点保持不变')
    params = messages[0].get('Parameters')
    if (not isinstance(params, list) or len(params) != 3
            or any(not isinstance(p, str) or not re.fullmatch(r'-?\d+', p) for p in params)):
        raise ValueError('脚部坐标回执缺少三个整数')
    block = tuple(map(int, params))
    if any(abs(v) > 30_000_000 for v in block):
        raise ValueError('脚部坐标超出支持范围')
    data = packet.get('DataSet')
    if data not in (None, ''):
        try:
            decoded = json.loads(data)
            pos = decoded['position']
            actual = tuple(pos[k] for k in ('x', 'y', 'z')) if isinstance(pos, dict) else tuple(pos)
            if len(actual) != 3 or any(type(v) is not int for v in actual) or actual != block:
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            raise ValueError('脚部原始数据与坐标不一致') from None
    return block


def check_stationary(before, after, block):
    if before['dimension'] != after['dimension'] or any(abs(a-b) > .1 for a, b in zip(before['point'], after['point'])):
        raise ValueError('保存时位置变化，请站稳后重新保存')
    if any(math.floor(sample['point'][axis]) != block[axis]
           for sample in (before, after) for axis in (0, 2)):
        raise ValueError('脚部坐标与玩家采样不一致')


def deadline():
    return (datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat()


class PersonalHomes(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.roster = ObservedRoster()
        self.stopping = False
        self.requests = set()

    async def on_start(self, ctx):
        await self.roster.start(ctx)
        # Only active actions need recovery; each index page is bounded to 100 IDs.
        meta = await ctx.storage.get('index:accounts', {'pages': 0})
        for page in range(meta['pages']):
            for player in await ctx.storage.get(f'index:accounts:{page}', []):
                await self.reconcile(ctx, player, restarting=True)
        ctx.every(5, lambda: self.reconcile_all(ctx), name='home-receipts')

    async def on_stop(self, ctx):
        self.stopping = True
        await self.roster.close()

    async def reconcile_all(self, ctx):
        meta = await ctx.storage.get('index:accounts', {'pages': 0})
        for page in range(meta['pages']):
            for player in await ctx.storage.get(f'index:accounts:{page}', []):
                await self.reconcile(ctx, player)

    async def sample(self, ctx, identity):
        self.roster.require_current(identity)
        exact_target(identity.name)
        try:
            raw = await ctx.positions.sample(target='players', name=identity.name, limit=2)
        except (ProtocolError, TimeoutError, OperationSubmissionError, IPCRejected) as error:
            ctx.log.info('home position read unavailable: %s', type(error).__name__)
            raise ValueError('位置读取未确认，请稍后站稳再操作；本次未保存或传送') from None
        self.roster.require_current(identity)
        rows = raw.get('entities', [])
        if raw.get('complete') is not True or len(rows) != 1:
            raise ValueError('无法取得唯一完整玩家位置')
        row = rows[0]
        unique = row.get('uniqueId')
        if not isinstance(unique, str) or not re.fullmatch(r'-?\d+', unique) or int(unique) != identity.entity_unique_id:
            raise ValueError('位置采样的实体身份不匹配')
        point = tuple(row['position'][k] for k in ('x', 'y', 'z'))
        if any(type(v) not in (int, float) or not math.isfinite(v) or abs(v) > 30_000_000 for v in point):
            raise ValueError('玩家坐标不合法')
        if type(row['dimension']) is not int or row['dimension'] not in DIMENSIONS:
            raise ValueError('不支持此维度')
        return {'dimension': row['dimension'], 'point': point}

    def allowed(self, ctx, dimension):
        if DIMENSIONS.get(dimension) not in ctx.config.allowed_dimensions:
            raise ValueError('服主未启用此维度的传送点')

    async def reply(self, ctx, identity, text):
        try:
            self.roster.require_current(identity)
        except IdentityUnavailable:
            ctx.log.info('home response suppressed: identity no longer current')
            return
        key = uuid.uuid4().hex
        plan = ctx.notifications.prepare(identity.name, text, idempotency_key=key, deadline=deadline())
        intents = [part.intent for part in plan.parts]
        record = self.record(key, 'notification', intents)
        async with self.lock:
            tx = await ctx.storage.transaction(commit_id=key, keys=[])
            try:
                self.roster.require_current(identity)
            except IdentityUnavailable:
                ctx.log.info('home response suppressed: identity changed during preparation')
                return
            if self.stopping:
                return
            tx.set('notice:' + key, record)
            for intent in intents:
                tx.action(intent)
            try:
                await tx.save()
            except (CommitUncertain, IPCRejected) as error:
                ctx.log.info('home notification not confirmed: %s commit=%s; no replay', type(error).__name__, key)

    @staticmethod
    def record(key, kind, intents):
        return {'business_key': key, 'commit_id': key, 'kind': kind, 'state': 'pending',
                'actions': [{'intent': i.payload(), 'operation_id': None, 'receipt': None, 'state': 'pending'} for i in intents]}

    async def reconcile(self, ctx, player, restarting=False):
        account_key = 'account:' + player
        async with self.lock:
            account = await ctx.storage.get(account_key, {})
            active = account.get('active')
            if not active:
                return
            record = await ctx.storage.get('action:' + active)
        if not record:
            raise RuntimeError('home active action record missing')
        if record['actions']:
            updated = await recover_actions(ctx, record)
        else:
            updated = dict(record)
        # A save probe must never be finalized after restart with stale samples.
        if restarting and record['kind'] == 'save':
            updated['abandoned'] = True
        if restarting and not record['actions']:
            updated['state'] = 'cancelled'
        if record['kind'] == 'go' and updated['state'] == 'succeeded':
            try:
                confirmed = CommandResult(updated['actions'][0]['receipt']).is_success
            except Exception:
                confirmed = False
            if not confirmed:
                updated['state'] = 'unknown'
        terminal = updated['state'] in ('succeeded', 'failed', 'cancelled')
        if record['kind'] == 'save' and not updated.get('abandoned'):
            terminal = False  # The live save owner validates the second sample.
        async with self.lock:
            tx = await ctx.storage.transaction(keys=[account_key, 'action:' + active])
            account = tx.get(account_key, {})
            current = tx.get('action:' + active)
            if account.get('active') != active or current != record:
                return
            tx.set('action:' + active, updated)
            if terminal:
                account['active'] = None
                if record['kind'] == 'go' and updated['state'] == 'succeeded':
                    account['last_success'] = time.time()
                tx.set(account_key, account)
            await tx.save()

    async def begin(self, ctx, event, identity, key, kind, name=None, replace=False):
        account_key = 'account:' + identity.uuid
        async with self.lock:
            meta = await ctx.storage.get('index:accounts', {'pages': 0})
            page = max(0, meta['pages'] - 1)
            keys = [account_key, 'action:' + key, 'index:accounts', f'index:accounts:{page}']
            tx = await ctx.storage.transaction(event=event.payload, commit_id=key, keys=keys)
            self.roster.require_current(identity)
            if self.stopping or not ctx.config.enabled:
                raise ValueError('插件已关闭，请求取消')
            if tx.get('action:' + key) is not None:
                await event.ack()
                return None
            account = tx.get(account_key, {'homes': {}, 'active': None, 'last_success': 0})
            if account['active']:
                raise ValueError('已有请求处理中或结果未知，请用status查询原请求，勿重复传送')
            if kind == 'save':
                if name in account['homes'] and not replace:
                    raise ValueError('同名地点已存在；覆盖请使用replace 地点名')
                if name not in account['homes'] and len(account['homes']) >= ctx.config.max_homes:
                    raise ValueError('地点数量已达上限；降配额不会删除旧地点')
            if kind == 'go':
                if name not in account['homes']:
                    raise ValueError('该地点不存在')
                remaining = account['last_success'] + ctx.config.cooldown_seconds - time.time()
                if remaining > 0:
                    raise ValueError(f'传送冷却剩余{math.ceil(remaining)}秒')
            record = self.record(key, kind, [])
            record['name'] = name
            record['acked'] = True
            record['event'] = {k: event.payload[k] for k in ('subscription_id', 'event_id', 'delivery_token')}
            if kind == 'go':
                record['destination'] = account['homes'][name]
            if tx.get(account_key) is None:
                members = tx.get(f'index:accounts:{page}', [])
                if len(members) == 100:
                    page += 1
                    members = []
                members.append(identity.uuid)
                tx.set(f'index:accounts:{page}', members)
                tx.set('index:accounts', {'pages': page + 1})
            account['active'] = key
            account['last_request'] = key
            tx.set(account_key, account)
            tx.set('action:' + key, record)
            await tx.save()
            return record

    async def submit(self, ctx, identity, record, command):
        self.roster.require_current(identity)
        key = record['business_key']
        commit = key + '_op'
        intent = ctx.commands.prepare(command, idempotency_key=commit, deadline=deadline(), identity='ws', wait=True, timeout=8)
        async with self.lock:
            tx = await ctx.storage.transaction(commit_id=commit, keys=['action:' + key])
            current = tx.get('action:' + key)
            self.roster.require_current(identity)
            if self.stopping or not ctx.config.enabled:
                raise ValueError('插件已关闭，请求取消')
            if current['actions'] or current['state'] != 'pending':
                raise ValueError('请求已提交或终止，不能重复执行')
            updated = {**current, **self.record(commit, current['kind'], [intent]), 'business_key': key}
            tx.set('action:' + key, updated)
            tx.action(intent)
            await tx.save()
        # No business lock while waiting for the original operation.
        updated = await recover_actions(ctx, updated)
        operation = updated['actions'][0].get('operation_id')
        if operation:
            try:
                await ctx.operations.wait(operation, timeout=12)
            except (OperationWaitTimeout, ProtocolError, IPCRejected) as error:
                ctx.log.info('home operation read unresolved: %s; only reconcile original receipt', type(error).__name__)
        await self.reconcile(ctx, identity.uuid)
        return await ctx.storage.get('action:' + key)

    async def finish(self, ctx, identity, key, *, home=None, cancelled=False):
        async with self.lock:
            account_key = 'account:' + identity.uuid
            prior = await ctx.storage.get('action:' + key)
            ack_event = prior.get('event') if not prior['actions'] and not prior.get('acked') else None
            tx = await ctx.storage.transaction(event=ack_event, keys=[account_key, 'action:' + key])
            account, record = tx.get(account_key), tx.get('action:' + key)
            if account['active'] != key:
                return
            if home is not None:
                self.roster.require_current(identity)
                if self.stopping or not ctx.config.enabled:
                    raise ValueError('插件已关闭，本次地点未保存')
                self.allowed(ctx, home['dimension'])
                account['homes'][record['name']] = home
                record['saved'] = True
            if ack_event:
                record['acked'] = True
            if cancelled:
                record['abandoned'] = True
                if not record['actions']:
                    record['state'] = 'cancelled'
            if not record['actions'] or record['state'] in ('succeeded', 'failed', 'cancelled'):
                account['active'] = None
            tx.set(account_key, account)
            tx.set('action:' + key, record)
            await tx.save()

    async def save_home(self, ctx, event, identity, key, name, replace):
        record = await self.begin(ctx, event, identity, key, 'save', name, replace=replace)
        if record is None:
            return
        try:
            before = await self.sample(ctx, identity)
            self.allowed(ctx, before['dimension'])
            command = f'execute as {exact_target(identity.name)} at @s anchored feet run testforblock ~ ~ ~ air'
            result = await self.submit(ctx, identity, record, command)
            if result['state'] != 'succeeded':
                raise ValueError('脚部查询未确认成功，原地点保持不变；status可核对原操作')
            block = feet_block(result['actions'][0]['receipt'])
            after = await self.sample(ctx, identity)
            check_stationary(before, after, block)
            self.roster.require_current(identity)
            await self.finish(ctx, identity, key, home={'dimension': before['dimension'],
                              'position': [block[0]+.5, block[1], block[2]+.5], 'updated_at': time.time()})
            await self.reply(ctx, identity, f'已按方块中心保存地点「{name}」。')
        except ValueError:
            await self.finish(ctx, identity, key, cancelled=True)
            raise

    async def go_home(self, ctx, event, identity, key, name):
        record = await self.begin(ctx, event, identity, key, 'go', name)
        if record is None:
            return
        try:
            destination = record['destination']
            self.allowed(ctx, destination['dimension'])
            before = await self.sample(ctx, identity)
            self.allowed(ctx, before['dimension'])
            if not ctx.config.cross_dimension and before['dimension'] != destination['dimension']:
                raise ValueError('跨维度传送未启用')
            if ctx.config.warmup_seconds:
                await self.reply(ctx, identity, f'{ctx.config.warmup_seconds}秒后传送，移动超过0.5格将取消。')
            for _ in range(ctx.config.warmup_seconds):
                await asyncio.sleep(1)
                if self.stopping or not ctx.config.enabled:
                    raise ValueError('插件停止或关闭，倒计时已取消')
                if moved(before, await self.sample(ctx, identity), .5):
                    raise ValueError('位置变化，传送已取消')
            current = await self.sample(ctx, identity)
            if ctx.config.warmup_seconds and moved(before, current, .5):
                raise ValueError('位置变化，传送已取消')
            if not ctx.config.cross_dimension and current['dimension'] != destination['dimension']:
                raise ValueError('当前位置维度已变化')
            self.allowed(ctx, current['dimension'])
            if self.stopping or not ctx.config.enabled:
                raise ValueError('插件已关闭')
            coords = ' '.join(format(v, '.1f') for v in destination['position'])
            command = f"execute in {DIMENSIONS[destination['dimension']]} run teleport {exact_target(identity.name)} {coords} {str(ctx.config.check_for_blocks).lower()}"
            result = await self.submit(ctx, identity, record, command)
            await self.reply(ctx, identity, '传送成功。' if result['state'] == 'succeeded' else f"传送结果：{result['state']}；使用status核对原操作，不自动重试。")
        except ValueError:
            await self.finish(ctx, identity, key, cancelled=True)
            raise

    async def mutate(self, ctx, event, identity, key, verb, names):
        account_key = 'account:' + identity.uuid
        async with self.lock:
            tx = await ctx.storage.transaction(event=event.payload, commit_id=key, keys=[account_key, 'action:' + key])
            self.roster.require_current(identity)
            if self.stopping or not ctx.config.enabled:
                raise ValueError('插件已关闭，请求取消')
            if tx.get('action:' + key) is not None:
                await event.ack()
                return
            account = tx.get(account_key, {'homes': {}, 'active': None, 'last_success': 0})
            if account['active']:
                active = await ctx.storage.get('action:' + account['active'])
                if active['kind'] == 'save':
                    raise ValueError('正在保存地点，请等待原保存请求结束')
            self.roster.require_current(identity)
            if self.stopping or not ctx.config.enabled:
                raise ValueError('插件已关闭，请求取消')
            source = names[0]
            if source not in account['homes']:
                raise ValueError('该地点不存在')
            if verb == 'rename':
                if names[1] in account['homes']:
                    raise ValueError('目标地点名已存在')
                account['homes'][names[1]] = account['homes'].pop(source)
            else:
                del account['homes'][source]
            tx.set(account_key, account)
            tx.set('action:' + key, {'state': 'succeeded', 'kind': verb})
            await tx.save()
        await self.reply(ctx, identity, '地点已重命名。' if verb == 'rename' else '地点已删除。')

    async def ack(self, event):
        async with self.lock:
            await event.ack()

    async def on_event(self, ctx, event):
        if self.stopping:
            await self.ack(event)
            return
        payload = event.payload.get('payload', {})
        text = payload.get('message', '')
        if (event.kind == 'chat.received' and isinstance(text, str)
                and text.split(maxsplit=1)[:1] in [[a] for a in ctx.config.aliases]):
            # SDK has a bounded background-task pool (32 by default); leave
            # room for roster, receipt timer and their drain operations.
            if len(self.requests) >= 16:
                await self.ack(event)
                try:
                    identity = self.roster.resolve_chat(payload)
                except IdentityUnavailable:
                    ctx.log.info('busy home request rejected: identity unavailable')
                else:
                    await self.reply(ctx, identity, '当前请求较多，请稍后再试。')
                return
            task = ctx.spawn(self.handle_event(ctx, event), name='home-request')
            self.requests.add(task)
            task.add_done_callback(self.requests.discard)
        else:
            await self.ack(event)

    async def handle_event(self, ctx, event):
        if event.kind != 'chat.received':
            await self.ack(event)
            return
        payload = event.payload.get('payload', {})
        text = payload.get('message', '')
        if not isinstance(text, str) or text.split(maxsplit=1)[:1] not in [[a] for a in ctx.config.aliases]:
            await self.ack(event)
            return
        identity = None
        key = 'home_' + hashlib.sha256(str(event.payload['event_id']).encode()).hexdigest()
        try:
            identity = self.roster.resolve_chat(payload)
            if self.stopping or not ctx.config.enabled:
                await self.ack(event)
                await self.reply(ctx, identity, '个人传送点已关闭。')
                return
            if len(text.encode()) > 512:
                raise ValueError('指令过长')
            args = shlex.split(text)
            verb = args[1] if len(args) > 1 else 'help'
            await self.reconcile(ctx, identity.uuid)
            # Deduplicate before any business validation or position probe.
            if await ctx.storage.get('action:' + key) is not None:
                await self.ack(event)
                return
            if verb in ('save', 'replace', 'go', 'delete') and len(args) == 3:
                name = home_name(args[2])
                if verb in ('save', 'replace'):
                    await self.save_home(ctx, event, identity, key, name, verb == 'replace')
                elif verb == 'go':
                    await self.go_home(ctx, event, identity, key, name)
                else:
                    await self.mutate(ctx, event, identity, key, verb, [name])
            elif verb == 'rename' and len(args) == 4:
                await self.mutate(ctx, event, identity, key, verb, [home_name(a) for a in args[2:]])
            else:
                await self.ack(event)
                account = await ctx.storage.get('account:' + identity.uuid, {'homes': {}, 'active': None})
                if verb == 'list' and len(args) == 2:
                    entries = [f'{n} ({DIMENSIONS[h["dimension"]]})' for n,h in account['homes'].items()]
                    # Reply in bounded chunks, not one oversized notification.
                    for start in range(0, max(1, len(entries)), 5):
                        await self.reply(ctx, identity, '地点：' + ('、'.join(entries[start:start+5]) or '无'))
                elif verb == 'status' and len(args) == 2:
                    active = account['active'] or account.get('last_request')
                    action = await ctx.storage.get('action:' + active) if active else None
                    if action:
                        ids = ','.join(a.get('operation_id') or '待查回' for a in action['actions'])
                        state = '地点已保存' if action.get('saved') else action['state']
                        await self.reply(ctx, identity, f"原请求 {active}：{state}；操作 {ids or '未提交'}，不自动重试。")
                    else:
                        await self.reply(ctx, identity, '当前没有请求记录。')
                else:
                    await self.reply(ctx, identity, '用法：!home save/replace/go/delete 名称；rename 旧名 新名；list；status。带空格名称用引号。保存按方块中心，请站到完整方块上的空地；不支持脚部非空气状态。')
        except ValueError as error:
            # Mutating requests already ACKed in begin/mutate; rejected requests
            # are ACKed exactly once here. Never resubmit a world operation.
            if await ctx.storage.get('action:' + key) is None:
                await self.ack(event)
            if identity is not None:
                try:
                    await self.reply(ctx, identity, str(error))
                except ValueError:
                    ctx.log.info('home response suppressed: identity unavailable')
            else:
                ctx.log.info('home request rejected: reliable observed identity required')


if __name__ == '__main__':
    PersonalHomes().run()
