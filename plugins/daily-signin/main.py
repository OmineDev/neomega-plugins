"""Native natural-day sign-in. Game effects are immutable, receipt-led claims."""
import asyncio
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
from string import Formatter
from zoneinfo import ZoneInfo

from neomega_runtime import Plugin
from neomega_runtime.managed import IPCRejected
from neomega_runtime.storage import CommitUncertain
from community_support import ObservedRoster, exact_target, config_fingerprint, recover_actions


@dataclass(frozen=True)
class Reward:
    kind: str = field(default='item', metadata={'enum': ['item', 'scoreboard', 'command']})
    item: str = 'minecraft:apple'
    count: int = field(default=1, metadata={'minimum': 1, 'maximum': 32767})
    data: int = field(default=0, metadata={'minimum': 0, 'maximum': 2147483647})
    objective: str = ''
    amount: int = field(default=1, metadata={'minimum': 1, 'maximum': 2147483647})
    command: str = ''

    def __post_init__(self):
        if self.kind not in ('item', 'scoreboard', 'command'):
            raise ValueError('reward kind must be item, scoreboard or command')
        if self.kind == 'item' and (len(self.item) > 256 or not re.fullmatch(r'(?:[a-z0-9_.-]+:)?[a-z0-9_./-]+', self.item)):
            raise ValueError('invalid item identifier')
        if self.kind == 'scoreboard' and not re.fullmatch(r'[A-Za-z0-9_.-]{1,16}', self.objective):
            raise ValueError('invalid scoreboard objective')
        if self.kind == 'command':
            if not self.command.strip() or len(self.command.encode()) > 4096 or any(c in self.command for c in '\r\n\x00'):
                raise ValueError('command must be one nonempty line up to 4096 bytes')
            for _, name, spec, conversion in Formatter().parse(self.command):
                if name is not None and (name != 'player' or spec or conversion):
                    raise ValueError('only plain {player} is supported')


@dataclass(frozen=True)
class Rewards:
    enabled: bool = False
    base: list[Reward] = field(default_factory=list, metadata={'maxItems': 16})

    def __post_init__(self):
        if len(self.base) > 16:
            raise ValueError('at most 16 base rewards')
        if self.enabled and not self.base:
            raise ValueError('enabled rewards require a base reward')


@dataclass(frozen=True)
class Milestone:
    rule_id: str
    count: int = field(metadata={'minimum': 1})
    rewards: list[Reward] = field(metadata={'minItems': 1, 'maxItems': 16})

    def __post_init__(self):
        if not re.fullmatch(r'[a-z0-9_-]{1,48}', self.rule_id) or not 1 <= len(self.rewards) <= 16 or self.count < 1:
            raise ValueError('invalid milestone')


@dataclass(frozen=True)
class Milestones:
    enabled: bool = False
    rules: list[Milestone] = field(default_factory=list, metadata={'maxItems': 32})

    def __post_init__(self):
        if len(self.rules) > 32:
            raise ValueError('at most 32 milestone rules per kind')


@dataclass(frozen=True)
class History:
    enabled: bool = True


@dataclass(frozen=True)
class Announcement:
    enabled: bool = False


@dataclass(frozen=True)
class Settings:
    enabled: bool = True
    timezone: str = 'Asia/Shanghai'
    aliases: list[str] = field(default_factory=lambda: ['!签到', '!signin'], metadata={'minItems': 1, 'maxItems': 8})
    rewards: Rewards = field(default_factory=Rewards)
    consecutive: Milestones = field(default_factory=Milestones)
    cumulative: Milestones = field(default_factory=Milestones)
    announcement: Announcement = field(default_factory=Announcement)
    history: History = field(default_factory=History)

    def __post_init__(self):
        ZoneInfo(self.timezone)
        if not 1 <= len(self.aliases) <= 8 or len(set(self.aliases)) != len(self.aliases) or any(
                not v.startswith('!') or len(v) > 32 or len(v.split()) != 1 or any(ord(c) < 32 for c in v)
                for v in self.aliases):
            raise ValueError('aliases must be unique !commands')
        rules = self.consecutive.rules + self.cumulative.rules
        if len({r.rule_id for r in rules}) != len(rules):
            raise ValueError('rule_id must be unique across milestone kinds')
        # Any pair of milestone counts may coincide for some account.
        maximum = len(self.rewards.base) if self.rewards.enabled else 0
        for group in (self.consecutive, self.cumulative):
            totals = {}
            for rule in group.rules if group.enabled and self.rewards.enabled else []:
                totals[rule.count] = totals.get(rule.count, 0) + len(rule.rewards)
            maximum += max(totals.values(), default=0)
        if maximum + int(self.announcement.enabled) > 16:
            raise ValueError('a sign-in may contain at most 16 actions including announcement')


def now():
    return datetime.now(timezone.utc)


def claim_key(player_uuid, day):
    return f'day:{player_uuid}:{day}'


def next_account(account, day, streak_id):
    """Pure accepted-sign-in progression; rejects wall-clock rollback."""
    last = account.get('last_day')
    if last and day <= last:
        raise ValueError('该日期已签到或早于最后签到日期；不会重复计数。')
    continuous = last and date.fromisoformat(day) - date.fromisoformat(last) == timedelta(days=1)
    total = account.get('total_count', 0) + 1
    streak = account.get('streak_count', 0) + 1 if continuous else 1
    return dict(account, last_day=day, total_count=total, streak_count=streak,
                best_streak=max(account.get('best_streak', 0), streak),
                streak_id=account['streak_id'] if continuous else streak_id,
                history=(account.get('history', []) + [dict(day=day, total=total, streak=streak)])[-30:])


def selected_rewards(config, account):
    if not config.rewards.enabled:
        return []
    chosen = [('base', reward) for reward in config.rewards.base]
    for kind, group, count in (('consecutive', config.consecutive, account['streak_count']),
                               ('cumulative', config.cumulative, account['total_count'])):
        if group.enabled:
            for rule in group.rules:
                if rule.count == count:
                    chosen.extend((kind + ':' + rule.rule_id, reward) for reward in rule.rewards)
    return chosen


def prepare_reward(ctx, player, reward, key, deadline):
    options = dict(idempotency_key=key, deadline=deadline)
    if reward.kind == 'item':
        return ctx.player_actions.give(player.name, reward.item, count=reward.count, data=reward.data, **options)
    if reward.kind == 'scoreboard':
        return ctx.scoreboard.prepare_add(player.name, reward.objective, reward.amount, **options)
    return ctx.commands.prepare(reward.command.format(player=exact_target(player.name)), **options)


def describe(record, *, include_ids=False):
    states = {'pending': '等待回执', 'succeeded': '已确认', 'failed': '失败（不自动补发）',
              'partial': '部分完成（待核对）', 'unknown': '结果未知（待核对，勿重复领取）'}
    prefix = f"{record['day']} 已记录；累计 {record['total_count']} 次，连签 {record['streak_count']} 天"
    if record.get('archived'):
        return prefix + '；详细回执已清理，不会补发。'
    rewards = [a for a in record['actions'] if a['role'] != 'announcement']
    if not rewards:
        return prefix + '；本次纯记账。'
    return prefix + '；' + '；'.join(
        f"奖励{i + 1} {states.get(a['state'], a['state'])}" +
        (f" operation={a['operation_id']}" if include_ids and a.get('operation_id') else '')
        for i, a in enumerate(rewards))


class DailySignin(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.roster = ObservedRoster()
        self.scan_page = 0
        self.unresolved_commit = None

    async def on_start(self, ctx):
        await self.install_rules(ctx)
        await self.roster.start(ctx)
        ctx.every(10, lambda: self.reconcile_page(ctx), immediate=True, name='signin-receipts')

    async def on_stop(self, ctx):
        await self.roster.close()

    async def install_rules(self, ctx):
        rules = [(kind, rule) for kind, group in (('consecutive', ctx.config.consecutive),
                                                ('cumulative', ctx.config.cumulative)) for rule in group.rules]
        keys = ['settings:timezone'] + ['rule:' + r.rule_id for _, r in rules]
        async with self.lock:
            tx = await ctx.storage.transaction(keys=keys)
            zone = tx.get('settings:timezone')
            if zone is not None and zone != ctx.config.timezone:
                raise ValueError('timezone is immutable after initialization; migration required')
            tx.set('settings:timezone', ctx.config.timezone)
            for kind, rule in rules:
                key = 'rule:' + rule.rule_id
                fingerprint = config_fingerprint(dict(kind=kind, **asdict(rule)))
                old = tx.get(key)
                if old is not None and old != fingerprint:
                    raise ValueError('milestone rule_id cannot be reused with different rewards/count/kind')
                tx.set(key, fingerprint)
            await tx.save()

    async def accept(self, ctx, event, player):
        async with self.lock:
            if self.unresolved_commit is not None:
                raise RuntimeError('previous commit unresolved; restart only after receipt reconciliation')
            # A revision rejection is the only branch allowed to recalculate.
            for attempt in range(4):
                day = now().astimezone(ZoneInfo(ctx.config.timezone)).date().isoformat()
                key = claim_key(player.uuid, day)
                account_key = 'account:' + player.uuid
                head = await ctx.storage.get('index:head', 0)
                page_key = f'index:claims:{head}'
                commit_id = 'signin_' + hashlib.sha256(key.encode()).hexdigest()
                event_key = 'event:' + hashlib.sha256((event.payload['subscription_id'] + ':' + event.payload['event_id']).encode()).hexdigest()
                tx = await ctx.storage.transaction(event=event.payload, commit_id=commit_id,
                                                   keys=[key, account_key, 'index:head', page_key, event_key])
                if tx.get('index:head', 0) != head:
                    continue
                previous = tx.get(event_key)
                if previous is not None:
                    await event.ack()
                    saved = await ctx.storage.get(previous['business_key'])
                    if saved is None:
                        saved = dict(previous, actions=[], state='succeeded', archived=True)
                    return previous['business_key'], saved
                existing = tx.get(key)
                if existing is not None:
                    # This distinct event also needs durable provenance: a lost
                    # ACK response may otherwise redeliver it after midnight.
                    duplicate_commit = 'seen_' + event_key.removeprefix('event:')
                    duplicate = await ctx.storage.transaction(event=event.payload,
                        commit_id=duplicate_commit, keys=[event_key])
                    duplicate.set(event_key, {k: existing[k] for k in
                        ('business_key', 'day', 'total_count', 'streak_count')})
                    try:
                        await duplicate.save()
                    except IPCRejected as exc:
                        if exc.code == 'revision_conflict' and attempt < 3:
                            continue
                        raise
                    except CommitUncertain:
                        self.unresolved_commit = duplicate_commit
                        raise
                    return key, existing
                account = next_account(tx.get(account_key, {}), day, commit_id)
                player = self.roster.require_current(player)
                due = (now() + timedelta(seconds=10)).isoformat()
                intents, actions = [], []
                for role, reward in selected_rewards(ctx.config, account):
                    intent = prepare_reward(ctx, player, reward, f'{commit_id}_{len(actions)}', due)
                    intents.append(intent)
                    actions.append(dict(role=role, intent=intent.payload(), operation_id=None, receipt=None, state='pending'))
                if ctx.config.announcement.enabled:
                    message = f'{player.name} 完成签到，累计 {account["total_count"]} 次，连签 {account["streak_count"]} 天。'
                    intent = ctx.commands.prepare('tellraw @a ' + json.dumps({'rawtext': [{'text': message}]}, ensure_ascii=False),
                                                  idempotency_key=f'{commit_id}_{len(actions)}', deadline=due)
                    intents.append(intent)
                    actions.append(dict(role='announcement', intent=intent.payload(), operation_id=None, receipt=None, state='pending'))
                record = dict(business_key=key, commit_id=commit_id, uuid=player.uuid, day=day,
                              total_count=account['total_count'], streak_count=account['streak_count'],
                              streak_id=account['streak_id'], config_fingerprint=config_fingerprint(asdict(ctx.config)),
                              actions=actions, state='pending' if actions else 'succeeded')
                # Retain compact event evidence across midnight/redelivery, even
                # after detailed receipts age out. No old event can claim a new day.
                tx.set(account_key, account).set(key, record).set(event_key, {
                    k: record[k] for k in ('business_key', 'day', 'total_count', 'streak_count')})
                page = tx.get(page_key, [])
                page.append(key)
                tx.set(page_key, page)
                if len(page) == 100:
                    tx.set('index:head', head + 1)
                for intent in intents:
                    tx.action(intent)
                try:
                    await tx.save()
                except IPCRejected as exc:
                    if exc.code == 'revision_conflict' and attempt < 3:
                        continue
                    raise
                except CommitUncertain:
                    # No new admission in this process after uncertain storage IO.
                    self.unresolved_commit = commit_id
                    raise
                return key, record
            raise RuntimeError('sign-in CAS contention')

    async def reconcile(self, ctx, key, original):
        # Never hold the business lock while looking up world operations.
        updated = await recover_actions(ctx, original) if original['actions'] else original
        if updated != original:
            async with self.lock:
                tx = await ctx.storage.transaction(keys=[key])
                if tx.get(key) == original:
                    tx.set(key, updated)
                    await tx.save()
        return updated

    async def reconcile_page(self, ctx):
        head = await ctx.storage.get('index:head', 0)
        if self.scan_page > head:
            self.scan_page = 0
        page_key = f'index:claims:{self.scan_page}'
        self.scan_page += 1
        page = await ctx.storage.get(page_key, [])
        cutoff = (now().astimezone(ZoneInfo(ctx.config.timezone)).date() - timedelta(days=90)).isoformat()
        for key in page:
            original = await ctx.storage.get(key)
            if original is None:
                continue
            record = await self.reconcile(ctx, key, original)
            # unknown/partial are retained indefinitely; never interpreted as failed.
            if record['day'] < cutoff and record['state'] in ('succeeded', 'failed'):
                async with self.lock:
                    tx = await ctx.storage.transaction(keys=[key, page_key])
                    if tx.get(key) == record:
                        tx.delete(key)
                        tx.set(page_key, [v for v in tx.get(page_key, []) if v != key])
                        await tx.save()

    async def reply(self, ctx, event, name, message):
        key = 'reply_' + hashlib.sha256(event.payload['delivery_token'].encode()).hexdigest()
        command = 'tellraw ' + exact_target(name) + ' ' + json.dumps({'rawtext': [{'text': message}]}, ensure_ascii=False)
        # ACK is already final. Notification failure never causes a second claim.
        await ctx.commands.execute(command, idempotency_key=key, deadline=(now() + timedelta(seconds=10)).isoformat())

    async def on_event(self, ctx, event):
        body = event.payload.get('payload', {})
        parts = body.get('message', '').split()
        if event.kind != 'chat.received' or not parts or parts[0] not in ctx.config.aliases:
            await event.ack()
            return
        try:
            player = self.roster.resolve_chat(body)
        except (ValueError, LookupError, RuntimeError):
            await event.ack()
            ctx.log.warning('sign-in rejected: identity observation unavailable or inconsistent')
            # No account mutation. A syntactically exact source name may receive rejection only.
            try:
                exact_target(body.get('player', ''))
            except ValueError:
                return
            await self.reply(ctx, event, body['player'], '暂时无法确认你的在线身份，本次未签到。')
            return
        argument = parts[1:] or ['领取']
        if argument == ['领取'] and ctx.config.enabled:
            try:
                key, record = await self.accept(ctx, event, player)
            except ValueError as error:
                await event.ack()
                await self.reply(ctx, event, player.name, str(error))
                return
            await self.reply(ctx, event, player.name, describe(await self.reconcile(ctx, key, record)))
            return
        await event.ack()
        account = await ctx.storage.get('account:' + player.uuid, {})
        if argument == ['状态']:
            today = now().astimezone(ZoneInfo(ctx.config.timezone)).date()
            last = account.get('last_day')
            streak = account.get('streak_count', 0) if last and date.fromisoformat(last) >= today - timedelta(days=1) else 0
            message = f"累计 {account.get('total_count', 0)} 次；最后签到 {last or '无'}；当前连签 {streak} 天，最长 {account.get('best_streak', 0)} 天。"
        elif argument == ['历史']:
            message = ('最近签到：\n' + '\n'.join(f"{r['day']} 累计{r['total']}次/连签{r['streak']}天" for r in reversed(account.get('history', [])))) if ctx.config.history.enabled else '签到历史查询已关闭。'
        elif len(argument) == 2 and argument[0] == '回执':
            try:
                day = date.fromisoformat(argument[1]).isoformat()
            except ValueError:
                message = '日期格式：YYYY-MM-DD。'
            else:
                key = claim_key(player.uuid, day)
                record = await ctx.storage.get(key)
                message = describe(await self.reconcile(ctx, key, record), include_ids=True) if record else '该日期没有详细回执（未签到或已清理）；不会补发。'
        elif argument == ['待核对']:
            pending = []
            for page_number in range((await ctx.storage.get('index:head', 0)) + 1):
                for key in await ctx.storage.get(f'index:claims:{page_number}', []):
                    if key.startswith('day:' + player.uuid + ':'):
                        record = await ctx.storage.get(key)
                        if record is not None and record['state'] not in ('succeeded', 'failed'):
                            pending.append(describe(await self.reconcile(ctx, key, record)))
                            if len(pending) == 10:
                                break
                if len(pending) == 10:
                    break
            message = '\n'.join(pending) if pending else '没有待核对签到。'
            if len(pending) == 10:
                message += '\n本次最多显示10条；可用“回执 日期”查询。'
        elif argument == ['领取'] and not ctx.config.enabled:
            message = '新签到已关闭；仍可查询状态、历史、待核对和原回执。'
        else:
            message = f'{ctx.config.aliases[0]} [领取|状态|历史|待核对|回执 YYYY-MM-DD]'
        await self.reply(ctx, event, player.name, message)


if __name__ == '__main__':
    DailySignin().run()
