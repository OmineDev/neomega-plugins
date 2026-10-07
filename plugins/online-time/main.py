"""Observed online time; conservative checkpoints and immutable reward claims."""
import asyncio
import base64
import copy
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import heapq
import json
import re
from types import SimpleNamespace
from uuid import uuid4
from zoneinfo import ZoneInfo

from neomega_runtime import Plugin
from neomega_runtime.managed import IPCRejected
from neomega_runtime.game import Commands
from neomega_runtime.player_actions import PlayerActions
from neomega_runtime.operations import Operations, OperationSubmissionError, OperationSubmissionTimeout
from neomega_runtime.storage import CommitUncertain
from community_support import ObservedRoster, exact_target, config_fingerprint, recover_actions

NS = 1_000_000_000
PAGE = 100


def encoded_size(value):
    """Upper bound for SDK JSON and Host Go JSON HTML escaping."""
    raw = json.dumps(value, ensure_ascii=False, allow_nan=False)
    for character in '<>&\u2028\u2029':
        raw = raw.replace(character, '\\u%04x' % ord(character))
    return len(raw.encode())


def prepare_reward(ctx, name, reward, key, deadline):
    target = exact_target(name)
    if reward['kind'] == 'item':
        return ctx.player_actions.give(name, reward['item'], count=reward['count'],
                                       idempotency_key=key, deadline=deadline)
    command = (f'scoreboard players add {target} {reward["objective"]} {reward["amount"]}'
               if reward['kind'] == 'scoreboard' else reward['command'].replace('{player}', target))
    if len(command.encode()) > 32 << 10:
        raise ValueError('expanded reward command exceeds 32 KiB')
    return ctx.commands.prepare(command, idempotency_key=key, deadline=deadline)


def validate_reward_budget(rewards):
    operations = Operations(None, 's' * 256)
    ctx = SimpleNamespace(commands=Commands(operations), player_actions=PlayerActions(operations))
    stored, submitted = [], []
    for i, reward in enumerate(rewards):
        intent = prepare_reward(ctx, '<' * 256, reward, '0' * 64 + '_' + str(i),
                                '2000-01-01T00:00:00.000000+00:00').payload()
        stored.append(dict(intent=intent, operation_id=None, receipt=None, state='pending'))
        action = dict(intent)
        action.pop('session')
        submitted.append(action)
    if encoded_size(dict(rewards=rewards, stored=stored, submitted=submitted)) > 192 << 10:
        raise ValueError('expanded reward snapshot and dual intents exceed 192 KiB')


def check_commit_budget(tx):
    if encoded_size(tx.prepare().payload()) > 256 << 10:
        raise ValueError('Host-encoded storage commit exceeds 256 KiB')


@dataclass(frozen=True)
class Calendar:
    enabled: bool = True
    timezone: str = 'Asia/Shanghai'

    def __post_init__(self):
        ZoneInfo(self.timezone)


@dataclass(frozen=True)
class Leaderboard:
    enabled: bool = False
    top: int = field(default=10, metadata={'minimum': 1, 'maximum': 50})


@dataclass(frozen=True)
class Reward:
    kind: str = 'item'
    item: str = 'minecraft:apple'
    count: int = field(default=1, metadata={'minimum': 1, 'maximum': 32767})
    objective: str = ''
    amount: int = field(default=1, metadata={'minimum': 1, 'maximum': 2147483647})
    command: str = field(default='', metadata={'maxLength': 2048})

    def __post_init__(self):
        if len(self.command) > 2048:
            raise ValueError('reward command exceeds 2048 characters')
        if self.kind not in ('item', 'scoreboard', 'command'):
            raise ValueError('unknown reward kind')
        if self.kind == 'item' and not re.fullmatch(r'[a-z0-9_]+(?::[a-z0-9_./-]+)?', self.item):
            raise ValueError('invalid item identifier')
        if self.kind == 'scoreboard' and not re.fullmatch(r'[A-Za-z0-9_.-]{1,16}', self.objective):
            raise ValueError('invalid objective')
        if self.kind == 'command':
            remainder = self.command.replace('{player}', '')
            if not self.command.strip() or any(c in remainder for c in '{}\r\n\x00'):
                raise ValueError('command supports only {player}')


@dataclass(frozen=True)
class Rule:
    id: str = field(default='', metadata={'minLength': 1, 'maxLength': 32})
    period: str = 'total'
    seconds: int = field(default=3600, metadata={'minimum': 1, 'maximum': 3153600000})
    rewards: list[Reward] = field(default_factory=list, metadata={'maxItems': 16})

    def __post_init__(self):
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,32}', self.id) or self.period not in ('total', 'day'):
            raise ValueError('invalid rule ID or period')
        if not 1 <= len(self.rewards) <= 16:
            raise ValueError('each rule needs 1..16 rewards')
        validate_reward_budget([asdict(r) for r in self.rewards])


@dataclass(frozen=True)
class Rewards:
    enabled: bool = False
    rules: list[Rule] = field(default_factory=list, metadata={'maxItems': 16})

    def __post_init__(self):
        if len(self.rules) > 16:
            raise ValueError('rewards allow at most 16 rules')
        if self.enabled and not self.rules:
            raise ValueError('enabled rewards require rules')
        if len({r.id for r in self.rules}) != len(self.rules):
            raise ValueError('duplicate rule ID')
        if len(json.dumps(asdict(self), ensure_ascii=False).encode()) > 65536:
            raise ValueError('reward configuration exceeds 64 KiB')
        if encoded_size(asdict(self)) > 192 << 10:
            raise ValueError('aggregate reward snapshots exceed 192 KiB Host budget')


@dataclass(frozen=True)
class Settings:
    enabled: bool = True
    aliases: list[str] = field(default_factory=lambda: ['!在线'], metadata={'minItems': 1, 'maxItems': 10})
    calendar: Calendar = field(default_factory=Calendar)
    leaderboard: Leaderboard = field(default_factory=Leaderboard)
    rewards: Rewards = field(default_factory=Rewards)

    def __post_init__(self):
        if not 1 <= len(self.aliases) <= 10:
            raise ValueError('aliases require 1..10 entries')
        if len(set(self.aliases)) != len(self.aliases) or any(
                not a or len(a) > 32 or any(c.isspace() or ord(c) < 32 for c in a) for a in self.aliases):
            raise ValueError('aliases must be distinct single words')
        if not self.calendar.enabled and any(r.period == 'day' for r in self.rewards.rules):
            raise ValueError('day rewards require calendar.enabled')


def day_parts(start, end, elapsed_ns, zone):
    """Allocate the exact monotonic delta across local midnight boundaries."""
    result = {}
    cursor, allocated = start, 0
    wall_seconds = (end - start).total_seconds()
    if wall_seconds <= 0:
        return {end.astimezone(zone).date().isoformat(): elapsed_ns}
    while cursor < end:
        local = cursor.astimezone(zone)
        midnight = datetime.combine(local.date() + timedelta(days=1), time(), zone).astimezone(timezone.utc)
        edge = min(midnight, end)
        upto = elapsed_ns if edge == end else round(elapsed_ns * (edge - start).total_seconds() / wall_seconds)
        key = local.date().isoformat()
        result[key] = result.get(key, 0) + upto - allocated
        cursor, allocated = edge, upto
    return result


def duration(ns):
    seconds = ns // NS
    return f'{seconds // 3600}小时{seconds // 60 % 60}分{seconds % 60}秒'


def claim_id(player_uuid, rule_id, period):
    return hashlib.sha256(f'{player_uuid}/{rule_id}/{period}'.encode()).hexdigest()


class OnlineTime(Plugin):
    config_type = Settings

    async def on_start(self, ctx):
        self.ctx, self.settings = ctx, ctx.config
        self.lock = asyncio.Lock()
        self.zone = ZoneInfo(self.settings.calendar.timezone)
        self.active, self.anchor = {}, None
        self.stopping = False
        self.current_day = datetime.now(timezone.utc).astimezone(self.zone).date()
        self.roster = ObservedRoster(on_observation=self.observe)
        self.bot_name = (await ctx.robot())['bot_name']
        self.bot_uuid = None
        if self.settings.enabled and not self.bot_name:
            raise ValueError('robot name unavailable; cannot establish robot UUID exclusion')
        async with self.lock:
            await self.configure()
        # Recovery remains read-only with respect to world actions, including when disabled.
        await self.reconcile_all()
        if self.settings.enabled:
            await self.roster.start(ctx)
        self.recovery_task = ctx.every(30, self.reconcile_all, name='online-receipts')
        self.reward_task = ctx.every(5, self.submit_online, name='online-rewards')

    async def configure(self):
        ids = [r.id for r in self.settings.rewards.rules]
        old = await self.ctx.storage.get('policy', {})
        keys = ['policy'] + ['rule:' + i for i in sorted(set(ids + old.get('rule_ids', [])))]
        tx = await self.ctx.storage.transaction(keys=keys)
        old = tx.get('policy', {})
        if old.get('calendar_used') and old['timezone'] != self.settings.calendar.timezone:
            raise ValueError('timezone is immutable after calendar accounting; migration required')
        enabled = self.settings.enabled and self.settings.rewards.enabled
        generation = old.get('generation', 0) + (old.get('rewards_enabled') != enabled)
        for rule in self.settings.rewards.rules:
            fingerprint = config_fingerprint(asdict(rule))
            prior = tx.get('rule:' + rule.id)
            if prior and (prior['fingerprint'] != fingerprint or prior.get('retired')):
                raise ValueError('reward rule ID cannot be changed or reused: ' + rule.id)
            tx.set('rule:' + rule.id, prior or dict(fingerprint=fingerprint, generation=generation,
                                                  enabled_at=datetime.now(timezone.utc).isoformat()))
        for removed in set(old.get('rule_ids', [])) - set(ids):
            prior = tx.get('rule:' + removed)
            prior['retired'] = True
            tx.set('rule:' + removed, prior)
        self.generation = generation
        tx.set('policy', dict(timezone=self.settings.calendar.timezone,
                             calendar_used=old.get('calendar_used', False),
                             rewards_enabled=enabled, generation=generation, rule_ids=ids))
        await self.save(tx)

    async def save(self, tx):
        # No catch-and-recompute on an unknown response, including checkpoints.
        try:
            return await tx.save()
        except CommitUncertain:
            receipt = await self.ctx.storage.receipt(tx.commit_id)
            if receipt is None:
                raise
            return receipt

    async def load_account(self, identity, now):
        key = 'account:' + identity.uuid
        account = await self.ctx.storage.get(key)
        if account is None:
            shard = hashlib.sha256(identity.uuid.encode('utf-8')).hexdigest()[:2]
            count_key = 'index:' + shard + ':count'
            for attempt in range(4):
                count = await self.ctx.storage.get(count_key, 0)
                page_key = f'index:{shard}:{count // PAGE}'
                tx = await self.ctx.storage.transaction(keys=[key, count_key, page_key])
                if tx.get(key) is not None:
                    account = tx.get(key)
                    break
                if tx.get(count_key, 0) != count:
                    continue
                account = dict(uuid=identity.uuid, name=identity.name, total_ns=0, dates=[],
                               checkpoint_id=None, action_count=0, rule_generations={}, high_day='')
                page = tx.get(page_key, [])
                page.append(identity.uuid)
                tx.set(page_key, page).set(count_key, count + 1).set(key, account)
                try:
                    await self.save(tx)
                    break
                except IPCRejected as exc:
                    if exc.code != 'revision_conflict' or attempt == 3:
                        raise
            else:
                raise RuntimeError('account creation remained conflicted')
        days = {}
        if account['dates']:
            tx = await self.ctx.storage.transaction(keys=[f'day:{identity.uuid}:{d}' for d in account['dates']])
            days = {d: tx.get(f'day:{identity.uuid}:{d}', 0) for d in account['dates']}
        state = dict(identity=identity, account=account, total_ns=account['total_ns'], days=days,
                     saved_ns=now, dirty=False)
        self.active[identity.uuid] = state
        # Baseline newly enabled rules before adding any observed time.
        await self.checkpoint(state, now)
        return state

    async def observe(self, snapshot, monotonic_ns, wall_utc, healthy):
        async with self.lock:
            if not healthy:
                for state in list(self.active.values()):
                    await self.checkpoint(state, monotonic_ns)
                self.active.clear()
                self.anchor = None
                return
            if self.stopping:
                return
            self.current_day = wall_utc.astimezone(self.zone).date()
            if self.bot_uuid is None and self.bot_name:
                matches = [p for p in snapshot if p.name == self.bot_name]
                if len(matches) == 1:
                    self.bot_uuid = matches[0].uuid
            if self.bot_uuid is None:
                # No observed robot yet: do not start an ambiguous accounting baseline.
                self.anchor = None
                return
            current = {p.uuid: p for p in snapshot if p.uuid != self.bot_uuid}
            if self.anchor:
                previous_ns, previous_wall, epoch = self.anchor
                elapsed = monotonic_ns - previous_ns
                new_epochs = {p.epoch for p in snapshot}
                # Empty snapshots are valid removals; a nonempty new epoch is a discontinuity.
                continuous = not new_epochs or new_epochs == {epoch}
                wall_delta = (wall_utc - previous_wall).total_seconds()
                if continuous and elapsed >= 0 and abs(wall_delta - elapsed / NS) <= 2:
                    parts = day_parts(previous_wall, wall_utc, elapsed, self.zone) if self.settings.calendar.enabled else {}
                    for state in self.active.values():
                        if parts and min(parts) < state['account'].get('high_day', ''):
                            continue
                        if len(parts) > 1:
                            # One atomic checkpoint per date keeps at most one
                            # earned snapshot per day-rule in each transaction.
                            # Checkpoint the final date too: a healthy delayed
                            # observation may contain a large final-day delta.
                            for day, delta in parts.items():
                                state['total_ns'] += delta
                                state['days'][day] = state['days'].get(day, 0) + delta
                                state['dirty'] = True
                                await self.checkpoint(state, monotonic_ns)
                        else:
                            state['total_ns'] += elapsed
                            for day, delta in parts.items():
                                state['days'][day] = state['days'].get(day, 0) + delta
                            state['dirty'] = True
                elif abs(wall_delta - elapsed / NS) > 2:
                    self.ctx.log.warning('online-time clock_discontinuity; interval discarded')
                if not continuous:
                    for state in self.active.values():
                        await self.checkpoint(state, monotonic_ns)
                    self.active.clear()
            for player_uuid in set(self.active) - set(current):
                await self.checkpoint(self.active.pop(player_uuid), monotonic_ns)
            for player_uuid, identity in current.items():
                state = self.active.get(player_uuid)
                if state is None:
                    state = await self.load_account(identity, monotonic_ns)
                state['identity'] = identity
                state['account']['name'] = identity.name
                if monotonic_ns - state['saved_ns'] >= 30 * NS:
                    await self.checkpoint(state, monotonic_ns)
            self.anchor = (monotonic_ns, wall_utc, next(iter(current.values())).epoch if current else
                           (self.anchor[2] if self.anchor else None))
    async def submit_online(self):
        if self.stopping or not self.settings.enabled or not self.settings.rewards.enabled:
            return
        async with self.lock:
            identities = [state['identity'] for state in self.active.values()]
        for identity in identities:
            if await self.submit_earned(identity) is False:
                break  # Installation quota: defer remaining claims to the next tick.

    async def checkpoint(self, state, now):
        player_uuid = state['identity'].uuid
        account_key = 'account:' + player_uuid
        for attempt in range(4):
            old = state['account']
            latest = max(self.current_day, date.fromisoformat(old['high_day']) if old.get('high_day') else self.current_day)
            cutoff = (latest - timedelta(days=89)).isoformat()
            dates = sorted(d for d in state['days'] if d >= cutoff)[-90:]
            expired = set(old['dates']) - set(dates)
            possible_claims = sum(1 if r.period == 'total' else len(dates) for r in self.settings.rewards.rules)
            page_numbers = range(old['action_count'] // PAGE,
                                 (old['action_count'] + possible_claims) // PAGE + 1)
            keys = [account_key, 'policy'] + [f'day:{player_uuid}:{d}' for d in sorted(set(dates) | expired)]
            keys += [f'actions:{player_uuid}:{p}' for p in sorted(page_numbers)]
            commit_id = uuid4().hex
            tx = await self.ctx.storage.transaction(keys=keys, commit_id=commit_id)
            persisted = tx.get(account_key)
            if persisted['checkpoint_id'] != old['checkpoint_id']:
                raise RuntimeError('account checkpoint changed outside its owner')
            updated = dict(old, total_ns=state['total_ns'], dates=dates, checkpoint_id=commit_id,
                           rule_generations={r.id: old['rule_generations'][r.id] for r in self.settings.rewards.rules
                                             if r.id in old['rule_generations']})
            old_days = {d: tx.get(f'day:{player_uuid}:{d}', 0) for d in dates}
            if dates:
                updated['high_day'] = max(old.get('high_day', ''), dates[-1])
            if self.settings.rewards.enabled and self.settings.enabled:
                for rule in self.settings.rewards.rules:
                    generation = updated['rule_generations'].get(rule.id)
                    updated['rule_generations'][rule.id] = self.generation
                    if generation != self.generation:
                        continue
                    periods = ['total'] if rule.period == 'total' else dates
                    for period in periods:
                        before = old['total_ns'] if period == 'total' else old_days[period]
                        after = state['total_ns'] if period == 'total' else state['days'][period]
                        if before < rule.seconds * NS <= after:
                            business = claim_id(player_uuid, rule.id, period)
                            key = 'action:' + business
                            record = dict(business_key=business, player_uuid=player_uuid, rule_id=rule.id,
                                          period=period, rewards=[asdict(r) for r in rule.rewards],
                                          state='earned_pending', commit_id=None, actions=[],
                                          earned_at=datetime.now(timezone.utc).isoformat())
                            tx.set(key, record)
                            index_key = f'actions:{player_uuid}:{updated["action_count"] // PAGE}'
                            page = tx.get(index_key, [])
                            page.append(business)
                            tx.set(index_key, page)
                            updated['action_count'] += 1
            for d in dates:
                tx.set(f'day:{player_uuid}:{d}', state['days'][d])
            for d in expired:
                tx.delete(f'day:{player_uuid}:{d}')
            tx.set(account_key, updated)
            if dates:
                policy = tx.get('policy')
                policy['calendar_used'] = True
                tx.set('policy', policy)
            check_commit_budget(tx)
            try:
                await self.save(tx)
                state['account'], state['saved_ns'], state['dirty'] = updated, now, False
                state['days'] = {d: state['days'][d] for d in dates}
                return
            except IPCRejected as exc:
                if exc.code != 'revision_conflict' or attempt == 3:
                    raise

    async def account_ids(self):
        for shard_number in range(256):
            shard = f'{shard_number:02x}'
            count = await self.ctx.storage.get(f'index:{shard}:count', 0)
            for page in range((count + PAGE - 1) // PAGE):
                for player_uuid in await self.ctx.storage.get(f'index:{shard}:{page}', []):
                    yield player_uuid

    async def action_ids(self, player_uuid):
        account = await self.ctx.storage.get('account:' + player_uuid)
        for page in range((account['action_count'] + PAGE - 1) // PAGE):
            for business in await self.ctx.storage.get(f'actions:{player_uuid}:{page}', []):
                yield business

    def reward_intent(self, identity, reward, key, deadline):
        return prepare_reward(self.ctx, identity.name, reward, key, deadline)

    async def reject_budget(self, key, original):
        tx = await self.ctx.storage.transaction(keys=[key])
        tx.set(key, dict(original, admission_rejection='config_budget_exceeded'))
        await self.save(tx)
        self.ctx.log.warning('online-time reward configuration exceeds admission budget; eligibility retained: %s', original['business_key'])

    async def submit_earned(self, identity):
        async for business in self.action_ids(identity.uuid):
            async with self.lock:
                if self.stopping or not self.settings.enabled or not self.settings.rewards.enabled:
                    return
                key = 'action:' + business
                plan = await self.ctx.storage.transaction(keys=[key])
                record = plan.get(key)
                if record['state'] != 'earned_pending' or record.get('admission_rejection') == 'config_budget_exceeded':
                    continue
                try:
                    identity = self.roster.require_current(identity)
                except ValueError:
                    return
                original = copy.deepcopy(record)
                try:
                    validate_reward_budget(record['rewards'])
                    deadline = (datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat()
                    intents = [self.reward_intent(identity, r, business + '_' + str(i), deadline)
                               for i, r in enumerate(record['rewards'])]
                    record.update(commit_id=uuid4().hex, state='pending', actions=[
                        dict(intent=intent.payload(), operation_id=None, receipt=None, state='pending') for intent in intents])
                except ValueError:
                    await self.reject_budget(key, original)
                    continue
                preview = await self.ctx.storage.transaction(keys=[key], commit_id=record['commit_id'])
                try:
                    preview.set(key, record)
                    for intent in intents:
                        preview.action(intent)
                    check_commit_budget(preview)
                    plan.set(key, record)
                    check_commit_budget(plan)
                except ValueError:
                    # Old, pre-validation earned snapshots remain owned claims.
                    # Do not rewrite their rewards, split actions, or stop timing.
                    await self.reject_budget(key, original)
                    continue
                # Persist the fixed plan BEFORE admission. A crash in this gap leaves
                # an unresolved claim; it can never invent another commit on restart.
                await self.save(plan)
                tx = await self.ctx.storage.transaction(keys=[key], commit_id=record['commit_id'])
                try:
                    self.roster.require_current(identity)
                except ValueError:
                    # The durable plan stays unresolved, without a new admission.
                    return
                tx.set(key, record)
                for intent in intents:
                    tx.action(intent)
                try:
                    check_commit_budget(tx)
                except ValueError:
                    await self.reject_budget(key, original)
                    continue
                try:
                    await self.save(tx)
                except IPCRejected as exc:
                    if exc.code != 'quota_exceeded':
                        raise
                    # Host rejected the entire action transaction. Only this
                    # definite outcome may restore the original earned claim.
                    rejected = await self.ctx.storage.transaction(keys=[key])
                    record.update(state='earned_pending', actions=[], commit_id=None,
                                  last_rejection_commit_id=tx.commit_id,
                                  last_rejection_reason=exc.code)
                    rejected.set(key, record)
                    await self.save(rejected)
                    self.ctx.log.warning('online-time reward admission rejected: quota_exceeded; eligibility retained')
                    return False
            await self.reconcile_one(business)

    @staticmethod
    def evidence_prefix(key):
        return 'evidence:' + hashlib.sha256(key.encode()).hexdigest()

    async def hydrate(self, record):
        """Caller holds lock so referenced evidence cannot be reclaimed."""
        hydrated = copy.deepcopy(record)
        for action in hydrated['actions']:
            ref = action.get('receipt_ref')
            if ref is None:
                continue
            pieces = [await self.ctx.storage.get(ref['key'] + ':' + str(i))
                      for i in range(ref['chunks'])]
            raw = b''.join(base64.b64decode(piece, validate=True) for piece in pieces)
            if len(raw) != ref['bytes'] or hashlib.sha256(raw).hexdigest() != ref['sha256']:
                raise ValueError('online-time receipt evidence integrity mismatch')
            action['receipt'] = json.loads(raw)
        return hydrated

    async def persist_receipts(self, key, record):
        """Register first, write bounded chunks, then let caller publish refs."""
        saved = copy.deepcopy(record)
        prefix = self.evidence_prefix(key)
        for action in saved['actions']:
            receipt = action.get('receipt')
            if receipt is None:
                continue
            raw = json.dumps(receipt, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()
            digest = hashlib.sha256(raw).hexdigest()
            node_key = prefix + ':' + digest
            chunks = [base64.b64encode(raw[i:i + 32768]).decode('ascii')
                      for i in range(0, len(raw), 32768)]
            tx = await self.ctx.storage.transaction(keys=[prefix, node_key])
            if tx.get(node_key) is None:
                tx.set(node_key, dict(next=tx.get(prefix), chunks=len(chunks), cursor=0))
                tx.set(prefix, node_key)
                await self.save(tx)
            for i, chunk in enumerate(chunks):
                chunk_key = node_key + ':' + str(i)
                tx = await self.ctx.storage.transaction(keys=[chunk_key])
                if tx.get(chunk_key) != chunk:
                    tx.set(chunk_key, chunk)
                    await self.save(tx)
            action['receipt_ref'] = dict(key=node_key, chunks=len(chunks), bytes=len(raw), sha256=digest)
            action['receipt'] = None
        return saved

    async def clean_evidence(self, key, record):
        """Caller holds lock; preserve all live refs, reclaim interrupted writes."""
        live = {a['receipt_ref']['key'] for a in record['actions'] if a.get('receipt_ref')}
        prefix = self.evidence_prefix(key)
        previous = None
        node_key = await self.ctx.storage.get(prefix)
        while node_key is not None:
            node = await self.ctx.storage.get(node_key)
            if node_key in live:
                previous, node_key = node_key, node['next']
                continue
            for i in range(node['cursor'], node['chunks']):
                chunk_key = node_key + ':' + str(i)
                tx = await self.ctx.storage.transaction(keys=[node_key, chunk_key])
                node = dict(node, cursor=i + 1)
                tx.delete(chunk_key).set(node_key, node)
                await self.save(tx)
            link = previous or prefix
            tx = await self.ctx.storage.transaction(keys=[link, node_key])
            if previous is None:
                tx.set(prefix, node['next'])
            else:
                tx.set(previous, dict(tx.get(previous), next=node['next']))
            tx.delete(node_key)
            await self.save(tx)
            node_key = node['next']

    async def reconcile_one(self, business):
        key = 'action:' + business
        async with self.lock:
            original = await self.ctx.storage.get(key)
            await self.clean_evidence(key, original)
            if original['state'] == 'earned_pending':
                return
            hydrated = await self.hydrate(original)
        updated = (hydrated if original['state'] in ('succeeded', 'failed')
                   else await recover_actions(self.ctx, hydrated))
        inline = any(a.get('receipt') is not None and not a.get('receipt_ref') for a in original['actions'])
        if updated == hydrated and not inline:
            return
        async with self.lock:
            if await self.ctx.storage.get(key) != original:
                return
            saved = await self.persist_receipts(key, updated)
            for attempt in range(4):
                tx = await self.ctx.storage.transaction(keys=[key])
                if tx.get(key) != original:
                    return
                tx.set(key, saved)
                try:
                    await self.save(tx)
                    break
                except IPCRejected as exc:
                    if exc.code != 'revision_conflict' or attempt == 3:
                        raise
            await self.clean_evidence(key, saved)

    async def reconcile_all(self):
        async for player_uuid in self.account_ids():
            async for business in self.action_ids(player_uuid):
                await self.reconcile_one(business)

    async def leaderboard(self, period, day):
        if period not in ('总', '今日', '本周'):
            raise ValueError('排行类型：总 / 今日 / 本周')
        if period != '总' and not self.settings.calendar.enabled:
            raise ValueError('日周统计已关闭')
        days = [day.isoformat()] if period == '今日' else [
            (day - timedelta(days=i)).isoformat() for i in range(day.weekday() + 1)]
        top = []
        async for player_uuid in self.account_ids():
            keys = ['account:' + player_uuid] + ([] if period == '总' else [f'day:{player_uuid}:{d}' for d in days])
            tx = await self.ctx.storage.transaction(keys=keys)
            account = tx.get(keys[0])
            score = account['total_ns'] if period == '总' else sum(tx.get(k, 0) for k in keys[1:])
            entry = (score, player_uuid, account['name'])
            if len(top) < self.settings.leaderboard.top:
                heapq.heappush(top, entry)
            elif entry > top[0]:
                heapq.heapreplace(top, entry)
        return '已确认保存的在线时长榜：\n' + '\n'.join(
            f'{i}. {name}：{duration(score)}' for i, (score, _, name) in enumerate(sorted(top, reverse=True), 1))

    async def reply(self, identity, message):
        try:
            identity = self.roster.require_current(identity)
        except ValueError:
            self.ctx.log.info('online-time notification skipped: player no longer current')
            return
        # Notifications are not reward claims and cannot cause reward replay.
        intent = self.ctx.commands.prepare('tellraw ' + exact_target(identity.name) + ' ' + json.dumps(
            {'rawtext': [{'text': message}]}, ensure_ascii=False), idempotency_key=uuid4().hex,
            deadline=(datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat())
        try:
            await self.ctx.operations.submit(intent)
        except IPCRejected as exc:
            self.ctx.log.warning('online-time notification rejected: %s', exc.code)
        except (OperationSubmissionError, OperationSubmissionTimeout) as exc:
            self.ctx.log.warning('online-time notification unresolved: %s; no retry', type(exc).__name__)

    async def acknowledge(self, event):
        async with self.lock:
            await event.ack()

    async def on_event(self, ctx, event):
        body = event.payload.get('payload', {})
        parts = body.get('message', '').split()
        if event.kind != 'chat.received' or not parts or parts[0] not in self.settings.aliases:
            await self.acknowledge(event)
            return
        if not self.settings.enabled or self.stopping:
            await self.acknowledge(event)
            return
        try:
            identity = self.roster.resolve_chat(body)
        except ValueError:
            await self.acknowledge(event)
            ctx.log.warning('online-time rejected chat: reliable identity unavailable')
            return
        today = datetime.now(timezone.utc).astimezone(self.zone).date()
        try:
            if len(parts) >= 2 and parts[1] == '排行':
                if not self.settings.leaderboard.enabled:
                    message = '在线排行未开启。'
                else:
                    message = await self.leaderboard(parts[2] if len(parts) == 3 else '总', today)
            elif len(parts) == 1:
                async with self.lock:
                    state = self.active.get(identity.uuid)
                    if state is None:
                        raise ValueError('在线计时尚未建立，请稍后查询')
                    await self.checkpoint(state, self.roster.last_confirmed_ns)
                    message = '累计在线：' + duration(state['total_ns'])
                    if self.settings.calendar.enabled:
                        week = [(today - timedelta(days=i)).isoformat() for i in range(today.weekday() + 1)]
                        message += '\n今日：' + duration(state['days'].get(today.isoformat(), 0))
                        message += '\n本周：' + duration(sum(state['days'].get(d, 0) for d in week))
                pending = 0
                async for business in self.action_ids(identity.uuid):
                    record = await ctx.storage.get('action:' + business)
                    if record['state'] not in ('succeeded', 'failed'):
                        pending += 1
                if pending:
                    message += f'\n奖励待领取或核对：{pending} 笔；未知回执不会补发。'
            else:
                message = self.settings.aliases[0] + '：查询自己；' + self.settings.aliases[0] + ' 排行 总/今日/本周'
        except ValueError as error:
            message = str(error)
        await self.acknowledge(event)
        await self.reply(identity, message)
        if self.settings.rewards.enabled:
            await self.submit_earned(identity)

    async def on_stop(self, ctx):
        self.stopping = True
        await self.roster.close()
        async with self.lock:
            for state in self.active.values():
                await self.checkpoint(state, state['saved_ns'])
            self.active.clear()
            self.anchor = None


if __name__ == '__main__':
    OnlineTime().run()
