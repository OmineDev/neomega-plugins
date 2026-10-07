"""Native scheduled commands; durable admission, never replay a world write."""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import re
import uuid
from zoneinfo import ZoneInfo

from neomega_runtime import Plugin
from community_support import config_fingerprint, receipt_state, recover_actions

UTC = timezone.utc


def now():
    return datetime.now(UTC)


def validate_task(task):
    allowed = {'id', 'enabled', 'commands', 'schedule', 'run_on_start', 'resume_token'}
    if not isinstance(task, dict) or set(task) - allowed:
        raise ValueError('invalid task fields')
    if not re.fullmatch(r'[a-z][a-z0-9_-]{0,47}', task.get('id', '')):
        raise ValueError('task id must be 1..48 lowercase letters/digits/_/-')
    for name in ('enabled', 'run_on_start'):
        if type(task.get(name, False)) is not bool:
            raise ValueError(name + ' must be boolean')
    token = task.get('resume_token', '')
    if not isinstance(token, str) or len(token) > 80:
        raise ValueError('resume_token must contain at most 80 characters')
    commands = task.get('commands')
    if not isinstance(commands, list) or not 1 <= len(commands) <= 16:
        raise ValueError('task commands must contain 1..16 commands')
    for command in commands:
        if (not isinstance(command, str) or not command.strip() or len(command.encode()) > 4096
                or any(ord(c) < 32 for c in command)):
            raise ValueError('command must be one nonempty line of at most 4096 UTF-8 bytes')
    schedule = task.get('schedule')
    if not isinstance(schedule, dict):
        raise ValueError('schedule is required')
    if schedule.get('kind') == 'interval':
        if set(schedule) != {'kind', 'seconds'} or type(schedule['seconds']) is not int or not 10 <= schedule['seconds'] <= 31536000:
            raise ValueError('interval seconds must be integer 10..31536000')
    elif schedule.get('kind') == 'daily':
        if set(schedule) != {'kind', 'times', 'timezone'}:
            raise ValueError('daily requires times and timezone only')
        ZoneInfo(schedule['timezone'])
        times = schedule['times']
        if (not isinstance(times, list) or not 1 <= len(times) <= 48
                or any(not isinstance(t, str) or not re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]', t) for t in times)
                or len(set(times)) != len(times)):
            raise ValueError('daily times must be 1..48 unique HH:MM values')
        if task.get('run_on_start', False):
            raise ValueError('run_on_start is only valid for interval tasks')
    else:
        raise ValueError('schedule kind must be interval or daily')


@dataclass
class Settings:
    enabled: bool = True
    tasks: list = field(default_factory=list)

    def __post_init__(self):
        if type(self.enabled) is not bool or not isinstance(self.tasks, list) or len(self.tasks) > 100:
            raise ValueError('enabled must be boolean; tasks must have at most 100 entries')
        for task in self.tasks:
            validate_task(task)
        if len({t['id'] for t in self.tasks}) != len(self.tasks):
            raise ValueError('task ids must be unique')


def next_daily(schedule, after, watermark=''):
    """First valid fold only: missing times skipped, repeated local slots once."""
    zone = ZoneInfo(schedule['timezone'])
    date = after.astimezone(zone).date()
    for offset in range(370):
        day = date + timedelta(days=offset)
        for clock in sorted(schedule['times']):
            slot = day.isoformat() + 'T' + clock
            if slot <= watermark:
                continue
            local = datetime.fromisoformat(slot).replace(tzinfo=zone, fold=0)
            instant = local.astimezone(UTC)
            if instant.astimezone(zone).replace(tzinfo=None) != local.replace(tzinfo=None):
                continue
            if instant > after:
                return instant, slot
    raise ValueError('no future daily slot within 370 days; check clock and watermark')


class ScheduledCommands(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.states = {}

    async def save_state(self, ctx, task_id, state):
        # No automatic CAS/transport retry: failure halts this worker.
        async with self.lock:
            tx = await ctx.storage.transaction(keys=['task:' + task_id])
            tx.set('task:' + task_id, state)
            await tx.save()
        self.states[task_id] = state

    async def on_start(self, ctx):
        tasks = {task['id']: task for task in ctx.config.tasks}
        async with self.lock:
            tx = await ctx.storage.transaction(keys=['registry'])
            registry = tx.get('registry', {})
            if len(set(registry) | set(tasks)) > 100:
                raise ValueError('installation lifetime limit is 100 stable task ids, including tombstones')
            for task_id in registry:
                if task_id not in tasks:
                    registry[task_id] = 'deleted'
            for task_id in tasks:
                if registry.get(task_id) == 'deleted':
                    raise ValueError('deleted task id cannot be reused: ' + task_id)
                registry[task_id] = 'active'
            tx.set('registry', registry)
            await tx.save()
        # Deleted/disabled tasks still reconcile old actions without executing.
        for task_id in registry:
            state = await ctx.storage.get('task:' + task_id, None)
            if state is None:
                state = {'round': 0, 'status': 'idle', 'watermark': '', 'unresolved': [],
                         'fingerprint': '', 'resume_token': '', 'used_resume_tokens': [], 'active_actions': []}
            for key in list(dict.fromkeys(state['unresolved'] + state['active_actions'])):
                record = await ctx.storage.get(key)
                if record is None:
                    raise RuntimeError('missing durable action record: ' + key)
                updated = await recover_actions(ctx, record)
                async with self.lock:
                    tx = await ctx.storage.transaction(keys=[key])
                    tx.set(key, updated)
                    await tx.save()
                if updated['state'] in ('succeeded', 'failed') and key in state['unresolved']:
                    state['unresolved'].remove(key)
                elif updated['state'] not in ('succeeded', 'failed') and key not in state['unresolved']:
                    state['unresolved'].append(key)
            if state['status'] == 'running':
                state['status'] = 'paused'
                state['reason'] = 'interrupted_round_not_replayed'
            task = tasks.get(task_id)
            if task:
                fingerprint = config_fingerprint({k: v for k, v in task.items() if k not in ('enabled', 'resume_token')})
                token = task.get('resume_token', '')
                token_hash = config_fingerprint(token)
                used_tokens = state.setdefault('used_resume_tokens', [])
                if state['status'] == 'paused' and token and token != state['resume_token']:
                    if token_hash in used_tokens:
                        raise ValueError('resume_token must never reuse an earlier value')
                    if len(used_tokens) >= 100:
                        raise ValueError('task has reached 100 administrator resumptions')
                    used_tokens.append(token_hash)
                    state['status'] = 'idle'
                    state['reason'] = 'administrator_resumed_future_rounds_only'
                    ctx.log.warning('task=%s resume permits future rounds only; old effects are not retried', task_id)
                state['resume_token'] = token
                if state['fingerprint'] and fingerprint != state['fingerprint']:
                    ctx.log.info('task=%s configuration changed; old round remains immutable', task_id)
                state['fingerprint'] = fingerprint
            await self.save_state(ctx, task_id, state)
        if ctx.config.enabled:
            for task in tasks.values():
                if task.get('enabled', False) and self.states[task['id']]['status'] != 'paused':
                    ctx.spawn(self.run_task(ctx, task), name='schedule-' + task['id'])

    async def run_task(self, ctx, task):
        task_id = task['id']
        first = True
        while self.states[task_id]['status'] != 'paused':
            schedule = task['schedule']
            slot = None
            if schedule['kind'] == 'interval':
                if not (first and task.get('run_on_start', False)
                        and self.states[task_id].get('reason') != 'administrator_resumed_future_rounds_only'):
                    await asyncio.sleep(schedule['seconds'])
            else:
                due, slot = next_daily(schedule, now(), self.states[task_id]['watermark'])
                # Recheck wall time frequently: wall rollback must not execute early.
                while now() < due:
                    await asyncio.sleep(min(1.0, (due - now()).total_seconds()))
                # Clock jumps and a stalled worker do not backfill missed slots.
                if (now() - due).total_seconds() >= 60:
                    continue
            first = False
            await self.run_round(ctx, task, slot)

    async def run_round(self, ctx, task, slot):
        task_id = task['id']
        state = dict(self.states[task_id])
        if state['status'] == 'paused':
            return
        if len(state['unresolved']) >= 100:
            state.update(status='paused', reason='unresolved_action_limit')
            await self.save_state(ctx, task_id, state)
            return
        state.update(round=state['round'] + 1, status='running', active_actions=[], reason='')
        if slot:
            if slot <= state['watermark']:
                return
            state['watermark'] = slot
        await self.save_state(ctx, task_id, state)
        for index, command in enumerate(task['commands']):
            key = f"action:{task_id}:{state['round']}:{index}"
            commit_id = uuid.uuid4().hex
            intent = ctx.commands.prepare(command, idempotency_key=commit_id,
                         deadline=(now() + timedelta(seconds=10)).isoformat())
            record = {'business_key': key, 'commit_id': commit_id, 'state': 'pending',
                      'actions': [{'intent': intent.payload(), 'operation_id': None,
                                   'receipt': None, 'state': 'pending'}]}
            state['active_actions'].append(key)
            async with self.lock:
                tx = await ctx.storage.transaction(commit_id=commit_id, keys=['task:' + task_id, key])
                tx.set('task:' + task_id, state)
                tx.set(key, record)
                tx.action(intent)
                try:
                    result = await tx.save()
                except Exception:
                    # Query original commit only; never reconstruct an intent.
                    result = None
            if result:
                record['actions'][0]['operation_id'] = result['operation_ids'][0]
                try:
                    receipt = await ctx.operations.wait(result['operation_ids'][0], timeout=20)
                    record['actions'][0].update(receipt=receipt, state=receipt_state(receipt))
                    record['state'] = receipt_state(receipt)
                except Exception:
                    record = await recover_actions(ctx, record)
            else:
                record = await recover_actions(ctx, record)
            # A pending result after a bounded wait is unresolved, not success.
            if record['state'] == 'pending':
                record['state'] = 'unknown'
            if record['state'] != 'succeeded':
                state.update(status='paused', reason='command_' + record['state'])
                if record['state'] in ('unknown', 'partial') and key not in state['unresolved']:
                    state['unresolved'].append(key)
            async with self.lock:
                tx = await ctx.storage.transaction(keys=['task:' + task_id, key])
                tx.set(key, record)
                tx.set('task:' + task_id, state)
                await tx.save()
            self.states[task_id] = state
            ctx.log.info('task=%s round=%s command=%s state=%s operation_id=%s',
                         task_id, state['round'], index + 1, record['state'],
                         record['actions'][0]['operation_id'])
            if state['status'] == 'paused':
                return
        state.update(status='idle', completed_at=now().isoformat())
        await self.save_state(ctx, task_id, state)


if __name__ == '__main__':
    ScheduledCommands().run()
