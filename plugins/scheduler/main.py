"""Installation-isolated durable pull scheduler, with leased stable triggers."""
import asyncio
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from neomega_runtime import Plugin, service, ServiceRejected
from neomega_runtime.storage import CommitUncertain
from neomega_runtime.managed import IPCRejected


def owner(call):
    if call is None or not call.installation_id:
        raise ServiceRejected('trusted_context_required')
    return call.installation_id


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def text(value, name):
    if not isinstance(value, str) or not 1 <= len(value.encode()) <= 128:
        raise ServiceRejected('invalid_' + name)
    return value


def instant(value):
    try:
        dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            raise ValueError()
        return dt.timestamp()
    except (TypeError, ValueError, AttributeError, OverflowError):
        raise ServiceRejected('invalid_due_at') from None


@dataclass
class Settings:
    max_schedules_per_owner: int = field(default=128, metadata={'minimum': 1, 'maximum': 128})
    lease_seconds: int = field(default=60, metadata={'minimum': 5, 'maximum': 3600})
    max_catch_up: int = field(default=32, metadata={'minimum': 1, 'maximum': 128})


class Scheduler(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = None
        self.pending = None
        self.pending_path = None

    async def on_start(self, ctx):
        await self.initialize(ctx)

    async def initialize(self, ctx):
        self.pending_path = ctx.data_dir / 'scheduler-pending.json'
        if self.pending_path.exists():
            self.pending = json.loads(self.pending_path.read_text())['commit_id']
        async with self.lock:
            if not await self.reconcile_commit(ctx):
                return
            tx = await ctx.storage.transaction(keys=['schema_version'])
            version = tx.get('schema_version')
            if version is None:
                tx.set('schema_version', 1)
                await self.save(ctx, tx)
            elif type(version) is not int or version != 1:
                raise RuntimeError('unsupported schema_version; migration required')

    def clear_pending(self):
        if self.pending_path is not None:
            self.pending_path.unlink(missing_ok=True)
        self.pending = None

    async def reconcile_commit(self, ctx):
        if self.pending is None:
            return True
        if await ctx.storage.receipt(self.pending) is None:
            return False
        self.clear_pending()
        return True

    async def save(self, ctx, tx):
        # Write the uncertain barrier before crossing the IPC boundary. The
        # marker survives a worker crash; absence of receipt is never failure.
        tx.prepare()
        self.pending = tx.commit_id
        if self.pending_path is not None:
            temporary = self.pending_path.with_suffix('.tmp')
            with temporary.open('w') as stream:
                json.dump({'commit_id': self.pending}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.pending_path)
            fd = os.open(self.pending_path.parent, os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        try:
            await tx.save()
        except IPCRejected:
            self.clear_pending()
            raise
        except (CommitUncertain, asyncio.CancelledError):
            raise
        self.clear_pending()

    async def on_maintenance(self, ctx, request):
        async with self.lock:
            token = request['token']
            if not await self.reconcile_commit(ctx):
                return {'status': 'busy', 'token': token}
            if request['operation'] == 'seal':
                if self.sealed not in (None, token):
                    return {'status': 'busy', 'token': token}
                self.sealed = token
                return {'status': 'sealed', 'token': token}
            if self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            self.sealed = None
            return {'status': 'released', 'token': token}

    async def mutate(self, ctx, a, call, operation, update):
        caller = owner(call)
        request = text(a.get('request_id'), 'request_id')
        receipt_key = 'request:' + digest([caller, request])
        key = 'owner:' + digest(caller)
        fingerprint = digest([operation, a])
        archive_key = 'archive:' + digest([caller, a.get('schedule_id')])
        async with self.lock:
            if self.sealed is not None:
                raise ServiceRejected('maintenance_sealed')
            if not await self.reconcile_commit(ctx):
                raise ServiceRejected('commit_unresolved')
            tx = await ctx.storage.transaction(keys=[key, receipt_key, archive_key])
            old = tx.get(receipt_key)
            if old is not None:
                if old['fingerprint'] != fingerprint:
                    raise ServiceRejected('idempotency_conflict')
                return old['result']
            state = tx.get(key, {'revision': 0, 'schedules': {}})
            if 'expected_revision' in a and a['expected_revision'] != state['revision']:
                raise ServiceRejected('revision_conflict')
            result = update(state, tx.get(archive_key))
            sid = a.get('schedule_id')
            row = state['schedules'].get(sid)
            if row and row['state'] in ('completed', 'cancelled'):
                tx.set(archive_key, row)
                del state['schedules'][sid]
            state['revision'] += 1
            result['revision'] = state['revision']
            tx.set(key, state)
            tx.set(receipt_key, {'owner': caller, 'operation': operation, 'fingerprint': fingerprint,
                                'result': result, 'at': time.time()})
            await self.save(ctx, tx)
            return result

    @service('schedule', with_context=True)
    async def schedule(self, ctx, a, call):
        sid = text(a.get('schedule_id'), 'schedule_id')
        due = instant(a.get('due_at'))
        interval = a.get('interval_seconds', 0)
        if type(interval) is not int or interval < 0 or interval > 31536000:
            raise ServiceRejected('invalid_interval')
        policy = a.get('missed', 'coalesce')
        if policy not in ('coalesce', 'skip', 'catch_up'):
            raise ServiceRejected('invalid_missed_policy')
        try:
            ZoneInfo(a.get('timezone', 'UTC'))
        except (ValueError, TypeError, KeyError):
            raise ServiceRejected('invalid_timezone') from None
        payload = a.get('payload', {})
        if len(json.dumps(payload, allow_nan=False).encode()) > 512:
            raise ServiceRejected('payload_too_large')
        def update(state, archived):
            if sid in state['schedules'] or archived is not None:
                raise ServiceRejected('schedule_exists')
            if len(state['schedules']) >= ctx.config.max_schedules_per_owner:
                raise ServiceRejected('schedule_limit')
            state['schedules'][sid] = {'schedule_id': sid, 'due_at': due, 'interval_seconds': interval,
                'timezone': a.get('timezone', 'UTC'), 'missed': policy, 'payload': payload,
                'state': 'active', 'lease': None, 'completed': 0}
            return {'status': 'scheduled', 'schedule': state['schedules'][sid]}
        return await self.mutate(ctx, a, call, 'schedule', update)

    @service('cancel', with_context=True)
    async def cancel(self, ctx, a, call):
        def update(state, archived):
            row = state['schedules'].get(a.get('schedule_id'))
            if row is None:
                if archived is not None:
                    return {'status': archived['state'], 'schedule_id': a['schedule_id']}
                raise ServiceRejected('schedule_not_found')
            row.update(state='cancelled', lease=None)
            return {'status': 'cancelled', 'schedule_id': row['schedule_id']}
        return await self.mutate(ctx, a, call, 'cancel', update)

    @service('query', with_context=True)
    async def query(self, ctx, a, call):
        state = await ctx.storage.get('owner:' + digest(owner(call)), {'revision': 0, 'schedules': {}})
        if a.get('schedule_id') is not None:
            row = state['schedules'].get(a['schedule_id'])
            if row is None:
                row = await ctx.storage.get('archive:' + digest([owner(call), a['schedule_id']]))
            return {'revision': state['revision'], 'schedule': row, 'status': 'found' if row else 'not_found'}
        offset = a.get('offset', 0)
        if type(offset) is not int or offset < 0:
            raise ServiceRejected('invalid_offset')
        rows = list(state['schedules'].values())
        return {'revision': state['revision'], 'schedules': rows[offset:offset + 16],
                'next_offset': offset + 16 if offset + 16 < len(rows) else None}

    @service('claim', with_context=True)
    async def claim(self, ctx, a, call):
        now = time.time()
        limit = a.get('limit', 16)
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ServiceRejected('invalid_limit')
        def update(state, archived):
            triggers = []
            for row in sorted(state['schedules'].values(), key=lambda r: r['due_at']):
                if len(triggers) >= limit:
                    break
                if row['state'] != 'active' or row['due_at'] > now:
                    continue
                lease = row['lease']
                if lease and lease['expires_at'] > now:
                    continue
                # Expired delivery retains scheduled slot and stable trigger ID.
                if lease is None:
                    interval = row['interval_seconds']
                    if interval and now - row['due_at'] >= interval:
                        missed = int((now - row['due_at']) // interval)
                        if row['missed'] == 'skip':
                            row['due_at'] += (missed + 1) * interval
                            continue
                        if row['missed'] == 'coalesce':
                            row['due_at'] += missed * interval
                        elif missed >= ctx.config.max_catch_up:
                            row['due_at'] += (missed - ctx.config.max_catch_up + 1) * interval
                trigger_id = digest([owner(call), row['schedule_id'], row['due_at']])
                row['lease'] = {'trigger_id': trigger_id, 'token': uuid.uuid4().hex,
                                'expires_at': now + ctx.config.lease_seconds}
                triggers.append(dict(row['lease'], schedule_id=row['schedule_id'],
                                     scheduled_at=row['due_at'], payload=row['payload']))
            return {'status': 'claimed', 'triggers': triggers}
        return await self.mutate(ctx, a, call, 'claim', update)

    @service('ack', with_context=True)
    async def ack(self, ctx, a, call):
        if a.get('outcome') not in ('succeeded', 'failed', 'unknown'):
            raise ServiceRejected('invalid_outcome')
        def update(state, archived):
            row = state['schedules'].get(a.get('schedule_id'))
            lease = row.get('lease') if row else None
            if not lease or lease['token'] != a.get('token') or lease['trigger_id'] != a.get('trigger_id'):
                raise ServiceRejected('lease_conflict')
            if lease['expires_at'] <= time.time():
                raise ServiceRejected('lease_expired')
            row['last_ack'] = dict(trigger_id=lease['trigger_id'], outcome=a['outcome'], at=time.time())
            row['lease'] = None
            row['completed'] += 1
            if a['outcome'] == 'unknown':
                row['state'] = 'paused_unknown'
            elif row['interval_seconds']:
                row['due_at'] += row['interval_seconds']
            else:
                row['state'] = 'completed'
            return {'status': 'acked', 'schedule': row}
        return await self.mutate(ctx, a, call, 'ack', update)


if __name__ == '__main__':
    Scheduler().run()
