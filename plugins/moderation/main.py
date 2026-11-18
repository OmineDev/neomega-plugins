"""Durable player bans with absolute expiry and recorded kick admissions."""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import time
import uuid

from neomega_runtime import Plugin, ServiceRejected, service
from neomega_runtime.http import ProtocolError
from neomega_runtime.managed import IPCError, IPCRejected
from neomega_runtime.players import player_target
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from neomega_runtime.storage import CommitUncertain

SCHEMA_VERSION = 1
MAX_EXPIRY_CLEANUP = 16
MAX_PLAYER_ID_BYTES = 160
MAX_REASON_BYTES = 512
LIST_PAGE_SIZE = 16
SCHEDULER_UNAVAILABLE_CODES = frozenset({
    'unavailable',
    'provider_unavailable',
    'service_unavailable',
    'permission_denied',
    'not_found',
})
SCHEDULER_SERVICES = (
    'plugin.neomega.scheduler.schedule',
    'plugin.neomega.scheduler.query',
    'plugin.neomega.scheduler.claim',
    'plugin.neomega.scheduler.ack',
    'plugin.neomega.scheduler.cancel',
)


def owner(call):
    if call is None or not call.installation_id:
        raise ServiceRejected('trusted_context_required')
    return call.installation_id


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def valid_player_id(pid) -> bool:
    if not isinstance(pid, str) or len(pid.encode('utf-8')) > MAX_PLAYER_ID_BYTES:
        return False
    if any(ord(c) < 32 for c in pid):
        return False
    if not pid.startswith(('uuid:', 'xuid:')):
        return False
    return bool(pid.split(':', 1)[1])


def timestamp(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ServiceRejected('invalid_expiry')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.timestamp()
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise ServiceRejected('invalid_expiry') from None


def dedupe_cleanup(items, *, limit: int = MAX_EXPIRY_CLEANUP) -> list[dict]:
    if not isinstance(items, list):
        return []
    ordered: dict[str, dict] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        sid = item.get('schedule_id')
        if not isinstance(sid, str) or not sid:
            continue
        requested = bool(item.get('requested', False))
        if sid in ordered:
            ordered[sid]['requested'] = ordered[sid]['requested'] or requested
        else:
            ordered[sid] = {'schedule_id': sid, 'requested': requested}
    deduped = list(ordered.values())
    if limit > 0 and len(deduped) > limit:
        return deduped[-limit:]
    return deduped


@dataclass(frozen=True)
class Settings:
    writer_installations: list[str] = field(default_factory=list, metadata={
        'title': '允许处罚的插件安装 ID',
        'description': '允许封禁/解封的调用安装 ID；空列表禁止写入。',
    })
    enforce: bool = field(default=True, metadata={'title': '执行踢出'})
    poll_seconds: int = field(default=5, metadata={'title': '扫描间隔（秒）', 'minimum': 1, 'maximum': 60})
    max_players: int = field(default=256, metadata={'title': '最大玩家档案数', 'minimum': 1, 'maximum': 1024})


class Moderation(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = None
        self.pending = None
        self.pending_path = None
        self.schema_ready = False
        self.scheduler_available = None

    def now(self) -> float:
        return time.time()

    async def initialize(self, ctx):
        self.pending_path = ctx.data_dir / 'moderation-pending.json'
        if self.pending_path.exists():
            self.pending = json.loads(self.pending_path.read_text())['commit_id']
        async with self.lock:
            reconciled = await self.reconcile_commit(ctx)
            version = await ctx.storage.get('schema_version')
            if version is not None:
                if type(version) is not int or version != SCHEMA_VERSION:
                    raise RuntimeError('unsupported schema_version; migration required')
                self.schema_ready = True
                return
            if not reconciled:
                self.schema_ready = False
                return
            await self._init_schema(ctx)

    async def ensure_schema(self, ctx):
        if self.schema_ready:
            return
        await self._init_schema(ctx)

    async def _init_schema(self, ctx):
        tx = await ctx.storage.transaction(keys=['schema_version'])
        version = tx.get('schema_version')
        if version is None:
            tx.set('schema_version', SCHEMA_VERSION)
            await self.save(ctx, tx)
        elif type(version) is not int or version != SCHEMA_VERSION:
            raise RuntimeError('unsupported schema_version; migration required')
        self.schema_ready = True

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
            if self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            if request['operation'] == 'seal':
                self.sealed = token
                return {'status': 'sealed', 'token': token}
            self.sealed = None
            return {'status': 'released', 'token': token}

    def active(self, row):
        return bool(row and row['state'] == 'banned' and
                    (row['expires_at'] is None or row['expires_at'] > self.now()))

    def _scheduler_enabled(self, ctx) -> bool:
        caps = getattr(ctx, 'capabilities', None)
        if isinstance(caps, dict) and isinstance(caps.get('services'), (list, tuple, set)):
            available = set(caps['services'])
            return all(svc in available for svc in SCHEDULER_SERVICES)
        return True

    def _is_scheduler_unavailable(self, exc: BaseException) -> bool:
        if isinstance(exc, IPCRejected):
            return True
        return isinstance(exc, ServiceRejected) and exc.code in SCHEDULER_UNAVAILABLE_CODES

    def _mark_scheduler_unavailable(self, ctx):
        if self.scheduler_available is not False:
            self.scheduler_available = False
            ctx.log.info('neomega.scheduler unavailable; using built-in periodic expiry sweep')

    def _mark_scheduler_available(self, ctx):
        if self.scheduler_available is False:
            ctx.log.info('neomega.scheduler available; resuming scheduler integration')
        self.scheduler_available = True

    def enqueue_cleanup(self, row) -> list[dict]:
        cleanup = dedupe_cleanup(row.get('expiry_cleanup', []) if row else [])
        if row and row.get('expires_at') is not None and (row.get('expiry_requested') or row.get('expiry_scheduled')):
            cleanup.append({
                'schedule_id': self.schedule_id(row),
                'requested': bool(row.get('expiry_requested')),
            })
            cleanup = dedupe_cleanup(cleanup)
        return cleanup

    async def change(self, ctx, a, call, action):
        caller = owner(call)
        if caller not in ctx.config.writer_installations:
            raise ServiceRejected('writer_not_authorized')
        if not isinstance(a, dict):
            raise ServiceRejected('invalid_argument')
        allowed = (
            {'request_id', 'player_id', 'expected_revision', 'expires_at', 'reason'}
            if action == 'ban'
            else {'request_id', 'player_id', 'expected_revision', 'reason'}
        )
        if set(a) - allowed:
            raise ServiceRejected('invalid_argument')
        request = a.get('request_id')
        pid = a.get('player_id')
        if not isinstance(request, str) or not 1 <= len(request) <= 128:
            raise ServiceRejected('invalid_request_id')
        if not valid_player_id(pid):
            raise ServiceRejected('invalid_player_id')
        expected_revision = a.get('expected_revision')
        if type(expected_revision) is not int or expected_revision < 0:
            raise ServiceRejected('invalid_argument')
        expiry = timestamp(a.get('expires_at')) if action == 'ban' else None
        reason = a.get('reason', '')
        if not isinstance(reason, str) or len(reason.encode('utf-8')) > MAX_REASON_BYTES or any(ord(c) < 32 for c in reason):
            raise ServiceRejected('invalid_reason')
        key, receipt_key = 'ban:' + digest(pid), 'request:' + digest([caller, request])
        fingerprint = digest([action, a])
        async with self.lock:
            if self.sealed is not None:
                raise ServiceRejected('maintenance_sealed')
            if not await self.reconcile_commit(ctx):
                raise ServiceRejected('commit_unresolved')
            await self.ensure_schema(ctx)
            tx = await ctx.storage.transaction(keys=['registry', key, receipt_key])
            previous = tx.get(receipt_key)
            if previous:
                if previous['fingerprint'] != fingerprint:
                    raise ServiceRejected('idempotency_conflict')
                return previous['result']
            now = self.now()
            if expiry is not None and expiry <= now:
                raise ServiceRejected('expiry_in_past')
            row = tx.get(key)
            revision = row['revision'] if row else 0
            if expected_revision != revision:
                raise ServiceRejected('revision_conflict')
            registry = tx.get('registry', [])
            if key not in registry:
                if len(registry) >= ctx.config.max_players:
                    raise ServiceRejected('player_limit')
                registry.append(key)
            cleanup = self.enqueue_cleanup(row)
            row = {
                'player_id': pid,
                'state': 'banned' if action == 'ban' else 'unbanned',
                'revision': revision + 1,
                'reason': reason,
                'expires_at': expiry,
                'actor': caller,
                'updated_at': now,
                'present': False,
                'enforcement_sequence': 0,
                'enforcement': None,
                'expiry_scheduled': False,
                'expiry_requested': False,
                'expiry_cleanup': cleanup,
            }
            result = {'status': 'saved', 'ban': row, 'active': self.active(row)}
            tx.set(key, row).set('registry', registry)
            tx.set(receipt_key, {
                'fingerprint': fingerprint,
                'operation': action,
                'actor': caller,
                'at': now,
                'result': result,
            })
            await self.save(ctx, tx)
            return result

    @service('ban', with_context=True)
    async def ban(self, ctx, a, call):
        return await self.change(ctx, a, call, 'ban')

    @service('unban', with_context=True)
    async def unban(self, ctx, a, call):
        return await self.change(ctx, a, call, 'unban')

    @service('check', with_context=True)
    async def check(self, ctx, a, call):
        owner(call)
        if not isinstance(a, dict) or set(a) != {'player_id'} or not valid_player_id(a.get('player_id')):
            raise ServiceRejected('invalid_argument')
        row = await ctx.storage.get('ban:' + digest(a['player_id']))
        return {'active': self.active(row), 'ban': row, 'revision': row['revision'] if row else 0}

    @service('list', with_context=True)
    async def list_bans(self, ctx, a, call):
        owner(call)
        if not isinstance(a, dict) or set(a) - {'offset'}:
            raise ServiceRejected('invalid_argument')
        offset = a.get('offset', 0)
        if type(offset) is not int or offset < 0:
            raise ServiceRejected('invalid_offset')
        keys = await ctx.storage.get('registry', [])
        page_keys = keys[offset:offset + LIST_PAGE_SIZE]
        if page_keys:
            snap = await ctx.storage.transaction(keys=page_keys)
            rows = [snap.get(key) for key in page_keys]
        else:
            rows = []
        return {
            'bans': [dict(row, active=self.active(row)) for row in rows if row is not None],
            'next_offset': offset + LIST_PAGE_SIZE if offset + LIST_PAGE_SIZE < len(keys) else None,
        }

    async def on_start(self, ctx):
        await self.initialize(ctx)
        ctx.every(ctx.config.poll_seconds, lambda: self.sweep_tick(ctx), immediate=True, name='moderation-maintenance')

    async def sweep_tick(self, ctx):
        try:
            await self.sweep(ctx)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Read/receipt failures leave the durable original admission untouched.
            ctx.log.warning('moderation sweep incomplete: %s', type(exc).__name__)

    async def sweep(self, ctx):
        async with self.lock:
            if self.sealed is not None:
                return
            if not await self.reconcile_commit(ctx):
                return
            await self.ensure_schema(ctx)
        roster = None
        if ctx.config.enforce:
            try:
                roster = await ctx.players()
            except (IPCError, ProtocolError, TimeoutError, ConnectionError, OSError) as exc:
                ctx.log.warning('moderation player list unavailable: %s', type(exc).__name__)
        keys = await ctx.storage.get('registry', [])
        scheduler_ok = self._scheduler_enabled(ctx)
        if not scheduler_ok:
            self._mark_scheduler_unavailable(ctx)
        for key in keys:
            try:
                scheduler_ok = await self.sweep_player(ctx, key, roster, scheduler_ok=scheduler_ok)
            except Exception as exc:
                ctx.log.warning('moderation player sweep incomplete: %s', type(exc).__name__)
            # A failed local commit fences all subsequent writes, even when
            # a remote provider failure is isolated to one player.
            async with self.lock:
                if self.sealed is not None:
                    return
                if not await self.reconcile_commit(ctx):
                    return
        if scheduler_ok:
            await self.consume_expiry(ctx)

    async def sweep_player(self, ctx, key, roster, *, scheduler_ok: bool = True) -> bool:
        row = await ctx.storage.get(key)
        if row is None:
            return scheduler_ok
        if scheduler_ok and row.get('expiry_cleanup'):
            scheduler_ok = await self.cleanup_expiry(ctx, key, row)
            row = await ctx.storage.get(key)
            if row is None:
                return scheduler_ok
        if row['state'] != 'banned':
            return scheduler_ok
        if row['expires_at'] is not None and row['expires_at'] <= self.now():
            async with self.lock:
                if self.sealed is not None or not await self.reconcile_commit(ctx):
                    return scheduler_ok
                tx = await ctx.storage.transaction(keys=[key])
                fresh = tx.get(key)
                if (fresh is None or fresh['revision'] != row['revision'] or fresh['state'] != 'banned'
                        or fresh['expires_at'] is None or fresh['expires_at'] > self.now()):
                    return scheduler_ok
                fresh['expiry_cleanup'] = self.enqueue_cleanup(fresh)
                fresh['state'] = 'expired'
                tx.set(key, fresh)
                await self.save(ctx, tx)
            return scheduler_ok
        if row['expires_at'] is not None and not row.get('expiry_scheduled') and scheduler_ok:
            try:
                scheduler_ok = await self.schedule_expiry(ctx, key, row)
            except Exception as exc:
                ctx.log.warning('moderation expiry scheduling incomplete: %s', type(exc).__name__)
                async with self.lock:
                    if not await self.reconcile_commit(ctx):
                        return scheduler_ok
            row = await ctx.storage.get(key)
            if row is None or row['state'] != 'banned':
                return scheduler_ok
        if not ctx.config.enforce or roster is None:
            return scheduler_ok
        kind, value = row['player_id'].split(':', 1)
        matches = [p for p in roster if p.get(kind) == value]
        if len(matches) != 1:
            if row.get('present'):
                async with self.lock:
                    if self.sealed is not None or not await self.reconcile_commit(ctx):
                        return scheduler_ok
                    tx = await ctx.storage.transaction(keys=[key])
                    fresh = tx.get(key)
                    if fresh is not None and fresh['revision'] == row['revision'] and fresh.get('present'):
                        fresh['present'] = False
                        tx.set(key, fresh)
                        await self.save(ctx, tx)
            return scheduler_ok
        target_name = matches[0].get('name')
        if not isinstance(target_name, str) or not target_name:
            return scheduler_ok
        # Cross-check the identity provider; names alone never identify a ban.
        try:
            resolved = await ctx.services.call('plugin.neomega.players.resolve', {kind: value})
        except ServiceRejected as exc:
            if exc.code != 'not_found':
                ctx.log.warning('moderation player resolve rejected: %s', exc.code)
            return scheduler_ok
        except (IPCError, ProtocolError, TimeoutError, ConnectionError, OSError,
                ServiceWaitTimeout, ServiceUncertain, ServiceFailed) as exc:
            ctx.log.warning('moderation player resolve incomplete: %s', type(exc).__name__)
            return scheduler_ok
        if not isinstance(resolved, dict) or resolved.get(kind) != value:
            return scheduler_ok
        if kind == 'xuid' and matches[0].get('uuid') and resolved.get('uuid') and matches[0]['uuid'] != resolved['uuid']:
            return scheduler_ok
        if row.get('present') and row.get('session') == ctx.operations.session:
            await self.reconcile_enforcement(ctx, key, row)
            return scheduler_ok
        # Each observed rejoin is a new enforcement; never replay an uncertain kick.
        if row.get('enforcement') and row['enforcement']['state'] in ('pending', 'unknown'):
            await self.reconcile_enforcement(ctx, key, row)
            return scheduler_ok
        target = player_target(target_name)
        commit_id = uuid.uuid4().hex
        async with self.lock:
            if self.sealed is not None or not await self.reconcile_commit(ctx):
                return scheduler_ok
            tx = await ctx.storage.transaction(keys=[key], commit_id=commit_id)
            fresh = tx.get(key)
            if (fresh is None or fresh['revision'] != row['revision'] or fresh['state'] != 'banned'
                    or (fresh['expires_at'] is not None and fresh['expires_at'] <= self.now())):
                return scheduler_ok
            if fresh.get('present') and fresh.get('session') == ctx.operations.session:
                row = fresh
            elif fresh.get('enforcement') and fresh['enforcement']['state'] in ('pending', 'unknown'):
                row = fresh
            else:
                fresh['present'] = True
                fresh['session'] = ctx.operations.session
                fresh['enforcement_sequence'] += 1
                fresh['enforcement'] = {'commit_id': commit_id, 'state': 'pending', 'operation_id': None}
                command = 'kick ' + target + ' ' + json.dumps(fresh['reason'] or '已被封禁', ensure_ascii=False)
                intent = ctx.commands.prepare(
                    command,
                    idempotency_key=commit_id,
                    deadline=(datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat(),
                )
                tx.set('enforcement:' + commit_id, dict(
                    fresh['enforcement'],
                    player_id=fresh['player_id'],
                    ban_revision=fresh['revision'],
                    sequence=fresh['enforcement_sequence'],
                ))
                tx.set(key, fresh).action(intent)
                await self.save(ctx, tx)
                row = fresh
        await self.reconcile_enforcement(ctx, key, row)
        return scheduler_ok

    async def reconcile_enforcement(self, ctx, key, row):
        if row is None or not row.get('enforcement'):
            return
        evidence = dict(row['enforcement'])
        if evidence.get('state') not in ('pending', 'unknown'):
            return
        receipt = await ctx.storage.receipt(evidence['commit_id'])
        if receipt and receipt['operation_ids']:
            evidence['operation_id'] = receipt['operation_ids'][0]
            operation = await ctx.operations.get(evidence['operation_id'])
            evidence['state'] = operation['state'] if operation['state'] in ('succeeded', 'failed', 'cancelled') else 'unknown'
            evidence['receipt'] = {'state': operation['state'], 'operation_id': evidence['operation_id']}
        else:
            evidence['state'] = 'unknown'
        async with self.lock:
            if self.sealed is not None or not await self.reconcile_commit(ctx):
                return
            tx = await ctx.storage.transaction(keys=[key])
            fresh = tx.get(key)
            if (fresh is None or fresh['revision'] != row['revision']
                    or not fresh.get('enforcement')
                    or fresh['enforcement'].get('commit_id') != evidence['commit_id']):
                return
            if evidence == fresh['enforcement']:
                return
            fresh['enforcement'] = evidence
            tx.set(key, fresh)
            tx.set('enforcement:' + evidence['commit_id'], dict(
                evidence,
                player_id=fresh['player_id'],
                ban_revision=fresh['revision'],
                sequence=fresh['enforcement_sequence'],
            ))
            await self.save(ctx, tx)

    def schedule_id(self, row):
        return 'ban-' + digest([row['player_id'], row['revision']])

    async def cleanup_expiry(self, ctx, key, row) -> bool:
        if row is None:
            return True
        items = dedupe_cleanup(row.get('expiry_cleanup', []))
        if not items:
            return True
        for item in items:
            sid = item['schedule_id']
            try:
                current = await ctx.services.call('plugin.neomega.scheduler.query', {'schedule_id': sid})
                self._mark_scheduler_available(ctx)
                if current.get('status') == 'not_found':
                    # An unanswered registration might still commit remotely.
                    if item.get('requested'):
                        continue
                elif current.get('status') == 'found':
                    if current['schedule']['state'] not in ('completed', 'cancelled'):
                        # Stable request and payload: scheduler durably deduplicates
                        # cancellation even if its response was lost.
                        await ctx.services.call('plugin.neomega.scheduler.cancel', {
                            'request_id': 'cancel-' + sid, 'schedule_id': sid,
                        })
                else:
                    continue
                async with self.lock:
                    if self.sealed is not None:
                        return True
                    if not await self.reconcile_commit(ctx):
                        raise ServiceRejected('commit_unresolved')
                    tx = await ctx.storage.transaction(keys=[key])
                    fresh = tx.get(key)
                    if fresh is None:
                        return True
                    existing = dedupe_cleanup(fresh.get('expiry_cleanup', []))
                    updated = [entry for entry in existing if entry.get('schedule_id') != sid]
                    if updated != fresh.get('expiry_cleanup', []):
                        fresh['expiry_cleanup'] = updated
                        tx.set(key, fresh)
                        await self.save(ctx, tx)
            except Exception as exc:
                if self._is_scheduler_unavailable(exc):
                    self._mark_scheduler_unavailable(ctx)
                    return False
                ctx.log.warning('moderation expiry cleanup incomplete: %s', type(exc).__name__)
                async with self.lock:
                    if not await self.reconcile_commit(ctx):
                        raise ServiceRejected('commit_unresolved') from exc
        return True

    async def schedule_expiry(self, ctx, key, row) -> bool:
        if row is None:
            return True
        sid = self.schedule_id(row)
        expected_rev = row['revision']
        try:
            current = await ctx.services.call('plugin.neomega.scheduler.query', {'schedule_id': sid})
        except (IPCRejected, ServiceRejected) as exc:
            if self._is_scheduler_unavailable(exc):
                self._mark_scheduler_unavailable(ctx)
                return False
            raise
        self._mark_scheduler_available(ctx)
        if current.get('status') == 'not_found':
            if row.get('expiry_requested'):
                return True  # Query an uncertain admission; never resubmit it.
            due_at = datetime.fromtimestamp(row['expires_at'], timezone.utc).isoformat()
            payload = {'player_id': row['player_id'], 'revision': expected_rev}
            async with self.lock:
                if self.sealed is not None or not await self.reconcile_commit(ctx):
                    return True
                tx = await ctx.storage.transaction(keys=[key])
                fresh = tx.get(key)
                if (fresh is None or fresh['revision'] != expected_rev or fresh['state'] != 'banned'
                        or fresh['expires_at'] is None or fresh.get('expiry_scheduled')
                        or fresh.get('expiry_requested')):
                    return True
                fresh['expiry_requested'] = True
                tx.set(key, fresh)
                await self.save(ctx, tx)
            try:
                await ctx.services.call('plugin.neomega.scheduler.schedule', {
                    'request_id': sid,
                    'schedule_id': sid,
                    'due_at': due_at,
                    'payload': payload,
                })
            except (ServiceRejected, IPCRejected) as exc:
                async with self.lock:
                    if self.sealed is None and await self.reconcile_commit(ctx):
                        tx = await ctx.storage.transaction(keys=[key])
                        fresh = tx.get(key)
                        if (fresh is not None and fresh['revision'] == expected_rev
                                and fresh.get('expiry_requested') and not fresh.get('expiry_scheduled')):
                            fresh['expiry_requested'] = False
                            tx.set(key, fresh)
                            await self.save(ctx, tx)
                if self._is_scheduler_unavailable(exc):
                    self._mark_scheduler_unavailable(ctx)
                    return False
                raise
        elif current.get('status') != 'found':
            return True
        async with self.lock:
            if self.sealed is not None or not await self.reconcile_commit(ctx):
                return True
            tx = await ctx.storage.transaction(keys=[key])
            fresh = tx.get(key)
            if (fresh is not None and fresh['revision'] == expected_rev
                    and fresh['state'] == 'banned' and not fresh.get('expiry_scheduled')):
                fresh['expiry_scheduled'] = True
                tx.set(key, fresh)
                await self.save(ctx, tx)
        return True

    async def consume_expiry(self, ctx):
        try:
            claimed = await ctx.services.call(
                'plugin.neomega.scheduler.claim',
                {'request_id': uuid.uuid4().hex, 'limit': 16},
            )
        except (IPCRejected, ServiceRejected) as exc:
            if self._is_scheduler_unavailable(exc):
                self._mark_scheduler_unavailable(ctx)
                return
            ctx.log.warning('moderation expiry claim rejected: %s', type(exc).__name__)
            return
        except (ServiceWaitTimeout, ServiceUncertain, ServiceFailed,
                IPCError, ProtocolError, TimeoutError, ConnectionError, OSError) as exc:
            ctx.log.warning('moderation expiry claim incomplete: %s', type(exc).__name__)
            return
        self._mark_scheduler_available(ctx)
        for trigger in claimed.get('triggers', []):
            try:
                async with self.lock:
                    if self.sealed is not None:
                        return
                    if not await self.reconcile_commit(ctx):
                        return
                    key = 'ban:' + digest(trigger['payload']['player_id'])
                    tx = await ctx.storage.transaction(keys=[key])
                    row = tx.get(key)
                    if (row and row['state'] == 'banned'
                            and row['revision'] == trigger['payload']['revision']
                            and row['expires_at'] is not None and row['expires_at'] <= self.now()):
                        row['expiry_cleanup'] = self.enqueue_cleanup(row)
                        row['state'] = 'expired'
                        tx.set(key, row)
                        await self.save(ctx, tx)
                await ctx.services.call('plugin.neomega.scheduler.ack', {
                    'request_id': 'ack-' + trigger['token'],
                    'schedule_id': trigger['schedule_id'],
                    'trigger_id': trigger['trigger_id'],
                    'token': trigger['token'],
                    'outcome': 'succeeded',
                })
            except Exception as exc:
                if self._is_scheduler_unavailable(exc):
                    self._mark_scheduler_unavailable(ctx)
                    return
                ctx.log.warning('moderation expiry trigger incomplete: %s', type(exc).__name__)
                async with self.lock:
                    if not await self.reconcile_commit(ctx):
                        return


if __name__ == '__main__':
    Moderation().run()
