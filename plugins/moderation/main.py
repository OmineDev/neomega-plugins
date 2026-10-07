"""Durable player bans with absolute expiry and recorded kick admissions."""
import asyncio
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from neomega_runtime import Plugin, service, ServiceRejected
from neomega_runtime.storage import CommitUncertain
from neomega_runtime.managed import IPCRejected
from neomega_runtime.players import player_target


def owner(call):
    if call is None or not call.installation_id:
        raise ServiceRejected('trusted_context_required')
    return call.installation_id


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def timestamp(value):
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError()
        return parsed.timestamp()
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise ServiceRejected('invalid_expiry') from None


@dataclass
class Settings:
    writer_installations: list[str] = field(default_factory=list, metadata={'title': '允许处罚的插件安装 ID', 'description': '允许封禁/解封的调用安装 ID；空列表禁止写入。'})
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

    async def initialize(self, ctx):
        self.pending_path = ctx.data_dir / 'moderation-pending.json'
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
            if self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            if request['operation'] == 'seal':
                self.sealed = token
                return {'status': 'sealed', 'token': token}
            self.sealed = None
            return {'status': 'released', 'token': token}

    def active(self, row):
        return bool(row and row['state'] == 'banned' and
                    (row['expires_at'] is None or row['expires_at'] > time.time()))

    async def change(self, ctx, a, call, action):
        if owner(call) not in ctx.config.writer_installations:
            raise ServiceRejected('writer_not_authorized')
        request = a.get('request_id')
        pid = a.get('player_id')
        if not isinstance(request, str) or not 1 <= len(request) <= 128:
            raise ServiceRejected('invalid_request_id')
        if not isinstance(pid, str) or len(pid) > 160 or not pid.startswith(('uuid:', 'xuid:')) or not pid.split(':', 1)[1]:
            raise ServiceRejected('invalid_player_id')
        expiry = timestamp(a.get('expires_at')) if action == 'ban' else None
        reason = a.get('reason', '')
        if not isinstance(reason, str) or len(reason.encode()) > 512 or any(ord(c) < 32 for c in reason):
            raise ServiceRejected('invalid_reason')
        key, receipt_key = 'ban:' + digest(pid), 'request:' + digest([owner(call), request])
        fingerprint = digest([action, a])
        async with self.lock:
            if self.sealed is not None:
                raise ServiceRejected('maintenance_sealed')
            if not await self.reconcile_commit(ctx):
                raise ServiceRejected('commit_unresolved')
            tx = await ctx.storage.transaction(keys=['registry', key, receipt_key])
            previous = tx.get(receipt_key)
            if previous:
                if previous['fingerprint'] != fingerprint:
                    raise ServiceRejected('idempotency_conflict')
                return previous['result']
            if expiry is not None and expiry <= time.time():
                raise ServiceRejected('expiry_in_past')
            row = tx.get(key)
            revision = row['revision'] if row else 0
            if type(a.get('expected_revision')) is not int or a['expected_revision'] != revision:
                raise ServiceRejected('revision_conflict')
            registry = tx.get('registry', [])
            if key not in registry:
                if len(registry) >= ctx.config.max_players:
                    raise ServiceRejected('player_limit')
                registry.append(key)
            cleanup = row.get('expiry_cleanup', []) if row else []
            if row and row['expires_at'] is not None:
                cleanup.append({'schedule_id': self.schedule_id(row),
                                'requested': row.get('expiry_requested', True)})
            row = {'player_id': pid, 'state': 'banned' if action == 'ban' else 'unbanned',
                   'revision': revision + 1, 'reason': reason, 'expires_at': expiry,
                   'actor': owner(call), 'updated_at': time.time(), 'present': False,
                   'enforcement_sequence': 0, 'enforcement': None, 'expiry_scheduled': False,
                   'expiry_requested': False, 'expiry_cleanup': cleanup}
            result = {'status': 'saved', 'ban': row, 'active': self.active(row)}
            tx.set(key, row).set('registry', registry)
            tx.set(receipt_key, {'fingerprint': fingerprint, 'operation': action, 'actor': owner(call),
                                'at': time.time(), 'result': result})
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
        row = await ctx.storage.get('ban:' + digest(a.get('player_id')))
        return {'active': self.active(row), 'ban': row, 'revision': row['revision'] if row else 0}

    @service('list', with_context=True)
    async def list_bans(self, ctx, a, call):
        owner(call)
        offset = a.get('offset', 0)
        if type(offset) is not int or offset < 0:
            raise ServiceRejected('invalid_offset')
        keys = await ctx.storage.get('registry', [])
        rows = [await ctx.storage.get(key) for key in keys[offset:offset + 16]]
        return {'bans': [dict(row, active=self.active(row)) for row in rows],
                'next_offset': offset + 16 if offset + 16 < len(keys) else None}

    async def on_start(self, ctx):
        await self.initialize(ctx)
        ctx.spawn(self.run(ctx), name='moderation-maintenance')

    async def run(self, ctx):
        while True:
            try:
                await self.sweep(ctx)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Read/receipt failures leave the durable original admission untouched.
                ctx.log.warning('moderation sweep incomplete: %s', type(exc).__name__)
            await asyncio.sleep(ctx.config.poll_seconds)

    async def sweep(self, ctx):
        async with self.lock:
            if self.sealed is not None:
                return
            if not await self.reconcile_commit(ctx):
                return
            roster = await ctx.players() if ctx.config.enforce else []
            keys = await ctx.storage.get('registry', [])
            for key in keys:
                try:
                    await self.sweep_player(ctx, key, roster)
                except Exception as exc:
                    ctx.log.warning('moderation player sweep incomplete: %s', type(exc).__name__)
                # A failed local commit fences all subsequent writes, even when
                # a remote provider failure is isolated to one player.
                if not await self.reconcile_commit(ctx):
                    return
            await self.consume_expiry(ctx)

    async def sweep_player(self, ctx, key, roster):
        row = await ctx.storage.get(key)
        await self.cleanup_expiry(ctx, key, row)
        if row['state'] != 'banned':
            return
        if row['expires_at'] is not None and row['expires_at'] <= time.time():
            tx = await ctx.storage.transaction(keys=[key])
            row.setdefault('expiry_cleanup', []).append({
                'schedule_id': self.schedule_id(row),
                'requested': row.get('expiry_requested', True)})
            row['state'] = 'expired'
            tx.set(key, row)
            await self.save(ctx, tx)
            return
        if row['expires_at'] is not None and not row['expiry_scheduled']:
            try:
                await self.schedule_expiry(ctx, key, row)
            except Exception as exc:
                ctx.log.warning('moderation expiry scheduling incomplete: %s', type(exc).__name__)
                if not await self.reconcile_commit(ctx):
                    return
        if not ctx.config.enforce:
            return
        kind, value = row['player_id'].split(':', 1)
        matches = [p for p in roster if p.get(kind) == value]
        if len(matches) != 1:
            if row['present']:
                row['present'] = False
                tx = await ctx.storage.transaction(keys=[key])
                tx.set(key, row)
                await self.save(ctx, tx)
            return
        # Cross-check the identity provider; names alone never identify a ban.
        resolved = await ctx.services.call('plugin.neomega.players.resolve', {kind: value})
        if resolved.get('status') != 'resolved' or resolved['player'].get(kind) != value:
            return
        if row['present'] and row.get('session') == ctx.operations.session:
            await self.reconcile_enforcement(ctx, key, row)
            return
        # Each observed rejoin is a new enforcement; never replay an uncertain kick.
        if row.get('enforcement') and row['enforcement']['state'] in ('pending', 'unknown'):
            await self.reconcile_enforcement(ctx, key, row)
            return
        tx = await ctx.storage.transaction(keys=[key])
        commit_id = uuid.uuid4().hex
        tx.commit_id = commit_id
        row['present'] = True
        row['session'] = ctx.operations.session
        row['enforcement_sequence'] += 1
        row['enforcement'] = {'commit_id': commit_id, 'state': 'pending', 'operation_id': None}
        command = 'kick ' + player_target(matches[0]['name']) + ' ' + json.dumps(row['reason'] or '已被封禁', ensure_ascii=False)
        intent = ctx.commands.prepare(command, idempotency_key=commit_id,
                    deadline=(datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat())
        tx.set('enforcement:' + commit_id, dict(row['enforcement'], player_id=row['player_id'], ban_revision=row['revision'], sequence=row['enforcement_sequence']))
        tx.set(key, row).action(intent)
        await self.save(ctx, tx)
        await self.reconcile_enforcement(ctx, key, row)

    async def reconcile_enforcement(self, ctx, key, row):
        evidence = row.get('enforcement')
        if not evidence or evidence['state'] not in ('pending', 'unknown'):
            return
        receipt = await ctx.storage.receipt(evidence['commit_id'])
        if receipt and receipt['operation_ids']:
            evidence['operation_id'] = receipt['operation_ids'][0]
            operation = await ctx.operations.get(evidence['operation_id'])
            evidence['state'] = operation['state'] if operation['state'] in ('succeeded', 'failed', 'cancelled') else 'unknown'
            evidence['receipt'] = {'state': operation['state'], 'operation_id': evidence['operation_id']}
        else:
            evidence['state'] = 'unknown'
        tx = await ctx.storage.transaction(keys=[key])
        tx.set(key, row)
        tx.set('enforcement:' + evidence['commit_id'], dict(evidence, player_id=row['player_id'], ban_revision=row['revision'], sequence=row['enforcement_sequence']))
        await self.save(ctx, tx)

    def schedule_id(self, row):
        return 'ban-' + digest([row['player_id'], row['revision']])

    async def cleanup_expiry(self, ctx, key, row):
        for item in list(row.get('expiry_cleanup', [])):
            try:
                sid = item['schedule_id']
                current = await ctx.services.call('plugin.neomega.scheduler.query', {'schedule_id': sid})
                if current.get('status') == 'not_found':
                    # An unanswered registration might still commit remotely.
                    if item['requested']:
                        continue
                elif current.get('status') == 'found':
                    if current['schedule']['state'] not in ('completed', 'cancelled'):
                        # Stable request and payload: scheduler durably deduplicates
                        # cancellation even if its response was lost.
                        await ctx.services.call('plugin.neomega.scheduler.cancel', {
                            'request_id': 'cancel-' + sid, 'schedule_id': sid})
                else:
                    continue
                row['expiry_cleanup'].remove(item)
                tx = await ctx.storage.transaction(keys=[key])
                tx.set(key, row)
                await self.save(ctx, tx)
            except Exception as exc:
                ctx.log.warning('moderation expiry cleanup incomplete: %s', type(exc).__name__)
                if not await self.reconcile_commit(ctx):
                    raise ServiceRejected('commit_unresolved') from exc

    async def schedule_expiry(self, ctx, key, row):
        sid = self.schedule_id(row)
        current = await ctx.services.call('plugin.neomega.scheduler.query', {'schedule_id': sid})
        if current.get('status') == 'not_found':
            if row.get('expiry_requested'):
                return  # Query an uncertain admission; never resubmit it.
            tx = await ctx.storage.transaction(keys=[key])
            row['expiry_requested'] = True
            tx.set(key, row)
            await self.save(ctx, tx)
            try:
                await ctx.services.call('plugin.neomega.scheduler.schedule', {
                    'request_id': sid, 'schedule_id': sid,
                    'due_at': datetime.fromtimestamp(row['expires_at'], timezone.utc).isoformat(),
                    'payload': {'player_id': row['player_id'], 'revision': row['revision']}})
            except ServiceRejected:
                row['expiry_requested'] = False
                tx = await ctx.storage.transaction(keys=[key])
                tx.set(key, row)
                await self.save(ctx, tx)
                raise
        elif current.get('status') != 'found':
            return
        tx = await ctx.storage.transaction(keys=[key])
        row['expiry_scheduled'] = True
        tx.set(key, row)
        await self.save(ctx, tx)

    async def consume_expiry(self, ctx):
        claimed = await ctx.services.call('plugin.neomega.scheduler.claim', {'request_id': uuid.uuid4().hex, 'limit': 16})
        for trigger in claimed['triggers']:
            try:
                key = 'ban:' + digest(trigger['payload']['player_id'])
                tx = await ctx.storage.transaction(keys=[key])
                row = tx.get(key)
                if row and row['state'] == 'banned' and row['revision'] == trigger['payload']['revision'] and row['expires_at'] is not None and row['expires_at'] <= time.time():
                    row.setdefault('expiry_cleanup', []).append({
                        'schedule_id': self.schedule_id(row),
                        'requested': row.get('expiry_requested', True)})
                    row['state'] = 'expired'
                    tx.set(key, row)
                    await self.save(ctx, tx)
                await ctx.services.call('plugin.neomega.scheduler.ack', {
                    'request_id': 'ack-' + trigger['token'], 'schedule_id': trigger['schedule_id'],
                    'trigger_id': trigger['trigger_id'], 'token': trigger['token'], 'outcome': 'succeeded'})
            except Exception as exc:
                ctx.log.warning('moderation expiry trigger incomplete: %s', type(exc).__name__)
                if not await self.reconcile_commit(ctx):
                    return


if __name__ == '__main__':
    Moderation().run()
