"""Bounded shared subchunk requests, verified cache and pull subscriptions."""
import asyncio
import base64
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
import time
import uuid
from pathlib import Path

from neomega_runtime import Plugin
from neomega_runtime.http import ProtocolError
from neomega_runtime.services import service, ServiceRejected
from neomega_runtime.bulk import BulkClient, BulkError, snapshot
from neomega_runtime.subchunk import parse_subchunk
from neomega_runtime.storage import CommitUncertain
from neomega_runtime.managed import IPCError, IPCRejected


def deadline():
    return (datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat()


@dataclass(frozen=True)
class Settings:
    max_requests: int = field(default=128, metadata={'minimum': 1, 'maximum': 1024})
    max_subscriptions: int = field(default=128, metadata={'minimum': 1, 'maximum': 1024})
    cache_seconds: float = field(default=30.0, metadata={'minimum': 1, 'maximum': 3600})
    observe_timeout: float = field(default=15.0, metadata={'minimum': 1, 'maximum': 300})

    def __post_init__(self):
        if type(self.max_requests) is not int or not 1 <= self.max_requests <= 1024:
            raise ValueError('queue limits must be 1..1024')
        if type(self.max_subscriptions) is not int or not 1 <= self.max_subscriptions <= 1024:
            raise ValueError('queue limits must be 1..1024')
        if isinstance(self.cache_seconds, bool) or not math.isfinite(self.cache_seconds) or not 1 <= self.cache_seconds <= 3600:
            raise ValueError('invalid cache or observation timeout')
        if isinstance(self.observe_timeout, bool) or not math.isfinite(self.observe_timeout) or not 1 <= self.observe_timeout <= 300:
            raise ValueError('invalid cache or observation timeout')


class Chunks(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self._io_lock = asyncio.Lock()
        self.sealed = None
        self._sealing = None
        self.tick_cursor = 0
        self.client = None
        self.epoch = None
        self.rows = {}
        self.persisted = {}
        self._evicted = set()
        self.subscribers = {}
        self.path = None

    async def on_maintenance(self, ctx, request):
        token = request['token']
        operation = request['operation']
        if operation != 'seal':
            async with self.lock:
                if self.sealed not in (None, token):
                    return {'status': 'busy', 'token': token}
                self.sealed = None
                self._sealing = None
                return {'status': 'released', 'token': token}

        async with self.lock:
            if self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            if self._sealing not in (None, token):
                return {'status': 'busy', 'token': token}
            self._sealing = token
            check_rows = [
                (key, row['commit_id'], list(row['operation_ids']))
                for key, row in self.rows.items()
                if row['state'] in ('waiting', 'unknown')
                or row.get('receipt', {}).get('state') in ('accepted', 'dispatching', 'pending', 'running', 'unknown')
            ]

        resolved = {}
        busy = False
        try:
            for key, commit_id, opids in check_rows:
                if not opids:
                    found = await ctx.storage.receipt(commit_id)
                    if found is None:
                        busy = True
                        break
                    opids = found['operation_ids']
                receipts = [await ctx.operations.get(i) for i in opids]
                if any(r['state'] not in ('succeeded', 'failed', 'cancelled') for r in receipts):
                    busy = True
                    break
                resolved[key] = (opids, receipts[-1] if receipts else None)

            if busy:
                return {'status': 'busy', 'token': token}

            async with self.lock:
                if self.sealed not in (None, token):
                    return {'status': 'busy', 'token': token}
                for key, (opids, last_receipt) in resolved.items():
                    row = self.rows.get(key)
                    if row is not None:
                        row['operation_ids'] = list(opids)
                        if last_receipt is not None:
                            row['receipt'] = last_receipt
                for row in self.rows.values():
                    if row['state'] in ('waiting', 'unknown') or row.get('receipt', {}).get('state') in ('accepted', 'dispatching', 'pending', 'running', 'unknown'):
                        return {'status': 'busy', 'token': token}
                self.sealed = token
        finally:
            async with self.lock:
                if self._sealing == token:
                    self._sealing = None

        await self.save(ctx)
        return {'status': 'sealed', 'token': token}

    def writable(self):
        if self.sealed is not None or self._sealing is not None:
            raise ServiceRejected('maintenance_sealed')

    async def on_start(self, ctx):
        self.client = BulkClient(max_transfers=1, max_waiters=0)
        self.path = ctx.data_dir / 'verified_chunks'
        self.path.mkdir(parents=True, exist_ok=True)
        self.rows = {p.name.split('.')[0]: json.loads(p.read_text()) for p in self.path.glob('*.request.json')}
        self.persisted = {k: json.dumps(v, sort_keys=True) for k, v in self.rows.items()}
        # A new Worker must revalidate non-terminal observations without reviving
        # already-observed or epoch-expired rows back into waiting.
        for row in self.rows.values():
            state = row.get('state')
            if state in ('observed', 'epoch_expired', 'unobserved', 'failed', 'cancelled'):
                row.setdefault('action_state', row.get('receipt', {}).get(
                    'state', state if state in ('failed', 'cancelled') else 'succeeded'))
                continue
            row['cached_at'] = 0
            row['observation'] = 'waiting'
            row.setdefault('action_state', row.get('receipt', {}).get(
                'state', state if state in ('queued', 'failed', 'cancelled') else 'unknown'))
            self.update_state(row)
        ctx.every(1.0, lambda: self.tick(ctx), name='chunks-pump')

    async def on_stop(self, ctx):
        if self.client:
            await self.client.aclose()

    async def save(self, ctx):
        if self.path is None:
            return
        async with self.lock:
            snapshot_rows = {key: json.dumps(row, sort_keys=True) for key, row in self.rows.items()}
            evicted = list(self._evicted)
            self._evicted.clear()
        async with self._io_lock:
            for old_id in evicted:
                if old_id in snapshot_rows:
                    continue
                (self.path / old_id).unlink(missing_ok=True)
                (self.path / (old_id + '.request.json')).unlink(missing_ok=True)
                self.persisted.pop(old_id, None)
            for key, encoded in snapshot_rows.items():
                if self.persisted.get(key) == encoded:
                    continue
                path = self.path / (key + '.request.json')
                temp = path.with_suffix('.pending')
                with temp.open('w') as stream:
                    stream.write(encoded)
                    stream.flush()
                    os.fsync(stream.fileno())
                temp.replace(path)
                directory = os.open(self.path, os.O_RDONLY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
                self.persisted[key] = encoded

    @staticmethod
    def _require_dict(args, allowed_keys):
        if not isinstance(args, dict) or (set(args) - allowed_keys):
            raise ServiceRejected('invalid_argument')

    @staticmethod
    def _require_id(args, field_name):
        value = args.get(field_name)
        if not isinstance(value, str) or not value or len(value) > 128:
            raise ServiceRejected('invalid_argument')
        return value

    @staticmethod
    def coordinates(args):
        if not isinstance(args, dict) or 'pos' not in args or 'dimension' not in args:
            raise ServiceRejected('invalid_argument')
        p = args['pos']
        d = args['dimension']
        if (type(d) is not int or not -(2**31) <= d < 2**31
                or type(p) is not list or len(p) != 3
                or any(type(v) is not int or not -(2**31) <= v < 2**31 for v in p)
                or not -128 <= p[1] <= 127):
            raise ServiceRejected('invalid_argument')
        return d, p

    @staticmethod
    def _validate_max_age(args, ctx):
        if 'max_age' not in args:
            return ctx.config.cache_seconds
        age = args['max_age']
        if type(age) not in (int, float) or not math.isfinite(age) or not 0 <= age <= ctx.config.cache_seconds:
            raise ServiceRejected('invalid_argument')
        return age

    def _prune_subscribers(self, now=None):
        now = time.time() if now is None else now
        self.subscribers = {k: v for k, v in self.subscribers.items() if v['expires'] > now}

    async def world_epoch(self, ctx):
        try:
            cap = await ctx.peer.call('capabilities.get', {})
        except IPCRejected:
            raise ServiceRejected('snapshot_unavailable') from None
        data = cap.get('data', {}) if isinstance(cap, dict) else {}
        if not isinstance(data, dict) or data.get('world_snapshot') is not True or not data.get('world_epoch'):
            raise ServiceRejected('snapshot_unavailable')
        return data['world_epoch']

    def _admit_locked(self, ctx, args, owner, epoch):
        self.writable()
        d, pos = self.coordinates(args)
        age = self._validate_max_age(args, ctx)
        now = time.time()
        self._prune_subscribers(now)
        key = hashlib.sha256(repr((epoch, d, pos)).encode()).hexdigest()
        row = self.rows.get(key)
        if row and row['state'] in ('queued', 'waiting', 'unknown'):
            row['owners'] = sorted(set(row['owners']) | {owner})
            return self.public(row)
        if row and now - row.get('cached_at', 0) <= age and row['state'] == 'observed':
            row['owners'] = sorted(set(row['owners']) | {owner})
            return self.public(row)

        preserved_owners = set(row['owners']) if row else set()
        preserved_owners.update(
            sub['owner'] for sub in self.subscribers.values() if sub['request_id'] == key
        )
        merged_owners = sorted(preserved_owners | {owner})

        if key not in self.rows and len(self.rows) >= ctx.config.max_requests:
            referenced = {sub['request_id'] for sub in self.subscribers.values()}
            expired = [
                k for k, r in self.rows.items()
                if k not in referenced
                and r['state'] not in ('queued', 'waiting', 'unknown')
                and now - r.get('cached_at', 0) > ctx.config.cache_seconds
            ]
            if not expired:
                raise ServiceRejected('queue_full')
            old = self.rows.pop(expired[0])
            self._evicted.add(old['request_id'])

        row = {
            'request_id': key,
            'dimension': d,
            'pos': list(pos),
            'epoch': epoch,
            'state': 'queued',
            'owners': merged_owners,
            'created_at': now,
            'cached_at': 0,
            'commit_id': uuid.uuid4().hex,
            'operation_ids': [],
            'action_state': 'queued',
            'observation': 'waiting',
        }
        self.rows[key] = row
        return self.public(row)

    async def admit(self, ctx, args, owner):
        self.coordinates(args)
        self._validate_max_age(args, ctx)
        epoch = await self.world_epoch(ctx)
        async with self.lock:
            pub = self._admit_locked(ctx, args, owner, epoch)
        await self.save(ctx)
        return pub

    @staticmethod
    def update_state(row):
        if row.get('state') == 'epoch_expired':
            return
        action = row['action_state']
        if action in ('queued', 'unknown', 'failed', 'cancelled'):
            row['state'] = action
        elif action == 'succeeded' and row['observation'] == 'observed':
            row['state'] = 'observed'
        elif action == 'succeeded' and row['observation'] == 'unobserved':
            row['state'] = 'unobserved'
        else:
            row['state'] = 'waiting'

    async def _fetch_baseline(self, ctx, key):
        # Snapshot metadata has no event timestamp. Capture a pre-action revision
        # fence; only a later revision proves an observation after this instant.
        started = time.time()
        try:
            desc = await snapshot(ctx.peer, key)
        except IPCRejected as exc:
            if exc.code != 'cache_miss':
                raise
            revision = 0
        else:
            # consume releases the temporary Host grant even though only metadata
            # is needed for the fence; dropping a descriptor leaks grant quota.
            await self.client.consume(desc, lambda part: None)
            if not isinstance(desc, dict) or not isinstance(desc.get('metadata'), dict) or desc['metadata'].get('key') != key:
                raise ValueError('snapshot key mismatch')
            revision = desc['metadata']['revision']
            if type(revision) is not int or revision < 0:
                raise ValueError('invalid snapshot revision')
        return started, revision

    @staticmethod
    def public(row):
        return {k: v for k, v in row.items() if k not in ('owners', 'commit_id')}

    def owned(self, args, call):
        rid = self._require_id(args, 'request_id')
        row = self.rows.get(rid)
        if not row or call.installation_id not in row['owners']:
            raise ServiceRejected('request_not_found')
        return row

    @service('request', with_context=True)
    async def request(self, ctx, args, call):
        self._require_dict(args, {'dimension', 'pos', 'max_age'})
        self.coordinates(args)
        self._validate_max_age(args, ctx)
        epoch = await self.world_epoch(ctx)
        async with self.lock:
            pub = self._admit_locked(ctx, args, call.installation_id, epoch)
        await self.save(ctx)
        return pub

    @service('read', with_context=True)
    async def read(self, ctx, args, call):
        self._require_dict(args, {'request_id', 'cell', 'nbt_offset', 'offset', 'length'})
        has_cell = 'cell' in args
        if has_cell:
            if 'offset' in args or 'length' in args:
                raise ServiceRejected('invalid_argument')
            cell = args['cell']
            if type(cell) is not list or len(cell) != 3 or any(type(v) is not int or not 0 <= v <= 15 for v in cell):
                raise ServiceRejected('invalid_argument')
            nbt_offset = args.get('nbt_offset', 0)
            if type(nbt_offset) is not int or nbt_offset < 0:
                raise ServiceRejected('invalid_argument')
        else:
            if 'nbt_offset' in args:
                raise ServiceRejected('invalid_argument')
            offset = args.get('offset', 0)
            length = args.get('length', 16384)
            if type(offset) is not int or offset < 0 or type(length) is not int or not 1 <= length <= 16384:
                raise ServiceRejected('invalid_argument')

        async with self.lock:
            row = self.owned(args, call)
            result = self.public(row)
            row_epoch = row['epoch']
            obs = row.get('observation')
            req_id = row['request_id']

        if row_epoch != await self.world_epoch(ctx):
            result['state'] = 'epoch_expired'
            return result

        if obs == 'observed':
            if self.path is None:
                raise ServiceRejected('artifact_missing')
            try:
                raw = (self.path / req_id).read_bytes()
            except FileNotFoundError:
                raise ServiceRejected('artifact_missing') from None
            except OSError:
                raise ServiceRejected('artifact_missing') from None

            if has_cell:
                try:
                    view = parse_subchunk(raw)
                except (TypeError, ValueError):
                    raise ServiceRejected('artifact_corrupt') from None
                nbt = view.nbt(*cell)
                if (nbt is None and nbt_offset > 0) or (nbt is not None and nbt_offset > len(nbt.data)):
                    raise ServiceRejected('invalid_argument')
                result['cell'] = {
                    'layers': [view.block(*cell, layer=i) for i in range(view.layer_count)],
                    'nbt_complete': view.nbt_complete,
                    'nbt': None if nbt is None else {
                        'encoding': nbt.encoding,
                        'length': len(nbt.data),
                        'offset': nbt_offset,
                        'data_base64': base64.b64encode(nbt.data[nbt_offset:nbt_offset + 16384]).decode(),
                    },
                }
            else:
                if offset > len(raw):
                    raise ServiceRejected('invalid_argument')
                result.update(
                    offset=offset,
                    length=len(raw),
                    data_base64=base64.b64encode(raw[offset:offset + length]).decode(),
                )
        return result

    @service('cancel', with_context=True)
    async def cancel(self, ctx, args, call):
        self._require_dict(args, {'request_id'})
        async with self.lock:
            row = self.owned(args, call)
            row['owners'].remove(call.installation_id)
            if not row['owners'] and row['state'] == 'queued':
                row['action_state'] = 'cancelled'
                self.update_state(row)
            # Shared in-flight reads are never cancelled for another consumer.
            req_id = row['request_id']
        await self.save(ctx)
        return {'request_id': req_id, 'state': 'detached'}

    @service('subscribe', with_context=True)
    async def subscribe(self, ctx, args, call):
        self._require_dict(args, {'dimension', 'pos', 'ttl', 'max_age'})
        self.coordinates(args)
        ttl = args.get('ttl', 60)
        if type(ttl) not in (int, float) or not math.isfinite(ttl) or not 1 <= ttl <= 3600:
            raise ServiceRejected('invalid_argument')
        self._validate_max_age(args, ctx)
        epoch = await self.world_epoch(ctx)
        async with self.lock:
            self.writable()
            now = time.time()
            self._prune_subscribers(now)
            if len(self.subscribers) >= ctx.config.max_subscriptions:
                raise ServiceRejected('subscription_limit')
            row = self._admit_locked(ctx, args, call.installation_id, epoch)
            sid = uuid.uuid4().hex
            self.subscribers[sid] = {
                'owner': call.installation_id,
                'expires': now + ttl,
                'request_id': row['request_id'],
                'args': dict(args),
                'cursor': 0,
                'last': None,
            }
        await self.save(ctx)
        return {'subscription_id': sid, 'request_id': row['request_id'], 'cursor': 0}

    @service('poll', with_context=True)
    async def poll(self, ctx, args, call):
        self._require_dict(args, {'subscription_id', 'cursor'})
        sid = self._require_id(args, 'subscription_id')
        cursor = args.get('cursor', 0)
        if type(cursor) is not int or cursor < 0:
            raise ServiceRejected('invalid_argument')
        async with self.lock:
            self._prune_subscribers()
            sub = self.subscribers.get(sid)
            if not sub or sub['owner'] != call.installation_id:
                raise ServiceRejected('subscription_expired')
            return {
                'cursor': sub['cursor'],
                'gap': cursor < sub['cursor'] - 1,
                'updates': [] if cursor == sub['cursor'] or sub['last'] is None else [sub['last']],
            }

    async def _process_tick_row(self, ctx, snap):
        req_id = snap['request_id']
        key = dict(epoch=snap['epoch'], dimension=snap['dimension'], **dict(zip(('x', 'y', 'z'), snap['pos'])))

        if not snap['has_baseline']:
            try:
                started, revision = await self._fetch_baseline(ctx, key)
            except IPCRejected as exc:
                async with self.lock:
                    row = self.rows.get(req_id)
                    if row is not None:
                        if exc.code == 'world_epoch_expired':
                            row['state'] = 'epoch_expired'
                        else:
                            row['error'] = exc.code
                return
            except (BulkError, TimeoutError, ValueError) as exc:
                async with self.lock:
                    row = self.rows.get(req_id)
                    if row is not None:
                        row['error'] = type(exc).__name__
                return
            async with self.lock:
                row = self.rows.get(req_id)
                if row is None or row['epoch'] != snap['epoch']:
                    return
                row['baseline_revision'] = revision
                row['observation_not_before'] = started
                snap['baseline_revision'] = revision
                snap['observation_not_before'] = started

        if snap['state'] == 'queued':
            async with self.lock:
                if self.sealed is not None or self._sealing is not None:
                    return
                row = self.rows.get(req_id)
                if row is None or row['state'] != 'queued':
                    return
                if any(r['state'] == 'unknown' and not r['operation_ids'] for r in self.rows.values()):
                    return

            try:
                tx = await ctx.storage.transaction(commit_id=snap['commit_id'], keys=[])
                tx.set('chunk_action_' + snap['commit_id'], {'request_id': req_id, 'state': 'admitted'})
                tx.action(ctx.world.subchunk(snap['dimension'], snap['pos'], idempotency_key=snap['commit_id'], deadline=deadline()))
            except IPCRejected as exc:
                async with self.lock:
                    row = self.rows.get(req_id)
                    if row is not None and row['state'] == 'queued':
                        row['action_state'] = 'failed'
                        row['error'] = exc.code
                        self.update_state(row)
                return
            except (IPCError, TimeoutError, ProtocolError, ValueError) as exc:
                async with self.lock:
                    row = self.rows.get(req_id)
                    if row is not None and row['state'] == 'queued':
                        row['error'] = type(exc).__name__
                return

            try:
                receipt = await tx.save()
            except IPCRejected as exc:
                async with self.lock:
                    row = self.rows.get(req_id)
                    if row is not None and row['state'] == 'queued':
                        row['action_state'] = 'failed'
                        row['error'] = exc.code
                        self.update_state(row)
                return
            except (CommitUncertain, IPCError, TimeoutError, ProtocolError) as exc:
                async with self.lock:
                    row = self.rows.get(req_id)
                    if row is not None and row['state'] == 'queued':
                        row['action_state'] = 'unknown'
                        row['error'] = type(exc).__name__
                        row['submitted_at'] = time.time()
                        self.update_state(row)
                return

            async with self.lock:
                row = self.rows.get(req_id)
                if row is None:
                    return
                row['operation_ids'] = receipt['operation_ids']
                row['action_state'] = 'pending'
                row['submitted_at'] = time.time()
                row.pop('error', None)
                self.update_state(row)
                snap['operation_ids'] = list(receipt['operation_ids'])
                snap['submitted_at'] = row['submitted_at']
                snap['state'] = row['state']

        if not snap['operation_ids']:
            try:
                receipt = await ctx.storage.receipt(snap['commit_id'])
            except (IPCRejected, IPCError, TimeoutError, ProtocolError) as exc:
                async with self.lock:
                    row = self.rows.get(req_id)
                    if row is not None:
                        row['error'] = exc.code if isinstance(exc, IPCRejected) else type(exc).__name__
                return
            if receipt is None:
                return
            async with self.lock:
                row = self.rows.get(req_id)
                if row is None:
                    return
                row['operation_ids'] = receipt['operation_ids']
                row['action_state'] = 'pending'
                row.setdefault('submitted_at', time.time())
                self.update_state(row)
                snap['operation_ids'] = list(receipt['operation_ids'])
                snap['submitted_at'] = row['submitted_at']
                snap['state'] = row['state']

        op_receipts = []
        try:
            for opid in snap['operation_ids']:
                op_receipts.append(await ctx.operations.get(opid))
        except (IPCRejected, IPCError, TimeoutError, ProtocolError) as exc:
            async with self.lock:
                row = self.rows.get(req_id)
                if row is not None:
                    row['error'] = exc.code if isinstance(exc, IPCRejected) else type(exc).__name__
            return

        states = [r['state'] for r in op_receipts]
        new_action_state = next(
            (s for s in ('unknown', 'accepted', 'dispatching', 'running', 'pending', 'failed', 'cancelled') if s in states),
            'succeeded',
        )
        async with self.lock:
            row = self.rows.get(req_id)
            if row is None:
                return
            if op_receipts:
                row['receipt'] = op_receipts[-1]
            row['action_state'] = new_action_state
            self.update_state(row)
            if row['state'] not in ('waiting', 'unknown'):
                return
            baseline_revision = row['baseline_revision']
            observe_since = max(row.get('submitted_at') or 0, row.get('observation_not_before') or 0) or row['created_at']
            obs_not_before = row['observation_not_before']

        try:
            desc = await snapshot(ctx.peer, key)
            data = bytearray()

            def collect(part):
                if len(data) + len(part) > 1 << 20:
                    raise ValueError('snapshot exceeds 1 MiB')
                data.extend(part)

            ref = await self.client.consume(desc, collect)
            if len(data) > 1 << 20:
                raise ValueError('snapshot exceeds 1 MiB')
            metadata = desc['metadata']
            if metadata['key'] != key:
                raise ValueError('snapshot key mismatch')
            if metadata['revision'] <= baseline_revision:
                async with self.lock:
                    row = self.rows.get(req_id)
                    if row is not None and row['state'] in ('waiting', 'unknown'):
                        row['observation'] = 'unobserved' if time.time() - observe_since >= ctx.config.observe_timeout else 'waiting'
                        self.update_state(row)
                return
            parse_subchunk(data)
        except IPCRejected as exc:
            async with self.lock:
                row = self.rows.get(req_id)
                if row is not None and row['state'] in ('waiting', 'unknown'):
                    row['observation_error'] = exc.code
                    if exc.code == 'world_epoch_expired':
                        row['state'] = 'epoch_expired'
                    else:
                        row['observation'] = 'unobserved' if time.time() - observe_since >= ctx.config.observe_timeout else 'waiting'
                        self.update_state(row)
            return
        except (BulkError, TimeoutError, ValueError, KeyError, TypeError) as exc:
            async with self.lock:
                row = self.rows.get(req_id)
                if row is not None and row['state'] in ('waiting', 'unknown'):
                    row['observation_error'] = type(exc).__name__
                    row['observation'] = 'unobserved' if time.time() - observe_since >= ctx.config.observe_timeout else 'waiting'
                    self.update_state(row)
            return

        target = self.path / req_id
        temp = target.with_suffix('.part')
        async with self._io_lock:
            temp.write_bytes(data)
            temp.replace(target)
        async with self.lock:
            row = self.rows.get(req_id)
            if row is not None and row['state'] in ('waiting', 'unknown'):
                row.update(
                    observation='observed',
                    cached_at=obs_not_before,
                    digest=ref['digest'],
                    metadata=metadata,
                )
                row.pop('observation_error', None)
                self.update_state(row)

    async def tick(self, ctx):
        async with self.lock:
            if self.sealed is not None or self._sealing is not None:
                return
            self._prune_subscribers()

        try:
            epoch = await self.world_epoch(ctx)
        except (ServiceRejected, IPCRejected, TimeoutError):
            epoch = None

        selected_snapshot = None
        async with self.lock:
            if self.sealed is not None or self._sealing is not None:
                return
            now = time.time()
            self._prune_subscribers(now)
            if epoch is not None:
                for row in self.rows.values():
                    if row['state'] in ('queued', 'waiting', 'unknown') and row['epoch'] != epoch:
                        row['state'] = 'epoch_expired'
                all_rows = list(self.rows.values())
                if all_rows:
                    n = len(all_rows)
                    start = self.tick_cursor % n
                    has_unresolved_unknown = any(
                        r['state'] == 'unknown' and not r['operation_ids']
                        for r in all_rows
                    )
                    advanced = False
                    for offset in range(n):
                        idx = (start + offset) % n
                        candidate = all_rows[idx]
                        if candidate['state'] not in ('queued', 'waiting', 'unknown'):
                            continue
                        if candidate['state'] == 'queued' and has_unresolved_unknown:
                            continue
                        selected_snapshot = {
                            'request_id': candidate['request_id'],
                            'epoch': candidate['epoch'],
                            'dimension': candidate['dimension'],
                            'pos': list(candidate['pos']),
                            'state': candidate['state'],
                            'commit_id': candidate['commit_id'],
                            'operation_ids': list(candidate['operation_ids']),
                            'has_baseline': 'baseline_revision' in candidate,
                            'baseline_revision': candidate.get('baseline_revision'),
                            'observation_not_before': candidate.get('observation_not_before'),
                            'submitted_at': candidate.get('submitted_at'),
                            'created_at': candidate['created_at'],
                        }
                        self.tick_cursor = (idx + 1) % n
                        advanced = True
                        break
                    if not advanced:
                        self.tick_cursor = (start + 1) % n

        if selected_snapshot is not None:
            await self._process_tick_row(ctx, selected_snapshot)

        refresh_jobs = []
        async with self.lock:
            now = time.time()
            self._prune_subscribers(now)
            for sid, sub in list(self.subscribers.items()):
                row = self.rows.get(sub['request_id'])
                if row:
                    update = self.public(row)
                    if sub['last'] != update:
                        sub['last'] = update
                        sub['cursor'] += 1
                    ref_ts = row.get('cached_at') or row.get('submitted_at') or row['created_at']
                    if (self.sealed is None and self._sealing is None
                            and row['state'] not in ('queued', 'waiting', 'unknown')
                            and (row['state'] == 'epoch_expired' or now - ref_ts > ctx.config.cache_seconds)):
                        refresh_jobs.append((sid, dict(sub['args']), sub['owner']))

        if refresh_jobs and epoch is not None:
            async with self.lock:
                if self.sealed is None and self._sealing is None:
                    for sid, sub_args, sub_owner in refresh_jobs:
                        if sid not in self.subscribers:
                            continue
                        try:
                            fresh = self._admit_locked(ctx, sub_args, sub_owner, epoch)
                            self.subscribers[sid]['request_id'] = fresh['request_id']
                            if self.subscribers[sid]['last'] != fresh:
                                self.subscribers[sid]['last'] = fresh
                                self.subscribers[sid]['cursor'] += 1
                        except ServiceRejected:
                            pass

        await self.save(ctx)


if __name__ == '__main__':
    Chunks().run()
