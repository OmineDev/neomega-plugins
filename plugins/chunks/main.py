"""Bounded shared subchunk requests, verified cache and pull subscriptions."""
import asyncio
import base64
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import time
import uuid
from pathlib import Path

from neomega_runtime import Plugin
from neomega_runtime.services import service, ServiceRejected
from neomega_runtime.bulk import BulkClient, snapshot
from neomega_runtime.subchunk import parse_subchunk
from neomega_runtime.managed import IPCRejected


def deadline():
    return (datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat()


@dataclass(frozen=True)
class Settings:
    max_requests: int = 128
    max_subscriptions: int = 128
    cache_seconds: float = 30.0
    observe_timeout: float = 15.0

    def __post_init__(self):
        if not 1 <= self.max_requests <= 1024 or not 1 <= self.max_subscriptions <= 1024:
            raise ValueError('queue limits must be 1..1024')
        if not 1 <= self.cache_seconds <= 3600 or not 1 <= self.observe_timeout <= 300:
            raise ValueError('invalid cache or observation timeout')


class Chunks(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = None
        self.tick_cursor = 0
        self.client = None
        self.epoch = None
        self.rows = {}
        self.subscribers = {}

    async def on_maintenance(self, ctx, request):
        async with self.lock:
            token = request['token']
            if self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            if request['operation'] == 'seal':
                for row in self.rows.values():
                    if row['state'] in ('waiting','unknown') or row.get('receipt',{}).get('state') in ('pending','running','unknown'):
                        if not row['operation_ids']:
                            found = await ctx.storage.receipt(row['commit_id'])
                            if found is None:
                                return {'status':'busy','token':token}
                            row['operation_ids'] = found['operation_ids']
                        receipts = [await ctx.operations.get(i) for i in row['operation_ids']]
                        if any(r['state'] not in ('succeeded','failed','cancelled') for r in receipts):
                            return {'status':'busy','token':token}
                await self.save(ctx)
                self.sealed = token
                return {'status': 'sealed', 'token': token}
            self.sealed = None
            return {'status': 'released', 'token': token}

    def writable(self):
        if self.sealed is not None:
            raise ServiceRejected('maintenance_sealed')

    async def on_start(self, ctx):
        self.client = BulkClient(max_transfers=1, max_waiters=0)
        self.path = ctx.data_dir / 'verified_chunks'
        self.path.mkdir(parents=True, exist_ok=True)
        self.rows = {p.name.split('.')[0]:json.loads(p.read_text()) for p in self.path.glob('*.request.json')}
        self.persisted = {k:json.dumps(v,sort_keys=True) for k,v in self.rows.items()}
        # A new Worker must revalidate observation freshness and world epoch.
        for row in self.rows.values():
            row['cached_at'] = 0
            row['observation'] = 'waiting'
            row.setdefault('action_state', row.get('receipt', {}).get('state',
                           row['state'] if row['state'] in ('queued', 'failed', 'cancelled') else 'unknown'))
            self.update_state(row)
        ctx.every(1.0, lambda: self.tick(ctx), name='chunks-pump')

    async def on_stop(self, ctx):
        if self.client:
            await self.client.aclose()

    async def save(self, ctx):
        for key,row in self.rows.items():
            encoded = json.dumps(row,sort_keys=True)
            if self.persisted.get(key) == encoded:
                continue
            path = self.path / (key+'.request.json')
            temp = path.with_suffix('.pending')
            with temp.open('w') as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            temp.replace(path)
            directory = os.open(self.path,os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            self.persisted[key] = encoded

    @staticmethod
    def coordinates(args):
        p = args['pos']
        d = args['dimension']
        if type(d) is not int or not -(2**31) <= d < 2**31 or not isinstance(p, list) or len(p) != 3 or any(type(v) is not int or not -(2**31) <= v < 2**31 for v in p) or not -128 <= p[1] <= 127:
            raise ServiceRejected('invalid_coordinates')
        return d, p

    async def world_epoch(self, ctx):
        cap = await ctx.peer.call('capabilities.get', {})
        data = cap.get('data', {})
        if data.get('world_snapshot') is not True or not data.get('world_epoch'):
            raise ServiceRejected('snapshot_unavailable')
        return data['world_epoch']

    async def admit(self, ctx, args, owner):
        self.writable()
        d, pos = self.coordinates(args)
        epoch = await self.world_epoch(ctx)
        key = hashlib.sha256(repr((epoch, d, pos)).encode()).hexdigest()
        row = self.rows.get(key)
        age = args.get('max_age', ctx.config.cache_seconds)
        if type(age) not in (int, float) or not 0 <= age <= ctx.config.cache_seconds:
            raise ServiceRejected('invalid_cache_age')
        if row and row['state'] in ('queued', 'waiting', 'unknown'):
            row['owners'] = sorted(set(row['owners'] + [owner]))
            await self.save(ctx)
            return self.public(row)
        if row and time.time() - row.get('cached_at', 0) <= age and row['state'] == 'observed':
            row['owners'] = sorted(set(row['owners'] + [owner]))
            await self.save(ctx)
            return self.public(row)
        if len(self.rows) >= ctx.config.max_requests:
            expired = [k for k, r in self.rows.items() if r['state'] not in ('queued', 'waiting', 'unknown') and time.time() - r.get('cached_at', 0) > ctx.config.cache_seconds]
            if not expired:
                raise ServiceRejected('queue_full')
            old = self.rows.pop(expired[0])
            cached = self.path / old['request_id']
            cached.unlink(missing_ok=True)
            (self.path / (old['request_id']+'.request.json')).unlink(missing_ok=True)
            self.persisted.pop(old['request_id'],None)
        row = {'request_id': key, 'dimension': d, 'pos': pos, 'epoch': epoch,
               'state': 'queued', 'owners': [owner], 'created_at': time.time(), 'cached_at': 0,
               'commit_id': uuid.uuid4().hex, 'operation_ids': [],
               'action_state': 'queued', 'observation': 'waiting'}
        self.rows[key] = row
        await self.save(ctx)
        return self.public(row)

    @staticmethod
    def update_state(row):
        action = row['action_state']
        if action in ('queued', 'unknown', 'failed', 'cancelled'):
            row['state'] = action
        elif action == 'succeeded' and row['observation'] == 'observed':
            row['state'] = 'observed'
        elif action == 'succeeded' and row['observation'] == 'unobserved':
            row['state'] = 'unobserved'
        else:
            row['state'] = 'waiting'

    async def baseline(self, ctx, row, key):
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
            if desc['metadata']['key'] != key:
                raise ValueError('snapshot key mismatch')
            revision = desc['metadata']['revision']
        row['baseline_revision'] = revision
        row['observation_not_before'] = started
        await self.save(ctx)

    @staticmethod
    def public(row):
        return {k: v for k, v in row.items() if k not in ('owners', 'commit_id')}

    def owned(self, args, call):
        row = self.rows.get(args['request_id'])
        if not row or call.installation_id not in row['owners']:
            raise ServiceRejected('request_not_found')
        return row

    @service('request', with_context=True)
    async def request(self, ctx, args, call):
        async with self.lock:
            return await self.admit(ctx, args, call.installation_id)

    @service('read', with_context=True)
    async def read(self, ctx, args, call):
        async with self.lock:
            row = self.owned(args, call)
            result = self.public(row)
            if row['epoch'] != await self.world_epoch(ctx):
                result['state'] = 'epoch_expired'
                return result
            if row.get('observation') == 'observed':
                raw = (self.path / row['request_id']).read_bytes()
                if 'cell' in args:
                    cell = args['cell']
                    offset = args.get('nbt_offset', 0)
                    if type(offset) is not int or offset < 0:
                        raise ServiceRejected('invalid_slice')
                    view = parse_subchunk(raw)
                    nbt = view.nbt(*cell)
                    result['cell'] = {'layers': [view.block(*cell, layer=i) for i in range(view.layer_count)],
                                      'nbt_complete': view.nbt_complete,
                                      'nbt': None if nbt is None else {'encoding': nbt.encoding, 'length': len(nbt.data), 'offset': args.get('nbt_offset', 0),
                                               'data_base64': base64.b64encode(nbt.data[args.get('nbt_offset', 0):args.get('nbt_offset', 0)+16384]).decode()}}
                else:
                    offset, length = args.get('offset', 0), args.get('length', 16384)
                    if type(offset) is not int or not 0 <= offset <= len(raw) or type(length) is not int or not 1 <= length <= 16384:
                        raise ServiceRejected('invalid_slice')
                    result.update(offset=offset, length=len(raw), data_base64=base64.b64encode(raw[offset:offset+length]).decode())
            return result

    @service('cancel', with_context=True)
    async def cancel(self, ctx, args, call):
        async with self.lock:
            row = self.owned(args, call)
            row['owners'].remove(call.installation_id)
            if not row['owners'] and row['state'] == 'queued':
                row['action_state'] = 'cancelled'
                self.update_state(row)
            # Shared in-flight reads are never cancelled for another consumer.
            await self.save(ctx)
            return {'request_id': row['request_id'], 'state': 'detached'}

    @service('subscribe', with_context=True)
    async def subscribe(self, ctx, args, call):
        async with self.lock:
            self.coordinates(args)
            ttl = args.get('ttl', 60)
            if type(ttl) not in (int, float) or not 1 <= ttl <= 3600:
                raise ServiceRejected('invalid_ttl')
            if len(self.subscribers) >= ctx.config.max_subscriptions:
                raise ServiceRejected('subscription_limit')
            row = await self.admit(ctx, args, call.installation_id)
            sid = uuid.uuid4().hex
            self.subscribers[sid] = {'owner': call.installation_id, 'expires': time.time()+ttl,
                                     'request_id': row['request_id'], 'args': args, 'cursor': 0, 'last': None}
            return {'subscription_id': sid, 'request_id': row['request_id'], 'cursor': 0}

    @service('poll', with_context=True)
    async def poll(self, ctx, args, call):
        async with self.lock:
            sub = self.subscribers.get(args['subscription_id'])
            if not sub or sub['owner'] != call.installation_id or sub['expires'] <= time.time():
                raise ServiceRejected('subscription_expired')
            cursor = args.get('cursor', 0)
            return {'cursor': sub['cursor'], 'gap': cursor < sub['cursor']-1,
                    'updates': [] if cursor == sub['cursor'] or sub['last'] is None else [sub['last']]}

    async def tick(self, ctx):
        async with self.lock:
            if self.sealed is not None:
                return
            now = time.time()
            self.subscribers = {k: v for k, v in self.subscribers.items() if v['expires'] > now}
            rows = list(self.rows.values())
            if rows:
                pivot = self.tick_cursor % len(rows)
                rows = rows[pivot:] + rows[:pivot]
                self.tick_cursor = (pivot + 1) % len(rows)
            for row in rows:
                if row['state'] not in ('queued', 'waiting', 'unknown'):
                    continue
                if row['epoch'] != await self.world_epoch(ctx):
                    row['state'] = 'epoch_expired'
                    continue
                key = dict(epoch=row['epoch'], dimension=row['dimension'], **dict(zip(('x','y','z'), row['pos'])))
                if row['state'] != 'queued' and 'baseline_revision' not in row:
                    await self.baseline(ctx, row, key)
                if row['state'] == 'queued':
                    if any(r['state']=='unknown' and not r['operation_ids'] for r in self.rows.values()):
                        continue
                    await self.baseline(ctx, row, key)
                    # Commit admission record and action atomically. On lost reply query commit ID.
                    row['action_state'] = 'unknown'
                    self.update_state(row)
                    await self.save(ctx)
                    tx = await ctx.storage.transaction(commit_id=row['commit_id'],keys=[])
                    tx.set('chunk_action_'+row['commit_id'],{'request_id':row['request_id'],'state':'admitted'})
                    tx.action(ctx.world.subchunk(row['dimension'], row['pos'], idempotency_key=row['commit_id'], deadline=deadline()))
                    try:
                        receipt = await tx.save()
                    except Exception as exc:
                        row['error'] = type(exc).__name__
                        await self.save(ctx)
                        continue
                    row['operation_ids'] = receipt['operation_ids']
                    row['action_state'] = 'pending'
                    self.update_state(row)
                if not row['operation_ids']:
                    receipt = await ctx.storage.receipt(row['commit_id'])
                    if receipt is None:
                        continue
                    row['operation_ids'] = receipt['operation_ids']
                    row['action_state'] = 'pending'
                    self.update_state(row)
                receipts = []
                for opid in row['operation_ids']:
                    receipt = await ctx.operations.get(opid)
                    row['receipt'] = receipt
                    receipts.append(receipt['state'])
                row['action_state'] = next((state for state in ('unknown', 'running', 'pending', 'failed', 'cancelled') if state in receipts), 'succeeded')
                self.update_state(row)
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
                    if metadata['revision'] <= row['baseline_revision']:
                        row['observation'] = 'unobserved' if now-row['created_at'] >= ctx.config.observe_timeout else 'waiting'
                        self.update_state(row)
                        break
                    parse_subchunk(data)
                    if desc['metadata']['key'] != key:
                        raise ValueError('snapshot key mismatch')
                except IPCRejected as exc:
                    if exc.code not in ('cache_miss', 'world_epoch_expired', 'unsupported'):
                        raise
                    row['observation_error'] = exc.code
                    row['observation'] = 'unobserved' if now-row['created_at'] >= ctx.config.observe_timeout else 'waiting'
                    self.update_state(row)
                else:
                    target = self.path / row['request_id']
                    temp = target.with_suffix('.part')
                    temp.write_bytes(data)
                    temp.replace(target)
                    row.update(observation='observed', cached_at=row['observation_not_before'],
                               digest=ref['digest'], metadata=metadata)
                    self.update_state(row)
                # One active network request per tick bounds work and leaves time for consumers.
                break
            for sub in self.subscribers.values():
                row = self.rows.get(sub['request_id'])
                if row:
                    update = self.public(row)
                    if sub['last'] != update:
                        sub['last'] = update
                        sub['cursor'] += 1
                    if row['state'] not in ('queued', 'waiting', 'unknown') and now-row.get('cached_at', row['created_at']) > ctx.config.cache_seconds:
                        fresh = await self.admit(ctx, sub['args'], sub['owner'])
                        sub['request_id'] = fresh['request_id']
            await self.save(ctx)


if __name__ == '__main__':
    Chunks().run()
