"""Shared bounded evidence cache. No inferred inventory or replayed writes."""
import asyncio
import copy
import json
import hashlib
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import time
from uuid import uuid4
from neomega_runtime import Plugin, service, ServiceRejected, PacketSubscriptionError
from neomega_runtime.protocol import packets
from neomega_runtime.http import ProtocolError
from neomega_runtime.managed import IPCRejected

@dataclass(frozen=True)
class Config:
    interval: float = field(default=2.0, metadata={'title': '采样间隔', 'description': '共享采样的间隔，单位秒。', 'minimum': .5, 'maximum': 60})
    stale_after: float = field(default=10.0, metadata={'title': '过期时间', 'description': '距最后一次成功采样超过此秒数时标记过期。', 'minimum': 1, 'maximum': 300})
    max_subscriptions: int = field(default=128, metadata={'title': '订阅上限', 'description': '允许同时存在的订阅数量。', 'minimum': 1, 'maximum': 1024})
    event_capacity: int = field(default=256, metadata={'title': '事件缓存', 'description': '最多保留的近期事件数量。', 'minimum': 16, 'maximum': 1024})
    subscription_ttl: int = field(default=60, metadata={'title': '订阅有效期', 'description': '订阅未续期时自动过期，单位秒。', 'minimum': 5, 'maximum': 300})

class Observations(Plugin):
    config_type = Config

    async def on_start(self, ctx):
        self.cache, self.subscriptions, self.payloads = {}, {}, {}
        self.events = deque(maxlen=ctx.config.event_capacity)
        self.sequence = 0
        self.source_seen = deque(maxlen=256)
        self.epoch = uuid4().hex
        self.tick_samples = deque(maxlen=8)
        self.lock = asyncio.Lock()
        self.sealed = False
        self.maintenance_token = None
        self.stream_status = 'starting'
        self.tasks = [ctx.every(ctx.config.interval, lambda: self.sample(ctx), immediate=True),
                      ctx.spawn(self.observe(ctx), name='shared-protocol-observations')]

    async def on_stop(self, ctx):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.cache.clear()
        self.payloads.clear()
        self.subscriptions.clear()

    async def on_maintenance(self, ctx, request):
        token, operation = request['token'], request['operation']
        if operation == 'seal':
            if self.maintenance_token not in (None, token):
                return {'status': 'busy', 'token': token}
            self.maintenance_token = token
            self.sealed = True
        elif operation == 'release':
            if self.maintenance_token not in (None, token):
                return {'status': 'busy', 'token': token}
            self.sealed = False
            self.maintenance_token = None
        else:
            return {'status': 'unsupported', 'token': token}
        if self.sealed:
            async with self.lock:
                pass
        return {'status': 'sealed' if self.sealed else 'released', 'token': token}

    def publish(self, kind, data, source, *, complete=False, observed_at=None, successful=True):
        now = time.time()
        timestamp = datetime.now(timezone.utc).isoformat()
        previous = self.cache.get(kind)
        if not successful:
            self.sequence += 1
            if previous is not None:
                row = copy.deepcopy(previous)
            else:
                row = dict(kind=kind, data=None, source=source, complete=False,
                           observed_at=None, sampled_at=None, sequence=None, epoch=self.epoch,
                           last_success_at=None, last_success_value=None)
            row.update(last_attempt_at=timestamp, last_error=copy.deepcopy(data),
                       attempt_sequence=self.sequence)
            self.cache[kind] = row
            event = copy.deepcopy(row)
            event.update(sequence=self.sequence, payload_sequence=row['sequence'])
            self.events.append(event)
            return
        encoded = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
        self.payloads[kind] = (self.sequence + 1, encoded)
        # Both data and last_success_value travel in the service envelope.
        if len(json.dumps(data, ensure_ascii=False).encode()) > 16000:
            data = {'state': 'partial', 'reason': 'read_payload_pages',
                    'operation_id': data.get('operation_id'), 'payload_available': True,
                    'length': len(encoded), 'sha256': hashlib.sha256(encoded.encode()).hexdigest()}
            complete = False
        self.sequence += 1
        row = dict(kind=kind, data=data, source=source, complete=complete,
                   observed_at=observed_at or datetime.now(timezone.utc).isoformat(),
                   sampled_at=now, sequence=self.sequence, epoch=self.epoch,
                   last_success_at=observed_at or timestamp, last_success_value=copy.deepcopy(data),
                   last_attempt_at=timestamp, last_error=None)
        self.cache[kind] = row
        self.events.append(row)

    def active(self):
        now = time.time()
        self.subscriptions = {k: v for k, v in self.subscriptions.items() if v['expires_at'] > now}
        return set(k for v in self.subscriptions.values() for k in v['kinds'])

    async def sample(self, ctx, kinds=None):
        if self.sealed:
            return
        async with self.lock:
            if self.sealed:
                return
            requested = self.active() if kinds is None else kinds
            if 'position' in requested:
                try:
                    data = await ctx.positions.sample(timeout=5, target='players', limit=64)
                    self.publish('position', data, 'framework.entities.query', complete=data.get('complete', False))
                except (ProtocolError, IPCRejected, TimeoutError) as error:
                    self.publish('position', {'state': 'unavailable', 'error': type(error).__name__}, 'framework.entities.query', successful=False)
            if 'inventory' in requested:
                op = None
                try:
                    deadline = (datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat()
                    intent = ctx.inventory.observe(0, idempotency_key=uuid4().hex, deadline=deadline)
                    op = await ctx.operations.submit(intent)
                    receipt = await ctx.operations.wait(op, timeout=5)
                    if receipt['state'] == 'succeeded':
                        self.publish('inventory', {'subject': 'self', 'operation_id': op, 'receipt': receipt},
                                     'framework.self.inventory.observe', complete=False)
                    else:
                        self.publish('inventory', {'state': receipt['state'], 'operation_id': op},
                                     'framework.self.inventory.observe', successful=False)
                except (ProtocolError, IPCRejected, TimeoutError) as error:
                    self.publish('inventory', {'state': 'unavailable', 'operation_id': op,
                        'error': type(error).__name__}, 'framework.self.inventory.observe', successful=False)

    async def observe(self, ctx):
        try:
            async with ctx.packets.typed.subscribe([packets.ActorEvent, packets.Respawn, packets.SetTime]) as stream:
                self.stream_status = 'active'
                async for event in stream:
                    source_key = (event.connection_id, event.sequence)
                    if source_key in self.source_seen:
                        continue
                    self.source_seen.append(source_key)
                    if self.epoch != event.connection_id:
                        self.epoch = event.connection_id
                        self.cache.clear()
                        self.payloads.clear()
                        self.events.clear()
                        self.tick_samples.clear()
                        self.publish('gap', {'reason': 'connection_changed'}, 'host')
                    if self.sealed:
                        continue
                    model = event.model
                    if isinstance(model, packets.ActorEvent) and model.EventType == 3:
                        self.publish('death', {'entity_runtime_id': str(model.EntityRuntimeID), 'identity': None},
                                     'ActorEventDeath', observed_at=event.observed_at)
                    elif isinstance(model, packets.Respawn):
                        self.publish('respawn', {'entity_runtime_id': str(model.EntityRuntimeID),
                            'state': model.State, 'identity': None}, 'Respawn', observed_at=event.observed_at)
                    elif isinstance(model, packets.SetTime):
                        now = time.monotonic()
                        if self.tick_samples and (model.Time <= self.tick_samples[-1][1] or now-self.tick_samples[-1][0] > 120):
                            self.tick_samples.clear()
                        self.tick_samples.append((now, model.Time))
                        data = {'value': None, 'quality': 'unavailable', 'window_seconds': 0}
                        if len(self.tick_samples) >= 2:
                            a, b = self.tick_samples[0], self.tick_samples[-1]
                            elapsed = b[0]-a[0]
                            value = (b[1]-a[1])/elapsed if elapsed > 0 else 0
                            if elapsed >= 1 and 0 < value <= 25:
                                data = {'value': value, 'quality': 'daytime_estimate', 'window_seconds': elapsed}
                        self.publish('tps', data, 'SetTime', observed_at=event.observed_at)
        except PacketSubscriptionError as error:
            self.stream_status = error.reason
            for kind in ('death', 'respawn', 'tps'):
                self.cache.pop(kind, None)
                self.payloads.pop(kind, None)
            self.publish('gap', {'reason': error.reason}, 'host')
        finally:
            if self.stream_status == 'active':
                self.stream_status = 'closed'
                for kind in ('death', 'respawn', 'tps'):
                    self.cache.pop(kind, None)
                    self.payloads.pop(kind, None)
                self.publish('gap', {'reason': 'stream_closed'}, 'host')

    async def dispatch(self, ctx, args, call, action):
        self.active()
        if action == 'read':
            payload = self.payloads.get(args['kind'])
            if not payload:
                raise ServiceRejected('not_found')
            sequence, encoded = payload
            if sequence != args['sequence']:
                raise ServiceRejected('conflict')
            offset, limit = args.get('offset', 0), args.get('limit', 8192)
            if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 8192:
                raise ServiceRejected('invalid_argument')
            fragment = encoded[offset:offset+limit]
            return {'fragment': fragment, 'offset': offset, 'next_offset': offset+len(fragment),
                    'length': len(encoded), 'sequence': sequence, 'done': offset+len(fragment) >= len(encoded),
                    'sha256': hashlib.sha256(encoded.encode()).hexdigest()}
        if action == 'status':
            return {'epoch': self.epoch, 'sequence': self.sequence, 'stream': self.stream_status,
                    'subscriptions': len(self.subscriptions), 'sealed': self.sealed,
                    'inventory_coverage': 'self_only', 'death_coverage': 'protocol_visible_entities'}
        if action == 'unsubscribe':
            sub = self.subscriptions.get(args['subscription_id'])
            if sub and sub['owner'] != call.installation_id:
                raise ServiceRejected('permission_denied')
            self.subscriptions.pop(args['subscription_id'], None)
            return {'removed': bool(sub)}
        if action == 'subscribe':
            if self.sealed:
                raise ServiceRejected('busy')
            if len(self.subscriptions) >= ctx.config.max_subscriptions:
                raise ServiceRejected('busy')
            kinds = args.get('kinds', ['position'])
            if not kinds or len(kinds) > 6 or set(kinds)-{'position','inventory','death','respawn','tps','gap'}:
                raise ServiceRejected('invalid_argument')
            sid = uuid4().hex
            self.subscriptions[sid] = {'owner': call.installation_id, 'kinds': kinds,
                'expires_at': time.time()+ctx.config.subscription_ttl}
            return {'subscription_id': sid, 'cursor': self.sequence, 'epoch': self.epoch,
                    'expires_at': self.subscriptions[sid]['expires_at']}
        if action == 'poll':
            sub = self.subscriptions.get(args['subscription_id'])
            if not sub or sub['owner'] != call.installation_id:
                raise ServiceRejected('not_found')
            sub['expires_at'] = time.time()+ctx.config.subscription_ttl
            cursor = args.get('cursor', 0)
            rows = [copy.deepcopy(e) for e in self.events if e['sequence'] > cursor and e['kind'] in sub['kinds']]
            # A bounded page fits the 64 KiB service envelope; preserve cursor when paging.
            page, size = [], 0
            for row in rows[:16]:
                size += len(json.dumps(row, ensure_ascii=False).encode())
                if size > 48000:
                    break
                page.append(row)
            rows = page
            return {'events': rows, 'cursor': rows[-1]['sequence'] if rows else self.sequence,
                    'epoch': self.epoch, 'gap': bool(self.events and cursor < self.events[0]['sequence']-1)}
        kinds = args.get('kinds', ['position','inventory','tps'])
        if len(kinds) > 6 or set(kinds)-{'position','inventory','death','respawn','tps','gap'}:
            raise ServiceRejected('invalid_argument')
        if action == 'refresh':
            if self.sealed:
                raise ServiceRejected('busy')
            await self.sample(ctx, kinds)
        rows = {}
        budget = 48000
        for kind in kinds:
            row = copy.deepcopy(self.cache.get(kind))
            if row:
                row['stale'] = row['sampled_at'] is None or time.time()-row['sampled_at'] > ctx.config.stale_after
            if row:
                size = len(json.dumps(row, ensure_ascii=False).encode())
                if size > budget:
                    row = {'state': 'partial', 'reason': 'request_kind_separately', 'complete': False}
                budget -= len(json.dumps(row, ensure_ascii=False).encode())
            rows[kind] = row or {'state': 'unavailable', 'source': None, 'complete': False}
        if args.get('player') and 'inventory' in kinds:
            rows['inventory'] = {'state': 'unavailable', 'reason': 'other_player_inventory_not_visible', 'complete': False}
        return {'epoch': self.epoch, 'observations': rows}

    @service('read', major=1, with_context=True)
    async def read(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'read')

    @service('subscribe', major=1, with_context=True)
    async def subscribe(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'subscribe')

    @service('snapshot', major=1, with_context=True)
    async def snapshot(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'snapshot')

    @service('refresh', major=1, with_context=True)
    async def refresh(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'refresh')

    @service('status', major=1, with_context=True)
    async def status(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'status')

    @service('poll', major=1, with_context=True)
    async def poll(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'poll')

    @service('unsubscribe', major=1, with_context=True)
    async def unsubscribe(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'unsubscribe')

if __name__ == '__main__':
    Observations().run()
