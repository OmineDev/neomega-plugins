"""Leased command-block text routes; text never conveys player authority."""
import asyncio
import json
from collections import deque
from dataclasses import dataclass, field
import time
from uuid import uuid4
from neomega_runtime import Plugin, service, ServiceRejected, PacketSubscriptionError
from neomega_runtime.protocol import packets

@dataclass(frozen=True)
class Config:
    max_routes: int = field(default=128, metadata={'minimum': 1, 'maximum': 1024})
    lease_seconds: int = field(default=60, metadata={'minimum': 5, 'maximum': 300})
    queue_capacity: int = field(default=128, metadata={'minimum': 8, 'maximum': 256})
    messages_per_second: int = field(default=20, metadata={'minimum': 1, 'maximum': 100})
    max_message_bytes: int = field(default=2048, metadata={'minimum': 32, 'maximum': 4096})

class CBBridge(Plugin):
    config_type = Config

    async def on_start(self, ctx):
        self.routes, self.seen = {}, deque(maxlen=256)
        self.sequence = 0
        self.sealed = False
        self.maintenance_token = None
        self.stream_status = 'starting'
        self.task = ctx.spawn(self.observe(ctx), name='command-block-text')

    async def on_stop(self, ctx):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        self.routes.clear()

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
        return {'status': 'sealed' if self.sealed else 'released', 'token': token}

    def prune(self):
        now = time.time()
        self.routes = {k: r for k, r in self.routes.items() if r['expires_at'] > now}

    async def observe(self, ctx):
        try:
            async with ctx.packets.typed.subscribe([packets.Text]) as stream:
                self.stream_status = 'active'
                async for event in stream:
                    self.prune()
                    if self.sealed:
                        continue
                    model = event.model
                    text = self.message_text(model, ctx.config.max_message_bytes)
                    if text is None:
                        continue
                    key = (event.connection_id, event.sequence)
                    if key in self.seen:
                        continue
                    self.seen.append(key)
                    self.sequence += 1
                    now = time.monotonic()
                    for route in sorted(self.routes.values(), key=lambda r: (-r['priority'], r['id'])):
                        if not text.startswith(route['prefix']):
                            continue
                        route['recent'] = [t for t in route['recent'] if now-t < 1]
                        if len(route['recent']) >= ctx.config.messages_per_second:
                            route['dropped'] += 1
                            route['lost_through'] = self.sequence
                        else:
                            route['recent'].append(now)
                            if len(route['events']) == ctx.config.queue_capacity:
                                route['dropped'] += 1
                                route['lost_through'] = max(route['lost_through'], route['events'][0]['sequence'])
                            route['events'].append({'sequence': self.sequence, 'text': text,
                                'payload': text[len(route['prefix']):], 'source_name': str(model.SourceName),
                                'connection_id': event.connection_id, 'source_sequence': event.sequence,
                                'observed_at': event.observed_at, 'authenticated_player': None,
                                'source': 'system_text_untrusted'})
                        if route['consume']:
                            break
        except PacketSubscriptionError as error:
            self.stream_status = error.reason
        finally:
            if self.stream_status == 'active':
                self.stream_status = 'closed'

    @staticmethod
    def message_text(model, limit):
        # neomega-core TextTypeRaw=0; ObjectWhisper/Object/ObjectAnnouncement=9/10/11.
        # Explicit rawtext text segments support tellraw; no selectors or translation
        # are evaluated locally and no Text payload grants player authority.
        if not isinstance(model.Message, str) or len(model.Message.encode()) > limit:
            return None
        if model.TextType == 0:
            return model.Message
        if model.TextType not in (9, 10, 11):
            return None
        try:
            body = json.loads(model.Message)
        except (ValueError, TypeError):
            return None
        parts = body.get('rawtext') if isinstance(body, dict) else None
        if not isinstance(parts, list) or any(not isinstance(part, dict) or set(part) != {'text'}
                                             or not isinstance(part['text'], str) for part in parts):
            return None
        text = ''.join(part['text'] for part in parts)
        return text if len(text.encode()) <= limit else None

    async def dispatch(self, ctx, args, call, action):
        self.prune()
        if action == 'status':
            offset, limit = args.get('offset', 0), args.get('limit', 64)
            if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 64:
                raise ServiceRejected('invalid_argument')
            routes = [self.describe(r) for r in self.routes.values() if r['owner'] == call.installation_id]
            page = routes[offset:offset+limit]
            return {'stream': self.stream_status, 'sealed': self.sealed, 'sequence': self.sequence,
                    'routes': page, 'total': len(routes), 'next_offset': offset+len(page),
                    'done': offset+len(page) >= len(routes)}
        if action == 'register':
            if self.sealed or len(self.routes) >= ctx.config.max_routes:
                raise ServiceRejected('busy')
            prefix = args['prefix']
            if not prefix or len(prefix.encode()) > 128:
                raise ServiceRejected('invalid_argument')
            priority = args.get('priority', 0)
            for route in self.routes.values():
                if route['prefix'] == prefix and route['priority'] == priority:
                    raise ServiceRejected('conflict')
            rid = uuid4().hex
            route = {'id': rid, 'owner': call.installation_id, 'prefix': prefix,
                     'priority': priority, 'consume': args.get('consume', True),
                     'expires_at': time.time()+ctx.config.lease_seconds,
                     'events': deque(maxlen=ctx.config.queue_capacity), 'recent': [], 'dropped': 0, 'lost_through': 0}
            self.routes[rid] = route
            return self.describe(route)
        route = self.routes.get(args['route_id'])
        if not route or route['owner'] != call.installation_id:
            raise ServiceRejected('not_found')
        if action == 'unregister':
            del self.routes[route['id']]
            return {'removed': True}
        route['expires_at'] = time.time()+ctx.config.lease_seconds
        if action == 'subscribe':
            return dict(self.describe(route), cursor=self.sequence)
        cursor = args.get('cursor', 0)
        rows = [r for r in route['events'] if r['sequence'] > cursor][:4]
        return {'events': rows, 'cursor': rows[-1]['sequence'] if rows else self.sequence,
                'dropped': route['dropped'], 'gap': cursor < route['lost_through'],
                'expires_at': route['expires_at']}

    @staticmethod
    def describe(route):
        return {k: route[k] for k in ('id','prefix','priority','consume','expires_at','dropped')}

    @service('register', major=1, with_context=True)
    async def register(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'register')

    @service('unregister', major=1, with_context=True)
    async def unregister(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'unregister')

    @service('subscribe', major=1, with_context=True)
    async def subscribe(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'subscribe')

    @service('poll', major=1, with_context=True)
    async def poll(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'poll')

    @service('status', major=1, with_context=True)
    async def status(self, ctx, arguments, call):
        return await self.dispatch(ctx, arguments, call, 'status')

if __name__ == '__main__':
    CBBridge().run()
