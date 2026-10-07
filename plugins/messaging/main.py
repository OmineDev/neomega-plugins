"""Persistent, authenticated OneBot/Telegram message routing provider."""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

from ws import OneBotSocket

from neomega_runtime import Plugin, service, ServiceRejected
from neomega_runtime.managed import IPCRejected
from neomega_runtime.storage import CommitUncertain


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


def deadline():
    return (datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat()


@dataclass
class AdminCommand:
    name: str
    command: str
    capability: str

    def __post_init__(self):
        if not self.name or any(c.isspace() for c in self.name) or not self.command or len(self.command) > 2048:
            raise ValueError('invalid admin command')


@dataclass
class ExternalIdentity:
    external_id: str
    player_id: str


@dataclass
class Route:
    id: str = 'group'
    adapter: str = 'polaris_ws'
    endpoint: str = 'ws://localhost:18766/'
    secret: str = ''
    webhook_secret: str = 'secret:bridge-webhook'
    group_id: str = ''
    game_to_group: bool = True
    group_to_game: bool = True
    allowed_installations: list[str] = field(default_factory=list)
    prefix: str = ''
    cb_prefix: str = ''
    admin_commands: list[AdminCommand] = field(default_factory=list)
    identities: list[ExternalIdentity] = field(default_factory=list)
    roles_namespace: str = ''
    messages_per_minute: int = 30

    def __post_init__(self):
        if not self.id or len(self.id) > 64 or not all(c.isalnum() or c in '_-' for c in self.id):
            raise ValueError('invalid route id')
        if self.adapter not in ('onebot', 'telegram', 'polaris_ws') or not 1 <= self.messages_per_minute <= 600:
            raise ValueError('invalid adapter or limit')
        target = urlsplit(self.endpoint)
        if target.username or target.password or target.query or target.fragment or target.scheme not in (('ws', 'wss') if self.adapter == 'polaris_ws' else ('http', 'https')):
            raise ValueError('invalid endpoint')
        if target.scheme in ('http', 'ws') and target.hostname not in ('localhost', '127.0.0.1', '::1'):
            raise ValueError('remote endpoints require TLS')
        if self.adapter == 'telegram' and self.endpoint.rstrip('/') != 'https://api.telegram.org':
            raise ValueError('Telegram endpoint must be api.telegram.org')
        if self.adapter in ('onebot', 'polaris_ws') and (not self.group_id.isdigit() or int(self.group_id) <= 0):
            raise ValueError('OneBot group_id must be a positive QQ group number')
        if self.adapter == 'telegram' and (not self.group_id or not self.group_id.lstrip('-').isdigit()):
            raise ValueError('Telegram group_id must be a numeric chat ID')
        if self.admin_commands and not self.roles_namespace:
            raise ValueError('admin commands require an explicit roles namespace')
        if len({c.name for c in self.admin_commands}) != len(self.admin_commands) or len({i.external_id for i in self.identities}) != len(self.identities):
            raise ValueError('duplicate admin command or external identity')
        if (self.secret and not self.secret.startswith('secret:')) or (not self.secret and self.adapter != 'polaris_ws') or not self.webhook_secret.startswith('secret:'):
            raise ValueError('use installation secret references')


@dataclass
class Settings:
    routes: list[Route] = field(default_factory=list)
    webhook_host: str = '127.0.0.1'
    webhook_port: int = 0
    poll_seconds: float = 2.0
    max_records: int = 10000
    max_text_bytes: int = 3000
    subscription_history: int = 1000

    def __post_init__(self):
        if not 0 <= self.webhook_port <= 65535 or not 0.25 <= self.poll_seconds <= 60:
            raise ValueError('invalid webhook/poll settings')
        if not 10 <= self.max_records <= 100000 or not 64 <= self.max_text_bytes <= 4096 or not 10 <= self.subscription_history <= 10000:
            raise ValueError('invalid capacity')
        if len(self.routes) > 20 or len({r.id for r in self.routes}) != len(self.routes):
            raise ValueError('duplicate or excessive routes')
        tg = [r.secret for r in self.routes if r.adapter == 'telegram']
        if len(tg) != len(set(tg)):
            raise ValueError('one Telegram route per bot token; use a distinct bot per group')


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def http_json(url, body, token=None):
    headers = {'Content-Type': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers)
    with urllib.request.build_opener(NoRedirect).open(req, timeout=10) as response:
        raw = response.read(1048577)
    if len(raw) > 1048576:
        raise ValueError('response too large')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('invalid response')
    return value


class Messaging(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = None
        self.server = None
        self.windows = {}
        self.health = {}
        self.active_http = 0
        self.webhooks = 0
        self.pending = None
        self.pending_path = None
        self.cb_routes = {}
        self.sockets = {}
        self.stopping = False

    def clear_pending(self):
        self.pending = None
        self.pending_path.unlink(missing_ok=True)
        fd = os.open(self.pending_path.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    async def writable(self, ctx):
        if self.sealed:
            raise ServiceRejected('busy')
        if self.pending:
            if await ctx.storage.receipt(self.pending) is None:
                raise ServiceRejected('unknown')
            self.clear_pending()

    async def state(self, ctx, *, ids=(), owner=None, events=False, route=None):
        meta = await ctx.storage.get('msg_meta', None)
        if meta is None:
            if await ctx.storage.get('messaging', None) is not None:
                raise RuntimeError('legacy draft messaging storage requires explicit migration')
            meta = {'schema_version': 2, 'offsets': {}, 'sequence': 0, 'delivery_count': 0,
                    'seen_count': 0, 'unresolved': 0}
        if meta.get('schema_version') != 2:
            raise RuntimeError('unsupported messaging state version')
        state = dict(meta, deliveries={}, seen={}, events=[], queues={}, owners={})
        selected = list(ids)
        if owner is not None:
            selected += await ctx.storage.get('msg_owner_' + digest(owner), [])
        if route is not None:
            queue = await ctx.storage.get('msg_queue_' + route, [])
            state['queues'][route] = queue
            selected += queue[:1]
        for key in dict.fromkeys(selected):
            record = await ctx.storage.get('msg_delivery_' + key)
            if record:
                state['deliveries'][key] = record
        if events:
            first = max(1, meta['sequence'] - ctx.config.subscription_history + 1)
            # Scan bounded pages; each Host request reads just one small value.
            start = max(first, events if type(events) is int else first)
            for seq in range(start, min(meta['sequence'] + 1, start + 200)):
                row = await ctx.storage.get('msg_event_' + str(seq % ctx.config.subscription_history))
                if row and row['sequence'] == seq:
                    state['events'].append(row)
        return state

    async def save(self, ctx, state, *, event=None, intent=None):
        await self.writable(ctx)
        tx = await ctx.storage.transaction(keys=[], event=event.payload if event is not None else None)
        tx.set('msg_meta', {k: state[k] for k in ('schema_version', 'offsets', 'sequence', 'delivery_count', 'seen_count', 'unresolved')})
        for key, row in state['deliveries'].items():
            tx.set('msg_delivery_' + key, row)
        for key, row in state['seen'].items():
            row['commit_id'] = tx.commit_id
            tx.set('msg_seen_' + key, row)
        for route, queue in state['queues'].items():
            tx.set('msg_queue_' + route, queue)
        for owner, keys in state['owners'].items():
            tx.set('msg_owner_' + digest(owner), keys)
        for row in state['events']:
            tx.set('msg_event_' + str(row['sequence'] % ctx.config.subscription_history), row)
        if intent is not None:
            tx.action(intent)
        tx.prepare()
        self.pending = tx.commit_id
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
        except CommitUncertain:
            raise ServiceRejected('unknown') from None
        # Cancellation preserves the original commit ID for receipt-only reconciliation.
        self.clear_pending()

    def route(self, ctx, route_id):
        for route in ctx.config.routes:
            if route.id == route_id:
                return route
        raise ServiceRejected('not_found')

    def authorized(self, route, owner):
        return owner in route.allowed_installations

    def rate(self, route, direction):
        now = time.monotonic()
        key = (route.id, direction)
        entries = [t for t in self.windows.get(key, []) if now - t < 60]
        self.windows[key] = entries
        if len(entries) >= route.messages_per_minute:
            return False
        entries.append(now)
        return True

    def emit(self, ctx, state, record):
        state['sequence'] += 1
        state['events'].append({'sequence': state['sequence'], **record})
        state['events'] = state['events'][-ctx.config.subscription_history:]

    async def enqueue(self, ctx, state, owner, route, text, request_id):
        if self.sealed:
            raise ServiceRejected('busy')
        if not isinstance(text, str) or not 1 <= len(text.encode()) <= ctx.config.max_text_bytes:
            raise ServiceRejected('invalid_argument')
        if not isinstance(request_id, str) or not 1 <= len(request_id) <= 128:
            raise ServiceRejected('invalid_argument')
        key = digest([owner, request_id])
        parameters = digest([route.id, text])
        await self.writable(ctx)
        previous = state['deliveries'].get(key) or await ctx.storage.get('msg_delivery_' + key)
        if previous:
            if previous['parameters'] != parameters:
                raise ServiceRejected('conflict')
            return previous
        if state['delivery_count'] >= ctx.config.max_records:
            raise ServiceRejected('busy')
        record = {'delivery_id': key, 'owner': owner, 'request_id': request_id, 'parameters': parameters,
                  'route_id': route.id, 'text': text, 'state': 'queued', 'created_at': time.time(), 'direction': 'outbound'}
        queue = state['queues'].setdefault(route.id, await ctx.storage.get('msg_queue_' + route.id, []))
        if len(queue) >= 64:
            raise ServiceRejected('busy')
        queue.append(key)
        owner_keys = state['owners'].setdefault(owner, await ctx.storage.get('msg_owner_' + digest(owner), []))
        owner_keys.append(key)
        state['owners'][owner] = owner_keys[-100:]
        state['delivery_count'] += 1
        state['deliveries'][key] = record
        self.emit(ctx, state, self.public(record))
        return record

    @staticmethod
    def public(record):
        return {k: v for k, v in record.items() if k not in ('parameters', 'text')}

    async def messaging(self, ctx, arguments, call_context):
        if call_context is None:
            raise ServiceRejected('permission_denied')
        owner = call_context.installation_id
        action = arguments.get('action')
        async with self.lock:
            if action == 'send':
                await self.writable(ctx)
            after = arguments.get('after', 0)
            if action == 'subscribe' and (type(after) is not int or after < 0):
                raise ServiceRejected('invalid_argument')
            key = arguments.get('delivery_id')
            if key is not None and (not isinstance(key, str) or len(key) != 64 or any(c not in '0123456789abcdef' for c in key)):
                raise ServiceRejected('invalid_argument')
            state = await self.state(ctx, ids=[key] if key else [], owner=owner if action == 'status' and not key else None,
                                     events=(after + 1) if action == 'subscribe' else False)
            visible = {r.id for r in ctx.config.routes if self.authorized(r, owner)}
            if action == 'routes':
                return {'routes': [{'route_id': r.id, 'adapter': r.adapter, 'game_to_group': r.game_to_group,
                        'group_to_game': r.group_to_game, 'health': self.health.get(r.id, 'not_connected')}
                        for r in ctx.config.routes if r.id in visible]}
            if action == 'send':
                route = self.route(ctx, arguments.get('route_id'))
                if route.id not in visible:
                    raise ServiceRejected('permission_denied')
                record = await self.enqueue(ctx, state, owner, route, arguments.get('text'), arguments.get('request_id'))
                await self.save(ctx, state)
                return self.public(record)
            if action == 'status':
                key = arguments.get('delivery_id')
                records = [r for r in state['deliveries'].values() if r['owner'] == owner and r['route_id'] in visible]
                if key:
                    records = [r for r in records if r['delivery_id'] == key]
                    if not records:
                        raise ServiceRejected('not_found')
                rows, size = [], 0
                for row in reversed(records[-100:]):
                    public = self.public(row)
                    size += len(json.dumps(public, ensure_ascii=False).encode())
                    if size > 45000:
                        break
                    rows.append(public)
                return {'deliveries': list(reversed(rows)), 'sealed': self.sealed, 'limited': len(rows) < len(records)}
            if action == 'subscribe':
                after, limit = arguments.get('after', 0), arguments.get('limit', 100)
                if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 200:
                    raise ServiceRejected('invalid_argument')
                events = [r for r in state['events'] if r['sequence'] > after and r['route_id'] in visible
                          and r.get('owner') in (owner, 'external')][:limit]
                bounded, size = [], 0
                for event in events:
                    size += len(json.dumps(event, ensure_ascii=False).encode())
                    if size > 45000:
                        break
                    bounded.append(event)
                events = bounded
                return {'events': events, 'cursor': events[-1]['sequence'] if events else (state['events'][-1]['sequence'] if state['events'] else state['sequence']),
                        'gap': after < max(0, state['sequence'] - ctx.config.subscription_history)}
            raise ServiceRejected('invalid_argument')

    @service('send', with_context=True)
    async def send(self, ctx, args, call):
        return await self.messaging(ctx, {**args, 'action': 'send'}, call)

    @service('routes', with_context=True)
    async def routes(self, ctx, args, call):
        return await self.messaging(ctx, {**args, 'action': 'routes'}, call)

    @service('status', with_context=True)
    async def status(self, ctx, args, call):
        return await self.messaging(ctx, {**args, 'action': 'status'}, call)

    @service('subscribe', with_context=True)
    async def subscribe(self, ctx, args, call):
        return await self.messaging(ctx, {**args, 'action': 'subscribe'}, call)

    async def remote(self, ctx, route, method, body):
        if route.adapter == 'polaris_ws':
            connection = self.sockets.get(route.id)
            if connection is None:
                raise ConnectionError('gateway disconnected')
            self.active_http += 1
            try:
                return await connection.call(method, body)
            finally:
                self.active_http -= 1
        token = ctx.secrets.get(route.secret).strip()
        url = route.endpoint.rstrip('/')
        if route.adapter == 'telegram':
            url += '/bot' + token + '/' + method
            token = None
        else:
            url += '/' + method
        self.active_http += 1
        try:
            # Bounded network timeout; never include exceptions containing token URLs in logs.
            task = asyncio.create_task(asyncio.to_thread(http_json, url, body, token))
            try:
                return await asyncio.shield(task)
            except asyncio.CancelledError:
                await task
                raise
        finally:
            self.active_http -= 1

    async def flush(self, ctx):
        # Mark each message before network I/O. Restart never replays dispatching/unknown.
        for route in ctx.config.routes:
            if route.adapter == 'polaris_ws' and route.id not in self.sockets:
                continue
            async with self.lock:
                if self.sealed:
                    return
                await self.writable(ctx)
                state = await self.state(ctx, route=route.id)
                for old in state['deliveries'].values():
                    if old['state'] == 'dispatching':
                        old['state'] = 'unknown'
                        state['queues'][route.id].remove(old['delivery_id'])
                        await self.save(ctx, state)
                record = next((r for r in state['deliveries'].values() if r['route_id'] == route.id and r['state'] == 'queued'), None)
                if not record or not self.rate(route, 'outbound'):
                    continue
                record['state'] = 'dispatching'
                state['unresolved'] += 1
                await self.save(ctx, state)
                key, text = record['delivery_id'], record['text']
            try:
                if route.adapter == 'telegram':
                    response = await self.remote(ctx, route, 'sendMessage', {'chat_id': route.group_id, 'text': text})
                    ok, remote_id = response.get('ok') is True, response.get('result', {}).get('message_id')
                else:
                    response = await self.remote(ctx, route, 'send_group_msg', {'group_id': int(route.group_id),
                                    'message': [{'type': 'text', 'data': {'text': text}}], 'auto_escape': True})
                    ok, remote_id = response.get('status') == 'ok' and response.get('retcode') == 0, (response.get('data') or {}).get('message_id')
                result = 'delivered' if ok else 'rejected'
            except Exception:
                result, remote_id = 'unknown', None
            async with self.lock:
                await self.writable(ctx)
                state = await self.state(ctx, ids=[key], route=route.id)
                record = state['deliveries'][key]
                state['queues'][route.id].remove(key)
                if result != 'unknown':
                    state['unresolved'] -= 1
                record.update(state=result, remote_id=str(remote_id) if remote_id is not None else None, updated_at=time.time())
                if result in ('delivered', 'rejected'):
                    record.pop('text', None)
                self.health[route.id] = 'connected' if result != 'unknown' else 'unavailable'
                self.emit(ctx, state, self.public(record))
                await self.save(ctx, state)

    async def incoming(self, ctx, route, message_id, sender, text, *, self_message=False, external_id=None):
        if self_message or not route.group_to_game:
            return
        if not isinstance(text, str) or not text or len(text.encode()) > ctx.config.max_text_bytes:
            return
        key = digest([route.id, str(message_id)])
        async with self.lock:
            if self.sealed:
                raise ServiceRejected('busy')
            await self.writable(ctx)
            state = await self.state(ctx)
            if await ctx.storage.get('msg_seen_' + key):
                return
            if state['seen_count'] >= ctx.config.max_records:
                raise ServiceRejected('busy')
            if not self.rate(route, 'inbound'):
                raise ServiceRejected('busy')
            # Only a configured immutable command may cross the administration boundary.
            command_text = None
            rejected = False
            if text.startswith('/admin '):
                command = next((c for c in route.admin_commands if text == '/admin ' + c.name), None)
                identity = next((i for i in route.identities if i.external_id == str(external_id)), None)
                rejected = True
                if command and identity:
                    try:
                        decision = await ctx.services.call('plugin.neomega.roles.check', {
                            'namespace': route.roles_namespace, 'player_id': identity.player_id,
                            'capability': command.capability, 'scope': route.id})
                        if decision.get('allowed') is True:
                            command_text, rejected = command.command, False
                    except Exception:
                        rejected = True
            formatted = '[' + route.id + '] ' + str(sender)[:80] + ': ' + text
            intent = None if rejected else ctx.commands.prepare(command_text or ('tellraw @a ' + json.dumps({'rawtext': [{'text': formatted}]}, ensure_ascii=False)),
                                          idempotency_key='bridge_' + key, deadline=deadline())
            state['seen_count'] += 1
            state['seen'][key] = {'route_id': route.id, 'received_at': time.time(), 'commit_id': 'bridge_' + key}
            self.emit(ctx, state, {'route_id': route.id, 'owner': 'external', 'direction': 'inbound',
                                  'message_id': str(message_id), 'sender': str(sender)[:80], 'text': text, 'state': 'permission_denied' if rejected else 'accepted'})
            await self.save(ctx, state, intent=intent)

    async def poll(self, ctx):
        for route in ctx.config.routes:
            if route.adapter != 'telegram' or not route.group_to_game or self.sealed:
                continue
            async with self.lock:
                state = await self.state(ctx)
                offset = state['offsets'].get(route.id, 0)
            try:
                result = await self.remote(ctx, route, 'getUpdates', {'offset': offset, 'limit': 50, 'timeout': 0,
                                                                  'allowed_updates': ['message']})
                if result.get('ok') is not True:
                    self.health[route.id] = 'unavailable'
                    continue
                for update in result['result']:
                    message = update.get('message', {})
                    if str(message.get('chat', {}).get('id')) == route.group_id:
                        sender = message.get('from', {})
                        await self.incoming(ctx, route, message.get('message_id'), sender.get('first_name', sender.get('id', '')),
                                            message.get('text', ''), self_message=sender.get('is_bot') is True, external_id=sender.get('id'))
                    async with self.lock:
                        await self.writable(ctx)
                        state = await self.state(ctx)
                        state['offsets'][route.id] = int(update['update_id']) + 1
                        await self.save(ctx, state)
                self.health[route.id] = 'connected'
            except Exception:
                self.health[route.id] = 'unavailable'

    async def onebot_event(self, ctx, route, event):
        if (event.get('post_type') != 'message' or event.get('message_type') != 'group'
                or str(event.get('group_id')) != route.group_id or event.get('message_id') is None):
            return
        segments = event.get('message')
        if isinstance(segments, list):
            text = ''.join(str(p.get('data', {}).get('text', '')) for p in segments if isinstance(p, dict) and p.get('type') == 'text')
        elif isinstance(segments, str):
            # CQ-string mode is forwarded literally, never evaluated as CQ or a command.
            text = segments
        else:
            return
        sender = event.get('sender', {})
        await self.incoming(ctx, route, event['message_id'], sender.get('card') or sender.get('nickname') or event.get('user_id'),
                            text, self_message=event.get('self_id') == event.get('user_id'), external_id=event.get('user_id'))

    async def socket_loop(self, ctx, route):
        while not self.stopping:
            connection = OneBotSocket(route.endpoint, ctx.secrets.get(route.secret).strip() if route.secret else '')
            receiver = None
            try:
                await connection.connect()
                self.sockets[route.id] = connection
                self.health[route.id] = 'connected'
                inbox = asyncio.Queue(maxsize=64)
                receiver = asyncio.create_task(connection.receive(inbox))
                while not receiver.done():
                    next_event = asyncio.create_task(inbox.get())
                    try:
                        ready, _ = await asyncio.wait((receiver, next_event), return_when=asyncio.FIRST_COMPLETED)
                        if next_event in ready:
                            try:
                                await self.onebot_event(ctx, route, next_event.result())
                            except ServiceRejected:
                                self.health[route.id] = 'backpressured'
                    finally:
                        next_event.cancel()
                        await asyncio.gather(next_event, return_exceptions=True)
                await receiver
            except asyncio.CancelledError:
                raise
            except Exception:
                self.health[route.id] = 'unavailable'
            finally:
                self.sockets.pop(route.id, None)
                if receiver:
                    receiver.cancel()
                    await asyncio.gather(receiver, return_exceptions=True)
                await connection.close()
            if not self.stopping:
                await asyncio.sleep(5)

    async def poll_cbbridge(self, ctx):
        if self.sealed:
            return
        for route in ctx.config.routes:
            if not route.cb_prefix or not route.game_to_group:
                continue
            try:
                subscription = self.cb_routes.get(route.id)
                if subscription is None:
                    registered = await ctx.services.call('plugin.neomega.cbbridge.register',
                                                         {'prefix': route.cb_prefix, 'priority': 0, 'consume': False})
                    subscription = {'route_id': registered['id'], 'cursor': 0}
                    current = await ctx.services.call('plugin.neomega.cbbridge.subscribe', {'route_id': registered['id']})
                    subscription['cursor'] = current['cursor']
                    self.cb_routes[route.id] = subscription
                result = await ctx.services.call('plugin.neomega.cbbridge.poll', subscription)
                for event in result['events']:
                    async with self.lock:
                        await self.writable(ctx)
                        state = await self.state(ctx)
                        # Untrusted system text is never used as an administrative identity.
                        await self.enqueue(ctx, state, 'game', route, '[Command block] ' + event['payload'],
                                           digest(['cb', route.id, event['connection_id'], event['source_sequence']]))
                        await self.save(ctx, state)
                subscription['cursor'] = result['cursor']
                if result.get('gap'):
                    self.health[route.id] = 'cbbridge_gap'
            except ServiceRejected as exc:
                if exc.code in ('not_found', 'unavailable'):
                    self.cb_routes.pop(route.id, None)
                self.health[route.id] = 'cbbridge_' + exc.code
            except Exception:
                self.health[route.id] = 'cbbridge_unavailable'

    async def webhook(self, ctx, reader, writer):
        self.webhooks += 1
        code = 400
        try:
            header = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 5)
            if len(header) > 8192:
                raise ValueError('header too large')
            lines = header.decode('ascii').split('\r\n')
            method, path, _ = lines[0].split(' ')
            headers = dict(line.lower().split(':', 1) for line in lines[1:] if ':' in line)
            headers = {k.strip(): v.strip() for k, v in headers.items()}
            route = self.route(ctx, path.removeprefix('/'))
            length = int(headers.get('content-length', '0'))
            if method != 'POST' or route.adapter != 'onebot' or not 0 < length <= 65536 or 'transfer-encoding' in headers:
                raise ValueError('invalid webhook')
            body = await asyncio.wait_for(reader.readexactly(length), 5)
            secret = ctx.secrets.get(route.webhook_secret).strip().encode()
            expected = 'sha1=' + hmac.new(secret, body, hashlib.sha1).hexdigest()
            if not hmac.compare_digest(expected, headers.get('x-signature', '')):
                code = 403
            else:
                event = json.loads(body)
                if event.get('post_type') == 'message' and event.get('message_type') == 'group' and str(event.get('group_id')) == route.group_id:
                    segments = event.get('message')
                    # Require structured segments: never interpret CQ escape strings.
                    if isinstance(segments, list) and event.get('message_id') is not None:
                        text = ''.join(str(p.get('data', {}).get('text', '')) for p in segments if p.get('type') == 'text')
                        sender = event.get('sender', {})
                        await self.incoming(ctx, route, event['message_id'], sender.get('card') or sender.get('nickname') or event.get('user_id'),
                                            text, self_message=event.get('self_id') == event.get('user_id'), external_id=event.get('user_id'))
                code = 200
        except ServiceRejected:
            code = 503
        except Exception:
            code = 400
        finally:
            self.webhooks -= 1
            writer.write(f'HTTP/1.1 {code} Response\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}'.encode())
            try:
                await writer.drain()
            except (ConnectionError, OSError):
                pass
            writer.close()
            await writer.wait_closed()

    async def on_start(self, ctx):
        self.pending_path = ctx.data_dir / 'messaging-pending.json'
        if self.pending_path.exists():
            self.pending = json.loads(self.pending_path.read_text())['commit_id']
        # Uncertain transactions are reconciled only by their original Host receipt.
        async with self.lock:
            await self.state(ctx)
        if ctx.config.webhook_port:
            def accept(reader, writer):
                if self.webhooks >= 64 or self.sealed:
                    writer.close()
                    return
                ctx.spawn(self.webhook(ctx, reader, writer), name='bridge-webhook')
            self.server = await asyncio.start_server(accept, ctx.config.webhook_host, ctx.config.webhook_port, limit=8192)
        ctx.every(ctx.config.poll_seconds, lambda: self.flush(ctx), name='bridge-outbox')
        ctx.every(ctx.config.poll_seconds, lambda: self.poll(ctx), name='bridge-inbound')
        ctx.every(ctx.config.poll_seconds, lambda: self.poll_cbbridge(ctx), name='bridge-cbbridge')
        for route in ctx.config.routes:
            if route.adapter == 'polaris_ws':
                ctx.spawn(self.socket_loop(ctx, route), name='bridge-ws-' + route.id)

    async def on_event(self, ctx, event):
        if event.kind != 'chat.received':
            await event.ack()
            return
        payload = event.payload['payload']
        # Only actual player chats with a roster-confirmed XUID are bridge sources.
        name, xuid, text = payload.get('player'), payload.get('xuid'), payload.get('message', '')
        if not name or not xuid or not isinstance(text, str):
            await event.ack()
            return
        players = await ctx.players()
        if not any(p.get('name') == name and p.get('xuid') == xuid for p in players):
            await event.ack()
            return
        async with self.lock:
            await self.writable(ctx)
            state = await self.state(ctx)
            token = event.payload['delivery_token']
            for route in ctx.config.routes:
                if route.game_to_group and text.startswith(route.prefix):
                    try:
                        await self.enqueue(ctx, state, 'game', route, '<' + name + '> ' + text,
                                           digest([token, route.id]))
                    except ServiceRejected as exc:
                        if exc.code == 'busy':
                            raise
                        continue
            await self.save(ctx, state, event=event)

    async def on_maintenance(self, ctx, request):
        async with self.lock:
            if request['operation'] == 'release':
                if self.sealed not in (None, request['token']):
                    return {'status': 'busy', 'token': request['token']}
                self.sealed = None
                return {'status': 'released', 'token': request['token']}
            if self.pending:
                if await ctx.storage.receipt(self.pending) is None:
                    return {'status': 'busy', 'token': request['token']}
                self.clear_pending()
            state = await self.state(ctx)
            if self.active_http or state['unresolved']:
                return {'status': 'busy', 'token': request['token']}
            if self.sealed not in (None, request['token']):
                return {'status': 'busy', 'token': request['token']}
            self.sealed = request['token']
            return {'status': 'sealed', 'token': request['token']}

    async def on_stop(self, ctx):
        self.stopping = True
        for connection in list(self.sockets.values()):
            await connection.close()
        for subscription in self.cb_routes.values():
            try:
                await ctx.services.call('plugin.neomega.cbbridge.unregister', {'route_id': subscription['route_id']})
            except Exception:
                pass
        if self.server:
            self.server.close()
            await self.server.wait_closed()


if __name__ == '__main__':
    Messaging().run()
