"""Persistent, authenticated OneBot/Telegram message routing provider."""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import math
import os
import re
import time
import urllib.error
import urllib.request
from urllib.parse import urlsplit

from ws import OneBotSocket

from neomega_runtime import Plugin, ProtocolError, service, ServiceRejected
from neomega_runtime.managed import IPCError, IPCRejected
from neomega_runtime.secrets import SecretError
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
        if (self.adapter not in ('onebot', 'telegram', 'polaris_ws')
                or type(self.messages_per_minute) is not int
                or not 1 <= self.messages_per_minute <= 600):
            raise ValueError('invalid adapter or limit')
        target = urlsplit(self.endpoint)
        if target.username or target.password or target.query or target.fragment or target.scheme not in (('ws', 'wss') if self.adapter == 'polaris_ws' else ('http', 'https')):
            raise ValueError('invalid endpoint')
        if target.scheme in ('http', 'ws') and target.hostname not in ('localhost', '127.0.0.1', '::1'):
            raise ValueError('remote endpoints require TLS')
        if self.adapter == 'telegram' and self.endpoint.rstrip('/') != 'https://api.telegram.org':
            raise ValueError('Telegram endpoint must be api.telegram.org')
        if self.adapter in ('onebot', 'polaris_ws') and (not self.group_id.isascii() or not self.group_id.isdigit() or int(self.group_id) <= 0):
            raise ValueError('OneBot group_id must be a positive QQ group number')
        if self.adapter == 'telegram' and (not re.fullmatch(r'-?[0-9]+', self.group_id) or int(self.group_id) == 0):
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
    retention_seconds: int = 86400
    inbound_retention_seconds: int = 86400
    clock_skew_seconds: int = 60

    def __post_init__(self):
        if (type(self.webhook_port) is not int or not 0 <= self.webhook_port <= 65535
                or type(self.poll_seconds) not in (int, float) or not math.isfinite(self.poll_seconds)
                or not 0.25 <= self.poll_seconds <= 60):
            raise ValueError('invalid webhook/poll settings')
        if (type(self.max_records) is not int or not 10 <= self.max_records <= 100000
                or type(self.max_text_bytes) is not int or not 64 <= self.max_text_bytes <= 4096
                or type(self.subscription_history) is not int or not 10 <= self.subscription_history <= 10000):
            raise ValueError('invalid capacity')
        if (type(self.retention_seconds) is not int or not 1 <= self.retention_seconds <= 2592000
                or type(self.inbound_retention_seconds) is not int or not 1 <= self.inbound_retention_seconds <= 2592000
                or type(self.clock_skew_seconds) is not int or not 0 <= self.clock_skew_seconds <= 300):
            raise ValueError('invalid retention or clock skew')
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
        self.in_flight = set()
        self.pending = None
        self.pending_path = None
        self.cb_routes = {}
        self.sockets = {}
        self.stopping = False

    def clear_pending(self):
        self.pending = None
        if self.pending_path is None:
            return
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

    async def _load_state(self, ctx, *, ids=(), owner=None, events=False, route=None):
        meta = await ctx.storage.get('msg_meta', None)
        exists = meta is not None
        was_v2 = False
        if meta is None:
            if await ctx.storage.get('messaging', None) is not None:
                raise RuntimeError('legacy draft messaging storage requires explicit migration')
            meta = {'schema_version': 2, 'offsets': {}, 'sequence': 0, 'delivery_count': 0,
                    'seen_count': 0, 'unresolved': 0}
        elif meta.get('schema_version') == 2:
            was_v2 = True
        persisted_rollback = bool(meta.get('clock_rollback', False))
        if meta.get('schema_version') == 2:
            # No full snapshot: old counts and all original per-record keys survive.
            now = int(time.time() * 1000)
            meta = dict(meta, schema_version=3, legacy_cutoff=now, last_seen_clock=now,
                        expired_before=now - ctx.config.retention_seconds * 1000,
                        inbound_expired_before=now - ctx.config.inbound_retention_seconds * 1000,
                        gc_delivery_cursor=None, gc_seen_cursor=None, clock_rollback=False)
        if meta.get('schema_version') != 3:
            raise RuntimeError('unsupported messaging state version')
        state = dict(meta, deliveries={}, seen={}, events=[], queues={}, owners={})
        selected = list(ids)
        if owner is not None:
            selected += await ctx.storage.get('msg_owner_' + digest(owner), [])
        if route is not None:
            queue = await ctx.storage.get('msg_queue_' + route, [])
            state['queues'][route] = queue
            selected += queue
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
        return state, {'exists': exists, 'was_v2': was_v2, 'persisted_rollback': persisted_rollback}

    async def state(self, ctx, *, ids=(), owner=None, events=False, route=None):
        state, _ = await self._load_state(ctx, ids=ids, owner=owner, events=events, route=route)
        return state

    async def save(self, ctx, state, *, event=None, intent=None, deletes=()):
        await self.writable(ctx)
        if event is not None:
            tx = await event.transaction(keys=[])
        else:
            tx = await ctx.storage.transaction(keys=[])
        tx.set('msg_meta', {k: state[k] for k in ('schema_version', 'offsets', 'sequence', 'delivery_count', 'seen_count', 'unresolved',
                   'legacy_cutoff', 'last_seen_clock', 'expired_before', 'inbound_expired_before',
                   'gc_delivery_cursor', 'gc_seen_cursor', 'clock_rollback')})
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
        for key in deletes:
            tx.delete(key)
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

    async def housekeeping(self, ctx):
        """Called under the admission lock; cutoff commits before any record deletion."""
        await self.writable(ctx)
        state, info = await self._load_state(ctx)
        system_now = int(time.time() * 1000)
        now = max(system_now, state['last_seen_clock'])
        state['clock_rollback'] = system_now < state['last_seen_clock']
        state['last_seen_clock'] = now
        state['expired_before'] = max(state['expired_before'], now - ctx.config.retention_seconds * 1000)
        state['inbound_expired_before'] = max(state['inbound_expired_before'], now - ctx.config.inbound_retention_seconds * 1000)
        cutoff_saved = False
        meta_dirty = info['was_v2'] or (info['exists'] and state['clock_rollback'] != info['persisted_rollback'])
        for kind, prefix, count, cursor in (
                ('delivery', 'msg_delivery_', 'delivery_count', 'gc_delivery_cursor'),
                ('seen', 'msg_seen_', 'seen_count', 'gc_seen_cursor')):
            page = await ctx.storage.scan_keys(prefix=prefix, after_key=state[cursor], limit=32)
            deletes = []
            for key in page['keys']:
                row = await ctx.storage.get(key)
                if row is None:
                    continue
                if kind == 'delivery':
                    if row['state'] not in ('delivered', 'rejected'):
                        continue
                    if row.get('id_policy') == 'source_window':
                        expired = row['created_at'] * 1000 < state['inbound_expired_before']
                    else:
                        issued = self.request_time(row['request_id'])
                        expired = (issued < state['expired_before'] if issued is not None
                                   else now >= state['legacy_cutoff'] + ctx.config.retention_seconds * 1000)
                    # Also keep a complete window of terminal result history.
                    expired = expired and row.get('updated_at', row['created_at']) * 1000 < now - ctx.config.retention_seconds * 1000
                else:
                    born = row.get('source_time_ms', int(row['received_at'] * 1000))
                    expired = born < state['inbound_expired_before']
                    if 'source_time_ms' not in row and row.get('id_policy') != 'source_window':
                        expired = expired and now >= state['legacy_cutoff'] + ctx.config.inbound_retention_seconds * 1000
                    if expired:
                        expired = await self.inbound_settled(ctx, row)
                if expired:
                    deletes.append(key)
            if deletes and not cutoff_saved:
                await self.save(ctx, state)
                cutoff_saved = True
                meta_dirty = False
            if deletes or state[cursor] != page['next_after_key']:
                state[count] = max(0, state[count] - len(deletes))
                state[cursor] = page['next_after_key']
                await self.save(ctx, state, deletes=deletes)
                cutoff_saved = True
                meta_dirty = False
        if meta_dirty and not cutoff_saved:
            await self.save(ctx, state)
        return state

    @staticmethod
    def request_time(request_id):
        if not isinstance(request_id, str):
            return None
        match = re.fullmatch(r'v2:([0-9]{1,16}):[A-Za-z0-9_-]{8,128}', request_id)
        return int(match[1]) if match else None

    async def inbound_settled(self, ctx, row):
        try:
            receipt = await ctx.storage.receipt(row['commit_id'])
        except (IPCRejected, IPCError, ProtocolError, TimeoutError):
            return False
        if receipt is None:
            return False
        for operation_id in receipt['operation_ids']:
            try:
                result = await ctx.operations.get(operation_id)
            except IPCRejected as exc:
                if exc.code == 'not_found':
                    return False
                return False
            except (IPCError, ProtocolError, TimeoutError):
                return False
            evidence = result.get('result')
            if (result.get('state') not in ('succeeded', 'failed', 'cancelled')
                    or not isinstance(evidence, dict) or evidence.get('state') != result['state']
                    or evidence.get('partial_effects')):
                return False
        return True

    async def collect(self, ctx):
        async with self.lock:
            if not self.sealed:
                try:
                    await self.housekeeping(ctx)
                except (ServiceRejected, IPCRejected, CommitUncertain, IPCError, ProtocolError, TimeoutError, OSError) as exc:
                    ctx.log.warning('messaging housekeeping skipped: %s', exc)

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

    async def enqueue(self, ctx, state, owner, route, text, request_id, *, internal=False):
        if self.sealed:
            raise ServiceRejected('busy')
        if not isinstance(text, str) or not 1 <= len(text.encode()) <= ctx.config.max_text_bytes:
            raise ServiceRejected('invalid_argument')
        if not isinstance(request_id, str) or not 1 <= len(request_id) <= 160:
            raise ServiceRejected('invalid_argument')
        key = digest([owner, request_id])
        parameters = digest([route.id, text])
        await self.writable(ctx)
        previous = state['deliveries'].get(key) or await ctx.storage.get('msg_delivery_' + key)
        if not internal:
            issued = self.request_time(request_id)
            if issued is None:
                if not previous:
                    raise ServiceRejected('request_id_upgrade_required' if not request_id.startswith('v2:') else 'invalid_request_id')
            elif issued < state['expired_before']:
                raise ServiceRejected('request_expired')
            elif issued > state['last_seen_clock'] + ctx.config.clock_skew_seconds * 1000:
                raise ServiceRejected('request_in_future')
        if previous:
            if previous['parameters'] != parameters:
                raise ServiceRejected('conflict')
            return previous
        if state['delivery_count'] >= ctx.config.max_records:
            raise ServiceRejected('capacity_exhausted')
        record = {'delivery_id': key, 'owner': owner, 'request_id': request_id, 'parameters': parameters,
                  'route_id': route.id, 'text': text, 'state': 'queued', 'created_at': state['last_seen_clock'] / 1000, 'direction': 'outbound',
                  'id_policy': 'source_window' if internal else 'v2'}
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

    @staticmethod
    def _map_storage_error(exc):
        if isinstance(exc, CommitUncertain):
            return ServiceRejected('unknown')
        if isinstance(exc, IPCRejected):
            if exc.code == 'revision_conflict':
                return ServiceRejected('busy')
            if exc.code in ('capacity_exhausted', 'payload_too_large'):
                return ServiceRejected('capacity_exhausted')
        return ServiceRejected('unavailable')

    async def messaging(self, ctx, arguments, call_context):
        if call_context is None:
            raise ServiceRejected('permission_denied')
        if not isinstance(arguments, dict):
            raise ServiceRejected('invalid_argument')
        owner = call_context.installation_id
        action = arguments.get('action')
        visible = {r.id for r in ctx.config.routes if self.authorized(r, owner)}
        if action == 'routes':
            if set(arguments) != {'action'}:
                raise ServiceRejected('invalid_argument')
            return {'routes': [{'route_id': r.id, 'adapter': r.adapter, 'game_to_group': r.game_to_group,
                    'group_to_game': r.group_to_game, 'health': self.health.get(r.id, 'not_connected')}
                    for r in ctx.config.routes if r.id in visible]}
        if action == 'send':
            if set(arguments) != {'action', 'route_id', 'text', 'request_id'}:
                raise ServiceRejected('invalid_argument')
            route_id = arguments.get('route_id')
            text = arguments.get('text')
            request_id = arguments.get('request_id')
            if (not isinstance(route_id, str) or not route_id
                    or not isinstance(text, str) or not 1 <= len(text.encode()) <= ctx.config.max_text_bytes
                    or not isinstance(request_id, str) or not 1 <= len(request_id) <= 160):
                raise ServiceRejected('invalid_argument')
            route = self.route(ctx, route_id)
            if route.id not in visible:
                raise ServiceRejected('permission_denied')
            async with self.lock:
                try:
                    state = await self.housekeeping(ctx)
                    record = await self.enqueue(ctx, state, owner, route, text, request_id)
                    if record is state['deliveries'].get(record['delivery_id']):
                        await self.save(ctx, state)
                except (IPCRejected, CommitUncertain, IPCError, ProtocolError, TimeoutError) as exc:
                    raise self._map_storage_error(exc) from None
                return self.public(record)
        if action == 'status':
            if not set(arguments) <= {'action', 'delivery_id'}:
                raise ServiceRejected('invalid_argument')
            key = None
            if 'delivery_id' in arguments:
                key = arguments['delivery_id']
                if not isinstance(key, str) or len(key) != 64 or any(c not in '0123456789abcdef' for c in key):
                    raise ServiceRejected('invalid_argument')
            async with self.lock:
                try:
                    state = await self.state(ctx, ids=[key] if key else [], owner=owner if not key else None)
                except (IPCRejected, IPCError, ProtocolError, TimeoutError) as exc:
                    raise self._map_storage_error(exc) from None
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
                return {'deliveries': list(reversed(rows)), 'sealed': self.sealed, 'limited': len(rows) < len(records),
                        'clock_rollback': state['clock_rollback'], 'expired_before': state['expired_before']}
        if action == 'subscribe':
            if not set(arguments) <= {'action', 'after', 'limit'}:
                raise ServiceRejected('invalid_argument')
            after, limit = arguments.get('after', 0), arguments.get('limit', 100)
            if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 200:
                raise ServiceRejected('invalid_argument')
            async with self.lock:
                try:
                    state = await self.state(ctx, events=(after + 1))
                except (IPCRejected, IPCError, ProtocolError, TimeoutError) as exc:
                    raise self._map_storage_error(exc) from None
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
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        return await self.messaging(ctx, {**args, 'action': 'send'}, call)

    @service('routes', with_context=True)
    async def routes(self, ctx, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        return await self.messaging(ctx, {**args, 'action': 'routes'}, call)

    @service('status', with_context=True)
    async def status(self, ctx, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        return await self.messaging(ctx, {**args, 'action': 'status'}, call)

    @service('subscribe', with_context=True)
    async def subscribe(self, ctx, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
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

    async def _recover_dispatching_for_route(self, ctx, state, route_id):
        changed = False
        for old in list(state['deliveries'].values()):
            if old.get('route_id') == route_id and old.get('state') == 'dispatching' and old.get('delivery_id') not in self.in_flight:
                old['state'] = 'unknown'
                old['updated_at'] = time.time()
                queue = state['queues'].get(route_id, [])
                if old['delivery_id'] in queue:
                    queue.remove(old['delivery_id'])
                state['unresolved'] = max(0, state['unresolved'] - 1)
                self.emit(ctx, state, self.public(old))
                changed = True
        if changed:
            await self.save(ctx, state)
        return changed

    async def flush(self, ctx):
        # Mark each message before network I/O. Restart never replays dispatching/unknown.
        for route in ctx.config.routes:
            key = None
            text = None
            try:
                async with self.lock:
                    if self.sealed:
                        return
                    await self.writable(ctx)
                    state = await self.state(ctx, route=route.id)
                    await self._recover_dispatching_for_route(ctx, state, route.id)
                    if route.adapter == 'polaris_ws' and route.id not in self.sockets:
                        continue
                    record = next((r for r in state['deliveries'].values() if r['route_id'] == route.id and r['state'] == 'queued'), None)
                    if not record or not self.rate(route, 'outbound'):
                        continue
                    record['state'] = 'dispatching'
                    state['unresolved'] += 1
                    await self.save(ctx, state)
                    key, text = record['delivery_id'], record['text']
                    self.in_flight.add(key)
            except (ServiceRejected, IPCRejected, CommitUncertain, IPCError, ProtocolError, TimeoutError, OSError) as exc:
                ctx.log.warning('flush pre-dispatch skipped on route %s: %s', route.id, exc)
                if isinstance(exc, ServiceRejected) and exc.code in ('busy', 'unknown'):
                    return
                continue
            try:
                try:
                    if route.adapter == 'telegram':
                        response = await self.remote(ctx, route, 'sendMessage', {'chat_id': route.group_id, 'text': text})
                        result_obj = response.get('result') if isinstance(response.get('result'), dict) else {}
                        ok, remote_id = response.get('ok') is True, result_obj.get('message_id')
                    else:
                        response = await self.remote(ctx, route, 'send_group_msg', {'group_id': int(route.group_id),
                                        'message': [{'type': 'text', 'data': {'text': text}}], 'auto_escape': True})
                        data_obj = response.get('data') if isinstance(response.get('data'), dict) else {}
                        ok, remote_id = response.get('status') == 'ok' and response.get('retcode') == 0, data_obj.get('message_id')
                    result = 'delivered' if ok else 'rejected'
                except Exception:
                    result, remote_id = 'unknown', None
                try:
                    async with self.lock:
                        await self.writable(ctx)
                        state = await self.state(ctx, ids=[key], route=route.id)
                        record = state['deliveries'].get(key)
                        if record is not None:
                            queue = state['queues'].get(route.id, [])
                            if key in queue:
                                queue.remove(key)
                            if record['state'] == 'dispatching':
                                state['unresolved'] = max(0, state['unresolved'] - 1)
                            record.update(state=result, remote_id=str(remote_id) if remote_id is not None else None, updated_at=time.time())
                            if result in ('delivered', 'rejected'):
                                record.pop('text', None)
                            self.emit(ctx, state, self.public(record))
                            await self.save(ctx, state)
                        self.health[route.id] = 'connected' if result != 'unknown' else 'unavailable'
                except (ServiceRejected, IPCRejected, CommitUncertain, IPCError, ProtocolError, TimeoutError, OSError) as exc:
                    self.health[route.id] = 'unavailable'
                    ctx.log.warning('flush post-dispatch save failed on route %s: %s', route.id, exc)
            finally:
                if key is not None:
                    self.in_flight.discard(key)

    async def incoming(self, ctx, route, message_id, sender, text, *, self_message=False, external_id=None, source_time=None):
        if self_message or not route.group_to_game:
            return
        if not isinstance(text, str) or not text or len(text.encode()) > ctx.config.max_text_bytes:
            return
        if message_id is None:
            raise ServiceRejected('event_id_required')
        key = digest([route.adapter, route.id, str(message_id)])
        async with self.lock:
            if self.sealed:
                raise ServiceRejected('busy')
            await self.writable(ctx)
            state = await self.housekeeping(ctx)
            if source_time is None and route.adapter == 'polaris_ws':
                source_ms = None  # This gateway may emit valid untimed events.
            elif type(source_time) is not int or source_time <= 0:
                raise ServiceRejected('event_time_required')
            else:
                source_ms = source_time * 1000
                if source_ms < state['inbound_expired_before']:
                    raise ServiceRejected('event_expired')
                if source_ms > state['last_seen_clock'] + ctx.config.clock_skew_seconds * 1000:
                    raise ServiceRejected('event_in_future')
            # Preserve the pre-v3 route/message dedup key while its evidence exists.
            if (await ctx.storage.get('msg_seen_' + key)
                    or await ctx.storage.get('msg_seen_' + digest([route.id, str(message_id)]))):
                return
            if state['seen_count'] >= ctx.config.max_records:
                raise ServiceRejected('capacity_exhausted')
            if not self.rate(route, 'inbound'):
                raise ServiceRejected('busy')
            # Only a configured immutable command may cross the administration boundary.
            command_text = None
            rejected = False
            if text.startswith('/admin '):
                command = next((c for c in route.admin_commands if text == '/admin ' + c.name), None)
                identity = next((i for i in route.identities if external_id is not None and i.external_id == str(external_id)), None)
                rejected = True
                if command and identity:
                    try:
                        decision = await ctx.services.call('plugin.neomega.roles.check', {
                            'namespace': route.roles_namespace, 'player_id': identity.player_id,
                            'capability': command.capability, 'scope': route.id})
                        if isinstance(decision, dict) and decision.get('allowed') is True:
                            command_text, rejected = command.command, False
                    except Exception:
                        rejected = True
            sender_text = '' if sender is None else str(sender)[:80]
            formatted = '[' + route.id + '] ' + sender_text + ': ' + text
            intent = None if rejected else ctx.commands.prepare(command_text or ('tellraw @a ' + json.dumps({'rawtext': [{'text': formatted}]}, ensure_ascii=False)),
                                          idempotency_key='bridge_' + key, deadline=deadline())
            state['seen_count'] += 1
            state['seen'][key] = {'route_id': route.id, 'source': route.adapter,
                                  'received_at': state['last_seen_clock'] / 1000,
                                  'id_policy': 'source_window', 'commit_id': 'bridge_' + key}
            if source_ms is not None:
                state['seen'][key]['source_time_ms'] = source_ms
            self.emit(ctx, state, {'route_id': route.id, 'owner': 'external', 'direction': 'inbound',
                                  'message_id': str(message_id), 'sender': sender_text, 'text': text, 'state': 'permission_denied' if rejected else 'accepted'})
            await self.save(ctx, state, intent=intent)

    @staticmethod
    def _extract_onebot_text(segments, *, allow_str=False):
        if isinstance(segments, list):
            parts = []
            for p in segments:
                if not isinstance(p, dict) or p.get('type') != 'text':
                    continue
                data = p.get('data') if isinstance(p.get('data'), dict) else {}
                val = data.get('text')
                if val is not None:
                    parts.append(str(val))
            return ''.join(parts)
        if allow_str and isinstance(segments, str):
            # CQ-string mode is forwarded literally, never evaluated as CQ or a command.
            return segments
        return None

    @staticmethod
    def _extract_onebot_sender(event):
        sender = event.get('sender') if isinstance(event.get('sender'), dict) else {}
        label = sender.get('card') or sender.get('nickname') or event.get('user_id')
        return '' if label is None else label

    async def poll(self, ctx):
        for route in ctx.config.routes:
            if route.adapter != 'telegram' or not route.group_to_game or self.sealed:
                continue
            try:
                async with self.lock:
                    state = await self.state(ctx)
                    offset = state['offsets'].get(route.id, 0)
                result = await self.remote(ctx, route, 'getUpdates', {'offset': offset, 'limit': 50, 'timeout': 0,
                                                                  'allowed_updates': ['message']})
                updates = result.get('result') if isinstance(result, dict) else None
                if not isinstance(result, dict) or result.get('ok') is not True or not isinstance(updates, list):
                    self.health[route.id] = 'unavailable'
                    continue
                event_error = None
                for update in updates:
                    if not isinstance(update, dict) or type(update.get('update_id')) is not int:
                        continue
                    message = update.get('message')
                    if isinstance(message, dict):
                        chat = message.get('chat') if isinstance(message.get('chat'), dict) else {}
                        if str(chat.get('id')) == route.group_id:
                            sender = message.get('from') if isinstance(message.get('from'), dict) else {}
                            sender_name = sender.get('first_name')
                            if sender_name is None:
                                sender_name = sender.get('username')
                            if sender_name is None:
                                sender_name = sender.get('id')
                            text = message.get('text')
                            try:
                                await self.incoming(
                                    ctx, route, message.get('message_id'),
                                    '' if sender_name is None else sender_name,
                                    '' if text is None else text,
                                    self_message=sender.get('is_bot') is True,
                                    external_id=sender.get('id'),
                                    source_time=message.get('date'),
                                )
                            except ServiceRejected as exc:
                                if exc.code not in ('event_expired', 'event_time_required', 'event_in_future', 'event_id_required', 'invalid_argument'):
                                    raise
                                event_error = exc.code
                    async with self.lock:
                        await self.writable(ctx)
                        state = await self.state(ctx)
                        state['offsets'][route.id] = int(update['update_id']) + 1
                        await self.save(ctx, state)
                self.health[route.id] = event_error or 'connected'
            except ServiceRejected as exc:
                self.health[route.id] = exc.code
            except Exception:
                self.health[route.id] = 'unavailable'

    async def onebot_event(self, ctx, route, event):
        if not isinstance(event, dict):
            return
        if (event.get('post_type') != 'message' or event.get('message_type') != 'group'
                or str(event.get('group_id')) != route.group_id or event.get('message_id') is None):
            return
        text = self._extract_onebot_text(event.get('message'), allow_str=True)
        if text is None:
            return
        await self.incoming(
            ctx, route, event['message_id'], self._extract_onebot_sender(event), text,
            self_message=event.get('self_id') is not None and event.get('self_id') == event.get('user_id'),
            external_id=event.get('user_id'), source_time=event.get('time'),
        )

    async def socket_loop(self, ctx, route):
        while not self.stopping:
            connection = None
            receiver = None
            try:
                token = ctx.secrets.get(route.secret).strip() if route.secret else ''
                connection = OneBotSocket(route.endpoint, token)
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
                            except ServiceRejected as exc:
                                self.health[route.id] = exc.code
                            except (IPCRejected, CommitUncertain, IPCError, ProtocolError, TimeoutError, ValueError, TypeError) as exc:
                                self.health[route.id] = 'unavailable'
                                ctx.log.warning('ws event handling error on route %s: %s', route.id, exc)
                    finally:
                        next_event.cancel()
                        await asyncio.gather(next_event, return_exceptions=True)
                await receiver
            except asyncio.CancelledError:
                raise
            except (SecretError, Exception):
                self.health[route.id] = 'unavailable'
            finally:
                self.sockets.pop(route.id, None)
                if receiver:
                    receiver.cancel()
                    await asyncio.gather(receiver, return_exceptions=True)
                if connection is not None:
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
                quarantined_error = None
                for event in result.get('events') or ():
                    if (not isinstance(event, dict) or not isinstance(event.get('payload'), str)
                            or event.get('connection_id') is None or event.get('source_sequence') is None):
                        quarantined_error = 'cbbridge_invalid_argument'
                        continue
                    try:
                        async with self.lock:
                            await self.writable(ctx)
                            state = await self.housekeeping(ctx)
                            # Untrusted system text is never used as an administrative identity.
                            await self.enqueue(ctx, state, 'game', route, '[Command block] ' + event['payload'],
                                               digest(['cb', route.id, event['connection_id'], event['source_sequence']]), internal=True)
                            await self.save(ctx, state)
                    except ServiceRejected as exc:
                        if exc.code == 'invalid_argument':
                            quarantined_error = 'cbbridge_invalid_argument'
                            ctx.log.warning('quarantining invalid cbbridge event on route %s', route.id)
                            continue
                        raise
                subscription['cursor'] = result['cursor']
                if result.get('gap'):
                    self.health[route.id] = 'cbbridge_gap'
                elif quarantined_error:
                    self.health[route.id] = quarantined_error
            except ServiceRejected as exc:
                if exc.code in ('not_found', 'unavailable'):
                    self.cb_routes.pop(route.id, None)
                self.health[route.id] = 'cbbridge_' + exc.code
            except (CommitUncertain, IPCRejected, IPCError, ProtocolError, TimeoutError, Exception):
                self.health[route.id] = 'cbbridge_unavailable'

    async def webhook(self, ctx, reader, writer):
        self.webhooks += 1
        code = 400
        error = None
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
                if (isinstance(event, dict) and event.get('post_type') == 'message'
                        and event.get('message_type') == 'group' and str(event.get('group_id')) == route.group_id
                        and event.get('message_id') is not None):
                    # Require structured segments: never interpret CQ escape strings.
                    text = self._extract_onebot_text(event.get('message'), allow_str=False)
                    if text is not None:
                        await self.incoming(
                            ctx, route, event['message_id'], self._extract_onebot_sender(event), text,
                            self_message=event.get('self_id') is not None and event.get('self_id') == event.get('user_id'),
                            external_id=event.get('user_id'), source_time=event.get('time'),
                        )
                code = 200
        except ServiceRejected as exc:
            if exc.code == 'not_found':
                code = 404
            elif exc.code in ('event_expired', 'event_time_required', 'event_in_future', 'event_id_required', 'invalid_argument'):
                code = 422
            else:
                code = 503
            error = exc.code
        except (SecretError, CommitUncertain, IPCRejected, IPCError, ProtocolError, TimeoutError):
            code = 503
            error = 'unavailable'
        except Exception:
            code = 400
        finally:
            self.webhooks -= 1
            body = json.dumps({'error': error} if error else {}).encode()
            try:
                writer.write(f'HTTP/1.1 {code} Response\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n'.encode() + body)
                await writer.drain()
            except (ConnectionError, OSError, RuntimeError):
                pass
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), 5)
            except (ConnectionError, OSError, asyncio.TimeoutError, RuntimeError):
                pass

    async def _start_webhook_server(self, ctx):
        if not ctx.config.webhook_port or self.server is not None or self.stopping or self.sealed:
            return

        def accept(reader, writer):
            if self.stopping or self.sealed or self.webhooks >= 24:
                writer.close()
                return
            try:
                ctx.spawn(self.webhook(ctx, reader, writer), name='bridge-webhook')
            except RuntimeError:
                writer.close()

        self.server = await asyncio.start_server(accept, ctx.config.webhook_host, ctx.config.webhook_port, limit=8192)

    async def _stop_webhook_server(self):
        server = self.server
        self.server = None
        if server is not None:
            server.close()
            try:
                await asyncio.wait_for(server.wait_closed(), 5)
            except (OSError, asyncio.TimeoutError, RuntimeError):
                pass

    async def _webhook_shutdown_guard(self, ctx):
        try:
            await asyncio.Future()
        finally:
            self.stopping = True
            if self.server is not None:
                self.server.close()

    async def on_start(self, ctx):
        self.stopping = False
        self.pending_path = ctx.data_dir / 'messaging-pending.json'
        if self.pending_path.exists():
            self.pending = json.loads(self.pending_path.read_text())['commit_id']
        # Uncertain transactions are reconciled only by their original Host receipt.
        async with self.lock:
            await self.state(ctx)
            try:
                await self.housekeeping(ctx)
            except ServiceRejected as exc:
                if exc.code != 'unknown':
                    raise
                # Keep read-only status available while the original receipt is unresolved.
        if ctx.config.webhook_port:
            await self._start_webhook_server(ctx)
            ctx.spawn(self._webhook_shutdown_guard(ctx), name='bridge-webhook-guard')
        ctx.every(ctx.config.poll_seconds, lambda: self.collect(ctx), name='bridge-gc')
        ctx.every(ctx.config.poll_seconds, lambda: self.flush(ctx), name='bridge-outbox')
        ctx.every(ctx.config.poll_seconds, lambda: self.poll(ctx), name='bridge-inbound')
        ctx.every(ctx.config.poll_seconds, lambda: self.poll_cbbridge(ctx), name='bridge-cbbridge')
        for route in ctx.config.routes:
            if route.adapter == 'polaris_ws':
                ctx.spawn(self.socket_loop(ctx, route), name='bridge-ws-' + route.id)

    async def _safe_ack(self, ctx, event):
        if event.ack_handled:
            return
        try:
            await event.ack()
        except (CommitUncertain, IPCRejected, IPCError, ProtocolError, TimeoutError) as exc:
            event.defer_ack()
            ctx.log.warning('failed to ack event cleanly, deferring: %s', exc)

    async def on_event(self, ctx, event):
        if event.kind != 'chat.received':
            await self._safe_ack(ctx, event)
            return
        payload = event.payload.get('payload')
        if not isinstance(payload, dict):
            await self._safe_ack(ctx, event)
            return
        # Only actual player chats with a roster-confirmed XUID are bridge sources.
        name, xuid, text = payload.get('player'), payload.get('xuid'), payload.get('message', '')
        if not isinstance(name, str) or not name or not isinstance(xuid, str) or not xuid or not isinstance(text, str):
            await self._safe_ack(ctx, event)
            return
        targets = [route for route in ctx.config.routes if route.game_to_group and text.startswith(route.prefix)]
        if not targets:
            await self._safe_ack(ctx, event)
            return
        try:
            players = await ctx.players()
        except (IPCRejected, IPCError, ProtocolError, TimeoutError, ValueError) as exc:
            ctx.log.warning('skipping chat bridge event while player roster unavailable: %s', exc)
            await self._safe_ack(ctx, event)
            return
        if not any(p.get('name') == name and p.get('xuid') == xuid for p in players):
            await self._safe_ack(ctx, event)
            return
        token = event.payload.get('delivery_token')
        if not isinstance(token, str) or not token:
            await self._safe_ack(ctx, event)
            return
        async with self.lock:
            try:
                await self.writable(ctx)
                state = await self.housekeeping(ctx)
                enqueued = False
                for route in targets:
                    try:
                        await self.enqueue(ctx, state, 'game', route, '<' + name + '> ' + text,
                                           digest([token, route.id]), internal=True)
                        enqueued = True
                    except ServiceRejected as exc:
                        if exc.code in ('busy', 'capacity_exhausted', 'unknown'):
                            raise
                        continue
                if enqueued:
                    await self.save(ctx, state, event=event)
                    return
            except ServiceRejected as exc:
                if exc.code == 'unknown':
                    event.defer_ack()
                    ctx.log.warning('deferring chat event ack while storage state is unknown')
                    return
                if exc.code == 'busy' and self.sealed:
                    event.defer_ack()
                    return
                ctx.log.warning('dropping chat bridge event (%s)', exc.code)
            except (CommitUncertain, IPCRejected, IPCError, ProtocolError, TimeoutError) as exc:
                if isinstance(exc, CommitUncertain) or self.pending is not None:
                    event.defer_ack()
                    ctx.log.warning('deferring chat event ack after uncertain commit: %s', exc)
                    return
                if isinstance(exc, IPCRejected) and exc.code == 'invalid_delivery':
                    event.defer_ack()
                    return
                ctx.log.warning('chat bridge event storage error (%s); acknowledging cleanly', exc)
        await self._safe_ack(ctx, event)

    async def _recover_unresolved(self, ctx, state):
        if not state['unresolved'] or self.active_http or self.in_flight:
            return
        after_key = None
        recovered = False
        while True:
            page = await ctx.storage.scan_keys(prefix='msg_delivery_', after_key=after_key, limit=64)
            for key in page['keys']:
                row = await ctx.storage.get(key)
                if not isinstance(row, dict) or row.get('state') != 'dispatching':
                    continue
                delivery_id = row.get('delivery_id')
                if delivery_id in self.in_flight:
                    continue
                route_id = row.get('route_id')
                row['state'] = 'unknown'
                row['updated_at'] = time.time()
                state['deliveries'][delivery_id] = row
                if isinstance(route_id, str) and route_id:
                    queue = state['queues'].setdefault(route_id, await ctx.storage.get('msg_queue_' + route_id, []))
                    if delivery_id in queue:
                        queue.remove(delivery_id)
                self.emit(ctx, state, self.public(row))
                recovered = True
            after_key = page['next_after_key']
            if after_key is None:
                break
        if state['unresolved'] or recovered:
            state['unresolved'] = 0
            await self.save(ctx, state)

    async def on_maintenance(self, ctx, request):
        server_to_stop = None
        restart_server = False
        async with self.lock:
            if request['operation'] == 'release':
                if self.sealed not in (None, request['token']):
                    return {'status': 'busy', 'token': request['token']}
                self.sealed = None
                restart_server = bool(ctx.config.webhook_port and not self.stopping and self.server is None)
            else:
                if self.active_http or self.webhooks or self.in_flight:
                    return {'status': 'busy', 'token': request['token']}
                if self.pending:
                    try:
                        if await ctx.storage.receipt(self.pending) is None:
                            return {'status': 'busy', 'token': request['token']}
                    except (IPCRejected, IPCError, ProtocolError, TimeoutError):
                        return {'status': 'busy', 'token': request['token']}
                    self.clear_pending()
                if self.sealed not in (None, request['token']):
                    return {'status': 'busy', 'token': request['token']}
                try:
                    state = await self.state(ctx)
                    await self._recover_unresolved(ctx, state)
                except (ServiceRejected, IPCRejected, CommitUncertain, IPCError, ProtocolError, TimeoutError):
                    return {'status': 'busy', 'token': request['token']}
                if state['unresolved']:
                    return {'status': 'busy', 'token': request['token']}
                self.sealed = request['token']
                server_to_stop = self.server
                self.server = None
        if server_to_stop is not None:
            server_to_stop.close()
            try:
                await asyncio.wait_for(server_to_stop.wait_closed(), 5)
            except (OSError, asyncio.TimeoutError, RuntimeError):
                pass
        if restart_server:
            await self._start_webhook_server(ctx)
        if request['operation'] == 'release':
            return {'status': 'released', 'token': request['token']}
        return {'status': 'sealed', 'token': request['token']}

    async def on_stop(self, ctx):
        self.stopping = True
        await self._stop_webhook_server()
        for connection in list(self.sockets.values()):
            await connection.close()
        for subscription in self.cb_routes.values():
            try:
                await ctx.services.call('plugin.neomega.cbbridge.unregister', {'route_id': subscription['route_id']})
            except Exception:
                pass


if __name__ == '__main__':
    Messaging().run()
