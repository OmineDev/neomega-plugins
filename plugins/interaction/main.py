"""Shared menu sessions. Consumer polling is intentional: no delegated business writes."""
import asyncio
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import shlex
import time
import uuid

from neomega_runtime import Plugin, service, ServiceRejected, PlayerNotFound
from neomega_common import escape_text


@dataclass(frozen=True)
class Settings:
    max_sessions: int = 128
    max_registrations: int = 256
    session_ttl: int = 120
    result_ttl: int = 600
    page_size: int = 4
    snowball_adapter: bool = False

    def __post_init__(self):
        if not 1 <= self.max_sessions <= 4096 or not 1 <= self.max_registrations <= 4096:
            raise ValueError('capacity must be 1..4096')
        if not 10 <= self.session_ttl <= 600 or not 30 <= self.result_ttl <= 3600 or not 1 <= self.page_size <= 10:
            raise ValueError('invalid TTL or page size')


class Interaction(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.registrations = {}
        self.sessions = {}
        self.requests = {}
        self.deliveries = {}
        self.sealed = None

    @staticmethod
    def owner(call):
        if call is None:
            raise ServiceRejected('permission_denied')
        return (call.installation_id, call.generation)

    def prune(self, ctx):
        now = time.monotonic()
        for key, row in list(self.registrations.items()):
            if row['expires'] <= now:
                del self.registrations[key]
        for key, row in list(self.sessions.items()):
            if row['state'] == 'active' and row['expires'] <= now:
                row.update(state='expired', finished=now)
            if row['state'] != 'active' and now - row['finished'] >= ctx.config.result_ttl:
                del self.sessions[key]
        self.deliveries = {key: end for key, end in self.deliveries.items() if end > now}
        for key, row in list(self.requests.items()):
            if now >= row['expires']:
                del self.requests[key]

    def replay(self, owner, method, args):
        request = args.get('request_id')
        if not isinstance(request, str) or not 1 <= len(request) <= 128:
            raise ServiceRejected('invalid_argument')
        key = (owner, method, request)
        digest = hashlib.sha256(json.dumps(args, sort_keys=True, allow_nan=False).encode()).hexdigest()
        old = self.requests.get(key)
        if old and old['digest'] != digest:
            raise ServiceRejected('conflict')
        if old:
            return key, digest, deepcopy(old['result'])
        if len(self.requests) >= 8192:
            raise ServiceRejected('busy')
        return key, digest, None

    def remember(self, key, digest, result, ctx):
        self.requests[key] = dict(digest=digest, result=deepcopy(result), expires=time.monotonic() + ctx.config.result_ttl)
        return result

    @staticmethod
    def validate_menu(menu):
        if not isinstance(menu, dict) or not isinstance(menu.get('title'), str) or not 1 <= len(menu['title']) <= 100:
            raise ServiceRejected('invalid_argument')
        steps = menu.get('steps')
        if not isinstance(steps, list) or not 1 <= len(steps) <= 32:
            raise ServiceRejected('invalid_argument')
        ids = set()
        for step in steps:
            if not isinstance(step, dict) or step.get('kind') not in ('select', 'confirm', 'text', 'integer'):
                raise ServiceRejected('invalid_argument')
            if not isinstance(step.get('id'), str) or not step['id'] or step['id'] in ids or not isinstance(step.get('prompt'), str):
                raise ServiceRejected('invalid_argument')
            ids.add(step['id'])
            if len(step['prompt']) > 500:
                raise ServiceRejected('invalid_argument')
            if step['kind'] == 'select':
                options = step.get('options')
                if not isinstance(options, list) or not 1 <= len(options) <= 256:
                    raise ServiceRejected('invalid_argument')
                if any(not isinstance(x, dict) or not isinstance(x.get('label'), str) or len(x['label']) > 100 or 'value' not in x for x in options):
                    raise ServiceRejected('invalid_argument')
            if step['kind'] == 'integer' and (type(step.get('min', -2147483648)) is not int or type(step.get('max', 2147483647)) is not int or step.get('min', -2147483648) > step.get('max', 2147483647)):
                raise ServiceRejected('invalid_argument')
        permission = menu.get('permission')
        if permission is not None and (not isinstance(permission, dict) or not isinstance(permission.get('capability'), str)):
            raise ServiceRejected('invalid_argument')

    async def allowed(self, ctx, menu, player):
        permission = menu.get('permission')
        if permission:
            try:
                identity = await ctx.services.call('plugin.neomega.players.resolve', {'uuid': player['uuid']})
                result = await ctx.services.call('plugin.neomega.roles.check', {
                    'player_id': identity['player']['player_id'], 'capability': permission['capability'],
                    'scope': permission.get('scope', 'global'), 'namespace': permission['namespace']})
            except Exception as exc:
                raise ServiceRejected('dependency_unavailable') from exc
            if result.get('allowed') is not True:
                raise ServiceRejected('permission_denied')

    def public(self, session):
        return {key: deepcopy(session[key]) for key in ('session_id', 'menu_id', 'player', 'state', 'step', 'page', 'answers', 'revision', 'source', 'answer_sources', 'args', 'notification')}

    def lookup(self, args, owner):
        row = self.sessions.get(args.get('session_id'))
        if not row or row['owner'] != owner:
            raise ServiceRejected('not_found')
        return row

    @service('register', with_context=True)
    async def register(self, ctx, args, call):
        async with self.lock:
            self.prune(ctx)
            owner = self.owner(call)
            key, digest, previous = self.replay(owner, 'register', args)
            if previous is not None:
                return previous
            if self.sealed:
                raise ServiceRejected('busy')
            menu_id, menu = args.get('menu_id'), deepcopy(args.get('menu'))
            if not isinstance(menu_id, str) or not 1 <= len(menu_id) <= 100:
                raise ServiceRejected('invalid_argument')
            self.validate_menu(menu)
            if menu.get('permission'):
                menu['permission'].setdefault('namespace', owner[0])
            ttl = args.get('ttl_seconds', 300)
            if type(ttl) is not int or not 10 <= ttl <= 3600:
                raise ServiceRejected('invalid_argument')
            names = args.get('commands', [])
            if not isinstance(names, list) or len(names) > 16 or any(not isinstance(n, str) or not n.replace('_', '').replace('-', '').isalnum() or len(n) > 64 or n in ('menu', 'next', 'prev', 'cancel', 'confirm') for n in names) or len(names) != len(set(names)):
                raise ServiceRejected('invalid_argument')
            argument_specs = args.get('args', [])
            if not isinstance(argument_specs, list) or len(argument_specs) > 16 or any(not isinstance(a, dict) or not isinstance(a.get('name'), str) or a.get('type') not in ('text', 'integer', 'number') for a in argument_specs):
                raise ServiceRejected('invalid_argument')
            identity = (owner, menu_id)
            if identity not in self.registrations and len(self.registrations) >= ctx.config.max_registrations:
                raise ServiceRejected('busy')
            for other_key, other in self.registrations.items():
                if other_key != identity and set(names) & set(other['commands']):
                    raise ServiceRejected('conflict')
            self.registrations[identity] = dict(menu_id=menu_id, menu=menu, commands=names, args=argument_specs, owner=owner, expires=time.monotonic() + ttl)
            return self.remember(key, digest, {'menu_id': menu_id, 'ttl_seconds': ttl}, ctx)

    @service('unregister', with_context=True)
    async def unregister(self, ctx, args, call):
        async with self.lock:
            owner = self.owner(call)
            self.registrations.pop((owner, args.get('menu_id')), None)
            for row in self.sessions.values():
                if row['owner'] == owner and row['menu_id'] == args.get('menu_id') and row['state'] == 'active':
                    row.update(state='cancelled', finished=time.monotonic(), revision=row['revision'] + 1)
            return {'removed': True}

    async def create_session(self, ctx, registration, player, arguments=None):
        self.prune(ctx)
        if self.sealed:
            raise ServiceRejected('busy')
        if len(self.sessions) >= ctx.config.max_sessions:
            raise ServiceRejected('busy')
        if not player.get('uuid'):
            raise ServiceRejected('unsupported')
        if any(s['state'] == 'active' and s['player']['uuid'] == player['uuid'] for s in self.sessions.values()):
            raise ServiceRejected('busy')
        await self.allowed(ctx, registration['menu'], player)
        row = dict(session_id=uuid.uuid4().hex, menu_id=registration['menu_id'], owner=registration['owner'], menu=deepcopy(registration['menu']), player=player, state='active', step=0, page=0, answers={}, answer_sources={}, revision=1, source='pending', args=arguments or {}, expires=time.monotonic() + ctx.config.session_ttl, notification={'state': 'pending'}, nonce=None)
        self.sessions[row['session_id']] = row
        return row

    @service('open', with_context=True)
    async def open(self, ctx, args, call):
        async with self.lock:
            owner = self.owner(call)
            self.prune(ctx)
            key, digest, previous = self.replay(owner, 'open', args)
            if previous is not None:
                return previous
            registration = self.registrations.get((owner, args.get('menu_id')))
            if registration is None:
                raise ServiceRejected('not_found')
            query = args.get('player')
            if not isinstance(query, dict) or not query:
                raise ServiceRejected('invalid_argument')
            player = await ctx.resolve_player(**query)
            row = await self.create_session(ctx, registration, player)
            self.remember(key, digest, self.public(row), ctx)
            await self.show(ctx, row)
            return self.remember(key, digest, self.public(row), ctx)

    async def tell(self, ctx, player, text):
        key = uuid.uuid4().hex
        plan = ctx.notifications.prepare(player['name'], text,
            idempotency_key=key, deadline=(datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat())
        tx = await ctx.storage.transaction(commit_id=key)
        for part in plan.parts:
            tx.action(part.intent)
        return await tx.save()

    async def show(self, ctx, row):
        step = row['menu']['steps'][row['step']]
        lines = [escape_text(row['menu']['title']), escape_text(step['prompt'])]
        if step['kind'] == 'select':
            start = row['page'] * ctx.config.page_size
            lines.extend(f'{i + 1}. {escape_text(opt["label"])}' for i, opt in enumerate(step['options'][start:start + ctx.config.page_size]))
            lines.append(f'第 {row["page"] + 1}/{(len(step["options"]) - 1) // ctx.config.page_size + 1} 页；!next / !prev 翻页')
        elif step['kind'] == 'confirm':
            lines.append('输入 yes 确认，no 否定')
        lines.append('输入 !cancel 取消')
        try:
            receipt = await self.tell(ctx, row['player'], '\n'.join(lines))
            row['notification'] = {'state': 'accepted', 'receipt': receipt}
            # Shared channel ownership remains in display, and expires without a keepalive.
            channel = await ctx.services.call('plugin.neomega.display.channel_acquire', {
                'request_id': uuid.uuid4().hex, 'player': row['player']['name'], 'channel': 'actionbar',
                'text': escape_text(step['prompt'])[:100], 'priority': 30, 'ttl_seconds': 5})
            row['notification']['display'] = channel
        except Exception as exc:
            # The notification may already be accepted. Never resend automatically.
            row['notification'] = {'state': 'unknown', 'error': type(exc).__name__, 'call_id': getattr(exc, 'call_id', None)}

    async def input(self, ctx, row, value, source):
        if row['state'] != 'active':
            raise ServiceRejected('conflict')
        await self.allowed(ctx, row['menu'], row['player'])
        step = row['menu']['steps'][row['step']]
        if value == '!cancel':
            row.update(state='cancelled', finished=time.monotonic(), revision=row['revision'] + 1)
            return
        if value in ('!next', '!prev') and step['kind'] == 'select':
            pages = (len(step['options']) - 1) // ctx.config.page_size + 1
            row['page'] = (row['page'] + (1 if value == '!next' else -1)) % pages
            row['nonce'] = None
            row['revision'] += 1
            await self.show(ctx, row)
            return
        if step['kind'] == 'select':
            if not str(value).isdigit():
                raise ServiceRejected('invalid_argument')
            index = row['page'] * ctx.config.page_size + int(value) - 1
            if not 1 <= int(value) <= ctx.config.page_size or index >= len(step['options']):
                raise ServiceRejected('invalid_argument')
            parsed = deepcopy(step['options'][index]['value'])
        elif step['kind'] == 'confirm':
            if str(value).lower() not in ('yes', 'no', '是', '否'):
                raise ServiceRejected('invalid_argument')
            parsed = str(value).lower() in ('yes', '是')
        elif step['kind'] == 'integer':
            try:
                parsed = int(value)
            except (ValueError, TypeError):
                raise ServiceRejected('invalid_argument') from None
            if not step.get('min', -2147483648) <= parsed <= step.get('max', 2147483647):
                raise ServiceRejected('invalid_argument')
        else:
            if not isinstance(value, str) or not 1 <= len(value) <= 1024:
                raise ServiceRejected('invalid_argument')
            parsed = value
        row['answers'][step['id']] = parsed
        row['answer_sources'][step['id']] = source
        sources = set(row['answer_sources'].values())
        source = source if len(sources) == 1 else 'mixed'
        row.update(step=row['step'] + 1, page=0, nonce=None, source=source, revision=row['revision'] + 1)
        if row['step'] == len(row['menu']['steps']):
            row.update(state='completed', finished=time.monotonic())
        else:
            await self.show(ctx, row)

    @service('respond', with_context=True)
    async def respond(self, ctx, args, call):
        async with self.lock:
            self.prune(ctx)
            owner = self.owner(call)
            key, digest, previous = self.replay(owner, 'respond', args)
            if previous is not None:
                return previous
            row = self.lookup(args, owner)
            if args.get('expected_revision') != row['revision']:
                raise ServiceRejected('conflict')
            # A provider call does not prove player input: the result labels this explicitly.
            await self.input(ctx, row, args.get('value'), 'consumer')
            return self.remember(key, digest, self.public(row), ctx)

    @service('cancel', with_context=True)
    async def cancel(self, ctx, args, call):
        async with self.lock:
            row = self.lookup(args, self.owner(call))
            if row['state'] == 'active':
                row.update(state='cancelled', finished=time.monotonic(), revision=row['revision'] + 1)
            return self.public(row)

    @service('status', with_context=True)
    async def status(self, ctx, args, call):
        async with self.lock:
            self.prune(ctx)
            owner = self.owner(call)
            if 'session_id' in args:
                return self.public(self.lookup(args, owner))
            return {'sessions': [self.public(row) for row in self.sessions.values() if row['owner'] == owner]}

    async def on_start(self, ctx):
        if ctx.config.snowball_adapter:
            from neomega_runtime.protocol import packets
            async def snowball_packets():
                async with ctx.packets.typed.subscribe([packets.Text]) as stream:
                    async for event in stream:
                        message = event.model.Message
                        if message.startswith('{'):
                            try:
                                raw = json.loads(message)
                            except ValueError:
                                continue
                            parts = raw.get('rawtext', []) if isinstance(raw, dict) else []
                            if not isinstance(parts, list):
                                continue
                            message = ''.join(part.get('text', '') for part in parts if isinstance(part, dict) and isinstance(part.get('text', ''), str))
                        if message.startswith('NEO_MENU_SNOW '):
                            async with self.lock:
                                await self.chat(ctx, {'message': message})
            ctx.spawn(snowball_packets(), name='snowball-input-adapter')
        async def sweep():
            async with self.lock:
                self.prune(ctx)
                roster = await ctx.players()
                active = {(p['uuid'], p['xuid']) for p in roster}
                for row in self.sessions.values():
                    if row['state'] == 'active' and (row['player']['uuid'], row['player']['xuid']) not in active:
                        row.update(state='disconnected', finished=time.monotonic())
        ctx.every(2, sweep, name='menu-expiration')

    async def on_event(self, ctx, event):
        async with self.lock:
            self.prune(ctx)
            token = event.payload['delivery_token']
            if token in self.deliveries:
                await event.ack()
                return
            if len(self.deliveries) >= 8192:
                self.deliveries.pop(next(iter(self.deliveries)))
            body = event.payload.get('payload', {})
            try:
                if event.kind == 'player.list_removed':
                    for row in self.sessions.values():
                        if row['state'] == 'active' and row['player']['uuid'] in (body if isinstance(body, list) else [body.get('uuid')]):
                            row.update(state='disconnected', finished=time.monotonic())
                elif event.kind == 'chat.received':
                    await self.chat(ctx, body)
            except (ServiceRejected, ValueError, PlayerNotFound) as exc:
                # Invalid input stays in the current step. No event callback escapes into worker failure.
                ctx.log.info('menu input refused: %s', str(exc))
                if body.get('player'):
                    try:
                        player = await ctx.resolve_player(name=body['player'], xuid=body.get('xuid') or None)
                        await self.tell(ctx, player, '输入未被接受：' + str(exc) + '。请按菜单提示重试。')
                    except PlayerNotFound:
                        pass
            self.deliveries[token] = time.monotonic() + ctx.config.result_ttl
            await event.ack()

    async def chat(self, ctx, body):
        message = body.get('message', '')
        if not isinstance(message, str):
            return
        # Command-block output has no trusted player actor. It can only stage a preview.
        if ctx.config.snowball_adapter and message.startswith('NEO_MENU_SNOW '):
            parts = message.split(' ', 2)
            if len(parts) != 3 or parts[1] not in ('1', '2', '3', '4'):
                return
            for row in self.sessions.values():
                if row['state'] == 'active' and row['player']['name'] == parts[2]:
                    step = row['menu']['steps'][row['step']]
                    if step['kind'] != 'select':
                        return
                    if row.get('preview_after', 0) > time.monotonic():
                        return
                    row['preview_after'] = time.monotonic() + 2
                    token = uuid.uuid4().hex[:8]
                    row['nonce'] = (token, parts[1], row['revision'], time.monotonic() + 15)
                    await self.tell(ctx, row['player'], f'雪球选择 {parts[1]}；输入 !confirm {token} 确认。')
                    return
            return
        if not body.get('player'):
            return
        player = await ctx.resolve_player(name=body['player'], xuid=body.get('xuid') or None)
        row = next((r for r in self.sessions.values() if r['state'] == 'active' and r['player'] == player), None)
        if row:
            if message.startswith('!confirm '):
                nonce = row['nonce']
                if nonce is None or message[9:] != nonce[0] or nonce[2] != row['revision'] or time.monotonic() > nonce[3]:
                    raise ServiceRejected('stale')
                await self.input(ctx, row, nonce[1], 'snowball_confirmed_chat')
            else:
                await self.input(ctx, row, message, 'chat')
            return
        if message == '!menu':
            available = []
            for registration in self.registrations.values():
                try:
                    await self.allowed(ctx, registration['menu'], player)
                except Exception:
                    continue
                available.extend('!' + n + ' — ' + registration['menu']['title'] for n in registration['commands'][:1])
            await self.tell(ctx, player, '\n'.join(available) or '暂无可用菜单')
        elif message.startswith('!'):
            parts = shlex.split(message[1:])
            if not parts:
                return
            registration = next((r for r in self.registrations.values() if parts[0] in r['commands']), None)
            if registration:
                specs = registration['args']
                if len(parts) - 1 != len(specs):
                    raise ServiceRejected('invalid_argument')
                arguments = {}
                for spec, value in zip(specs, parts[1:]):
                    parsed = {'text': str, 'integer': int, 'number': float}[spec['type']](value)
                    if isinstance(parsed, float) and not math.isfinite(parsed):
                        raise ServiceRejected('invalid_argument')
                    arguments[spec['name']] = parsed
                row = await self.create_session(ctx, registration, player, arguments)
                await self.show(ctx, row)

    async def on_stop(self, ctx):
        self.registrations.clear()
        self.sessions.clear()
        self.requests.clear()
        self.deliveries.clear()

    async def on_maintenance(self, ctx, request):
        async with self.lock:
            if self.sealed not in (None, request['token']):
                return {'status': 'busy', 'token': request['token']}
            if request['operation'] == 'release':
                self.sealed = None
                return {'status': 'released', 'token': request['token']}
            if any(row['state'] == 'active' for row in self.sessions.values()):
                return {'status': 'busy', 'token': request['token']}
            self.sealed = request['token']
            return {'status': 'sealed', 'token': request['token']}


if __name__ == '__main__':
    Interaction().run()
