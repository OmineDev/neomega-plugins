"""Drive neomega.messaging through real SDK storage, Worker/Plugin.serve(), and edge-case tests."""
import asyncio
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import logging
from pathlib import Path
import socket
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from neomega_runtime import Event, PluginContext, ProtocolError, ServiceRejected
from neomega_runtime.config import load_config
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config_schema import check_config_schema, validate_config
from neomega_runtime.managed import IPCRejected, ManagedPeer
from neomega_runtime.secrets import SecretError
from neomega_runtime.services import ServiceCallContext
from neomega_runtime.storage import CommitUncertain

import example
from main import Messaging, Route, Settings, digest

PLUGIN_DIR = Path(__file__).resolve().parents[1]


def make_call_context(installation_id='consumer-installation', call_id='c1'):
    now = datetime.now(timezone.utc)
    return ServiceCallContext(
        installation_id=installation_id,
        generation=1,
        call_id=call_id,
        accepted_at=now,
        deadline=now + timedelta(seconds=10),
    )


def make_v2_request_id(suffix='abcdefgh'):
    return f'v2:{int(time.time() * 1000)}:{suffix}'


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class FakeHost:
    """In-memory Host supporting state.get, state.scan, state.commit, events.commit, state.receipt, operations.get."""

    def __init__(self):
        self.values = {}
        self.revision = 0
        self.commits = []
        self.receipts = {}
        self.operations = {}
        self.deliveries = set()
        self.next_commit_error = None
        self.commit_uncertain_persists = False

    async def call(self, method, payload, timeout=None):
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        if method == 'state.scan':
            prefix = payload.get('prefix', '')
            after_key = payload.get('after_key', '')
            limit = payload.get('limit', 256)
            matching = sorted(k for k in self.values if k.startswith(prefix) and k > after_key)
            keys = matching[:limit]
            next_after = keys[-1] if len(matching) > limit else None
            return {'revision': self.revision, 'keys': keys, 'next_after_key': next_after}
        if method == 'state.receipt':
            cid = payload['commit_id']
            row = self.receipts.get(cid)
            if row is None:
                return {'commit_id': cid, 'found': False}
            return {'commit_id': cid, 'found': True, **row}
        if method == 'operations.get':
            op_id = payload['operation_id']
            if op_id not in self.operations:
                raise IPCRejected('not_found', 'missing operation')
            return copy.deepcopy(self.operations[op_id])
        if method in ('state.commit', 'events.commit'):
            if self.next_commit_error is not None:
                err = self.next_commit_error
                self.next_commit_error = None
                if self.commit_uncertain_persists:
                    self._apply_commit(method, payload)
                if isinstance(err, dict):
                    return err
                raise err
            return self._apply_commit(method, payload)
        raise AssertionError(f'unexpected host call: {method}')

    def _apply_commit(self, method, payload):
        if payload['expected_state_revision'] != self.revision:
            raise IPCRejected('revision_conflict', 'stale revision')
        if method == 'events.commit':
            dkey = (payload['subscription_id'], payload['event_id'], payload['delivery_token'])
            if dkey in self.deliveries:
                raise IPCRejected('invalid_delivery', 'already acked')
            self.deliveries.add(dkey)
        for mutation in payload['mutations']:
            if mutation.get('delete'):
                self.values.pop(mutation['key'], None)
            else:
                self.values[mutation['key']] = copy.deepcopy(mutation['value'])
        self.revision += 1
        self.commits.append(copy.deepcopy(payload))
        op_ids = [f'op_{self.revision}_{i}' for i in range(len(payload['actions']))]
        for op_id in op_ids:
            self.operations[op_id] = {'operation_id': op_id, 'state': 'succeeded', 'result': {'state': 'succeeded'}}
        receipt = {'commit_id': payload['commit_id'], 'revision': self.revision, 'duplicate': False, 'operation_ids': op_ids}
        self.receipts[payload['commit_id']] = {'revision': self.revision, 'operation_ids': op_ids}
        return receipt


class ServeWriter:
    def __init__(self):
        self.requests = asyncio.Queue()
        self.responses = asyncio.Queue()
        self.closed = False

    def write(self, data):
        for line in data.splitlines():
            if not line:
                continue
            frame = json.loads(line)
            if frame.get('type') == 'request':
                self.requests.put_nowait(frame)
            else:
                self.responses.put_nowait(frame)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


class MetadataAndSchemaTests(unittest.TestCase):
    def test_manifest_and_release_metadata(self):
        manifest = json.loads((PLUGIN_DIR / 'manifest.json').read_text())
        release = json.loads((PLUGIN_DIR / 'release.json').read_text())
        self.assertEqual(manifest['version'], '1.1.1')
        self.assertEqual(release['kind'], 'app')
        for export in manifest['exports']:
            self.assertEqual(export['input_schema']['additionalProperties'], False)
            self.assertEqual(export['output_schema']['type'], 'object')

    def test_schema_secrets_bounds_and_example_config(self):
        schema = json.loads((PLUGIN_DIR / 'config.schema.json').read_text())
        example_cfg = json.loads((PLUGIN_DIR / 'config.example.json').read_text())
        check_config_schema(schema)
        validated = validate_config(schema, example_cfg)
        self.assertEqual(validated['routes'][0]['id'], 'group')
        loaded = load_config(Settings, PLUGIN_DIR / 'config.example.json')
        self.assertEqual(len(loaded.routes), 1)
        self.assertEqual(loaded.routes[0].webhook_secret, 'secret:bridge-webhook')

        # webhook_secret requires secret:<name> via x-secret
        bad_webhook = copy.deepcopy(example_cfg)
        bad_webhook['routes'][0]['webhook_secret'] = 'plaintext-secret'
        with self.assertRaises(ConfigError) as caught:
            validate_config(schema, bad_webhook)
        self.assertEqual(caught.exception.code, 'config_secret_reference')

        # secret accepts both "" (for local polaris_ws) and secret:<name>
        good_secret = copy.deepcopy(example_cfg)
        good_secret['routes'][0]['secret'] = 'secret:ws_token'
        validate_config(schema, good_secret)

        # Numeric bounds enforced by schema
        for field_name, bad_val in (
            ('webhook_port', 70000),
            ('poll_seconds', 0.1),
            ('max_records', 5),
            ('max_text_bytes', 32),
            ('subscription_history', 5),
        ):
            bad_cfg = copy.deepcopy(example_cfg)
            bad_cfg[field_name] = bad_val
            with self.subTest(field=field_name), self.assertRaises(ConfigError) as exc:
                validate_config(schema, bad_cfg)
            self.assertEqual(exc.exception.code, 'config_range')

        bad_rpm = copy.deepcopy(example_cfg)
        bad_rpm['routes'][0]['messages_per_minute'] = 601
        with self.assertRaises(ConfigError) as exc:
            validate_config(schema, bad_rpm)
        self.assertEqual(exc.exception.code, 'config_range')

        # Route dataclass rejects empty secret for telegram/onebot and non-secret: non-empty secrets
        with self.assertRaises(ValueError):
            Route(id='r1', adapter='telegram', endpoint='https://api.telegram.org', secret='', group_id='-100123')
        with self.assertRaises(ValueError):
            Route(id='r1', adapter='polaris_ws', endpoint='ws://localhost:18766/', secret='raw', group_id='123')
        with self.assertRaises(ValueError):
            Settings(webhook_port=True)


class MessagingUnitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)
        secrets_dir = self.data_dir / 'secrets'
        secrets_dir.mkdir(mode=0o700)
        for name, val in (('tg', '123:ABC'), ('ob', 'ob-token'), ('bridge-webhook', 'wh-secret')):
            p = secrets_dir / name
            p.write_text(val)
            p.chmod(0o600)
        self.host = FakeHost()
        self.plugin = Messaging()
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('messaging-test')), self.data_dir)
        self.ctx.capabilities = {'session': 's1'}
        from neomega_runtime.operations import Operations
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.config = Settings(
            routes=[
                Route(
                    id='group',
                    adapter='telegram',
                    endpoint='https://api.telegram.org',
                    secret='secret:tg',
                    webhook_secret='secret:bridge-webhook',
                    group_id='-10012345',
                    allowed_installations=['consumer-installation'],
                    cb_prefix='cb:',
                ),
                Route(
                    id='ob',
                    adapter='onebot',
                    endpoint='http://127.0.0.1:5700',
                    secret='secret:ob',
                    webhook_secret='secret:bridge-webhook',
                    group_id='987654',
                    allowed_installations=['consumer-installation'],
                ),
            ]
        )
        self.plugin.pending_path = self.data_dir / 'messaging-pending.json'

    async def asyncTearDown(self):
        await self.plugin.on_stop(self.ctx)
        self.tmp.cleanup()

    async def test_housekeeping_dirty_only_save_and_acl_before_save(self):
        # Unauthorized/invalid send must NOT run housekeeping or write to storage.
        for bad_call, args, expected in (
            (make_call_context('other-app'), {'route_id': 'group', 'text': 'hi', 'request_id': make_v2_request_id()}, 'permission_denied'),
            (make_call_context('consumer-installation'), {'route_id': 'missing', 'text': 'hi', 'request_id': make_v2_request_id()}, 'not_found'),
            (make_call_context('consumer-installation'), {'route_id': 'group', 'text': '', 'request_id': make_v2_request_id()}, 'invalid_argument'),
        ):
            with self.assertRaises(ServiceRejected) as caught:
                await self.plugin.send(self.ctx, args, bad_call)
            self.assertEqual(caught.exception.code, expected)
            self.assertEqual(len(self.host.commits), 0)

        # Repeated collect() on idle state must perform 0 commits.
        await self.plugin.collect(self.ctx)
        await self.plugin.collect(self.ctx)
        self.assertEqual(len(self.host.commits), 0)

        # Send one message -> commits once, then idle collect() commits 0 times while unexpired.
        await self.plugin.send(self.ctx, {'route_id': 'group', 'text': 'hello', 'request_id': make_v2_request_id()}, make_call_context())
        commits_after_send = len(self.host.commits)
        self.assertEqual(commits_after_send, 1)
        await self.plugin.collect(self.ctx)
        self.assertEqual(len(self.host.commits), commits_after_send)

        # Clock rollback sets clock_rollback=True and saves once.
        self.host.values['msg_meta']['last_seen_clock'] = int(time.time() * 1000) + 60000
        await self.plugin.collect(self.ctx)
        self.assertEqual(len(self.host.commits), commits_after_send + 1)
        self.assertTrue(self.host.values['msg_meta']['clock_rollback'])

    async def test_service_input_validation_and_example_helpers(self):
        call_ctx = make_call_context()
        for handler, bad_args in (
            (self.plugin.send, None),
            (self.plugin.send, {'route_id': 'group', 'text': 'hi'}),
            (self.plugin.send, {'route_id': 'group', 'text': 'hi', 'request_id': make_v2_request_id(), 'extra': 1}),
            (self.plugin.send, {'route_id': '', 'text': 'hi', 'request_id': make_v2_request_id()}),
            (self.plugin.routes, {'unexpected': 1}),
            (self.plugin.routes, ['bad']),
            (self.plugin.status, {'delivery_id': 'not-hex'}),
            (self.plugin.status, {'delivery_id': None}),
            (self.plugin.status, {'extra': 1}),
            (self.plugin.subscribe, {'after': -1}),
            (self.plugin.subscribe, {'limit': 0}),
            (self.plugin.subscribe, {'limit': 201}),
            (self.plugin.subscribe, {'extra': True}),
        ):
            with self.subTest(handler=handler.__name__, args=bad_args), self.assertRaises(ServiceRejected) as caught:
                await handler(self.ctx, bad_args, call_ctx)
            self.assertEqual(caught.exception.code, 'invalid_argument')

        # Exercise example.py end-to-end against Messaging @service handlers.
        async def dispatch_service(service_name, arguments):
            method = service_name.rsplit('.', 1)[-1]
            return await getattr(self.plugin, method)(self.ctx, arguments, call_ctx)

        fake_consumer_ctx = SimpleNamespace(services=SimpleNamespace(call=AsyncMock(side_effect=dispatch_service)))
        status_resp = await example.send_notice(fake_consumer_ctx, make_v2_request_id('example123'))
        self.assertEqual(len(status_resp['deliveries']), 1)
        self.assertEqual(status_resp['deliveries'][0]['state'], 'queued')
        sub_resp = await example.read_messages(fake_consumer_ctx, cursor=0)
        self.assertEqual(len(sub_resp['events']), 1)

    async def test_unresolved_recovery_and_seal_not_blocked_by_unknown(self):
        await self.plugin.send(
            self.ctx,
            {'route_id': 'group', 'text': 'timeout msg', 'request_id': make_v2_request_id('unk12345')},
            make_call_context(),
        )
        with patch.object(self.plugin, 'remote', AsyncMock(side_effect=TimeoutError('tg timeout'))):
            await self.plugin.flush(self.ctx)
        self.assertEqual(self.host.values['msg_meta']['unresolved'], 0)
        deliveries = [v for k, v in self.host.values.items() if k.startswith('msg_delivery_')]
        self.assertEqual(deliveries[0]['state'], 'unknown')

        # seal must succeed even after an operation became 'unknown'.
        res = await self.plugin.on_maintenance(self.ctx, {'operation': 'seal', 'token': 't1'})
        self.assertEqual(res, {'status': 'sealed', 'token': 't1'})
        res_rel = await self.plugin.on_maintenance(self.ctx, {'operation': 'release', 'token': 't1'})
        self.assertEqual(res_rel, {'status': 'released', 'token': 't1'})

        # Simulate crashed worker leaving a stale 'dispatching' record + unresolved=1 on a disconnected WS route.
        self.ctx.config.routes.append(
            Route(id='ws1', adapter='polaris_ws', endpoint='ws://127.0.0.1:18766/', secret='', group_id='111')
        )
        rec = {
            'delivery_id': 'a' * 64, 'owner': 'consumer-installation', 'request_id': make_v2_request_id('stale123'),
            'parameters': 'b' * 64, 'route_id': 'ws1', 'text': 'stuck', 'state': 'dispatching',
            'created_at': time.time(), 'direction': 'outbound', 'id_policy': 'v2',
        }
        self.host.values['msg_delivery_' + rec['delivery_id']] = rec
        self.host.values['msg_queue_ws1'] = [rec['delivery_id']]
        self.host.values['msg_meta']['unresolved'] = 1
        res2 = await self.plugin.on_maintenance(self.ctx, {'operation': 'seal', 'token': 't2'})
        self.assertEqual(res2, {'status': 'sealed', 'token': 't2'})
        self.assertEqual(self.host.values['msg_meta']['unresolved'], 0)
        self.assertEqual(self.host.values['msg_delivery_' + rec['delivery_id']]['state'], 'unknown')
        self.assertEqual(self.host.values['msg_queue_ws1'], [])

    async def test_cbbridge_invalid_argument_advances_cursor(self):
        poll_calls = []

        async def mock_services(name, args):
            if name == 'plugin.neomega.cbbridge.register':
                return {'id': 'sub-1'}
            if name == 'plugin.neomega.cbbridge.subscribe':
                return {'cursor': 10}
            if name == 'plugin.neomega.cbbridge.poll':
                poll_calls.append(dict(args))
                return {
                    'cursor': 12,
                    'gap': False,
                    'events': [
                        {'connection_id': 'c1', 'source_sequence': 11, 'payload': 'x' * 5000},
                        {'connection_id': 'c1', 'source_sequence': 12, 'payload': 'hello cb'},
                    ],
                }
            raise AssertionError(f'unexpected call {name}')

        self.ctx.services = SimpleNamespace(call=AsyncMock(side_effect=mock_services))
        await self.plugin.poll_cbbridge(self.ctx)
        self.assertEqual(self.plugin.cb_routes['group']['cursor'], 12)
        self.assertEqual(self.plugin.health['group'], 'cbbridge_invalid_argument')
        deliveries = [v for k, v in self.host.values.items() if k.startswith('msg_delivery_')]
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]['text'], '[Command block] hello cb')

    async def test_socket_loop_missing_secret_does_not_crash(self):
        bad_route = Route(
            id='ws-secret', adapter='polaris_ws', endpoint='ws://127.0.0.1:1',
            secret='secret:nonexistent', group_id='123456',
        )
        task = asyncio.create_task(self.plugin.socket_loop(self.ctx, bad_route))
        await asyncio.sleep(0.05)
        self.assertFalse(task.done())
        self.assertEqual(self.plugin.health['ws-secret'], 'unavailable')
        self.plugin.stopping = True
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_json_null_fields_across_telegram_onebot_ws_and_webhook(self):
        now_sec = int(time.time())
        # Telegram getUpdates with JSON null message / chat / from / first_name / text
        tg_payload = {
            'ok': True,
            'result': [
                {'update_id': 1, 'message': None},
                {'update_id': 2, 'message': {'message_id': 10, 'chat': None, 'from': None, 'date': now_sec, 'text': 'ignored'}},
                {'update_id': 3, 'message': {
                    'message_id': 11, 'chat': {'id': -10012345},
                    'from': None, 'date': now_sec, 'text': 'channel post',
                }},
                {'update_id': 4, 'message': {
                    'message_id': 12, 'chat': {'id': -10012345},
                    'from': {'id': 77, 'first_name': None, 'is_bot': False}, 'date': now_sec, 'text': None,
                }},
            ],
        }
        with patch.object(self.plugin, 'remote', AsyncMock(return_value=tg_payload)):
            await self.plugin.poll(self.ctx)
        self.assertEqual(self.host.values['msg_meta']['offsets']['group'], 5)
        self.assertEqual(self.host.values['msg_meta']['seen_count'], 1)

        # OneBot WS event with sender: null and segment data: null
        await self.plugin.onebot_event(self.ctx, self.ctx.config.routes[1], {
            'post_type': 'message', 'message_type': 'group', 'group_id': 987654,
            'message_id': 101, 'time': now_sec, 'sender': None,
            'message': [{'type': 'text', 'data': None}, None, {'type': 'text', 'data': {'text': 'ws null ok'}}],
        })
        self.assertEqual(self.host.values['msg_meta']['seen_count'], 2)

        # OneBot HTTP webhook with sender: null, segment data: null, and writer.wait_closed raising ConnectionResetError
        body = json.dumps({
            'post_type': 'message', 'message_type': 'group', 'group_id': 987654,
            'message_id': 102, 'time': now_sec, 'sender': None,
            'message': [None, {'type': 'text', 'data': None}, {'type': 'text', 'data': {'text': 'wh null ok'}}],
        }).encode()
        sig = 'sha1=' + hmac.new(b'wh-secret', body, hashlib.sha1).hexdigest()
        req = (
            f'POST /ob HTTP/1.1\r\nHost: localhost\r\nContent-Length: {len(body)}\r\nX-Signature: {sig}\r\n\r\n'.encode()
            + body
        )
        reader = asyncio.StreamReader()
        reader.feed_data(req)
        reader.feed_eof()

        written = bytearray()

        class DisconnectingWriter:
            def write(self, data):
                written.extend(data)

            async def drain(self):
                pass

            def close(self):
                pass

            async def wait_closed(self):
                raise ConnectionResetError('client disconnected')

        await self.plugin.webhook(self.ctx, reader, DisconnectingWriter())
        self.assertTrue(bytes(written).startswith(b'HTTP/1.1 200 Response'))
        self.assertEqual(self.plugin.webhooks, 0)
        self.assertEqual(self.host.values['msg_meta']['seen_count'], 3)


class MessagingServeTests(unittest.IsolatedAsyncioTestCase):
    async def _run_harness(self, config_dict, *, roster=None):
        host = FakeHost()
        roster_ref = list(roster if roster is not None else [{'name': 'Steve', 'uuid': 'u1', 'xuid': '42'}])
        players_error = {'exc': None}
        reader = asyncio.StreamReader(limit=1 << 20)
        writer = ServeWriter()
        peer = ManagedPeer(reader, writer)
        plugin = Messaging()

        def send(frame):
            reader.feed_data((json.dumps(frame) + '\n').encode())

        async def wait_response(req_id):
            while True:
                msg = await asyncio.wait_for(writer.responses.get(), 3)
                if msg.get('id') == req_id:
                    return msg

        async def pump():
            while True:
                frame = await writer.requests.get()
                req_id, method, payload = frame['id'], frame['method'], frame['payload']
                if method == 'hello':
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {
                        'protocol_major': 1, 'protocol_minor': 0, 'installation_id': 'inst-1', 'generation': 1,
                        'capabilities': [], 'limits': {
                            'max_frame_bytes': 1 << 20, 'outgoing_requests': 128,
                            'inbound_requests': 32, 'queue_frames': 32}}}})
                elif method == 'capabilities.get':
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {'session': 's1'}}})
                elif method == 'players.list':
                    if players_error['exc'] is not None:
                        err = players_error['exc']
                        send({'type': 'response', 'id': req_id, 'payload': {'error': {
                            'code': err, 'message': err, 'retryable': False, 'request_id': req_id}}})
                    else:
                        send({'type': 'response', 'id': req_id, 'payload': {'result': {'session': 's1', 'players': copy.deepcopy(roster_ref)}}})
                else:
                    try:
                        res = await host.call(method, payload)
                        send({'type': 'response', 'id': req_id, 'payload': {'result': res}})
                    except IPCRejected as exc:
                        send({'type': 'response', 'id': req_id, 'payload': {'error': {
                            'code': exc.code, 'message': exc.message, 'retryable': False, 'request_id': req_id}}})

        return host, players_error, peer, plugin, send, wait_response, pump

    async def test_serve_on_event_single_ack_error_resilience_and_webhook_shutdown(self):
        wh_port = find_free_port()
        cfg = {
            'routes': [{
                'id': 'group',
                'adapter': 'onebot',
                'endpoint': 'http://127.0.0.1:5700',
                'secret': 'secret:ob',
                'webhook_secret': 'secret:wh',
                'group_id': '123456',
                'allowed_installations': ['consumer-1'],
            }],
            'webhook_host': '127.0.0.1',
            'webhook_port': wh_port,
            'poll_seconds': 60.0,
        }
        host, players_error, peer, plugin, send, wait_response, pump = await self._run_harness(cfg)
        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            (data_dir / 'config.json').write_text(json.dumps(cfg))
            secrets_dir = data_dir / 'secrets'
            secrets_dir.mkdir(mode=0o700)
            for name, val in (('ob', 'tok'), ('wh', 'wh-sec')):
                p = secrets_dir / name
                p.write_text(val)
                p.chmod(0o600)

            serve_task = asyncio.create_task(plugin.serve(peer, data_dir=data_dir))
            pump_task = asyncio.create_task(pump())
            try:
                send({'type': 'request', 'id': 'v1', 'method': 'lifecycle.validate',
                      'payload': {'installation_id': 'inst-1', 'generation': 1, 'revision': 1}})
                self.assertIn('result', (await wait_response('v1'))['payload'])
                send({'type': 'request', 'id': 'a1', 'method': 'lifecycle.activate',
                      'payload': {'installation_id': 'inst-1', 'generation': 1, 'revision': 1, 'state_version': 3}})
                self.assertTrue((await wait_response('a1'))['payload']['result']['activated'])
                self.assertIsNotNone(plugin.server)

                # 1. Valid chat.received -> single events.commit with delivery + no double-ACK
                send({'type': 'notification', 'method': 'event', 'payload': {
                    'kind': 'chat.received', 'subscription_id': 'chat', 'event_id': 'e1', 'delivery_token': 'd1',
                    'payload': {'player': 'Steve', 'xuid': '42', 'message': 'hello world'}}})
                # 2. Unmatched player -> single events.commit (empty mutations), no double-ACK
                send({'type': 'notification', 'method': 'event', 'payload': {
                    'kind': 'chat.received', 'subscription_id': 'chat', 'event_id': 'e2', 'delivery_token': 'd2',
                    'payload': {'player': 'Ghost', 'xuid': '99', 'message': 'ignored'}}})
                for _ in range(50):
                    if ('chat', 'e2', 'd2') in host.deliveries:
                        break
                    await asyncio.sleep(0.02)
                self.assertEqual(len(host.commits), 2)
                self.assertEqual(len(host.commits[0]['mutations']) > 0, True)
                self.assertEqual(host.commits[1]['mutations'], [])

                # 3. ctx.players() returns not_in_game -> single events.commit ACK, no crash
                players_error['exc'] = 'not_in_game'
                send({'type': 'notification', 'method': 'event', 'payload': {
                    'kind': 'chat.received', 'subscription_id': 'chat', 'event_id': 'e3', 'delivery_token': 'd3',
                    'payload': {'player': 'Steve', 'xuid': '42', 'message': 'while offline'}}})
                for _ in range(50):
                    if ('chat', 'e3', 'd3') in host.deliveries:
                        break
                    await asyncio.sleep(0.02)
                players_error['exc'] = None
                self.assertFalse(serve_task.done())
                self.assertEqual(host.deliveries, {('chat', 'e1', 'd1'), ('chat', 'e2', 'd2'), ('chat', 'e3', 'd3')})

                # 4. CommitUncertain on on_event -> defer_ack(), no double-ACK, no crash!
                host.next_commit_error = {'invalid': 'receipt'}
                host.commit_uncertain_persists = True
                send({'type': 'notification', 'method': 'event', 'payload': {
                    'kind': 'chat.received', 'subscription_id': 'chat', 'event_id': 'e4', 'delivery_token': 'd4',
                    'payload': {'player': 'Steve', 'xuid': '42', 'message': 'uncertain'}}})
                for _ in range(50):
                    if ('chat', 'e4', 'd4') in host.deliveries:
                        break
                    await asyncio.sleep(0.02)
                self.assertFalse(serve_task.done())
                self.assertIsNotNone(plugin.pending)

                # Service send via Worker while pending is reconciled on receipt (commit landed on host) -> succeeds!
                now = datetime.now(timezone.utc)
                call_ctx = {
                    'installation_id': 'consumer-1', 'generation': 1, 'call_id': 'c1',
                    'accepted_at': now.isoformat(), 'deadline': (now + timedelta(seconds=10)).isoformat(),
                }
                send({'type': 'request', 'id': 's1', 'method': 'services.online.invoke',
                      'payload': {'service': 'plugin.neomega.messaging.send', 'major': 1, 'call_id': 'c1',
                                  'call_context': call_ctx,
                                  'arguments': {'route_id': 'group', 'text': 'notice', 'request_id': make_v2_request_id('srv12345')}}})
                res_s1 = await wait_response('s1')
                self.assertIn('result', res_s1['payload'])
                self.assertIsNone(plugin.pending)

                # Service invalid_argument returns service_rejected without crashing worker
                call_ctx2 = dict(call_ctx, call_id='c2')
                send({'type': 'request', 'id': 's2', 'method': 'services.online.invoke',
                      'payload': {'service': 'plugin.neomega.messaging.subscribe', 'major': 1, 'call_id': 'c2',
                                  'call_context': call_ctx2, 'arguments': {'after': -5}}})
                res_s2 = await wait_response('s2')
                self.assertEqual(res_s2['payload']['error']['code'], 'service_rejected')
                self.assertEqual(res_s2['payload']['error']['details']['code'], 'invalid_argument')

                # 5. maintenance seal shuts down webhook server cleanly, release restarts it
                send({'type': 'request', 'id': 'm1', 'method': 'lifecycle.maintenance',
                      'payload': {'installation_id': 'inst-1', 'generation': 1, 'operation': 'seal', 'token': 'tok-1'}})
                res_m1 = await wait_response('m1')
                self.assertEqual(res_m1['payload']['result']['status'], 'sealed')
                self.assertIsNone(plugin.server)

                send({'type': 'request', 'id': 'm2', 'method': 'lifecycle.maintenance',
                      'payload': {'installation_id': 'inst-1', 'generation': 1, 'operation': 'release', 'token': 'tok-1'}})
                res_m2 = await wait_response('m2')
                self.assertEqual(res_m2['payload']['result']['status'], 'released')
                self.assertIsNotNone(plugin.server)

                # 6. Drain closes webhook server cleanly without accept/task-sealing race
                send({'type': 'request', 'id': 'dr1', 'method': 'lifecycle.drain',
                      'payload': {'installation_id': 'inst-1', 'generation': 1}})
                self.assertTrue((await wait_response('dr1'))['payload']['result']['drained'])
                self.assertIsNone(plugin.server)
                self.assertFalse(serve_task.done())
            finally:
                serve_task.cancel()
                pump_task.cancel()
                await asyncio.gather(serve_task, pump_task, return_exceptions=True)


if __name__ == '__main__':
    unittest.main()
