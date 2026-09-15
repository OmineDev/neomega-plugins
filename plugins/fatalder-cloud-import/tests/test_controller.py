import asyncio
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import timedelta
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from fatalder_plugin.controller import Controller, now, expired
from fatalder_plugin.client import WorkerClient


class Store:
    def __init__(self):
        self.values = {}
    async def get(self, key, default=None):
        return copy.deepcopy(self.values.get(key, default))
    async def transaction(self, **kw):
        values = copy.deepcopy(self.values)
        async def save():
            self.values.update(values)
            return {}
        return SimpleNamespace(set=lambda k,v: values.update({k:copy.deepcopy(v)}), save=save,
                               action=lambda _: None)


class ControllerTests(unittest.IsolatedAsyncioTestCase):
    def test_go_rfc3339nano_deadlines(self):
        self.assertTrue(expired('2000-01-01T00:00:00.414893591Z'))
        self.assertFalse(expired('2100-01-01T00:00:00.1Z'))

    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tasks = []
        def spawn(coro, **kw):
            task = asyncio.create_task(coro)
            self.tasks.append(task)
            return task
        self.ctx = SimpleNamespace(storage=Store(), data_dir=self.temp.name,
            config=SimpleNamespace(worker_url='https://example.test', api_key='secret:key',
                target_server_id='server', rental_server_code='123', files_directory='imports',
                admin_uuids=['admin'], poll_seconds=2, revoke_operator_on_completion=True),
            spawn=spawn, every=lambda *a, **kw: None, players=AsyncMock(return_value=[]),
            log=SimpleNamespace(warning=lambda *a: None))
        self.client = SimpleNamespace()
        self.operator = SimpleNamespace(inspect=AsyncMock(return_value=dict(uuid='bot',name='Bot',
            epoch='e', revision=1, entity_unique_id=1, is_op=False)),
            set_permission=AsyncMock(return_value=dict(state='succeeded',changed=True)))
        self.c = Controller(self.ctx, self.client, self.operator)
        self.c.request = AsyncMock(return_value={})
        self.c.tell = AsyncMock()
        self.c.state = dict(job_id='job', phase='running', binding=self.c.binding())
        self.snapshot = dict(session_id='s', attempt_id='a', bot_uuid='bot', bot_name='Bot',
            server_id='server', rental_server_code='123', phase='authorize',
            deadline=(now()+timedelta(seconds=90)).isoformat())

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def test_grant_then_cleanup_only_owned_permission(self):
        await self.c.operator_gate(self.snapshot)
        self.assertTrue(self.c.state['lease']['owned'])
        self.snapshot['phase'] = 'cleanup'
        await self.c.operator_gate(self.snapshot)
        args = self.operator.set_permission.call_args.args
        self.assertFalse(args[1])
        self.assertTrue(args[0]['owned'])
        self.assertNotIn('lease', self.c.state)

    async def test_default_policy_retains_operator_and_records_before_ack(self):
        from fatalder_plugin.settings import Settings
        self.assertFalse(Settings.__dataclass_fields__['revoke_operator_on_completion'].default)
        self.ctx.config.revoke_operator_on_completion = False
        await self.c.operator_gate(self.snapshot)
        self.operator.set_permission.reset_mock()
        self.snapshot['phase'] = 'cleanup'
        async def check_saved(*args):
            saved = self.ctx.storage.values['fatalder.controller']
            self.assertEqual(saved['lease']['cleanup_policy'], 'retain')
            self.assertEqual(saved['last_operator_session']['cleanup_outcome'], 'operator_retained')
        self.c.request.side_effect = check_saved
        await self.c.operator_gate(self.snapshot)
        self.operator.set_permission.assert_not_called()
        self.assertNotIn('lease', self.c.state)

    async def test_retained_cleanup_survives_lost_ack_reload_and_config_change(self):
        self.ctx.config.revoke_operator_on_completion = False
        await self.c.operator_gate(self.snapshot)
        self.snapshot['phase'] = 'cleanup'
        self.c.request.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            await self.c.operator_gate(self.snapshot)
        self.ctx.config.revoke_operator_on_completion = True
        restored = Controller(self.ctx, self.client, self.operator)
        restored.request = AsyncMock(return_value={})
        await restored.start()
        self.operator.set_permission.reset_mock()
        await restored.operator_gate(self.snapshot)
        self.operator.set_permission.assert_not_called()
        self.assertNotIn('lease', restored.state)
        self.assertEqual(restored.state['last_operator_session']['cleanup_policy'], 'retain')

    async def test_unknown_grant_retained_cleanup_closes_without_replaying(self):
        self.ctx.config.revoke_operator_on_completion = False
        self.operator.set_permission.return_value = dict(state='unknown')
        await self.c.operator_gate(self.snapshot)
        self.operator.set_permission.reset_mock()
        self.snapshot['phase'] = 'cleanup'
        await self.c.operator_gate(self.snapshot)
        self.operator.set_permission.assert_not_called()
        self.assertNotIn('lease', self.c.state)
        self.assertFalse(self.c.state['last_operator_session']['owned'])

    async def test_wire_ready_preserves_confirmation_after_poll(self):
        self.c.state.update(phase='READY', quote={'quote_id': 'q'}, confirm_token='token')
        self.c.request.return_value = {'state': 'READY'}
        self.c.read_events = AsyncMock()
        await self.c.tick()
        self.assertEqual(self.c.state['phase'], 'quoted')
        self.assertEqual(self.c.state['confirm_token'], 'token')

    async def test_wire_terminal_states_release_slot_after_poll(self):
        self.c.read_events = AsyncMock()
        for state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            self.c.request.return_value = {'state': state}
            await self.c.tick()
            self.assertEqual(self.c.state['phase'], state.lower())

    async def test_wire_ready_reconciles_original_start(self):
        original = {'idempotency_key': 'same-key'}
        self.c.state['start'] = original
        self.c.request.return_value = {'state': 'READY'}
        self.c.run_control = AsyncMock()
        await self.c.command('Admin', ['恢复'])
        await asyncio.gather(*self.tasks)
        self.c.run_control.assert_awaited_once_with('start', original)

    async def test_passcode_resolved_only_for_outgoing_start_and_recover(self):
        self.ctx.config.rental_access = SimpleNamespace(passcode='secret:rental')
        self.ctx.secrets = SimpleNamespace(get=lambda ref: 'private-passcode')
        body = {'idempotency_key': 'original'}
        for action in ('start', 'recover'):
            await self.c.run_control(action, body)
            self.assertEqual(self.c.request.call_args.args[2]['rental_server_passcode'], 'private-passcode')
            self.assertNotIn('rental_server_passcode', body)
            self.assertNotIn('private-passcode', str(self.c.state))
        await self.c.run_control('pause', {})
        self.assertIsNone(self.c.request.call_args.args[2])

    async def test_pause_resume_cancel_send_empty_http_body(self):
        seen = []
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                seen.append((self.path, body))
                self.send_response(204 if body == b'' else 400)
                self.end_headers()
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            self.c.client = WorkerClient(f'http://127.0.0.1:{server.server_port}',
                lambda: 'test-key', allow_loopback_http=True)
            self.c.request = Controller.request.__get__(self.c)
            for action in ('pause', 'resume', 'cancel'):
                await self.c.run_control(action, {})
            self.assertEqual(seen, [(f'/v2/jobs/job/{action}', b'')
                                   for action in ('pause', 'resume', 'cancel')])
        finally:
            await asyncio.to_thread(server.shutdown)
            server.server_close()
            thread.join()

    async def test_existing_op_is_never_revoked(self):
        self.operator.inspect.return_value['is_op'] = True
        await self.c.operator_gate(self.snapshot)
        self.snapshot['phase'] = 'cleanup'
        await self.c.operator_gate(self.snapshot)
        self.operator.set_permission.assert_not_called()
        self.assertEqual(self.c.request.call_count, 2)

    async def test_unknown_grant_does_not_ack_or_drop_lease(self):
        self.operator.set_permission.return_value = dict(state='unknown')
        await self.c.operator_gate(self.snapshot)
        self.assertIn('lease', self.c.state)
        self.c.request.assert_not_called()
        self.snapshot['phase'] = 'cleanup'
        await self.c.operator_gate(self.snapshot)
        self.assertTrue(self.operator.set_permission.call_args.kwargs['reconcile_only'])
        self.c.request.assert_not_called()

    async def test_cleanup_without_grant_reconciles_without_issuing_one(self):
        self.operator.set_permission.return_value = dict(state='pending')
        await self.c.operator_gate(self.snapshot)
        self.operator.set_permission.return_value = dict(state='succeeded',changed=False)
        self.snapshot['phase'] = 'cleanup'
        await self.c.operator_gate(self.snapshot)
        self.assertTrue(self.operator.set_permission.call_args.kwargs['reconcile_only'])
        self.assertNotIn('lease', self.c.state)

    async def test_mismatched_connection_never_gets_ack(self):
        await self.c.operator_gate(self.snapshot)
        self.c.request.reset_mock()
        self.snapshot.update(phase='cleanup', session_id='other')
        await self.c.operator_gate(self.snapshot)
        self.c.request.assert_not_called()
        self.assertIn('lease', self.c.state)

    async def test_wrong_server_or_expired_gate_is_ignored(self):
        self.snapshot['rental_server_code'] = 'other'
        await self.c.operator_gate(self.snapshot)
        self.operator.inspect.assert_not_called()
        self.snapshot['rental_server_code'] = '123'
        self.snapshot['deadline'] = (now()-timedelta(seconds=1)).isoformat()
        await self.c.operator_gate(self.snapshot)
        self.operator.inspect.assert_not_called()

    async def test_lost_cleanup_ack_cleared_by_durable_snapshot(self):
        await self.c.operator_gate(self.snapshot)
        self.c.request.side_effect = TimeoutError()
        self.snapshot['phase'] = 'cleanup'
        with self.assertRaises(TimeoutError):
            await self.c.operator_gate(self.snapshot)
        self.assertTrue(self.c.state['lease']['cleaned'])
        self.snapshot['phase'] = 'cleaned'
        await self.c.operator_gate(self.snapshot)
        self.assertNotIn('lease', self.c.state)

    async def test_reload_observes_without_restarting(self):
        self.ctx.storage.values['fatalder.controller'] = self.c.state
        await self.c.start()
        self.c.request.assert_not_called()
        self.assertEqual(self.tasks, [])

    async def test_reload_refuses_different_worker_or_account(self):
        self.ctx.storage.values['fatalder.controller'] = self.c.state
        self.ctx.config.api_key = 'secret:other'
        with self.assertRaises(ValueError):
            await self.c.start()

    async def test_source_traversal_and_symlink_rejected(self):
        root = self.c.files_root()
        (root / 'link').symlink_to(Path(self.temp.name) / 'outside')
        for name in ('../outside', '/etc/passwd', 'link', '..', 'a\\b'):
            with self.assertRaises(ValueError):
                self.c.source_path(name)

    async def test_expired_confirmation_never_starts(self):
        self.c.state.update(phase='quoted', confirm_token='t',quote={'expires_at': (now()-timedelta(seconds=1)).isoformat()})
        with self.assertRaises(ValueError):
            await self.c.command('Admin',['确认','t'])
        self.assertEqual(self.tasks, [])

    async def test_confirmation_saves_original_key_before_request(self):
        self.c.state.update(phase='quoted', confirm_token='t', quote={'quote_id':'q', 'expires_at': (now()+timedelta(seconds=20)).isoformat()})
        await self.c.command('Admin',['确认','t'])
        saved = self.ctx.storage.values['fatalder.controller']
        self.assertEqual(saved['phase'], 'start_pending')
        await asyncio.gather(*self.tasks)
        self.assertEqual(self.c.request.call_args.args[2], saved['start'])
        with self.assertRaises(ValueError):
            await self.c.command('Admin',['确认','t'])

    async def test_non_admin_chat_ack_without_command(self):
        event = SimpleNamespace(kind='chat.received',payload={'payload':{'message':'!导入 确认 t','player':'x'}},ack=AsyncMock())
        await self.c.handle(event)
        event.ack.assert_awaited_once()
        self.c.request.assert_not_called()

    async def test_prepare_body_and_key_persist_before_network(self):
        self.c.state = {}
        root = self.c.files_root()
        (root / 'house.bdx').write_bytes(b'building')
        ref = dict(object_id='obj',sha256='sha256:'+'a'*64,size_bytes=8,display_name='house.bdx')
        self.client.upload = lambda _: ref
        async def prepare(method, path, body):
            saved = self.ctx.storage.values['fatalder.controller']
            self.assertEqual(saved['prepare'], body)
            self.assertTrue(body['spec']['managed_operator'])
            self.assertEqual(body['spec']['target']['account_source'], 'service_center')
            self.assertEqual(body['spec']['tasks'][0]['build']['source'], ref)
            self.assertEqual(body['spec']['tasks'][0]['build']['start_position'], {'x':1,'y':64,'z':2})
            return dict(job_id='j',quote=dict(quote_id='q',amount=10,currency='unit',units=1,
                       expires_at=(now()+timedelta(seconds=30)).isoformat()))
        self.c.request.side_effect = prepare
        await self.c.command('Admin',['house.bdx','1','64','2','overworld'])
        self.assertEqual(self.c.state['phase'], 'quoted')
        self.assertEqual(self.tasks, [])

    async def test_bad_download_never_acknowledged_or_advanced(self):
        event = dict(seq=1,task_key='import',payload={'gate':dict(gate_id='g',attempt_id='a',
                     kind='conversion',object={'object_id':'o'})})
        self.client.events = lambda *args: iter([event])
        def bad_download(*args):
            raise ValueError('hash mismatch')
        self.client.download = bad_download
        await self.c.read_events('job')
        self.c.request.assert_not_called()
        self.assertEqual(self.c.state.get('cursor',0),0)

    async def test_duplicate_event_not_acknowledged_again(self):
        self.c.state['cursor'] = 3
        self.client.events = lambda *args: iter([dict(seq=3,payload={'gate':{'object':{}}}),dict(seq=4,payload={})])
        await self.c.read_events('job')
        self.c.request.assert_not_called()
        self.assertEqual(self.c.state['cursor'],4)
