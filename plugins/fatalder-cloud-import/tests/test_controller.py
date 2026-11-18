import asyncio
import copy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from datetime import timedelta
import json
import logging
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from neomega_runtime import Event, PluginContext
from neomega_runtime.operations import Operations

from fatalder_plugin.controller import Controller, chunk_tellraw, now, expired
from fatalder_plugin.client import WorkerClient
from fatalder_plugin.durable import barrier
from main import CloudImport


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

    async def test_maintenance_busy_preserves_active_slot_and_background(self):
        before = copy.deepcopy(self.c.state)
        result = await self.c.maintenance({'operation': 'seal', 'token': 'backup'})
        self.assertEqual(result['status'], 'busy')
        self.assertEqual(self.c.state, before)
        self.assertFalse(self.c.stopped.is_set())
        self.c.request.assert_not_called()

    async def test_maintenance_seal_blocks_writers_until_matching_release(self):
        self.c.state = dict(job_id='job', phase='succeeded', cursor=1)
        self.client.events = lambda *args: iter([dict(seq=2)])
        event = SimpleNamespace(kind='other', payload={}, ack=AsyncMock())
        self.assertEqual((await self.c.maintenance(dict(operation='seal', token='a')))['status'], 'sealed')
        before = copy.deepcopy(self.c.state)
        await self.c.handle(event)
        await self.c.tick()
        await self.c.command('Admin', ['状态'])
        await self.c.run_control('start', {})
        await self.c.read_events('job')
        self.assertEqual(self.c.state, before)
        self.assertEqual(self.ctx.storage.values, {})
        event.ack.assert_not_called()
        self.c.tell.assert_not_called()
        self.c.request.assert_not_called()
        self.assertEqual((await self.c.maintenance(dict(operation='release', token='b')))['status'], 'busy')
        self.assertEqual((await self.c.maintenance(dict(operation='release', token='a')))['status'], 'released')
        self.assertEqual((await self.c.maintenance(dict(operation='release', token='a')))['status'], 'released')
        await self.c.handle(event)
        event.ack.assert_awaited_once()

    async def test_maintenance_does_not_overtake_admitted_control(self):
        self.c.state['phase'] = 'succeeded'
        await self.c.command('Admin', ['取消'])
        result = await self.c.maintenance(dict(operation='seal', token='a'))
        self.assertEqual(result['status'], 'busy')
        await asyncio.gather(*self.tasks)
        self.c.request.assert_awaited_once()
        self.assertFalse(self.c.stopped.is_set())

    async def test_maintenance_rejects_uncertain_durable_state(self):
        for extra in ({'phase': 'quoted'}, {'phase': 'prepare_pending'},
                      {'phase': 'start_pending'}, {'phase': 'paused'},
                      {'lease': {'cleaned': True}}, {'review': {'token': 'r'}},
                      {'recovery_required': True},
                      {'task_outcomes': {'a': {'state': 'unknown'}}}):
            with self.subTest(extra=extra):
                self.c.state = dict(job_id='job', phase='failed')
                self.c.state.update(extra)
                before = copy.deepcopy(self.c.state)
                self.assertEqual((await self.c.maintenance(dict(operation='seal', token='a')))['status'], 'busy')
                self.assertEqual(self.c.state, before)
        self.c.state = dict(phase='failed', task_outcomes={'a': {'state': 'failed', 'content': 'incomplete'}})
        self.assertEqual((await self.c.maintenance(dict(operation='seal', token='a')))['status'], 'sealed')

    async def test_maintenance_busy_returns_while_event_ack_in_flight(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def ack():
            entered.set()
            await release.wait()
        event = SimpleNamespace(kind='other', payload={}, ack=ack)
        task = self.ctx.spawn(self.c.handle(event))
        await entered.wait()
        self.c.state = {}
        try:
            result = await asyncio.wait_for(self.c.maintenance(dict(operation='seal', token='a')), 0.1)
            self.assertEqual(result['status'], 'busy')
            self.assertFalse(task.done())
            self.assertFalse(self.c.stopped.is_set())
        finally:
            release.set()
            await task

    async def test_maintenance_and_new_import_have_one_admission_winner(self):
        root = self.c.files_root()
        (root / 'house.bdx').write_bytes(b'building')
        ref = dict(object_id='obj', sha256='sha256:'+'a'*64, size_bytes=8, display_name='house.bdx')
        self.client.upload = lambda _: ref
        self.c.request.return_value = dict(job_id='j', quote=dict(quote_id='q', amount=10,
            currency='unit', units=1, expires_at=(now()+timedelta(seconds=30)).isoformat()))
        for seal_first in (True, False):
            with self.subTest(seal_first=seal_first):
                self.c.state = {}
                self.c.request.reset_mock()
                self.ctx.storage.values.clear()
                seal = self.c.maintenance(dict(operation='seal', token='a'))
                command = self.c.command('Admin', ['house.bdx', '1', '64', '2', 'overworld'])
                if seal_first:
                    result, _ = await asyncio.gather(seal, command)
                    self.assertEqual(result['status'], 'sealed')
                    self.c.request.assert_not_called()
                    self.assertEqual(self.ctx.storage.values, {})
                    await self.c.maintenance(dict(operation='release', token='a'))
                else:
                    _, result = await asyncio.gather(command, seal)
                    self.assertEqual(result['status'], 'busy')
                    self.c.request.assert_awaited_once()
                    self.assertEqual(self.c.state['phase'], 'quoted')

    async def test_maintenance_seal_blocks_already_waiting_reader_commit(self):
        self.c.state = dict(job_id='job', phase='succeeded', cursor=1)
        entered, release = threading.Event(), threading.Event()
        def events():
            entered.set()
            release.wait(5)
            yield dict(seq=2)
        self.client.events = lambda *args: events()
        reader = self.ctx.spawn(self.c.read_events('job'))
        await asyncio.to_thread(entered.wait, 5)
        try:
            self.assertEqual((await self.c.maintenance(dict(operation='seal', token='a')))['status'], 'sealed')
        finally:
            release.set()
            await reader
        self.assertEqual(self.c.state['cursor'], 1)
        self.assertEqual(self.ctx.storage.values, {})
        self.assertFalse(self.c.stopped.is_set())

    async def test_control_request_keeps_observer_available_but_blocks_maintenance(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def request(method, path, body=None):
            if method == 'POST':
                entered.set()
                await release.wait()
                return {}
            return dict(job_id='job', state='RUNNING')
        self.c.request.side_effect = request
        self.c.read_events = AsyncMock()
        control = self.ctx.spawn(self.c.run_control('start', {}))
        await entered.wait()
        try:
            await asyncio.wait_for(self.c.tick(), 0.2)
            self.assertEqual((await self.c.maintenance(dict(operation='seal', token='a')))['status'], 'busy')
        finally:
            release.set()
            await control

    async def test_retained_terminal_poll_releases_same_session_without_writes(self):
        self.ctx.config.revoke_operator_on_completion = False
        await self.c.operator_gate(self.snapshot)
        self.snapshot['phase'] = 'retained'
        self.c.request.return_value = dict(job_id='job', state='SUCCEEDED',
            spec={'operator_cleanup_policy': 'retain'}, operator_session=self.snapshot,
            task_outcomes={'import': {'state': 'succeeded', 'content': 'incomplete'}})
        self.c.read_events = AsyncMock()
        self.c.request.reset_mock()
        self.operator.set_permission.reset_mock()
        await self.c.tick()
        await self.c.tick()
        self.assertNotIn('lease', self.c.state)
        self.operator.set_permission.assert_not_called()
        self.assertTrue(all(c.args[0] == 'GET' for c in self.c.request.call_args_list))
        self.assertEqual(self.c.state['task_outcomes']['import']['content'], 'incomplete')

    async def test_legacy_cleanup_requires_explicit_evidence_bound_confirmation(self):
        await self.c.operator_gate(self.snapshot)
        self.snapshot['phase'] = 'cleanup_failed'
        job = dict(job_id='job', state='FAILED', operator_session=self.snapshot,
            spec={'tasks': [{'task_key': 'import'}]},
            task_outcomes={'import': {'state': 'succeeded', 'content': 'incomplete'}})
        self.c.request.return_value = job
        self.c.request.reset_mock()
        self.operator.set_permission.reset_mock()
        await self.c.command('Admin', ['核查'])
        self.assertIn('lease', self.c.state)
        token = self.c.state['review']['token']
        job['task_outcomes']['import']['state'] = 'unknown'
        await self.c.command('Admin', ['确认核查', token])
        self.assertIn('lease', self.c.state)
        job['task_outcomes']['import']['state'] = 'succeeded'
        await self.c.command('Admin', ['核查'])
        await self.c.command('Admin', ['确认核查', self.c.state['review']['token']])
        self.assertNotIn('lease', self.c.state)
        self.assertEqual(self.c.state['phase'], 'failed')
        self.assertEqual(self.c.state['last_operator_session']['cleanup_outcome'], 'explicit_retention_review')
        self.operator.set_permission.assert_not_called()
        self.assertTrue(all(c.args[0] == 'GET' for c in self.c.request.call_args_list))

    async def test_authorization_uses_frozen_worker_policy_after_config_change(self):
        self.c.request.return_value = dict(job_id='job', state='RUNNING',
            spec={'operator_cleanup_policy': 'retain'}, operator_session=self.snapshot)
        self.c.read_events = AsyncMock()
        await self.c.tick()
        self.assertEqual(self.c.state['lease']['cleanup_policy'], 'retain')

    async def test_retained_wrong_session_and_recovery_keep_lease(self):
        await self.c.operator_gate(self.snapshot)
        job = dict(job_id='job', state='SUCCEEDED', spec={'operator_cleanup_policy': 'retain'},
            operator_session=dict(self.snapshot, phase='retained', session_id='wrong'))
        await self.c.reconcile_retained(job)
        self.assertIn('lease', self.c.state)
        job['operator_session']['session_id'] = 's'
        job['recovery_required'] = True
        await self.c.reconcile_retained(job)
        self.assertIn('lease', self.c.state)

    async def test_retained_event_closes_old_connection_but_keeps_running_slot(self):
        self.c.state['worker_cleanup_policy'] = 'retain'
        await self.c.operator_gate(self.snapshot)
        retained = dict(self.snapshot, phase='retained')
        self.client.events = lambda *args: iter([dict(seq=1, kind='target.operator_session', payload=retained)])
        await self.c.read_events('job')
        self.assertNotIn('lease', self.c.state)
        self.assertEqual(self.c.state['phase'], 'running')
        await self.c.operator_gate(dict(self.snapshot, session_id='next'))
        self.client.events = lambda *args: iter([dict(seq=2, kind='target.operator_session', payload=retained)])
        await self.c.read_events('job')
        self.assertEqual(self.c.state['lease']['session_id'], 'next')

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

    async def test_sealed_or_barriered_handle_defers_ack_on_real_event(self):
        self.c.state = dict(job_id='job', phase='succeeded')
        self.assertEqual((await self.c.maintenance(dict(operation='seal', token='a')))['status'], 'sealed')
        peer = SimpleNamespace(call=AsyncMock(return_value={}))
        ev_ctx = PluginContext(SimpleNamespace(peer=peer, log=logging.getLogger('fatalder-test')), self.temp.name)
        ev = Event(ev_ctx, {'kind': 'chat.received', 'payload': {'player': 'Admin', 'message': '!导入 状态'},
                            'subscription_id': 'chat', 'event_id': 'e1', 'delivery_token': 'dt1'})
        await self.c.handle(ev)
        self.assertTrue(ev.ack_handled)
        self.assertTrue(ev._deferred)
        self.assertFalse(ev._acked)
        peer.call.assert_not_awaited()

        await self.c.maintenance(dict(operation='release', token='a'))
        barrier(self.ctx).write_text(json.dumps({'commit_id': 'c1'}))
        ev2 = Event(ev_ctx, {'kind': 'chat.received', 'payload': {'player': 'Admin', 'message': '!导入 状态'},
                             'subscription_id': 'chat', 'event_id': 'e2', 'delivery_token': 'dt2'})
        await self.c.handle(ev2)
        self.assertTrue(ev2._deferred)
        self.assertFalse(ev2._acked)

    async def test_xuid_mismatch_and_word_boundary_rejected(self):
        self.ctx.players = AsyncMock(return_value=[{'name': 'Admin', 'uuid': 'admin', 'xuid': '111'}])
        # No word boundary after !导入 -> acked without executing command
        ev_word = SimpleNamespace(kind='chat.received',
                                  payload={'payload': {'message': '!导入测试 状态', 'player': 'Admin', 'xuid': '111'}},
                                  ack=AsyncMock())
        await self.c.handle(ev_word)
        ev_word.ack.assert_awaited_once()
        self.c.tell.assert_not_called()

        # XUID mismatch -> acked without executing command
        ev_xuid = SimpleNamespace(kind='chat.received',
                                  payload={'payload': {'message': '!导入 状态', 'player': 'Admin', 'xuid': '999'}},
                                  ack=AsyncMock())
        await self.c.handle(ev_xuid)
        ev_xuid.ack.assert_awaited_once()
        self.c.tell.assert_not_called()

    async def test_tell_chunks_long_messages_within_320_bytes(self):
        actions = []

        class CmdStore:
            def __init__(self):
                self.values = {}
            async def transaction(self, commit_id=None, keys=None):
                async def save():
                    return {}
                return SimpleNamespace(set=lambda k, v: None,
                                       action=lambda intent: actions.append(intent),
                                       save=save)

        ctx = SimpleNamespace(
            storage=CmdStore(),
            data_dir=self.temp.name,
            commands=SimpleNamespace(prepare=lambda cmd, **kw: {'cmd': cmd, **kw}),
            config=self.ctx.config,
        )
        ctrl = Controller(ctx, self.client, self.operator)
        await ctrl.tell('Admin', '甲' * 150)
        self.assertGreater(len(actions), 1)
        for act in actions:
            self.assertLessEqual(len(act['cmd'].encode('utf-8')), 320)
        self.assertEqual(len(chunk_tellraw('@a[name="Admin"]', '甲' * 150)), len(actions))

    async def test_prepare_releases_lock_during_upload_and_http(self):
        self.c.state = {}
        root = self.c.files_root()
        (root / 'house.bdx').write_bytes(b'building')
        ref = dict(object_id='obj', sha256='sha256:' + 'a' * 64, size_bytes=8, display_name='house.bdx')
        upload_entered, upload_release = threading.Event(), threading.Event()
        http_entered, http_release = asyncio.Event(), asyncio.Event()

        def slow_upload(_):
            upload_entered.set()
            upload_release.wait(5)
            return ref

        async def slow_prepare(method, path, body):
            self.assertFalse(self.c.lock.locked())
            http_entered.set()
            await http_release.wait()
            return dict(job_id='j', quote=dict(quote_id='q', amount=10, currency='unit', units=1,
                                               expires_at=(now() + timedelta(seconds=30)).isoformat()))

        self.client.upload = slow_upload
        self.c.request.side_effect = slow_prepare
        task = asyncio.create_task(self.c.command('Admin', ['house.bdx', '1', '64', '2', 'overworld']))
        await asyncio.to_thread(upload_entered.wait, 5)
        self.assertFalse(self.c.lock.locked())
        self.assertEqual((await self.c.maintenance(dict(operation='seal', token='a')))['status'], 'busy')
        upload_release.set()

        await http_entered.wait()
        self.assertFalse(self.c.lock.locked())
        self.assertEqual((await self.c.maintenance(dict(operation='seal', token='a')))['status'], 'busy')
        http_release.set()
        await task
        self.assertEqual(self.c.state['phase'], 'quoted')

    async def test_on_maintenance_token_fallback(self):
        plugin = CloudImport()
        res = await plugin.on_maintenance(self.ctx, {'operation': 'seal', 'token': 'tok-1'})
        self.assertEqual(res, {'status': 'unsupported', 'token': 'tok-1'})
        res2 = await self.c.maintenance({'operation': 'unknown', 'token': 'tok-2'})
        self.assertEqual(res2, {'status': 'unsupported', 'token': 'tok-2'})

    async def test_serve_ack_lifecycle(self):
        commits = []
        state = {'revision': 0, 'values': {}}

        async def call(method, payload, timeout=None):
            if method == 'capabilities.get':
                return {'session': 's1', 'capabilities': []}
            if method == 'state.get':
                keys = payload.get('keys')
                vals = state['values'] if keys is None else {k: state['values'][k] for k in keys if k in state['values']}
                return {'revision': state['revision'], 'values': copy.deepcopy(vals), 'session': 's1'}
            if method in ('state.commit', 'events.commit'):
                state['revision'] += 1
                commits.append(payload)
                return {'commit_id': payload['commit_id'], 'revision': state['revision'],
                        'duplicate': False, 'operation_ids': []}
            if method == 'players.list':
                return {'session': 's1', 'players': []}
            if method == 'events.ack':
                commits.append({'delivery_token': payload['delivery_token'], 'ack_only': True})
                return {}
            raise AssertionError(f'unexpected call {method}')

        events = [
            {'kind': 'chat.received', 'payload': {'player': 'Steve', 'xuid': '2', 'message': '你好'},
             'subscription_id': 'chat', 'event_id': 'ev1', 'delivery_token': 'dt1'},
        ]

        class DriveWorker:
            def __init__(self, peer, **callbacks):
                self.peer, self.callbacks = peer, callbacks
                self.log = logging.getLogger('fatalder-serve-test')
                self.session = 'install-1'
                self._scopes = self._streams = None

            async def run(self):
                config = {
                    'worker_url': 'https://worker.example.com',
                    'api_key': 'secret:fatalder_api_key',
                    'target_server_id': 'example',
                    'rental_server_code': '123456',
                    'admin_uuids': ['00000000-0000-4000-8000-000000000001'],
                }
                await self.callbacks['on_validate'](self.peer, {'config': config})
                await self.callbacks['on_activate'](self.peer, {'config': config})
                for ev in events:
                    await self.callbacks['on_event'](self.peer, ev)
                await self.callbacks['on_drain'](self.peer, {})

            def spawn(self, coro, *, name=None):
                return asyncio.create_task(coro)

        peer = SimpleNamespace(call=call)
        config = {
            'worker_url': 'https://worker.example.com',
            'api_key': 'secret:fatalder_api_key',
            'target_server_id': 'example',
            'rental_server_code': '123456',
            'admin_uuids': ['00000000-0000-4000-8000-000000000001'],
        }
        with tempfile.TemporaryDirectory() as tmp, patch('neomega_runtime.plugin.Worker', DriveWorker):
            (Path(tmp) / 'config.json').write_text(json.dumps(config))
            await CloudImport().serve(peer, data_dir=tmp)
        self.assertEqual([c['delivery_token'] for c in commits], ['dt1'])
