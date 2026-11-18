"""Tests for neomega.moderation covering services, scheduler fallback, locking, and Plugin.serve()."""
import asyncio
import copy
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import PluginContext, ServiceRejected
from neomega_runtime.config import load_config
from neomega_runtime.config_schema import check_config_schema, schema_for_config
from neomega_runtime.managed import IPCRejected, ManagedPeer
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceCallContext
from datetime import datetime, timedelta, timezone

import examples
from main import MAX_EXPIRY_CLEANUP, Moderation, Settings, dedupe_cleanup, digest


PLUGIN_DIR = Path(__file__).resolve().parents[1]


def make_call(installation_id='admin-app', generation=1, call_id='c1'):
    now = datetime.now(timezone.utc)
    return ServiceCallContext(
        installation_id=installation_id,
        generation=generation,
        call_id=call_id,
        accepted_at=now,
        deadline=now + timedelta(seconds=10),
    )


class FakeHost:
    """In-memory Host supporting state, receipts, operations, players, and services."""
    def __init__(self):
        self.values = {}
        self.revision = 0
        self.commits = []
        self.receipts = {}
        self.operations = {}
        self.roster = []
        self.session = 's1'
        self.players_error = None
        self.receipt_hook = None
        self.tx_commit_ids = []

    async def call(self, method, payload, timeout=None):
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': self.session}
        if method == 'state.receipt':
            cid = payload['commit_id']
            if self.receipt_hook is not None:
                await self.receipt_hook(cid)
            return self.receipts.get(cid, {'commit_id': cid, 'found': False})
        if method == 'state.commit':
            assert payload['expected_state_revision'] == self.revision
            self.tx_commit_ids.append(payload['commit_id'])
            for mutation in payload['mutations']:
                if mutation.get('delete'):
                    self.values.pop(mutation['key'], None)
                else:
                    self.values[mutation['key']] = copy.deepcopy(mutation['value'])
            self.revision += 1
            self.commits.append(copy.deepcopy(payload))
            op_ids = []
            for idx, action in enumerate(payload['actions']):
                op_id = f'op-{self.revision}-{idx}'
                op_ids.append(op_id)
                self.operations[op_id] = {
                    'operation_id': op_id,
                    'state': 'succeeded',
                    'can_retry': False,
                    'result': {'state': 'succeeded'},
                    'action': action,
                }
            receipt = {
                'commit_id': payload['commit_id'],
                'found': True,
                'revision': self.revision,
                'duplicate': False,
                'operation_ids': op_ids,
            }
            self.receipts[payload['commit_id']] = receipt
            return copy.deepcopy(receipt)
        if method == 'operations.get':
            op_id = payload['operation_id']
            return copy.deepcopy(self.operations[op_id])
        if method == 'players.list':
            if self.players_error is not None:
                raise self.players_error
            return {'session': self.session, 'players': copy.deepcopy(self.roster)}
        if method == 'capabilities.get':
            return {'session': self.session, 'capabilities': []}
        raise AssertionError(f'unexpected RPC: {method}')


class FakeScheduler:
    """In-memory neomega.scheduler service provider."""
    def __init__(self):
        self.schedules = {}
        self.triggers = []
        self.requests = {}
        self.cancelled = []
        self.acked = []

    async def handle(self, service, arguments):
        if service == 'plugin.neomega.scheduler.query':
            sid = arguments['schedule_id']
            if sid in self.schedules:
                return {'status': 'found', 'schedule': copy.deepcopy(self.schedules[sid])}
            return {'status': 'not_found'}
        if service == 'plugin.neomega.scheduler.schedule':
            sid = arguments['schedule_id']
            self.schedules[sid] = {
                'schedule_id': sid,
                'due_at': arguments['due_at'],
                'payload': copy.deepcopy(arguments['payload']),
                'state': 'scheduled',
            }
            return {'status': 'scheduled', 'schedule_id': sid}
        if service == 'plugin.neomega.scheduler.cancel':
            sid = arguments['schedule_id']
            self.cancelled.append(sid)
            if sid in self.schedules:
                self.schedules[sid]['state'] = 'cancelled'
            return {'status': 'cancelled', 'schedule_id': sid}
        if service == 'plugin.neomega.scheduler.claim':
            return {'triggers': copy.deepcopy(self.triggers)}
        if service == 'plugin.neomega.scheduler.ack':
            self.acked.append(arguments['trigger_id'])
            sid = arguments['schedule_id']
            if sid in self.schedules:
                self.schedules[sid]['state'] = 'completed'
            self.triggers = [t for t in self.triggers if t['trigger_id'] != arguments['trigger_id']]
            return {'status': 'acked'}
        raise AssertionError(f'unexpected scheduler call: {service}')


class ModerationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)
        self.host = FakeHost()
        self.tasks = []
        self.every_calls = []
        self.scheduler = FakeScheduler()
        self.scheduler_installed = True
        self.players_archive = {}
        self.resolve_hook = None
        self.scheduler_hook = None
        self.clock = 1000.0

        self.worker = SimpleNamespace(
            peer=self.host,
            log=logging.getLogger('moderation-test'),
            spawn=lambda coro, name=None: self._track_task(coro),
        )
        self.ctx = PluginContext(self.worker, self.data_dir)
        self.ctx.config = Settings(writer_installations=['admin-app'])
        self.ctx._operations = Operations(self.host, self.host.session)
        self.ctx.services.call = AsyncMock(side_effect=self._service_call)
        orig_every = self.ctx.every

        def capture_every(seconds, callback, *, immediate=False, name=None):
            self.every_calls.append((seconds, immediate, name))
            return orig_every(seconds, callback, immediate=immediate, name=name)

        self.ctx.every = capture_every
        self.plugin = Moderation()
        self.plugin.now = lambda: self.clock
        await self.plugin.initialize(self.ctx)

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tmp.cleanup()

    def _track_task(self, coro):
        task = asyncio.ensure_future(coro)
        self.tasks.append(task)
        return task

    async def _service_call(self, service, arguments, *, major=1, timeout=5, idempotency_key=None):
        if service == 'plugin.neomega.players.resolve':
            if self.resolve_hook is not None:
                await self.resolve_hook(arguments)
            for profile in self.players_archive.values():
                if any(arguments.get(k) == profile.get(k) and profile.get(k) for k in ('name', 'uuid', 'xuid')):
                    return copy.deepcopy(profile)
            raise ServiceRejected('not_found')
        if service.startswith('plugin.neomega.scheduler.'):
            if self.scheduler_hook is not None:
                await self.scheduler_hook(service, arguments)
            if not self.scheduler_installed:
                raise IPCRejected('permission_denied', 'r1', False)
            return await self.scheduler.handle(service, arguments)
        raise AssertionError(f'unexpected service: {service}')

    async def test_metadata_and_config_alignment(self):
        manifest = json.loads((PLUGIN_DIR / 'manifest.json').read_text())
        release = json.loads((PLUGIN_DIR / 'release.json').read_text())
        schema = json.loads((PLUGIN_DIR / 'config.schema.json').read_text())
        self.assertEqual(manifest['version'], '1.0.1')
        self.assertEqual(release['kind'], 'provider')
        deps = {d['id']: d for d in manifest['dependencies']}
        self.assertEqual(deps['neomega.players'], {'id': 'neomega.players', 'major': 2, 'min_version': '2.0.0'})
        self.assertEqual(deps['neomega.scheduler'], {'id': 'neomega.scheduler', 'major': 1, 'min_version': '1.0.0', 'optional': True})
        check_config_schema(schema)
        self.assertEqual(schema_for_config(Settings), schema)
        self.assertEqual(load_config(Settings, PLUGIN_DIR / 'config.example.json'), Settings())

    async def test_ban_unban_check_list_and_validation(self):
        call = make_call()
        res = await self.plugin.ban(self.ctx, {
            'request_id': 'req-1',
            'player_id': 'uuid:u1',
            'expected_revision': 0,
            'reason': 'griefing',
        }, call)
        self.assertEqual(res['status'], 'saved')
        self.assertTrue(res['active'])
        self.assertEqual(res['ban']['revision'], 1)

        # Idempotent replay returns exact cached result; different args reject.
        replay = await self.plugin.ban(self.ctx, {
            'request_id': 'req-1',
            'player_id': 'uuid:u1',
            'expected_revision': 0,
            'reason': 'griefing',
        }, call)
        self.assertEqual(replay, res)
        with self.assertRaises(ServiceRejected) as ctx_err:
            await self.plugin.ban(self.ctx, {
                'request_id': 'req-1',
                'player_id': 'uuid:u1',
                'expected_revision': 0,
                'reason': 'other',
            }, call)
        self.assertEqual(ctx_err.exception.code, 'idempotency_conflict')

        # Check & list
        chk = await self.plugin.check(self.ctx, {'player_id': 'uuid:u1'}, call)
        self.assertTrue(chk['active'])
        self.assertEqual(chk['revision'], 1)
        listed = await self.plugin.list_bans(self.ctx, {}, call)
        self.assertEqual(len(listed['bans']), 1)
        self.assertIsNone(listed['next_offset'])

        # Unban
        unbanned = await self.plugin.unban(self.ctx, {
            'request_id': 'req-2',
            'player_id': 'uuid:u1',
            'expected_revision': 1,
            'reason': 'appealed',
        }, call)
        self.assertFalse(unbanned['active'])
        self.assertEqual(unbanned['ban']['revision'], 2)

        # Invalid arguments on all @service handlers raise ServiceRejected cleanly
        for bad in (None, [], 'str', {}, {'player_id': ''}, {'player_id': 'name:Steve'}, {'player_id': 'uuid:u1', 'extra': 1}):
            with self.subTest(check_bad=bad):
                with self.assertRaises(ServiceRejected) as caught:
                    await self.plugin.check(self.ctx, bad, call)
                self.assertEqual(caught.exception.code, 'invalid_argument')
        for bad_list in (None, [], {'offset': 0, 'extra': 1}):
            with self.assertRaises(ServiceRejected) as caught:
                await self.plugin.list_bans(self.ctx, bad_list, call)
            self.assertEqual(caught.exception.code, 'invalid_argument')
        with self.assertRaises(ServiceRejected) as caught:
            await self.plugin.list_bans(self.ctx, {'offset': -1}, call)
        self.assertEqual(caught.exception.code, 'invalid_offset')
        for bad_ban in (None, [], {'request_id': 'r1', 'player_id': 'uuid:u1', 'expected_revision': -1},
                        {'request_id': 'r1', 'player_id': 'uuid:u1', 'expected_revision': '0'},
                        {'request_id': 'r1', 'player_id': 'uuid:u1', 'expected_revision': 0, 'extra': 1}):
            with self.subTest(ban_bad=bad_ban):
                with self.assertRaises(ServiceRejected) as caught:
                    await self.plugin.ban(self.ctx, bad_ban, call)
                self.assertEqual(caught.exception.code, 'invalid_argument')

        # examples.py integration
        example_ctx = SimpleNamespace(services=SimpleNamespace(call=AsyncMock()))
        async def example_dispatch(service_name, arguments):
            member = service_name.rsplit('.', 1)[-1]
            fn = {'check': self.plugin.check, 'ban': self.plugin.ban, 'unban': self.plugin.unban}[member]
            return await fn(self.ctx, arguments, call)
        example_ctx.services.call.side_effect = example_dispatch
        ex_ban = await examples.ban_player(example_ctx, 'xuid:99', 'ex-1')
        self.assertTrue(ex_ban['active'])
        ex_unban = await examples.unban_player(example_ctx, 'xuid:99', 'ex-2')
        self.assertFalse(ex_unban['active'])

    async def test_players_resolve_flat_contract_and_not_found(self):
        call = make_call()
        await self.plugin.ban(self.ctx, {
            'request_id': 'r1',
            'player_id': 'uuid:u1',
            'expected_revision': 0,
            'reason': 'cheating',
        }, call)
        self.host.roster = [{'name': 'Steve', 'uuid': 'u1', 'xuid': '42'}]

        # Before players plugin has recorded Steve, players.resolve rejects with not_found: no kick!
        await self.plugin.sweep(self.ctx)
        row = self.host.values['ban:' + digest('uuid:u1')]
        self.assertFalse(row['present'])
        self.assertEqual(row['enforcement_sequence'], 0)

        # Once players plugin returns flat dict, kick executes and enforcement reconciles!
        self.players_archive['u1'] = {
            'id': 'u1',
            'name': 'Steve',
            'uuid': 'u1',
            'xuid': '42',
            'names': [],
            'last_seen': '2026-11-18T00:00:00+00:00',
        }
        await self.plugin.sweep(self.ctx)
        row = self.host.values['ban:' + digest('uuid:u1')]
        self.assertTrue(row['present'])
        self.assertEqual(row['enforcement_sequence'], 1)
        self.assertEqual(row['enforcement']['state'], 'succeeded')
        kick_commit = next(c for c in self.host.commits if c['actions'])
        self.assertEqual(kick_commit['commit_id'], row['enforcement']['commit_id'])
        self.assertEqual(kick_commit['actions'][0]['arguments']['cmd'], 'kick @a[name="Steve"] "cheating"')

        # Player leaves -> present resets to False; rejoins -> new enforcement executes.
        self.host.roster = []
        await self.plugin.sweep(self.ctx)
        row = self.host.values['ban:' + digest('uuid:u1')]
        self.assertFalse(row['present'])

        self.host.roster = [{'name': 'Steve', 'uuid': 'u1', 'xuid': '42'}]
        await self.plugin.sweep(self.ctx)
        row = self.host.values['ban:' + digest('uuid:u1')]
        self.assertTrue(row['present'])
        self.assertEqual(row['enforcement_sequence'], 2)
        self.assertEqual(row['enforcement']['state'], 'succeeded')

    async def test_absent_scheduler_fallback_via_ctx_every(self):
        self.scheduler_installed = False
        self.ctx.config = Settings(writer_installations=['admin-app'], enforce=False, poll_seconds=1)
        call = make_call()
        expires = datetime.fromtimestamp(self.clock + 10, timezone.utc).isoformat()
        await self.plugin.ban(self.ctx, {
            'request_id': 'r1',
            'player_id': 'uuid:u1',
            'expected_revision': 0,
            'expires_at': expires,
        }, call)

        await self.plugin.on_start(self.ctx)
        self.assertEqual(self.every_calls, [(1, True, 'moderation-maintenance')])
        # Wait briefly for immediate=True sweep to complete.
        await asyncio.sleep(0.05)
        row = self.host.values['ban:' + digest('uuid:u1')]
        self.assertEqual(row['state'], 'banned')
        self.assertFalse(row['expiry_requested'])
        self.assertEqual(row['expiry_cleanup'], [])

        # Advance clock past expiry and run sweep (fallback periodic sweep expires timed ban).
        self.clock += 20
        await self.plugin.sweep(self.ctx)
        row = self.host.values['ban:' + digest('uuid:u1')]
        self.assertEqual(row['state'], 'expired')
        self.assertEqual(row['expiry_cleanup'], [])
        chk = await self.plugin.check(self.ctx, {'player_id': 'uuid:u1'}, call)
        self.assertFalse(chk['active'])

    async def test_absent_scheduler_service_rejected_fallback(self):
        # Also verify ServiceRejected('unavailable') from scheduler is handled identically.
        async def reject_scheduler(service, arguments):
            raise ServiceRejected('unavailable')
        self.scheduler_hook = reject_scheduler
        self.ctx.config = Settings(writer_installations=['admin-app'], enforce=False)
        call = make_call()
        expires = datetime.fromtimestamp(self.clock + 5, timezone.utc).isoformat()
        await self.plugin.ban(self.ctx, {
            'request_id': 'r1',
            'player_id': 'xuid:77',
            'expected_revision': 0,
            'expires_at': expires,
        }, call)
        await self.plugin.sweep(self.ctx)
        self.clock += 10
        await self.plugin.sweep(self.ctx)
        row = self.host.values['ban:' + digest('xuid:77')]
        self.assertEqual(row['state'], 'expired')
        self.assertEqual(row['expiry_cleanup'], [])

    async def test_present_scheduler_schedules_claims_acks_and_cancels(self):
        self.ctx.config = Settings(writer_installations=['admin-app'], enforce=False)
        call = make_call()
        expires = datetime.fromtimestamp(self.clock + 30, timezone.utc).isoformat()
        ban_res = await self.plugin.ban(self.ctx, {
            'request_id': 'r1',
            'player_id': 'uuid:u1',
            'expected_revision': 0,
            'expires_at': expires,
        }, call)
        sid = self.plugin.schedule_id(ban_res['ban'])
        await self.plugin.sweep(self.ctx)
        row = self.host.values['ban:' + digest('uuid:u1')]
        self.assertTrue(row['expiry_requested'])
        self.assertTrue(row['expiry_scheduled'])
        self.assertIn(sid, self.scheduler.schedules)

        # Unban enqueues cleanup and next sweep cancels the old schedule cleanly.
        await self.plugin.unban(self.ctx, {
            'request_id': 'r2',
            'player_id': 'uuid:u1',
            'expected_revision': 1,
        }, call)
        await self.plugin.sweep(self.ctx)
        self.assertIn(sid, self.scheduler.cancelled)
        row = self.host.values['ban:' + digest('uuid:u1')]
        self.assertEqual(row['expiry_cleanup'], [])

        # Re-ban, schedule, then trigger claim & ack.
        expires2 = datetime.fromtimestamp(self.clock + 15, timezone.utc).isoformat()
        ban2 = await self.plugin.ban(self.ctx, {
            'request_id': 'r3',
            'player_id': 'uuid:u1',
            'expected_revision': 2,
            'expires_at': expires2,
        }, call)
        sid2 = self.plugin.schedule_id(ban2['ban'])
        await self.plugin.sweep(self.ctx)
        self.clock += 20
        self.scheduler.triggers.append({
            'trigger_id': 'trig-1',
            'schedule_id': sid2,
            'token': 'tok-1',
            'payload': {'player_id': 'uuid:u1', 'revision': 3},
        })
        await self.plugin.consume_expiry(self.ctx)
        self.assertEqual(self.host.values['ban:' + digest('uuid:u1')]['state'], 'expired')
        self.assertEqual(self.scheduler.acked, ['trig-1'])

    async def test_sweep_releases_lock_across_rpcs_and_prevents_clobber(self):
        call = make_call()
        await self.plugin.ban(self.ctx, {
            'request_id': 'r1',
            'player_id': 'uuid:u1',
            'expected_revision': 0,
            'reason': 'ban',
        }, call)
        self.host.roster = [{'name': 'Steve', 'uuid': 'u1', 'xuid': '42'}]
        self.players_archive['u1'] = {
            'id': 'u1', 'name': 'Steve', 'uuid': 'u1', 'xuid': '42', 'names': [], 'last_seen': 'now',
        }

        entered_rpc = asyncio.Event()
        resume_rpc = asyncio.Event()

        async def slow_resolve(arguments):
            entered_rpc.set()
            await resume_rpc.wait()

        self.resolve_hook = slow_resolve
        sweep_task = asyncio.create_task(self.plugin.sweep(self.ctx))
        await asyncio.wait_for(entered_rpc.wait(), 1.0)

        # Lock MUST NOT be held while awaiting players.resolve!
        self.assertFalse(self.plugin.lock.locked())
        # Concurrent unban succeeds immediately while sweep is in-flight, and sweep won't clobber it!
        await asyncio.wait_for(self.plugin.unban(self.ctx, {
            'request_id': 'r2',
            'player_id': 'uuid:u1',
            'expected_revision': 1,
        }, call), 1.0)

        resume_rpc.set()
        await asyncio.wait_for(sweep_task, 1.0)
        row = self.host.values['ban:' + digest('uuid:u1')]
        self.assertEqual(row['state'], 'unbanned')
        self.assertEqual(row['revision'], 2)
        self.assertEqual(row['enforcement_sequence'], 0)

    async def test_cleanup_expiry_none_and_dedupe_bound(self):
        # None row must not crash cleanup_expiry or sweep_player.
        self.assertTrue(await self.plugin.cleanup_expiry(self.ctx, 'ban:missing', None))
        self.assertTrue(await self.plugin.sweep_player(self.ctx, 'ban:missing', []))

        # Row disappears before cleanup_expiry commits removal -> returns True cleanly.
        key = 'ban:' + digest('uuid:u1')
        ghost_row = {'player_id': 'uuid:u1', 'state': 'unbanned', 'revision': 1,
                     'expiry_cleanup': [{'schedule_id': 'ban-1', 'requested': False}]}
        self.assertTrue(await self.plugin.cleanup_expiry(self.ctx, key, ghost_row))

        # Deduplication and bounding of expiry_cleanup when scheduler keeps returning not_found.
        call = make_call()
        items = [{'schedule_id': f'ban-{i}', 'requested': True} for i in range(MAX_EXPIRY_CLEANUP + 10)]
        items.append({'schedule_id': 'ban-20', 'requested': False})
        deduped = dedupe_cleanup(items)
        self.assertEqual(len(deduped), MAX_EXPIRY_CLEANUP)
        self.assertEqual(len({d['schedule_id'] for d in deduped}), MAX_EXPIRY_CLEANUP)

        # Simulate scheduler down after expiry_requested=True across many ban/unban cycles.
        rev = 0
        for i in range(MAX_EXPIRY_CLEANUP + 5):
            expires = datetime.fromtimestamp(self.clock + 100 + i, timezone.utc).isoformat()
            await self.plugin.ban(self.ctx, {
                'request_id': f'b-{i}',
                'player_id': 'uuid:u2',
                'expected_revision': rev,
                'expires_at': expires,
            }, call)
            rev += 1
            # Mark as requested so enqueue_cleanup records it.
            self.host.values['ban:' + digest('uuid:u2')]['expiry_requested'] = True
            await self.plugin.unban(self.ctx, {
                'request_id': f'u-{i}',
                'player_id': 'uuid:u2',
                'expected_revision': rev,
            }, call)
            rev += 1
        row = self.host.values['ban:' + digest('uuid:u2')]
        self.assertEqual(len(row['expiry_cleanup']), MAX_EXPIRY_CLEANUP)

    async def test_initialize_unresolved_commit_checks_and_defers_schema_version(self):
        # Case 1: unresolved commit + unsupported schema_version=2 still raises!
        pending_file = self.data_dir / 'moderation-pending.json'
        pending_file.write_text(json.dumps({'commit_id': 'c-unresolved'}))
        self.host.values['schema_version'] = 2
        p1 = Moderation()
        with self.assertRaisesRegex(RuntimeError, 'unsupported schema_version'):
            await p1.initialize(self.ctx)
        self.assertTrue(pending_file.exists())

        # Case 2: unresolved commit + schema_version=None keeps pending barrier intact,
        # then initializes schema_version=1 once receipt resolves.
        self.host.values.pop('schema_version')
        p2 = Moderation()
        await p2.initialize(self.ctx)
        self.assertFalse(p2.schema_ready)
        self.assertEqual(p2.pending, 'c-unresolved')
        self.assertTrue(pending_file.exists())
        self.assertEqual((await p2.on_maintenance(self.ctx, {'operation': 'seal', 'token': 't1'}))['status'], 'busy')
        with self.assertRaises(ServiceRejected) as caught:
            await p2.ban(self.ctx, {'request_id': 'r1', 'player_id': 'uuid:u1', 'expected_revision': 0}, make_call())
        self.assertEqual(caught.exception.code, 'commit_unresolved')

        # Receipt arrives -> next sweep reconciles and writes schema_version=1.
        self.host.receipts['c-unresolved'] = {'commit_id': 'c-unresolved', 'found': True, 'revision': 0, 'operation_ids': []}
        await p2.sweep(self.ctx)
        self.assertTrue(p2.schema_ready)
        self.assertIsNone(p2.pending)
        self.assertFalse(pending_file.exists())
        self.assertEqual(self.host.values['schema_version'], 1)


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


class ModerationServeTests(unittest.IsolatedAsyncioTestCase):
    async def test_serve_lifecycle_services_maintenance_and_invalid_argument(self):
        host = FakeHost()
        reader = asyncio.StreamReader(limit=1 << 20)
        writer = ServeWriter()
        peer = ManagedPeer(reader, writer)
        plugin = Moderation()
        plugin.now = lambda: 1000.0

        def send(frame):
            reader.feed_data((json.dumps(frame) + '\n').encode())

        async def wait_response(req_id):
            while True:
                msg = await asyncio.wait_for(writer.responses.get(), 2)
                if msg.get('id') == req_id:
                    return msg

        async def pump():
            while True:
                frame = await writer.requests.get()
                req_id, method, payload = frame['id'], frame['method'], frame['payload']
                if method == 'hello':
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {
                        'protocol_major': 1, 'protocol_minor': 0, 'installation_id': 'mod-1', 'generation': 1,
                        'capabilities': [], 'limits': {
                            'max_frame_bytes': 1 << 20, 'outgoing_requests': 128,
                            'inbound_requests': 32, 'queue_frames': 32}}}})
                elif method == 'capabilities.get':
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {'session': 's1', 'capabilities': []}}})
                elif method == 'players.list':
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {'session': 's1', 'players': []}}})
                elif method == 'services.online.submit':
                    send({'type': 'response', 'id': req_id, 'payload': {'error': {
                        'code': 'permission_denied', 'message': 'scheduler absent', 'retryable': False, 'request_id': req_id}}})
                elif method in ('state.get', 'state.commit', 'state.receipt', 'operations.get'):
                    res = await host.call(method, payload)
                    send({'type': 'response', 'id': req_id, 'payload': {'result': res}})

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / 'config.json').write_text(json.dumps({
                'writer_installations': ['caller-1'],
                'enforce': False,
                'poll_seconds': 5,
                'max_players': 256,
            }))
            serve_task = asyncio.create_task(plugin.serve(peer, data_dir=tmp_path))
            pump_task = asyncio.create_task(pump())
            try:
                send({'type': 'request', 'id': 'v1', 'method': 'lifecycle.validate',
                      'payload': {'installation_id': 'mod-1', 'generation': 1, 'revision': 1}})
                self.assertIn('result', (await wait_response('v1'))['payload'])

                send({'type': 'request', 'id': 'a1', 'method': 'lifecycle.activate',
                      'payload': {'installation_id': 'mod-1', 'generation': 1, 'revision': 1, 'state_version': 1}})
                self.assertTrue((await wait_response('a1'))['payload']['result']['activated'])

                now_iso = datetime.now(timezone.utc).isoformat()
                deadline_iso = (datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat()

                def call_ctx(call_id):
                    return {
                        'installation_id': 'caller-1',
                        'generation': 1,
                        'call_id': call_id,
                        'accepted_at': now_iso,
                        'deadline': deadline_iso,
                    }

                # Valid ban
                send({'type': 'request', 'id': 's1', 'method': 'services.online.invoke',
                      'payload': {'service': 'plugin.neomega.moderation.ban', 'major': 1, 'call_id': 'c1',
                                  'call_context': call_ctx('c1'),
                                  'arguments': {'request_id': 'r1', 'player_id': 'uuid:u1', 'expected_revision': 0}}})
                res1 = await wait_response('s1')
                self.assertEqual(res1['payload']['result']['status'], 'saved')
                self.assertTrue(res1['payload']['result']['active'])

                # Invalid argument shape on bus=False must return service_rejected, NOT crash serve_task!
                send({'type': 'request', 'id': 's2', 'method': 'services.online.invoke',
                      'payload': {'service': 'plugin.neomega.moderation.check', 'major': 1, 'call_id': 'c2',
                                  'call_context': call_ctx('c2'),
                                  'arguments': ['not-a-dict']}})
                res2 = await wait_response('s2')
                self.assertEqual(res2['payload']['error']['code'], 'service_rejected')
                self.assertEqual(res2['payload']['error']['details']['code'], 'invalid_argument')
                self.assertFalse(serve_task.done())

                # Maintenance seal & release
                send({'type': 'request', 'id': 'm1', 'method': 'lifecycle.maintenance',
                      'payload': {'installation_id': 'mod-1', 'generation': 1, 'operation': 'seal', 'token': 'tok-1'}})
                self.assertEqual((await wait_response('m1'))['payload']['result']['status'], 'sealed')
                send({'type': 'request', 'id': 'm2', 'method': 'lifecycle.maintenance',
                      'payload': {'installation_id': 'mod-1', 'generation': 1, 'operation': 'release', 'token': 'tok-1'}})
                self.assertEqual((await wait_response('m2'))['payload']['result']['status'], 'released')

                # Clean drain
                send({'type': 'request', 'id': 'dr1', 'method': 'lifecycle.drain',
                      'payload': {'installation_id': 'mod-1', 'generation': 1}})
                self.assertTrue((await wait_response('dr1'))['payload']['result']['drained'])
                self.assertFalse(serve_task.done())
            finally:
                serve_task.cancel()
                pump_task.cancel()
                await asyncio.gather(serve_task, pump_task, return_exceptions=True)


if __name__ == '__main__':
    unittest.main()
