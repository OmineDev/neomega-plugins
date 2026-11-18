"""Comprehensive tests for worldtools through real SDK Plugin.serve() and units."""
import asyncio
import copy
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from neomega_runtime import PluginContext, ServiceRejected
from neomega_runtime.config import load_config
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config_schema import check_config_schema, validate_config
from neomega_runtime.managed import IPCError, IPCRejected, ManagedPeer

from main import Settings, WorldTools

PLUGIN_DIR = Path(__file__).resolve().parents[1]


def call_ctx(installation_id='caller-1', generation=1, call_id='c1'):
    now = datetime.now(timezone.utc)
    return {
        'installation_id': installation_id,
        'generation': generation,
        'call_id': call_id,
        'accepted_at': now.isoformat(),
        'deadline': (now + timedelta(seconds=25)).isoformat(),
    }


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


class FakeWorldHost:
    def __init__(self):
        self.revision = 0
        self.values = {}
        self.commits = {}
        self.operations = {}
        self.retained = []
        self.services = {}
        self.world_epoch = 'epoch-1'
        self.session = 'sess-1'
        self.op_seq = 0
        self.svc_seq = 0
        self.commit_gate = None
        self.commit_error = None
        self.retain_error = None
        self.capabilities_error = None
        self.svc_wait_gate = None
        self.svc_submit_error = None
        self.svc_state = 'replied'
        self.svc_result = None

    async def handle(self, method, payload, req_id):
        if method == 'hello':
            return {'result': {
                'protocol_major': 1, 'protocol_minor': 0,
                'installation_id': 'inst-worldtools', 'generation': 1,
                'capabilities': [],
                'limits': {
                    'max_frame_bytes': 1 << 20,
                    'outgoing_requests': 128,
                    'inbound_requests': 32,
                    'queue_frames': 32,
                },
            }}
        if method == 'capabilities.get':
            if self.capabilities_error is not None:
                return {'error': dict(self.capabilities_error, request_id=req_id)}
            data = {'world_epoch': self.world_epoch} if self.world_epoch is not None else {}
            return {'result': {'session': self.session, 'data': data}}
        if method == 'state.get':
            keys = payload.get('keys')
            vals = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'result': {'revision': self.revision, 'values': copy.deepcopy(vals), 'session': self.session}}
        if method == 'state.receipt':
            cid = payload['commit_id']
            row = self.commits.get(cid)
            if row is None:
                return {'result': {'commit_id': cid, 'found': False}}
            return {'result': {'commit_id': cid, 'found': True, 'revision': row['revision'], 'operation_ids': row['operation_ids']}}
        if method == 'state.commit':
            if self.commit_gate is not None:
                await self.commit_gate.wait()
            if self.commit_error is not None:
                err = self.commit_error
                if callable(err):
                    err = err(payload)
                if err is not None:
                    return {'error': dict(err, request_id=req_id)}
            cid = payload['commit_id']
            if cid in self.commits:
                prev = self.commits[cid]
                return {'result': {'commit_id': cid, 'revision': prev['revision'], 'duplicate': True, 'operation_ids': prev['operation_ids']}}
            if payload['expected_state_revision'] != self.revision:
                return {'error': {'code': 'revision_conflict', 'message': 'stale', 'retryable': True, 'request_id': req_id}}
            for mut in payload['mutations']:
                if mut.get('delete'):
                    self.values.pop(mut['key'], None)
                else:
                    self.values[mut['key']] = copy.deepcopy(mut['value'])
            op_ids = []
            for action in payload['actions']:
                self.op_seq += 1
                op_id = f'op-{self.op_seq}'
                op_ids.append(op_id)
                self.operations[op_id] = self._run_action(op_id, action)
            self.revision += 1
            self.commits[cid] = {'revision': self.revision, 'operation_ids': op_ids}
            return {'result': {'commit_id': cid, 'revision': self.revision, 'duplicate': False, 'operation_ids': op_ids}}
        if method == 'operations.get':
            return {'result': copy.deepcopy(self.operations[payload['operation_id']])}
        if method == 'artifacts.retain':
            if self.retain_error is not None:
                return {'error': dict(self.retain_error, request_id=req_id)}
            self.retained.append((payload['artifact_id'], payload['idempotency_key']))
            return {'result': {
                'artifact_id': payload['artifact_id'],
                'size': 16,
                'sha256': 'a' * 64,
                'received': 16,
                'complete': True,
            }}
        if method == 'services.online.submit':
            if self.svc_submit_error is not None:
                return {'error': dict(self.svc_submit_error, request_id=req_id)}
            self.svc_seq += 1
            call_id = f'svc-{self.svc_seq}'
            self.services[call_id] = {
                'call_id': call_id,
                'state': 'pending',
                'error': '',
                'result': None,
                'payload': payload,
            }
            return {'result': {'call_id': call_id}}
        if method == 'services.online.get':
            if self.svc_wait_gate is not None:
                await self.svc_wait_gate.wait()
            row = self.services[payload['call_id']]
            if row['state'] == 'pending':
                res = self.svc_result(row['payload']) if callable(self.svc_result) else self.svc_result
                row['state'] = self.svc_state
                row['result'] = res
            return {'result': {k: v for k, v in row.items() if k != 'payload'}}
        raise AssertionError(f'unexpected host method: {method}')

    def _run_action(self, op_id, action):
        op = action['operation']
        args = action['arguments']
        if op == 'framework.world.block.read':
            return {
                'request_id': 'r',
                'operation_id': op_id,
                'state': 'succeeded',
                'can_retry': False,
                'expires_at': '2030-01-01T00:00:00Z',
                'result': {
                    'state': 'succeeded',
                    'evidence': 'verified',
                    'result': {'abs': args['pos'], 'foreground': {'name': 'minecraft:stone', 'states': {}}},
                },
            }
        if op == 'framework.world.region.read':
            ox, oy, oz = args['pos']
            sx, sy, sz = args['size']
            blocks = []
            for x in range(ox, ox + sx):
                for y in range(oy, oy + sy):
                    for z in range(oz, oz + sz):
                        blocks.append(copy.deepcopy(self._block_for(x, y, z)))
            return {
                'request_id': 'r',
                'operation_id': op_id,
                'state': 'succeeded',
                'can_retry': False,
                'expires_at': '2030-01-01T00:00:00Z',
                'result': {
                    'state': 'succeeded',
                    'evidence': 'verified',
                    'result': {'pos': [ox, oy, oz], 'size': [sx, sy, sz], 'blocks': blocks},
                },
            }
        if op == 'framework.world.bdx.export':
            return {
                'request_id': 'r',
                'operation_id': op_id,
                'state': 'succeeded',
                'can_retry': False,
                'expires_at': '2030-01-01T00:00:00Z',
                'result': {
                    'state': 'succeeded',
                    'evidence': 'verified',
                    'result': {
                        'artifact': {'artifact_id': 'art-123', 'size': 16, 'sha256': 'a' * 64},
                        'pos': args['pos'],
                        'size': args['size'],
                    },
                },
            }
        return {
            'request_id': 'r',
            'operation_id': op_id,
            'state': 'succeeded',
            'can_retry': False,
            'expires_at': '2030-01-01T00:00:00Z',
            'result': {'state': 'succeeded', 'evidence': 'verified', 'result': {'ok': True, 'args': args}},
        }

    def _block_for(self, x, y, z):
        if hasattr(self, 'world_blocks') and (x, y, z) in self.world_blocks:
            return self.world_blocks[(x, y, z)]
        return {'abs': [x, y, z], 'foreground': {'name': 'minecraft:stone', 'states': {}}}


class ConfigAndManifestTests(unittest.TestCase):
    def test_config_schema_and_example_align_with_settings(self):
        schema = json.loads((PLUGIN_DIR / 'config.schema.json').read_text())
        check_config_schema(schema)
        example = json.loads((PLUGIN_DIR / 'config.example.json').read_text())
        validated = validate_config(schema, example)
        self.assertEqual(validated, example)
        loaded = load_config(Settings, PLUGIN_DIR / 'config.example.json')
        self.assertEqual(loaded, Settings())

        bounds = {
            'max_tasks': (1, 4096),
            'max_blocks': (1, 1048576),
            'max_steps': (1, 1024),
            'max_state_bytes': (1048576, 268435456),
        }
        for key, (low, high) in bounds.items():
            validate_config(schema, {key: low})
            validate_config(schema, {key: high})
            Settings(**{key: low})
            Settings(**{key: high})
            for bad in (low - 1, high + 1, True):
                with self.subTest(key=key, bad=bad):
                    with self.assertRaises(ConfigError):
                        validate_config(schema, {key: bad})
                    with self.assertRaises(ValueError):
                        Settings(**{key: bad})

    def test_manifest_and_release_metadata(self):
        manifest = json.loads((PLUGIN_DIR / 'manifest.json').read_text())
        release = json.loads((PLUGIN_DIR / 'release.json').read_text())
        self.assertEqual(manifest['version'], '1.0.1')
        self.assertEqual(release['kind'], 'provider')
        self.assertNotIn('library.lock.json', release['files'])
        allowed_keys = {
            'type', 'properties', 'required', 'additionalProperties',
            'items', 'enum', 'pattern', 'minimum', 'maximum',
        }

        def check_runtime_schema(node):
            self.assertIsInstance(node, dict)
            self.assertEqual(set(node) - allowed_keys, set())
            self.assertIn(node.get('type'), ('object', 'array', 'string', 'number', 'integer', 'boolean', 'null'))
            for prop in node.get('properties', {}).values():
                check_runtime_schema(prop)
            if 'items' in node:
                check_runtime_schema(node['items'])

        for export in manifest['exports']:
            self.assertIn('required', export['input_schema'])
            self.assertIn('required', export['output_schema'])
            check_runtime_schema(export['input_schema'])
            check_runtime_schema(export['output_schema'])


class WorldToolsServeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.host = FakeWorldHost()
        self.reader = asyncio.StreamReader(limit=1 << 20)
        self.writer = ServeWriter()
        self.peer = ManagedPeer(self.reader, self.writer)
        self.plugin = WorldTools()
        self.seq = 0

        async def dispatch(frame):
            reply = await self.host.handle(frame['method'], frame['payload'], frame['id'])
            self._send({'type': 'response', 'id': frame['id'], 'payload': reply})

        async def pump():
            in_flight = set()
            try:
                while True:
                    frame = await self.writer.requests.get()
                    task = asyncio.create_task(dispatch(frame))
                    in_flight.add(task)
                    task.add_done_callback(in_flight.discard)
            finally:
                for task in list(in_flight):
                    task.cancel()
                if in_flight:
                    await asyncio.gather(*in_flight, return_exceptions=True)

        self.serve_task = asyncio.create_task(self.plugin.serve(self.peer, data_dir=Path(self.tmp.name)))
        self.pump_task = asyncio.create_task(pump())
        self.assertIn('result', (await self._rpc('lifecycle.validate', {
            'installation_id': 'inst-worldtools', 'generation': 1, 'revision': 1,
        }))['payload'])
        self.assertTrue((await self._rpc('lifecycle.activate', {
            'installation_id': 'inst-worldtools', 'generation': 1, 'revision': 1, 'state_version': 1,
        }))['payload']['result']['activated'])

    async def asyncTearDown(self):
        self.serve_task.cancel()
        self.pump_task.cancel()
        await asyncio.gather(self.serve_task, self.pump_task, return_exceptions=True)
        self.tmp.cleanup()

    def _send(self, frame):
        self.reader.feed_data((json.dumps(frame) + '\n').encode())

    async def _rpc(self, method, payload):
        self.seq += 1
        req_id = f'req-{self.seq}'
        self._send({'type': 'request', 'id': req_id, 'method': method, 'payload': payload})
        while True:
            msg = await asyncio.wait_for(self.writer.responses.get(), 3)
            if msg.get('id') == req_id:
                return msg

    async def call_service(self, name, arguments):
        self.seq += 1
        cid = f'call-{self.seq}'
        res = await self._rpc('services.online.invoke', {
            'service': f'plugin.neomega.worldtools.{name}',
            'major': 1,
            'call_id': cid,
            'call_context': call_ctx(call_id=cid),
            'arguments': arguments,
        })
        return res['payload']

    async def call_rejected(self, name, arguments):
        payload = await self.call_service(name, arguments)
        self.assertEqual(payload['error']['code'], 'service_rejected', payload)
        return payload['error']['details']['code']

    async def drive_until_done(self, task_id, limit=20):
        for _ in range(limit):
            await self.plugin.tick(self.plugin_ctx_proxy())
            res = (await self.call_service('status', {'task_id': task_id}))['result']
            if res['state'] not in ('running', 'cancelling'):
                return res
        return res

    def plugin_ctx_proxy(self):
        # Use a lightweight proxy only if driving tick synchronously; the background every() loop also ticks.
        raise NotImplementedError

    async def wait_task_done(self, task_id, timeout=5.0):
        end = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < end:
            res = (await self.call_service('status', {'task_id': task_id}))['result']
            if res['state'] not in ('running', 'cancelling'):
                return res
            await asyncio.sleep(0.05)
        self.fail(f'task {task_id} did not finish in time: {res}')

    async def test_inspect_retains_dimension_and_executes(self):
        created = (await self.call_service('inspect', {
            'request_key': 'insp-1', 'pos': [1, 64, 2], 'dimension': 1,
        }))['result']
        self.assertEqual(created['dimension'], 1)
        self.assertEqual(created['steps'][0]['args'], {'pos': [1, 64, 2], 'dimension': 1})
        started = (await self.call_service('execute', {
            'task_id': created['task_id'], 'expected_revision': created['revision'],
        }))['result']
        self.assertEqual(started['state'], 'running')
        done = await self.wait_task_done(created['task_id'])
        self.assertEqual(done['state'], 'succeeded')
        op = next(iter(self.host.operations.values()))
        self.assertEqual(op['result']['result']['abs'], [1, 64, 2])
        # Verify dimension reached Host action arguments.
        commit = next(iter(self.host.commits.values()))
        self.assertEqual(commit['operation_ids'], ['op-1'])

    async def test_portable_backup_and_differential_restore_receipt_nesting(self):
        self.host.world_blocks = {
            (0, 64, 0): {
                'abs': [0, 64, 0],
                'foreground': {'name': 'minecraft:chest', 'states': {}},
                'entity_data': {'Items': [{'Name': 'minecraft:apple', 'Count': 3, 'Damage': 0, 'Slot': 2}]},
            },
            (0, 64, 1): {
                'abs': [0, 64, 1],
                'foreground': {'name': 'minecraft:command_block', 'states': {'facing_direction': 3}},
                'entity_data': {'Command': 'say hi', 'CustomName': 'cb', 'TickDelay': 5, 'auto': 1},
            },
            (1, 64, 0): {
                'abs': [1, 64, 0],
                'foreground': {'name': 'minecraft:air', 'states': {}},
            },
            (1, 64, 1): {
                'abs': [1, 64, 1],
                'foreground': {'name': 'minecraft:stone', 'states': {}},
            },
        }
        backup = (await self.call_service('prepare', {
            'request_key': 'bk-1',
            'kind': 'backup',
            'args': {'dimension': 0, 'pos': [0, 64, 0], 'size': [2, 1, 2]},
        }))['result']
        await self.call_service('execute', {'task_id': backup['task_id'], 'expected_revision': backup['revision']})
        done = await self.wait_task_done(backup['task_id'])
        self.assertEqual(done['state'], 'succeeded')

        snap = (await self.call_service('snapshot', {'task_id': backup['task_id'], 'index': 0, 'offset': 0}))['result']
        self.assertGreater(snap['length'], 0)

        # Mutate live world: chest replaced by dirt, command block gone, air block now has chest with NBT.
        self.host.world_blocks = {
            (0, 64, 0): {'abs': [0, 64, 0], 'foreground': {'name': 'minecraft:dirt', 'states': {}}},
            (0, 64, 1): {'abs': [0, 64, 1], 'foreground': {'name': 'minecraft:air', 'states': {}}},
            (1, 64, 0): {
                'abs': [1, 64, 0],
                'foreground': {'name': 'minecraft:chest', 'states': {}},
                'entity_data': {'Items': []},
            },
            (1, 64, 1): {'abs': [1, 64, 1], 'foreground': {'name': 'minecraft:stone', 'states': {}}},
        }
        restore = (await self.call_service('prepare', {
            'request_key': 'rs-1',
            'kind': 'restore',
            'args': {
                'backup_id': backup['task_id'],
                'anvil_pos': [10, 64, 10],
                'workspace_pos': [11, 64, 10],
            },
        }))['result']
        await self.call_service('execute', {'task_id': restore['task_id'], 'expected_revision': restore['revision']})
        rs_done = await self.wait_task_done(restore['task_id'])
        self.assertEqual(rs_done['state'], 'succeeded')
        # Check generated steps: (1,64,0) was air with nbt_absent & had chest -> only ONE set_block air (no duplicate).
        steps = self.plugin.tasks[restore['task_id']]['steps']
        air_resets = [s for s in steps if s['method'] == 'set_block' and s['args']['pos'] == [1, 64, 0]]
        self.assertEqual(len(air_resets), 1)
        self.assertTrue(any(s['method'] == 'container' and s['args']['pos'] == [0, 64, 0] for s in steps))
        self.assertTrue(any(s['method'] == 'command_block' and s['args']['pos'] == [0, 64, 1] for s in steps))

    async def test_structure_backup_and_bdx_restore_receipt_nesting_and_malformed(self):
        backup = (await self.call_service('prepare', {
            'request_key': 'bk-struct',
            'kind': 'backup',
            'args': {'mode': 'structure', 'pos': [0, 64, 0], 'size': [1, 1, 1]},
        }))['result']
        await self.call_service('execute', {'task_id': backup['task_id'], 'expected_revision': backup['revision']})
        done = await self.wait_task_done(backup['task_id'])
        self.assertEqual(done['state'], 'succeeded')

        restore = (await self.call_service('prepare', {
            'request_key': 'rs-bdx',
            'kind': 'restore',
            'args': {'backup_id': backup['task_id'], 'mode': 'bdx', 'destination': [5, 64, 5]},
        }))['result']
        await self.call_service('execute', {'task_id': restore['task_id'], 'expected_revision': restore['revision']})
        rs_done = await self.wait_task_done(restore['task_id'])
        self.assertEqual(rs_done['state'], 'succeeded')
        self.assertEqual(len(self.host.retained), 1)
        self.assertEqual(self.host.retained[0][0], 'art-123')

        # Corrupt receipt nesting in stored backup -> clean ServiceRejected, worker stays alive.
        self.plugin.tasks[backup['task_id']]['steps'][1]['receipts'][0]['result']['result'] = {}
        self.assertEqual(await self.call_rejected('prepare', {
            'request_key': 'rs-bdx-bad',
            'kind': 'restore',
            'args': {'backup_id': backup['task_id'], 'mode': 'bdx'},
        }), 'invalid_backup_receipt')
        self.assertFalse(self.serve_task.done())

    async def test_all_service_handlers_reject_invalid_args_without_crashing(self):
        bad_calls = [
            ('prepare', 'not-a-dict', 'invalid_argument'),
            ('prepare', {}, 'invalid_argument'),
            ('prepare', {'request_key': 'k', 'kind': 'backup', 'args': 'bad'}, 'invalid_argument'),
            ('prepare', {'request_key': 'k', 'kind': 'backup', 'args': {'dimension': 0, 'pos': [0, 0], 'size': [1, 1, 1]}}, 'invalid_argument'),
            ('prepare', {'request_key': 'k', 'kind': 'container', 'args': {
                'pos': [0, 64, 0], 'anvil_pos': [1, 64, 1], 'workspace_pos': [2, 64, 2],
                'slots': {'99': {'item': {'name': 'stone'}, 'count': 1}},
            }}, 'invalid_argument'),
            ('prepare', {'request_key': 'k', 'kind': 'command_block', 'args': {'unexpected': 1}}, 'invalid_argument'),
            ('inspect', 'bad', 'invalid_argument'),
            ('inspect', {'request_key': 'k'}, 'invalid_argument'),
            ('inspect', {'request_key': 'k', 'pos': [1, 2], 'dimension': 0}, 'invalid_argument'),
            ('inspect', {'request_key': 'k', 'pos': [1, 2, 3], 'dimension': True}, 'invalid_argument'),
            ('execute', {}, 'invalid_argument'),
            ('status', {'task_id': ['unhashable']}, 'invalid_argument'),
            ('cancel', {}, 'invalid_argument'),
            ('resume', {}, 'invalid_argument'),
            ('snapshot', {}, 'invalid_argument'),
            ('fatalder_prepare', {}, 'invalid_argument'),
            ('fatalder_prepare', {'request_key': 'f1', 'source_name': 's', 'position': [0, 64, 0], 'dimension': 'mars'}, 'invalid_argument'),
            ('fatalder_status', {}, 'invalid_argument'),
            ('fatalder_confirm', {}, 'invalid_argument'),
            ('fatalder_pause', {}, 'invalid_argument'),
            ('fatalder_resume', {}, 'invalid_argument'),
            ('fatalder_cancel', {}, 'invalid_argument'),
            ('fatalder_recover', {}, 'invalid_argument'),
        ]
        for svc, args, expected in bad_calls:
            with self.subTest(svc=svc, args=args):
                self.assertEqual(await self.call_rejected(svc, args), expected)
        self.assertFalse(self.serve_task.done())

    async def test_tick_definite_ipcrejected_not_unknown_and_lock_released(self):
        # Definite IPCRejected must NOT mark step as 'unknown'.
        self.host.commit_error = {'code': 'permission_denied', 'message': 'denied', 'retryable': False, 'request_id': 'x'}
        t1 = (await self.call_service('inspect', {'request_key': 'insp-rej', 'pos': [0, 64, 0], 'dimension': 0}))['result']
        await self.call_service('execute', {'task_id': t1['task_id'], 'expected_revision': t1['revision']})
        done = await self.wait_task_done(t1['task_id'])
        self.assertEqual(done['state'], 'failed')
        self.assertEqual(done['steps'][0]['state'], 'failed')
        self.assertEqual(done['steps'][0]['error'], 'permission_denied')
        # Subsequent write works (admission_unknown fence is NOT triggered).
        self.host.commit_error = None

        # While commit is in flight, self.lock must be released so inspect/status still respond immediately!
        self.host.commit_gate = asyncio.Event()
        t2 = (await self.call_service('inspect', {'request_key': 'insp-slow', 'pos': [1, 64, 1], 'dimension': 0}))['result']
        await self.call_service('execute', {'task_id': t2['task_id'], 'expected_revision': t2['revision']})
        await asyncio.sleep(0.6)  # let tick enter tx.save() and block on commit_gate
        t3 = await asyncio.wait_for(
            self.call_service('inspect', {'request_key': 'insp-fast', 'pos': [2, 64, 2], 'dimension': 0}),
            timeout=1.0,
        )
        self.assertIn('result', t3)
        # Maintenance seal must report busy while tx.save() is in flight!
        seal = (await self._rpc('lifecycle.maintenance', {
            'installation_id': 'inst-worldtools', 'generation': 1, 'operation': 'seal', 'token': 'tok-1',
        }))['payload']['result']
        self.assertEqual(seal['status'], 'busy')
        self.host.commit_gate.set()
        self.host.commit_gate = None
        self.assertEqual((await self.wait_task_done(t2['task_id']))['state'], 'succeeded')

    async def test_tick_guards_epoch_and_retain_errors(self):
        self.host.capabilities_error = {'code': 'not_in_game', 'message': 'no', 'retryable': True, 'request_id': 'x'}
        self.assertEqual(await self.call_rejected('inspect', {'request_key': 'insp-ep', 'pos': [0, 64, 0]}), 'world_epoch_unavailable')
        self.host.capabilities_error = None
        t1 = (await self.call_service('inspect', {'request_key': 'insp-ep2', 'pos': [0, 64, 0]}))['result']
        await self.call_service('execute', {'task_id': t1['task_id'], 'expected_revision': t1['revision']})
        self.host.capabilities_error = {'code': 'not_in_game', 'message': 'no', 'retryable': True, 'request_id': 'x'}
        await asyncio.sleep(0.7)
        self.assertFalse(self.serve_task.done())
        self.host.capabilities_error = None
        self.assertEqual((await self.wait_task_done(t1['task_id']))['state'], 'succeeded')

        # Retain IPCRejected during tick fails the task cleanly without crashing worker.
        bk = (await self.call_service('prepare', {
            'request_key': 'bk-ret', 'kind': 'backup', 'args': {'mode': 'structure', 'pos': [0, 64, 0], 'size': [1, 1, 1]},
        }))['result']
        await self.call_service('execute', {'task_id': bk['task_id'], 'expected_revision': bk['revision']})
        await self.wait_task_done(bk['task_id'])
        self.host.retain_error = {'code': 'artifact_not_found', 'message': 'missing', 'retryable': False}
        rs = (await self.call_service('prepare', {
            'request_key': 'rs-ret', 'kind': 'restore', 'args': {'backup_id': bk['task_id'], 'mode': 'bdx'},
        }))['result']
        await self.call_service('execute', {'task_id': rs['task_id'], 'expected_revision': rs['revision']})
        rs_done = await self.wait_task_done(rs['task_id'])
        self.assertEqual(rs_done['state'], 'failed')
        self.assertEqual(rs_done['steps'][0]['error'], 'artifact_not_found')
        self.assertFalse(self.serve_task.done())

    async def test_fatalder_unresolved_states_bounded_status_and_budget_rollback(self):
        # 1. timed_out prepare -> unresolved_remote=True, later status reply -> observed!
        self.host.svc_state = 'timed_out'
        self.host.svc_result = None
        t1 = (await self.call_service('fatalder_prepare', {
            'request_key': 'fd-1', 'source_name': 'castle.bdx', 'position': [0, 64, 0], 'dimension': 'overworld',
        }))['result']
        self.assertEqual(t1['state'], 'remote_unknown')
        self.assertEqual(t1['remote_call_count'], 1)
        self.assertTrue(self.plugin.unresolved_remote(self.plugin.tasks[t1['task_id']]['remote_calls'][0]))

        # Status polls do NOT append to remote_calls!
        self.host.svc_state = 'replied'
        self.host.svc_result = lambda p: {
            'request_key': p['arguments']['request_key'],
            'job_id': 'job-1',
            'phase': 'QUOTED',
            'confirm_token': 'ct-1',
            'confirmation_key': 'ck-1',
        }
        for _ in range(10):
            st = (await self.call_service('fatalder_status', {'task_id': t1['task_id']}))['result']
            self.assertEqual(st['remote_call_count'], 1)
            self.assertEqual(st['state'], 'remote_quoted')
        self.assertEqual(self.plugin.tasks[t1['task_id']]['remote_calls'][0]['state'], 'observed')

        # Confirm with matching tokens -> interrupted -> resolved on status via accepted_start_key.
        self.host.svc_state = 'interrupted'
        self.host.svc_result = None
        c1 = (await self.call_service('fatalder_confirm', {
            'task_id': t1['task_id'], 'confirm_token': 'ct-1', 'confirmation_key': 'ck-1',
        }))['result']
        self.assertEqual(c1['state'], 'remote_unknown')
        self.assertEqual(c1['remote_call_count'], 2)
        self.assertEqual(await self.call_rejected('fatalder_pause', {
            'task_id': t1['task_id'], 'control_id': 'ctrl-1',
        }), 'remote_result_unknown')

        self.host.svc_state = 'replied'
        self.host.svc_result = lambda p: {
            'request_key': p['arguments']['request_key'],
            'job_id': 'job-1',
            'phase': 'RUNNING',
            'accepted_start_key': 'ck-1',
        }
        st2 = (await self.call_service('fatalder_status', {'task_id': t1['task_id']}))['result']
        self.assertEqual(st2['remote_call_count'], 2)
        self.assertEqual(self.plugin.tasks[t1['task_id']]['remote_calls'][1]['state'], 'observed')

    async def test_oversized_state_budget_does_not_mutate_memory(self):
        # Verify check_budget rejects BEFORE mutating self.tasks / remote_calls.
        from neomega_runtime.operations import Operations
        ctx = PluginContext(SimpleNamespace(peer=self.peer, log=logging.getLogger('wt-test')), Path(self.tmp.name))
        ctx._operations = Operations(self.peer, self.host.session)
        ctx.config = SimpleNamespace(max_tasks=256, max_blocks=262144, max_steps=1024, max_state_bytes=100)
        before_keys = set(self.plugin.tasks.keys())
        with self.assertRaises(ServiceRejected) as caught:
            await self.plugin.create(
                ctx,
                {'request_key': 'tiny-budget', 'kind': 'inspect', 'args': {'pos': [0, 64, 0]}},
                SimpleNamespace(installation_id='caller-1'),
            )
        self.assertEqual(caught.exception.code, 'state_byte_budget')
        self.assertEqual(set(self.plugin.tasks.keys()), before_keys)

        with self.assertRaises(ServiceRejected) as caught2:
            await self.plugin.fatalder_call(
                ctx,
                {'request_key': 'fd-tiny', 'source_name': 's.bdx', 'position': [0, 64, 0], 'dimension': 'overworld'},
                SimpleNamespace(installation_id='caller-1'),
                'prepare',
            )
        self.assertEqual(caught2.exception.code, 'state_byte_budget')
        self.assertEqual(set(self.plugin.tasks.keys()), before_keys)

        # Duplicate int/str container_slots rejected via create() too.
        ctx.config = Settings()
        with self.assertRaises(ServiceRejected) as caught3:
            await self.plugin.create(
                ctx,
                {'request_key': 'dup-slots', 'kind': 'container', 'args': {
                    'pos': [0, 64, 0], 'anvil_pos': [1, 64, 1], 'workspace_pos': [2, 64, 2],
                    'slots': {0: {'item': {'name': 'stone'}, 'count': 1}, '0': {'item': {'name': 'dirt'}, 'count': 1}},
                }},
                SimpleNamespace(installation_id='caller-1'),
            )
        self.assertEqual(caught3.exception.code, 'invalid_argument')


class WorldToolsUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_tick_commit_uncertain_and_malformed_snapshot_receipt(self):
        with tempfile.TemporaryDirectory() as tmp:
            host = FakeWorldHost()
            plugin = WorldTools()
            ctx = PluginContext(SimpleNamespace(peer=SimpleNamespace(call=lambda m, p, timeout=None: host.handle(m, p, 'u1').__await__() if False else None), log=logging.getLogger('wt-unit')), Path(tmp))

            async def call_peer(method, payload, timeout=None):
                res = await host.handle(method, payload, 'u1')
                if 'error' in res:
                    err = res['error']
                    raise IPCRejected(err['code'], err['request_id'], err['retryable'])
                return res['result']

            spawned = []
            peer = SimpleNamespace(call=call_peer)
            worker = SimpleNamespace(peer=peer, log=logging.getLogger('wt-unit'), spawn=lambda coro, name=None: spawned.append(asyncio.create_task(coro)) or spawned[-1])
            ctx = PluginContext(worker, Path(tmp))
            from neomega_runtime.operations import Operations
            ctx._operations = Operations(peer, 'sess-1')
            ctx.config = Settings()
            await plugin.on_start(ctx)
            for t in spawned:
                t.cancel()
            await asyncio.gather(*spawned, return_exceptions=True)

            created = await plugin.create(
                ctx,
                {'request_key': 'u-insp', 'kind': 'inspect', 'args': {'pos': [1, 2, 3], 'dimension': 2}},
                SimpleNamespace(installation_id='inst-a'),
            )
            await plugin.execute(
                ctx,
                {'task_id': created['task_id'], 'expected_revision': created['revision']},
                SimpleNamespace(installation_id='inst-a'),
            )

            # Simulate transport failure on state.commit (CommitUncertain) AFTER admission succeeded on host!
            orig_handle = host.handle

            async def uncertain_after_commit(method, payload, req_id):
                out = await orig_handle(method, payload, req_id)
                if method == 'state.commit':
                    raise IPCError('lost reply after commit')
                return out

            host.handle = uncertain_after_commit
            await plugin.tick(ctx)
            task = plugin.tasks[created['task_id']]
            self.assertEqual(task['state'], 'unknown')
            self.assertEqual(task['steps'][0]['state'], 'unknown')

            # Next tick reconciles via state.receipt without re-submitting state.commit!
            host.handle = orig_handle
            await plugin.tick(ctx)
            await plugin.tick(ctx)
            self.assertEqual(task['state'], 'succeeded')
            self.assertEqual(len(host.commits), 1)

            # Portable step_limit & restore_step_limit checks.
            ctx.config = Settings(max_steps=2, max_blocks=100)
            with self.assertRaises(ServiceRejected) as caught:
                await plugin.create(
                    ctx,
                    {'request_key': 'too-many-tiles', 'kind': 'backup', 'args': {'dimension': 0, 'pos': [0, 0, 0], 'size': [6, 2, 2]}},
                    SimpleNamespace(installation_id='inst-a'),
                )
            self.assertEqual(caught.exception.code, 'step_limit')

            # Malformed receipt passed directly to snapshot_completed raises ValueError cleanly.
            with self.assertRaises(ValueError):
                await plugin.snapshot_completed(ctx, task, {'method': '_snapshot_read', 'args': {'dimension': 0}}, {'result': 'not-a-dict'})

            # restore_step_limit during differential restore fails cleanly without crashing.
            ctx.config = Settings(max_steps=3, max_blocks=100)
            bk = await plugin.create(
                ctx,
                {'request_key': 'bk-small', 'kind': 'backup', 'args': {'dimension': 0, 'pos': [0, 0, 0], 'size': [2, 2, 1]}},
                SimpleNamespace(installation_id='inst-a'),
            )
            await plugin.execute(ctx, {'task_id': bk['task_id'], 'expected_revision': bk['revision']}, SimpleNamespace(installation_id='inst-a'))
            await plugin.tick(ctx)
            await plugin.tick(ctx)
            self.assertEqual(plugin.tasks[bk['task_id']]['state'], 'succeeded')
            host.world_blocks = {(x, y, 0): {'abs': [x, y, 0], 'foreground': {'name': 'minecraft:dirt', 'states': {}}} for x in range(2) for y in range(2)}
            rs = await plugin.create(
                ctx,
                {'request_key': 'rs-over-steps', 'kind': 'restore', 'args': {'backup_id': bk['task_id']}},
                SimpleNamespace(installation_id='inst-a'),
            )
            await plugin.execute(ctx, {'task_id': rs['task_id'], 'expected_revision': rs['revision']}, SimpleNamespace(installation_id='inst-a'))
            await plugin.tick(ctx)
            await plugin.tick(ctx)
            self.assertEqual(plugin.tasks[rs['task_id']]['state'], 'failed')
            self.assertEqual(plugin.tasks[rs['task_id']]['steps'][0]['error'], 'restore_step_limit')


if __name__ == '__main__':
    unittest.main()
