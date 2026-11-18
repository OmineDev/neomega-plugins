"""Comprehensive tests for neomega.chunks provider plugin."""
import asyncio
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
from pathlib import Path
import struct
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import PluginContext, ServiceRejected
from neomega_runtime.config import load_config
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config_schema import check_config_schema, schema_for_config, validate_config
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from neomega_runtime.storage import CommitUncertain

import examples
from main import Chunks, Settings


PLUGIN_DIR = Path(__file__).resolve().parents[1]


def build_nsc_v1(*, block_id=42, unknown_cell=None, nbt_cell=(1, 2, 3), nbt_data=b'\x0a\x00\x00', nbt_complete=True):
    """Build a valid neomega.subchunk.v1 binary payload."""
    parts = [b'NSC\x01', bytes([1, 1 if nbt_complete else 0]), struct.pack('<H', 1 if nbt_cell is not None else 0)]
    parts.append(struct.pack('<H', 1))  # palette len = 1
    parts.append(struct.pack('<I', block_id))
    indices = [0] * 4096
    if unknown_cell is not None:
        ux, uy, uz = unknown_cell
        indices[ux * 256 + uz * 16 + uy] = 65535
    parts.append(struct.pack('<4096H', *indices))
    if nbt_cell is not None:
        cx, cy, cz = nbt_cell
        cell_idx = cx * 256 + cz * 16 + cy
        enc = b'bedrock-nbt'
        parts.append(struct.pack('<HB', cell_idx, len(enc)))
        parts.append(enc)
        parts.append(struct.pack('<I', len(nbt_data)))
        parts.append(nbt_data)
    return b''.join(parts)


class LoopbackBulkServer:
    """Minimal loopback HTTP server implementing /bulk/v1/objects/<id>."""

    def __init__(self):
        self.objects = {}
        self.released = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                obj = outer.objects.get(self.path)
                if obj is None or self.headers.get('Authorization') != 'Bearer tok-1':
                    self.send_response(404)
                    self.send_header('Content-Length', '0')
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header('Content-Length', str(len(obj)))
                self.end_headers()
                self.wfile.write(obj)

            def do_DELETE(self):
                outer.released.append(self.path)
                self.send_response(204)
                self.send_header('Content-Length', '0')
                self.end_headers()

            def log_message(self, format, *args):
                pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def register(self, obj_id, data, *, metadata):
        path = f'/bulk/v1/objects/{obj_id}'
        self.objects[path] = data
        return {
            'url': f'http://127.0.0.1:{self.port}{path}',
            'token': 'tok-1',
            'ref': {
                'id': obj_id,
                'digest': 'sha256:' + hashlib.sha256(data).hexdigest(),
                'length': len(data),
                'format': 'neomega.subchunk.v1',
            },
            'metadata': metadata,
        }

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class FakePeer:
    """In-memory peer simulating Host RPCs for Chunks."""

    def __init__(self, bulk=None):
        self.bulk = bulk
        self.epoch = 'epoch-1'
        self.world_snapshot = True
        self.revision = 0
        self.values = {}
        self.receipts = {}
        self.operations = {}
        self.snapshots = {}
        self.commit_behavior = None
        self.snapshot_behavior = None
        self.cap_behavior = None
        self.op_seq = 0
        self.calls = []

    async def call(self, method, payload, timeout=None):
        self.calls.append((method, payload))
        if method == 'capabilities.get':
            if self.cap_behavior is not None:
                res = self.cap_behavior()
                if asyncio.iscoroutine(res):
                    return await res
                return res
            return {
                'session': 's1',
                'data': {'world_snapshot': self.world_snapshot, 'world_epoch': self.epoch},
            }
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': values, 'session': 's1'}
        if method == 'state.commit':
            if self.commit_behavior is not None:
                res = self.commit_behavior(payload)
                if asyncio.iscoroutine(res):
                    return await res
                if res is not None:
                    return res
            for mut in payload['mutations']:
                if mut.get('delete'):
                    self.values.pop(mut['key'], None)
                else:
                    self.values[mut['key']] = mut['value']
            self.revision += 1
            opids = []
            for _ in payload['actions']:
                self.op_seq += 1
                opid = f'op-{self.op_seq}'
                opids.append(opid)
                self.operations[opid] = {'operation_id': opid, 'state': 'accepted', 'can_retry': False, 'result': None}
            rec = {
                'commit_id': payload['commit_id'],
                'revision': self.revision,
                'duplicate': False,
                'operation_ids': opids,
            }
            self.receipts[payload['commit_id']] = {
                'commit_id': payload['commit_id'],
                'found': True,
                'revision': self.revision,
                'operation_ids': opids,
            }
            return rec
        if method == 'state.receipt':
            cid = payload['commit_id']
            return self.receipts.get(cid, {'commit_id': cid, 'found': False})
        if method == 'operations.get':
            return self.operations[payload['operation_id']]
        if method == 'world.snapshot':
            if self.snapshot_behavior is not None:
                res = self.snapshot_behavior(payload)
                if asyncio.iscoroutine(res):
                    return await res
                if res is not None:
                    return res
            key_tuple = (payload['epoch'], payload['dimension'], payload['x'], payload['y'], payload['z'])
            entry = self.snapshots.get(key_tuple)
            if entry is None:
                raise IPCRejected('cache_miss', 'w1', False)
            return entry
        raise AssertionError(f'unexpected method {method}')


def make_call(installation_id='caller-a', call_id='c1'):
    return SimpleNamespace(installation_id=installation_id, generation=1, call_id=call_id)


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


class ChunksTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.bulk = LoopbackBulkServer()
        self.peer = FakePeer(self.bulk)
        worker = SimpleNamespace(peer=self.peer, log=logging.getLogger('chunks-test'), spawn=lambda c, name=None: SimpleNamespace(cancel=lambda: None))
        self.ctx = PluginContext(worker, Path(self.tmp.name))
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.peer, 's1')
        self.plugin = Chunks()
        # Avoid spinning up an uncontrolled background timer in unit tests
        self.spawned = []
        self.ctx.every = lambda sec, cb, **kw: self.spawned.append((sec, cb, kw))
        await self.plugin.on_start(self.ctx)

    async def asyncTearDown(self):
        await self.plugin.on_stop(self.ctx)
        self.bulk.close()
        self.tmp.cleanup()

    async def assert_rejected(self, coro, expected_code):
        with self.assertRaises(ServiceRejected) as caught:
            await coro
        self.assertEqual(caught.exception.code, expected_code)

    async def test_export_input_validation_never_crashes(self):
        call = make_call()
        # request validation
        for bad in (
            'not-a-dict',
            [],
            {},
            {'dimension': 0},
            {'pos': [0, 0, 0]},
            {'dimension': True, 'pos': [0, 0, 0]},
            {'dimension': 0.5, 'pos': [0, 0, 0]},
            {'dimension': 2**31, 'pos': [0, 0, 0]},
            {'dimension': 0, 'pos': (0, 0, 0)},
            {'dimension': 0, 'pos': [0, 0]},
            {'dimension': 0, 'pos': [0, 128, 0]},
            {'dimension': 0, 'pos': [0, -129, 0]},
            {'dimension': 0, 'pos': [0, True, 0]},
            {'dimension': 0, 'pos': [0, 0, 0], 'max_age': -1},
            {'dimension': 0, 'pos': [0, 0, 0], 'max_age': 999},
            {'dimension': 0, 'pos': [0, 0, 0], 'max_age': True},
            {'dimension': 0, 'pos': [0, 0, 0], 'max_age': float('nan')},
            {'dimension': 0, 'pos': [0, 0, 0], 'extra': 1},
        ):
            with self.subTest(export='request', bad=bad):
                await self.assert_rejected(self.plugin.request(self.ctx, bad, call), 'invalid_argument')

        # subscribe validation
        for bad in (
            {},
            {'dimension': 0, 'pos': [0, 0, 0], 'ttl': 0},
            {'dimension': 0, 'pos': [0, 0, 0], 'ttl': 3601},
            {'dimension': 0, 'pos': [0, 0, 0], 'ttl': True},
            {'dimension': 0, 'pos': [0, 0, 0], 'ttl': float('inf')},
            {'dimension': 0, 'pos': [0, 0, 0], 'max_age': -0.1},
            {'dimension': 0, 'pos': [0, 0, 0], 'extra': 1},
        ):
            with self.subTest(export='subscribe', bad=bad):
                await self.assert_rejected(self.plugin.subscribe(self.ctx, bad, call), 'invalid_argument')

        # poll validation
        for bad in (
            'bad',
            {},
            {'subscription_id': ''},
            {'subscription_id': 123},
            {'subscription_id': 's1', 'cursor': -1},
            {'subscription_id': 's1', 'cursor': '0'},
            {'subscription_id': 's1', 'cursor': True},
            {'subscription_id': 's1', 'extra': 1},
        ):
            with self.subTest(export='poll', bad=bad):
                await self.assert_rejected(self.plugin.poll(self.ctx, bad, call), 'invalid_argument')

        # cancel validation
        for bad in ('bad', {}, {'request_id': ''}, {'request_id': 123}, {'request_id': 'r1', 'extra': 1}):
            with self.subTest(export='cancel', bad=bad):
                await self.assert_rejected(self.plugin.cancel(self.ctx, bad, call), 'invalid_argument')

        # read validation
        for bad in (
            'bad',
            {},
            {'request_id': ''},
            {'request_id': None},
            {'request_id': 'r1', 'cell': [0, 0]},
            {'request_id': 'r1', 'cell': [16, 0, 0]},
            {'request_id': 'r1', 'cell': [-1, 0, 0]},
            {'request_id': 'r1', 'cell': [True, 0, 0]},
            {'request_id': 'r1', 'cell': [0, 0, 0], 'nbt_offset': -1},
            {'request_id': 'r1', 'cell': [0, 0, 0], 'nbt_offset': True},
            {'request_id': 'r1', 'cell': [0, 0, 0], 'offset': 0},
            {'request_id': 'r1', 'nbt_offset': 0},
            {'request_id': 'r1', 'offset': -1},
            {'request_id': 'r1', 'length': 0},
            {'request_id': 'r1', 'length': 16385},
            {'request_id': 'r1', 'extra': 1},
        ):
            with self.subTest(export='read', bad=bad):
                await self.assert_rejected(self.plugin.read(self.ctx, bad, call), 'invalid_argument')

    async def test_read_checks_slice_bounds_and_missing_artifact(self):
        call = make_call()
        res = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [1, 2, 3]}, call)
        rid = res['request_id']
        # Simulate an observed row whose artifact file is missing
        row = self.plugin.rows[rid]
        row['action_state'] = 'succeeded'
        row['observation'] = 'observed'
        row['cached_at'] = time.time()
        self.plugin.update_state(row)

        await self.assert_rejected(self.plugin.read(self.ctx, {'request_id': rid}, call), 'artifact_missing')

        # Write real NSC v1 subchunk artifact
        raw = build_nsc_v1(block_id=77, unknown_cell=(2, 2, 2), nbt_cell=(1, 2, 3), nbt_data=b'hello-nbt')
        (self.plugin.path / rid).write_bytes(raw)

        # Unchecked slice offset > len(raw) and > len(nbt.data) must reject with invalid_argument
        await self.assert_rejected(self.plugin.read(self.ctx, {'request_id': rid, 'offset': len(raw) + 1}, call), 'invalid_argument')
        await self.assert_rejected(self.plugin.read(self.ctx, {'request_id': rid, 'cell': [1, 2, 3], 'nbt_offset': 10}, call), 'invalid_argument')
        # Cell with no NBT and nbt_offset > 0 rejects cleanly
        await self.assert_rejected(self.plugin.read(self.ctx, {'request_id': rid, 'cell': [0, 0, 0], 'nbt_offset': 1}, call), 'invalid_argument')

        # Valid cell reads (including NBT and unknown block returning None)
        cell_res = await self.plugin.read(self.ctx, {'request_id': rid, 'cell': [1, 2, 3], 'nbt_offset': 6}, call)
        self.assertEqual(cell_res['cell']['layers'], [77])
        self.assertEqual(base64.b64decode(cell_res['cell']['nbt']['data_base64']), b'nbt')
        self.assertEqual(cell_res['cell']['nbt']['length'], 9)

        unknown_res = await self.plugin.read(self.ctx, {'request_id': rid, 'cell': [2, 2, 2]}, call)
        self.assertEqual(unknown_res['cell']['layers'], [None])
        self.assertIsNone(unknown_res['cell']['nbt'])

    async def test_ipc_rejected_transitions_to_failed_without_stalling_queue(self):
        call = make_call()
        r1 = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [1, 0, 1]}, call)
        r2 = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [2, 0, 2]}, call)

        saved_states = []
        orig_save = self.plugin.save

        async def spy_save(ctx):
            for k, r in self.plugin.rows.items():
                saved_states.append((k, r['action_state'], r['state']))
            await orig_save(ctx)

        self.plugin.save = spy_save

        def fail_first_commit(payload):
            if payload['commit_id'] == self.plugin.rows[r1['request_id']]['commit_id']:
                raise IPCRejected('world_mode_unavailable', 'w1', False)
            return None

        self.peer.commit_behavior = fail_first_commit

        await self.plugin.tick(self.ctx)
        row1 = self.plugin.rows[r1['request_id']]
        self.assertEqual(row1['action_state'], 'failed')
        self.assertEqual(row1['state'], 'failed')
        self.assertEqual(row1['error'], 'world_mode_unavailable')
        # Verify 'unknown' was NEVER saved for r1!
        self.assertNotIn((r1['request_id'], 'unknown', 'unknown'), saved_states)

        # Next tick MUST NOT stall on r1; r2 proceeds cleanly!
        await self.plugin.tick(self.ctx)
        row2 = self.plugin.rows[r2['request_id']]
        self.assertEqual(row2['action_state'], 'accepted')
        self.assertEqual(row2['state'], 'waiting')
        self.assertTrue(row2['operation_ids'])

    async def test_commit_uncertain_becomes_unknown_and_reconciles_to_observed(self):
        call = make_call()
        r1 = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [3, 0, 3]}, call)
        rid = r1['request_id']

        # First commit actually succeeds on Host, but reply is lost -> CommitUncertain
        def lost_reply(payload):
            # Persist on peer side to simulate server-side commit before transport drop
            self.peer.commit_behavior = None
            asyncio.create_task(self.peer.call('state.commit', payload))
            raise CommitUncertain(SimpleNamespace(payload=lambda: payload))

        # Use synchronous side-effect so receipt is immediately present on Host
        def lost_reply_sync(payload):
            self.peer.revision += 1
            self.peer.op_seq += 1
            opid = f'op-{self.peer.op_seq}'
            self.peer.operations[opid] = {'operation_id': opid, 'state': 'succeeded', 'can_retry': False,
                                          'result': {'state': 'succeeded', 'evidence': 'dispatched'}}
            self.peer.receipts[payload['commit_id']] = {
                'commit_id': payload['commit_id'], 'found': True, 'revision': self.peer.revision, 'operation_ids': [opid]
            }
            raise CommitUncertain(SimpleNamespace(payload=lambda: payload))

        self.peer.commit_behavior = lost_reply_sync
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.plugin.rows[rid]['action_state'], 'unknown')
        self.assertEqual(self.plugin.rows[rid]['state'], 'unknown')
        self.assertEqual(self.plugin.rows[rid]['operation_ids'], [])

        # Prepare valid bulk snapshot with revision > baseline (0)
        raw = build_nsc_v1(block_id=99)
        key = ('epoch-1', 0, 3, 0, 3)
        self.peer.snapshots[key] = self.bulk.register(
            'obj-303', raw,
            metadata={'key': {'epoch': 'epoch-1', 'dimension': 0, 'x': 3, 'y': 0, 'z': 3}, 'revision': 5},
        )

        # Next tick reconciles via state.receipt, checks operations.get, fetches snapshot -> observed
        await self.plugin.tick(self.ctx)
        row = self.plugin.rows[rid]
        self.assertEqual(row['action_state'], 'succeeded')
        self.assertEqual(row['observation'], 'observed')
        self.assertEqual(row['state'], 'observed')

        read_res = await self.plugin.read(self.ctx, {'request_id': rid, 'cell': [0, 0, 0]}, call)
        self.assertEqual(read_res['cell']['layers'], [99])

    async def test_tick_resilience_and_lock_released_across_io(self):
        call = make_call()
        sub = await self.plugin.subscribe(self.ctx, {'dimension': 0, 'pos': [4, 0, 4]}, call)

        # 1. world_epoch raising snapshot_unavailable must not crash tick
        self.peer.world_snapshot = False
        await self.plugin.tick(self.ctx)
        self.peer.world_snapshot = True

        # 2. Slow tx.save() must release self.lock so concurrent read/poll proceed immediately
        entered = asyncio.Event()
        release = asyncio.Event()

        async def slow_commit(payload):
            entered.set()
            await release.wait()
            self.peer.commit_behavior = None
            return await self.peer.call('state.commit', payload)

        self.peer.commit_behavior = slow_commit
        tick_task = asyncio.create_task(self.plugin.tick(self.ctx))
        await asyncio.wait_for(entered.wait(), timeout=2)

        # Concurrent poll and read must NOT deadlock on self.lock
        poll_res = await asyncio.wait_for(
            self.plugin.poll(self.ctx, {'subscription_id': sub['subscription_id']}, call), timeout=0.5
        )
        self.assertEqual(poll_res['cursor'], 1)
        self.assertEqual(poll_res['updates'][0]['state'], 'queued')
        read_res = await asyncio.wait_for(
            self.plugin.read(self.ctx, {'request_id': sub['request_id']}, call), timeout=0.5
        )
        self.assertEqual(read_res['state'], 'queued')

        release.set()
        await asyncio.wait_for(tick_task, timeout=2)

        # 3. snapshot raising unexpected IPCRejected / BulkError must not crash tick
        self.peer.snapshot_behavior = lambda p: (_ for _ in ()).throw(IPCRejected('rate_limited', 'w1', True))
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.plugin.rows[sub['request_id']]['observation_error'], 'rate_limited')

        # 4. queue_full during subscriber refresh must not crash tick
        self.peer.snapshot_behavior = None
        self.ctx.config = Settings(max_requests=1, cache_seconds=1.0, observe_timeout=1.0)
        self.plugin.rows[sub['request_id']]['state'] = 'unobserved'
        self.plugin.rows[sub['request_id']]['action_state'] = 'succeeded'
        self.plugin.rows[sub['request_id']]['created_at'] = time.time() - 10
        await self.plugin.tick(self.ctx)

    async def test_owner_merging_on_subscription_refresh(self):
        call_a = make_call('inst-a')
        call_b = make_call('inst-b')
        sub_a = await self.plugin.subscribe(self.ctx, {'dimension': 0, 'pos': [5, 0, 5]}, call_a)
        req_b = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [5, 0, 5]}, call_b)
        rid = sub_a['request_id']
        self.assertEqual(rid, req_b['request_id'])
        self.assertEqual(self.plugin.rows[rid]['owners'], ['inst-a', 'inst-b'])

        # Mark row as stale observed so tick refreshes it for sub_a
        self.ctx.config = Settings(cache_seconds=1.0)
        row = self.plugin.rows[rid]
        row['action_state'] = 'succeeded'
        row['observation'] = 'observed'
        row['state'] = 'observed'
        row['cached_at'] = time.time() - 10

        await self.plugin.tick(self.ctx)
        refreshed = self.plugin.rows[rid]
        self.assertEqual(refreshed['state'], 'queued')
        # inst-b MUST still own the refreshed row!
        self.assertEqual(refreshed['owners'], ['inst-a', 'inst-b'])
        await self.plugin.read(self.ctx, {'request_id': rid}, call_b)

    async def test_on_start_does_not_revive_observed_or_epoch_expired(self):
        obs_id = 'a' * 64
        exp_id = 'b' * 64
        wait_id = 'c' * 64
        obs_row = {
            'request_id': obs_id, 'dimension': 0, 'pos': [1, 0, 1], 'epoch': 'epoch-1',
            'state': 'observed', 'action_state': 'succeeded', 'observation': 'observed',
            'owners': ['inst-a'], 'created_at': 100.0, 'cached_at': 1234.5,
            'commit_id': 'c1', 'operation_ids': ['op1'],
        }
        exp_row = {
            'request_id': exp_id, 'dimension': 0, 'pos': [2, 0, 2], 'epoch': 'epoch-old',
            'state': 'epoch_expired', 'action_state': 'succeeded', 'observation': 'waiting',
            'owners': ['inst-a'], 'created_at': 100.0, 'cached_at': 0,
            'commit_id': 'c2', 'operation_ids': ['op2'],
        }
        wait_row = {
            'request_id': wait_id, 'dimension': 0, 'pos': [3, 0, 3], 'epoch': 'epoch-1',
            'state': 'waiting', 'action_state': 'pending', 'observation': 'waiting',
            'owners': ['inst-a'], 'created_at': 100.0, 'cached_at': 555.0,
            'commit_id': 'c3', 'operation_ids': ['op3'],
        }
        for k, v in ((obs_id, obs_row), (exp_id, exp_row), (wait_id, wait_row)):
            (self.plugin.path / f'{k}.request.json').write_text(json.dumps(v))

        restarted = Chunks()
        await restarted.on_start(self.ctx)
        try:
            self.assertEqual(restarted.rows[obs_id]['state'], 'observed')
            self.assertEqual(restarted.rows[obs_id]['observation'], 'observed')
            self.assertEqual(restarted.rows[obs_id]['cached_at'], 1234.5)

            self.assertEqual(restarted.rows[exp_id]['state'], 'epoch_expired')
            self.assertEqual(restarted.rows[wait_id]['state'], 'waiting')
            self.assertEqual(restarted.rows[wait_id]['cached_at'], 0)
        finally:
            await restarted.on_stop(self.ctx)

    async def test_timeout_uses_baseline_submission_not_created_at_and_cursor_advances(self):
        self.ctx.config = Settings(observe_timeout=5.0)
        call = make_call()
        r_done = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [1, 0, 1]}, call)
        r_wait1 = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [2, 0, 2]}, call)
        r_done2 = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [3, 0, 3]}, call)
        r_wait2 = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [4, 0, 4]}, call)

        # Mark r_done and r_done2 inactive (observed)
        for rid in (r_done['request_id'], r_done2['request_id']):
            self.plugin.rows[rid]['action_state'] = 'succeeded'
            self.plugin.rows[rid]['observation'] = 'observed'
            self.plugin.rows[rid]['state'] = 'observed'
            self.plugin.rows[rid]['cached_at'] = time.time()

        # Simulate r_wait1 and r_wait2 sitting in queue longer than observe_timeout
        self.plugin.rows[r_wait1['request_id']]['created_at'] = time.time() - 60.0
        self.plugin.rows[r_wait2['request_id']]['created_at'] = time.time() - 60.0

        # Tick 1 must skip r_done, process r_wait1, and advance cursor past r_wait1 (to index 2)!
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.plugin.rows[r_wait1['request_id']]['state'], 'waiting')
        self.assertEqual(self.plugin.rows[r_wait1['request_id']]['observation'], 'waiting')
        # Tick 2 must NOT get stuck re-processing r_wait1; it must process r_wait2!
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.plugin.rows[r_wait2['request_id']]['state'], 'waiting')
        self.assertEqual(self.plugin.rows[r_wait2['request_id']]['observation'], 'waiting')

        # Now expire submitted_at/observation_not_before -> transitions to unobserved
        self.plugin.rows[r_wait1['request_id']]['submitted_at'] = time.time() - 10.0
        self.plugin.rows[r_wait1['request_id']]['observation_not_before'] = time.time() - 10.0
        opid = self.plugin.rows[r_wait1['request_id']]['operation_ids'][0]
        self.peer.operations[opid]['state'] = 'succeeded'
        self.peer.operations[opid]['result'] = {'state': 'succeeded', 'evidence': 'dispatched'}
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.plugin.rows[r_wait1['request_id']]['observation'], 'unobserved')
        self.assertEqual(self.plugin.rows[r_wait1['request_id']]['state'], 'unobserved')

    async def test_subscriber_pruning_and_eviction_protects_referenced_rows(self):
        self.ctx.config = Settings(max_requests=2, max_subscriptions=1, cache_seconds=1.0)
        call = make_call()
        sub = await self.plugin.subscribe(self.ctx, {'dimension': 0, 'pos': [1, 0, 1]}, call, )
        ref_id = sub['request_id']
        # Force subscriber expiration test: expired subscriber is pruned before limit check
        self.plugin.subscribers[sub['subscription_id']]['expires'] = time.time() - 1
        sub2 = await self.plugin.subscribe(self.ctx, {'dimension': 0, 'pos': [2, 0, 2]}, call)
        self.assertNotIn(sub['subscription_id'], self.plugin.subscribers)
        ref_id2 = sub2['request_id']

        # Both ref_id (unreferenced) and ref_id2 (referenced by sub2) are expired terminal rows
        for rid in (ref_id, ref_id2):
            row = self.plugin.rows[rid]
            row['action_state'] = 'succeeded'
            row['observation'] = 'observed'
            row['state'] = 'observed'
            row['cached_at'] = time.time() - 10

        # Admitting a 3rd chunk evicts ref_id (unreferenced), NOT ref_id2 (referenced by sub2)
        r3 = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [3, 0, 3]}, call)
        self.assertNotIn(ref_id, self.plugin.rows)
        self.assertIn(ref_id2, self.plugin.rows)

        # Mark r3 expired too, then remove unreferenced? r3 is unreferenced, ref_id2 is referenced.
        # If both are non-evictable (r3 still queued), a 4th request must fail queue_full rather than evicting ref_id2!
        await self.assert_rejected(self.plugin.request(self.ctx, {'dimension': 0, 'pos': [4, 0, 4]}, call), 'queue_full')
        self.assertIn(ref_id2, self.plugin.rows)

    async def test_unknown_with_observed_snapshot_and_poll_gap_and_corrupt_snapshot(self):
        call = make_call()
        sub = await self.plugin.subscribe(self.ctx, {'dimension': 0, 'pos': [6, 0, 6]}, call)
        rid = sub['request_id']

        # Tick 1: submits commit; set operation state to unknown
        await self.plugin.tick(self.ctx)
        opid = self.plugin.rows[rid]['operation_ids'][0]
        self.peer.operations[opid] = {
            'operation_id': opid, 'state': 'unknown', 'can_retry': False,
            'result': {'state': 'unknown', 'reason': 'subchunk_request_error'},
        }

        # Corrupt snapshot bytes must not crash tick
        key = ('epoch-1', 0, 6, 0, 6)
        self.peer.snapshots[key] = self.bulk.register(
            'obj-bad', b'BADDATA!',
            metadata={'key': {'epoch': 'epoch-1', 'dimension': 0, 'x': 6, 'y': 0, 'z': 6}, 'revision': 2},
        )
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.plugin.rows[rid]['observation_error'], 'ValueError')
        self.assertEqual(self.plugin.rows[rid]['state'], 'unknown')

        # Valid snapshot arrives while action_state remains unknown:
        # state must stay 'unknown', yet read() returns observed data!
        raw = build_nsc_v1(block_id=123)
        self.peer.snapshots[key] = self.bulk.register(
            'obj-good', raw,
            metadata={'key': {'epoch': 'epoch-1', 'dimension': 0, 'x': 6, 'y': 0, 'z': 6}, 'revision': 3},
        )
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.plugin.rows[rid]['action_state'], 'unknown')
        self.assertEqual(self.plugin.rows[rid]['observation'], 'observed')
        self.assertEqual(self.plugin.rows[rid]['state'], 'unknown')

        read_res = await self.plugin.read(self.ctx, {'request_id': rid, 'offset': 0, 'length': 16}, call)
        self.assertEqual(read_res['state'], 'unknown')
        self.assertEqual(base64.b64decode(read_res['data_base64']), raw[:16])

        # Poll from cursor=0 sees gap=True (cursor advanced across multiple state transitions)
        poll_res = await self.plugin.poll(self.ctx, {'subscription_id': sub['subscription_id'], 'cursor': 0}, call)
        self.assertGreaterEqual(poll_res['cursor'], 2)
        self.assertTrue(poll_res['gap'])
        self.assertEqual(len(poll_res['updates']), 1)
        self.assertEqual(poll_res['updates'][0]['observation'], 'observed')

        # Poll at current cursor returns empty updates
        poll_cur = await self.plugin.poll(self.ctx, {'subscription_id': sub['subscription_id'], 'cursor': poll_res['cursor']}, call)
        self.assertEqual(poll_cur['updates'], [])
        self.assertFalse(poll_cur['gap'])

        # Expired subscription rejected cleanly
        self.plugin.subscribers[sub['subscription_id']]['expires'] = time.time() - 1
        await self.assert_rejected(self.plugin.poll(self.ctx, {'subscription_id': sub['subscription_id']}, call), 'subscription_expired')

    async def test_maintenance_seal_and_release_without_holding_lock(self):
        call = make_call()
        r1 = await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [7, 0, 7]}, call)
        rid = r1['request_id']
        await self.plugin.tick(self.ctx)  # submits op-1 (pending)

        # Seal returns busy while op is pending
        res = await self.plugin.on_maintenance(self.ctx, {'operation': 'seal', 'token': 'tok-1'})
        self.assertEqual(res, {'status': 'busy', 'token': 'tok-1'})
        # _sealing must have been cleared so normal writes still work
        await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [8, 0, 8]}, call)

        # Finish op-1 and mark observed (op-1 succeeded; second row still queued)
        opid = self.plugin.rows[rid]['operation_ids'][0]
        self.peer.operations[opid]['state'] = 'succeeded'
        self.peer.operations[opid]['result'] = {'state': 'succeeded', 'evidence': 'dispatched'}
        self.plugin.rows[rid]['action_state'] = 'succeeded'
        self.plugin.rows[rid]['observation'] = 'observed'
        self.plugin.rows[rid]['cached_at'] = time.time()
        self.plugin.update_state(self.plugin.rows[rid])
        (self.plugin.path / rid).write_bytes(build_nsc_v1(block_id=55))

        sealed = await self.plugin.on_maintenance(self.ctx, {'operation': 'seal', 'token': 'tok-1'})
        self.assertEqual(sealed, {'status': 'sealed', 'token': 'tok-1'})
        await self.assert_rejected(self.plugin.request(self.ctx, {'dimension': 0, 'pos': [9, 0, 9]}, call), 'maintenance_sealed')
        # Reads remain allowed during seal
        read_res = await self.plugin.read(self.ctx, {'request_id': rid, 'cell': [0, 0, 0]}, call)
        self.assertEqual(read_res['cell']['layers'], [55])

        released = await self.plugin.on_maintenance(self.ctx, {'operation': 'release', 'token': 'tok-1'})
        self.assertEqual(released, {'status': 'released', 'token': 'tok-1'})
        await self.plugin.request(self.ctx, {'dimension': 0, 'pos': [9, 0, 9]}, call)

    async def test_config_schema_example_manifest_and_examples(self):
        schema = json.loads((PLUGIN_DIR / 'config.schema.json').read_text())
        check_config_schema(schema)
        self.assertEqual(schema, schema_for_config(Settings))

        example = json.loads((PLUGIN_DIR / 'config.example.json').read_text())
        self.assertEqual(validate_config(schema, example), example)
        loaded = load_config(Settings, PLUGIN_DIR / 'config.example.json')
        self.assertEqual(loaded, Settings(**example))

        for bad_cfg in (
            {'max_requests': 0},
            {'max_requests': 1025},
            {'max_subscriptions': 0},
            {'cache_seconds': 0.5},
            {'observe_timeout': 301},
            {'unknown': 1},
        ):
            with self.assertRaises(ConfigError):
                validate_config(schema, bad_cfg)

        release = json.loads((PLUGIN_DIR / 'release.json').read_text())
        self.assertEqual(release['kind'], 'provider')

        manifest = json.loads((PLUGIN_DIR / 'manifest.json').read_text())
        self.assertEqual(manifest['version'], '1.0.1')
        allowed_schema_keys = {'type', 'properties', 'required', 'additionalProperties', 'items', 'enum', 'pattern', 'minimum', 'maximum'}
        for exp in manifest['exports']:
            for s in (exp['input_schema'], exp['output_schema']):
                self.assertEqual(s['type'], 'object')
                self.assertTrue(s.get('properties'))
                self.assertTrue(set(s) <= allowed_schema_keys)

        # examples.py integration
        mock_ctx = SimpleNamespace(services=SimpleNamespace(call=AsyncMock(return_value={'request_id': 'abc'})))
        res = await examples.request_chunk(mock_ctx, 0, (1, 0, 2))
        self.assertEqual(res['request_id'], 'abc')
        mock_ctx.services.call.assert_awaited_once()
        args, kwargs = mock_ctx.services.call.call_args
        self.assertEqual(args[0], 'plugin.neomega.chunks.request')
        self.assertEqual(args[1], {'dimension': 0, 'pos': [1, 0, 2]})
        self.assertEqual(kwargs['timeout'], 25)


class ChunksServeTests(unittest.IsolatedAsyncioTestCase):
    async def test_serve_exports_and_invalid_input_does_not_kill_worker(self):
        from neomega_runtime.managed import ManagedPeer

        peer_state = FakePeer()
        reader = asyncio.StreamReader(limit=1 << 20)
        writer = ServeWriter()
        peer = ManagedPeer(reader, writer)
        plugin = Chunks()

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
                        'protocol_major': 1, 'protocol_minor': 0, 'installation_id': 'inst-chunks', 'generation': 1,
                        'capabilities': [], 'limits': {
                            'max_frame_bytes': 1 << 20, 'outgoing_requests': 128,
                            'inbound_requests': 32, 'queue_frames': 32}}}})
                else:
                    res = await peer_state.call(method, payload)
                    send({'type': 'response', 'id': req_id, 'payload': {'result': res}})

        with tempfile.TemporaryDirectory() as tmp:
            serve_task = asyncio.create_task(plugin.serve(peer, data_dir=Path(tmp)))
            pump_task = asyncio.create_task(pump())
            try:
                send({'type': 'request', 'id': 'v1', 'method': 'lifecycle.validate',
                      'payload': {'installation_id': 'inst-chunks', 'generation': 1, 'revision': 1}})
                self.assertIn('result', (await wait_response('v1'))['payload'])

                send({'type': 'request', 'id': 'a1', 'method': 'lifecycle.activate',
                      'payload': {'installation_id': 'inst-chunks', 'generation': 1, 'revision': 1, 'state_version': 1}})
                self.assertTrue((await wait_response('a1'))['payload']['result']['activated'])

                call_ctx = {
                    'installation_id': 'caller-1',
                    'generation': 1,
                    'call_id': 'c1',
                    'accepted_at': '2026-11-18T00:00:00+00:00',
                    'deadline': '2026-11-18T00:00:25+00:00',
                }

                # Patch datetime.now check indirectly by using current UTC timestamps
                from datetime import datetime, timedelta, timezone
                now_dt = datetime.now(timezone.utc)
                call_ctx['accepted_at'] = now_dt.isoformat()
                call_ctx['deadline'] = (now_dt + timedelta(seconds=25)).isoformat()

                # 1. Valid request via services.online.invoke
                send({'type': 'request', 'id': 's1', 'method': 'services.online.invoke',
                      'payload': {'service': 'plugin.neomega.chunks.request', 'major': 1, 'call_id': 'c1',
                                  'call_context': call_ctx,
                                  'arguments': {'dimension': 0, 'pos': [1, 0, 2]}}})
                res1 = await wait_response('s1')
                req_id = res1['payload']['result']['request_id']
                self.assertEqual(res1['payload']['result']['state'], 'queued')

                # 2. Bad read argument returns service_rejected and keeps worker alive!
                c2 = dict(call_ctx, call_id='c2')
                send({'type': 'request', 'id': 's2', 'method': 'services.online.invoke',
                      'payload': {'service': 'plugin.neomega.chunks.read', 'major': 1, 'call_id': 'c2',
                                  'call_context': c2,
                                  'arguments': {'request_id': req_id, 'cell': [99, 0, 0]}}})
                res2 = await wait_response('s2')
                self.assertEqual(res2['payload']['error']['code'], 'service_rejected')
                self.assertEqual(res2['payload']['error']['details']['code'], 'invalid_argument')
                self.assertFalse(serve_task.done())

                # 3. Cancel via services.online.invoke still succeeds on healthy worker
                c3 = dict(call_ctx, call_id='c3')
                send({'type': 'request', 'id': 's3', 'method': 'services.online.invoke',
                      'payload': {'service': 'plugin.neomega.chunks.cancel', 'major': 1, 'call_id': 'c3',
                                  'call_context': c3,
                                  'arguments': {'request_id': req_id}}})
                res3 = await wait_response('s3')
                self.assertEqual(res3['payload']['result'], {'request_id': req_id, 'state': 'detached'})

                # 4. Drain cleanly
                send({'type': 'request', 'id': 'dr1', 'method': 'lifecycle.drain',
                      'payload': {'installation_id': 'inst-chunks', 'generation': 1}})
                self.assertTrue((await wait_response('dr1'))['payload']['result']['drained'])
            finally:
                serve_task.cancel()
                pump_task.cancel()
                await asyncio.gather(serve_task, pump_task, return_exceptions=True)


if __name__ == '__main__':
    unittest.main()
