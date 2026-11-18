import asyncio
import copy
from datetime import timedelta
import hashlib
import json
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime.services import ServiceRejected

from fatalder_plugin.controller import Controller, STATE, history_key, now
from fatalder_plugin.services import ServiceAdapter


class Store:
    def __init__(self):
        self.values = {}

    async def get(self, key, default=None):
        return copy.deepcopy(self.values.get(key, default))

    async def transaction(self, commit_id=None, keys=None):
        pending = {}

        def set_val(k, v):
            pending[k] = copy.deepcopy(v)

        async def save():
            payload = json.dumps({'mutations': [{'key': k, 'value': v} for k, v in pending.items()]},
                                 ensure_ascii=False, separators=(',', ':'))
            if len(payload.encode('utf-8')) > 256 << 10:
                raise ValueError('storage commit exceeds 256 KiB')
            self.values.update(pending)
            return {}

        return SimpleNamespace(set=set_val, action=lambda _: None, save=save)


class ServiceHost(ServiceAdapter):
    def __init__(self, controller):
        self.controller = controller


class ServicesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tasks = []

        def spawn(coro, **kw):
            task = asyncio.create_task(coro)
            self.tasks.append(task)
            return task

        self.ctx = SimpleNamespace(
            storage=Store(),
            data_dir=self.temp.name,
            config=SimpleNamespace(
                worker_url='https://example.test',
                api_key='secret:key',
                target_server_id='server',
                rental_server_code='123',
                files_directory='imports',
                admin_uuids=['00000000-0000-4000-8000-000000000001'],
                service_installation_ids=['worldtools'],
                poll_seconds=2,
                revoke_operator_on_completion=False,
            ),
            spawn=spawn,
            every=lambda *a, **kw: None,
            players=AsyncMock(return_value=[]),
            log=SimpleNamespace(warning=lambda *a: None),
        )
        self.client = SimpleNamespace(
            upload=lambda path: {
                'object_id': 'obj-1',
                'sha256': 'sha256:' + 'a' * 64,
                'size_bytes': 8,
                'display_name': path.name,
            }
        )
        self.operator = SimpleNamespace()
        self.controller = Controller(self.ctx, self.client, self.operator)
        self.controller.request = AsyncMock(side_effect=self.mock_request)
        self.job_seq = 0
        self.service = ServiceHost(self.controller)
        (self.controller.files_root() / 'house.bdx').write_bytes(b'building')
        self.call = SimpleNamespace(installation_id='worldtools')

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def mock_request(self, method, path, body=None):
        if method == 'POST' and path == '/v2/jobs/prepare':
            self.job_seq += 1
            return {
                'job_id': f'job-{self.job_seq}',
                'quote': {
                    'quote_id': f'q-{self.job_seq}',
                    'amount': 10,
                    'currency': 'unit',
                    'units': 1,
                    'expires_at': (now() + timedelta(seconds=60)).isoformat(),
                },
            }
        if method == 'POST':
            return {}
        return {'job_id': self.controller.state.get('job_id'), 'state': 'RUNNING'}

    async def test_authorization_and_lifecycle_prepare_confirm_pause_status(self):
        with self.assertRaises(ServiceRejected) as err:
            await self.service.service_prepare(self.ctx, {}, SimpleNamespace(installation_id='stranger'))
        self.assertEqual(err.exception.code, 'caller_not_allowed')

        prep = await self.service.service_prepare(self.ctx, {
            'request_key': 'req-1',
            'source_name': 'house.bdx',
            'position': [1, 64, 2],
            'dimension': 'overworld',
        }, self.call)
        self.assertEqual(prep['phase'], 'quoted')
        self.assertEqual(prep['job_id'], 'job-1')

        # Idempotent replay returns same quote; different params reject with request_key_conflict.
        replay = await self.service.service_prepare(self.ctx, {
            'request_key': 'req-1',
            'source_name': 'house.bdx',
            'position': [1, 64, 2],
            'dimension': 'overworld',
        }, self.call)
        self.assertEqual(replay['confirm_token'], prep['confirm_token'])

        with self.assertRaises(ServiceRejected) as conflict:
            await self.service.service_prepare(self.ctx, {
                'request_key': 'req-1',
                'source_name': 'house.bdx',
                'position': [9, 64, 2],
                'dimension': 'overworld',
            }, self.call)
        self.assertEqual(conflict.exception.code, 'request_key_conflict')

        confirmed = await self.service.service_confirm(self.ctx, {
            'request_key': 'req-1',
            'job_id': 'job-1',
            'confirm_token': prep['confirm_token'],
            'confirmation_key': prep['confirmation_key'],
        }, self.call)
        self.assertEqual(confirmed['phase'], 'start_pending')
        self.assertEqual(confirmed['accepted_start_key'], prep['confirmation_key'])
        await asyncio.gather(*self.tasks)
        self.tasks.clear()

        paused = await self.service.service_pause(self.ctx, {
            'request_key': 'req-1',
            'job_id': 'job-1',
            'control_id': 'ctrl-1',
        }, self.call)
        self.assertEqual(paused['control']['action'], 'pause')
        await asyncio.gather(*self.tasks)
        self.tasks.clear()

        status = await self.service.service_status(self.ctx, {
            'request_key': 'req-1',
            'control_id': 'ctrl-1',
        }, self.call)
        self.assertEqual(status['control']['action_state'], 'completed')

    async def test_service_history_is_split_per_key_and_commits_stay_under_256kib(self):
        # Archive 255 tasks (many with controls); STATE stays small and every commit stays well under 256 KiB.
        for idx in range(255):
            if idx > 0:
                self.controller.state['phase'] = 'succeeded'
            res = await self.service.service_prepare(self.ctx, {
                'request_key': f'req-{idx}',
                'source_name': 'house.bdx',
                'position': [idx, 64, 0],
                'dimension': 'overworld',
            }, self.call)
            self.controller.state['controls'] = {
                f'c-{j}': {'control_id': f'c-{j}', 'action': 'pause', 'action_state': 'completed',
                           'observed_state': 'paused', 'desired_state_observed': True}
                for j in range(10)
            }
            self.assertEqual(res['request_key'], f'req-{idx}')

        self.controller.state['phase'] = 'succeeded'
        # Archiving the 255th task brings history_ids to 255; 256th archive hits retained_request_limit.
        await self.service.service_prepare(self.ctx, {
            'request_key': 'req-255',
            'source_name': 'house.bdx',
            'position': [255, 64, 0],
            'dimension': 'overworld',
        }, self.call)
        state_bytes = len(json.dumps(self.ctx.storage.values[STATE], ensure_ascii=False).encode('utf-8'))
        self.assertLess(state_bytes, 64 << 10)
        self.assertEqual(len(self.ctx.storage.values[STATE]['service_history_ids']), 255)

        # Historical status & control lookup still read from fatalder.history.<id>.
        old_id = hashlib.sha256(b'worldtools\0req-0').hexdigest()
        self.assertIn(history_key(old_id), self.ctx.storage.values)
        old_status = await self.service.service_status(self.ctx, {'request_key': 'req-0', 'control_id': 'c-0'}, self.call)
        self.assertEqual(old_status['request_key'], 'req-0')
        self.assertEqual(old_status['control']['control_id'], 'c-0')

        # 257th task hits retained_request_limit cleanly.
        self.controller.state['phase'] = 'succeeded'
        with self.assertRaises(ServiceRejected) as limit_err:
            await self.service.service_prepare(self.ctx, {
                'request_key': 'req-256',
                'source_name': 'house.bdx',
                'position': [256, 64, 0],
                'dimension': 'overworld',
            }, self.call)
        self.assertEqual(limit_err.exception.code, 'retained_request_limit')

    async def test_legacy_service_history_migrates_on_start(self):
        old_id = hashlib.sha256(b'worldtools\0legacy-1').hexdigest()
        self.ctx.storage.values[STATE] = {
            'binding': self.controller.binding(),
            'phase': 'succeeded',
            'service_history': {old_id: {'fingerprint': 'fp', 'result': {'request_key': 'legacy-1', 'controls': {}}}},
        }
        await self.controller.start()
        self.assertNotIn('service_history', self.ctx.storage.values[STATE])
        self.assertEqual(self.ctx.storage.values[STATE]['service_history_ids'], [old_id])
        self.assertIn(history_key(old_id), self.ctx.storage.values)
        res = await self.service.service_status(self.ctx, {'request_key': 'legacy-1'}, self.call)
        self.assertEqual(res['request_key'], 'legacy-1')


if __name__ == '__main__':
    unittest.main()
