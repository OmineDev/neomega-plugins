"""Drive the plugin through the real SDK storage against an in-memory Host."""
import asyncio
import copy
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from neomega_runtime import PluginContext, ProtocolError
from neomega_runtime.game import Commands
from neomega_runtime.managed import IPCRejected, ManagedPeer
from neomega_runtime.operations import Operations
from main import MAX_BROADCAST_BYTES, Settings, Task, TimedCommands, broadcast_commands

BEIJING = timezone(timedelta(hours=8))


def at(hour, minute, second=0, day=21):
    return datetime(2026, 10, day, hour, minute, second, tzinfo=BEIJING).astimezone(timezone.utc)


class FakeHost:
    """state.get / state.commit with the Host's global revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits, self.reads = {}, 0, [], []
        self.fail = None

    async def call(self, method, payload, timeout=None):
        if method == 'state.get':
            keys = payload.get('keys')
            self.reads.append(keys)
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        assert method == 'state.commit', method
        if self.fail is not None:
            raise self.fail
        assert payload['expected_state_revision'] == self.revision
        for mutation in payload['mutations']:
            self.values[mutation['key']] = mutation['value']
        self.revision += 1
        self.commits.append(payload)
        return {'commit_id': payload['commit_id'], 'revision': self.revision, 'duplicate': False,
                'operation_ids': [f'op{i}' for i in range(len(payload['actions']))]}

    def commands(self):
        return [action['arguments']['cmd'] for action in self.commits[-1]['actions']]


class TimedCommandsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('timed-test')), None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.plugin = TimedCommands()
        self.time = at(9, 30)
        self.plugin.now = lambda: self.time

    async def check(self, time):
        self.time = time
        await self.plugin.check(self.ctx)

    async def test_first_check_only_records(self):
        await self.check(at(9, 30))
        self.assertEqual(len(self.host.commits), 1)
        self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.host.values['task:整点公告'], int(at(9, 0).timestamp()) // 60)

    async def test_interval_task_runs_once_per_period(self):
        await self.check(at(9, 30))
        await self.check(at(10, 0, 5))
        self.assertEqual(len(self.host.commits), 2)
        self.assertEqual(self.host.commands(),
                         ['tellraw @a {"rawtext": [{"text": "感谢游玩，祝你玩得开心！"}]}'])
        for time in (at(10, 0, 15), at(10, 4), at(10, 59, 59)):
            await self.check(time)
        self.assertEqual(len(self.host.commits), 2)
        await self.check(at(11, 0, 1))
        self.assertEqual(len(self.host.commits), 3)
        self.assertEqual(len(self.host.commands()), 1)

    async def test_daily_task_waits_for_its_time(self):
        self.ctx.config = Settings(tasks=[Task('晚间清理', daily_at='20:00', commands=['/kill @e[type=item]'])])
        await self.check(at(19, 0))
        await self.check(at(19, 59, 59))
        self.assertEqual(len(self.host.commits), 1)
        self.assertEqual(self.host.commits[-1]['actions'], [])
        await self.check(at(20, 0, 3))
        self.assertEqual(self.host.commands(), ['kill @e[type=item]'])
        await self.check(at(23, 59))
        await self.check(at(8, 0, day=22))
        self.assertEqual(len(self.host.commits), 2)
        await self.check(at(20, 1, day=22))
        self.assertEqual(len(self.host.commits), 3)
        self.assertEqual(self.host.commands(), ['kill @e[type=item]'])

    async def test_missed_beyond_grace_only_advances(self):
        await self.check(at(9, 30))
        await self.check(at(10, 6))
        self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.host.values['task:整点公告'], int(at(10, 0).timestamp()) // 60)
        await self.check(at(10, 7))
        self.assertEqual(len(self.host.commits), 2)

    async def test_one_transaction_for_all_tasks(self):
        self.ctx.config = Settings(tasks=[
            Task('公告', every_minutes=60, message='整点了'),
            Task('奖励', every_minutes=30, commands=['give @a bread 1', 'say 发面包了']),
            Task('晚间', daily_at='20:00', message='晚上好'),
        ])
        await self.check(at(9, 50))
        await self.check(at(10, 0, 5))
        self.assertEqual(len(self.host.commits), 2)
        self.assertEqual(self.host.reads[-1], ['task:公告', 'task:奖励', 'task:晚间'])
        self.assertEqual(self.host.commands(), [
            'tellraw @a {"rawtext": [{"text": "整点了"}]}', 'give @a bread 1', 'say 发面包了'])
        self.assertEqual({m['key'] for m in self.host.commits[-1]['mutations']}, {'task:公告', 'task:奖励'})

    async def test_broadcast_escapes_quotes_and_keeps_chinese(self):
        message = '他说"欢迎"\\再见'
        self.ctx.config = Settings(tasks=[Task('公告', every_minutes=10, message=message)])
        await self.check(at(9, 55))
        await self.check(at(10, 0, 1))
        [command] = self.host.commands()
        self.assertEqual(command, 'tellraw @a {"rawtext": [{"text": "他说\\"欢迎\\"\\\\再见"}]}')
        self.assertEqual(json.loads(command.removeprefix('tellraw @a '))['rawtext'][0]['text'], message)

    async def test_uncertain_commit_is_logged_and_next_round_rereads(self):
        await self.check(at(9, 30))
        for failure in (ConnectionError('lost'), IPCRejected('revision_conflict', 'r1', True),
                        ProtocolError('bad reply'), TimeoutError('timed out')):
            self.host.fail = failure
            with self.assertLogs('timed-test', 'WARNING'):
                await self.check(at(10, 0, 5))
            self.assertEqual(len(self.host.commits), 1)
        self.host.fail = None
        await self.check(at(10, 0, 15))
        self.assertEqual(len(self.host.commits), 2)
        self.assertEqual(len(self.host.commands()), 1)

    async def test_prepare_value_error_does_not_crash_check(self):
        self.ctx.config = Settings(tasks=[
            Task('坏命令', every_minutes=10, commands=['say ok']),
            Task('好命令', every_minutes=10, message='正常广播'),
        ])
        await self.check(at(9, 55))
        good_intent = Commands(self.ctx.operations).prepare(
            'tellraw @a {"rawtext": [{"text": "正常广播"}]}',
            idempotency_key='k', deadline=at(10, 1).isoformat())
        with patch.object(Commands, 'prepare', side_effect=[ValueError('bad cmd'), good_intent]):
            with self.assertLogs('timed-test', 'WARNING'):
                await self.check(at(10, 0, 1))
        self.assertEqual(len(self.host.commits), 2)

        # Also if commands.prepare raises outside loop / tx.save raises ValueError (>256 KiB)
        with patch.object(Commands, 'prepare', side_effect=ValueError('boom')):
            with self.assertLogs('timed-test', 'WARNING'):
                await self.check(at(10, 10, 1))

    async def test_long_broadcast_chunks_tellraw_within_budget(self):
        message = '§a' + ('服务器维护公告内容' * 40)
        commands = broadcast_commands(message)
        self.assertGreater(len(commands), 1)
        for cmd in commands:
            self.assertLessEqual(len(cmd.encode('utf-8')), MAX_BROADCAST_BYTES)
            self.assertTrue(cmd.startswith('tellraw @a '))
        self.assertTrue(json.loads(commands[1].removeprefix('tellraw @a '))['rawtext'][0]['text'].startswith('§a'))

        self.ctx.config = Settings(tasks=[Task('长公告', every_minutes=10, message=message)])
        await self.check(at(9, 55))
        await self.check(at(10, 0, 1))
        self.assertEqual(self.host.commands(), commands)

    def test_rejects_invalid_settings(self):
        for broken in (dict(every_minutes=5, daily_at='08:00'), dict(), dict(daily_at='8点'),
                       dict(daily_at='24:00'), dict(every_minutes=5, commands=[' '])):
            with self.assertRaises(ValueError, msg=broken):
                Task('任务', **{'message': 'hi', **broken})
        with self.assertRaises(ValueError):
            Task(' ', every_minutes=5, message='hi')
        with self.assertRaises(ValueError):
            Task('a' * 252, every_minutes=5, message='hi')
        with self.assertRaises(ValueError):
            Task('中' * 85, every_minutes=5, message='hi')
        Task('a' * 251, every_minutes=5, message='hi')
        with self.assertRaises(ValueError):
            Task('空任务', every_minutes=5)
        with self.assertRaises(ValueError):
            Settings(tasks=[Task('a', every_minutes=5, message='x'), Task('a', every_minutes=9, message='y')])
        with self.assertRaises(ValueError):
            Settings(tasks=[Task(str(i), every_minutes=5, message='x') for i in range(65)])
        Settings(tasks=[Task(str(i), every_minutes=5, message='x') for i in range(64)])


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


class TimedCommandsServeTests(unittest.IsolatedAsyncioTestCase):
    async def test_serve_lifecycle_runs_initial_check_and_drains(self):
        host = FakeHost()
        reader = asyncio.StreamReader(limit=1 << 20)
        writer = ServeWriter()
        peer = ManagedPeer(reader, writer)
        plugin = TimedCommands()
        current_time = at(9, 30)
        plugin.now = lambda: current_time

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
                        'protocol_major': 1, 'protocol_minor': 0, 'installation_id': 'inst-1', 'generation': 1,
                        'capabilities': [], 'limits': {
                            'max_frame_bytes': 1 << 20, 'outgoing_requests': 128,
                            'inbound_requests': 32, 'queue_frames': 32}}}})
                elif method == 'capabilities.get':
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {'session': 's1'}}})
                elif method in ('state.get', 'state.commit'):
                    res = await host.call(method, payload)
                    send({'type': 'response', 'id': req_id, 'payload': {'result': res}})

        with tempfile.TemporaryDirectory() as tmp:
            serve_task = asyncio.create_task(plugin.serve(peer, data_dir=Path(tmp)))
            pump_task = asyncio.create_task(pump())
            try:
                send({'type': 'request', 'id': 'v1', 'method': 'lifecycle.validate',
                      'payload': {'installation_id': 'inst-1', 'generation': 1, 'revision': 1}})
                self.assertIn('result', (await wait_response('v1'))['payload'])
                send({'type': 'request', 'id': 'a1', 'method': 'lifecycle.activate',
                      'payload': {'installation_id': 'inst-1', 'generation': 1, 'revision': 1, 'state_version': 1}})
                self.assertTrue((await wait_response('a1'))['payload']['result']['activated'])
                for _ in range(30):
                    if host.commits:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(len(host.commits), 1)
                self.assertIn('task:整点公告', host.values)
                send({'type': 'request', 'id': 'dr1', 'method': 'lifecycle.drain',
                      'payload': {'installation_id': 'inst-1', 'generation': 1}})
                self.assertTrue((await wait_response('dr1'))['payload']['result']['drained'])
                self.assertFalse(serve_task.done())
            finally:
                serve_task.cancel()
                pump_task.cancel()
                await asyncio.gather(serve_task, pump_task, return_exceptions=True)


if __name__ == '__main__':
    unittest.main()
