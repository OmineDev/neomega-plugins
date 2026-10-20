"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
from datetime import datetime, timedelta, timezone
import json
import logging
from types import SimpleNamespace
import unittest

from neomega_runtime import PluginContext
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from main import Settings, Task, TimedCommands

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
        for failure in (ConnectionError('lost'), IPCRejected('revision_conflict', 'r1', True)):
            self.host.fail = failure
            with self.assertLogs('timed-test', 'WARNING'):
                await self.check(at(10, 0, 5))
            self.assertEqual(len(self.host.commits), 1)
        self.host.fail = None
        await self.check(at(10, 0, 15))
        self.assertEqual(len(self.host.commits), 2)
        self.assertEqual(len(self.host.commands()), 1)

    def test_rejects_invalid_settings(self):
        for broken in (dict(every_minutes=5, daily_at='08:00'), dict(), dict(daily_at='8点'),
                       dict(daily_at='24:00'), dict(every_minutes=5, commands=[' '])):
            with self.assertRaises(ValueError, msg=broken):
                Task('任务', **{'message': 'hi', **broken})
        with self.assertRaises(ValueError):
            Task(' ', every_minutes=5, message='hi')
        with self.assertRaises(ValueError):
            Task('空任务', every_minutes=5)
        with self.assertRaises(ValueError):
            Settings(tasks=[Task('a', every_minutes=5, message='x'), Task('a', every_minutes=9, message='y')])
        with self.assertRaises(ValueError):
            Settings(tasks=[Task(str(i), every_minutes=5, message='x') for i in range(65)])
        Settings(tasks=[Task(str(i), every_minutes=5, message='x') for i in range(64)])


if __name__ == '__main__':
    unittest.main()
