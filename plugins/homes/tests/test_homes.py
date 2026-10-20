"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
import json
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext, ProtocolError
from neomega_runtime.operations import Operations
from main import Homes, Settings

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}


class FakeHost:
    """state.get / state.commit / events.commit with the Host's global revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []

    async def call(self, method, payload, timeout=None):
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        assert method in ('state.commit', 'events.commit'), method
        assert payload['expected_state_revision'] == self.revision
        for mutation in payload['mutations']:
            if mutation.get('delete'):
                self.values.pop(mutation['key'], None)
            else:
                self.values[mutation['key']] = mutation['value']
        self.revision += 1
        self.commits.append(payload)
        return {'commit_id': payload['commit_id'], 'revision': self.revision, 'duplicate': False,
                'operation_ids': [f'op{i}' for i in range(len(payload['actions']))]}

    def commands(self):
        return [action['arguments']['cmd'] for action in self.commits[-1]['actions']]

    def teleports(self):
        return [command for command in self.commands() if not command.startswith('tellraw')]

    def reply(self):
        texts = []
        for command in self.commands():
            if command.startswith('tellraw'):
                texts += [part['text'] for part in json.loads(command[command.index('{'):])['rawtext']]
        return ''.join(texts)


def at(x, y, z, dimension=0):
    return {'complete': True, 'entities': [
        {'uniqueId': '-1', 'dimension': dimension, 'position': {'x': x, 'y': y, 'z': z}}]}


class HomesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('homes-test')), None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.resolve_player = AsyncMock(return_value=dict(PLAYER))
        self.ctx.positions.sample = AsyncMock(return_value=at(1.54, 64, -3.46))
        self.plugin = Homes()
        await self.plugin.on_start(self.ctx)
        self.now = 1000.0
        self.plugin.clock = lambda: self.now

    async def say(self, message):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'chat.received', 'payload': {'player': '玩家', 'xuid': '42', 'message': message},
            'subscription_id': 'chat', 'event_id': 'e1', 'delivery_token': 'd1'}))
        # Every event is acknowledged in exactly one commit.
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    async def test_set_home_records_rounded_position_and_dimension(self):
        await self.say('!设家')
        self.ctx.positions.sample.assert_awaited_with(target='players', name='玩家', limit=1)
        self.assertEqual(len(self.host.commits), 1)
        self.assertEqual(self.host.values['player:u1'],
                         {'家': {'dimension': 0, 'x': 1.5, 'y': 64.0, 'z': -3.5}})
        self.assertIn('已设置家「家」', self.host.reply())
        self.ctx.positions.sample.return_value = at(10, 70, 20, dimension=1)
        await self.say('!sethome 矿洞')
        self.assertEqual(self.host.values['player:u1']['矿洞'], {'dimension': 1, 'x': 10.0, 'y': 70.0, 'z': 20.0})

    async def test_same_name_overwrites(self):
        await self.say('!设家')
        self.ctx.positions.sample.return_value = at(5, 6, 7)
        await self.say('！设家 家')
        self.assertEqual(self.host.values['player:u1'], {'家': {'dimension': 0, 'x': 5.0, 'y': 6.0, 'z': 7.0}})
        self.assertIn('已更新「家」的位置', self.host.reply())

    async def test_limit_rejects_new_names_but_allows_overwrite(self):
        for name in ('a', 'b', 'c'):
            await self.say(f'!设家 {name}')
        await self.say('!设家 d')
        self.assertNotIn('d', self.host.values['player:u1'])
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        self.assertIn('上限 3 个', self.host.reply())
        self.assertIn('!删家', self.host.reply())
        await self.say('!设家 a')
        self.assertIn('已更新「a」', self.host.reply())

    async def test_lowered_limit_keeps_existing_homes(self):
        for name in ('a', 'b', 'c'):
            await self.say(f'!设家 {name}')
        self.ctx.config = Settings(max_homes=1)
        await self.say('!家列表')
        self.assertIn('（3/1）', self.host.reply())
        await self.say('!回家 c')
        self.assertTrue(self.host.teleports())

    async def test_go_home_teleports_in_each_dimension(self):
        for dimension, word in ((0, 'overworld'), (1, 'nether'), (2, 'the_end')):
            self.ctx.positions.sample.return_value = at(1.5, 64, -3.5, dimension)
            await self.say(f'!设家 d{dimension}')
            self.now += 100
            await self.say(f'!home d{dimension}')
            self.assertEqual(self.host.teleports(),
                             [f'execute in {word} run tp @a[name="玩家"] 1.5 64.0 -3.5'])
            self.assertIn(f'已传送到「d{dimension}」', self.host.reply())

    async def test_missing_home_lists_existing(self):
        await self.say('!hj')
        self.assertIn('你还没有设家', self.host.reply())
        await self.say('!设家 矿洞')
        await self.say('!回家')
        self.assertEqual(self.host.teleports(), [])
        self.assertIn('你还没有叫「家」的家，已有的家：「矿洞」', self.host.reply())

    async def test_cooldown_limits_going_home(self):
        await self.say('!设家')
        await self.say('!回家')
        self.assertTrue(self.host.teleports())
        self.now += 3.2
        await self.say('!回家')
        self.assertEqual(self.host.teleports(), [])
        self.assertIn('请 7 秒后再试', self.host.reply())
        self.now += 6.8
        await self.say('!回家')
        self.assertTrue(self.host.teleports())

    async def test_delete_home_and_last_one_removes_record(self):
        await self.say('!设家 a')
        await self.say('!设家 b')
        await self.say('!删家 a')
        self.assertEqual(list(self.host.values['player:u1']), ['b'])
        self.assertIn('已删除家「a」', self.host.reply())
        await self.say('!delhome x')
        self.assertIn('没有叫「x」的家，已有的家：「b」', self.host.reply())
        await self.say('!删家 b')
        self.assertNotIn('player:u1', self.host.values)

    async def test_list_homes(self):
        await self.say('!homes')
        self.assertIn('你还没有设家', self.host.reply())
        await self.say('!设家')
        await self.say('!设家 矿洞')
        await self.say('!家列表')
        self.assertIn('你的家（2/3）：「家」、「矿洞」', self.host.reply())

    async def test_position_read_failure_only_replies(self):
        for failure in (dict(side_effect=ProtocolError('position query failed')),
                        dict(side_effect=TimeoutError()),
                        dict(return_value={'complete': True, 'entities': []})):
            self.ctx.positions.sample = AsyncMock(**failure)
            await self.say('!设家')
            self.assertEqual(self.host.commits[-1]['mutations'], [])
            self.assertIn('暂时读不到你的位置，请稍后再试', self.host.reply())
        self.assertNotIn('player:u1', self.host.values)

    async def test_too_long_name_is_refused_without_reading_position(self):
        await self.say('!设家 ' + '长' * 17)
        self.ctx.positions.sample.assert_not_awaited()
        self.assertIn('最长 16 个字', self.host.reply())

    async def test_other_chat_and_bad_arguments_are_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!设家 "没闭合', '!删家', '!回家 a b'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])


if __name__ == '__main__':
    unittest.main()
