"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
from datetime import date
import json
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from main import OnlineTime, Reward, Settings

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}
ROBOT = {'name': '机器人', 'uuid': 'bot', 'xuid': '1'}


class FakeHost:
    """players/robot reads plus state.get / state.commit / events.commit with the Host's global revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.online, self.fail_commit, self.reads = [dict(ROBOT), dict(PLAYER)], None, []

    async def call(self, method, payload, timeout=None):
        if method == 'players.list':
            if self.online is None:
                raise IPCRejected('players_unavailable', 'r1', True)
            return {'session': 's1', 'players': copy.deepcopy(self.online)}
        if method == 'robot.status':
            return {'session': 's1', 'in_game': True, 'bot_name': ROBOT['name'], 'player_count': len(self.online or [])}
        if method == 'state.get':
            keys = payload.get('keys')
            self.reads.append(keys)
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        assert method in ('state.commit', 'events.commit'), method
        if self.fail_commit:
            raise self.fail_commit
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

    def reply(self):
        texts = []
        for command in self.commands():
            if command.startswith('tellraw'):
                texts += [part['text'] for part in json.loads(command[command.index('{'):])['rawtext']]
        return ''.join(texts)


class OnlineTimeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        worker = SimpleNamespace(peer=self.host, log=logging.getLogger('online-time-test'),
                                 spawn=lambda coroutine, name=None: coroutine.close())
        self.ctx = PluginContext(worker, None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.resolve_player = AsyncMock(return_value=dict(PLAYER))
        self.plugin = OnlineTime()
        await self.plugin.on_start(self.ctx)
        self.day = date(2026, 10, 21)
        self.plugin.today = lambda ctx: self.day

    async def say(self, message):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'chat.received', 'payload': {'player': '玩家', 'xuid': '42', 'message': message},
            'subscription_id': 'chat', 'event_id': 'e1', 'delivery_token': 'd1'}))
        # Every event is acknowledged in exactly one commit.
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    async def test_each_round_adds_one_minute_and_skips_the_robot(self):
        await self.plugin.tick(self.ctx)
        await self.plugin.tick(self.ctx)
        self.assertEqual(len(self.host.commits), 2)
        self.assertEqual(self.host.values['player:u1'], {'name': '玩家', 'total': 2, 'day': '2026-10-21', 'today': 2})
        self.assertNotIn('player:bot', self.host.values)
        self.assertEqual(self.host.values['top'], [{'id': 'u1', 'name': '玩家', 'minutes': 2}])
        self.assertEqual(self.host.reads[-1], ['player:u1', 'top'])

    async def test_new_day_restarts_today(self):
        self.host.values['player:u1'] = {'name': '旧名', 'total': 100, 'day': '2026-10-20', 'today': 30}
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.host.values['player:u1'], {'name': '玩家', 'total': 101, 'day': '2026-10-21', 'today': 1})

    async def test_reward_is_given_only_when_today_reaches_the_threshold(self):
        self.host.values['player:u1'] = {'name': '玩家', 'total': 500, 'day': '2026-10-21', 'today': 59}
        await self.plugin.tick(self.ctx)
        self.assertEqual(len(self.host.commits), 1)
        self.assertIn('give @a[name="玩家"] bread 5', self.host.commands())
        self.assertEqual(self.host.reply(), '今天已在线 1 小时，送你 5 个面包！')
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.host.commands(), [])
        self.assertEqual(self.host.values['player:u1']['today'], 61)

    async def test_threshold_already_passed_gives_nothing(self):
        self.ctx.config = Settings(rewards=[Reward(30, ['give {player} apple 1'], '半小时啦')])
        self.host.values['player:u1'] = {'name': '玩家', 'total': 500, 'day': '2026-10-21', 'today': 45}
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.host.commands(), [])

    async def test_ranking_keeps_only_top_ten(self):
        self.host.values['top'] = [{'id': f'p{i}', 'name': f'老玩家{i}', 'minutes': 100 - i} for i in range(10)]
        self.host.values['player:u1'] = {'name': '玩家', 'total': 95, 'day': '2026-10-21', 'today': 1}
        await self.plugin.tick(self.ctx)
        top = self.host.values['top']
        self.assertEqual(len(top), 10)
        self.assertEqual(top[5], {'id': 'u1', 'name': '玩家', 'minutes': 96})
        self.assertNotIn('p9', [entry['id'] for entry in top])
        await self.plugin.tick(self.ctx)
        self.assertEqual([entry['id'] for entry in self.host.values['top']].count('u1'), 1)

    async def test_many_players_are_counted_in_batches(self):
        self.host.online = [dict(ROBOT)] + [{'name': f'p{i}', 'uuid': '', 'xuid': str(i)} for i in range(300)]
        await self.plugin.tick(self.ctx)
        self.assertEqual(len(self.host.commits), 2)
        self.assertTrue(all(len(keys) <= 256 and keys[-1] == 'top' for keys in self.host.reads))
        self.assertEqual(self.host.values['player:xuid-299']['total'], 1)
        self.assertEqual(len(self.host.values['top']), 10)

    async def test_unresolved_save_is_logged_and_the_plugin_keeps_running(self):
        self.host.fail_commit = ConnectionError('lost')
        with self.assertLogs('online-time-test', 'WARNING') as logs:
            await self.plugin.tick(self.ctx)
        self.assertIn('下一轮重新统计', logs.output[0])
        self.assertNotIn('player:u1', self.host.values)
        self.host.fail_commit, self.host.online = None, None
        with self.assertLogs('online-time-test', 'WARNING'):
            await self.plugin.tick(self.ctx)
        self.host.online = [dict(PLAYER)]
        await self.plugin.tick(self.ctx)
        self.assertEqual(self.host.values['player:u1']['total'], 1)

    async def test_online_time_reply(self):
        await self.say('!在线时长')
        self.assertEqual(self.host.reply(), '今天在线 0 分钟，累计 0 分钟。')
        self.host.values['player:u1'] = {'name': '玩家', 'total': 750, 'day': '2026-10-21', 'today': 65}
        await self.say('！zxsc')
        self.assertEqual(self.host.reply(), '今天在线 1 小时 5 分钟，累计 12 小时 30 分钟。')
        self.day = date(2026, 10, 22)
        await self.say('!online')
        self.assertEqual(self.host.reply(), '今天在线 0 分钟，累计 12 小时 30 分钟。')

    async def test_ranking_reply(self):
        await self.say('!在线排行')
        self.assertEqual(self.host.reply(), '还没有人上榜，多在线一会儿吧。')
        self.host.values['top'] = [{'id': 'u1', 'name': '玩家', 'minutes': 125},
                                   {'id': 'u2', 'name': '路人', 'minutes': 60}]
        await self.say('!zxph')
        self.assertEqual(self.host.reply(), '在线时长排行：\n1. 玩家  2 小时 5 分钟\n2. 路人  1 小时')

    async def test_other_chat_is_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!在线时长 "没闭合'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])

    def test_rejects_ambiguous_settings(self):
        with self.assertRaises(ValueError):
            Settings(rewards=[Reward(60, [' '])])
        with self.assertRaises(ValueError):
            Settings(rewards=[Reward(60), Reward(60)])


if __name__ == '__main__':
    unittest.main()
