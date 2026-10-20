"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
import json
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock

from neomega_runtime import Event, PlayerNotFound, PluginContext
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from main import Settings, Welcome

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}


class FakeHost:
    """state.get / state.commit / events.commit with the Host's global revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.fail = None

    async def call(self, method, payload, timeout=None):
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        assert method in ('state.commit', 'events.commit'), method
        if self.fail is not None:
            raise self.fail
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


class WelcomeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('welcome-test')), None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.resolve_player = AsyncMock(return_value=dict(PLAYER))
        self.ctx.every = Mock()
        self.plugin = Welcome()
        await self.plugin.on_start(self.ctx)

    async def deliver(self, kind, payload):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': kind, 'payload': payload,
            'subscription_id': 'sub', 'event_id': 'e1', 'delivery_token': 'd1'}))
        # Every event is acknowledged in exactly one commit.
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    async def join(self):
        await self.deliver('player.list_added', {'uuid': 'u1', 'name': '玩家', 'xuid': '42'})

    async def say(self, message):
        await self.deliver('chat.received', {'player': '玩家', 'xuid': '42', 'message': message})

    async def test_first_join_sends_gift_and_records_seen_in_one_commit(self):
        await self.join()
        self.assertEqual(len(self.host.commits), 1)
        self.assertIn('give @a[name="玩家"] bread 16', self.host.commands())
        self.assertEqual(self.host.reply(), '欢迎 玩家 第一次来到服务器！送你一份新手礼包。')
        self.assertEqual(self.host.values, {'seen:u1': True})

    async def test_returning_player_only_gets_welcome(self):
        await self.join()
        await self.join()
        self.assertFalse(any(command.startswith('give') for command in self.host.commands()))
        self.assertEqual(self.host.reply(), '欢迎回来，玩家！输入 !帮助 查看可用命令。')
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_empty_first_join_text_falls_back_to_welcome(self):
        self.ctx.config = Settings(first_join_text='', first_join_commands=[])
        await self.join()
        self.assertEqual(self.host.reply(), '欢迎回来，玩家！输入 !帮助 查看可用命令。')
        self.assertFalse(any(command.startswith('give') for command in self.host.commands()))
        self.assertEqual(self.host.values, {'seen:u1': True})

    async def test_player_already_left_is_only_acknowledged(self):
        self.ctx.resolve_player.side_effect = PlayerNotFound('left')
        await self.join()
        self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_help_command(self):
        for message in ('!帮助', '！help', '!bz'):
            await self.say(message)
            self.assertEqual(self.host.reply(), '可用命令：!签到、!回家、!tpa 玩家名。有问题请联系管理员。')

    async def test_other_chat_is_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!帮助 "没闭合'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_announcements_rotate_in_order(self):
        self.ctx.every.assert_called_once()
        self.assertEqual(self.ctx.every.call_args.args[0], 600)
        self.ctx.config = Settings(announcements=['第一条', '第二条'])
        sent = []
        for _ in range(3):
            await self.plugin.announce(self.ctx)
            self.assertEqual(len(self.host.commands()), 1)
            self.assertTrue(self.host.commands()[0].startswith('tellraw @a '))
            sent.append(self.host.reply())
        self.assertEqual(sent, ['第一条', '第二条', '第一条'])
        self.assertNotIn('delivery_token', self.host.commits[-1])

    async def test_no_timer_without_announcements(self):
        ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('welcome-test')), None)
        ctx.config, ctx.every = Settings(announcements=[]), Mock()
        await Welcome().on_start(ctx)
        ctx.every.assert_not_called()

    async def test_announcement_commit_errors_do_not_crash(self):
        for error in (ConnectionError('lost'), IPCRejected('revision_conflict', 'r1', False)):
            self.host.fail = error
            with self.assertLogs('welcome-test', 'WARNING'):
                await self.plugin.announce(self.ctx)
        self.host.fail = None
        await self.join()
        self.assertEqual(self.host.values, {'seen:u1': True})

    def test_rejects_blank_entries(self):
        with self.assertRaises(ValueError):
            Settings(first_join_commands=[' '])
        with self.assertRaises(ValueError):
            Settings(announcements=[''])


if __name__ == '__main__':
    unittest.main()
