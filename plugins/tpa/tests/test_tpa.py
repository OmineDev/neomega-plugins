"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
import json
import logging
from types import SimpleNamespace
import unittest

from neomega_runtime import Event, PluginContext
from neomega_runtime.operations import Operations
from main import Settings, Tpa

ROSTER = [{'name': 'Steve', 'uuid': 'u1', 'xuid': '1'},
          {'name': 'Stella', 'uuid': 'u2', 'xuid': '2'},
          {'name': 'Alex', 'uuid': 'u3', 'xuid': '3'}]


class FakeHost:
    """players.list / state.get / state.commit / events.commit with the Host's global revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.players = copy.deepcopy(ROSTER)

    async def call(self, method, payload, timeout=None):
        if method == 'players.list':
            return {'session': 's1', 'players': copy.deepcopy(self.players)}
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        assert method in ('state.commit', 'events.commit'), method
        assert payload['expected_state_revision'] == self.revision
        assert payload['mutations'] == []   # 这个插件不写存储键
        self.revision += 1
        self.commits.append(payload)
        return {'commit_id': payload['commit_id'], 'revision': self.revision, 'duplicate': False,
                'operation_ids': [f'op{i}' for i in range(len(payload['actions']))]}

    def commands(self):
        return [action['arguments']['cmd'] for action in self.commits[-1]['actions']]

    def teleports(self):
        return [command for command in self.commands() if command.startswith('tp ')]

    def reply(self, name):
        """Text sent to one player in the last commit."""
        texts, target = [], f'tellraw @a[name="{name}"] '
        for command in self.commands():
            if command.startswith(target):
                texts += [part['text'] for part in json.loads(command[len(target):])['rawtext']]
        return ''.join(texts)


class TpaTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('tpa-test')), None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.plugin = Tpa()
        await self.plugin.on_start(self.ctx)
        self.t = 1000.0
        self.plugin.now = lambda: self.t

    async def say(self, name, message):
        xuid = next(p['xuid'] for p in ROSTER if p['name'] == name)
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'chat.received', 'payload': {'player': name, 'xuid': xuid, 'message': message},
            'subscription_id': 'chat', 'event_id': 'e1', 'delivery_token': 'd1'}))
        # Every event is acknowledged in exactly one commit.
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    async def test_request_notifies_both_in_one_commit(self):
        await self.say('Steve', '!tpa Alex')
        self.assertEqual(len(self.host.commits), 1)
        self.assertIn('已向 Alex 发送请求', self.host.reply('Steve'))
        self.assertEqual(self.host.reply('Alex'),
                         'Steve 想传送到你身边，发送 !同意 接受，!拒绝 拒绝（60 秒内有效）。')
        self.assertEqual(self.host.teleports(), [])

    async def test_exact_name_wins_and_unique_prefix_matches(self):
        self.host.players.append({'name': 'Ste', 'uuid': 'u4', 'xuid': '4'})
        await self.say('Alex', '!tpa Ste')
        self.assertIn('已向 Ste 发送请求', self.host.reply('Alex'))
        self.t += 20
        await self.say('Steve', '！tpa al')
        self.assertIn('已向 Alex 发送请求', self.host.reply('Steve'))

    async def test_ambiguous_and_missing_targets(self):
        await self.say('Alex', '!tpa st')
        self.assertEqual(self.host.reply('Alex'), '有多位玩家匹配“st”：Steve、Stella，请输入更完整的名字。')
        await self.say('Alex', '!tpa Bob')
        self.assertEqual(self.host.reply('Alex'), '没有找到在线玩家“Bob”。')
        await self.say('Steve', '!同意')
        self.assertEqual(self.host.reply('Steve'), '你没有待处理的传送请求。')

    async def test_cannot_request_self(self):
        await self.say('Alex', '!tpa alex')
        self.assertEqual(self.host.reply('Alex'), '不能向自己发送传送请求。')
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.reply('Alex'), '你没有待处理的传送请求。')

    async def test_tpa_and_tpahere_teleport_in_opposite_directions(self):
        await self.say('Steve', '!tpa Alex')
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.teleports(), ['tp @a[name="Steve"] @a[name="Alex"]'])
        self.assertEqual(self.host.reply('Alex'), '已同意 Steve 的传送请求。')
        self.assertEqual(self.host.reply('Steve'), 'Alex 同意了你的传送请求。')
        self.t += 20
        await self.say('Steve', '!tpahere Alex')
        self.assertIn('Steve 想把你拉到身边', self.host.reply('Alex'))
        await self.say('Alex', '!ty')
        self.assertEqual(self.host.teleports(), ['tp @a[name="Alex"] @a[name="Steve"]'])

    async def test_accept_latest_or_named_request(self):
        await self.say('Steve', '!tpa Alex')
        await self.say('Stella', '!tpahere Alex')
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.teleports(), ['tp @a[name="Alex"] @a[name="Stella"]'])
        await self.say('Alex', '!tpaccept steve')
        self.assertEqual(self.host.teleports(), ['tp @a[name="Steve"] @a[name="Alex"]'])
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.reply('Alex'), '你没有待处理的传送请求。')

    async def test_named_accept_without_request(self):
        await self.say('Steve', '!tpa Alex')
        await self.say('Alex', '!同意 Stella')
        self.assertEqual(self.host.reply('Alex'), '没有来自“Stella”的传送请求。')
        self.assertEqual(self.host.teleports(), [])

    async def test_new_request_replaces_old_one(self):
        await self.say('Steve', '!tpa Alex')
        self.t += 20
        await self.say('Steve', '!tpahere Alex')
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.teleports(), ['tp @a[name="Alex"] @a[name="Steve"]'])
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.reply('Alex'), '你没有待处理的传送请求。')

    async def test_expired_request_cannot_be_accepted(self):
        await self.say('Steve', '!tpa Alex')
        self.t += 61
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.teleports(), [])
        self.assertEqual(self.host.reply('Alex'), '你没有待处理的传送请求。')

    async def test_deny_notifies_sender(self):
        await self.say('Steve', '!tpa Alex')
        await self.say('Alex', '!jj')
        self.assertEqual(self.host.teleports(), [])
        self.assertEqual(self.host.reply('Alex'), '已拒绝 Steve 的传送请求。')
        self.assertEqual(self.host.reply('Steve'), 'Alex 拒绝了你的传送请求。')
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.reply('Alex'), '你没有待处理的传送请求。')

    async def test_cooldown_limits_requests(self):
        await self.say('Steve', '!tpa Alex')
        self.t += 3
        await self.say('Steve', '!tpa Stella')
        self.assertEqual(self.host.reply('Steve'), '请求太频繁了，请 7 秒后再试。')
        self.assertEqual(self.host.reply('Stella'), '')
        self.t += 7
        await self.say('Steve', '!tpa Stella')
        self.assertIn('已向 Stella 发送请求', self.host.reply('Steve'))

    async def test_failed_lookup_does_not_start_cooldown(self):
        await self.say('Steve', '!tpa Bob')
        await self.say('Steve', '!tpa Alex')
        self.assertIn('已向 Alex 发送请求', self.host.reply('Steve'))

    async def test_offline_sender_invalidates_request(self):
        await self.say('Steve', '!tpa Alex')
        self.host.players = [p for p in self.host.players if p['name'] != 'Steve']
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.teleports(), [])
        self.assertEqual(self.host.reply('Alex'), 'Steve 已经下线，这条请求已失效。')
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.reply('Alex'), '你没有待处理的传送请求。')

    async def test_other_chat_and_bad_arguments_are_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!tpa', '!tpa a b', '!tpa "没闭合'):
            await self.say('Steve', message)
            self.assertEqual(self.host.commits[-1]['actions'], [])


if __name__ == '__main__':
    unittest.main()
