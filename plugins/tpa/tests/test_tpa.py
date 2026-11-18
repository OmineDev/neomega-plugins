"""Drive the plugin through the real SDK storage against an in-memory Host."""
import asyncio
import copy
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from neomega_runtime import Event, PluginContext
from neomega_runtime.config import load_config
from neomega_runtime.config_schema import check_config_schema, schema_for_config
from neomega_runtime.operations import Operations
from main import Settings, Tpa, match

ROSTER = [{'name': 'Steve', 'uuid': 'u1', 'xuid': '1'},
          {'name': 'Stella', 'uuid': 'u2', 'xuid': '2'},
          {'name': 'Alex', 'uuid': 'u3', 'xuid': '3'}]


class FakeHost:
    """players.list / state.get / state.commit / events.commit with the Host's global revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.players = copy.deepcopy(ROSTER)
        self.bot_name = 'Bot'
        self.fail_players = False
        self.fail_commit = False

    async def call(self, method, payload, timeout=None):
        if method == 'robot.status':
            return {'session': 's1', 'in_game': True, 'bot_name': self.bot_name,
                    'player_count': len(self.players)}
        if method == 'players.list':
            if self.fail_players:
                raise TimeoutError('players slow')
            return {'session': 's1', 'players': copy.deepcopy(self.players)}
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        assert method in ('state.commit', 'events.commit'), method
        if self.fail_commit:
            raise ConnectionError('commit lost')
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

    async def test_match_rejects_empty_query_and_prefers_case_insensitive_exact(self):
        self.assertEqual(match(['Steve', 'Steve2'], ''), [])
        self.assertEqual(match(['Steve', 'Steve2'], 'steve'), ['Steve'])
        await self.say('Alex', '!tpa ""')
        self.assertEqual(self.host.reply('Alex'), '没有找到在线玩家“”。')
        await self.say('Steve', '!tpa Alex')
        await self.say('Alex', '!同意 ""')
        self.assertEqual(self.host.reply('Alex'), '没有来自“”的传送请求。')

    async def test_answer_keeps_request_when_players_or_save_fails(self):
        await self.say('Steve', '!tpa Alex')
        self.host.fail_players = True
        with self.assertRaises(TimeoutError):
            await self.say('Alex', '!同意')
        self.host.fail_players = False
        self.host.fail_commit = True
        with self.assertRaises(Exception):
            await self.say('Alex', '!同意')
        self.host.fail_commit = False
        await self.say('Alex', '!同意')
        self.assertEqual(self.host.teleports(), ['tp @a[name="Steve"] @a[name="Alex"]'])

    async def test_skips_robot_in_targets_and_chat(self):
        self.host.players.append({'name': 'Bot', 'uuid': 'u-bot', 'xuid': '99'})
        await self.say('Steve', '!tpa Bot')
        self.assertEqual(self.host.reply('Steve'), '没有找到在线玩家“Bot”。')
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'chat.received', 'payload': {'player': 'Bot', 'xuid': '99', 'message': '!tpa Alex'},
            'subscription_id': 'chat', 'event_id': 'ebot', 'delivery_token': 'dbot'}))
        self.assertEqual(self.host.commits[-1]['actions'], [])

    def test_schema_example_and_purposes_match_settings(self):
        root = Path(__file__).resolve().parents[1]
        schema = json.loads((root / 'config.schema.json').read_text())
        check_config_schema(schema)
        self.assertEqual(schema, schema_for_config(Settings))
        self.assertEqual(load_config(Settings, root / 'config.example.json'), Settings())
        manifest = json.loads((root / 'manifest.json').read_text())
        self.assertIn('event:chat.received', manifest['permissions']['purposes'])

    async def test_serve_ack_lifecycle(self):
        host = self.host
        events = [
            {'kind': 'chat.received', 'payload': {'player': 'Steve', 'xuid': '1', 'message': '你好'},
             'subscription_id': 'chat', 'event_id': 'ev1', 'delivery_token': 'dt1'},
        ]

        class DriveWorker:
            def __init__(self, peer, **callbacks):
                self.peer, self.callbacks = peer, callbacks
                self.log = logging.getLogger('tpa-serve-test')
                self.session = 'install-1'
                self._scopes = self._streams = None

            async def run(self):
                await self.callbacks['on_validate'](self.peer, {})
                await self.callbacks['on_activate'](self.peer, {})
                for ev in events:
                    await self.callbacks['on_event'](self.peer, ev)
                await self.callbacks['on_drain'](self.peer, {})

            def spawn(self, coro, *, name=None):
                return asyncio.create_task(coro)

        async def call_with_caps(method, payload, timeout=None):
            if method == 'capabilities.get':
                return {'session': 's1', 'capabilities': []}
            return await host.call(method, payload, timeout=timeout)

        peer = SimpleNamespace(call=call_with_caps)
        with tempfile.TemporaryDirectory() as tmp, patch('neomega_runtime.plugin.Worker', DriveWorker):
            await Tpa().serve(peer, data_dir=tmp)
        self.assertEqual([c['event_id'] for c in host.commits], ['ev1'])


if __name__ == '__main__':
    unittest.main()
