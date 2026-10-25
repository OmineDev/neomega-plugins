"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext, ProtocolError, ServiceRejected
from neomega_runtime.config import load_config
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config_schema import check_config_schema, schema_for_config
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceWaitTimeout
from main import Settings, Warp, Warps

ROOT = Path(__file__).resolve().parents[1]
ADMIN = {'name': 'Admin', 'uuid': 'u-admin', 'xuid': '1'}
PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}
PRESETS = [Warp(name='出生点', x=0, y=64, z=0, description='新手集合处'),
           Warp(name='下界枢纽', dimension=1, x=12.5, y=70, z=-8.5)]


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

    def game_commands(self):
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


class WarpsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.now = 1_800_000_000.0
        self.ctx = await self.start(Settings(admins=['admin'], warps=PRESETS))
        self.as_player(PLAYER)

    async def start(self, settings):
        """A fresh plugin process on the same Host data, as after a restart."""
        ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('warps-test')), None)
        ctx.config = settings
        ctx._operations = Operations(self.host, 's1')
        ctx.positions.sample = AsyncMock(return_value=at(1.54, 64, -3.46))
        ctx.services.call = AsyncMock(side_effect=IPCRejected('permission_denied', 'r1', False))
        self.plugin = Warps()
        await self.plugin.on_start(ctx)
        self.plugin.clock = lambda: self.now
        return ctx

    def as_player(self, player):
        self.speaker = player
        self.ctx.resolve_player = AsyncMock(return_value=dict(player))

    async def say(self, message, event_id='e1'):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'chat.received',
            'payload': {'player': self.speaker['name'], 'xuid': self.speaker['xuid'], 'message': message},
            'subscription_id': 'chat', 'event_id': event_id, 'delivery_token': 'd1'}))
        # Every event is acknowledged in exactly one commit.
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    # ---- 列表与传送 ----

    async def test_list_numbers_presets_then_game_warps(self):
        self.as_player(ADMIN)
        await self.say('!设传送点 矿洞 "东边的大矿洞"')
        self.as_player(PLAYER)
        await self.say('!传送点')
        self.assertEqual(self.host.reply(), '公共传送点：\n1. 出生点（主世界）：新手集合处\n'
                                            '2. 下界枢纽（下界）\n3. 矿洞（主世界）：东边的大矿洞\n'
                                            '发送 !传送 <编号或名称> 前往。')
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_empty_list(self):
        self.ctx.config = Settings()
        await self.say('!warps')
        self.assertIn('还没有公共传送点', self.host.reply())
        await self.say('!传送')
        self.assertIn('还没有公共传送点', self.host.reply())

    async def test_teleport_by_number_and_name_across_dimensions(self):
        await self.say('!传送 2')
        self.assertEqual(self.host.game_commands(),
                         ['execute in nether run tp @a[name="玩家"] 12.5 70 -8.5'])
        self.assertIn('已传送到「下界枢纽」', self.host.reply())
        self.now += 100
        await self.say('！cs 出生点')
        self.assertEqual(self.host.game_commands(), ['execute in overworld run tp @a[name="玩家"] 0 64 0'])

    async def test_game_warp_in_the_end_is_reachable(self):
        self.as_player(ADMIN)
        self.ctx.positions.sample.return_value = at(100.04, 50, 0.25, dimension=2)
        await self.say('!设传送点 End')
        self.assertEqual(self.host.values['warps'],
                         {'End': {'dimension': 2, 'x': 100.0, 'y': 50.0, 'z': 0.2, 'description': ''}})
        self.assertIn('末地 100 50 0.2', self.host.reply())
        await self.say('!warp end')
        self.assertEqual(self.host.game_commands(), ['execute in the_end run tp @a[name="Admin"] 100 50 0.2'])

    async def test_unknown_warp_and_out_of_range_number(self):
        for query in ('不存在', '9', '0'):
            await self.say(f'!传送 {query}')
            self.assertEqual(self.host.game_commands(), [])
            self.assertIn(f'没有找到传送点「{query}」', self.host.reply())
        self.assertNotIn('player:u1', self.host.values)

    # ---- 管理员 ----

    async def test_admin_creates_updates_and_deletes_game_warp(self):
        self.as_player(ADMIN)
        await self.say('!设传送点 矿洞 入口')
        self.ctx.positions.sample.assert_awaited_with(target='players', name='Admin', limit=1)
        self.assertEqual(self.host.values['warps'],
                         {'矿洞': {'dimension': 0, 'x': 1.5, 'y': 64.0, 'z': -3.5, 'description': '入口'}})
        self.assertIn('已设置传送点「矿洞」（主世界 1.5 64 -3.5）', self.host.reply())
        self.ctx.positions.sample.return_value = at(5, 6, 7)
        await self.say('!setwarp 矿洞')
        self.assertEqual(self.host.values['warps']['矿洞'],
                         {'dimension': 0, 'x': 5.0, 'y': 6.0, 'z': 7.0, 'description': '入口'})
        self.assertIn('已更新传送点「矿洞」', self.host.reply())
        await self.say('!删传送点 3')
        self.assertNotIn('warps', self.host.values)
        self.assertIn('已删除传送点「矿洞」', self.host.reply())

    async def test_admin_name_is_case_insensitive_and_delete_by_name(self):
        self.as_player({'name': 'ADMIN', 'uuid': 'u2', 'xuid': '2'})
        await self.say('!设传送点 a')
        await self.say('!设传送点 b')
        await self.say('!delwarp A')
        self.assertEqual(list(self.host.values['warps']), ['b'])

    async def test_non_admin_is_refused_without_reading_position(self):
        for message in ('!设传送点 矿洞', '!删传送点 1', '!设主城', '!删主城'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['mutations'], [])
            self.assertEqual(self.host.game_commands(), [])
            self.assertIn('只有管理员', self.host.reply())
        self.ctx.positions.sample.assert_not_awaited()

    async def test_presets_cannot_be_changed_in_game(self):
        self.as_player(ADMIN)
        await self.say('!设传送点 出生点')
        self.assertIn('「出生点」是插件设置里的传送点，请在设置里修改', self.host.reply())
        await self.say('!删传送点 1')
        self.assertIn('请在设置里删除', self.host.reply())
        await self.say('!删传送点 下界枢纽')
        self.assertIn('请在设置里删除', self.host.reply())
        self.assertNotIn('warps', self.host.values)
        self.ctx.positions.sample.assert_not_awaited()

    async def test_shadowed_game_warp_can_still_be_deleted(self):
        self.as_player(ADMIN)
        self.ctx.config = Settings(admins=['admin'])
        await self.say('!设传送点 出生点')
        self.ctx.config = Settings(admins=['admin'], warps=PRESETS)
        await self.say('!传送点')
        self.assertEqual(self.host.reply().count('出生点'), 1)
        await self.say('!删传送点 出生点')
        self.assertIn('已删除传送点「出生点」', self.host.reply())
        self.assertNotIn('warps', self.host.values)

    async def test_invalid_names_and_limit(self):
        self.as_player(ADMIN)
        for message, text in (('!设传送点 123', '不能是纯数字'), ('!设传送点 ' + '长' * 17, '最长 16 个字'),
                              ('!设传送点 a ' + '说' * 33, '说明最长 32 个字')):
            await self.say(message)
            self.assertIn(text, self.host.reply())
        self.ctx.positions.sample.assert_not_awaited()
        self.host.values['warps'] = {f'w{i}': {'dimension': 0, 'x': 0, 'y': 0, 'z': 0, 'description': ''}
                                     for i in range(62)}
        await self.say('!设传送点 新的')
        self.assertIn('已经有 64 个', self.host.reply())
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        await self.say('!设传送点 w0')
        self.assertIn('已更新传送点「w0」', self.host.reply())

    async def test_position_read_failure_only_replies(self):
        self.as_player(ADMIN)
        for failure in (dict(side_effect=ProtocolError('position query failed')),
                        dict(side_effect=TimeoutError()),
                        dict(return_value={'complete': True, 'entities': []})):
            self.ctx.positions.sample = AsyncMock(**failure)
            for message in ('!设传送点 a', '!设主城', '!设重生点'):
                await self.say(message)
                self.assertEqual(self.host.commits[-1]['mutations'], [])
                self.assertEqual(self.host.game_commands(), [])
                self.assertIn('暂时读不到你的位置', self.host.reply())

    async def test_unknown_dimension_is_refused(self):
        self.as_player(ADMIN)
        self.ctx.positions.sample.return_value = at(0, 0, 0, dimension=7)
        await self.say('!设传送点 a')
        self.assertIn('你所在的维度不支持', self.host.reply())
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    # ---- 主城 ----

    async def test_city_from_settings_game_override_and_reset(self):
        await self.say('!主城')
        self.assertIn('服务器还没有设置主城', self.host.reply())
        self.assertEqual(self.host.game_commands(), [])
        self.ctx.config = Settings(admins=['admin'], city_enabled=True, city_dimension=1,
                                   city_x=100.5, city_y=70, city_z=-200.5)
        await self.say('!主城')
        self.assertEqual(self.host.game_commands(),
                         ['execute in nether run tp @a[name="玩家"] 100.5 70 -200.5'])
        self.assertIn('已传送到主城', self.host.reply())

        self.as_player(ADMIN)
        self.ctx.positions.sample.return_value = at(8.04, 65, 9, dimension=0)
        await self.say('!设主城')
        self.assertEqual(self.host.values['city'], {'dimension': 0, 'x': 8.0, 'y': 65.0, 'z': 9.0})
        self.assertIn('已把主城设在这里（主世界 8 65 9）', self.host.reply())
        await self.say('!spawn')
        self.assertEqual(self.host.game_commands(), ['execute in overworld run tp @a[name="Admin"] 8 65 9'])

        await self.say('!删主城')
        self.assertNotIn('city', self.host.values)
        self.assertIn('现在使用插件设置里的主城坐标', self.host.reply())
        await self.say('!delcity')
        self.assertIn('游戏里没有设置过主城', self.host.reply())

    # ---- 冷却 ----

    async def test_teleport_cooldown_is_shared_and_survives_restart(self):
        self.ctx.config = Settings(warps=PRESETS, city_enabled=True)
        await self.say('!传送 1')
        self.assertEqual(self.host.values['player:u1'], {'teleport': self.now})
        self.now += 3.2
        await self.say('!主城')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('请 7 秒后再试', self.host.reply())

        self.ctx = await self.start(Settings(warps=PRESETS, city_enabled=True))
        self.as_player(PLAYER)
        await self.say('!传送 2')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('请 7 秒后再试', self.host.reply())
        self.now += 6.8
        await self.say('!传送 2')
        self.assertTrue(self.host.game_commands())

    async def test_cooldown_is_bounded_when_clock_moves_back(self):
        await self.say('!传送 1')
        self.now -= 3600
        await self.say('!传送 1')
        self.assertIn('请 10 秒后再试', self.host.reply())

    async def test_zero_cooldown(self):
        self.ctx.config = Settings(warps=PRESETS, teleport_cooldown_seconds=0)
        await self.say('!传送 1')
        await self.say('!传送 1')
        self.assertTrue(self.host.game_commands())

    # ---- 重生点 ----

    async def test_set_spawnpoint_uses_block_position_and_dimension(self):
        self.ctx.positions.sample.return_value = at(-3.5, 64.9, 10.2, dimension=1)
        await self.say('!设重生点')
        self.assertEqual(self.host.game_commands(),
                         ['execute in nether run spawnpoint @a[name="玩家"] -4 64 10'])
        self.assertIn('已把重生点设在这里（下界 -4 64 10）', self.host.reply())
        self.assertEqual(self.host.values['player:u1'], {'spawnpoint': self.now})

    async def test_spawnpoint_cooldown_is_separate_and_persistent(self):
        await self.say('!设重生点')
        await self.say('!传送 1')
        self.assertTrue(self.host.game_commands())
        self.now += 30
        self.ctx = await self.start(Settings(warps=PRESETS))
        self.as_player(PLAYER)
        await self.say('!setspawnpoint')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('设重生点还在冷却中，请 30 秒后再试', self.host.reply())
        self.now += 30
        await self.say('!设重生点')
        self.assertTrue(self.host.game_commands())
        self.assertEqual(set(self.host.values['player:u1']), {'teleport', 'spawnpoint'})

    async def test_spawnpoint_disabled(self):
        self.ctx.config = Settings(spawnpoint_enabled=False)
        await self.say('!设重生点')
        self.assertIn('没有开放 !设重生点', self.host.reply())
        self.ctx.positions.sample.assert_not_awaited()
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    # ---- 收费 ----

    def charged(self, **kwargs):
        self.ctx.config = Settings(warps=PRESETS, teleport_cost=5)
        self.ctx.services.call = AsyncMock(**kwargs)

    async def test_cost_is_charged_with_event_request_id(self):
        self.charged(return_value={'balance': 95, 'duplicate': False, 'currency': '金币', 'pending': False})
        await self.say('!传送 出生点', event_id='evt-7')
        self.ctx.services.call.assert_awaited_once_with(
            'plugin.neomega.economy.pay',
            {'player_id': 'u1', 'amount': 5, 'reason': '传送到出生点', 'request_id': 'warp-evt-7'}, timeout=5)
        self.assertTrue(self.host.game_commands())
        self.assertIn('花费 5 金币，剩余 95 金币', self.host.reply())

    async def test_insufficient_funds_does_not_teleport_or_start_cooldown(self):
        self.charged(side_effect=ServiceRejected('insufficient_funds'))
        await self.say('!传送 1')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('金币不足，传送一次需要 5 金币', self.host.reply())
        self.assertNotIn('player:u1', self.host.values)

    async def test_economy_not_installed_is_free(self):
        self.charged(side_effect=IPCRejected('permission_denied', 'r1', False))
        await self.say('!传送 1')
        self.assertTrue(self.host.game_commands())
        self.assertNotIn('花费', self.host.reply())

    async def test_uncertain_charge_retries_same_request_then_refuses(self):
        self.charged(side_effect=ServiceWaitTimeout('slow'))
        await self.say('!传送 1')
        self.assertEqual(self.ctx.services.call.await_count, 2)
        first, second = (call.args[1]['request_id'] for call in self.ctx.services.call.await_args_list)
        self.assertEqual(first, second)
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('扣款结果暂时不确定', self.host.reply())

        self.charged(side_effect=[ServiceRejected('unavailable'),
                                  {'balance': 1, 'duplicate': True, 'currency': '金币', 'pending': False}])
        await self.say('!传送 1')
        self.assertTrue(self.host.game_commands())

    async def test_other_rejection_refuses(self):
        self.charged(side_effect=ServiceRejected('player_offline'))
        await self.say('!传送 1')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('扣款失败', self.host.reply())

    async def test_no_charge_during_cooldown_or_for_unknown_warp(self):
        self.charged(return_value={'balance': 95, 'duplicate': False, 'currency': '金币', 'pending': False})
        await self.say('!传送 不存在')
        await self.say('!传送 1')
        self.now += 1
        await self.say('!传送 1')
        self.assertEqual(self.ctx.services.call.await_count, 1)

    async def test_spawnpoint_is_free(self):
        self.charged(return_value={'balance': 95, 'duplicate': False, 'currency': '金币', 'pending': False})
        await self.say('!设重生点')
        self.ctx.services.call.assert_not_awaited()

    # ---- 其他聊天 ----

    async def test_other_chat_and_bad_arguments_are_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!传送 "没闭合', '!设传送点', '!传送 a b', '!主城 x'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])


class ConfigTests(unittest.TestCase):
    def load(self, value):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.json'
            path.write_text(json.dumps(value, ensure_ascii=False))
            return load_config(Settings, path)

    def test_static_schema_matches_settings(self):
        schema = json.loads((ROOT / 'config.schema.json').read_text())
        check_config_schema(schema)
        self.assertEqual(schema, schema_for_config(Settings))

    def test_example_presets_load(self):
        settings = self.load(json.loads((ROOT / 'config.example.json').read_text()))
        self.assertEqual(settings.admins, ['Steve'])
        self.assertEqual(settings.warps[0], Warp(name='出生点', x=0.0, y=64.0, z=0.0, description='新手集合处'))
        self.assertEqual(settings.warps[1].dimension, 1)
        self.assertTrue(settings.city_enabled)

    def test_defaults(self):
        settings = self.load({})
        self.assertEqual((settings.warps, settings.teleport_cost, settings.city_enabled), ([], 0, False))

    def test_invalid_configurations_are_rejected(self):
        warp = {'name': 'a', 'x': 0, 'y': 0, 'z': 0}
        for value in ({'warps': [warp, dict(warp, name='A')]},
                      {'warps': [dict(warp, name='12')]},
                      {'warps': [dict(warp, name='长' * 17)]},
                      {'warps': [dict(warp, description='说' * 33)]},
                      {'warps': [dict(warp, dimension=3)]},
                      {'warps': [{'name': 'a', 'x': 0, 'y': 0}]},
                      {'warps': [dict(warp, name=f'w{i}') for i in range(65)]},
                      {'admins': ['  ']},
                      {'city_dimension': -1},
                      {'teleport_cost': -1},
                      {'unknown': 1}):
            with self.subTest(value=value), self.assertRaises(ConfigError):
                self.load(value)


if __name__ == '__main__':
    unittest.main()
