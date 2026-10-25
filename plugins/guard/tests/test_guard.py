"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
import dataclasses
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from neomega_runtime import Event, PluginContext
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config import load_config
from neomega_runtime.config_schema import schema_for_config
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from main import Guard, Settings

ROOT = Path(__file__).resolve().parents[1]
ROBOT = {'name': '机器人', 'uuid': 'bot', 'xuid': '1'}
STEVE = {'name': 'Steve', 'uuid': 'u-steve', 'xuid': '2'}
ALEX = {'name': 'Alex', 'uuid': 'u-alex', 'xuid': '3'}
BOB = {'name': 'Bob', 'uuid': 'u-bob', 'xuid': '4'}
ADMIN = {'name': 'Owner', 'uuid': 'u-owner', 'xuid': '5'}


def command_receipt(state, success_count, messages):
    return {'operation_id': 'q1', 'state': state, 'can_retry': False,
            'result': {'state': state, 'evidence': 'acknowledged',
                       'result': {'success_count': success_count, 'output_type': 3, 'data_set': 0,
                                  'messages': messages}}}


LEVEL_OK = command_receipt('succeeded', 1, [
    {'success': True, 'message': 'commands.testfor.success', 'parameters': ['Steve']}])
LEVEL_LOW = command_receipt('failed', 0, [
    {'success': False, 'message': 'commands.generic.noTargetMatch', 'parameters': []}])
SYNTAX_ERROR = command_receipt('failed', 0, [
    {'success': False, 'message': 'commands.generic.syntax', 'parameters': ['', 'lm', '']}])
LEVEL_UNKNOWN = {'operation_id': 'q1', 'state': 'unknown', 'can_retry': False,
                 'result': {'state': 'unknown', 'evidence': '', 'reason': 'response_omitted'}}


class FakeHost:
    """players/robot reads, command queries and state.get / state.commit / events.commit with revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.online = [dict(ROBOT), dict(STEVE)]
        self.bot_name = ROBOT['name']
        self.level = LEVEL_OK
        self.queries, self.fail_commit = [], None

    async def call(self, method, payload, timeout=None):
        if method == 'players.list':
            return {'session': 's1', 'players': copy.deepcopy(self.online)}
        if method == 'robot.status':
            return {'session': 's1', 'in_game': True, 'bot_name': self.bot_name, 'player_count': len(self.online)}
        if method == 'operations.submit':
            self.queries.append(payload['arguments']['cmd'])
            return {'operation_id': 'q1', 'state': 'accepted', 'duplicate': False}
        if method == 'operations.get':
            return copy.deepcopy(self.level)
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        assert method in ('state.commit', 'events.commit'), method
        if self.fail_commit is not None:
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

    def commands(self, commit=-1):
        return [action['arguments']['cmd'] for action in self.commits[commit]['actions']]

    def told(self, name, commit=-1):
        """Text sent to one player by tellraw in one commit."""
        texts, target = [], f'tellraw @a[name="{name}"] '
        for command in self.commands(commit):
            if command.startswith(target):
                texts += [part['text'] for part in json.loads(command[len(target):])['rawtext']]
        return ''.join(texts)

    def kicks(self, commit=-1):
        return [command for command in self.commands(commit) if command.startswith('kick ')]

    def records(self):
        return self.values.get('records', [])


class GuardTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('guard-test')), None)
        self.ctx.config = Settings(admins=['owner'])
        self.ctx._operations = Operations(self.host, 's1')
        self.timers = []
        self.ctx.later = lambda seconds, callback, name=None: self.timers.append((seconds, callback))
        self.plugin = Guard()
        self.t = 1000.0
        self.plugin.now = lambda: self.t
        await self.plugin.on_start(self.ctx)
        self.t += 5   # 启动后没有积压事件，第一条事件就是实时的

    def configure(self, **changes):
        self.ctx.config = dataclasses.replace(self.ctx.config, **changes)

    async def deliver(self, kind, payload, **extra):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': kind, 'payload': payload,
            'subscription_id': 'sub', 'event_id': 'e1', 'delivery_token': 'd1', **extra}))
        # Every event is acknowledged in exactly one commit.
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    async def join(self, player):
        if all(p['uuid'] != player['uuid'] for p in self.host.online):
            self.host.online.append(dict(player))
        await self.deliver('player.list_added', dict(player))

    async def say(self, player, message):
        if all(p['uuid'] != player['uuid'] for p in self.host.online):
            self.host.online.append(dict(player))
        await self.deliver('chat.received', {'player': player['name'], 'xuid': player['xuid'], 'message': message})

    def assert_only_acknowledged(self):
        self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def run_timers(self):
        timers, self.timers = self.timers, []
        for _, callback in timers:
            await callback()

    # ---- 默认配置与配置校验 ----

    async def test_defaults_only_watch_spam(self):
        settings = Settings()
        self.assertTrue(settings.spam_check)
        self.assertFalse(settings.level_check or settings.name_check or settings.player_limit_check)
        self.assertEqual(settings.spam_punish_commands, [])
        self.ctx.config = settings
        await self.join(dict(STEVE, name='广告Steve'))
        self.assert_only_acknowledged()
        self.assertEqual(self.host.queries, [])

    def test_static_schema_and_example_match_settings(self):
        self.assertEqual(json.loads((ROOT / 'config.schema.json').read_text()), schema_for_config(Settings))
        example = json.loads((ROOT / 'config.example.json').read_text())
        self.assertEqual(example, dataclasses.asdict(Settings()))
        self.assertEqual(load_config(Settings, ROOT / 'config.example.json'), Settings())

    def test_rejects_invalid_settings(self):
        for changes in ({'name_patterns': ['[']}, {'name_keywords': [' ']}, {'admins': ['']},
                        {'player_limit_exempt': ['']}, {'spam_punish_commands': ['']},
                        {'level_message': ' '}, {'spam_punish_message': ''}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                Settings(**changes)
        for value in ({'min_level': 0}, {'max_players': 0}, {'kick_delay_seconds': 31},
                      {'record_limit': 5000}, {'spam_window_seconds': 0}, {'unknown': 1}):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'config.json'
                path.write_text(json.dumps(value))
                with self.assertRaises((ConfigError, ValueError)):
                    load_config(Settings, path)

    # ---- 进服门槛 ----

    async def test_name_keyword_warns_then_kicks_after_delay(self):
        self.configure(name_check=True, name_keywords=['广告'])
        player = dict(BOB, name='卖广告的Bob')
        await self.join(player)
        self.assertEqual(self.host.told(player['name']), '[服务器守卫] 你的名字含有本服务器不允许的内容，请改名后再来。')
        self.assertTrue(any(c.startswith('titleraw @a[name="卖广告的Bob"] subtitle ') for c in self.host.commands()))
        self.assertTrue(any(c.startswith('titleraw @a[name="卖广告的Bob"] title ') for c in self.host.commands()))
        self.assertEqual(self.host.kicks(), [])   # 先提示，延后踢出
        self.assertEqual([seconds for seconds, _ in self.timers], [3])
        record = self.host.records()[-1]
        self.assertEqual((record['player'], record['uuid'], record['rule'], record['action']),
                         ('卖广告的Bob', 'u-bob', 'name', 'kick'))
        self.assertIn('广告', record['detail'])
        await self.run_timers()
        self.assertEqual(self.host.kicks(), ['kick @a[name="卖广告的Bob"] 你的名字含有本服务器不允许的内容，请改名后再来。'])
        self.assertNotIn('delivery_token', self.host.commits[-1])

    async def test_name_keyword_ignores_case_and_regex_matches(self):
        self.configure(name_check=True, name_keywords=['bot'], name_patterns=[r'^\d+$'])
        await self.join(dict(BOB, name='SpamBOT'))
        self.assertEqual(len(self.timers), 1)
        await self.join(dict(ALEX, name='123456'))
        self.assertEqual(self.host.records()[-1]['detail'], r'名字匹配正则 ^\d+$')
        await self.join(STEVE)
        self.assert_only_acknowledged()

    async def test_zero_delay_kicks_in_the_same_commit(self):
        self.configure(name_check=True, name_keywords=['bob'], kick_delay_seconds=0)
        await self.join(BOB)
        self.assertEqual(self.host.kicks(), ['kick @a[name="Bob"] 你的名字含有本服务器不允许的内容，请改名后再来。'])
        self.assertEqual(self.timers, [])

    async def test_kick_reason_keeps_selectors_literal(self):
        self.configure(name_check=True, name_keywords=['bob'], kick_delay_seconds=0,
                       name_message='请联系 @a 管理\n改名')
        await self.join(BOB)
        self.assertEqual(self.host.kicks(), ['kick @a[name="Bob"] 请联系 ＠a 管理 改名'])

    async def test_level_check_passes_when_selector_matches(self):
        self.configure(level_check=True, min_level=15)
        await self.join(STEVE)
        self.assertEqual(self.host.queries, ['testfor @a[name="Steve",lm=15]'])
        self.assert_only_acknowledged()

    async def test_level_check_kicks_when_no_target_matches(self):
        self.configure(level_check=True, min_level=15)
        self.host.level = LEVEL_LOW
        await self.join(STEVE)
        self.assertEqual(self.host.told('Steve'), '[服务器守卫] 需要达到 15 级才能进入本服务器，升级后再来吧。')
        self.assertEqual(self.host.records()[-1]['rule'], 'level')
        await self.run_timers()
        self.assertEqual(self.host.kicks(), ['kick @a[name="Steve"] 需要达到 15 级才能进入本服务器，升级后再来吧。'])

    async def test_level_check_accepts_success_message_with_parameters(self):
        # 实服 Host 返回的消息把参数拼在键后面：commands.testfor.success Steve。
        self.configure(level_check=True, min_level=15)
        self.host.level = command_receipt('succeeded', 1, [
            {'success': True, 'message': 'commands.testfor.success Steve', 'parameters': ['Steve']}])
        with self.assertNoLogs('guard-test', 'WARNING'):
            await self.join(STEVE)
        self.assert_only_acknowledged()

    async def test_join_checks_work_when_roster_has_no_xuid(self):
        # 实服 Host 的在线名单不带 XUID，进服事件带着；按两者一起找会找不到人，门槛整个失效。
        self.configure(name_check=True, name_keywords=['bob'], kick_delay_seconds=0)
        self.host.online.append(dict(BOB, xuid=''))
        await self.deliver('player.list_added', dict(BOB))
        self.assertEqual(self.host.kicks(), ['kick @a[name="Bob"] 你的名字含有本服务器不允许的内容，请改名后再来。'])

    async def test_roster_with_another_xuid_is_not_this_player(self):
        self.configure(name_check=True, name_keywords=['bob'], kick_delay_seconds=0)
        self.host.online.append(dict(BOB, xuid='99'))
        await self.deliver('player.list_added', dict(BOB))
        self.assert_only_acknowledged()

    async def test_inconclusive_level_check_lets_player_in(self):
        self.configure(level_check=True)
        for receipt in (LEVEL_UNKNOWN, SYNTAX_ERROR):
            self.host.level = receipt
            with self.subTest(receipt=receipt['state']), self.assertLogs('guard-test', 'WARNING'):
                await self.join(STEVE)
            self.assert_only_acknowledged()
        self.assertEqual(self.host.records(), [])

    async def test_admins_and_robot_skip_join_checks(self):
        self.configure(level_check=True, name_check=True, name_keywords=['o'], player_limit_check=True, max_players=1)
        self.host.level = LEVEL_LOW
        await self.join(ADMIN)
        self.assert_only_acknowledged()
        await self.join(ROBOT)
        self.assert_only_acknowledged()
        self.assertEqual(self.host.queries, [])

    async def test_unknown_robot_name_skips_join_checks(self):
        self.configure(name_check=True, name_keywords=['机器'])
        self.host.bot_name = None
        with self.assertLogs('guard-test', 'WARNING'):
            await self.join(ROBOT)
        self.assert_only_acknowledged()

    async def test_last_known_robot_name_is_kept(self):
        self.configure(name_check=True, name_keywords=['机器'])
        await self.join(STEVE)
        self.host.bot_name = None
        await self.join(ROBOT)
        self.assert_only_acknowledged()

    async def test_player_limit_kicks_only_the_newest_join(self):
        self.configure(player_limit_check=True, max_players=2)
        await self.join(STEVE)
        await self.join(ALEX)
        self.assert_only_acknowledged()   # 机器人不计人数
        await self.join(BOB)
        self.assertEqual(self.host.told('Bob'), '[服务器守卫] 服务器人数已满（上限 2 人），请稍后再来。')
        self.assertEqual(self.host.records()[-1]['detail'], '在线 3 人，上限 2 人')
        # 踢出生效前重投进服事件：已在线的老玩家不受影响，Bob 也不会重复处置。
        for player in (STEVE, ALEX, BOB):
            await self.join(player)
            self.assert_only_acknowledged()
        self.assertEqual(len(self.host.records()), 1)

    async def test_player_limit_exempt_by_name_or_uuid(self):
        self.configure(player_limit_check=True, max_players=1, player_limit_exempt=['u-alex', 'BOB'])
        await self.join(STEVE)
        await self.join(ALEX)
        self.assert_only_acknowledged()
        await self.join(BOB)
        self.assert_only_acknowledged()
        # 白名单玩家满员也能进，但仍计入在线人数。
        carol = {'name': 'Carol', 'uuid': 'u-carol', 'xuid': '6'}
        await self.join(carol)
        self.assertEqual(self.host.records()[-1]['detail'], '在线 4 人，上限 1 人')

    async def test_player_limit_counts_the_already_online_as_older(self):
        self.configure(player_limit_check=True, max_players=1)
        self.host.online.append(dict(ALEX))   # 插件启动前就在线，没有收到进服事件
        await self.join(STEVE)
        self.assertEqual(self.host.records()[-1]['player'], 'Steve')

    # ---- 防刷屏 ----

    async def test_flood_within_window_is_warned(self):
        for index in range(6):
            await self.say(STEVE, f'第 {index} 句')
            self.assert_only_acknowledged()
        await self.say(STEVE, '第 6 句')
        self.assertEqual(self.host.told('Steve'), '[服务器守卫] 请不要刷屏，再犯会被处罚。')
        record = self.host.records()[-1]
        self.assertEqual((record['rule'], record['action'], record['detail']),
                         ('spam', 'warn', '10 秒内发了 7 条消息，第 1 次警告'))
        self.assertEqual(self.host.kicks(), [])
        self.assertEqual(self.timers, [])

    async def restart(self):
        """新进程：内存清空，启动时间为当前 self.t。"""
        config = self.ctx.config
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('guard-test')), None)
        self.ctx.config = config
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.later = lambda seconds, callback, name=None: self.timers.append((seconds, callback))
        self.plugin = Guard()
        self.plugin.now = lambda: self.t
        await self.plugin.on_start(self.ctx)

    async def test_backlog_delivered_at_start_is_not_spam(self):
        # 新装插件时 Host 会补发积压的旧聊天，一口气到达；这些不能当成刚刚刷屏。
        await self.restart()
        for index in range(12):
            await self.say(STEVE, '!兑换 LIVETWICE' if index % 2 else f'旧消息 {index}')
            self.assert_only_acknowledged()
            self.t += 0.05
        self.assertEqual(self.host.records(), [])
        self.t += 2   # 积压处理完，之后是实时聊天
        for _ in range(3):
            await self.say(STEVE, '刷屏')
        self.assertEqual(self.host.records()[-1]['detail'], '10 秒内同一句话发了 3 次，第 1 次警告')

    async def test_backlog_wait_is_bounded(self):
        await self.restart()
        for index in range(130):   # 一直有消息、间隔不到 1 秒，最多等 60 秒
            await self.say(dict(ALEX, name=f'P{index}', uuid=f'u{index}', xuid=f'x{index}'), '你好')
            self.t += 0.5
        for _ in range(3):
            await self.say(STEVE, '刷屏')
        self.assertEqual(self.host.records()[-1]['player'], 'Steve')

    async def test_chat_from_previous_connection_is_not_spam(self):
        for _ in range(3):
            await self.deliver('chat.received', {'player': 'Steve', 'xuid': '2', 'message': '刷屏'}, session='old')
            self.assert_only_acknowledged()
        self.assertEqual(self.host.records(), [])

    async def test_messages_outside_window_do_not_count(self):
        for index in range(20):
            await self.say(STEVE, f'第 {index} 句')
            self.assert_only_acknowledged()
            self.t += 2
        self.assertEqual(self.host.records(), [])

    async def test_repeated_message_is_detected(self):
        await self.say(STEVE, '666')
        await self.say(STEVE, '  666 ')
        self.assert_only_acknowledged()
        await self.say(STEVE, '666')
        self.assertEqual(self.host.records()[-1]['detail'], '10 秒内同一句话发了 3 次，第 1 次警告')

    async def test_repeat_check_can_be_disabled(self):
        self.configure(spam_repeat_limit=0)
        for _ in range(6):
            await self.say(STEVE, '666')
        self.assert_only_acknowledged()

    async def test_second_violation_after_warning_kicks(self):
        for _ in range(3):
            await self.say(STEVE, '刷屏')
        self.assertEqual(self.host.records()[-1]['action'], 'warn')
        # 警告后重新计数：再发两句还不算违规。
        await self.say(STEVE, '刷屏')
        await self.say(STEVE, '刷屏')
        self.assert_only_acknowledged()
        await self.say(STEVE, '刷屏')
        self.assertEqual(self.host.told('Steve'), '[服务器守卫] 你因刷屏被请出服务器。')
        self.assertEqual(self.host.records()[-1]['action'], 'kick')
        await self.run_timers()
        self.assertEqual(self.host.kicks(), ['kick @a[name="Steve"] 你因刷屏被请出服务器。'])
        # 等待踢出期间继续刷屏不再重复处置。
        for _ in range(5):
            await self.say(STEVE, '刷屏')
            self.assert_only_acknowledged()

    async def test_punish_commands_replace_kick(self):
        self.configure(spam_warnings=0, spam_punish_commands=['/gamemode adventure {player}', 'tag {player} add muted'],
                       spam_punish_message='你因刷屏被禁言。')
        for _ in range(3):
            await self.say(STEVE, '刷屏')
        self.assertIn('gamemode adventure @a[name="Steve"]', self.host.commands())
        self.assertIn('tag @a[name="Steve"] add muted', self.host.commands())
        self.assertEqual(self.host.told('Steve'), '[服务器守卫] 你因刷屏被禁言。')
        self.assertEqual(self.host.records()[-1]['action'], 'punish')
        self.assertEqual(self.host.kicks(), [])
        self.assertEqual(self.timers, [])

    async def test_warnings_expire(self):
        for _ in range(3):
            await self.say(STEVE, '刷屏')
        self.t += 11 * 60
        for _ in range(3):
            await self.say(STEVE, '刷屏')
        self.assertEqual([r['action'] for r in self.host.records()], ['warn', 'warn'])

    async def test_several_warnings_before_punishment(self):
        self.configure(spam_warnings=2)
        for _ in range(9):
            await self.say(STEVE, '刷屏')
        self.assertEqual([r['action'] for r in self.host.records()], ['warn', 'warn', 'kick'])

    async def test_admins_robot_and_disabled_check_are_not_counted(self):
        for player in (ADMIN, dict(ADMIN, name='OWNER'), ROBOT):
            for _ in range(10):
                await self.say(player, '刷屏')
                self.assert_only_acknowledged()
        self.configure(spam_check=False)
        for _ in range(10):
            await self.say(STEVE, '刷屏')
            self.assert_only_acknowledged()

    async def test_commands_count_toward_spam(self):
        for _ in range(3):
            await self.say(STEVE, '!签到')
        self.assertEqual(self.host.records()[-1]['rule'], 'spam')

    # ---- 记录与查看 ----

    async def test_records_are_capped(self):
        self.configure(record_limit=10, spam_warnings=100)
        for index in range(12):
            for _ in range(3):
                await self.say(STEVE, f'刷屏{index}')
        records = self.host.records()
        self.assertEqual(len(records), 10)
        self.assertEqual(records[-1]['detail'], '10 秒内同一句话发了 3 次，第 12 次警告')

    async def test_admin_views_recent_records(self):
        await self.say(ADMIN, '!守卫记录')
        self.assertEqual(self.host.told('Owner'), '还没有处置记录。')
        for _ in range(3):
            await self.say(STEVE, '刷屏')
        await self.say(ADMIN, '！guard 3')
        text = self.host.told('Owner')
        self.assertTrue(text.startswith('最近 1 条处置记录（北京时间）：\n'))
        self.assertIn('Steve 刷屏，警告：10 秒内同一句话发了 3 次，第 1 次警告', text)
        await self.say(ADMIN, '!守卫记录 99')
        self.assertIn('1 到 20', self.host.told('Owner'))

    async def test_non_admin_cannot_view_records(self):
        await self.say(ALEX, '!守卫记录')
        self.assertEqual(self.host.told('Alex'), '只有管理员可以查看处置记录。')
        self.assertEqual(self.host.records(), [])

    async def test_failed_delayed_kick_is_logged(self):
        self.configure(name_check=True, name_keywords=['bob'])
        await self.join(BOB)
        self.host.fail_commit = IPCRejected('revision_conflict', 'r1', False)
        with self.assertLogs('guard-test', 'WARNING'):
            await self.run_timers()


if __name__ == '__main__':
    unittest.main()
