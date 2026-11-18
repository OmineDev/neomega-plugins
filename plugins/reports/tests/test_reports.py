"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
import dataclasses
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext, ProtocolError
from neomega_runtime.config import load_config
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config_schema import check_config_schema, schema_for_config
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from main import Reports, Settings

ROOT = Path(__file__).resolve().parents[1]
ADMIN = {'name': 'Admin', 'uuid': 'u-admin', 'xuid': '1'}
ADMIN2 = {'name': 'Mod', 'uuid': 'u-mod', 'xuid': '2'}
STEVE = {'name': 'Steve', 'uuid': 'u-steve', 'xuid': '3'}
ALEX = {'name': 'Alex', 'uuid': 'u-alex', 'xuid': '4'}
BOB = {'name': 'Bob', 'uuid': 'u-bob', 'xuid': '5'}


class FakeHost:
    """players.list and state.get / state.commit / events.commit with global revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.online = [dict(ADMIN), dict(STEVE), dict(ALEX)]

    async def call(self, method, payload, timeout=None):
        if method == 'players.list':
            return {'session': 's1', 'players': copy.deepcopy(self.online)}
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

    def commands(self, commit=-1):
        return [action['arguments']['cmd'] for action in self.commits[commit]['actions']]

    def told(self, name, commit=-1):
        texts, target = [], f'tellraw @a[name="{name}"] '
        for command in self.commands(commit):
            if command.startswith(target):
                texts += [part['text'] for part in json.loads(command[len(target):])['rawtext']]
        return ''.join(texts)

    def board(self):
        return self.values.get('board', {'next_id': 1, 'records': []})


def pos_sample(*rows):
    return {'complete': True, 'entities': list(rows)}


def entity_at(uuid_val, x, y, z, dimension=0):
    return {'uniqueId': uuid_val, 'dimension': dimension, 'position': {'x': x, 'y': y + 1.62, 'z': z}}


class ReportsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.mono = 1000.0
        self.wall = 1_800_000_000.0
        self.sample_mock = AsyncMock(return_value=pos_sample(
            entity_at('u-steve', 10.5, 64.0, -20.25, dimension=0),
            entity_at('u-alex', 100.0, 70.0, 50.0, dimension=1),
        ))
        self.ctx = await self.start(Settings(admins=['admin', 'mod']))
        self.mono += 5.0  # 越过启动积压窗口，后续事件按实时处理

    async def start(self, settings):
        ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('reports-test')), None)
        ctx.config = settings
        ctx._operations = Operations(self.host, 's1')
        ctx.positions.sample = self.sample_mock
        self.plugin = Reports()
        self.plugin.now = lambda: self.mono
        self.plugin.clock = lambda: self.wall
        await self.plugin.on_start(ctx)
        return ctx

    def configure(self, **changes):
        self.ctx.config = dataclasses.replace(self.ctx.config, **changes)

    async def deliver(self, kind, payload, event_id='e1', delivery_token='d1', **extra):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': kind, 'payload': payload,
            'subscription_id': 'sub', 'event_id': event_id, 'delivery_token': delivery_token, **extra}))
        self.assertEqual(self.host.commits[-1]['delivery_token'], delivery_token)

    async def say(self, player, message, event_id='e1'):
        await self.deliver('chat.received', {
            'player': player['name'], 'xuid': player['xuid'], 'message': message}, event_id=event_id)

    async def join(self, player, **extra):
        if all(p['uuid'] != player['uuid'] for p in self.host.online):
            self.host.online.append(dict(player))
        await self.deliver('player.list_added', dict(player), **extra)

    def assert_only_acknowledged(self):
        self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    # ---- 提交举报与坐标采样 ----

    async def test_submit_report_with_positions_and_notify_online_admins(self):
        self.host.online.append(dict(ADMIN2))
        await self.say(STEVE, '!举报 alex 他 恶意 破坏 建筑')
        self.sample_mock.assert_awaited_once_with(target='players', limit=64)

        board = self.host.board()
        self.assertEqual(board['next_id'], 2)
        self.assertEqual(len(board['records']), 1)
        record = board['records'][0]
        self.assertEqual(record['id'], 1)
        self.assertEqual(record['kind'], 'report')
        self.assertEqual(record['status'], 'open')
        self.assertEqual(record['reporter'], 'Steve')
        self.assertEqual(record['reporter_pid'], 'u-steve')
        self.assertEqual(record['target'], 'Alex')
        self.assertEqual(record['reason'], '他 恶意 破坏 建筑')
        self.assertTrue(record['created_at'].endswith('+08:00'))
        self.assertEqual(record['reporter_pos'], {'dimension': 0, 'x': 10.5, 'y': 64.0, 'z': -20.25})
        self.assertEqual(record['target_pos'], {'dimension': 1, 'x': 100.0, 'y': 70.0, 'z': 50.0})

        self.assertIn('已提交举报 #1', self.host.told('Steve'))
        self.assertIn('Steve 举报 Alex：他 恶意 破坏 建筑', self.host.told('Admin'))
        self.assertIn('主世界 10.5 64 -20.25', self.host.told('Admin'))
        self.assertIn('Steve 举报 Alex', self.host.told('Mod'))
        self.assertEqual(self.host.told('Alex'), '')

    async def test_report_offline_target_and_fullwidth_aliases(self):
        await self.say(STEVE, '！jb OfflineGriefer 在基地 放岩浆')
        record = self.host.board()['records'][0]
        self.assertEqual(record['target'], 'OfflineGriefer')
        self.assertEqual(record['reason'], '在基地 放岩浆')
        self.assertIsNotNone(record['reporter_pos'])
        self.assertIsNone(record['target_pos'])

        self.wall += 100
        await self.say(STEVE, '！report "Space Player" 恶意卡服')
        record2 = self.host.board()['records'][1]
        self.assertEqual((record2['id'], record2['target'], record2['reason']), (2, 'Space Player', '恶意卡服'))

    async def test_position_sample_failure_degrades_gracefully(self):
        for failure in (dict(side_effect=TimeoutError()),
                        dict(side_effect=ProtocolError('failed')),
                        dict(side_effect=IPCRejected('permission_denied', 'r1', False)),
                        dict(return_value={'complete': True, 'entities': []}),
                        dict(return_value={'complete': True, 'entities': [
                            {'uniqueId': 'u-steve', 'dimension': True, 'position': {'x': 1, 'y': 2, 'z': 3}}]})):
            self.host.values.clear()
            self.sample_mock.reset_mock()
            self.sample_mock.side_effect = failure.get('side_effect')
            self.sample_mock.return_value = failure.get('return_value')
            if 'side_effect' in failure:
                with self.assertLogs('reports-test', 'WARNING'):
                    await self.say(STEVE, '!举报 Alex 作弊')
            else:
                await self.say(STEVE, '!举报 Alex 作弊')
            record = self.host.board()['records'][0]
            self.assertIsNone(record['reporter_pos'])
            self.assertIsNone(record['target_pos'])
            self.assertIn('已提交举报 #1', self.host.told('Steve'))

    async def test_report_validation_rejects_bad_input_without_cooldown_or_sample(self):
        for message in ('!举报', '!举报 Alex', '!举报 "" 原因', '!举报 Alex "   "',
                        '!举报 Alex ' + '字' * 201):
            await self.say(STEVE, message)
            self.assertEqual(self.host.board()['records'], [])
            self.assertNotIn('cd:u-steve', self.host.values)
            self.assertTrue(self.host.told('Steve'))
        self.sample_mock.assert_not_awaited()

    async def test_cooldown_shared_between_report_and_feedback_and_survives_restart(self):
        await self.say(STEVE, '!举报 Alex 刷屏')
        self.assertEqual(len(self.host.board()['records']), 1)

        self.wall += 15.2
        self.sample_mock.reset_mock()
        await self.say(STEVE, '!留言 想提个建议')
        self.assertIn('45 秒后再试', self.host.told('Steve'))
        self.assertEqual(len(self.host.board()['records']), 1)

        await self.say(STEVE, '!举报 Alex 刷屏')
        self.assertIn('45 秒后再试', self.host.told('Steve'))
        self.sample_mock.assert_not_awaited()

        # 重启后冷却依然生效
        self.ctx = await self.start(self.ctx.config)
        self.mono += 5.0
        self.wall += 20.0
        await self.say(STEVE, '!留言 重启后测试')
        self.assertIn('25 秒后再试', self.host.told('Steve'))

        # 时钟回退不会拉长冷却
        self.wall -= 3600
        await self.say(STEVE, '!留言 时钟回退')
        self.assertIn('60 秒后再试', self.host.told('Steve'))

        # 超过冷却时间可提交
        self.wall += 3660
        await self.say(STEVE, '!ly 冷却结束')
        self.assertEqual(len(self.host.board()['records']), 2)

    async def test_zero_cooldown_and_disable_admin_submit_notify(self):
        self.configure(cooldown_seconds=0, notify_admins_on_submit=False)
        await self.say(STEVE, '!举报 Alex 第一次')
        await self.say(STEVE, '!留言 第二次')
        self.assertEqual(len(self.host.board()['records']), 2)
        self.assertEqual(self.host.told('Admin'), '')
        self.assertNotIn('cd:u-steve', self.host.values)

    # ---- 留言 ----

    async def test_feedback_command_and_disable_flag(self):
        await self.say(STEVE, '！feedback 建议 开放 更多 传送点')
        self.sample_mock.assert_not_awaited()
        record = self.host.board()['records'][0]
        self.assertEqual(record['kind'], 'feedback')
        self.assertEqual(record['reason'], '建议 开放 更多 传送点')
        self.assertEqual(record['target'], '')
        self.assertIn('已提交留言 #1', self.host.told('Steve'))
        self.assertIn('[新留言 #1] Steve：建议 开放 更多 传送点', self.host.told('Admin'))

        self.configure(allow_feedback=False)
        self.wall += 100
        await self.say(STEVE, '!留言 被关闭的留言')
        self.assertIn('未开放留言功能', self.host.told('Steve'))
        self.assertEqual(len(self.host.board()['records']), 1)

    async def test_feedback_validation(self):
        for message in ('!留言', '!留言 "  "', '!留言 ' + '测' * 201):
            await self.say(STEVE, message)
            self.assertEqual(self.host.board()['records'], [])
            self.assertTrue(self.host.told('Steve'))

    # ---- 我的举报 ----

    async def test_my_reports_shows_latest_five_and_reply(self):
        await self.say(STEVE, '!我的举报')
        self.assertIn('还没有提交过', self.host.told('Steve'))

        self.configure(cooldown_seconds=0)
        for index in range(1, 7):
            await self.say(STEVE, f'!留言 第{index}条留言')
        await self.say(ALEX, '!举报 Steve 测试干扰')
        await self.say(ADMIN, '!处理举报 6 已收到建议')

        # 玩家改名后依然按 pid 看到自己的记录
        renamed_steve = dict(STEVE, name='SteveNew')
        self.host.online = [dict(ADMIN), renamed_steve, dict(ALEX)]
        await self.say(renamed_steve, '！wdjb')
        reply = self.host.told('SteveNew')
        self.assertNotIn('#1 ', reply)
        self.assertNotIn('#7 ', reply)
        for num in (2, 3, 4, 5, 6):
            self.assertIn(f'#{num} ', reply)
        self.assertIn('[已处理] 留言', reply)
        self.assertIn('回复（Admin）：已收到建议', reply)

    # ---- 管理员查办 ----

    async def test_admin_list_pagination_and_all_reports(self):
        await self.say(STEVE, '!查举报')
        self.assertIn('只有管理员', self.host.told('Steve'))
        await self.say(STEVE, '!查全部举报')
        self.assertIn('只有管理员', self.host.told('Steve'))

        await self.say(ADMIN, '!查举报')
        self.assertIn('没有待处理的举报或留言', self.host.told('Admin'))
        await self.say(ADMIN, '!查全部举报')
        self.assertIn('还没有任何举报或留言记录', self.host.told('Admin'))

        self.configure(cooldown_seconds=0)
        for index in range(1, 7):
            await self.say(STEVE, f'!举报 Alex 违规行为{index}')
        await self.say(STEVE, '!留言 建议优化地图')
        await self.say(ADMIN, '!cljb 7 感谢建议')

        await self.say(ADMIN, '!cjb')
        page1 = self.host.told('Admin')
        self.assertIn('第 1/2 页，共 6 条', page1)
        self.assertIn('#6 [待处理] 举报 Alex', page1)
        self.assertIn('举报人 主世界 10.5 64 -20.25', page1)
        self.assertIn('目标 下界 100 70 50', page1)
        self.assertNotIn('#7', page1)

        await self.say(ADMIN, '！reports 2')
        page2 = self.host.told('Admin')
        self.assertIn('第 2/2 页，共 6 条', page2)
        self.assertIn('#1 [待处理]', page2)

        await self.say(ADMIN, '!查举报 3')
        self.assertIn('共 2 页', self.host.told('Admin'))
        await self.say(ADMIN, '!查举报 0')
        self.assertIn('大于 0 的整数', self.host.told('Admin'))

        await self.say(ADMIN, '！allreports 1')
        all_p1 = self.host.told('Admin')
        self.assertIn('第 1/2 页，共 7 条', all_p1)
        self.assertIn('#7 [已处理] 留言', all_p1)
        self.assertIn('处理（Admin', all_p1)
        self.assertIn('感谢建议', all_p1)

    async def test_resolve_online_and_offline_inbox_delivery(self):
        self.configure(cooldown_seconds=0)
        await self.say(STEVE, '!举报 Alex 偷拿箱子')
        await self.say(ALEX, '!留言 希望添加新人指引')

        # 权限与参数校验
        await self.say(STEVE, '!处理举报 1 自己处理')
        self.assertIn('只有管理员', self.host.told('Steve'))
        for bad in ('!处理举报', '!处理举报 1', '!处理举报 0 回复', '!处理举报 99 回复',
                    '!处理举报 1 "  "', '!处理举报 1 ' + '字' * 201):
            await self.say(ADMIN, bad)
            self.assertTrue(self.host.told('Admin'))

        # 在线提交人立即收到私聊，不写 inbox
        await self.say(ADMIN, '!resolve #1 已核实 并退还物品')
        self.assertIn('已处理 #1，并已私聊通知 Steve', self.host.told('Admin'))
        self.assertIn('你的举报 #1（目标：Alex）已处理（Admin）：已核实 并退还物品', self.host.told('Steve'))
        self.assertNotIn('inbox:u-steve', self.host.values)
        record1 = self.host.board()['records'][0]
        self.assertEqual((record1['status'], record1['resolved_by'], record1['reply']),
                         ('resolved', 'Admin', '已核实 并退还物品'))
        self.assertTrue(record1['resolved_at'].endswith('+08:00'))

        # 重复处理被拒绝
        await self.say(ADMIN, '!处理举报 1 再处理')
        self.assertIn('已经由 Admin 处理过了', self.host.told('Admin'))

        # 不在线提交人写入 inbox，并在下次进服自动送达并清空
        await self.say(STEVE, '!留言 离线反馈测试')
        self.host.online = [dict(ADMIN)]  # Steve 和 Alex 均下线
        await self.say(ADMIN, '!处理举报 2 已采纳你的建议')
        await self.say(ADMIN, '!处理举报 3 离线反馈已查看')
        self.assertIn('当前不在线，将在下次进服时自动通知', self.host.told('Admin'))
        self.assertEqual(len(self.host.values['inbox:u-alex']), 1)
        self.assertEqual(len(self.host.values['inbox:u-steve']), 1)

        # Steve 进服前在列表还未上线（事件到达时已离开），只确认事件，不清空 inbox
        await self.deliver('player.list_added', dict(STEVE))
        self.assert_only_acknowledged()
        self.assertIn('inbox:u-steve', self.host.values)

        # Alex 进服（模拟 Host 名单不带 xuid）成功收到通知并清空 inbox
        self.host.online.append(dict(ALEX, xuid=''))
        await self.deliver('player.list_added', dict(ALEX))
        self.assertIn('你的留言 #2 已有回复（Admin）：已采纳你的建议', self.host.told('Alex'))
        self.assertNotIn('inbox:u-alex', self.host.values)

        # Alex 再次触发进服不会重复发送
        await self.join(ALEX)
        self.assert_only_acknowledged()

    # ---- 管理员进服提醒与启动积压跳过 ----

    async def test_admin_join_reminder_and_backlog_skip(self):
        await self.say(STEVE, '!举报 Alex 挖矿作弊')
        self.wall += 100
        await self.say(STEVE, '!留言 建议')

        # 刚重启时积压的进服事件跳过提醒
        self.ctx = await self.start(self.ctx.config)
        self.mono += 0.05
        await self.join(ADMIN)
        self.assertEqual(self.host.told('Admin'), '')

        # 超过 CATCH_UP_GAP 后真实进服触发待处理提醒
        self.mono += 2.0
        await self.join(ADMIN)
        self.assertIn('当前有 2 条待处理记录（举报 1 条、留言 1 条），发送 !查举报 查看。', self.host.told('Admin'))

        # 旧 session 的事件不触发提醒
        await self.join(ADMIN, session='old')
        self.assertEqual(self.host.told('Admin'), '')

        # 关闭 notify_admins_on_join 时不提醒
        self.configure(notify_admins_on_join=False)
        await self.join(ADMIN)
        self.assert_only_acknowledged()

    async def test_backlog_wait_is_bounded_by_catch_up_limit(self):
        await self.say(STEVE, '!举报 Alex 挖矿作弊')
        self.ctx = await self.start(self.ctx.config)
        for index in range(125):
            self.mono += 0.5
            await self.join(dict(BOB, name=f'P{index}', uuid=f'ub{index}', xuid=f'x{index}'))
        await self.join(ADMIN)
        self.assertIn('当前有 1 条待处理记录', self.host.told('Admin'))

    async def test_xuid_fallback_pid_and_multiple_inbox_items(self):
        no_uuid = {'name': 'NoUuid', 'uuid': '', 'xuid': '999'}
        self.host.online.append(dict(no_uuid))
        self.sample_mock.return_value = pos_sample({'uniqueId': 'NoUuid', 'dimension': 2,
                                                    'position': {'x': 5, 'y': 65.62, 'z': -5}})
        self.configure(cooldown_seconds=60)
        await self.say(no_uuid, '!举报 Alex 第一次')
        self.assertEqual(self.host.board()['records'][0]['reporter_pid'], 'xuid-999')
        self.assertEqual(self.host.board()['records'][0]['reporter_pos'],
                         {'dimension': 2, 'x': 5.0, 'y': 64.0, 'z': -5.0})
        self.assertIn('cd:xuid-999', self.host.values)

        self.wall += 100
        await self.say(no_uuid, '!留言 第二次')
        self.host.online = [dict(ADMIN)]
        await self.say(ADMIN, '!处理举报 1 核实1')
        await self.say(ADMIN, '!处理举报 2 核实2')
        self.assertEqual(len(self.host.values['inbox:xuid-999']), 2)

        await self.join(no_uuid)
        msg = self.host.told('NoUuid')
        self.assertIn('你的举报 #1', msg)
        self.assertIn('你的留言 #2', msg)
        self.assertNotIn('inbox:xuid-999', self.host.values)

    async def test_admin_with_pending_inbox_gets_both_on_join(self):
        self.configure(cooldown_seconds=0)
        await self.say(ADMIN, '!留言 测试自身通知')
        await self.say(STEVE, '!举报 Alex 待处理项')
        self.host.online = [dict(ADMIN2)]
        await self.say(ADMIN2, '!处理举报 1 收到')

        self.host.online.append(dict(ADMIN))
        await self.deliver('player.list_added', dict(ADMIN))
        msg = self.host.told('Admin')
        self.assertIn('你的留言 #1 已有回复（Mod）：收到', msg)
        self.assertIn('当前有 1 条待处理记录', msg)
        self.assertNotIn('inbox:u-admin', self.host.values)

    # ---- 记录上限淘汰策略 ----

    async def test_max_records_evicts_oldest_resolved_first_then_oldest_open(self):
        self.configure(cooldown_seconds=0, max_records=20)
        for index in range(1, 21):
            await self.say(STEVE, f'!留言 记录{index}')
        # 处理第 2 条和第 5 条，保持其余 18 条为 open
        await self.say(ADMIN, '!处理举报 2 处理2')
        await self.say(ADMIN, '!处理举报 5 处理5')

        # 新增第 21 条 -> 优先淘汰最旧的 resolved (#2)
        await self.say(STEVE, '!留言 记录21')
        ids = [r['id'] for r in self.host.board()['records']]
        self.assertEqual(len(ids), 20)
        self.assertNotIn(2, ids)
        self.assertIn(1, ids)
        self.assertIn(5, ids)
        self.assertIn(21, ids)

        # 新增第 22 条 -> 淘汰剩下的 resolved (#5)
        await self.say(STEVE, '!留言 记录22')
        ids = [r['id'] for r in self.host.board()['records']]
        self.assertNotIn(5, ids)
        self.assertIn(1, ids)

        # 现在全部 20 条均为 open，新增第 23 条 -> 裁剪最旧的 open (#1)
        await self.say(STEVE, '!留言 记录23')
        ids = [r['id'] for r in self.host.board()['records']]
        self.assertEqual(len(ids), 20)
        self.assertNotIn(1, ids)
        self.assertEqual(ids[0], 3)
        self.assertEqual(ids[-1], 23)
        self.assertEqual(self.host.board()['next_id'], 24)

    # ---- 其他聊天 ----

    async def test_unrelated_chat_and_malformed_args_only_acknowledged(self):
        for message in ('你好世界', '!未知命令', '!举报 "未闭合', '!我的举报 多余参数', '!查举报 1 2'):
            await self.say(STEVE, message)
            self.assert_only_acknowledged()


class ConfigTests(unittest.TestCase):
    def load(self, value):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.json'
            path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')
            return load_config(Settings, path)

    def test_static_schema_and_example_match_settings(self):
        schema = json.loads((ROOT / 'config.schema.json').read_text(encoding='utf-8'))
        check_config_schema(schema)
        self.assertEqual(schema, schema_for_config(Settings))
        example = json.loads((ROOT / 'config.example.json').read_text(encoding='utf-8'))
        self.assertEqual(example, dataclasses.asdict(Settings()))
        self.assertEqual(self.load(example), Settings())

    def test_defaults(self):
        self.assertEqual(self.load({}), Settings())

    def test_invalid_configurations_are_rejected(self):
        for value in ({'admins': ['  ']}, {'cooldown_seconds': -1},
                      {'max_records': 19}, {'max_records': 1001},
                      {'allow_feedback': 'yes'}, {'unknown': 1}):
            with self.subTest(value=value), self.assertRaises(ConfigError):
                self.load(value)


if __name__ == '__main__':
    unittest.main()
