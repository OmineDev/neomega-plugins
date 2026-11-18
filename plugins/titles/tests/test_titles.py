"""Drive the plugin through the real SDK storage against an in-memory Host."""
import asyncio
import copy
import dataclasses
import hashlib
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from neomega_runtime import Event, PluginContext, ServiceRejected
from neomega_runtime.config import load_config
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config_schema import check_config_schema, schema_for_config
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from main import CATCH_UP_GAP, CATCH_UP_LIMIT, Settings, TitleDef, Titles, sanitize_horn_text

ROOT = Path(__file__).resolve().parents[1]
ROBOT = {'name': '机器人', 'uuid': 'u-bot', 'xuid': '0'}
OWNER = {'name': 'Owner', 'uuid': 'u-owner', 'xuid': '1'}
STEVE = {'name': 'Steve', 'uuid': 'u-steve', 'xuid': '2'}
ALEX = {'name': 'Alex', 'uuid': 'u-alex', 'xuid': '3'}
XUID_ONLY = {'name': 'Guest', 'uuid': '', 'xuid': '99'}


class FakeHost:
    """players.list, robot.status and state.get / state.commit / events.commit with Host revision CAS."""

    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.online = [dict(OWNER), dict(STEVE), dict(ALEX)]
        self.bot_name = ROBOT['name']

    async def call(self, method, payload, timeout=None):
        if method == 'players.list':
            return {'session': 's1', 'players': copy.deepcopy(self.online)}
        if method == 'robot.status':
            return {'session': 's1', 'in_game': True, 'bot_name': self.bot_name, 'player_count': len(self.online)}
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

    def broadcasts(self, commit=-1):
        texts, target = [], 'tellraw @a '
        for command in self.commands(commit):
            if command.startswith(target):
                texts.append(''.join(part['text'] for part in json.loads(command[len(target):])['rawtext']))
        return texts

    def sounds(self, commit=-1):
        return [command for command in self.commands(commit) if 'playsound ' in command]


class TitlesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.t = 1000.0
        self.now_wall = 1_800_000_000.0
        self.ctx = await self.start(Settings(admins=['owner']))
        self.t += 5.0  # 无积压事件，后续事件为实时事件。

    async def start(self, settings: Settings):
        ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('titles-test')), None)
        ctx.config = settings
        ctx._operations = Operations(self.host, 's1')
        ctx.services.call = AsyncMock(side_effect=IPCRejected('permission_denied', 'r1', False))
        self.plugin = Titles()
        self.plugin.now = lambda: self.t
        self.plugin.clock = lambda: self.now_wall
        await self.plugin.on_start(ctx)
        return ctx

    def configure(self, **changes):
        self.ctx.config = dataclasses.replace(self.ctx.config, **changes)

    async def deliver(self, kind, payload, *, event_id='e1', **extra):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': kind, 'payload': payload,
            'subscription_id': 'sub', 'event_id': event_id, 'delivery_token': 'd1', **extra}))
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    async def say(self, player, message, *, event_id='e1'):
        if all(p['name'] != player['name'] for p in self.host.online):
            self.host.online.append(dict(player))
        await self.deliver('chat.received',
                           {'player': player['name'], 'xuid': player['xuid'], 'message': message},
                           event_id=event_id)

    async def join(self, player, *, online=True, event_id='j1', **extra):
        if online and all(p['name'] != player['name'] for p in self.host.online):
            self.host.online.append(dict(player))
        await self.deliver('player.list_added', dict(player), event_id=event_id, **extra)

    def assert_only_acknowledged(self):
        self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    # ---- 普通聊天与基础身份 ----

    async def test_normal_chat_and_bad_args_are_never_intercepted(self):
        for msg in ('你好', '有人吗？', '!未知命令', '!买称号 "没闭合', '!称号 多余参数', '!卸下称号 x'):
            await self.say(STEVE, msg)
            self.assert_only_acknowledged()
            self.assertEqual(self.host.broadcasts(), [])

    async def test_xuid_only_player_uses_xuid_prefix_everywhere(self):
        self.configure(titles=[TitleDef(id='free', name='新人', display='§a[新人]', price=10, purchasable=True)])
        self.ctx.services.call = AsyncMock(return_value={'balance': 90, 'currency': '金币'})
        self.host.online.append(dict(XUID_ONLY))
        await self.say(XUID_ONLY, '!买称号 free', event_id='evt-xuid')
        self.ctx.services.call.assert_awaited_once_with(
            'plugin.neomega.economy.pay',
            {'player_id': 'xuid-99', 'amount': 10, 'reason': '购买称号 新人',
             'request_id': hashlib.sha256(b'buy:evt-xuid').hexdigest()},
            timeout=5,
        )
        self.assertEqual(self.host.values['player:xuid-99'], {'owned': ['free'], 'equipped': 'free'})

    # ---- 查看称号列表 ----

    async def test_list_titles_shows_details_status_and_role_commands(self):
        self.host.values['player:u-steve'] = {'owned': ['pioneer'], 'equipped': 'pioneer'}
        await self.say(STEVE, '!称号')
        text = self.host.told('Steve')
        self.assertIn('1. §6[开拓者]§r 开拓者 (pioneer) | 100 金币 | §a[佩戴中]§r - 服务器开拓纪念', text)
        self.assertIn('2. §b[赞助者]§r 赞助者 (vip) | 管理员发放 | §7[未拥有]§r - 管理员手动发放', text)
        self.assertIn('可用指令：!买称号 <编号或名称>、!佩戴称号 <编号或名称>、!卸下称号、!喇叭 <内容>', text)
        self.assertNotIn('管理员指令', text)

        await self.say(OWNER, '！titles')
        owner_text = self.host.told('Owner')
        self.assertIn('管理员指令：!发称号 <玩家名> <编号或名称>、!收称号 <玩家名> <编号或名称>', owner_text)

        self.host.values['player:u-steve'] = {'owned': ['vip'], 'equipped': None}
        await self.say(STEVE, '!ch')
        self.assertIn('§a[已拥有]§r', self.host.told('Steve'))

        self.configure(titles=[], horn_enabled=False)
        await self.say(STEVE, '!称号')
        empty_text = self.host.told('Steve')
        self.assertIn('还没有配置任何称号', empty_text)
        self.assertNotIn('!喇叭', empty_text)

    # ---- 购买称号 ----

    async def test_buy_paid_title_by_number_id_or_name_auto_equips_first(self):
        self.ctx.services.call = AsyncMock(return_value={'balance': 400, 'duplicate': False, 'currency': '金币'})
        await self.say(STEVE, '!买称号 1', event_id='buy-1')
        self.ctx.services.call.assert_awaited_once_with(
            'plugin.neomega.economy.pay',
            {'player_id': 'u-steve', 'amount': 100, 'reason': '购买称号 开拓者',
             'request_id': hashlib.sha256(b'buy:buy-1').hexdigest()},
            timeout=5,
        )
        self.assertEqual(self.host.values['player:u-steve'], {'owned': ['pioneer'], 'equipped': 'pioneer'})
        reply = self.host.told('Steve')
        self.assertIn('成功购买称号「开拓者」', reply)
        self.assertIn('已自动为你佩戴', reply)
        self.assertIn('花费 100 金币，剩余 400 金币', reply)

        # 再买第二个可购买称号时保留当前佩戴称号。
        self.configure(titles=[
            *self.ctx.config.titles,
            TitleDef(id='rich', name='富豪', display='§e[富豪]', price=50, purchasable=True),
        ])
        await self.say(STEVE, '！mch RICH', event_id='buy-2')
        self.assertEqual(self.host.values['player:u-steve'],
                         {'owned': ['pioneer', 'rich'], 'equipped': 'pioneer'})
        self.assertIn('发送 !佩戴称号 rich 即可切换佩戴', self.host.told('Steve'))

    async def test_buy_free_purchasable_title_needs_no_economy(self):
        self.configure(titles=[TitleDef(id='starter', name='新手', display='§a[新手]', price=0, purchasable=True)])
        await self.say(STEVE, '!买称号 新手')
        self.ctx.services.call.assert_not_awaited()
        self.assertEqual(self.host.values['player:u-steve'], {'owned': ['starter'], 'equipped': 'starter'})
        self.assertIn('已自动为你佩戴', self.host.told('Steve'))

    async def test_buy_title_rejections_and_economy_errors(self):
        await self.say(STEVE, '!买称号')
        self.assertIn('用法：!买称号', self.host.told('Steve'))

        await self.say(STEVE, '!买称号 99')
        self.assertIn('没有找到称号「99」', self.host.told('Steve'))

        await self.say(STEVE, '!买称号 vip')
        self.assertIn('不可购买，仅限管理员发放', self.host.told('Steve'))

        # 未安装经济插件时，收费称号提示无法购买。
        await self.say(STEVE, '!买称号 pioneer')
        self.assertIn('服务器未开启经济系统', self.host.told('Steve'))
        self.assertNotIn('player:u-steve', self.host.values)

        # 余额不足与其他拒绝。
        self.ctx.services.call = AsyncMock(side_effect=ServiceRejected('insufficient_funds'))
        await self.say(STEVE, '!买称号 pioneer')
        self.assertIn('金币不足，购买「开拓者」需要 100 金币', self.host.told('Steve'))
        self.assertNotIn('player:u-steve', self.host.values)

        self.ctx.services.call = AsyncMock(side_effect=ServiceRejected('player_offline'))
        await self.say(STEVE, '!买称号 pioneer')
        self.assertIn('扣款失败', self.host.told('Steve'))

        # 已拥有时不再扣款。
        self.host.values['player:u-steve'] = {'owned': ['pioneer'], 'equipped': 'pioneer'}
        self.ctx.services.call = AsyncMock()
        await self.say(STEVE, '!买称号 开拓者')
        self.ctx.services.call.assert_not_awaited()
        self.assertIn('已经拥有', self.host.told('Steve'))

    async def test_buy_unsettled_retries_once_with_same_request_id(self):
        session = 'd13361ff7a140ac2c7fd915377aaaa51c58fa124e9779506c143bb94c927d607'
        self.ctx.services.call = AsyncMock(side_effect=ServiceWaitTimeout('c1'))
        await self.say(STEVE, '!买称号 pioneer', event_id=f'game:{session}:10')
        self.assertEqual(self.ctx.services.call.await_count, 2)
        req1, req2 = (call.args[1]['request_id'] for call in self.ctx.services.call.await_args_list)
        self.assertEqual(req1, req2)
        self.assertLessEqual(len(req1), 64)
        self.assertIn('扣款结果暂时不确定', self.host.told('Steve'))
        self.assertNotIn('player:u-steve', self.host.values)

        # 第一次 unavailable、第二次 duplicate=True 成功时正常入账。
        self.ctx.services.call = AsyncMock(side_effect=[
            ServiceRejected('unavailable'),
            {'balance': 200, 'duplicate': True, 'currency': '金币'},
        ])
        await self.say(STEVE, '!买称号 pioneer', event_id=f'game:{session}:11')
        self.assertNotEqual(req1, self.ctx.services.call.await_args_list[0].args[1]['request_id'])
        self.assertEqual(self.host.values['player:u-steve'], {'owned': ['pioneer'], 'equipped': 'pioneer'})

        for err in (ServiceUncertain('i'), ServiceFailed({'call_id': 'c1', 'state': 'failed', 'error': 'boom'})):
            self.ctx.services.call = AsyncMock(side_effect=err)
            await self.say(ALEX, '!买称号 pioneer', event_id='evt-u')
            self.assertEqual(self.ctx.services.call.await_count, 2)
            self.assertNotIn('player:u-alex', self.host.values)

    # ---- 佩戴与卸下 ----

    async def test_equip_and_unequip_flow(self):
        await self.say(STEVE, '!佩戴称号')
        self.assertIn('用法：!佩戴称号', self.host.told('Steve'))
        await self.say(STEVE, '!佩戴称号 unknown')
        self.assertIn('没有找到称号', self.host.told('Steve'))
        await self.say(STEVE, '!佩戴称号 pioneer')
        self.assertIn('可发送 !买称号 pioneer 购买', self.host.told('Steve'))
        await self.say(STEVE, '!佩戴称号 vip')
        self.assertIn('仅限管理员发放', self.host.told('Steve'))
        await self.say(STEVE, '!卸下称号')
        self.assertIn('没有佩戴任何称号', self.host.told('Steve'))

        self.host.values['player:u-steve'] = {'owned': ['pioneer', 'vip'], 'equipped': None}
        await self.say(STEVE, '！pdch 2')
        self.assertEqual(self.host.values['player:u-steve']['equipped'], 'vip')
        self.assertIn('已佩戴称号「赞助者」', self.host.told('Steve'))

        await self.say(STEVE, '!佩戴称号 VIP')
        self.assertIn('已经在佩戴', self.host.told('Steve'))

        await self.say(STEVE, '！xxch')
        self.assertIsNone(self.host.values['player:u-steve']['equipped'])
        self.assertIn('已卸下', self.host.told('Steve'))

    # ---- 全服喇叭 ----

    async def test_horn_multiword_quoted_title_and_sound(self):
        self.host.values['player:u-steve'] = {'owned': ['pioneer'], 'equipped': 'pioneer'}
        await self.say(STEVE, '!喇叭 欢迎 大家 来玩')
        self.assertEqual(self.host.broadcasts(), ['§e[喇叭] §6[开拓者]§r §fSteve§e: §f欢迎 大家 来玩'])
        self.assertEqual(self.host.sounds(), ['execute as @a at @s run playsound random.orb @s'])

        self.now_wall += 30
        await self.say(ALEX, '！horn "单引号整段 内容"')
        self.assertEqual(self.host.broadcasts(), ['§e[喇叭] §fAlex§e: §f单引号整段 内容'])

    async def test_horn_sanitizes_section_codes_and_newlines(self):
        self.assertEqual(sanitize_horn_text('§c红§l粗 §§4套娃\n换行\r\x00§\n[管理]'), '红粗 4套娃 换行 [管理]')
        await self.say(STEVE, '!lb "§4[系统] 假的\n第二行 §"')
        self.assertEqual(self.host.broadcasts(), ['§e[喇叭] §fSteve§e: §f[系统] 假的 第二行'])

        self.now_wall += 30
        await self.say(STEVE, '!喇叭 §c§r')
        self.assertIn('不能为空', self.host.told('Steve'))
        self.assertEqual(self.host.broadcasts(), [])

        await self.say(STEVE, '!喇叭 ' + '字' * 201)
        self.assertIn('最多 200 个字', self.host.told('Steve'))
        self.assertEqual(self.host.broadcasts(), [])

        await self.say(STEVE, '!喇叭')
        self.assertIn('用法：!喇叭', self.host.told('Steve'))

    async def test_horn_economy_cooldown_and_toggles(self):
        self.ctx.services.call = AsyncMock(return_value={'balance': 90, 'currency': '金币'})
        await self.say(STEVE, '!喇叭 第一条', event_id='h1')
        self.ctx.services.call.assert_awaited_once_with(
            'plugin.neomega.economy.pay',
            {'player_id': 'u-steve', 'amount': 10, 'reason': '全服喇叭',
             'request_id': hashlib.sha256(b'horn:h1').hexdigest()},
            timeout=5,
        )
        self.assertIn('花费 10 金币，剩余 90 金币', self.host.told('Steve'))
        self.assertEqual(len(self.host.broadcasts()), 1)

        # 冷却期内不再扣款也不发喇叭，重启后依然保留冷却。
        self.ctx.services.call.reset_mock()
        self.now_wall += 4.2
        await self.say(STEVE, '!喇叭 太快了')
        self.ctx.services.call.assert_not_awaited()
        self.assertEqual(self.host.broadcasts(), [])
        self.assertIn('请 11 秒后再试', self.host.told('Steve'))

        self.ctx = await self.start(Settings(admins=['owner']))
        self.t += 5.0
        await self.say(STEVE, '!喇叭 重启后')
        self.assertIn('请 11 秒后再试', self.host.told('Steve'))

        # 时钟回拨不超过一次冷却。
        self.now_wall -= 3600
        await self.say(STEVE, '!喇叭 回拨')
        self.assertIn('请 15 秒后再试', self.host.told('Steve'))

        # 余额不足与扣款不确定不触发喇叭也不进入冷却。
        self.now_wall += 3620
        self.ctx.services.call = AsyncMock(side_effect=ServiceRejected('insufficient_funds'))
        await self.say(STEVE, '!喇叭 没钱了')
        self.assertEqual(self.host.broadcasts(), [])
        self.assertIn('金币不足，发送一次喇叭需要 10 金币', self.host.told('Steve'))

        self.ctx.services.call = AsyncMock(side_effect=ServiceWaitTimeout('t1'))
        await self.say(STEVE, '!喇叭 超时了')
        self.assertEqual(self.ctx.services.call.await_count, 2)
        self.assertEqual(self.host.broadcasts(), [])
        self.assertIn('扣款结果暂时不确定', self.host.told('Steve'))

        self.ctx.services.call = AsyncMock(side_effect=ServiceRejected('player_offline'))
        await self.say(STEVE, '!喇叭 拒绝')
        self.assertEqual(self.host.broadcasts(), [])
        self.assertIn('扣款失败', self.host.told('Steve'))

        self.ctx.services.call = AsyncMock(side_effect=[
            ServiceRejected('unavailable'),
            {'balance': 80, 'duplicate': True, 'currency': '金币'},
        ])
        await self.say(STEVE, '!喇叭 重试成功', event_id='h2')
        self.assertEqual(self.ctx.services.call.await_count, 2)
        self.assertEqual(len(self.host.broadcasts()), 1)
        self.assertIn('剩余 80 金币', self.host.told('Steve'))

        # 冷却期内之前失败并未记录 horn_at，充值后可立即发出；关闭声音与关闭喇叭测试。
        self.configure(horn_sound=False, horn_cooldown_seconds=0)
        self.ctx.services.call = AsyncMock(side_effect=IPCRejected('permission_denied', 'r1', False))
        await self.say(STEVE, '!喇叭 免费兜底')
        self.assertEqual(len(self.host.broadcasts()), 1)
        self.assertEqual(self.host.sounds(), [])

        self.configure(horn_enabled=False)
        await self.say(STEVE, '!喇叭 关闭后')
        self.assertIn('已关闭全服喇叭', self.host.told('Steve'))
        self.assertEqual(self.host.broadcasts(), [])

    # ---- 管理员发收称号与离线合并 ----

    async def test_admin_grant_and_revoke_online_player(self):
        await self.say(STEVE, '!发称号 Alex vip')
        self.assertIn('只有管理员', self.host.told('Steve'))
        self.assertNotIn('player:u-alex', self.host.values)

        await self.say(OWNER, '!发称号 Alex')
        self.assertIn('用法：!发称号', self.host.told('Owner'))
        await self.say(OWNER, '!发称号 Alex unknown')
        self.assertIn('没有找到称号', self.host.told('Owner'))

        await self.say(OWNER, '！granttitle Alex 2')
        self.assertEqual(self.host.values['player:u-alex'], {'owned': ['vip'], 'equipped': None})
        self.assertIn('已向在线玩家 Alex 发放称号「赞助者」', self.host.told('Owner'))
        self.assertIn('管理员授予了你称号「赞助者」', self.host.told('Alex'))

        await self.say(OWNER, '!发称号 Alex vip')
        self.assertIn('已经拥有称号「赞助者」', self.host.told('Owner'))

        # 玩家佩戴后再被管理员收回时同步清除佩戴状态。
        await self.say(ALEX, '!佩戴称号 vip')
        self.assertEqual(self.host.values['player:u-alex']['equipped'], 'vip')
        await self.say(OWNER, '!收称号 Alex 赞助者')
        self.assertEqual(self.host.values['player:u-alex'], {'owned': [], 'equipped': None})
        self.assertIn('已收回在线玩家 Alex 的称号「赞助者」', self.host.told('Owner'))
        self.assertIn('管理员收回了你的称号「赞助者」', self.host.told('Alex'))

        await self.say(OWNER, '!revoketitle Alex vip')
        self.assertIn('还没有拥有称号「赞助者」', self.host.told('Owner'))

        # 管理员给自己发称号只发一条回复（不重复发 target）。
        await self.say(OWNER, '!发称号 Owner pioneer')
        self.assertEqual(len(self.host.commands()), 1)

        # 目标名过长或在线同名冲突时有清晰提示。
        await self.say(OWNER, '!发称号 ' + 'a' * 250 + ' vip')
        self.assertIn('玩家名过长', self.host.told('Owner'))
        self.host.online.append(dict(ALEX, uuid='u-alex-2', xuid='33'))
        await self.say(OWNER, '!发称号 Alex vip')
        self.assertIn('有多名', self.host.told('Owner'))

    async def test_admin_offline_pending_merges_and_notifies_on_join(self):
        offline_bob = {'name': 'Bob', 'uuid': 'u-bob', 'xuid': '88'}
        self.host.values['player:u-bob'] = {'owned': ['pioneer'], 'equipped': 'pioneer'}

        await self.say(OWNER, '!发称号 bob vip')
        self.assertIn('不在线，已记录发放', self.host.told('Owner'))
        await self.say(OWNER, '!收称号 Bob pioneer')
        self.assertIn('不在线，已记录收回', self.host.told('Owner'))
        self.assertEqual(self.host.values['pending_name:bob'], {'vip': 'grant', 'pioneer': 'revoke'})

        # 离线 Bob 上线：自动合并授予 vip、收回 pioneer（并清除原佩戴），发私聊通知，且不播报已收回的旧称号。
        await self.join(offline_bob)
        self.assertNotIn('pending_name:bob', self.host.values)
        self.assertEqual(self.host.values['player:u-bob'], {'owned': ['vip'], 'equipped': None})
        bob_msg = self.host.told('Bob')
        self.assertIn('收到管理员授予的称号：「赞助者」', bob_msg)
        self.assertIn('管理员收回了你的称号：「开拓者」', bob_msg)
        self.assertEqual(self.host.broadcasts(), [])

    async def test_removed_config_title_behaves_gracefully(self):
        self.host.values['player:u-steve'] = {'owned': ['pioneer'], 'equipped': 'pioneer'}
        self.configure(titles=[TitleDef(id='vip', name='赞助者', display='§b[赞助者]', price=0, purchasable=True)])

        await self.say(STEVE, '!喇叭 测试')
        self.assertEqual(self.host.broadcasts(), ['§e[喇叭] §fSteve§e: §f测试'])
        await self.join(STEVE)
        self.assertEqual(self.host.broadcasts(), [])

        await self.say(STEVE, '!买称号 vip')
        self.assertEqual(self.host.values['player:u-steve']['equipped'], 'vip')
        await self.say(STEVE, '!卸下称号')
        self.assertIsNone(self.host.values['player:u-steve']['equipped'])

    # ---- 进服播报与重启防刷屏 ----

    async def test_live_join_broadcast_and_roster_xuid_handling(self):
        self.host.values['player:u-steve'] = {'owned': ['pioneer'], 'equipped': 'pioneer'}
        # 在线名单不带 XUID、进服事件带 XUID 时正常识别。
        self.host.online = [dict(STEVE, xuid='')]
        await self.join(STEVE, online=False)
        self.assertEqual(self.host.broadcasts(), ['§6[开拓者]§r §fSteve §e加入了服务器'])

        # 名单里是另一个 XUID 时不误认，不播报。
        self.host.online = [dict(STEVE, xuid='999')]
        await self.join(STEVE, online=False)
        self.assert_only_acknowledged()

        # 进服时玩家已下线：只确认事件。
        self.host.online = []
        await self.join(STEVE, online=False)
        self.assert_only_acknowledged()

        # 关闭进服播报后不播报，仍可合并待生效称号。
        self.host.online = [dict(ALEX)]
        self.host.values['pending_name:alex'] = {'vip': 'grant'}
        self.configure(join_broadcast=False)
        await self.join(ALEX)
        self.assertEqual(self.host.values['player:u-alex'], {'owned': ['vip'], 'equipped': None})
        self.assertEqual(self.host.broadcasts(), [])

    async def test_backlog_burst_and_old_session_do_not_broadcast_on_restart(self):
        self.host.values['player:u-steve'] = {'owned': ['pioneer'], 'equipped': 'pioneer'}
        self.host.values['player:u-alex'] = {'owned': ['vip'], 'equipped': 'vip'}
        self.host.values['pending_name:alex'] = {'pioneer': 'grant'}
        self.ctx = await self.start(Settings(admins=['owner']))

        # 刚重启连续补发积压事件：不播报进服头衔，但仍能合并在线玩家的待生效称号。
        for index in range(10):
            await self.join(STEVE, event_id=f'old-{index}')
            self.assertEqual(self.host.broadcasts(), [])
            self.t += 0.05
        await self.join(ALEX, event_id='old-alex')
        self.assertEqual(self.host.broadcasts(), [])
        self.assertEqual(self.host.values['player:u-alex']['owned'], ['vip', 'pioneer'])

        # 间隔超过 CATCH_UP_GAP 后恢复实时播报。
        self.t += CATCH_UP_GAP + 0.5
        await self.join(STEVE, event_id='live-1')
        self.assertEqual(self.host.broadcasts(), ['§6[开拓者]§r §fSteve §e加入了服务器'])

        # 上一次连接的事件不播报。
        await self.join(STEVE, event_id='prev-sess', session='old-session')
        self.assertEqual(self.host.broadcasts(), [])

        # 持续高频事件达到 CATCH_UP_LIMIT 后自动放行。
        self.ctx = await self.start(Settings(admins=['owner']))
        steps = int(CATCH_UP_LIMIT / 0.5) + 2
        for index in range(steps):
            await self.join(STEVE, event_id=f'flood-{index}')
            self.t += 0.5
        self.assertEqual(self.host.broadcasts(), ['§6[开拓者]§r §fSteve §e加入了服务器'])


class ConfigAndMetadataTests(unittest.TestCase):
    def load(self, value):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.json'
            path.write_text(json.dumps(value, ensure_ascii=False))
            return load_config(Settings, path)

    def test_static_schema_and_example_match_settings(self):
        schema = json.loads((ROOT / 'config.schema.json').read_text())
        check_config_schema(schema)
        self.assertEqual(schema, schema_for_config(Settings))
        raw_schema = json.dumps(schema)
        self.assertNotIn('anyOf', raw_schema)
        self.assertNotIn('x-secret', raw_schema)

        example = json.loads((ROOT / 'config.example.json').read_text())
        self.assertEqual(example, dataclasses.asdict(Settings()))
        loaded = load_config(Settings, ROOT / 'config.example.json')
        self.assertEqual(loaded, Settings())
        self.assertEqual(len(loaded.titles), 2)
        self.assertEqual((loaded.titles[0].id, loaded.titles[1].id), ('pioneer', 'vip'))

    def test_defaults(self):
        settings = self.load({})
        self.assertEqual(settings, Settings())
        self.assertTrue(settings.horn_enabled and settings.horn_sound and settings.join_broadcast)
        self.assertEqual((settings.horn_cost, settings.horn_cooldown_seconds), (10, 15))

    def test_rejects_invalid_configurations(self):
        valid = {'id': 'a', 'name': '甲', 'display': '§6[甲]'}
        for bad_title in (
            {'id': '', 'name': 'n', 'display': 'd'},
            {'id': 'p', 'name': ' ', 'display': 'd'},
            {'id': 'p', 'name': 'n', 'display': '  '},
            {'id': 'p ', 'name': 'n', 'display': 'd'},
            {'id': 'p', 'name': 'n\n1', 'display': 'd'},
            {'id': 'p', 'name': 'n', 'display': 'd', 'price': -1},
        ):
            with self.subTest(bad_title=bad_title), self.assertRaises(ValueError):
                TitleDef(**bad_title)

        for bad_settings in (
            {'admins': [' ']},
            {'horn_cost': -1},
            {'horn_cooldown_seconds': -1},
            {'titles': [TitleDef(id='a', name='甲', display='d1'), TitleDef(id='A', name='乙', display='d2')]},
            {'titles': [TitleDef(id='a', name='甲', display='d1'), TitleDef(id='b', name='甲', display='d2')]},
        ):
            with self.subTest(bad_settings=bad_settings), self.assertRaises(ValueError):
                Settings(**bad_settings)

        for value in (
            {'titles': [valid, {**valid, 'id': 'A', 'name': '乙'}]},
            {'titles': [valid, {**valid, 'id': 'b', 'name': '甲'}]},
            {'titles': [{**valid, 'id': ''}]},
            {'titles': [{**valid, 'name': ''}]},
            {'titles': [{**valid, 'display': ''}]},
            {'titles': [{**valid, 'price': -1}]},
            {'horn_cost': -1},
            {'horn_cooldown_seconds': -1},
            {'unknown': 1},
        ):
            with self.subTest(value=value), self.assertRaises((ConfigError, ValueError)):
                self.load(value)

    def test_manifest_and_release_files(self):
        manifest = json.loads((ROOT / 'manifest.json').read_text())
        self.assertEqual(manifest['id'], 'neomega.titles')
        self.assertEqual(manifest['version'], '1.0.1')
        purposes = manifest['permissions']['purposes']
        for key in (
            'event:chat.received',
            'event:player.list_added',
            'operation:framework.command.execute',
            'players:list',
            'service:plugin.neomega.economy.pay',
        ):
            self.assertTrue(isinstance(purposes.get(key), str) and purposes[key].strip(), key)

        release = json.loads((ROOT / 'release.json').read_text())
        self.assertEqual(release['name'], '称号与喇叭')
        self.assertNotIn('release.json', release['files'])
        self.assertFalse(any(name.startswith('tests/') for name in release['files']))


class TitlesRegressionTests(TitlesTests):
    async def test_admin_title_lookup_is_case_insensitive_for_online_players(self):
        await self.say(OWNER, '!发称号 alex vip')
        self.assertEqual(self.host.values['player:u-alex'], {'owned': ['vip'], 'equipped': None})
        self.assertNotIn('pending_name:alex', self.host.values)
        self.assertIn('已向在线玩家 Alex 发放称号', self.host.told('Owner'))
        self.assertIn('管理员授予了你称号', self.host.told('Alex'))

    async def test_lock_released_across_charge_economy_rpc(self):
        entered = asyncio.Event()
        release = asyncio.Event()

        async def slow_pay(*args, **kwargs):
            entered.set()
            await release.wait()
            return {'balance': 400, 'duplicate': False, 'currency': '金币'}

        self.ctx.services.call = AsyncMock(side_effect=slow_pay)
        first = asyncio.create_task(self.say(STEVE, '!买称号 pioneer', event_id='slow-buy'))
        await entered.wait()
        # Unrelated commands & chat complete without blocking on slow_pay.
        await self.say(ALEX, '!称号', event_id='fast-list')
        self.assertIn('称号列表：', self.host.told('Alex'))
        await self.say(ALEX, '你好', event_id='fast-chat')
        release.set()
        await first
        self.assertIn('pioneer', self.host.values['player:u-steve']['owned'])

    async def test_robot_skipped_on_join_and_reply_failure_keeps_pending(self):
        self.host.values['player:bot'] = {'owned': ['pioneer'], 'equipped': 'pioneer'}
        self.host.values['pending_name:机器人'] = {'vip': 'grant'}
        await self.join(ROBOT)
        self.assert_only_acknowledged()
        self.assertEqual(self.host.broadcasts(), [])
        self.assertIn('pending_name:机器人', self.host.values)

        self.host.values['pending_name:alex'] = {'vip': 'grant'}
        with patch.object(self.plugin, 'reply', side_effect=ValueError('boom')):
            await self.join(ALEX)
        self.assertIn('pending_name:alex', self.host.values)
        self.assertNotIn('player:u-alex', self.host.values)

    async def test_long_horn_broadcast_is_chunked_within_320_bytes(self):
        self.host.values['player:u-steve'] = {'owned': ['pioneer'], 'equipped': 'pioneer'}
        long_msg = '甲' * 180
        await self.say(STEVE, f'!喇叭 {long_msg}')
        raw_cmds = [c for c in self.host.commands() if c.startswith('tellraw @a ')]
        self.assertGreater(len(raw_cmds), 1)
        for cmd in raw_cmds:
            self.assertLessEqual(len(cmd.encode('utf-8')), 320)
        broadcasts = self.host.broadcasts()
        self.assertTrue(all(part.startswith('§f') for part in broadcasts[1:]))
        joined = ''.join(broadcasts).replace('§f', '')
        self.assertIn(long_msg, joined)

    async def test_serve_ack_lifecycle(self):
        host = self.host
        events = [
            {'kind': 'chat.received', 'payload': {'player': 'Steve', 'xuid': '2', 'message': '你好'},
             'subscription_id': 'chat', 'event_id': 'ev1', 'delivery_token': 'dt1'},
        ]

        class DriveWorker:
            def __init__(self, peer, **callbacks):
                self.peer, self.callbacks = peer, callbacks
                self.log = logging.getLogger('titles-serve-test')
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
            await Titles().serve(peer, data_dir=tmp)
        self.assertEqual([c['event_id'] for c in host.commits], ['ev1'])


if __name__ == '__main__':
    unittest.main()
