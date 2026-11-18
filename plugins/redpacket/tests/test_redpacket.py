"""Drive the redpacket plugin through the real SDK storage against an in-memory Host."""
import asyncio
import copy
import dataclasses
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from neomega_runtime import Event, PlayerNotFound, PluginContext, ServiceRejected
from neomega_runtime.config import load_config
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config_schema import check_config_schema, schema_for_config
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from neomega_runtime.storage import CommitUncertain

from main import (
    ECONOMY_GRANT,
    ECONOMY_PAY,
    FINISHED_RETENTION_SECONDS,
    MAX_FINISHED_KEEP,
    PACKETS,
    REFUND_NOTICES,
    SWEEP_INTERVAL_SECONDS,
    RedPacket,
    Settings,
    grab_request_id,
    pay_request_id,
    refund_request_id,
    split_amounts,
)

ROOT = Path(__file__).resolve().parents[1]
ADMIN = {'name': 'Admin', 'uuid': 'u-admin', 'xuid': '1'}
ALICE = {'name': 'Alice', 'uuid': 'u-alice', 'xuid': '10'}
BOB = {'name': 'Bob', 'uuid': 'u-bob', 'xuid': '20'}
CAROL = {'name': 'Carol', 'uuid': 'u-carol', 'xuid': '30'}
XUID_ONLY = {'name': 'NoUuid', 'uuid': '', 'xuid': '88'}


class FakeHost:
    """state.get / state.commit / events.commit with the Host's global revision CAS."""

    def __init__(self):
        self.values = {}
        self.revision = 0
        self.commits = []
        self.online = [dict(ADMIN), dict(ALICE), dict(BOB), dict(CAROL)]
        self.fail_state_commit = None

    async def call(self, method, payload, timeout=None):
        if method == 'players.list':
            return {'session': 's1', 'players': copy.deepcopy(self.online)}
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        assert method in ('state.commit', 'events.commit'), method
        if method == 'state.commit' and self.fail_state_commit is not None:
            raise self.fail_state_commit
        assert payload['expected_state_revision'] == self.revision
        for mutation in payload['mutations']:
            if mutation.get('delete'):
                self.values.pop(mutation['key'], None)
            else:
                self.values[mutation['key']] = copy.deepcopy(mutation['value'])
        self.revision += 1
        self.commits.append(copy.deepcopy(payload))
        return {
            'commit_id': payload['commit_id'],
            'revision': self.revision,
            'duplicate': False,
            'operation_ids': [f'op{i}' for i in range(len(payload['actions']))],
        }

    def commands(self, commit=-1):
        return [action['arguments']['cmd'] for action in self.commits[commit]['actions']]

    def broadcasts(self, commit=-1):
        texts = []
        for cmd in self.commands(commit):
            if cmd.startswith('tellraw @a '):
                body = json.loads(cmd[len('tellraw @a '):])['rawtext']
                texts.append(''.join(part['text'] for part in body))
        return texts

    def told(self, name, commit=-1):
        texts = []
        prefix = f'tellraw @a[name="{name}"] '
        for cmd in self.commands(commit):
            if cmd.startswith(prefix):
                body = json.loads(cmd[len(prefix):])['rawtext']
                texts.append(''.join(part['text'] for part in body))
        return ''.join(texts)


class SplitAmountsTests(unittest.TestCase):
    def test_split_amounts_properties_and_edge_cases(self):
        cases = [(1, 1), (10, 1), (10, 10), (11, 10), (100, 10), (1000, 20), (500, 100)]
        for total, count in cases:
            for _ in range(50):
                shares = split_amounts(total, count)
                self.assertEqual(len(shares), count)
                self.assertEqual(sum(shares), total)
                self.assertTrue(all(type(x) is int and x >= 1 for x in shares))

    def test_split_amounts_has_randomness(self):
        results = {tuple(split_amounts(100, 5)) for _ in range(20)}
        self.assertGreater(len(results), 1)

    def test_split_amounts_rejects_invalid_inputs(self):
        for total, count in ((0, 1), (5, 0), (5, -1), (4, 5), (10.0, 2), (True, 1), (10, True)):
            with self.subTest(total=total, count=count), self.assertRaises(ValueError):
                split_amounts(total, count)


class RedPacketTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.tasks = []
        self.every_calls = []
        self.logger = logging.getLogger('redpacket-test')
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False
        self.wall = 1_800_000_000.0
        self.mono = 1000.0
        self.events = 0
        self.outcomes = {ECONOMY_PAY: [], ECONOMY_GRANT: []}
        self.service_calls = []
        self.balances = {'u-admin': 1000, 'u-alice': 500, 'u-bob': 200, 'u-carol': 100, 'xuid-88': 300}
        self.ctx = await self.start_plugin(Settings(admins=['admin']))

    async def asyncTearDown(self):
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

    async def start_plugin(self, settings: Settings):
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()
        self.every_calls.clear()

        worker = SimpleNamespace(
            peer=self.host,
            log=self.logger,
            spawn=lambda coro, name=None: self._track_task(coro),
        )
        ctx = PluginContext(worker, None)
        ctx.config = settings
        ctx._operations = Operations(self.host, 's1')
        ctx.services.call = AsyncMock(side_effect=self.mock_economy)
        orig_every = ctx.every

        def capture_every(seconds, callback, *, immediate=False, name=None):
            self.every_calls.append((seconds, name))
            return orig_every(seconds, callback, immediate=immediate, name=name)

        ctx.every = capture_every

        self.plugin = RedPacket()
        self.plugin.clock = lambda: self.wall
        self.plugin.monotonic = lambda: self.mono
        await self.plugin.on_start(ctx)
        self.mono += 5.0  # 越过启动积压窗口
        return ctx

    def _track_task(self, coro):
        task = asyncio.ensure_future(coro)
        self.tasks.append(task)
        return task

    async def mock_economy(self, service, arguments, *, major=1, timeout=5, idempotency_key=None):
        self.assertEqual((major, timeout), (1, 5))
        self.service_calls.append((service, dict(arguments)))
        queue = self.outcomes.get(service)
        if queue:
            outcome = queue.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome

        pid = arguments['player_id']
        amount = arguments['amount']
        if service == ECONOMY_PAY:
            if self.balances.get(pid, 0) < amount:
                raise ServiceRejected('insufficient_funds')
            self.balances[pid] = self.balances.get(pid, 0) - amount
            return {'balance': self.balances[pid], 'duplicate': False, 'currency': '金币', 'pending': False}
        if service == ECONOMY_GRANT:
            self.balances[pid] = self.balances.get(pid, 0) + amount
            return {'balance': self.balances[pid], 'duplicate': False, 'currency': '金币', 'pending': False}
        raise AssertionError(f'unexpected service {service}')

    async def deliver(self, kind, payload, *, event_id=None, session='s1'):
        self.events += 1
        eid = event_id or f'e{self.events}'
        token = f'd{self.events}'
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': kind,
            'payload': payload,
            'subscription_id': kind,
            'event_id': eid,
            'delivery_token': token,
            'session': session,
        }))
        self.assertEqual(self.host.commits[-1]['delivery_token'], token)

    async def say(self, player, message, *, event_id=None):
        if all(p['name'] != player['name'] for p in self.host.online):
            self.host.online.append(dict(player))
        await self.deliver(
            'chat.received',
            {'player': player['name'], 'xuid': player['xuid'], 'message': message},
            event_id=event_id,
        )

    async def join(self, player, *, add_online=True, session='s1'):
        if add_online and all(p['name'] != player['name'] for p in self.host.online):
            self.host.online.append(dict(player))
        await self.deliver('player.list_added', dict(player), session=session)

    # ---- 启动调度 ----

    async def test_on_start_schedules_60_second_sweep(self):
        self.assertEqual(SWEEP_INTERVAL_SECONDS, 60)
        self.assertEqual(self.every_calls, [(60, 'redpacket-refund')])
        self.assertEqual(len(self.tasks), 1)

    # ---- 发红包 ----

    async def test_send_open_and_passcode_redpackets(self):
        await self.say(ALICE, '!发红包 100 4', event_id='evt-open')
        self.assertEqual(self.balances['u-alice'], 400)
        self.assertIn('已发出 #1 号红包（100 金币，4 份），现有 400 金币。', self.host.told('Alice'))
        [bc] = self.host.broadcasts()
        self.assertIn('Alice', bc)
        self.assertIn('100 金币', bc)
        self.assertIn('4 份', bc)
        self.assertIn('!抢 1', bc)

        pay_call = self.service_calls[-1]
        self.assertEqual(pay_call[0], ECONOMY_PAY)
        self.assertEqual(pay_call[1], {
            'player_id': 'u-alice',
            'amount': 100,
            'reason': '发红包',
            'request_id': pay_request_id('evt-open'),
        })

        pkt1 = self.host.values[PACKETS]['items'][0]
        self.assertEqual(pkt1['id'], 1)
        self.assertEqual(pkt1['sender_id'], 'u-alice')
        self.assertFalse(pkt1['is_system'])
        self.assertEqual(sum(pkt1['shares']), 100)
        self.assertEqual(len(pkt1['shares']), 4)
        self.assertEqual(pkt1['passcode'], '')
        self.assertFalse(pkt1['refunded'])

        await self.say(BOB, '！fhb 60 3 "新年 快乐"')
        self.assertEqual(self.balances['u-bob'], 140)
        [bc2] = self.host.broadcasts()
        self.assertIn('Bob', bc2)
        self.assertIn('60 金币', bc2)
        self.assertIn('3 份', bc2)
        self.assertIn('!抢 "新年 快乐"', bc2)

        await self.say(XUID_ONLY, '!sendrp 30 2 恭喜发财')
        self.assertEqual(self.balances['xuid-88'], 270)
        self.assertEqual(self.service_calls[-1][1]['player_id'], 'xuid-88')
        self.assertIn('!抢 恭喜发财', self.host.broadcasts()[0])

    async def test_send_packet_distinct_request_ids_in_long_session(self):
        session = 'd13361ff7a140ac2c7fd915377aaaa51c58fa124e9779506c143bb94c927d607'
        await self.say(ALICE, '!发红包 20 2', event_id=f'game:{session}:1')
        await self.say(ALICE, '!发红包 20 2', event_id=f'game:{session}:2')
        rids = [args['request_id'] for _, args in self.service_calls]
        self.assertEqual(len(rids), 2)
        self.assertNotEqual(rids[0], rids[1])
        self.assertTrue(all(1 <= len(r) <= 64 for r in rids))

    async def test_send_packet_validation_rejects_without_charging(self):
        bad_messages = (
            '!发红包',
            '!发红包 100',
            '!发红包 100 2 口令 多余',
            '!发红包 0 2',
            '!发红包 -10 2',
            '!发红包 abc 2',
            '!发红包 5 2',        # 小于 min_total=10
            '!发红包 10 11',      # total < count
            '!发红包 100 21',     # 超过 max_count=20
            '!发红包 100 2 123',  # 纯数字口令
            '!发红包 100 2 ' + 'a' * 25,
            '!发红包 100 2 ""',
        )
        for msg in bad_messages:
            with self.subTest(msg=msg):
                await self.say(ALICE, msg)
                self.assertEqual(self.host.commits[-1]['mutations'], [])
                self.assertTrue(self.host.told('Alice'))
        self.assertEqual(self.service_calls, [])

    async def test_send_packet_active_limit_rejects_without_charging(self):
        self.ctx = await self.start_plugin(Settings(admins=['admin'], max_active_packets=2))
        await self.say(ALICE, '!发红包 10 2')
        await self.say(ALICE, '!发红包 10 2')
        await self.say(ALICE, '!发红包 10 2')
        self.assertIn('已达上限（2 个）', self.host.told('Alice'))
        self.assertEqual(len(self.host.values[PACKETS]['items']), 2)
        self.assertEqual(len(self.service_calls), 2)

    async def test_send_packet_economy_errors_and_retry(self):
        # 余额不足
        await self.say(CAROL, '!发红包 200 2')
        self.assertIn('金币不足，发红包需要 200 金币', self.host.told('Carol'))
        self.assertNotIn(PACKETS, self.host.values)

        # IPCRejected
        self.outcomes[ECONOMY_PAY] = [IPCRejected('permission_denied', 'r1', False)]
        await self.say(ALICE, '!发红包 20 2')
        self.assertIn('经济服务当前不可用', self.host.told('Alice'))
        self.assertNotIn(PACKETS, self.host.values)

        # player_offline / other rejection
        self.outcomes[ECONOMY_PAY] = [ServiceRejected('player_offline')]
        await self.say(ALICE, '!发红包 20 2')
        self.assertIn('扣款失败', self.host.told('Alice'))

        self.outcomes[ECONOMY_PAY] = [ServiceRejected('invalid_argument')]
        await self.say(ALICE, '!发红包 20 2')
        self.assertIn('扣款失败', self.host.told('Alice'))

        # 重试一次成功（含 duplicate=True）
        self.service_calls.clear()
        self.outcomes[ECONOMY_PAY] = [
            ServiceWaitTimeout('t1'),
            {'balance': 480, 'duplicate': True, 'currency': '金币', 'pending': False},
        ]
        await self.say(ALICE, '!发红包 20 2', event_id='evt-retry-ok')
        self.assertEqual(len(self.service_calls), 2)
        self.assertEqual(self.service_calls[0][1]['request_id'], self.service_calls[1][1]['request_id'])
        self.assertEqual(len(self.host.values[PACKETS]['items']), 1)

        # 两次都不确定：不建包
        self.service_calls.clear()
        self.outcomes[ECONOMY_PAY] = [ServiceRejected('unavailable'), ServiceUncertain(None)]
        await self.say(ALICE, '!发红包 20 2', event_id='evt-retry-fail')
        self.assertEqual(len(self.service_calls), 2)
        self.assertIn('扣款结果暂时不确定', self.host.told('Alice'))
        self.assertEqual(len(self.host.values[PACKETS]['items']), 1)

    # ---- 抢红包 ----

    async def test_grab_open_packet_all_shares_and_luckiest_broadcast(self):
        await self.say(ALICE, '!发红包 100 3')
        pkt = self.host.values[PACKETS]['items'][0]
        shares = list(pkt['shares'])

        # 无参数时场上只有 1 个无口令红包，直接匹配
        await self.say(BOB, '!抢')
        self.assertEqual(self.balances['u-bob'], 200 + shares[0])
        self.assertIn(f'获得 {shares[0]} 金币', self.host.told('Bob'))
        self.assertEqual(self.host.broadcasts(), [])

        # 重复抢被拒绝
        grant_count = len([c for c in self.service_calls if c[0] == ECONOMY_GRANT])
        await self.say(BOB, '!抢 1')
        self.assertIn('已经抢过', self.host.told('Bob'))
        self.assertEqual(len([c for c in self.service_calls if c[0] == ECONOMY_GRANT]), grant_count)

        # Carol 按编号抢第 2 份
        await self.say(CAROL, '!qhb 1')
        self.assertEqual(self.balances['u-carol'], 100 + shares[1])
        self.assertEqual(self.host.broadcasts(), [])

        # Alice 用全角前缀别名抢最后 1 份，触发手气最佳播报
        await self.say(ALICE, '！grab 1')
        self.assertEqual(self.balances['u-alice'], 400 + shares[2])
        stored = self.host.values[PACKETS]['items'][0]
        self.assertEqual(len(stored['claims']), 3)
        self.assertEqual(sum(c['amount'] for c in stored['claims']), 100)
        best = max(stored['claims'], key=lambda c: c['amount'])
        [finish_bc] = self.host.broadcasts()
        self.assertIn('已被抢完', finish_bc)
        self.assertIn('手气最佳', finish_bc)
        self.assertIn(best['name'], finish_bc)
        self.assertIn(f'{best["amount"]} 金币', finish_bc)

        # 抢完后再抢被拒
        await self.say(XUID_ONLY, '!抢 1')
        self.assertIn('已经被抢完了', self.host.told('NoUuid'))

    async def test_grab_passcode_packet_and_cannot_bypass_by_id(self):
        await self.say(ALICE, '!发红包 50 2 芝麻开门')
        # 不能用编号绕过口令
        await self.say(BOB, '!抢 1')
        self.assertIn('口令红包', self.host.told('Bob'))
        self.assertNotIn('芝麻开门', self.host.told('Bob'))
        self.assertEqual(self.balances['u-bob'], 200)

        # 无参数时提示需要口令
        await self.say(BOB, '!抢')
        self.assertIn('没有无口令红包', self.host.told('Bob'))

        # 错口令
        await self.say(BOB, '!抢 错误口令')
        self.assertIn('口令不对', self.host.told('Bob'))

        # 正确口令
        await self.say(BOB, '!抢 芝麻开门')
        self.assertIn('恭喜你抢到了 #1 号红包', self.host.told('Bob'))

        # 重复抢同一口令红包
        await self.say(BOB, '!抢 芝麻开门')
        self.assertIn('已经抢过', self.host.told('Bob'))

        # 第二份抢完
        await self.say(CAROL, '!抢 芝麻开门')
        self.assertIn('手气最佳', self.host.broadcasts()[0])

        # 已抢完后再用口令抢
        await self.say(XUID_ONLY, '!抢 芝麻开门')
        self.assertIn('已经被抢完了', self.host.told('NoUuid'))

    async def test_grab_without_arg_disambiguation(self):
        await self.say(BOB, '!抢')
        self.assertIn('没有可抢的红包', self.host.told('Bob'))

        await self.say(ALICE, '!发红包 20 2')
        await self.say(ALICE, '!发红包 20 2')
        await self.say(BOB, '!抢')
        self.assertIn('有多个无口令红包', self.host.told('Bob'))

        # 如果只有 1 个无口令红包 + 1 个口令红包，无参数 !抢 直接匹配那个无口令红包
        await self.say(ALICE, '!抢 1')
        await self.say(CAROL, '!抢 1')
        await self.say(ALICE, '!发红包 20 2 秘密')
        await self.say(BOB, '!抢')
        self.assertIn('恭喜你抢到了 #2 号红包', self.host.told('Bob'))
        await self.say(BOB, '!抢')
        self.assertIn('已经抢过 #2 号红包', self.host.told('Bob'))

    async def test_grab_expired_unknown_and_bad_args(self):
        await self.say(ALICE, '!发红包 20 2')
        await self.say(ALICE, '!发红包 20 2 口令过期')
        self.wall += 31 * 60
        await self.say(BOB, '!抢 1')
        self.assertIn('已经过期', self.host.told('Bob'))
        await self.say(BOB, '!抢 口令过期')
        self.assertIn('已经过期', self.host.told('Bob'))
        await self.say(BOB, '!抢 999')
        self.assertIn('没有找到 #999 号红包', self.host.told('Bob'))
        await self.say(BOB, '!抢 a b')
        self.assertIn('用法：!抢', self.host.told('Bob'))
        await self.say(BOB, '!抢 ""')
        self.assertIn('请输入正确的口令或红包编号', self.host.told('Bob'))

    async def test_grab_economy_retry_and_failure_keeps_claim_unwritten(self):
        await self.say(ALICE, '!发红包 20 2')
        self.service_calls.clear()

        # 明确拒绝（IPCRejected）：不写入 claims，后续可重抢
        self.outcomes[ECONOMY_GRANT] = [IPCRejected('unavailable', 'r1', False)]
        await self.say(BOB, '!抢 1')
        self.assertIn('经济服务当前不可用', self.host.told('Bob'))
        self.assertEqual(self.host.values[PACKETS]['items'][0]['claims'], [])

        # GRANT_UNSURE：原子占位并记录 request_id，防止他人重复领取同一份额
        self.outcomes[ECONOMY_GRANT] = [ServiceWaitTimeout('g1'), ServiceFailed({'call_id': 'g2'})]
        await self.say(BOB, '!抢 1')
        self.assertEqual(len(self.service_calls), 3)
        self.assertEqual(self.service_calls[1][1]['request_id'], grab_request_id(1, 'u-bob'))
        self.assertEqual(self.service_calls[1][1]['request_id'], self.service_calls[2][1]['request_id'])
        self.assertIn('结果暂时不确定', self.host.told('Bob'))
        claims = self.host.values[PACKETS]['items'][0]['claims']
        self.assertEqual(len(claims), 1)
        self.assertEqual(claims[0]['request_id'], grab_request_id(1, 'u-bob'))
        self.assertTrue(claims[0]['unsure'])

        # 同一玩家重抢 unsettled 占位时复用同一 request_id，unavailable 后重试成功（pending=True）
        self.outcomes[ECONOMY_GRANT] = [
            ServiceRejected('unavailable'),
            {'balance': 0, 'duplicate': True, 'currency': '积分', 'pending': True},
        ]
        await self.say(BOB, '!抢 1')
        self.assertEqual(len(self.host.values[PACKETS]['items'][0]['claims']), 1)
        self.assertNotIn('unsure', self.host.values[PACKETS]['items'][0]['claims'][0])
        self.assertIn('将在下次确认在线时入账', self.host.told('Bob'))

    # ---- 查看红包清单 ----

    async def test_list_packets_shows_active_only(self):
        await self.say(ALICE, '!红包')
        self.assertIn('当前没有可抢的红包', self.host.told('Alice'))

        await self.say(ALICE, '!发红包 30 3')
        await self.say(BOB, '!发红包 40 2 暗号')
        await self.say(BOB, '!抢 1')
        await self.say(BOB, '！hb')
        reply = self.host.told('Bob')
        self.assertIn('#1 Alice：30 金币，剩 2/3 份，无口令', reply)
        self.assertIn('（已抢）', reply)
        self.assertIn('#2 Bob：40 金币，剩 2/2 份，需要口令', reply)

        # 过期后不再出现在列表里
        self.wall += 31 * 60
        await self.say(BOB, '!redpackets')
        self.assertIn('当前没有可抢的红包', self.host.told('Bob'))

    # ---- 系统红包 ----

    async def test_system_packet_admin_only_no_pay_and_grab_grants(self):
        await self.say(ALICE, '!系统红包 100 2')
        self.assertIn('只有管理员可以发系统红包', self.host.told('Alice'))
        self.assertEqual(self.service_calls, [])
        self.assertNotIn(PACKETS, self.host.values)

        await self.say(ADMIN, '!xthb 50 2 福利')
        self.assertEqual(self.balances['u-admin'], 1000)
        self.assertEqual(self.service_calls, [])
        self.assertIn('已发出 #1 号系统红包', self.host.told('Admin'))
        [bc] = self.host.broadcasts()
        self.assertIn('系统 发出了一个 50 金币的口令红包', bc)
        self.assertIn('!抢 福利', bc)

        await self.say(ALICE, '!红包')
        self.assertIn('#1 系统：50 金币', self.host.told('Alice'))

        await self.say(ALICE, '!抢 福利')
        await self.say(BOB, '!抢 福利')
        self.assertEqual(self.balances['u-alice'] + self.balances['u-bob'], 500 + 200 + 50)
        self.assertIn('系统 的 #1 号红包', self.host.broadcasts()[0])

    # ---- 过期退回与清理与进服事件 ----

    async def test_sweep_refunds_online_sender_and_skips_system_and_finished(self):
        # 1: 个人红包，被抢 1 份剩 2 份
        await self.say(ALICE, '!发红包 60 3')
        await self.say(BOB, '!抢 1')
        claimed_amount = self.host.values[PACKETS]['items'][0]['claims'][0]['amount']
        expected_refund = 60 - claimed_amount

        # 2: 系统红包，过期不应退回
        await self.say(ADMIN, '!系统红包 40 2')

        # 未过期前 sweep 无动作
        commits_before = len(self.host.commits)
        await self.plugin.sweep(self.ctx)
        self.assertEqual(len(self.host.commits), commits_before)

        # 过期后执行 sweep
        self.wall += 31 * 60
        self.service_calls.clear()
        await self.plugin.sweep(self.ctx)

        self.assertEqual(len(self.service_calls), 1)
        self.assertEqual(self.service_calls[0], (ECONOMY_GRANT, {
            'player_id': 'u-alice',
            'amount': expected_refund,
            'reason': '红包过期退回',
            'request_id': refund_request_id(1),
        }))
        self.assertEqual(self.balances['u-alice'], 500 - 60 + expected_refund)
        self.assertIn(f'#{1} 号红包已过期，剩余 {expected_refund} 金币已退回', self.host.told('Alice'))

        # 刚退回后仍保留记录（refunded=True），以便晚到玩家看到“已过期”
        pkt1 = self.host.values[PACKETS]['items'][0]
        self.assertTrue(pkt1['refunded'])
        self.assertEqual(self.host.values[PACKETS]['next_id'], 3)

        # 再次 sweep 不重复退回
        self.service_calls.clear()
        commits_before = len(self.host.commits)
        await self.plugin.sweep(self.ctx)
        self.assertEqual(self.service_calls, [])
        self.assertEqual(len(self.host.commits), commits_before)

        # 超过保留期后清理已终结旧红包，但保留 next_id
        self.wall += FINISHED_RETENTION_SECONDS + 1
        await self.plugin.sweep(self.ctx)
        self.assertEqual(self.host.values[PACKETS]['items'], [])
        self.assertEqual(self.host.values[PACKETS]['next_id'], 3)

    async def test_sweep_offline_sender_queues_refund_notice_for_join(self):
        await self.say(ALICE, '!发红包 50 2')
        # Alice 下线
        self.host.online = [p for p in self.host.online if p['uuid'] != 'u-alice']
        self.wall += 31 * 60
        self.outcomes[ECONOMY_GRANT] = [
            {'balance': 0, 'duplicate': False, 'currency': '金币', 'pending': True}
        ]
        await self.plugin.sweep(self.ctx)
        self.assertTrue(self.host.values[PACKETS]['items'][0]['refunded'])
        self.assertEqual(self.host.values[REFUND_NOTICES]['u-alice'],
                         [{'id': 1, 'amount': 50, 'currency': '金币'}])

        # Alice 再次进服时收到退回通知并清空待通知
        await self.join(ALICE)
        self.assertIn('#1 号红包已过期，剩余 50 金币已退回账户', self.host.told('Alice'))
        self.assertNotIn(REFUND_NOTICES, self.host.values)

        # 再次进服不再重复通知
        await self.join(ALICE)
        self.assertEqual(self.host.told('Alice'), '')

    async def test_sweep_retries_after_grant_failure_and_handles_commit_uncertain(self):
        await self.say(ALICE, '!发红包 30 2')
        self.wall += 31 * 60
        self.outcomes[ECONOMY_GRANT] = [ServiceWaitTimeout('s1'), ServiceWaitTimeout('s2')]
        with self.assertLogs('redpacket-test', 'WARNING'):
            await self.plugin.sweep(self.ctx)
        self.assertFalse(self.host.values[PACKETS]['items'][0]['refunded'])

        # 下一次 sweep 仍用同一个 request_id，并能正常处理 CommitUncertain 不崩溃
        self.host.fail_state_commit = CommitUncertain(None)
        with self.assertLogs('redpacket-test', 'WARNING'):
            await self.plugin.sweep(self.ctx)
        self.host.fail_state_commit = None
        self.assertFalse(self.host.values[PACKETS]['items'][0]['refunded'])

        await self.plugin.sweep(self.ctx)
        self.assertTrue(self.host.values[PACKETS]['items'][0]['refunded'])

    async def test_prunes_excess_finished_packets(self):
        self.ctx = await self.start_plugin(Settings(admins=['admin'], max_active_packets=30))
        for i in range(MAX_FINISHED_KEEP + 5):
            await self.say(ADMIN, '!系统红包 10 1')
            await self.say(ALICE, f'!抢 {i + 1}')
        await self.plugin.sweep(self.ctx)
        self.assertEqual(len(self.host.values[PACKETS]['items']), MAX_FINISHED_KEEP)
        self.assertEqual(self.host.values[PACKETS]['next_id'], MAX_FINISHED_KEEP + 6)

    async def test_join_backlog_suppression_and_live_active_reminder(self):
        await self.say(ALICE, '!发红包 20 2')

        # 模拟重启后积压进服事件（未过 CATCH_UP_GAP）
        self.ctx = await self.start_plugin(Settings(admins=['admin']))
        self.mono -= 5.0  # 撤销 start_plugin 的时间推进，模拟刚启动
        self.plugin.started = self.mono
        self.plugin.last_event = self.mono
        self.plugin.caught_up = False
        await self.join(BOB)
        self.assertEqual(self.host.told('Bob'), '')

        # 超过 CATCH_UP_GAP 后的实时进服：提醒一次，重投/重建列表不重复刷屏
        self.mono += 2.0
        await self.join(CAROL)
        self.assertIn('当前有 1 个可抢红包（#1，20 金币），发送 !抢 1 领取。', self.host.told('Carol'))
        await self.join(CAROL)
        self.assertEqual(self.host.told('Carol'), '')

        # 进服时玩家已下线：静默确认事件
        await self.join({'name': 'Ghost', 'uuid': 'u-ghost', 'xuid': '999'}, add_online=False)
        self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_other_chat_and_malformed_quotes_only_acknowledge(self):
        for msg in ('大家好', '!其他命令', '!发红包 "没闭合', '!红包 extra'):
            await self.say(ALICE, msg)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])

    def test_grab_request_id_bounds_long_player_id(self):
        long_id = 'x' * 120
        rid = grab_request_id(999, long_id)
        self.assertLessEqual(len(rid), 64)
        self.assertEqual(rid, grab_request_id(999, long_id))
        self.assertNotEqual(rid, grab_request_id(998, long_id))

    async def test_economy_v200_without_currency_defaults_to_coin(self):
        self.outcomes[ECONOMY_PAY] = [{'balance': 480, 'duplicate': False, 'pending': False}]
        await self.say(ALICE, '!发红包 20 2')
        self.assertIn('20 金币', self.host.told('Alice'))
        self.outcomes[ECONOMY_GRANT] = [{'balance': 210, 'duplicate': False, 'pending': False}]
        await self.say(BOB, '!抢 1')
        self.assertIn('金币', self.host.told('Bob'))

    async def test_duplicate_passcode_and_quoted_passcode(self):
        await self.say(ALICE, '!发红包 20 2 "新年 快乐"')
        self.wall += 31 * 60
        await self.say(ALICE, '!发红包 30 2 "新年 快乐"')
        # 跳过已过期的 1 号，直接抢活跃的 2 号
        await self.say(BOB, '!抢 "新年 快乐"')
        self.assertIn('恭喜你抢到了 #2 号红包', self.host.told('Bob'))

    async def test_send_and_grab_redelivery_are_idempotent(self):
        # 模拟发红包后 events.commit 丢回复并重投同一 event_id
        self.outcomes[ECONOMY_PAY] = [
            {'balance': 480, 'duplicate': False, 'currency': '金币', 'pending': False},
            {'balance': 480, 'duplicate': True, 'currency': '金币', 'pending': False},
        ]
        # 首次模拟 events.commit 未到达：直接再次投递同一 event_id
        await self.say(ALICE, '!发红包 20 2', event_id='evt-redeliv-send')
        # 清除刚才记录以模拟未落盘的场景，再验证同一 event_id 产生相同 request_id
        first_req = self.service_calls[-1][1]['request_id']
        self.host.values.pop(PACKETS)
        await self.say(ALICE, '!发红包 20 2', event_id='evt-redeliv-send')
        self.assertEqual(self.service_calls[-1][1]['request_id'], first_req)
        self.assertEqual(len(self.host.values[PACKETS]['items']), 1)

        # 模拟抢红包重投同一 event_id：使用同一个 (pkt_id, pid) 幂等键
        self.service_calls.clear()
        self.outcomes[ECONOMY_GRANT] = [
            {'balance': 210, 'duplicate': False, 'currency': '金币', 'pending': False},
            {'balance': 210, 'duplicate': True, 'currency': '金币', 'pending': False},
        ]
        await self.say(BOB, '!抢 1', event_id='evt-redeliv-grab')
        grab_req1 = self.service_calls[-1][1]['request_id']
        self.host.values[PACKETS]['items'][0]['claims'].clear()
        await self.say(BOB, '!抢 1', event_id='evt-redeliv-grab')
        self.assertEqual(self.service_calls[-1][1]['request_id'], grab_req1)
        self.assertEqual(len(self.host.values[PACKETS]['items'][0]['claims']), 1)

    async def test_sweep_falls_back_to_notice_when_players_list_fails(self):
        await self.say(ALICE, '!发红包 20 2')
        self.wall += 31 * 60
        orig_call = self.host.call

        async def fail_players(method, payload, timeout=None):
            if method == 'players.list':
                raise TimeoutError('players timeout')
            return await orig_call(method, payload, timeout=timeout)

        self.host.call = fail_players
        await self.plugin.sweep(self.ctx)
        self.host.call = orig_call
        self.assertTrue(self.host.values[PACKETS]['items'][0]['refunded'])
        self.assertIn('u-alice', self.host.values[REFUND_NOTICES])



class ConfigAndManifestTests(unittest.TestCase):
    def load(self, value):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.json'
            path.write_text(json.dumps(value, ensure_ascii=False))
            return load_config(Settings, path)

    def test_static_schema_matches_settings(self):
        schema = json.loads((ROOT / 'config.schema.json').read_text())
        check_config_schema(schema)
        self.assertEqual(schema, schema_for_config(Settings))

    def test_example_config_and_defaults_load(self):
        example = json.loads((ROOT / 'config.example.json').read_text())
        loaded = load_config(Settings, ROOT / 'config.example.json')
        self.assertEqual(dataclasses.asdict(loaded), example)
        self.assertEqual(loaded.admins, ['Steve'])
        self.assertEqual(self.load({}), Settings())

    def test_invalid_configurations_are_rejected(self):
        for value in (
            {'admins': ['  ']},
            {'min_total': 0},
            {'max_count': 0},
            {'max_count': 101},
            {'expire_minutes': 0},
            {'expire_minutes': 1441},
            {'max_active_packets': 0},
            {'max_active_packets': 51},
            {'unknown': 1},
        ):
            with self.subTest(value=value), self.assertRaises(ConfigError):
                self.load(value)

    def test_manifest_has_required_economy_dependency_and_purposes(self):
        manifest = json.loads((ROOT / 'manifest.json').read_text())
        self.assertEqual(manifest['id'], 'neomega.redpacket')
        self.assertEqual(manifest['version'], '1.0.1')
        self.assertEqual(manifest['dependencies'], [{'id': 'neomega.economy', 'major': 2, 'min_version': '2.0.0'}])
        self.assertEqual(manifest['exports'], [])
        purposes = manifest['permissions']['purposes']
        for key in (
            'event:chat.received',
            'event:player.list_added',
            'operation:framework.command.execute',
            'players:list',
            'service:plugin.neomega.economy.pay',
            'service:plugin.neomega.economy.grant',
        ):
            self.assertTrue(purposes.get(key), key)


class RedPacketRegressionTests(RedPacketTests):
    async def test_grant_unsure_prevents_next_grabber_double_spend_and_sweep_double_refund(self):
        await self.say(ALICE, '!发红包 100 2')
        pkt = self.host.values[PACKETS]['items'][0]
        shares = list(pkt['shares'])
        self.service_calls.clear()

        # Bob hits GRANT_UNSURE on shares[0].
        self.outcomes[ECONOMY_GRANT] = [ServiceWaitTimeout('g1'), ServiceWaitTimeout('g2')]
        await self.say(BOB, '!抢 1')
        self.assertIn('结果暂时不确定', self.host.told('Bob'))

        # Carol grabs next -> must get shares[1], NOT shares[0].
        await self.say(CAROL, '!抢 1')
        self.assertEqual(self.balances['u-carol'], 100 + shares[1])
        stored = self.host.values[PACKETS]['items'][0]
        self.assertEqual(len(stored['claims']), 2)
        self.assertEqual(sum(c['amount'] for c in stored['claims']), 100)

        # Even if only 1 share was claimed (say count=3) and packet expires, sweep must NOT refund Bob's unsure slot.
        await self.say(ALICE, '!发红包 90 3')
        shares2 = list(self.host.values[PACKETS]['items'][1]['shares'])
        self.outcomes[ECONOMY_GRANT] = [ServiceWaitTimeout('g3'), ServiceWaitTimeout('g4')]
        await self.say(BOB, '!抢 2')
        self.wall += 31 * 60
        self.service_calls.clear()
        await self.plugin.sweep(self.ctx)
        self.assertEqual(len(self.service_calls), 1)
        self.assertEqual(self.service_calls[0][1]['amount'], shares2[1] + shares2[2])

    async def test_sweep_releases_lock_across_grant_rpcs(self):
        await self.say(ALICE, '!发红包 40 2')
        self.wall += 31 * 60
        entered = asyncio.Event()
        release = asyncio.Event()
        orig_mock = self.mock_economy

        async def slow_grant(service, arguments, *, major=1, timeout=5, idempotency_key=None):
            if service == ECONOMY_GRANT:
                entered.set()
                await release.wait()
            return await orig_mock(service, arguments, major=major, timeout=timeout, idempotency_key=idempotency_key)

        self.ctx.services.call = AsyncMock(side_effect=slow_grant)
        sweep_task = asyncio.create_task(self.plugin.sweep(self.ctx))
        await entered.wait()
        # While sweep is awaiting ECONOMY_GRANT, unrelated commands complete immediately.
        await self.say(BOB, '!红包')
        self.assertIn('当前没有可抢的红包', self.host.told('Bob'))
        release.set()
        await sweep_task
        self.assertTrue(self.host.values[PACKETS]['items'][0]['refunded'])

    async def test_handle_join_preserves_refund_notices_when_reply_fails(self):
        self.host.values[REFUND_NOTICES] = {'u-alice': [{'id': 7, 'amount': 50, 'currency': '金币'}]}
        with patch.object(self.plugin, 'reply', side_effect=ValueError('bad')):
            await self.join(ALICE)
        self.assertEqual(self.host.values[REFUND_NOTICES],
                         {'u-alice': [{'id': 7, 'amount': 50, 'currency': '金币'}]})

        await self.join(ALICE)
        self.assertIn('#7 号红包已过期，剩余 50 金币已退回账户', self.host.told('Alice'))
        self.assertNotIn(REFUND_NOTICES, self.host.values)

    async def test_passcode_strips_formatting_and_broadcast_chunks_long_text(self):
        await self.say(ALICE, '!发红包 20 2 "§c§l新年§r快乐"')
        pkt = self.host.values[PACKETS]['items'][0]
        self.assertEqual(pkt['passcode'], '新年快乐')
        [bc] = self.host.broadcasts()
        self.assertNotIn('§', bc)
        self.assertIn('!抢 新年快乐', bc)

        # Only formatting codes is rejected.
        await self.say(ALICE, '!发红包 20 2 §c§r')
        self.assertIn('口令不能为空', self.host.told('Alice'))

        # Long broadcast is chunked within 320 bytes per tellraw @a command.
        long_sender = {'name': '甲' * 80, 'uuid': 'u-long', 'xuid': '99'}
        self.balances['u-long'] = 1000
        self.host.online.append(long_sender)
        await self.say(long_sender, '!发红包 20 2 "' + '福' * 20 + '"')
        cmds = [c for c in self.host.commands() if c.startswith('tellraw @a ')]
        self.assertGreater(len(cmds), 1)
        for cmd in cmds:
            self.assertLessEqual(len(cmd.encode('utf-8')), 320)

    async def test_serve_ack_lifecycle(self):
        host = self.host
        events = [
            {'kind': 'chat.received', 'payload': {'player': 'Alice', 'xuid': '10', 'message': '你好'},
             'subscription_id': 'chat', 'event_id': 'ev1', 'delivery_token': 'dt1'},
        ]

        class DriveWorker:
            def __init__(self, peer, **callbacks):
                self.peer, self.callbacks = peer, callbacks
                self.log = logging.getLogger('redpacket-serve-test')
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
            await RedPacket().serve(peer, data_dir=tmp)
        self.assertEqual([c['event_id'] for c in host.commits], ['ev1'])


if __name__ == '__main__':
    unittest.main()
