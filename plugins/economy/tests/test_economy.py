"""Drive the plugin through the real SDK storage and operations against an in-memory Host."""
import asyncio
import copy
import json
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import ChatRouter, Event, PlayerNotFound, PluginContext, ServiceRejected
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from main import Economy, Settings

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}
FRIEND = {'name': '小明', 'uuid': 'u2', 'xuid': '43'}
STEVE = {'name': 'Steve', 'uuid': 'u3', 'xuid': '44'}
OFFLINE = {'id': 'u3', 'name': 'Steve', 'uuid': 'u3', 'xuid': '44', 'names': [], 'last_seen': '2026-10-01T00:00:00+00:00'}


class FakeHost:
    """state.get/commit with the global revision CAS, plus the operations outbox.

    Actions are deduplicated by idempotency key and run at admission. Scores live
    in memory per holder; `inject[operation]` overrides the next results.
    """
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.scores, self.objectives, self.displays = {}, [], []
        self.ops, self.keys, self.submitted, self.inject = {}, {}, [], {}

    async def call(self, method, payload, timeout=None):
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        if method == 'operations.submit':
            return {'operation_id': self.admit(payload), 'state': 'accepted', 'duplicate': False}
        if method == 'operations.get':
            return copy.deepcopy(self.ops[payload['operation_id']])
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
                'operation_ids': [self.admit(action) for action in payload['actions']]}

    def admit(self, action):
        key = action['idempotency_key']
        if key not in self.keys:
            self.keys[key] = op_id = f'op{len(self.ops)}'
            self.submitted.append((action['operation'], action['arguments']))
            state, result = self.run(action['operation'], action['arguments'])
            outcome = {'state': state} if result is None else {'state': state, 'result': result}
            self.ops[op_id] = {'request_id': key, 'operation_id': op_id, 'state': state,
                               'expires_at': '2026-11-01T00:00:00Z', 'result': outcome, 'can_retry': False}
        return self.keys[key]

    def run(self, operation, args):
        forced = self.inject.get(operation) and self.inject[operation].pop(0)
        if forced in ('failed', 'cancelled'):
            return forced, None
        if operation == 'framework.scoreboard.add':
            # An unknown add may still have reached the server: apply it, lose the reply.
            self.scores[args['holder']] = self.scores.get(args['holder'], 0) + args['delta']
            if forced:
                return forced, None
            return 'succeeded', {'holder': args['holder'], 'objective': args['objective'],
                                 'found': True, 'value': self.scores[args['holder']]}
        if forced:
            return forced, None
        if operation == 'framework.scoreboard.get':
            found = args['holder'] in self.scores
            return 'succeeded', dict({'holder': args['holder'], 'objective': args['objective'], 'found': found},
                                     **({'value': self.scores[args['holder']]} if found else {}))
        if operation == 'framework.scoreboard.objectives.list':
            return 'succeeded', {'objectives': [{'name': name, 'display_name': display, 'criteria': 'dummy'}
                                                for name, display in self.objectives]}
        if operation == 'framework.scoreboard.objectives.add':
            self.objectives.append((args['objective'], args.get('display_name')))
        if operation == 'framework.scoreboard.objectives.setdisplay':
            self.displays.append((args['slot'], args['objective']))
        return 'succeeded', {'operation': operation, 'acknowledged': True}

    def adds(self):
        return [(args['holder'], args['delta']) for op, args in self.submitted if op == 'framework.scoreboard.add']

    def replies(self):
        """Text of the last commit's tellraw commands, grouped by recipient selector."""
        texts = {}
        for action in self.commits[-1]['actions']:
            command = action['arguments'].get('cmd', '')
            if command.startswith('tellraw'):
                target = command[len('tellraw '):command.index(' {')]
                body = json.loads(command[command.index(' {') + 1:])['rawtext']
                texts[target] = texts.get(target, '') + ''.join(part['text'] for part in body)
        return texts

    def reply(self, name='玩家'):
        return self.replies().get(f'@a[name="{name}"]', '')


class EconomyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.tasks = []
        logging.getLogger('economy-test').addHandler(logging.NullHandler())  # 预期内的警告不刷屏
        logging.getLogger('economy-test').propagate = False
        worker = SimpleNamespace(peer=self.host, log=logging.getLogger('economy-test'),
                                 spawn=lambda coroutine, name=None: self.tasks.append(asyncio.ensure_future(coroutine)))
        self.ctx = PluginContext(worker, None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.online = [dict(PLAYER), dict(FRIEND)]
        self.ctx.players = AsyncMock(side_effect=lambda: [dict(p) for p in self.online])
        self.ctx.resolve_player = AsyncMock(side_effect=self.resolve)
        self.ctx.services.call = AsyncMock(return_value=dict(OFFLINE))
        self.events = 0
        self.plugin = await self.start()

    async def start(self):
        self.ctx.chat = ChatRouter()
        plugin = Economy()
        await plugin.on_start(self.ctx)
        await asyncio.gather(*self.tasks)
        self.tasks.clear()
        return plugin

    async def resolve(self, name=None, **_):
        for player in self.online:
            if player['name'] == name:
                return dict(player)
        raise PlayerNotFound(name)

    async def deliver(self, kind, payload, event_id=None):
        self.events += 1
        event_id = event_id or f'e{self.events}'
        token = f'd{self.events}'
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': kind, 'payload': payload, 'subscription_id': kind,
            'event_id': event_id, 'delivery_token': token}))
        # Every event is acknowledged by exactly one commit.
        self.assertEqual([c for c in self.host.commits if c.get('delivery_token') == token][-1:],
                         [c for c in self.host.commits if c.get('event_id') == event_id][-1:])
        self.assertEqual(sum(c.get('delivery_token') == token for c in self.host.commits), 1)

    async def say(self, message, event_id=None):
        await self.deliver('chat.received', {'player': '玩家', 'xuid': '42', 'message': message}, event_id)

    async def join(self, player):
        if player not in self.online:
            self.online.append(dict(player))
        await self.deliver('player.list_added', dict(player))

    async def grant(self, player_id, amount, request_id, reason='签到奖励'):
        return await self.plugin.grant(self.ctx, {'player_id': player_id, 'amount': amount,
                                                  'reason': reason, 'request_id': request_id})

    async def pay(self, player_id, amount, request_id, reason='购买 面包'):
        return await self.plugin.pay(self.ctx, {'player_id': player_id, 'amount': amount,
                                                'reason': reason, 'request_id': request_id})

    async def transfer(self, from_id, to_id, amount, request_id='t1'):
        return await self.plugin.transfer(self.ctx, {'from_id': from_id, 'to_id': to_id, 'amount': amount,
                                                     'reason': '任务结算', 'request_id': request_id})

    async def rejected(self, call):
        with self.assertRaises(ServiceRejected) as caught:
            await call
        return caught.exception.code

    def done(self, balance, duplicate=False, pending=False, currency='金币'):
        return {'balance': balance, 'duplicate': duplicate, 'pending': pending, 'currency': currency}

    def record(self, player_id):
        return self.host.values['a:' + player_id]

    # ---- 服务 ----

    async def test_grant_online_adds_to_scoreboard_and_ignores_repeated_request(self):
        self.host.scores['玩家'] = 5  # 命令方块改过的分数照样算数
        self.assertEqual(await self.grant('u1', 10, 'r1'), self.done(15))
        self.assertEqual(self.host.scores['玩家'], 15)
        record = self.record('u1')
        self.assertEqual({k: record['recent'][0][k] for k in ('id', 'delta', 'reason')},
                         {'id': 'r1', 'delta': 10, 'reason': '签到奖励'})
        self.assertTrue(record['recent'][0]['at'].endswith('+00:00'))
        self.assertEqual(record['pending'], 0)
        self.assertEqual(await self.grant('u1', 10, 'r1'), self.done(15, duplicate=True))
        self.assertEqual(self.host.adds(), [('玩家', 10)])

    async def test_pay_reads_checks_and_deducts(self):
        self.host.scores['玩家'] = 30
        self.assertEqual(await self.pay('u1', 20, 'p1'), self.done(10))
        self.assertEqual(await self.pay('u1', 20, 'p1'), self.done(10, duplicate=True))
        self.assertEqual(await self.rejected(self.pay('u1', 20, 'p2')), 'insufficient_funds')
        self.assertEqual(self.host.scores['玩家'], 10)
        self.assertEqual([e['id'] for e in self.record('u1')['recent']], ['p1'])
        # A holder without a score has 0.
        self.assertEqual(await self.rejected(self.pay('u2', 1, 'p3')), 'insufficient_funds')
        self.assertNotIn('a:u2', self.host.values)

    async def test_pay_and_balance_refuse_offline_players(self):
        self.assertEqual(await self.rejected(self.pay('u3', 1, 'p1')), 'player_offline')
        self.assertEqual(await self.rejected(self.plugin.balance(self.ctx, {'player_id': 'u3'})), 'player_offline')
        self.assertEqual(self.host.adds(), [])

    async def test_balance_reads_the_scoreboard(self):
        self.assertEqual(await self.plugin.balance(self.ctx, {'player_id': 'u1'}),
                         {'balance': 0, 'pending': False, 'currency': '金币'})
        self.host.scores['玩家'] = 7
        self.ctx.config = Settings(currency_name='积分')
        self.assertEqual(await self.plugin.balance(self.ctx, {'player_id': 'u1'}),
                         {'balance': 7, 'pending': False, 'currency': '积分'})
        self.host.inject['framework.scoreboard.get'] = ['unknown']
        self.assertEqual(await self.rejected(self.plugin.balance(self.ctx, {'player_id': 'u1'})), 'unavailable')

    async def test_grant_to_offline_player_waits_until_they_join(self):
        self.assertEqual(await self.grant('u3', 5, 'r1'), self.done(0, pending=True))
        self.assertEqual(await self.grant('u3', 3, 'r2'), self.done(0, pending=True))
        self.assertEqual(await self.grant('u3', 3, 'r2'), self.done(0, duplicate=True, pending=True))
        self.assertEqual(self.record('u3')['pending'], 8)
        self.assertEqual(self.host.adds(), [])
        await self.join(STEVE)
        self.assertEqual(self.host.scores['Steve'], 8)
        self.assertEqual(self.host.reply('Steve'), '你离线期间收到 8 金币')
        record = self.record('u3')
        self.assertEqual((record['pending'], record['started']), (0, True))
        self.assertEqual((record['recent'][-1]['delta'], record['recent'][-1]['reason']), (8, '离线期间收入'))
        # Joining again pays nothing more.
        await self.join(STEVE)
        self.assertEqual(self.host.adds(), [('Steve', 8)])

    async def test_starting_balance_is_given_once_on_first_join(self):
        self.ctx.config = Settings(starting_balance=50)
        await self.join(PLAYER)
        self.assertEqual(self.host.scores['玩家'], 50)
        self.assertEqual(self.record('u1')['recent'][-1]['reason'], '初始金币')
        await self.join(PLAYER)
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        self.assertEqual(self.host.adds(), [('玩家', 50)])
        # Without a starting balance the first join only marks the account.
        self.ctx.config = Settings()
        await self.join(FRIEND)
        self.assertEqual(self.record('u2'), {'pending': 0, 'recent': [], 'started': True})

    async def test_join_credit_that_fails_goes_back_to_pending(self):
        await self.grant('u3', 5, 'r1')
        self.host.inject['framework.scoreboard.add'] = ['failed']
        await self.join(STEVE)
        self.assertEqual(self.record('u3')['pending'], 5)
        self.assertNotIn('Steve', self.host.scores)

    async def test_join_of_player_who_already_left_is_only_acknowledged(self):
        await self.grant('u3', 5, 'r1')
        await self.deliver('player.list_added', dict(STEVE))
        self.assertEqual(self.record('u3')['pending'], 5)
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_pay_when_player_just_left_undoes_the_entry(self):
        self.host.scores['玩家'] = 30
        self.host.inject['framework.scoreboard.add'] = ['failed']
        self.assertEqual(await self.rejected(self.pay('u1', 20, 'p1')), 'player_offline')
        self.assertEqual(self.record('u1')['recent'], [])
        self.assertEqual(self.host.scores['玩家'], 30)
        # The caller may retry the same request later.
        self.assertEqual(await self.pay('u1', 20, 'p1'), self.done(10))

    async def test_grant_that_fails_or_expires_goes_to_pending(self):
        for state in ('failed', 'cancelled'):
            self.host.inject['framework.scoreboard.add'] = [state]
            self.assertEqual(await self.grant('u1', 10, 'r-' + state), self.done(0, pending=True))
        record = self.record('u1')
        self.assertEqual(record['pending'], 20)
        self.assertEqual([e['reason'] for e in record['recent']], ['签到奖励', '签到奖励'])
        self.assertNotIn('玩家', self.host.scores)

    async def test_unknown_result_counts_as_applied_and_is_never_replayed(self):
        self.host.inject['framework.scoreboard.add'] = ['unknown']
        with self.assertLogs('economy-test', 'WARNING') as logs:
            self.assertEqual(await self.grant('u1', 10, 'r1'), self.done(10))
        self.assertIn('r1', logs.output[0])
        self.assertEqual(await self.grant('u1', 10, 'r1'), self.done(10, duplicate=True))
        # When the balance cannot be re-read either, the retry finds the entry.
        self.host.inject['framework.scoreboard.add'] = ['unknown']
        self.host.inject['framework.scoreboard.get'] = [None, 'unknown']  # 先读余额成功，回读失败
        self.assertEqual(await self.rejected(self.pay('u1', 4, 'p1')), 'unavailable')
        self.assertEqual(await self.pay('u1', 4, 'p1'), self.done(6, duplicate=True))
        self.assertEqual(self.host.adds(), [('玩家', 10), ('玩家', -4)])

    async def test_transfer_between_online_players(self):
        self.host.scores['玩家'] = 30
        self.assertEqual(await self.transfer('u1', 'u2', 12), self.done(18))
        self.assertEqual(self.host.scores['小明'], 12)
        payer, payee = self.record('u1')['recent'][-1], self.record('u2')['recent'][-1]
        self.assertEqual((payer['id'], payer['delta'], payee['id'], payee['delta']), ('t1', -12, 't1', 12))
        self.assertEqual(await self.transfer('u1', 'u2', 12), self.done(18, duplicate=True))
        self.assertEqual(self.host.adds(), [('玩家', -12), ('小明', 12)])

    async def test_transfer_finishes_when_payer_balance_cannot_be_reread(self):
        self.host.scores['玩家'] = 30
        self.host.inject['framework.scoreboard.add'] = ['unknown']
        self.host.inject['framework.scoreboard.get'] = [None, 'unknown', 'unknown']
        self.assertEqual(await self.rejected(self.transfer('u1', 'u2', 12)), 'unavailable')
        self.assertEqual(self.host.scores, {'玩家': 18, '小明': 12})
        self.assertEqual(await self.transfer('u1', 'u2', 12), self.done(18, duplicate=True))
        # A failed first read commits nothing and pays nobody.
        self.host.inject['framework.scoreboard.get'] = ['unknown']
        self.assertEqual(await self.rejected(self.transfer('u1', 'u2', 1, 't2')), 'unavailable')
        self.assertEqual(self.host.adds(), [('玩家', -12), ('小明', 12)])

    async def test_transfer_to_offline_player_goes_to_pending(self):
        self.host.scores['玩家'] = 30
        self.assertEqual(await self.transfer('u1', 'u3', 12), self.done(18, pending=True))
        self.assertEqual(self.record('u3')['pending'], 12)
        self.assertEqual(self.record('u3')['recent'][-1]['id'], 't1')

    async def test_transfer_resumes_after_interruption_between_steps(self):
        self.host.scores['玩家'] = 30
        # The payer step committed, then the plugin stopped before paying the payee.
        await self.pay('u1', 12, 't1', reason='任务结算')
        self.assertEqual(await self.transfer('u1', 'u2', 12), self.done(18))
        self.assertEqual(self.host.scores, {'玩家': 18, '小明': 12})
        self.assertEqual(await self.transfer('u1', 'u2', 12), self.done(18, duplicate=True))
        self.assertEqual(self.host.adds(), [('玩家', -12), ('小明', 12)])

    async def test_transfer_refusals(self):
        self.host.scores['玩家'] = 5
        self.assertEqual(await self.rejected(self.transfer('u1', 'u2', 6)), 'insufficient_funds')
        self.assertEqual(await self.rejected(self.transfer('u1', 'u1', 1)), 'invalid_argument')
        self.assertEqual(await self.rejected(self.transfer('u3', 'u1', 1)), 'player_offline')
        self.assertEqual(self.host.adds(), [])
        self.assertNotIn('a:u2', self.host.values)

    async def test_records_from_2_0_0_are_read_without_their_balance(self):
        self.ctx.config = Settings(starting_balance=50)
        old = {'id': 'r0', 'delta': 99, 'reason': '旧奖励', 'at': '2026-10-01T00:00:00+00:00'}
        self.host.values['a:u1'] = {'balance': 99, 'recent': [old]}
        self.assertEqual(await self.grant('u1', 10, 'r0'), self.done(0, duplicate=True))
        await self.join(PLAYER)  # 旧账户不再发初始金币
        self.assertEqual(self.host.adds(), [])
        self.assertEqual(await self.grant('u1', 10, 'r1'), self.done(10))
        self.assertEqual(self.record('u1')['pending'], 0)
        self.assertEqual([e['id'] for e in self.record('u1')['recent']], ['r0', 'r1'])
        self.assertNotIn('balance', self.record('u1'))

    async def test_recent_keeps_last_twenty_as_the_repeat_window(self):
        for index in range(25):
            await self.grant('u1', 1, f'r{index}')
        self.assertEqual([e['id'] for e in self.record('u1')['recent']], [f'r{i}' for i in range(5, 25)])
        self.assertEqual((await self.grant('u1', 1, 'r5'))['duplicate'], True)
        self.assertEqual(await self.grant('u1', 1, 'r0'), self.done(26))

    # ---- 启动 ----

    async def test_start_creates_missing_objective(self):
        self.assertEqual(self.host.objectives, [('coin', '金币')])
        self.assertEqual(self.host.displays, [])
        self.host.submitted.clear()
        await self.start()
        self.assertEqual([op for op, _ in self.host.submitted], ['framework.scoreboard.objectives.list'])

    async def test_start_sets_display_and_tolerates_failures(self):
        self.ctx.config = Settings(objective='gold', create_objective=False, display_slot='sidebar')
        await self.start()
        self.assertEqual(self.host.displays, [('sidebar', 'gold')])
        self.host.inject['framework.scoreboard.objectives.list'] = ['failed']
        self.ctx.config = Settings(objective='gold')
        await self.start()
        self.assertEqual(self.host.objectives, [('coin', '金币')])

    def test_settings_reject_bad_objective_or_slot(self):
        for options in ({'objective': 'has space'}, {'objective': 'x' * 17}, {'display_slot': 'top'}):
            with self.assertRaises(ValueError):
                Settings(**options)

    # ---- 玩家命令 ----

    async def test_balance_command(self):
        await self.say('!余额')
        self.assertEqual(self.host.reply(), '你有 0 金币')
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        self.host.scores['玩家'] = 7
        self.ctx.config = Settings(currency_name='积分')
        await self.say('！ye')
        self.assertEqual(self.host.reply(), '你有 7 积分')
        self.host.inject['framework.scoreboard.get'] = ['failed']
        await self.say('!余额')
        self.assertEqual(self.host.reply(), '暂时查不到余额，请稍后再试')

    async def test_history_lists_ten_newest_first(self):
        await self.say('!流水')
        self.assertEqual(self.host.reply(), '还没有收支记录。')
        for index in range(12):
            await self.grant('u1', index + 1, f'r{index}', reason=f'奖励{index}')
        await self.pay('u1', 3, 'p')
        await self.say('!ls')
        lines = self.host.reply().split('\n')
        self.assertEqual(lines[0], '最近的收支：')
        self.assertEqual(lines[1:4], ['-3 购买 面包', '+12 奖励11', '+11 奖励10'])
        self.assertEqual(len(lines), 11)

    async def test_transfer_command_to_online_player_notifies_both(self):
        self.host.scores['玩家'] = 30
        await self.say('!转账 小明 10')
        self.assertEqual(self.host.scores, {'玩家': 20, '小明': 10})
        self.assertEqual(self.host.reply(), '已转给 小明 10 金币，剩余 20 金币')
        self.assertEqual(self.host.reply('小明'), '玩家 转给你 10 金币，现有 10 金币')
        payer, payee = self.record('u1')['recent'][-1], self.record('u2')['recent'][-1]
        self.assertEqual(payer['id'], payee['id'])
        self.assertEqual((payer['reason'], payee['reason']), ('转给 小明', '收到 玩家 的转账'))
        self.ctx.services.call.assert_not_called()

    async def test_transfer_command_redelivery_does_not_pay_twice(self):
        self.host.scores['玩家'] = 30
        await self.say('!转账 小明 10', event_id='same')
        await self.say('!转账 小明 10', event_id='same')
        self.assertEqual(self.host.scores, {'玩家': 20, '小明': 10})
        self.assertEqual(self.host.reply(), '已转给 小明 10 金币，剩余 20 金币')

    async def test_transfer_command_to_offline_player(self):
        self.host.scores['玩家'] = 30
        await self.say('!zz Steve 5')
        self.ctx.services.call.assert_awaited_once_with(
            'plugin.neomega.players.resolve', {'name': 'Steve'}, timeout=5)
        self.assertEqual(self.record('u3')['pending'], 5)
        self.assertEqual(self.host.reply(), '已转给 Steve 5 金币，剩余 25 金币\nSteve 不在线，金币会在他下次进服时到账')
        self.assertNotIn('@a[name="Steve"]', self.host.replies())

    async def test_transfer_player_not_found(self):
        self.ctx.services.call.side_effect = ServiceRejected('not_found')
        await self.say('!pay Nobody 5')
        self.assertEqual(self.host.reply(), '找不到玩家 Nobody')
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_transfer_lookup_errors_ask_to_retry_later(self):
        for error in (ServiceFailed({'call_id': 'c1'}), ServiceWaitTimeout('c1'),
                      ServiceUncertain(None), ServiceRejected('service_deadline_expired')):
            self.ctx.services.call.side_effect = error
            await self.say('!转账 Steve 5')
            self.assertEqual(self.host.reply(), '暂时查不到玩家，请稍后再试')
            self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_transfer_disabled(self):
        self.ctx.config = Settings(allow_transfer=False)
        await self.say('!转账 小明 10')
        self.assertEqual(self.host.reply(), '服务器已关闭转账')
        self.ctx.players.assert_not_called()

    async def test_transfer_command_refusals(self):
        self.host.scores['玩家'] = 5
        await self.say('!转账 小明 6')
        self.assertEqual(self.host.reply(), '金币不足，转不出 6 金币')
        await self.say('!转账 玩家 1')
        self.assertEqual(self.host.reply(), '不能转账给自己')
        for amount in ('0', '-1', '1.5', 'abc'):
            await self.say(f'!转账 小明 {amount}')
            self.assertEqual(self.host.reply(), '用法：!转账 <玩家> <数量>，数量是正整数')
        self.host.inject['framework.scoreboard.get'] = ['unknown']
        await self.say('!转账 小明 1')
        self.assertEqual(self.host.reply(), '暂时查不到余额，请稍后再试')
        self.assertEqual(self.host.scores, {'玩家': 5})
        self.assertNotIn('a:u2', self.host.values)

    async def test_other_chat_is_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!转账 小明', '!转账 "没闭合 1'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])


if __name__ == '__main__':
    unittest.main()
