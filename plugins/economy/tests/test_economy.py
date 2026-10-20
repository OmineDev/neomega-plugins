"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
import json
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext, ServiceRejected
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from main import Economy, Settings

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}
FRIEND = {'name': '小明', 'uuid': 'u2', 'xuid': '43'}
OFFLINE = {'id': 'u3', 'name': 'Steve', 'uuid': 'u3', 'xuid': '44', 'names': [], 'last_seen': '2026-10-01T00:00:00+00:00'}


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

    def replies(self):
        """Text of the last commit's tellraw commands, grouped by recipient selector."""
        texts = {}
        for action in self.commits[-1]['actions']:
            command = action['arguments']['cmd']
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
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('economy-test')), None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.online = [dict(PLAYER), dict(FRIEND)]
        self.ctx.players = AsyncMock(side_effect=lambda: [dict(p) for p in self.online])
        self.ctx.resolve_player = AsyncMock(side_effect=lambda name=None, **_: dict(
            next(p for p in self.online if p['name'] == name)))
        self.ctx.services.call = AsyncMock(return_value=dict(OFFLINE))
        self.plugin = Economy()
        await self.plugin.on_start(self.ctx)

    async def say(self, message):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'chat.received', 'payload': {'player': '玩家', 'xuid': '42', 'message': message},
            'subscription_id': 'chat', 'event_id': 'e1', 'delivery_token': 'd1'}))
        # Every event is acknowledged in exactly one commit.
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    async def grant(self, player_id, amount, request_id, reason='签到奖励'):
        return await self.plugin.grant(self.ctx, {'player_id': player_id, 'amount': amount,
                                                  'reason': reason, 'request_id': request_id})

    def balance_of(self, player_id):
        return self.host.values['a:' + player_id]['balance']

    # ---- 服务 ----

    async def test_balance_of_new_account_uses_starting_balance_without_writing(self):
        self.ctx.config = Settings(starting_balance=50)
        self.assertEqual(await self.plugin.balance(self.ctx, {'player_id': 'u1'}), {'balance': 50})
        self.assertEqual(self.host.commits, [])
        await self.grant('u1', 10, 'r1')
        self.assertEqual(await self.plugin.balance(self.ctx, {'player_id': 'u1'}), {'balance': 60})

    async def test_grant_records_and_ignores_repeated_request(self):
        self.assertEqual(await self.grant('u1', 10, 'r1'), {'balance': 10, 'duplicate': False})
        record = self.host.values['a:u1']
        self.assertEqual(len(record['recent']), 1)
        self.assertEqual({k: record['recent'][0][k] for k in ('id', 'delta', 'reason')},
                         {'id': 'r1', 'delta': 10, 'reason': '签到奖励'})
        self.assertTrue(record['recent'][0]['at'].endswith('+00:00'))
        self.assertEqual(await self.grant('u1', 10, 'r1'), {'balance': 10, 'duplicate': True})
        self.assertEqual(len(self.host.commits), 1)

    async def test_pay_deducts_rejects_shortfall_and_ignores_repeated_request(self):
        await self.grant('u1', 30, 'r1')
        pay = {'player_id': 'u1', 'amount': 20, 'reason': '购买 面包', 'request_id': 'p1'}
        self.assertEqual(await self.plugin.pay(self.ctx, pay), {'balance': 10, 'duplicate': False})
        self.assertEqual(await self.plugin.pay(self.ctx, pay), {'balance': 10, 'duplicate': True})
        with self.assertRaises(ServiceRejected) as caught:
            await self.plugin.pay(self.ctx, dict(pay, request_id='p2'))
        self.assertEqual(caught.exception.code, 'insufficient_funds')
        self.assertEqual(self.balance_of('u1'), 10)
        self.assertEqual(len(self.host.commits), 2)

    async def test_pay_on_new_account_rejects_without_writing(self):
        with self.assertRaises(ServiceRejected) as caught:
            await self.plugin.pay(self.ctx, {'player_id': 'u9', 'amount': 1, 'reason': '购买', 'request_id': 'p1'})
        self.assertEqual(caught.exception.code, 'insufficient_funds')
        self.assertEqual(self.host.commits, [])

    async def test_transfer_writes_both_sides_with_one_id(self):
        await self.grant('u1', 30, 'r1')
        args = {'from_id': 'u1', 'to_id': 'u2', 'amount': 12, 'reason': '任务结算', 'request_id': 't1'}
        self.assertEqual(await self.plugin.transfer(self.ctx, args), {'balance': 18, 'duplicate': False})
        self.assertEqual(self.balance_of('u2'), 12)
        payer, payee = self.host.values['a:u1']['recent'][-1], self.host.values['a:u2']['recent'][-1]
        self.assertEqual((payer['id'], payer['delta']), ('t1', -12))
        self.assertEqual((payee['id'], payee['delta']), ('t1', 12))
        self.assertEqual(len(self.host.commits[-1]['mutations']), 2)
        # Repeated request returns the payer's current balance and changes nothing.
        self.assertEqual(await self.plugin.transfer(self.ctx, args), {'balance': 18, 'duplicate': True})
        self.assertEqual(len(self.host.commits), 2)

    async def test_transfer_rejects_shortfall_and_self(self):
        await self.grant('u1', 5, 'r1')
        args = {'from_id': 'u1', 'to_id': 'u2', 'amount': 6, 'reason': '任务结算', 'request_id': 't1'}
        with self.assertRaises(ServiceRejected) as caught:
            await self.plugin.transfer(self.ctx, args)
        self.assertEqual(caught.exception.code, 'insufficient_funds')
        with self.assertRaises(ServiceRejected) as caught:
            await self.plugin.transfer(self.ctx, dict(args, to_id='u1', amount=1))
        self.assertEqual(caught.exception.code, 'invalid_argument')
        self.assertEqual(len(self.host.commits), 1)
        self.assertNotIn('a:u2', self.host.values)

    async def test_recent_keeps_last_twenty_as_the_repeat_window(self):
        for index in range(25):
            await self.grant('u1', 1, f'r{index}')
        recent = self.host.values['a:u1']['recent']
        self.assertEqual([entry['id'] for entry in recent], [f'r{i}' for i in range(5, 25)])
        self.assertEqual((await self.grant('u1', 1, 'r5'))['duplicate'], True)
        # An id pushed out of the window is no longer recognised.
        self.assertEqual(await self.grant('u1', 1, 'r0'), {'balance': 26, 'duplicate': False})

    # ---- 玩家命令 ----

    async def test_balance_command(self):
        await self.say('!余额')
        self.assertEqual(self.host.reply(), '你有 0 金币')
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        await self.grant('u1', 7, 'r1')
        self.ctx.config = Settings(currency_name='积分')
        await self.say('！ye')
        self.assertEqual(self.host.reply(), '你有 7 积分')

    async def test_history_lists_ten_newest_first(self):
        await self.say('!流水')
        self.assertEqual(self.host.reply(), '还没有收支记录。')
        for index in range(12):
            await self.grant('u1', index + 1, f'r{index}', reason=f'奖励{index}')
        await self.plugin.pay(self.ctx, {'player_id': 'u1', 'amount': 3, 'reason': '购买 面包', 'request_id': 'p'})
        await self.say('!ls')
        lines = self.host.reply().split('\n')
        self.assertEqual(lines[0], '最近的收支：')
        self.assertEqual(lines[1:4], ['-3 购买 面包', '+12 奖励11', '+11 奖励10'])
        self.assertEqual(len(lines), 11)

    async def test_transfer_to_online_player_notifies_both(self):
        await self.grant('u1', 30, 'r1')
        await self.say('!转账 小明 10')
        self.assertEqual(self.balance_of('u1'), 20)
        self.assertEqual(self.balance_of('u2'), 10)
        self.assertEqual(self.host.reply(), '已转给 小明 10 金币，剩余 20 金币')
        self.assertEqual(self.host.reply('小明'), '玩家 转给你 10 金币，现有 10 金币')
        payer, payee = self.host.values['a:u1']['recent'][-1], self.host.values['a:u2']['recent'][-1]
        self.assertEqual(payer['id'], payee['id'])
        self.assertEqual((payer['reason'], payee['reason']), ('转给 小明', '收到 玩家 的转账'))
        self.ctx.services.call.assert_not_called()

    async def test_transfer_to_offline_player_resolves_through_players(self):
        await self.grant('u1', 30, 'r1')
        await self.say('!zz Steve 5')
        self.ctx.services.call.assert_awaited_once_with(
            'plugin.neomega.players.resolve', {'name': 'Steve'}, timeout=5)
        self.assertEqual(self.balance_of('u3'), 5)
        self.assertEqual(self.host.reply(), '已转给 Steve 5 金币，剩余 25 金币')
        self.assertIn('@a[name="Steve"]', self.host.replies())

    async def test_transfer_player_not_found(self):
        self.ctx.services.call.side_effect = ServiceRejected('not_found')
        await self.say('!pay Nobody 5')
        self.assertEqual(self.host.reply(), '找不到玩家 Nobody')
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_transfer_lookup_errors_ask_to_retry_later(self):
        await self.grant('u1', 30, 'r1')
        for error in (ServiceFailed({'call_id': 'c1'}), ServiceWaitTimeout('c1'),
                      ServiceUncertain(None), ServiceRejected('service_deadline_expired')):
            self.ctx.services.call.side_effect = error
            await self.say('!转账 Steve 5')
            self.assertEqual(self.host.reply(), '暂时查不到玩家，请稍后再试')
            self.assertEqual(self.host.commits[-1]['mutations'], [])
        self.assertEqual(self.balance_of('u1'), 30)

    async def test_transfer_disabled(self):
        await self.grant('u1', 30, 'r1')
        self.ctx.config = Settings(allow_transfer=False)
        await self.say('!转账 小明 10')
        self.assertEqual(self.host.reply(), '服务器已关闭转账')
        self.assertEqual(self.balance_of('u1'), 30)
        self.ctx.players.assert_not_called()

    async def test_transfer_command_refusals(self):
        await self.grant('u1', 5, 'r1')
        await self.say('!转账 小明 6')
        self.assertEqual(self.host.reply(), '金币不足，你只有 5 金币')
        await self.say('!转账 玩家 1')
        self.assertEqual(self.host.reply(), '不能转账给自己')
        for amount in ('0', '-1', '1.5', 'abc'):
            await self.say(f'!转账 小明 {amount}')
            self.assertEqual(self.host.reply(), '用法：!转账 <玩家> <数量>，数量是正整数')
        self.assertEqual(self.balance_of('u1'), 5)
        self.assertNotIn('a:u2', self.host.values)

    async def test_other_chat_is_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!转账 小明', '!转账 "没闭合 1'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])


if __name__ == '__main__':
    unittest.main()
