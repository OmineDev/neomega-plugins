"""Drive the shop through the real SDK storage and neomega_menu against an in-memory Host.

Economy calls go through a replaced ctx.services.call that replays scripted outcomes.
"""
import asyncio
import copy
import json
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext, ServiceRejected
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceUncertain, ServiceWaitTimeout
from main import Item, Settings, Shop

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}
PAY, BALANCE = 'plugin.neomega.economy.pay', 'plugin.neomega.economy.balance'


def text_of(command):
    return ''.join(part['text'] for part in json.loads(command[command.index('{'):])['rawtext'])


class FakeHost:
    """state.get / state.commit / events.commit with the global revision CAS, plus direct operations."""
    def __init__(self):
        self.values, self.revision, self.commits, self.sent = {}, 0, [], []
        self.lose_state_commit = False

    async def call(self, method, payload, timeout=None):
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        if method == 'operations.submit':
            self.sent.append(text_of(payload['arguments']['cmd']))
            return {'operation_id': f'op{len(self.sent)}', 'state': 'accepted', 'duplicate': False}
        assert method in ('state.commit', 'events.commit'), method
        if method == 'state.commit' and self.lose_state_commit:
            raise ConnectionError('reply lost')
        assert payload['expected_state_revision'] == self.revision
        for mutation in payload['mutations']:
            self.values[mutation['key']] = mutation['value']
        self.revision += 1
        self.commits.append(payload)
        return {'commit_id': payload['commit_id'], 'revision': self.revision, 'duplicate': False,
                'operation_ids': [f'op{i}' for i in range(len(payload['actions']))]}

    def deliveries(self):
        return [c for c in self.commits if 'delivery_token' not in c]

    def commands(self):
        return [a['arguments']['cmd'] for c in self.deliveries() for a in c['actions']]


class ShopTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host, self.tasks = FakeHost(), []
        worker = SimpleNamespace(peer=self.host, log=logging.getLogger('shop-test'),
                                 spawn=lambda coroutine, name=None: self.track(coroutine))
        self.ctx = PluginContext(worker, None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.resolve_player = AsyncMock(return_value=dict(PLAYER))
        self.outcomes, self.calls = {PAY: [], BALANCE: []}, []
        self.ctx.services.call = self.call
        self.plugin = Shop()
        await self.plugin.on_start(self.ctx)

    def track(self, coroutine):
        task = asyncio.ensure_future(coroutine)
        self.tasks.append(task)
        return task

    async def call(self, service, arguments, *, major=1, timeout=5, idempotency_key=None):
        self.assertEqual((major, timeout), (1, 5))
        self.calls.append((service, dict(arguments)))
        # The menu looks up the balance first; unscripted lookups behave as if economy were busy.
        outcomes = self.outcomes[service]
        outcome = outcomes.pop(0) if outcomes or service != BALANCE else ServiceWaitTimeout('busy')
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def say(self, message):
        for _ in range(5):  # let the menu flow reach its next wait
            await asyncio.sleep(0)
        commits = len(self.host.commits)
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'chat.received', 'payload': {'player': '玩家', 'xuid': '42', 'message': message},
            'subscription_id': 'chat', 'event_id': 'e1', 'delivery_token': 'd1'}))
        # Every chat event is acknowledged in exactly one commit.
        self.assertEqual(len(self.host.commits), commits + 1)
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    async def buy(self, *replies):
        await self.say('!商店')
        for reply in replies:
            await self.say(reply)
        await asyncio.gather(*self.tasks)

    def pays(self):
        return [arguments for service, arguments in self.calls if service == PAY]

    async def test_purchase_charges_then_delivers_in_one_commit(self):
        self.outcomes[PAY] = [{'balance': 90, 'duplicate': False}]
        await self.buy('1', '1')
        self.assertIn('1. 面包 ×16 — 10 金币', self.host.sent[0])
        self.assertIn('2. 钻石 — 100 金币', self.host.sent[0])
        self.assertIn('购买「面包 ×16」需要 10 金币，确认吗？', self.host.sent[1])
        [pay] = self.pays()
        self.assertEqual({k: pay[k] for k in ('player_id', 'amount', 'reason')},
                         {'player_id': 'u1', 'amount': 10, 'reason': '购买 面包 ×16'})
        self.assertRegex(pay['request_id'], r'^[0-9a-f]{32}$')
        [delivery] = self.host.deliveries()
        self.assertEqual(delivery['mutations'], [])
        commands = self.host.commands()
        self.assertIn('give @a[name="玩家"] bread 16', commands)
        self.assertIn('购买成功，剩余 90 金币。', ''.join(text_of(c) for c in commands if c.startswith('tellraw')))

    async def test_menu_shows_balance_in_the_economy_currency(self):
        self.outcomes[BALANCE] = [{'balance': 120, 'currency': '积分', 'pending': False}]
        self.outcomes[PAY] = [{'balance': 110, 'duplicate': False, 'currency': '积分', 'pending': False}]
        await self.buy('1', '1')
        self.assertIn('积分商店（你有 120 积分）', self.host.sent[0])
        self.assertIn('1. 面包 ×16 — 10 积分', self.host.sent[0])
        self.assertIn('需要 10 积分', self.host.sent[1])
        self.assertIn('购买成功，剩余 110 积分。',
                      ''.join(text_of(c) for c in self.host.commands() if c.startswith('tellraw')))

    async def test_player_leaving_before_payment_charges_and_says_nothing(self):
        self.outcomes[PAY] = [ServiceRejected('player_offline')]
        await self.buy('1', '1')
        self.assertEqual(self.host.deliveries(), [])
        self.assertEqual(len(self.host.sent), 2)

    async def test_full_width_prefix_and_alias_open_the_shop(self):
        self.outcomes[PAY] = [{'balance': 0, 'duplicate': False}]
        await self.say('！sd')
        await self.say('2')
        await self.say('确认')
        await asyncio.gather(*self.tasks)
        self.assertEqual(self.pays()[0]['amount'], 100)
        self.assertIn('give @a[name="玩家"] diamond 1', self.host.commands())

    async def test_insufficient_funds_shows_balance_and_delivers_nothing(self):
        self.outcomes[PAY] = [ServiceRejected('insufficient_funds')]
        self.outcomes[BALANCE] = [ServiceWaitTimeout('c0'), {'balance': 30}]
        await self.buy('2', '1')
        self.assertEqual(self.calls[-1], (BALANCE, {'player_id': 'u1'}))
        self.assertEqual(self.host.sent[-1], '金币不足，需要 100 金币，你现在有 30 金币。')
        self.assertEqual(self.host.deliveries(), [])

    async def test_insufficient_funds_without_balance_when_lookup_fails(self):
        self.outcomes[PAY] = [ServiceRejected('insufficient_funds')]
        self.outcomes[BALANCE] = [ServiceWaitTimeout('c0'), ServiceWaitTimeout('c1')]
        await self.buy('2', '1')
        self.assertEqual(self.host.sent[-1], '金币不足。')

    async def test_cancel_at_menu_or_confirmation_charges_nothing(self):
        await self.buy('0')
        self.assertIn('已取消', self.host.sent[-1])
        self.tasks.clear()
        await self.buy('1', '不要')
        self.assertEqual(self.pays(), [])
        self.assertEqual(self.host.deliveries(), [])

    async def test_timeout_retries_with_same_request_id(self):
        self.outcomes[PAY] = [ServiceWaitTimeout('c1'), {'balance': 90, 'duplicate': False}]
        await self.buy('1', '1')
        first, second = self.pays()
        self.assertEqual(first, second)
        self.assertIn('give @a[name="玩家"] bread 16', self.host.commands())

    async def test_retry_still_unsettled_delivers_nothing(self):
        self.outcomes[PAY] = [ServiceUncertain(None), ServiceWaitTimeout('c2')]
        await self.buy('1', '1')
        first, second = self.pays()
        self.assertEqual(first['request_id'], second['request_id'])
        self.assertEqual(self.host.deliveries(), [])
        self.assertEqual(self.host.sent[-1], '扣款结果暂时不确定，请用 !流水 查看；没有扣款可以重新购买。')

    async def test_duplicate_charge_counts_as_success(self):
        self.outcomes[PAY] = [ServiceWaitTimeout('c1'), {'balance': 90, 'duplicate': True}]
        await self.buy('1', '1')
        self.assertIn('give @a[name="玩家"] bread 16', self.host.commands())

    async def test_lost_delivery_commit_tells_player_to_contact_admin(self):
        self.outcomes[PAY] = [{'balance': 90, 'duplicate': False}]
        self.host.lose_state_commit = True
        with self.assertLogs('shop-test', 'WARNING'):
            await self.buy('1', '1')
        self.assertEqual(self.host.sent[-1], '发货结果不确定，请联系管理员。')
        self.assertEqual(len(self.pays()), 1)

    async def test_other_chat_is_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!商店 "没闭合'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.tasks, [])

    def test_rejects_unusable_settings(self):
        with self.assertRaises(ValueError):
            Settings(items=[])
        with self.assertRaises(ValueError):
            Settings(items=[Item('钻石', 1, ['give {player} diamond 1'])] * 2)
        with self.assertRaises(ValueError):
            Settings(items=[Item('钻石', 1, [' '])])


if __name__ == '__main__':
    unittest.main()
