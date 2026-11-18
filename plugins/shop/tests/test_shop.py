"""Drive the shop through the real SDK storage and neomega_menu against an in-memory Host.

Economy calls go through a replaced ctx.services.call that replays scripted outcomes.
"""
import asyncio
import copy
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from neomega_runtime import Event, PluginContext, ServiceRejected
from neomega_runtime.config import load_config
from neomega_runtime.config_schema import check_config_schema, schema_for_config
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

    async def test_unavailable_charge_retries_then_reports_uncertain(self):
        self.outcomes[PAY] = [ServiceRejected('unavailable'), {'balance': 90, 'duplicate': True}]
        await self.buy('1', '1')
        first, second = self.pays()
        self.assertEqual(first['request_id'], second['request_id'])
        self.assertIn('give @a[name="玩家"] bread 16', self.host.commands())
        self.calls.clear()
        self.outcomes[PAY] = [ServiceRejected('unavailable'), ServiceRejected('unavailable')]
        await self.buy('1', '1')
        self.assertIn('扣款结果暂时不确定', self.host.sent[-1])
        self.assertEqual(len(self.host.deliveries()), 1)

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
            Settings(items=[Item(' ', 1, ['give {player} diamond 1'])])
        with self.assertRaises(ValueError):
            Settings(items=[Item('钻石', 1, [])])
        with self.assertRaises(ValueError):
            Settings(items=[Item('钻石', 1, ['give {player} diamond 1'])] * 2)
        with self.assertRaises(ValueError):
            Settings(items=[Item('钻石', 1, ['give {player} diamond 1']),
                            Item(' 钻石 ', 2, ['give {player} bread 1'])])
        with self.assertRaises(ValueError):
            Settings(items=[Item('钻石', 1, [' '])])

    def test_schema_example_and_purposes_match_settings(self):
        root = Path(__file__).resolve().parents[1]
        schema = json.loads((root / 'config.schema.json').read_text())
        check_config_schema(schema)
        self.assertEqual(schema, schema_for_config(Settings))
        self.assertEqual(load_config(Settings, root / 'config.example.json'), Settings())
        manifest = json.loads((root / 'manifest.json').read_text())
        self.assertIn('event:chat.received', manifest['permissions']['purposes'])

    async def test_spawn_guards_target_and_unknown_rejection(self):
        self.outcomes[PAY] = [ServiceRejected('unexpected_code')]
        with self.assertLogs('shop-test', 'WARNING'):
            await self.buy('1', '1')
        self.assertEqual(self.host.sent[-1], '扣款失败，请稍后再试。')
        self.assertEqual(self.host.deliveries(), [])

        self.tasks.clear()
        self.calls.clear()
        self.ctx.resolve_player = AsyncMock(return_value={'name': 'bad"name', 'uuid': 'u2', 'xuid': '43'})
        with self.assertLogs('shop-test', 'WARNING'):
            await self.buy('1', '1')
        self.assertEqual(self.pays(), [])

    async def test_serve_ack_lifecycle(self):
        host = self.host
        events = [
            {'kind': 'chat.received', 'payload': {'player': '玩家', 'xuid': '42', 'message': '你好'},
             'subscription_id': 'chat', 'event_id': 'ev1', 'delivery_token': 'dt1'},
        ]

        class DriveWorker:
            def __init__(self, peer, **callbacks):
                self.peer, self.callbacks = peer, callbacks
                self.log = logging.getLogger('shop-serve-test')
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
            await Shop().serve(peer, data_dir=tmp)
        self.assertEqual([c['event_id'] for c in host.commits], ['ev1'])


if __name__ == '__main__':
    unittest.main()
