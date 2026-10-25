"""Drive the plugin through the real SDK storage against an in-memory Host."""
import asyncio
import copy
from datetime import date
import json
import logging
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext
from neomega_runtime.config import load_config
from neomega_runtime.config_schema import schema_for_config
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceWaitTimeout
from neomega_runtime.storage import CommitUncertain
from main import Code, RedeemCodes, Settings

ROOT = Path(__file__).resolve().parents[1]


def player(name):
    return {'name': name, 'uuid': 'u-' + name, 'xuid': 'x-' + name}


def text_of(command):
    return ''.join(part['text'] for part in json.loads(command[command.index('{'):])['rawtext'])


class FakeHost:
    """state.get / state.commit / events.commit with the Host's global revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits, self.sent = {}, 0, [], []
        self.lose_commit = False

    async def call(self, method, payload, timeout=None):
        await asyncio.sleep(0)  # a real IPC round trip lets other tasks run
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        if method == 'operations.submit':
            self.sent.append(text_of(payload['arguments']['cmd']))
            return {'operation_id': f'op{len(self.sent)}', 'state': 'accepted', 'duplicate': False}
        assert method in ('state.commit', 'events.commit'), method
        if self.lose_commit:
            raise ConnectionError('reply lost')
        if payload['expected_state_revision'] != self.revision:
            raise IPCRejected('revision_conflict', 'r1', True)
        for mutation in payload['mutations']:
            if mutation.get('delete'):
                self.values.pop(mutation['key'], None)
            else:
                self.values[mutation['key']] = mutation['value']
        self.revision += 1
        self.commits.append(payload)
        return {'commit_id': payload['commit_id'], 'revision': self.revision, 'duplicate': False,
                'operation_ids': [f'op{i}' for i in range(len(payload['actions']))]}

    def commands(self, commit=None):
        commit = self.commits[-1] if commit is None else commit
        return [action['arguments']['cmd'] for action in commit['actions']]

    def rewards(self):
        """Every non-reply command committed so far."""
        return [c for commit in self.commits for c in self.commands(commit) if not c.startswith('tellraw')]

    def reply(self, commit=None):
        return ''.join(text_of(c) for c in self.commands(commit) if c.startswith('tellraw'))


CODES = [
    Code('VIP2026', ['give {player} diamond 1', '/xp 10L {player}'], message='欢迎 VIP！'),
    Code('LIMITED', ['give {player} apple 1'], total_uses=2),
    Code('TWICE', ['give {player} bread 1'], per_player_uses=2),
    Code('NEWYEAR', ['give {player} cake 1'], expires='2026-10-21'),
    Code('COINS', ['give {player} stick 1'], coins=50),
]


class RedeemTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.day = date(2026, 10, 21)
        self.plugin, self.ctx = await self.start()
        self.ctx.services.call = AsyncMock(side_effect=IPCRejected('permission_denied', 'r1', False))

    async def start(self, config=None):
        ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('redeem-test')), None)
        ctx.config = config or Settings(codes=CODES)
        ctx._operations = Operations(self.host, 's1')
        ctx.resolve_player = AsyncMock(side_effect=lambda name, xuid=None: player(name))
        plugin = RedeemCodes()
        await plugin.on_start(ctx)
        plugin.today = lambda ctx: self.day
        return plugin, ctx

    def event(self, message, name='玩家', token='d1'):
        return Event(self.ctx, {
            'kind': 'chat.received', 'payload': {'player': name, 'xuid': 'x-' + name, 'message': message},
            'subscription_id': 'chat', 'event_id': 'e-' + token, 'delivery_token': token})

    async def say(self, message, name='玩家'):
        await self.plugin.on_event(self.ctx, self.event(message, name))
        # Every event is acknowledged in exactly one commit.
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')
        return self.host.reply()

    async def test_success_rewards_records_and_replies_in_one_commit(self):
        reply = await self.say('!兑换 VIP2026')
        self.assertEqual(len(self.host.commits), 1)
        self.assertEqual(self.host.rewards(), ['give @a[name="玩家"] diamond 1', 'xp 10L @a[name="玩家"]'])
        self.assertEqual(reply, '欢迎 VIP！')
        self.assertEqual(self.host.values, {'code:vip2026': 1, 'use:vip2026:u-玩家': 1})

    async def test_code_is_case_insensitive_and_aliases_work(self):
        self.assertEqual(await self.say('!兑换 vip2026'), '欢迎 VIP！')
        self.assertEqual(await self.say('！cdk Vip2026'), '你已经领过这个兑换码了。')
        self.assertEqual(await self.say('!CDK vIP2026'), '你已经领过这个兑换码了。')
        self.assertEqual(self.host.values['code:vip2026'], 1)

    async def test_expired_code_gives_nothing(self):
        self.assertEqual(await self.say('!兑换 newyear'), '兑换成功！')  # the expiry day itself still counts
        self.day = date(2026, 10, 22)
        self.assertEqual(await self.say('!兑换 newyear', '别人'), '这个兑换码已经过期了（有效期到 2026-10-21）。')
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        self.assertEqual(self.host.rewards(), ['give @a[name="玩家"] cake 1'])

    async def test_total_uses_run_out(self):
        for name in ('甲', '乙'):
            self.assertEqual(await self.say('!兑换 LIMITED', name), '兑换成功！')
        self.assertEqual(await self.say('!兑换 LIMITED', '丙'), '这个兑换码已经被领完了。')
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        self.assertEqual(len(self.host.rewards()), 2)
        self.assertEqual(self.host.values['code:limited'], 2)

    async def test_per_player_limit(self):
        self.assertIn('这个兑换码你还能再用 1 次。', await self.say('!兑换 TWICE'))
        self.assertEqual(await self.say('!兑换 TWICE'), '兑换成功！')
        self.assertEqual(await self.say('!兑换 TWICE'), '这个兑换码每人只能用 2 次，你已经用完了。')
        self.assertEqual(await self.say('!兑换 TWICE', '别人'), '兑换成功！\n这个兑换码你还能再用 1 次。')
        self.assertEqual(self.host.values['use:twice:u-玩家'], 2)
        self.assertEqual(self.host.values['code:twice'], 3)

    async def test_concurrent_redemptions_never_exceed_total(self):
        names = [f'玩家{i}' for i in range(8)]
        await asyncio.gather(*(self.plugin.on_event(self.ctx, self.event('!兑换 LIMITED', name, f't{i}'))
                               for i, name in enumerate(names)))
        self.assertEqual(len(self.host.commits), 8)
        self.assertEqual(len(self.host.rewards()), 2)
        replies = [self.host.reply(commit) for commit in self.host.commits]
        self.assertEqual(replies.count('兑换成功！'), 2)
        self.assertEqual(replies.count('这个兑换码已经被领完了。'), 6)
        self.assertEqual(self.host.values['code:limited'], 2)

    async def test_concurrent_same_player_redeems_once(self):
        await asyncio.gather(*(self.plugin.on_event(self.ctx, self.event('!兑换 VIP2026', '玩家', f't{i}'))
                               for i in range(5)))
        self.assertEqual(len(self.host.rewards()), 2)  # one redemption, two commands
        self.assertEqual(self.host.values, {'code:vip2026': 1, 'use:vip2026:u-玩家': 1})

    async def test_foreign_write_between_read_and_commit_is_rejected_whole(self):
        # Another commit lands while the coin grant is in flight: the snapshot is stale.
        async def grant(*args, **kwargs):
            self.host.revision += 1
            return {'balance': 50, 'duplicate': False}
        self.ctx.services.call = AsyncMock(side_effect=grant)
        with self.assertRaises(IPCRejected):
            await self.plugin.on_event(self.ctx, self.event('!兑换 COINS'))
        self.assertEqual((self.host.values, self.host.commits), ({}, []))
        # Redelivery evaluates again from fresh state; economy dedupes on the same request_id.
        self.ctx.services.call = AsyncMock(return_value={'balance': 50, 'duplicate': True})
        await self.say('!兑换 COINS')
        self.assertEqual(self.host.values['code:coins'], 1)
        self.assertEqual(self.ctx.services.call.await_args.args[1]['request_id'], 'cdk-1-coins')

    async def test_no_economy_skips_coins_but_gives_other_rewards(self):
        reply = await self.say('!兑换 COINS')
        self.assertEqual(reply, '兑换成功！')
        self.assertEqual(self.host.rewards(), ['give @a[name="玩家"] stick 1'])
        self.assertEqual(self.host.values['use:coins:u-玩家'], 1)

    async def test_grants_coins_with_stable_request_id(self):
        self.ctx.services.call = AsyncMock(return_value={'balance': 80, 'duplicate': False,
                                                         'currency': '积分', 'pending': False})
        self.assertIn('获得 50 积分，现有 80 积分。', await self.say('!兑换 coins'))
        name, arguments = self.ctx.services.call.await_args.args
        self.assertEqual(name, 'plugin.neomega.economy.grant')
        self.assertEqual(arguments, {'player_id': 'u-玩家', 'amount': 50, 'reason': '兑换码 COINS',
                                     'request_id': 'cdk-1-coins'})

    async def test_unsettled_grant_still_records_redemption(self):
        self.ctx.services.call = AsyncMock(side_effect=ServiceWaitTimeout('c1'))
        self.assertIn('金币奖励结果暂时不确定', await self.say('!兑换 COINS'))
        self.assertEqual(self.host.values['code:coins'], 1)

    async def test_records_survive_restart(self):
        await self.say('!兑换 VIP2026')
        await self.say('!兑换 LIMITED', '甲')
        await self.say('!兑换 LIMITED', '乙')
        self.plugin, self.ctx = await self.start()
        self.assertEqual(await self.say('!兑换 VIP2026'), '你已经领过这个兑换码了。')
        self.assertEqual(await self.say('!兑换 LIMITED', '丙'), '这个兑换码已经被领完了。')
        self.assertEqual(len(self.host.rewards()), 4)

    async def test_uncertain_commit_warns_player_and_restarts(self):
        self.host.lose_commit = True
        with self.assertLogs('redeem-test', 'WARNING'), self.assertRaises(CommitUncertain):
            await self.plugin.on_event(self.ctx, self.event('!兑换 VIP2026'))
        self.assertIn('兑换结果暂时不确定', self.host.sent[-1])
        # The event was not acknowledged, so the Host redelivers it after restart.
        self.host.lose_commit = False
        self.plugin, self.ctx = await self.start()
        self.assertEqual(await self.say('!兑换 VIP2026'), '欢迎 VIP！')
        self.assertEqual(self.host.values['code:vip2026'], 1)

    async def test_unknown_code_and_usage(self):
        self.assertEqual(await self.say('!兑换 NOPE'), '兑换码不存在，请检查是否输错。')
        self.assertEqual(await self.say('!兑换'), '用法：!兑换 <兑换码>，例如 !兑换 ABC123')
        self.assertEqual(await self.say('!兑换 a b'), '用法：!兑换 <兑换码>，例如 !兑换 ABC123')
        self.assertEqual(self.host.values, {})

    async def test_other_chat_is_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!兑换 "没闭合'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])

    def test_rejects_ambiguous_settings(self):
        for codes in ([Code('ABC', ['say hi']), Code('abc', ['say hi'])],
                      [Code('有 空格', ['say hi'])], [Code('', ['say hi'])], [Code('X' * 33, ['say hi'])],
                      [Code('ABC', [' '])], [Code('ABC')],
                      [Code('ABC', ['say hi'], expires='2026-13-01')],
                      [Code('ABC', ['say hi'], expires='2026/12/31')]):
            with self.assertRaises(ValueError):
                Settings(codes=codes)
        Settings(codes=[Code('新年-快乐_2026', coins=1, expires='2026-12-31')])

    def test_static_schema_and_example_match_settings(self):
        self.assertEqual(json.loads((ROOT / 'config.schema.json').read_text()), schema_for_config(Settings))
        example = load_config(Settings, ROOT / 'config.example.json')
        self.assertEqual(example.codes[0].code, 'XINSHOU2026')


if __name__ == '__main__':
    unittest.main()
