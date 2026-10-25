"""Drive the menu through the real SDK storage and neomega_menu against an in-memory Host.

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
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceUncertain, ServiceWaitTimeout
from main import HelpPlugins, Item, Menu, Settings, item_commands

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}
PAY, BALANCE = 'plugin.neomega.economy.pay', 'plugin.neomega.economy.balance'
TARGET = '@a[name="玩家"]'


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
            if mutation.get('delete'):
                self.values.pop(mutation['key'], None)
            else:
                self.values[mutation['key']] = mutation['value']
        self.revision += 1
        self.commits.append(payload)
        return {'commit_id': payload['commit_id'], 'revision': self.revision, 'duplicate': False,
                'operation_ids': [f'op{i}' for i in range(len(payload['actions']))]}

    def game_commands(self):
        """Every committed non-reply command, in order."""
        return [a['arguments']['cmd'] for c in self.commits for a in c['actions']
                if not a['arguments']['cmd'].startswith('tellraw')]

    def replies(self):
        """Replies sent directly by menus and committed with actions, in order of appearance."""
        committed = [text_of(a['arguments']['cmd']) for c in self.commits for a in c['actions']
                     if a['arguments']['cmd'].startswith('tellraw')]
        return self.sent + committed

    def last_reply(self):
        commands = [a['arguments']['cmd'] for a in self.commits[-1]['actions']]
        return ''.join(text_of(c) for c in commands if c.startswith('tellraw'))


ITEMS = [
    Item('回主城', ['tp {player} 0 100 0'], trigger='主城', cooldown_seconds=30, group='传送'),
    Item('回出生点', ['/tp @s 0 64 0'], as_player=True, group='传送'),
    Item('领取面包', ['give {player} bread 8'], trigger='面包', cost=5, group='福利'),
    Item('设为白天', ['time set day'], trigger='白天', admin_only=True, group='管理'),
    Item('自杀', ['kill {player}']),
]


class MenuTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host, self.tasks = FakeHost(), []
        worker = SimpleNamespace(peer=self.host, log=logging.getLogger('menu-plugin-test'),
                                 spawn=lambda coroutine, name=None: self.track(coroutine))
        self.ctx = PluginContext(worker, None)
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.resolve_player = AsyncMock(return_value=dict(PLAYER))
        self.outcomes, self.calls = {PAY: [], BALANCE: []}, []
        self.ctx.services.call = self.call
        self.clock = 1000.0
        await self.start(Settings(items=ITEMS))

    async def start(self, settings):
        self.ctx.config = settings
        self.ctx.chat = type(self.ctx.chat)()
        self.plugin = Menu()
        self.plugin.now = lambda: self.clock
        await self.plugin.on_start(self.ctx)

    def track(self, coroutine):
        task = asyncio.ensure_future(coroutine)
        self.tasks.append(task)
        return task

    async def call(self, service, arguments, *, major=1, timeout=5, idempotency_key=None):
        self.assertEqual((major, timeout), (1, 5))
        self.calls.append((service, dict(arguments)))
        outcome = self.outcomes[service].pop(0)
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

    async def chat(self, *messages):
        for message in messages:
            await self.say(message)
        await asyncio.gather(*self.tasks)
        self.tasks.clear()

    def pays(self):
        return [arguments for service, arguments in self.calls if service == PAY]

    # ---- 菜单展示、分组与翻页 ----

    async def test_groups_come_first_then_items_of_the_chosen_group(self):
        await self.chat('!菜单', '1', '2')
        top = self.host.sent[0]
        self.assertIn('服务器菜单', top)
        for line in ('1. [传送]', '2. [福利]', '3. 自杀'):
            self.assertIn(line, top)
        self.assertNotIn('管理', top)  # 非管理员看不到只含管理员项的分组
        self.assertIn('服务器菜单 - 传送', self.host.sent[1])
        self.assertIn('1. 回主城（!主城）', self.host.sent[1])
        self.assertEqual(self.host.game_commands(), [f'execute as {TARGET} at @s run tp @s 0 64 0'])
        self.assertEqual(self.host.last_reply(), '已执行「回出生点」。')
        self.assertNotIn('delivery_token', self.host.commits[-1])

    async def test_menu_argument_opens_a_group_directly(self):
        await self.chat('!菜单 福利', '0')
        self.assertIn('服务器菜单 - 福利', self.host.sent[0])
        self.assertIn('1. 领取面包 — 5 金币（!面包）', self.host.sent[0])
        self.assertEqual(self.host.sent[-1], '已取消。')
        await self.chat('!cd 不存在')
        self.assertEqual(self.host.sent[-1], '没有「不存在」这个分组。')

    async def test_many_items_are_paged_and_numbers_work_on_any_page(self):
        items = [Item(f'项目{n}', [f'say {n}']) for n in range(1, 21)]
        await self.start(Settings(items=items, page_size=8))
        await self.chat('!菜单', '+', '下一页', '+', '-', '12')
        pages = self.host.sent[:5]
        self.assertIn('服务器菜单（第 1/3 页）', pages[0])
        self.assertIn('8. 项目8', pages[0])
        self.assertNotIn('9. 项目9', pages[0])
        self.assertIn('+ 下一页，- 上一页', pages[0])
        self.assertIn('第 2/3 页', pages[1])
        self.assertIn('9. 项目9', pages[1])
        self.assertIn('第 3/3 页', pages[2])
        self.assertIn('20. 项目20', pages[2])
        self.assertIn('第 1/3 页', pages[3])  # 最后一页再翻回到第一页
        self.assertIn('第 3/3 页', pages[4])
        self.assertEqual(self.host.game_commands(), ['say 12'])

    async def test_invalid_numbers_reprompt_then_close(self):
        await self.chat('!菜单', '9', 'abc', '99')
        self.assertIn('没有这个选项，请输入 1-3 之间的编号。', self.host.sent[1])
        self.assertEqual(self.host.sent[-1], '输入错误次数过多，菜单已关闭。')
        self.assertEqual(self.host.game_commands(), [])

    async def test_single_page_without_groups(self):
        await self.start(Settings(items=[Item('自杀', ['kill {player}'])]))
        await self.chat('！menu', '1')
        self.assertIn('输入编号 1-1 选择，输入 0 取消。', self.host.sent[0])
        self.assertEqual(self.host.game_commands(), [f'kill {TARGET}'])

    # ---- 快捷指令与占位替换 ----

    async def test_free_shortcut_runs_in_the_event_commit(self):
        await self.chat('!主城')
        [commit] = self.host.commits
        self.assertEqual(commit['delivery_token'], 'd1')
        self.assertEqual(self.host.game_commands(), [f'tp {TARGET} 0 100 0'])
        self.assertEqual(self.host.last_reply(), '已执行「回主城」。')
        self.assertEqual(self.host.values, {'cooldown:u1': {'回主城': 1030}})
        self.assertEqual(self.tasks, [])

    async def test_full_width_prefix_and_custom_message(self):
        items = [Item('签名', ['say hi'], trigger='!签名', message='好的')]
        await self.start(Settings(items=items))
        await self.chat('！签名')
        self.assertEqual(self.host.game_commands(), ['say hi'])
        self.assertEqual(self.host.last_reply(), '好的')
        self.assertEqual(self.host.values, {})

    def test_placeholder_slash_and_execute_as(self):
        item = Item('x', ['/give {player} apple', 'tp {player} ~ ~1 ~'], as_player=True)
        self.assertEqual(item_commands(item, TARGET), [
            f'execute as {TARGET} at @s run give {TARGET} apple',
            f'execute as {TARGET} at @s run tp {TARGET} ~ ~1 ~'])
        self.assertEqual(item_commands(Item('y', [' say {player} ']), TARGET), [f'say {TARGET}'])

    # ---- 冷却 ----

    async def test_cooldown_blocks_until_it_expires(self):
        await self.chat('!主城')
        self.clock += 10
        await self.chat('!主城')
        self.assertEqual(self.host.last_reply(), '「回主城」还要等 20 秒才能再用。')
        self.assertEqual(len(self.host.game_commands()), 1)
        await self.chat('!菜单', '1', '1')  # 菜单里选同一项也受冷却限制
        self.assertEqual(self.host.sent[-1], '「回主城」还要等 20 秒才能再用。')
        self.clock += 21
        await self.chat('!主城')
        self.assertEqual(len(self.host.game_commands()), 2)
        self.assertEqual(self.host.values['cooldown:u1'], {'回主城': 1061})

    async def test_expired_cooldowns_are_pruned(self):
        await self.chat('!主城')
        self.clock += 100
        await self.chat('!菜单', '3')  # 没有冷却的项目，顺便清掉过期记录
        self.assertNotIn('cooldown:u1', self.host.values)

    # ---- 仅管理员 ----

    async def test_admin_only_items_are_hidden_and_refused_for_players(self):
        await self.chat('!白天')
        self.assertEqual(self.host.last_reply(), '「设为白天」只有管理员可以使用。')
        self.assertEqual(self.host.game_commands(), [])
        await self.start(Settings(items=ITEMS, admins=['玩家']))
        await self.chat('!菜单', '3', '1')
        self.assertIn('3. [管理]', self.host.sent[0])
        self.assertEqual(self.host.game_commands(), ['time set day'])
        await self.chat('!白天')
        self.assertEqual(self.host.game_commands()[-1], 'time set day')

    async def test_no_visible_items(self):
        await self.start(Settings(items=[Item('设为白天', ['time set day'], admin_only=True)]))
        await self.chat('!菜单')
        self.assertEqual(self.host.sent, ['菜单里还没有你能用的项目。'])

    # ---- 金币 ----

    async def test_paid_item_confirms_charges_then_executes(self):
        self.outcomes[BALANCE] = [{'balance': 50, 'currency': '积分', 'pending': False}]
        self.outcomes[PAY] = [{'balance': 45, 'duplicate': False, 'currency': '积分', 'pending': False}]
        await self.chat('!面包', '1')
        self.assertEqual(self.host.sent[0], '「领取面包」需要 5 积分（你有 50 积分），确认吗？\n输入 1 确认，输入 0 取消。')
        [pay] = self.pays()
        self.assertEqual({k: pay[k] for k in ('player_id', 'amount', 'reason')},
                         {'player_id': 'u1', 'amount': 5, 'reason': '领取面包'})
        self.assertRegex(pay['request_id'], r'^[0-9a-f]{32}$')
        self.assertEqual(self.host.game_commands(), [f'give {TARGET} bread 8'])
        self.assertEqual(self.host.last_reply(), '已执行「领取面包」，花费 5 积分，剩余 45 积分。')

    async def test_insufficient_funds_executes_nothing(self):
        self.outcomes[BALANCE] = [{'balance': 3, 'currency': '金币', 'pending': False},
                                  {'balance': 3, 'currency': '金币', 'pending': False}]
        self.outcomes[PAY] = [ServiceRejected('insufficient_funds')]
        await self.chat('!菜单', '2', '1', '确认')
        self.assertEqual(self.host.sent[-1], '金币不足，需要 5 金币，你现在有 3 金币。')
        self.assertEqual(self.host.game_commands(), [])

    async def test_economy_not_installed(self):
        self.outcomes[BALANCE] = [IPCRejected('permission_denied', 'r1', False)]
        await self.chat('!面包')
        self.assertEqual(self.host.sent, ['本服没有启用经济插件，「领取面包」暂时不能使用。'])
        self.assertEqual(self.pays(), [])
        await self.chat('!主城')  # 免费项目不受影响
        self.assertEqual(self.host.game_commands(), [f'tp {TARGET} 0 100 0'])

    async def test_cancelled_confirmation_charges_nothing(self):
        self.outcomes[BALANCE] = [ServiceWaitTimeout('busy')]
        await self.chat('!面包', '不要')
        self.assertIn('「领取面包」需要 5 金币，确认吗？', self.host.sent[0])
        self.assertEqual(self.pays(), [])
        self.assertEqual(self.host.game_commands(), [])

    async def test_unsettled_charge_retries_with_the_same_request_id(self):
        self.outcomes[BALANCE] = [{'balance': 50, 'currency': '金币', 'pending': False}] * 2
        self.outcomes[PAY] = [ServiceWaitTimeout('c1'), {'balance': 45, 'duplicate': True}]
        await self.chat('!面包', '1')
        first, second = self.pays()
        self.assertEqual(first, second)
        self.assertEqual(self.host.game_commands(), [f'give {TARGET} bread 8'])
        self.calls.clear()
        self.outcomes[PAY] = [ServiceUncertain(None), ServiceRejected('unavailable')]
        await self.chat('!面包', '1')
        self.assertEqual(self.host.sent[-1], '扣款结果暂时不确定，请用 !流水 查看；没有扣款可以重新使用。')
        self.assertEqual(len(self.host.game_commands()), 1)

    async def test_lost_commit_after_payment_tells_player_to_contact_admin(self):
        self.outcomes[BALANCE] = [{'balance': 50, 'currency': '金币', 'pending': False}]
        self.outcomes[PAY] = [{'balance': 45, 'duplicate': False}]
        self.host.lose_state_commit = True
        with self.assertLogs('menu-plugin-test', 'WARNING'):
            await self.chat('!面包', '1')
        self.assertEqual(self.host.sent[-1], '执行结果不确定，请联系管理员。')
        self.assertEqual(len(self.pays()), 1)

    # ---- 帮助 ----

    async def test_command_list_shows_menu_shortcuts_and_ticked_plugins(self):
        await self.start(Settings(items=ITEMS, help_plugins=HelpPlugins(signin=True, tpa=True),
                                  help_extra=['有问题找腐竹']))
        await self.chat('!指令')
        text = self.host.last_reply()
        self.assertEqual(text.splitlines()[:4], ['本服可用命令：', '!菜单 — 打开服务器菜单',
                                                 '!主城 — 回主城', '!面包 — 领取面包'])
        self.assertNotIn('白天', text)
        self.assertIn('!签到 — 领取每日签到奖励', text)
        self.assertIn('!tpa 玩家', text)
        self.assertNotIn('!商店', text)
        self.assertTrue(text.endswith('有问题找腐竹'))

    async def test_help_is_only_taken_over_when_enabled(self):
        await self.chat('!帮助')
        self.assertEqual(self.host.commits[-1]['actions'], [])
        await self.start(Settings(items=ITEMS, help_takeover=True, admins=['玩家']))
        for message in ('!帮助', '！help', '!zl'):
            await self.chat(message)
            self.assertIn('!白天 — 设为白天', self.host.last_reply())

    async def test_other_chat_is_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!菜单 a b', '!主城 "没闭合'):
            await self.chat(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])
        self.assertEqual(self.host.sent, [])

    # ---- 配置校验 ----

    def test_rejects_unusable_settings(self):
        bad = [
            dict(items=[]),
            dict(menu_commands=[]),
            dict(items=[Item('a', ['say a']), Item('a', ['say b'])]),
            dict(items=[Item('a', [])]),
            dict(items=[Item('a', [' '])]),
            dict(items=[Item(' ', ['say a'])]),
            dict(items=[Item('a', ['say a'], trigger='x'), Item('b', ['say b'], trigger='!x')]),
            dict(items=[Item('a', ['say a'], trigger='菜单')]),
            dict(items=[Item('a', ['say a'], trigger='指令')]),
            dict(items=[Item('a', ['say a'], trigger='帮助')], help_takeover=True),
            dict(items=[Item('a', ['say a'], trigger='回 家')]),
            dict(menu_commands=['菜单', '！菜单']),
            dict(admins=['']),
            dict(help_extra=['']),
        ]
        for options in bad:
            with self.subTest(options=options), self.assertRaises(ValueError):
                Settings(**options)
        Settings(items=[Item('a', ['say a'], trigger='帮助')])  # 不接管时 !帮助 可以给菜单项用


if __name__ == '__main__':
    unittest.main()
