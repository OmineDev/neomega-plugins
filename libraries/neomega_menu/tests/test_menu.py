"""Menus against the real SDK ChatWaiters, Notifications and Storage with an in-memory Host."""
import asyncio
import json
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext
from neomega_runtime.operations import Operations
import neomega_menu as menu

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}


class FakeHost:
    def __init__(self):
        self.revision, self.commits, self.sent = 0, [], []

    async def call(self, method, payload, timeout=None):
        if method == 'state.get':
            return {'revision': self.revision, 'values': {}, 'session': 's1'}
        if method == 'operations.submit':
            command = payload['arguments']['cmd']
            self.sent.append(''.join(p['text'] for p in json.loads(command[command.index('{'):])['rawtext']))
            return {'operation_id': 'op' + str(len(self.sent)), 'state': 'accepted', 'duplicate': False}
        assert method == 'events.commit' and payload['expected_state_revision'] == self.revision
        self.revision += 1
        self.commits.append(payload)
        return {'commit_id': payload['commit_id'], 'revision': self.revision, 'duplicate': False, 'operation_ids': []}


class MenuTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('menu-test')), None)
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.resolve_player = AsyncMock(return_value=dict(PLAYER))

    async def reply(self, message, player='玩家'):
        await asyncio.sleep(0)
        return await menu.feed(self.ctx, Event(self.ctx, {
            'kind': 'chat.received', 'payload': {'player': player, 'xuid': '42', 'message': message},
            'subscription_id': 'chat', 'event_id': 'e', 'delivery_token': 'd'}))

    async def test_choose_returns_zero_based_index_and_acknowledges_reply(self):
        flow = asyncio.create_task(menu.choose(self.ctx, PLAYER, '商店', ['面包', '钻石']))
        self.assertTrue(await self.reply('2'))
        self.assertEqual(await flow, 1)
        self.assertIn('1. 面包', self.host.sent[0])
        self.assertIn('2. 钻石', self.host.sent[0])
        self.assertEqual(len(self.host.commits), 1)

    async def test_invalid_input_reprompts_then_gives_up(self):
        flow = asyncio.create_task(menu.choose(self.ctx, PLAYER, '商店', ['面包']))
        for text in ('9', 'abc', '-1'):
            self.assertTrue(await self.reply(text))
        self.assertIsNone(await flow)
        self.assertIn('没有这个选项', self.host.sent[1])
        self.assertIn('输入错误次数过多', self.host.sent[-1])

    async def test_cancel_and_confirm(self):
        flow = asyncio.create_task(menu.choose(self.ctx, PLAYER, '商店', ['面包']))
        await self.reply('取消')
        self.assertIsNone(await flow)
        flow = asyncio.create_task(menu.confirm(self.ctx, PLAYER, '确定购买？'))
        await self.reply('1')
        self.assertTrue(await flow)
        flow = asyncio.create_task(menu.confirm(self.ctx, PLAYER, '确定购买？'))
        await self.reply('不要')
        self.assertFalse(await flow)
        self.assertEqual('已取消。', self.host.sent[-1])

    async def test_timeout_closes_menu(self):
        self.assertIsNone(await menu.ask(self.ctx, PLAYER, '名字？', timeout=0.01))
        self.assertIn('菜单已关闭', self.host.sent[-1])

    async def test_unrelated_chat_is_not_consumed(self):
        self.assertFalse(await self.reply('你好'))
        flow = asyncio.create_task(menu.ask(self.ctx, PLAYER, '名字？'))
        self.assertFalse(await self.reply('你好', player='别人'))
        self.assertTrue(await self.reply('小明'))
        self.assertEqual(await flow, '小明')

    async def test_second_menu_for_same_player_is_refused(self):
        first = asyncio.create_task(menu.ask(self.ctx, PLAYER, '第一个？'))
        await asyncio.sleep(0)
        self.assertIsNone(await menu.ask(self.ctx, PLAYER, '第二个？'))
        self.assertIn('还有一个菜单没有完成', self.host.sent[-1])
        await self.reply('好')
        self.assertEqual(await first, '好')


if __name__ == '__main__':
    unittest.main()
