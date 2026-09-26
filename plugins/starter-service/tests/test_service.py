"""Local handler checks using SDK routing/intents, without a Host or game."""
import asyncio
import json
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock

from neomega_runtime import Event, PluginContext, PlayerNotFound
from neomega_runtime.operations import Operations
from main import Settings, StarterService


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.worker = SimpleNamespace(peer=None, log=logging.getLogger('starter-test'))
        self.ctx = PluginContext(self.worker, None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(None, 'offline-session')
        self.ctx.operations.wait = AsyncMock(return_value={'state': 'succeeded'})
        self.ctx.resolve_player = AsyncMock(return_value={'name': '玩家', 'xuid': '42'})
        self.tx = Mock()
        self.tx.save = AsyncMock(return_value={'operation_ids': ['op1']})
        self.ctx.storage.transaction = AsyncMock(return_value=self.tx)
        self.ctx.every = Mock()
        self.plugin = StarterService()
        await self.plugin.on_start(self.ctx)

    def event(self, kind, **body):
        return Event(self.ctx, {'kind': kind, 'payload': body, 'delivery_token': 'delivery1'})

    def texts(self):
        commands = [call.args[0].payload()['arguments']['cmd'] for call in self.tx.action.call_args_list]
        return ''.join(json.loads(cmd[cmd.index('{'):])['rawtext'][0]['text'] for cmd in commands)

    async def test_welcome_and_help(self):
        await self.plugin.on_event(self.ctx, self.event('player.list_added', name='玩家', xuid='42'))
        self.assertEqual(self.texts(), '欢迎 玩家！输入 !help 查看帮助。')
        self.ctx.storage.transaction.assert_awaited_once()
        self.assertEqual(self.ctx.storage.transaction.call_args.kwargs['event']['delivery_token'], 'delivery1')
        self.tx.reset_mock()
        await self.plugin.on_event(self.ctx, self.event('chat.received', player='玩家', xuid='42', message='!help'))
        self.assertEqual(self.texts(), self.ctx.config.help_text)
        self.ctx.resolve_player.assert_awaited_with(name='玩家', xuid='42')

    async def test_unrelated_chat_and_departed_join_only_ack(self):
        for message in ('你好', '!other', '!help \"unfinished'):
            await self.plugin.on_event(self.ctx, self.event('chat.received', message=message))
        self.ctx.resolve_player.side_effect = PlayerNotFound()
        await self.plugin.on_event(self.ctx, self.event('player.list_added', name='玩家'))
        self.assertEqual(self.tx.save.await_count, 4)
        self.tx.action.assert_not_called()

    async def test_registered_timer_generates_announcement(self):
        self.ctx.every.assert_called_once()
        args, kwargs = self.ctx.every.call_args
        self.assertEqual(args[0], 300)
        self.assertEqual(kwargs, {'name': 'announcement'})
        await args[1]()
        self.assertEqual(self.texts(), self.ctx.config.announcement_text)
        intent = self.tx.action.call_args.args[0].payload()
        self.assertTrue(intent['arguments']['cmd'].startswith('tellraw @a '))

    async def test_unknown_is_not_retried(self):
        self.ctx.operations.wait.return_value = {'state': 'unknown'}
        with self.assertRaisesRegex(RuntimeError, 'op1'):
            await self.plugin.announce(self.ctx)
        self.tx.save.assert_awaited_once()
        self.ctx.operations.wait.assert_awaited_once()

    async def test_event_waits_for_inflight_announcement(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def save():
            entered.set()
            await release.wait()
            return {'operation_ids': []}
        self.tx.save.side_effect = save
        announcement = asyncio.create_task(self.plugin.announce(self.ctx))
        await entered.wait()
        event = asyncio.create_task(self.plugin.on_event(self.ctx,
                                self.event('chat.received', message='hello')))
        try:
            await asyncio.sleep(0)
            self.assertFalse(event.done())
            self.ctx.storage.transaction.assert_awaited_once()
        finally:
            release.set()
            await asyncio.gather(announcement, event)
        self.assertEqual(self.tx.save.await_count, 2)

    def test_invalid_settings(self):
        for value in (9, float('inf'), float('nan')):
            with self.assertRaises(ValueError):
                Settings(announcement_interval=value)
        for value in ('', ' ', '中' * 67):
            with self.assertRaises(ValueError):
                Settings(help_text=value)
