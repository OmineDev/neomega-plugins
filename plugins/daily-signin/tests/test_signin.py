"""Drive the plugin through the real SDK storage against an in-memory Host."""
import asyncio
import copy
from datetime import date
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext
from neomega_runtime.managed import IPCRejected, ManagedPeer
from neomega_runtime.operations import Operations
from neomega_runtime.services import ServiceWaitTimeout
from main import DailySignin, Milestone, Settings

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}


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

    def commands(self):
        return [action['arguments']['cmd'] for action in self.commits[-1]['actions']]

    def reply(self):
        texts = []
        for command in self.commands():
            if command.startswith('tellraw'):
                texts += [part['text'] for part in json.loads(command[command.index('{'):])['rawtext']]
        return ''.join(texts)


class SigninTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('signin-test')), None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.resolve_player = AsyncMock(return_value=dict(PLAYER))
        self.ctx.services.call = AsyncMock(side_effect=IPCRejected('permission_denied', 'r1', False))
        self.plugin = DailySignin()
        await self.plugin.on_start(self.ctx)
        self.day = date(2026, 10, 21)
        self.plugin.today = lambda ctx: self.day

    async def say(self, message):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'chat.received', 'payload': {'player': '玩家', 'xuid': '42', 'message': message},
            'subscription_id': 'chat', 'event_id': 'e1', 'delivery_token': 'd1'}))
        # Every event is acknowledged in exactly one commit.
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')

    async def test_first_signin_rewards_and_records_in_one_commit(self):
        await self.say('!签到')
        self.assertEqual(len(self.host.commits), 1)
        self.assertIn('give @a[name="玩家"] bread 3', self.host.commands())
        self.assertIn('签到成功，获得 3 个面包。', self.host.reply())
        self.assertIn('已连续签到 1 天，累计 1 天。', self.host.reply())
        self.assertEqual(self.host.values['player:u1'], {'day': '2026-10-21', 'streak': 1, 'total': 1})

    async def test_second_signin_same_day_gives_nothing(self):
        await self.say('!签到')
        await self.say('！签到')
        self.assertFalse(any(command.startswith('give') for command in self.host.commands()))
        self.assertIn('今天已经签到过了', self.host.reply())
        self.assertEqual(self.host.values['player:u1']['total'], 1)

    async def test_streak_continues_resets_and_pays_milestone(self):
        for offset in range(7):
            self.day = date(2026, 10, 1 + offset)
            await self.say('!qd')
        self.assertIn('give @a[name="玩家"] diamond 1', self.host.commands())
        self.assertIn('连续签到 7 天', self.host.reply())
        self.day = date(2026, 10, 9)
        await self.say('!qd')
        self.assertEqual(self.host.values['player:u1'], {'day': '2026-10-09', 'streak': 1, 'total': 8})
        self.assertNotIn('give @a[name="玩家"] diamond 1', self.host.commands())

    async def test_info_reports_broken_streak_as_zero(self):
        await self.say('!签到信息')
        self.assertIn('你还没有签到过', self.host.reply())
        await self.say('!签到')
        self.day = date(2026, 10, 25)
        await self.say('!签到信息')
        self.assertIn('今天还没签到，当前连续 0 天，累计 1 天。', self.host.reply())

    async def test_other_chat_is_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!签到 "没闭合'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_no_economy_means_no_coin_line(self):
        await self.say('!签到')
        self.assertNotIn('金币', self.host.reply())
        self.assertEqual(self.host.values['player:u1']['streak'], 1)

    async def test_grants_daily_and_milestone_coins_once_per_day(self):
        self.ctx.services.call = AsyncMock(return_value={'balance': 25, 'duplicate': False,
                                                         'currency': '积分', 'pending': False})
        self.ctx.config = Settings(milestones=[Milestone(2, coins=5)])
        self.day = date(2026, 10, 20)
        await self.say('!签到')
        self.day = date(2026, 10, 21)
        await self.say('!签到')
        self.assertIn('获得 15 积分，现有 25 积分。', self.host.reply())
        await self.say('!签到')
        self.assertEqual(self.ctx.services.call.await_count, 2)
        name, arguments = self.ctx.services.call.await_args.args
        self.assertEqual(name, 'plugin.neomega.economy.grant')
        self.assertEqual(arguments, {'player_id': 'u1', 'amount': 15, 'reason': '签到奖励',
                                     'request_id': 'signin-2026-10-21'})

    async def test_unsettled_grant_still_records_signin(self):
        self.ctx.services.call = AsyncMock(side_effect=ServiceWaitTimeout('c1'))
        await self.say('!签到')
        self.assertIn('金币奖励结果暂时不确定', self.host.reply())
        self.assertEqual(self.host.values['player:u1']['total'], 1)

    async def test_empty_reward_list_only_records(self):
        self.ctx.config = Settings(commands=[], milestones=[])
        await self.say('!签到')
        self.assertFalse(any(command.startswith('give') for command in self.host.commands()))
        self.assertEqual(self.host.values['player:u1']['streak'], 1)

    def test_rejects_ambiguous_settings(self):
        with self.assertRaises(ValueError):
            Settings(commands=[' '])
        with self.assertRaises(ValueError):
            Settings(milestones=[Milestone(7, [' '])])
        with self.assertRaises(ValueError):
            Settings(milestones=[Milestone(7), Milestone(7)])


class ServeWriter:
    def __init__(self):
        self.requests = asyncio.Queue()
        self.responses = asyncio.Queue()
        self.closed = False

    def write(self, data):
        for line in data.splitlines():
            if not line:
                continue
            frame = json.loads(line)
            if frame.get('type') == 'request':
                self.requests.put_nowait(frame)
            else:
                self.responses.put_nowait(frame)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


class SigninServeTests(unittest.IsolatedAsyncioTestCase):
    async def test_serve_single_ack_and_no_invalid_delivery(self):
        host = FakeHost()
        deliveries = set()
        reader = asyncio.StreamReader(limit=1 << 20)
        writer = ServeWriter()
        peer = ManagedPeer(reader, writer)
        plugin = DailySignin()
        plugin.today = lambda ctx: date(2026, 10, 21)

        def send(frame):
            reader.feed_data((json.dumps(frame) + '\n').encode())

        async def wait_response(req_id):
            while True:
                msg = await asyncio.wait_for(writer.responses.get(), 2)
                if msg.get('id') == req_id:
                    return msg

        async def pump():
            while True:
                frame = await writer.requests.get()
                req_id, method, payload = frame['id'], frame['method'], frame['payload']
                if method == 'hello':
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {
                        'protocol_major': 1, 'protocol_minor': 0, 'installation_id': 'inst-1', 'generation': 1,
                        'capabilities': [], 'limits': {
                            'max_frame_bytes': 1 << 20, 'outgoing_requests': 128,
                            'inbound_requests': 32, 'queue_frames': 32}}}})
                elif method == 'capabilities.get':
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {'session': 's1'}}})
                elif method == 'players.list':
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {'session': 's1', 'players': [dict(PLAYER)]}}})
                elif method == 'services.online.submit':
                    send({'type': 'response', 'id': req_id, 'payload': {'error': {
                        'code': 'permission_denied', 'message': 'no economy', 'retryable': False, 'request_id': req_id}}})
                elif method == 'events.commit':
                    key = (payload['subscription_id'], payload['event_id'], payload['delivery_token'])
                    if key in deliveries:
                        send({'type': 'response', 'id': req_id, 'payload': {'error': {
                            'code': 'invalid_delivery', 'message': 'already acked', 'retryable': False, 'request_id': req_id}}})
                    else:
                        deliveries.add(key)
                        res = await host.call(method, payload)
                        send({'type': 'response', 'id': req_id, 'payload': {'result': res}})
                elif method in ('state.get', 'state.commit'):
                    res = await host.call(method, payload)
                    send({'type': 'response', 'id': req_id, 'payload': {'result': res}})

        with tempfile.TemporaryDirectory() as tmp:
            serve_task = asyncio.create_task(plugin.serve(peer, data_dir=Path(tmp)))
            pump_task = asyncio.create_task(pump())
            try:
                send({'type': 'request', 'id': 'v1', 'method': 'lifecycle.validate',
                      'payload': {'installation_id': 'inst-1', 'generation': 1, 'revision': 1}})
                self.assertIn('result', (await wait_response('v1'))['payload'])
                send({'type': 'request', 'id': 'a1', 'method': 'lifecycle.activate',
                      'payload': {'installation_id': 'inst-1', 'generation': 1, 'revision': 1, 'state_version': 1}})
                self.assertTrue((await wait_response('a1'))['payload']['result']['activated'])
                send({'type': 'notification', 'method': 'event', 'payload': {
                    'kind': 'chat.received', 'subscription_id': 'chat', 'event_id': 'e1', 'delivery_token': 'd1',
                    'payload': {'player': '玩家', 'xuid': '42', 'message': '!签到'}}})
                send({'type': 'notification', 'method': 'event', 'payload': {
                    'kind': 'chat.received', 'subscription_id': 'chat', 'event_id': 'e2', 'delivery_token': 'd2',
                    'payload': {'player': '玩家', 'xuid': '42', 'message': '普通聊天'}}})
                send({'type': 'notification', 'method': 'event', 'payload': {
                    'kind': 'chat.received', 'subscription_id': 'chat', 'event_id': 'e3', 'delivery_token': 'd3',
                    'payload': {'player': '玩家', 'xuid': '42', 'message': '!签到信息'}}})
                send({'type': 'request', 'id': 'dr1', 'method': 'lifecycle.drain',
                      'payload': {'installation_id': 'inst-1', 'generation': 1}})
                self.assertTrue((await wait_response('dr1'))['payload']['result']['drained'])
                self.assertEqual(len(host.commits), 3)
                self.assertEqual(deliveries, {('chat', 'e1', 'd1'), ('chat', 'e2', 'd2'), ('chat', 'e3', 'd3')})
                self.assertEqual(host.values['player:u1']['streak'], 1)
                self.assertFalse(serve_task.done())
            finally:
                serve_task.cancel()
                pump_task.cancel()
                await asyncio.gather(serve_task, pump_task, return_exceptions=True)


if __name__ == '__main__':
    unittest.main()
