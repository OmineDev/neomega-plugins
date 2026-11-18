"""Drive the plugin through the real SDK storage and operations against an in-memory Host."""
import asyncio
import copy
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from neomega_runtime import Event, PlayerNotFound, PluginContext, find_player
from neomega_runtime.managed import IPCRejected, ManagedPeer
from neomega_runtime.operations import Operations
from main import Settings, Welcome

PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}


class FakeHost:
    """state.get / state.commit / events.commit / operations.get with the Host's global revision CAS."""
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.fail = None
        self.ops = {}
        self.op_counter = 0
        self.scripted_outcomes = []  # optional queue of (predicate_or_None, outcome_dict)
        self.executed_cmds = []

    def enqueue_no_target_match(self, count=1):
        for _ in range(count):
            self.scripted_outcomes.append({
                'state': 'failed',
                'evidence': 'acknowledged',
                'reason': 'command_unsuccessful',
                'result': {
                    'success_count': 0,
                    'output_type': 0,
                    'data_set': 0,
                    'messages': [{'success': False, 'message': 'commands.generic.noTargetMatch', 'parameters': []}],
                },
            })

    def _run_action(self, action):
        self.op_counter += 1
        op_id = f'op{self.op_counter}'
        cmd = action['arguments'].get('cmd', '')
        self.executed_cmds.append(cmd)
        if self.scripted_outcomes:
            outcome = self.scripted_outcomes.pop(0)
        else:
            outcome = {
                'state': 'succeeded',
                'evidence': 'acknowledged',
                'result': {
                    'success_count': 1,
                    'output_type': 0,
                    'data_set': 0,
                    'messages': [],
                },
            }
        self.ops[op_id] = {
            'request_id': action['idempotency_key'],
            'operation_id': op_id,
            'state': outcome['state'],
            'expires_at': '2026-11-01T00:00:00Z',
            'result': outcome,
            'can_retry': False,
        }
        return op_id

    async def call(self, method, payload, timeout=None):
        if method == 'state.get':
            keys = payload.get('keys')
            values = self.values if keys is None else {k: self.values[k] for k in keys if k in self.values}
            return {'revision': self.revision, 'values': copy.deepcopy(values), 'session': 's1'}
        if method == 'operations.get':
            return copy.deepcopy(self.ops[payload['operation_id']])
        assert method in ('state.commit', 'events.commit'), method
        if self.fail is not None:
            raise self.fail
        assert payload['expected_state_revision'] == self.revision
        for mutation in payload['mutations']:
            if mutation.get('delete'):
                self.values.pop(mutation['key'], None)
            else:
                self.values[mutation['key']] = mutation['value']
        self.revision += 1
        self.commits.append(payload)
        op_ids = [self._run_action(action) for action in payload['actions']]
        return {'commit_id': payload['commit_id'], 'revision': self.revision, 'duplicate': False,
                'operation_ids': op_ids}

    def commands(self, commit=-1):
        return [action['arguments']['cmd'] for action in self.commits[commit]['actions']]

    def reply(self, commit=-1):
        texts = []
        for command in self.commands(commit):
            if command.startswith('tellraw'):
                texts += [part['text'] for part in json.loads(command[command.index('{'):])['rawtext']]
        return ''.join(texts)


class WelcomeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.tasks = []
        self.online = [dict(PLAYER)]
        worker = SimpleNamespace(
            peer=self.host,
            log=logging.getLogger('welcome-test'),
            spawn=lambda coro, name=None: self._track(asyncio.create_task(coro, name=name)),
        )
        self.ctx = PluginContext(worker, None)
        self.ctx.config = Settings()
        self.ctx._operations = Operations(self.host, 's1')
        self.ctx.players = AsyncMock(side_effect=lambda: [dict(p) for p in self.online])
        self.ctx.resolve_player = AsyncMock(side_effect=lambda **kw: find_player(self.online, **kw))
        self.ctx.every = Mock()
        self.plugin = Welcome()
        await self.plugin.on_start(self.ctx)

    async def asyncTearDown(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

    def _track(self, task):
        self.tasks.append(task)
        return task

    async def drain_tasks(self):
        while any(not t.done() for t in self.tasks):
            pending = [t for t in self.tasks if not t.done()]
            await asyncio.gather(*pending)

    async def deliver(self, kind, payload, event_id='e1', token='d1'):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': kind, 'payload': payload,
            'subscription_id': 'sub', 'event_id': event_id, 'delivery_token': token}))
        commit = next(c for c in reversed(self.host.commits) if c.get('event_id') == event_id)
        self.assertEqual(commit['delivery_token'], token)

    async def join(self, payload=None, *, drain=True, event_id='e1', token='d1'):
        await self.deliver('player.list_added', payload or {'uuid': 'u1', 'name': '玩家', 'xuid': '42'},
                           event_id=event_id, token=token)
        if drain:
            await self.drain_tasks()

    async def say(self, message):
        await self.deliver('chat.received', {'player': '玩家', 'xuid': '42', 'message': message})

    async def test_first_join_sends_gift_and_records_seen_after_delivery(self):
        await self.join(drain=False)
        # Immediately after event commit: gift admitted, pending saved, seen NOT yet marked
        self.assertIn('pending:u1', self.host.values)
        self.assertNotIn('seen:u1', self.host.values)
        self.assertIn('give @a[name="玩家"] bread 16', self.host.commands(0))
        self.assertEqual(self.host.reply(0), '欢迎 玩家 第一次来到服务器！送你一份新手礼包。')

        await self.drain_tasks()
        self.assertEqual(self.host.values, {'seen:u1': True})
        self.assertEqual([c for c in self.host.executed_cmds if c.startswith('give')], ['give @a[name="玩家"] bread 16'])

    async def test_returning_player_only_gets_welcome(self):
        await self.join()
        await self.join(event_id='e2', token='d2')
        self.assertFalse(any(command.startswith('give') for command in self.host.commands()))
        self.assertEqual(self.host.reply(), '欢迎回来，玩家！输入 !帮助 查看可用命令。')
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_empty_first_join_text_falls_back_to_welcome(self):
        self.ctx.config = Settings(first_join_text='', first_join_commands=[])
        await self.join()
        self.assertEqual(self.host.reply(0), '欢迎回来，玩家！输入 !帮助 查看可用命令。')
        self.assertFalse(any(command.startswith('give') for command in self.host.commands(0)))
        self.assertEqual(self.host.values, {'seen:u1': True})

    async def test_notargetmatch_retries_while_online_and_marks_seen_once(self):
        # Attempt 1: give + tellraw both fail with noTargetMatch
        self.host.enqueue_no_target_match(2)
        with patch('main.asyncio.sleep', new=AsyncMock()) as sleep_mock:
            await self.join()
        sleep_mock.assert_awaited_once()
        self.assertEqual(self.host.values, {'seen:u1': True})
        self.assertEqual(self.host.executed_cmds.count('give @a[name="玩家"] bread 16'), 2)

    async def test_partial_gift_delivery_only_retries_failed_command(self):
        self.ctx.config = Settings(first_join_commands=['give {player} bread 8', 'give {player} apple 4'])
        # Attempt 1: bread succeeds, apple gets noTargetMatch, tellraw succeeds
        self.host.scripted_outcomes = [
            {'state': 'succeeded', 'evidence': 'acknowledged',
             'result': {'success_count': 1, 'output_type': 0, 'data_set': 0, 'messages': []}},
            {'state': 'failed', 'evidence': 'acknowledged', 'reason': 'command_unsuccessful',
             'result': {'success_count': 0, 'output_type': 0, 'data_set': 0,
                        'messages': [{'success': False, 'message': 'commands.generic.noTargetMatch', 'parameters': []}]}},
            {'state': 'succeeded', 'evidence': 'acknowledged',
             'result': {'success_count': 1, 'output_type': 0, 'data_set': 0, 'messages': []}},
        ]
        with patch('main.asyncio.sleep', new=AsyncMock()):
            await self.join()
        self.assertEqual(self.host.values, {'seen:u1': True})
        self.assertEqual(self.host.executed_cmds.count('give @a[name="玩家"] bread 8'), 1)
        self.assertEqual(self.host.executed_cmds.count('give @a[name="玩家"] apple 4'), 2)

    async def test_duplicate_join_while_verifying_does_not_duplicate_gift(self):
        self.host.enqueue_no_target_match(2)
        gate = asyncio.Event()
        orig_sleep = asyncio.sleep

        async def paused_sleep(_):
            gate.set()
            await orig_sleep(0.02)

        with patch('main.asyncio.sleep', side_effect=paused_sleep):
            await self.join(drain=False, event_id='e1', token='d1')
            await gate.wait()
            # Host rebuilds player list while retry task is in flight
            await self.join(drain=False, event_id='e2', token='d2')
            await self.drain_tasks()
        self.assertEqual(self.host.values, {'seen:u1': True})
        self.assertEqual(self.host.executed_cmds.count('give @a[name="玩家"] bread 16'), 2)

    async def test_player_leaves_during_retry_preserves_pending_until_rejoin(self):
        self.host.enqueue_no_target_match(2)

        async def leave_during_sleep(_):
            self.online.clear()

        with patch('main.asyncio.sleep', side_effect=leave_during_sleep):
            await self.join()
        self.assertNotIn('seen:u1', self.host.values)
        self.assertIn('pending:u1', self.host.values)

        # Player rejoins later and receives their gift cleanly
        self.online.append(dict(PLAYER))
        await self.join(event_id='e2', token='d2')
        self.assertEqual(self.host.values, {'seen:u1': True})

    async def test_bounded_retry_exhaustion_leaves_seen_unset(self):
        self.host.enqueue_no_target_match(10)
        with patch('main.asyncio.sleep', new=AsyncMock()) as sleep_mock, \
             self.assertLogs('welcome-test', 'WARNING'):
            await self.join()
        self.assertEqual(sleep_mock.await_count, 4)
        self.assertNotIn('seen:u1', self.host.values)
        self.assertIn('pending:u1', self.host.values)

    async def test_empty_xuid_and_missing_name_handled_cleanly(self):
        self.online = [{'name': '玩家', 'uuid': 'u1', 'xuid': ''}]
        await self.join({'name': '玩家', 'uuid': 'u1', 'xuid': ''})
        self.assertEqual(self.host.values, {'seen:u1': True})

        await self.join({'uuid': '', 'xuid': ''}, event_id='e2', token='d2')
        self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_player_already_left_is_only_acknowledged(self):
        self.online.clear()
        await self.join()
        self.assertEqual(self.host.commits[-1]['actions'], [])
        self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_help_command(self):
        for message in ('!帮助', '！help', '!bz'):
            await self.say(message)
            self.assertEqual(self.host.reply(), '可用命令：!签到、!回家、!tpa 玩家名。有问题请联系管理员。')

    async def test_other_chat_is_only_acknowledged(self):
        for message in ('你好', '!其他命令', '!帮助 "没闭合'):
            await self.say(message)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])

    async def test_announcements_rotate_in_order(self):
        self.ctx.every.assert_called_once()
        self.assertEqual(self.ctx.every.call_args.args[0], 600)
        self.ctx.config = Settings(announcements=['第一条', '第二条'])
        sent = []
        for _ in range(3):
            await self.plugin.announce(self.ctx)
            self.assertEqual(len(self.host.commands()), 1)
            self.assertTrue(self.host.commands()[0].startswith('tellraw @a '))
            sent.append(self.host.reply())
        self.assertEqual(sent, ['第一条', '第二条', '第一条'])
        self.assertNotIn('delivery_token', self.host.commits[-1])

    async def test_no_timer_without_announcements(self):
        ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('welcome-test')), None)
        ctx.config, ctx.every = Settings(announcements=[]), Mock()
        await Welcome().on_start(ctx)
        ctx.every.assert_not_called()

    async def test_announcement_commit_errors_do_not_crash(self):
        for error in (ConnectionError('lost'), IPCRejected('revision_conflict', 'r1', False)):
            self.host.fail = error
            with self.assertLogs('welcome-test', 'WARNING'):
                await self.plugin.announce(self.ctx)
        self.host.fail = None
        await self.join()
        self.assertEqual(self.host.values, {'seen:u1': True})

    def test_rejects_blank_entries(self):
        with self.assertRaises(ValueError):
            Settings(first_join_commands=[' '])
        with self.assertRaises(ValueError):
            Settings(announcements=[''])


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


class WelcomeServeTests(unittest.IsolatedAsyncioTestCase):
    async def test_serve_single_ack_gift_delivery_and_notargetmatch_retry(self):
        host = FakeHost()
        host.enqueue_no_target_match(2)
        deliveries = set()
        roster = [{'name': '玩家', 'uuid': 'u1', 'xuid': ''}]
        reader = asyncio.StreamReader(limit=1 << 20)
        writer = ServeWriter()
        peer = ManagedPeer(reader, writer)
        plugin = Welcome()

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
                    send({'type': 'response', 'id': req_id, 'payload': {'result': {'session': 's1', 'players': copy.deepcopy(roster)}}})
                elif method == 'events.commit':
                    key = (payload['subscription_id'], payload['event_id'], payload['delivery_token'])
                    if key in deliveries:
                        send({'type': 'response', 'id': req_id, 'payload': {'error': {
                            'code': 'invalid_delivery', 'message': 'already acked', 'retryable': False, 'request_id': req_id}}})
                    else:
                        deliveries.add(key)
                        res = await host.call(method, payload)
                        send({'type': 'response', 'id': req_id, 'payload': {'result': res}})
                else:
                    res = await host.call(method, payload)
                    send({'type': 'response', 'id': req_id, 'payload': {'result': res}})

        with tempfile.TemporaryDirectory() as tmp, patch('main.RETRY_DELAYS', (0.01,)):
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
                    'kind': 'player.list_added', 'subscription_id': 'join', 'event_id': 'e1', 'delivery_token': 'd1',
                    'payload': {'name': '玩家', 'uuid': 'u1', 'xuid': ''}}})
                send({'type': 'notification', 'method': 'event', 'payload': {
                    'kind': 'chat.received', 'subscription_id': 'chat', 'event_id': 'e2', 'delivery_token': 'd2',
                    'payload': {'player': '玩家', 'xuid': '', 'message': '!帮助'}}})
                for _ in range(50):
                    if host.values.get('seen:u1') is True:
                        break
                    await asyncio.sleep(0.01)
                self.assertEqual(host.values, {'seen:u1': True})

                send({'type': 'request', 'id': 'dr1', 'method': 'lifecycle.drain',
                      'payload': {'installation_id': 'inst-1', 'generation': 1}})
                self.assertTrue((await wait_response('dr1'))['payload']['result']['drained'])
                self.assertEqual(deliveries, {('join', 'e1', 'd1'), ('chat', 'e2', 'd2')})
                self.assertFalse(serve_task.done())
            finally:
                serve_task.cancel()
                pump_task.cancel()
                await asyncio.gather(serve_task, pump_task, return_exceptions=True)


if __name__ == '__main__':
    unittest.main()
