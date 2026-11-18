"""Drive the plugin through the real SDK storage, positions, and packet streams against an in-memory Host."""
import asyncio
import copy
import dataclasses
import json
import logging
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from neomega_runtime import (Event, PacketSubscriptionError, PluginContext,
                             ProtocolError, ServiceRejected)
from neomega_runtime.config import load_config
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config_schema import check_config_schema, schema_for_config
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from neomega_runtime.protocol import PROTOCOL_VERSION, SCHEMA_ID, StringBytes, packets
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from main import Back, Settings, charge_request_id

ROOT = Path(__file__).resolve().parents[1]
ROBOT = {'name': '机器人', 'uuid': 'u-bot', 'xuid': '1'}
PLAYER = {'name': '玩家', 'uuid': 'u1', 'xuid': '42'}
ALEX = {'name': 'Alex', 'uuid': 'u2', 'xuid': '43'}
NO_UUID_PLAYER = {'name': 'XuidOnly', 'uuid': '', 'xuid': '99'}


def at(x, y, z, dimension=0, unique_id='u1', entity_id=None):
    """位置查询结果；参数是脚的坐标，查询报的 y 是眼睛高度（脚 + 1.62）。"""
    row = {'uniqueId': unique_id, 'dimension': dimension, 'position': {'x': x, 'y': y + 1.62, 'z': z}}
    if entity_id is not None:
        row['id'] = entity_id
    return {'complete': True, 'entities': [row]}


def death_packet(name, message='death.attack.mob', *, text_type=2, needs_translation=True, extra_params=('Zombie',)):
    params = [name, *extra_params] if name is not None else []
    return packets.Text(
        TextType=text_type,
        NeedsTranslation=needs_translation,
        SourceName='',
        Message=message,
        Parameters=params,
        XUID='',
        PlatformChatID='',
    )


class FakeHost:
    """players / robot / positions / typed packet subscriptions / state CAS."""
    def __init__(self):
        self.values, self.revision, self.commits = {}, 0, []
        self.online = [dict(ROBOT), dict(PLAYER)]
        self.bot_name = ROBOT['name']
        self.robot_error = None
        self.players_error = None
        self.sample_by_name = {}
        self.all_samples = at(1.5, 64, -3.5, dimension=0, unique_id=PLAYER['uuid'])
        self.query_error = None
        self.query_calls = []

        # Packet subscription simulation
        self.sub_counter = 0
        self.sub_batches = []  # queue of responses or exceptions for packets.subscribe.read
        self.sub_closed_handles = []
        self.sub_open_error = None

    def enqueue_packet(self, model):
        self.sub_batches.append({'packet': model})

    def enqueue_terminal(self, reason):
        self.sub_batches.append({'terminal': {'reason': reason}})

    async def call(self, method, payload, timeout=None):
        if method == 'robot.status':
            if self.robot_error is not None:
                raise self.robot_error
            return {'session': 's1', 'in_game': True, 'bot_name': self.bot_name, 'player_count': len(self.online)}
        if method == 'players.list':
            if self.players_error is not None:
                raise self.players_error
            return {'session': 's1', 'players': copy.deepcopy(self.online)}
        if method == 'operations.submit':
            self.query_calls.append(copy.deepcopy(payload['arguments']))
            if self.query_error is not None:
                raise self.query_error
            return {'operation_id': 'q1', 'state': 'accepted', 'duplicate': False}
        if method == 'operations.get':
            args = self.query_calls[-1]
            if args.get('name'):
                result = self.sample_by_name.get(args['name'], {'complete': True, 'entities': []})
            else:
                result = self.all_samples
            return {
                'operation_id': 'q1', 'state': 'succeeded', 'can_retry': False,
                'result': {'state': 'succeeded', 'evidence': 'acknowledged', 'result': copy.deepcopy(result)},
            }
        if method == 'packets.subscribe.open':
            if self.sub_open_error is not None:
                raise self.sub_open_error
            self.sub_counter += 1
            self._sub_seq = 0
            return {
                'handle': f'sub-{self.sub_counter}',
                'connection_id': 'c1',
                'schema_hash': SCHEMA_ID,
                'protocol_version': PROTOCOL_VERSION,
            }
        if method == 'packets.subscribe.read':
            if not self.sub_batches:
                return {'events': []}
            item = self.sub_batches.pop(0)
            if isinstance(item, Exception):
                raise item
            if 'terminal' in item:
                return {'events': [], 'terminal': item['terminal']}
            self._sub_seq += 1
            model = item['packet']
            return {'events': [{
                'sequence': self._sub_seq,
                'packet_id': 9,
                'packet': model.to_envelope(),
                'context': {
                    'mode': 'connection',
                    'connection_id': 'c1',
                    'protocol_version': PROTOCOL_VERSION,
                    'shield_id': 0,
                    'revision': 1,
                },
                'observed_at': '2026-11-18T00:00:00Z',
            }]}
        if method == 'packets.subscribe.close':
            self.sub_closed_handles.append(payload['handle'])
            return {'closed': True}
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

    def commands(self, commit=-1):
        return [action['arguments']['cmd'] for action in self.commits[commit]['actions']]

    def game_commands(self, commit=-1):
        return [cmd for cmd in self.commands(commit) if not cmd.startswith('tellraw')]

    def reply(self, commit=-1):
        texts = []
        for cmd in self.commands(commit):
            if cmd.startswith('tellraw'):
                texts += [part['text'] for part in json.loads(cmd[cmd.index('{'):])['rawtext']]
        return ''.join(texts)


class BackTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.now = 1_800_000_000.0
        self.tasks = []
        self.ctx = await self.start(Settings(), spawn_tasks=False)
        self.as_player(PLAYER)

    async def asyncTearDown(self):
        await self.cancel_tasks()

    async def cancel_tasks(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

    async def start(self, settings, *, spawn_tasks=False):
        await self.cancel_tasks()
        tasks = self.tasks

        def spawn(coro, *, name=None):
            if spawn_tasks:
                task = asyncio.create_task(coro, name=name)
                tasks.append(task)
                return task
            coro.close()
            return SimpleNamespace(done=lambda: True, cancel=lambda: False)

        worker = SimpleNamespace(peer=self.host, log=logging.getLogger('back-test'), spawn=spawn)
        ctx = PluginContext(worker, None)
        ctx.config = settings
        ctx.capabilities = {
            'session': 's1',
            'packet_subscriptions': {
                'open': 'packets.subscribe.open',
                'read': 'packets.subscribe.read',
                'close': 'packets.subscribe.close',
                'min_host_api_minor': 5,
                'max_events': 64,
                'max_bytes': 524288,
            },
        }
        ctx._operations = Operations(self.host, 's1')
        ctx.services.call = AsyncMock(side_effect=IPCRejected('permission_denied', 'r1', False))
        self.plugin = Back()
        self.plugin.clock = lambda: self.now
        await self.plugin.on_start(ctx)
        self.plugin.clock = lambda: self.now
        return ctx

    def as_player(self, player):
        self.speaker = player
        self.ctx.resolve_player = AsyncMock(return_value=dict(player))

    async def deliver(self, kind, payload, event_id='e1', delivery_token='d1'):
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': kind, 'payload': payload,
            'subscription_id': 'sub', 'event_id': event_id, 'delivery_token': delivery_token}))
        self.assertEqual(self.host.commits[-1]['delivery_token'], delivery_token)

    async def say(self, message, event_id='e1'):
        await self.deliver('chat.received', {
            'player': self.speaker['name'], 'xuid': self.speaker['xuid'], 'message': message},
            event_id=event_id)

    # ---- 在线玩家缓存与实体映射 ----

    async def test_startup_cache_excludes_bot_and_tolerates_robot_failure(self):
        self.assertNotIn(ROBOT['uuid'], self.plugin.online)
        self.assertIn(PLAYER['uuid'], self.plugin.online)
        self.assertEqual(self.plugin.online[PLAYER['uuid']]['player_id'], 'u1')

        # If robot() fails at startup, later refresh purges bot when known
        self.host.robot_error = IPCRejected('unavailable', 'r1', True)
        self.ctx = await self.start(Settings(), spawn_tasks=False)
        self.assertIn(ROBOT['uuid'], self.plugin.online)
        self.host.robot_error = None
        await self.plugin.refresh_bot_name(self.ctx)
        self.assertNotIn(ROBOT['uuid'], self.plugin.online)

    async def test_join_and_leave_maintain_cache_and_preserve_death_record(self):
        await self.deliver('player.list_added', {
            'uuid': ALEX['uuid'], 'name': ALEX['name'], 'xuid': ALEX['xuid'], 'entity_unique_id': -99})
        self.assertEqual(self.plugin.online[ALEX['uuid']]['entity_unique_id'], -99)

        # Bot join is excluded
        await self.deliver('player.list_added', {
            'uuid': ROBOT['uuid'], 'name': ROBOT['name'], 'xuid': ROBOT['xuid'], 'entity_unique_id': -1})
        self.assertNotIn(ROBOT['uuid'], self.plugin.online)

        # Sample Alex using entity_unique_id string as uniqueId
        self.host.all_samples = at(10, 64, 20, dimension=1, unique_id='-99')
        await self.plugin.sample_once(self.ctx)
        self.assertEqual(self.plugin.last_pos['u2']['dimension'], 1)

        # Death records persistent state
        self.host.sample_by_name[ALEX['name']] = at(11, 65, 21, dimension=1, unique_id=ALEX['uuid'])
        await self.plugin.handle_death_packet(self.ctx, death_packet(ALEX['name']))
        self.assertIn('death:u2', self.host.values)

        # Leave only has uuid; clears online and last_pos, keeps durable death:<pid>
        await self.deliver('player.list_removed', {'uuid': ALEX['uuid']})
        self.assertNotIn(ALEX['uuid'], self.plugin.online)
        self.assertNotIn('u2', self.plugin.last_pos)
        self.assertIn('death:u2', self.host.values)

        # Removing unknown uuid safely acknowledges
        await self.deliver('player.list_removed', {'uuid': 'nonexistent'})

    # ---- 后台坐标采样 ----

    async def test_sample_once_converts_feet_skips_void_and_unknown_entities(self):
        self.host.all_samples = {'complete': True, 'entities': [
            {'uniqueId': ROBOT['uuid'], 'dimension': 0, 'position': {'x': 0, 'y': 70, 'z': 0}},
            {'uniqueId': 'unknown-uuid', 'dimension': 0, 'position': {'x': 5, 'y': 70, 'z': 5}},
            # y = 201.62001037597656 eye -> 200.0 feet
            {'uniqueId': PLAYER['uuid'], 'dimension': 2, 'position': {'x': 12.3456, 'y': 201.62001037597656, 'z': -8.5}},
        ]}
        await self.plugin.sample_once(self.ctx)
        self.assertEqual(self.plugin.last_pos['u1'], {
            'dimension': 2, 'x': 12.346, 'y': 200.0, 'z': -8.5, 'at': self.now,
        })

        # Void sample (y = -60.1 < -60) is ignored when allow_void_return=False
        self.now += 2
        self.host.all_samples = at(0, -60.1, 0, dimension=0, unique_id=PLAYER['uuid'])
        await self.plugin.sample_once(self.ctx)
        self.assertEqual(self.plugin.last_pos['u1']['y'], 200.0)

        # Boundary y == -60.0 is valid
        self.host.all_samples = at(0, -60.0, 0, dimension=0, unique_id=PLAYER['uuid'])
        await self.plugin.sample_once(self.ctx)
        self.assertEqual(self.plugin.last_pos['u1']['y'], -60.0)

        # Void sample recorded when allow_void_return=True
        self.ctx.config = Settings(allow_void_return=True)
        self.host.all_samples = at(0, -80.0, 0, dimension=0, unique_id=PLAYER['uuid'])
        await self.plugin.sample_once(self.ctx)
        self.assertEqual(self.plugin.last_pos['u1']['y'], -80.0)

    # ---- 死亡监听与回退 ----

    async def test_death_records_live_position_and_notifies(self):
        self.host.sample_by_name[PLAYER['name']] = at(100.5, 72.0, -50.25, dimension=1, unique_id=PLAYER['uuid'])
        await self.plugin.handle_death_packet(self.ctx, death_packet(PLAYER['name']))
        self.assertEqual(self.host.values['death:u1'], {
            'dimension': 1, 'x': 100.5, 'y': 72.0, 'z': -50.25, 'at': self.now, 'used': False,
        })
        self.assertIn('下界 100.5 72 -50.25', self.host.reply())
        self.assertIn('!返回', self.host.reply())

    async def test_death_notification_disabled_still_saves_without_reply(self):
        self.ctx.config = Settings(notify_on_death=False)
        self.host.sample_by_name[PLAYER['name']] = at(1, 64, 2, dimension=0, unique_id=PLAYER['uuid'])
        await self.plugin.handle_death_packet(self.ctx, death_packet(PLAYER['name']))
        self.assertIn('death:u1', self.host.values)
        self.assertEqual(self.host.commits[-1]['actions'], [])

    async def test_death_falls_back_to_recent_sample_on_query_failure_or_void(self):
        # Sample valid position at t=self.now
        self.host.all_samples = at(30, 65, 40, dimension=0, unique_id=PLAYER['uuid'])
        await self.plugin.sample_once(self.ctx)

        # Query failure within 10s -> falls back to last_pos
        self.now += 5
        self.host.query_error = ProtocolError('position query failed')
        await self.plugin.handle_death_packet(self.ctx, death_packet(PLAYER['name']))
        self.assertEqual(self.host.values['death:u1'], {
            'dimension': 0, 'x': 30.0, 'y': 65.0, 'z': 40.0, 'at': self.now, 'used': False,
        })

        # Void live sample within 10s -> falls back to last_pos
        self.host.query_error = None
        self.host.sample_by_name[PLAYER['name']] = at(99, -75, 99, dimension=2, unique_id=PLAYER['uuid'])
        self.now += 4  # 9s after initial sample
        await self.plugin.handle_death_packet(self.ctx, death_packet(PLAYER['name']))
        self.assertEqual(self.host.values['death:u1']['x'], 30.0)
        self.assertEqual(self.host.values['death:u1']['dimension'], 0)

        # Stale last_pos (> 10s) and void live sample -> does NOT overwrite existing death:u1
        self.now += 2  # 11s after initial sample
        await self.plugin.handle_death_packet(self.ctx, death_packet(PLAYER['name']))
        self.assertEqual(self.host.values['death:u1']['at'], self.now - 2)
        self.assertIn('虚空中死亡', self.host.reply())

    async def test_non_death_packets_and_stringbytes_handling(self):
        # Non-death packets ignored
        for pkt in (
            death_packet(PLAYER['name'], message='multiplayer.player.joined'),
            death_packet(PLAYER['name'], text_type=1),
            death_packet(PLAYER['name'], needs_translation=False),
            death_packet(None),
            death_packet('NamedWolf'),
            death_packet(ROBOT['name']),
        ):
            await self.plugin.handle_death_packet(self.ctx, pkt)
        self.assertNotIn('death:u1', self.host.values)

        # StringBytes with invalid UTF-8 bytes in Message or Parameters is handled safely
        sb_msg = StringBytes(b'death.attack.\xffmob')
        model = SimpleNamespace(
            TextType=2, NeedsTranslation=True, Message=sb_msg,
            Parameters=[PLAYER['name'], StringBytes(b'\xffZombie')],
        )
        self.host.sample_by_name[PLAYER['name']] = at(5, 64, 5, dimension=0, unique_id=PLAYER['uuid'])
        await self.plugin.handle_death_packet(self.ctx, model)
        self.assertEqual(self.host.values['death:u1']['x'], 5.0)

        # Invalid UTF-8 in victim name safely doesn't match and doesn't crash
        bad_victim = SimpleNamespace(
            TextType=2, NeedsTranslation=True, Message='death.attack.mob',
            Parameters=[StringBytes(b'\xff\xfe')],
        )
        await self.plugin.handle_death_packet(self.ctx, bad_victim)

    async def test_death_loop_real_subscription_resubscribes_and_stops_on_terminal(self):
        self.host.sample_by_name[PLAYER['name']] = at(7, 80, 9, dimension=0, unique_id=PLAYER['uuid'])
        # Batch 1: overflow -> triggers sleep(2) and resubscribe
        self.host.enqueue_terminal('overflow')
        # Batch 2: valid death packet, then permission_revoked -> exits cleanly
        self.host.enqueue_packet(death_packet(PLAYER['name']))
        self.host.enqueue_terminal('permission_revoked')

        with patch('main.asyncio.sleep', new=AsyncMock()):
            await self.plugin.death_loop(self.ctx)

        self.assertEqual(self.host.sub_counter, 2)
        self.assertEqual(self.host.values['death:u1'], {
            'dimension': 0, 'x': 7.0, 'y': 80.0, 'z': 9.0, 'at': self.now, 'used': False,
        })
        self.assertEqual(self.host.sub_closed_handles, ['sub-1', 'sub-2'])

        # Closed terminal exits cleanly on first stream
        self.host.enqueue_terminal('closed')
        await self.plugin.death_loop(self.ctx)
        self.assertEqual(self.host.sub_counter, 3)

    async def test_spawned_loops_run_and_cancel_cleanly_on_teardown(self):
        self.host.all_samples = at(22, 64, 33, dimension=0, unique_id=PLAYER['uuid'])
        self.host.sample_by_name[PLAYER['name']] = at(23, 64, 34, dimension=0, unique_id=PLAYER['uuid'])
        self.host.enqueue_packet(death_packet(PLAYER['name']))
        self.ctx = await self.start(Settings(sample_interval_seconds=1), spawn_tasks=True)
        # Allow spawned loops a few ticks to run sampling and consume the packet
        for _ in range(10):
            await asyncio.sleep(0.01)
            if 'death:u1' in self.host.values:
                break
        self.assertIn('u1', self.plugin.last_pos)
        self.assertEqual(self.host.values['death:u1']['x'], 23.0)

    # ---- !返回 与 !死亡点 命令 ----

    async def test_back_and_deathpos_commands_and_prefixes(self):
        # No record yet
        await self.say('!返回')
        self.assertIn('没有找到你的死亡记录', self.host.reply())
        await self.say('！死亡点')
        self.assertIn('没有找到你的死亡记录', self.host.reply())

        # Record a death in the End
        self.host.sample_by_name[PLAYER['name']] = at(100.5, 55, -20.25, dimension=2, unique_id=PLAYER['uuid'])
        await self.plugin.handle_death_packet(self.ctx, death_packet(PLAYER['name']))

        self.now += 15
        await self.say('!deathpos')
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        self.assertIn('末地 100.5 55 -20.25', self.host.reply())
        self.assertIn('9 分 45 秒', self.host.reply())

        # Return using fullwidth ！ and alias
        await self.say('！back')
        self.assertEqual(self.host.game_commands(), ['execute in the_end run tp @a[name="玩家"] 100.5 55 -20.25'])
        self.assertIn('已传送回死亡地点（末地 100.5 55 -20.25）', self.host.reply())
        self.assertTrue(self.host.values['death:u1']['used'])

        # One-time blocks second return
        self.now += 60
        await self.say('!fh')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('已经用过了', self.host.reply())

    async def test_multi_use_cooldown_persistence_and_expiry(self):
        self.ctx.config = Settings(one_time=False, cooldown_seconds=30, expire_seconds=120)
        self.host.sample_by_name[PLAYER['name']] = at(1, 64, 2, dimension=1, unique_id=PLAYER['uuid'])
        await self.plugin.handle_death_packet(self.ctx, death_packet(PLAYER['name']))

        await self.say('!返回')
        self.assertTrue(self.host.game_commands())

        # Cooldown blocks immediately and survives restart
        self.now += 10.2
        self.ctx = await self.start(Settings(one_time=False, cooldown_seconds=30, expire_seconds=120))
        self.as_player(PLAYER)
        await self.say('!返回')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('请 20 秒后再试', self.host.reply())

        # Clock moving backwards stays bounded to cooldown_seconds
        self.now -= 3600
        await self.say('!返回')
        self.assertIn('请 30 秒后再试', self.host.reply())

        # Advance past cooldown (still within 120s expiry)
        self.now += 3600 + 20
        await self.say('!返回')
        self.assertTrue(self.host.game_commands())

        # Advance past expiry
        self.now += 100  # 130.2s total > 120s
        await self.say('!死亡点')
        self.assertIn('已过期', self.host.reply())
        await self.say('!返回')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('已经过期', self.host.reply())

    async def test_zero_expire_and_zero_cooldown(self):
        self.ctx.config = Settings(expire_seconds=0, cooldown_seconds=0, one_time=False)
        self.host.sample_by_name[PLAYER['name']] = at(5, 64, 6, dimension=0, unique_id=PLAYER['uuid'])
        await self.plugin.handle_death_packet(self.ctx, death_packet(PLAYER['name']))

        self.now += 100_000
        await self.say('!死亡点')
        self.assertIn('长期有效', self.host.reply())
        await self.say('!返回')
        await self.say('!返回')
        self.assertTrue(self.host.game_commands())

    async def test_void_record_blocked_when_allow_void_return_turned_off(self):
        self.ctx.config = Settings(allow_void_return=True)
        self.host.sample_by_name[PLAYER['name']] = at(0, -70, 0, dimension=2, unique_id=PLAYER['uuid'])
        await self.plugin.handle_death_packet(self.ctx, death_packet(PLAYER['name']))
        self.assertEqual(self.host.values['death:u1']['y'], -70.0)

        # Config changed to forbid void return before player uses !返回
        self.ctx.config = Settings(allow_void_return=False)
        await self.say('!死亡点')
        self.assertIn('虚空位置不可返回', self.host.reply())
        await self.say('!返回')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('虚空中，不允许返回', self.host.reply())
        self.assertFalse(self.host.values['death:u1']['used'])

        # Turning it back on allows return
        self.ctx.config = Settings(allow_void_return=True)
        await self.say('!返回')
        self.assertEqual(self.host.game_commands(), ['execute in the_end run tp @a[name="玩家"] 0 -70 0'])

    # ---- 经济收费 ----

    def charged(self, **kwargs):
        self.ctx.config = Settings(cost=15)
        self.ctx.services.call = AsyncMock(**kwargs)

    async def seed_death(self, player=PLAYER):
        self.host.values['death:' + (player['uuid'] or 'xuid-' + player['xuid'])] = {
            'dimension': 0, 'x': 10.0, 'y': 64.0, 'z': 20.0, 'at': self.now, 'used': False,
        }

    async def test_economy_pay_success_and_xuid_fallback_id(self):
        await self.seed_death()
        self.charged(return_value={'balance': 85, 'duplicate': False, 'currency': '金币', 'pending': False})
        session = 'd13361ff7a140ac2c7fd915377aaaa51c58fa124e9779506c143bb94c927d607'
        await self.say('!返回', event_id=f'game:{session}:42')
        self.ctx.services.call.assert_awaited_once_with(
            'plugin.neomega.economy.pay',
            {'player_id': 'u1', 'amount': 15, 'reason': '返回死亡点',
             'request_id': charge_request_id(f'game:{session}:42')},
            timeout=5,
        )
        self.assertLessEqual(len(charge_request_id(f'game:{session}:42')), 64)
        self.assertIn('花费 15 金币，剩余 85 金币', self.host.reply())
        self.assertTrue(self.host.game_commands())

        # Player without uuid uses xuid-<xuid> without player: prefix
        await self.seed_death(NO_UUID_PLAYER)
        self.as_player(NO_UUID_PLAYER)
        self.charged(return_value={'balance': 50, 'duplicate': False, 'pending': False})
        await self.say('!返回', event_id='evt-xuid')
        self.assertEqual(self.ctx.services.call.await_args.args[1]['player_id'], 'xuid-99')
        self.assertIn('花费 15 金币，剩余 50 金币', self.host.reply())

    async def test_economy_insufficient_funds_and_unsettled_retry(self):
        await self.seed_death()
        self.charged(side_effect=ServiceRejected('insufficient_funds'))
        await self.say('!返回')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('金币不足，返回一次需要 15 金币', self.host.reply())
        self.assertFalse(self.host.values['death:u1']['used'])
        self.assertNotIn('cd:u1', self.host.values)

        # Two consecutive unsettled failures -> retries once with same request_id, then refuses
        for exc in (ServiceWaitTimeout('c1'), ServiceUncertain('intent'),
                    ServiceFailed({'call_id': 'c1', 'state': 'failed'})):
            self.charged(side_effect=exc)
            await self.say('!返回', event_id='evt-u')
            self.assertEqual(self.ctx.services.call.await_count, 2)
            req_ids = [c.args[1]['request_id'] for c in self.ctx.services.call.await_args_list]
            self.assertEqual(req_ids[0], req_ids[1])
            self.assertEqual(self.host.game_commands(), [])
            self.assertIn('扣款结果暂时不确定', self.host.reply())
            self.assertFalse(self.host.values['death:u1']['used'])

        # Unavailable on first try, duplicate success on retry -> teleports
        self.charged(side_effect=[
            ServiceRejected('unavailable'),
            {'balance': 5, 'duplicate': True, 'currency': '金币', 'pending': False},
        ])
        await self.say('!返回', event_id='evt-ok')
        self.assertEqual(self.ctx.services.call.await_count, 2)
        self.assertTrue(self.host.game_commands())
        self.assertTrue(self.host.values['death:u1']['used'])

    async def test_no_charge_when_refused_before_payment(self):
        self.charged(return_value={'balance': 99, 'duplicate': False, 'currency': '金币', 'pending': False})
        # Missing record
        await self.say('!返回')
        # Expired record
        self.host.values['death:u1'] = {
            'dimension': 0, 'x': 10.0, 'y': 64.0, 'z': 20.0, 'at': self.now - 700, 'used': False,
        }
        await self.say('!返回')
        # Already used record
        self.host.values['death:u1'] = {
            'dimension': 0, 'x': 10.0, 'y': 64.0, 'z': 20.0, 'at': self.now, 'used': True,
        }
        await self.say('!返回')
        # Void record
        self.host.values['death:u1'] = {
            'dimension': 0, 'x': 10.0, 'y': -70.0, 'z': 20.0, 'at': self.now, 'used': False,
        }
        await self.say('!返回')
        # Cooldown active
        self.host.values['death:u1'] = {
            'dimension': 0, 'x': 10.0, 'y': 64.0, 'z': 20.0, 'at': self.now, 'used': False,
        }
        self.host.values['cd:u1'] = {'at': self.now - 5}
        await self.say('!返回')
        self.ctx.services.call.assert_not_awaited()

    async def test_economy_not_installed_is_free_and_other_rejection_refuses(self):
        await self.seed_death()
        self.charged(side_effect=ServiceRejected('player_offline'))
        await self.say('!返回')
        self.assertEqual(self.host.game_commands(), [])
        self.assertIn('扣款失败', self.host.reply())

        self.charged(side_effect=IPCRejected('permission_denied', 'r1', False))
        await self.say('!返回')
        self.assertTrue(self.host.game_commands())
        self.assertNotIn('花费', self.host.reply())

    async def test_other_chat_and_bad_arguments_only_acknowledge(self):
        for msg in ('你好', '!其他命令', '!返回 多余参数', '!死亡点 多余参数', '!返回 "没闭合'):
            await self.say(msg)
            self.assertEqual(self.host.commits[-1]['actions'], [])
            self.assertEqual(self.host.commits[-1]['mutations'], [])


class ConfigAndManifestTests(unittest.TestCase):
    def load(self, value):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'config.json'
            path.write_text(json.dumps(value, ensure_ascii=False))
            return load_config(Settings, path)

    def test_static_schema_matches_settings(self):
        schema = json.loads((ROOT / 'config.schema.json').read_text())
        check_config_schema(schema)
        self.assertEqual(schema, schema_for_config(Settings))

    def test_example_and_defaults_load(self):
        example = json.loads((ROOT / 'config.example.json').read_text())
        self.assertEqual(example, dataclasses.asdict(Settings()))
        self.assertEqual(load_config(Settings, ROOT / 'config.example.json'), Settings())
        self.assertEqual(self.load({}), Settings())

    def test_invalid_configurations_are_rejected(self):
        for bad in (
            {'sample_interval_seconds': 0},
            {'sample_interval_seconds': 11},
            {'expire_seconds': -1},
            {'cooldown_seconds': -1},
            {'cost': -1},
            {'one_time': 'yes'},
            {'notify_on_death': 1},
            {'allow_void_return': 'false'},
            {'unknown': 1},
        ):
            with self.subTest(bad=bad), self.assertRaises(ConfigError):
                self.load(bad)

    def test_manifest_purposes_complete(self):
        manifest = json.loads((ROOT / 'manifest.json').read_text())
        self.assertEqual(manifest['id'], 'neomega.back')
        self.assertEqual(manifest['version'], '1.0.0')
        self.assertEqual(manifest['host_api'], {'major': 1, 'min_minor': 5})
        perms = manifest['permissions']
        purposes = perms['purposes']
        expected = {
            *(f'event:{s["kind"]}' for s in manifest['subscriptions']),
            *(f'operation:{op}' for op in perms['operations']),
            *(f'players:{p}' for p in perms['players']),
            *(f'packet_observe:{pid}' for pid in perms['packet_observe_ids']),
            *(f'service:{svc}' for svc in perms['services']),
        }
        self.assertEqual(set(purposes), expected)
        self.assertTrue(all(isinstance(purposes[k], str) and purposes[k].strip() for k in expected))


if __name__ == '__main__':
    unittest.main()
