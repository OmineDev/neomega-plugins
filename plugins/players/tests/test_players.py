"""Drive the plugin through the real SDK storage against an in-memory Host."""
import copy
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime import Event, PluginContext, ServiceRejected, find_player
from main import Players


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


class PlayersTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.host = FakeHost()
        self.ctx = PluginContext(SimpleNamespace(peer=self.host, log=logging.getLogger('players-test')), None)
        self.roster = []
        self.ctx.resolve_player = AsyncMock(side_effect=lambda **kw: find_player(self.roster, **kw))
        self.plugin = Players()
        await self.plugin.on_start(self.ctx)
        self.clock = '2026-10-21T00:00:00+00:00'
        self.plugin.now = lambda: self.clock

    async def join(self, name, uuid='u1', xuid='42', online=True):
        player = {'name': name, 'uuid': uuid, 'xuid': xuid}
        self.roster = [player] if online else []
        before = len(self.host.commits)
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'player.list_added', 'subscription_id': 'joins', 'event_id': 'e1', 'delivery_token': 'd1',
            'payload': {'name': name, 'uuid': uuid, 'xuid': xuid, 'entity_unique_id': 7}}))
        # Every join is acknowledged in exactly one commit.
        self.assertEqual(len(self.host.commits), before + 1)
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd1')
        self.assertEqual(self.host.commits[-1]['actions'], [])

    async def resolve(self, **arguments):
        return await self.plugin.resolve(self.ctx, arguments)

    async def rejected(self, **arguments):
        with self.assertRaises(ServiceRejected) as caught:
            await self.resolve(**arguments)
        return caught.exception.code

    async def test_first_join_records_profile_and_indexes(self):
        await self.join('Steve')
        self.assertEqual(self.host.values, {
            'p:u1': {'name': 'Steve', 'uuid': 'u1', 'xuid': '42', 'names': [],
                     'first_seen': self.clock, 'last_seen': self.clock},
            'n:steve': 'u1', 'x:42': 'u1'})

    async def test_unchanged_rejoin_only_updates_last_seen(self):
        await self.join('Steve')
        first = self.clock
        self.clock = '2026-10-22T08:00:00+00:00'
        await self.join('Steve')
        self.assertEqual([m['key'] for m in self.host.commits[-1]['mutations']], ['p:u1'])
        record = self.host.values['p:u1']
        self.assertEqual((record['first_seen'], record['last_seen']), (first, self.clock))

    async def test_rename_keeps_old_name_findable(self):
        await self.join('Steve')
        await self.join('Alex')
        self.assertEqual(self.host.values['p:u1']['name'], 'Alex')
        self.assertEqual(self.host.values['p:u1']['names'], ['Steve'])
        self.assertEqual(self.host.values['n:steve'], 'u1')
        self.assertEqual((await self.resolve(name='steve'))['name'], 'Alex')
        await self.join('Steve')  # 改回旧名：当前名不出现在曾用名里
        self.assertEqual(self.host.values['p:u1']['names'], ['Alex'])

    async def test_names_capped_at_five_and_evicted_index_removed(self):
        for name in ('N0', 'N1', 'N2', 'N3', 'N4', 'N5', 'N6'):
            await self.join(name)
        self.assertEqual(self.host.values['p:u1']['names'], ['N1', 'N2', 'N3', 'N4', 'N5'])
        self.assertNotIn('n:n0', self.host.values)
        self.assertEqual(self.host.values['n:n1'], 'u1')
        self.assertEqual(await self.rejected(name='N0'), 'not_found')

    async def test_name_taken_by_another_player_overwrites_and_survives_eviction(self):
        await self.join('Steve')
        await self.join('Alex')
        await self.join('Steve', uuid='u2', xuid='43')
        self.assertEqual(self.host.values['n:steve'], 'u2')
        self.assertEqual((await self.resolve(name='STEVE'))['id'], 'u2')
        for name in ('B1', 'B2', 'B3', 'B4', 'B5'):
            await self.join(name)
        self.assertNotIn('Steve', self.host.values['p:u1']['names'])
        self.assertEqual(self.host.values['n:steve'], 'u2')

    async def test_xuid_index_only_for_players_with_uuid(self):
        await self.join('Bedrock', uuid='', xuid='99')
        self.assertIn('p:xuid-99', self.host.values)
        self.assertNotIn('x:99', self.host.values)
        self.assertEqual(self.host.values['n:bedrock'], 'xuid-99')
        self.assertEqual((await self.resolve(xuid='99'))['id'], 'xuid-99')

    async def test_resolve_by_name_uuid_and_xuid(self):
        self.clock = '2026-10-21T12:00:00+00:00'
        await self.join('Steve')
        expected = {'id': 'u1', 'name': 'Steve', 'uuid': 'u1', 'xuid': '42', 'names': [],
                    'last_seen': self.clock}
        self.assertEqual(await self.resolve(name='sTeVe'), expected)
        self.assertEqual(await self.resolve(uuid='u1'), expected)
        self.assertEqual(await self.resolve(xuid='42'), expected)

    async def test_resolve_rejections(self):
        await self.join('Steve')
        self.assertEqual(await self.rejected(name='Nobody'), 'not_found')
        self.assertEqual(await self.rejected(uuid='u9'), 'not_found')
        self.assertEqual(await self.rejected(xuid='7'), 'not_found')
        self.assertEqual(await self.rejected(), 'invalid_argument')
        self.assertEqual(await self.rejected(name='Steve', uuid='u1'), 'invalid_argument')

    async def test_player_already_gone_is_only_acknowledged(self):
        await self.join('Steve', online=False)
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        twin = {'name': 'Steve', 'uuid': 'u1', 'xuid': '42'}
        self.roster = [twin, dict(twin, uuid='u2')]
        await self.plugin.on_event(self.ctx, Event(self.ctx, {
            'kind': 'player.list_added', 'subscription_id': 'joins', 'event_id': 'e2', 'delivery_token': 'd2',
            'payload': {'name': 'Steve', 'uuid': 'u1', 'xuid': '42', 'entity_unique_id': 7}}))
        self.assertEqual(self.host.commits[-1]['delivery_token'], 'd2')
        self.assertEqual(self.host.commits[-1]['mutations'], [])
        self.assertEqual(self.host.values, {})


if __name__ == '__main__':
    unittest.main()
