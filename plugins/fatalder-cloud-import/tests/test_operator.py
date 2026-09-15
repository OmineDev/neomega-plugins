import copy
import unittest
from types import SimpleNamespace
from fatalder_plugin.operator import Operator, _target


class Transaction:
    def __init__(self, store, commit_id):
        self.store, self.commit_id = store, commit_id
        self.values = copy.deepcopy(store.values)
        self.actions = []
    def get(self, key):
        return self.values.get(key)
    def set(self, key, value):
        self.values[key] = value
    def action(self, value):
        self.actions.append(value)
    async def save(self):
        self.store.values = self.values
        if self.actions:
            self.store.actions.extend(self.actions)
            self.store.receipts[self.commit_id] = {'operation_ids': ['action']}
            if self.store.lost_reply:
                raise TimeoutError()
        return {}


class Storage:
    def __init__(self):
        self.values, self.receipts, self.actions = {}, {}, []
        self.lost_reply = False
    async def transaction(self, commit_id=None):
        return Transaction(self, commit_id)
    async def receipt(self, commit_id):
        return self.receipts.get(commit_id)


class Context:
    def __init__(self):
        self.storage = Storage()
        self.commands = SimpleNamespace(prepare=lambda command, **kw: dict(command=command, **kw))
        self.operations = SimpleNamespace(get=self.get_operation, prepare=lambda op,args,**kw: dict(operation=op,arguments=args,**kw))
        self.state = 'succeeded'
        self.closed = 0
        self.snapshot = dict(epoch='epoch', revision=1, roster_observed=True, players=[dict(
            list=dict(uuid='uuid', username='Bot 名字', entity_unique_id=123),
            ability=dict(entity_unique_id=123, player_permissions=1, command_permissions=0, layers=[dict(type=1,abilities=65535,values=63)]))])
    async def get_operation(self, _):
        return dict(state=self.state)
    async def observe_players(self):
        return SimpleNamespace(close=self.close), copy.deepcopy(self.snapshot)
    async def close(self):
        self.closed += 1
    def op(self):
        self.snapshot['revision'] += 1
        self.snapshot['players'][0]['ability'].update(player_permissions=2, command_permissions=1)


class OperatorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ctx = Context()
        self.operator = Operator(self.ctx)
        self.identity = await self.operator.inspect('uuid', 'Bot 名字')

    async def test_missing_ability_and_duplicate_identity_fail_closed(self):
        self.ctx.snapshot['players'][0].pop('ability')
        self.assertIsNone(await self.operator.inspect('uuid', 'Bot 名字'))
        self.assertEqual(self.ctx.closed, 2)

    async def test_duplicate_name_does_not_authorize(self):
        player = copy.deepcopy(self.ctx.snapshot['players'][0])
        player['list']['uuid'] = 'other'
        self.ctx.snapshot['players'].append(player)
        self.assertIsNone(await self.operator.inspect('uuid', 'Bot 名字'))

    async def test_roster_not_observed(self):
        self.ctx.snapshot['roster_observed'] = False
        self.assertIsNone(await self.operator.inspect('uuid', 'Bot 名字'))

    async def test_no_deop_for_unowned_permission(self):
        self.ctx.op()
        row = await self.operator.set_permission(self.identity, False, 'r')
        self.assertEqual(row['reason'], 'permission_not_owned')
        self.assertEqual(self.ctx.storage.actions, [])

    async def test_success_needs_readback_and_reloads_do_not_replay(self):
        row = await self.operator.set_permission(self.identity, True, 'r')
        self.assertEqual(row['state'], 'pending')
        self.ctx.op()
        row = await Operator(self.ctx).set_permission(self.identity, True, 'r')
        self.assertEqual(row['state'], 'succeeded')
        self.assertTrue(row['changed'])
        self.assertEqual(len(self.ctx.storage.actions), 1)

    async def test_lost_reply_reconciles_original_commit(self):
        self.ctx.storage.lost_reply = True
        row = await self.operator.set_permission(self.identity, True, 'r')
        self.assertEqual(row['state'], 'unknown')
        self.ctx.op()
        row = await Operator(self.ctx).set_permission(self.identity, True, 'r')
        self.assertEqual(row['state'], 'succeeded')
        self.assertEqual(len(self.ctx.storage.actions), 1)

    async def test_unknown_action_never_replays(self):
        self.ctx.state = 'unknown'
        await self.operator.set_permission(self.identity, True, 'r')
        self.ctx.op()
        row = await Operator(self.ctx).set_permission(self.identity, True, 'r')
        self.assertEqual(row['state'], 'unknown')
        self.assertEqual(len(self.ctx.storage.actions), 1)

    async def test_missing_receipt_does_not_resubmit(self):
        await self.operator.set_permission(self.identity, True, 'r')
        self.ctx.storage.receipts.clear()
        row = await Operator(self.ctx).set_permission(self.identity, True, 'r')
        self.assertEqual(row['state'], 'unknown')
        self.assertEqual(len(self.ctx.storage.actions), 1)

    async def test_receipt_cannot_be_rebound_to_another_identity(self):
        await self.operator.set_permission(self.identity, True, 'r')
        other = dict(self.identity, entity_unique_id=456)
        row = await self.operator.set_permission(other, True, 'r')
        self.assertEqual(row['reason'], 'receipt_identity_mismatch')
        self.assertEqual(len(self.ctx.storage.actions), 1)

    async def test_same_name_rejoin_does_not_count_as_success(self):
        await self.operator.set_permission(self.identity, True, 'r')
        self.ctx.op()
        self.ctx.snapshot['epoch'] = 'new epoch'
        row = await self.operator.set_permission(self.identity, True, 'r')
        self.assertEqual(row['state'], 'unknown')

    async def test_already_op_returns_unchanged(self):
        self.ctx.op()
        row = await self.operator.set_permission(self.identity, True, 'r')
        self.assertFalse(row['changed'])
        self.assertEqual(self.ctx.storage.actions, [])

    def test_selector_escape_is_fail_closed(self):
        for name in ('evil" ]', 'evil\\', 'evil\n', ''):
            with self.assertRaises(ValueError):
                _target(name)
        self.assertEqual(_target('Bot 名字'), '@a[name="Bot 名字"]')

    async def test_cleanup_reconciliation_never_submits_absent_grant(self):
        result = await self.operator.set_permission(self.identity, True, 'none', reconcile_only=True)
        self.assertEqual(result, dict(state='succeeded', changed=False))
        self.assertEqual(self.ctx.storage.actions, [])

    async def test_failed_grant_with_fresh_non_op_can_be_cleaned(self):
        self.ctx.state = 'failed'
        await self.operator.set_permission(self.identity, True, 'failed')
        result = await self.operator.set_permission(self.identity, True, 'failed', reconcile_only=True)
        self.assertEqual(result['state'], 'succeeded')
        self.assertFalse(result['changed'])
        self.assertEqual(len(self.ctx.storage.actions), 1)

    async def test_packet_targets_entity_id_and_restores_prior_permissions(self):
        await self.operator.set_permission(self.identity, True, 'grant')
        packet = self.ctx.storage.actions[0]['arguments']['packet']
        self.assertEqual(packet['EntityUniqueID'], 123)
        self.assertEqual(packet['RequestedPermissions'], 255)
        self.ctx.op()
        await self.operator.set_permission(dict(self.identity, owned=True), False, 'revoke')
        packet = self.ctx.storage.actions[1]['arguments']['packet']
        self.assertEqual(packet['PermissionLevel'], 1)
        self.assertEqual(packet['RequestedPermissions'], 63)

    async def test_cleanup_ignores_non_requestable_ability_bit_changes(self):
        ability = self.ctx.snapshot['players'][0]['ability']
        ability['layers'][0]['values'] = 0x103f
        identity = await self.operator.inspect('uuid', 'Bot 名字')
        self.ctx.op()
        await self.operator.set_permission(dict(identity, owned=True), False, 'revoke')
        ability.update(player_permissions=1, command_permissions=0)
        ability['layers'][0]['values'] = 0x203f
        row = await self.operator.set_permission(dict(identity, owned=True), False, 'revoke')
        self.assertEqual(row['state'], 'succeeded')
        packet = self.ctx.storage.actions[0]['arguments']['packet']
        self.assertEqual(packet['RequestedPermissions'], 63)
        self.assertEqual(len(self.ctx.storage.actions), 1)

    async def test_cleanup_low_bit_mismatch_stays_pending_with_safe_readback(self):
        self.ctx.op()
        identity = dict(self.identity, owned=True)
        await self.operator.set_permission(identity, False, 'revoke')
        ability = self.ctx.snapshot['players'][0]['ability']
        ability.update(player_permissions=1, command_permissions=0)
        ability['layers'][0]['values'] = 62
        row = await self.operator.set_permission(identity, False, 'revoke')
        self.assertEqual(row['state'], 'pending')
        self.assertEqual(row['readback'], dict(
            expected=dict(is_op=False, permission_level=1, requested_permissions=63),
            observed=dict(is_op=False, permission_level=1, requested_permissions=62)))
        self.assertEqual(len(self.ctx.storage.actions), 1)
