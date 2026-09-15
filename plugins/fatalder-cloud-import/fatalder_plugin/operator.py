"""Identity checked OP actions with durable, non-replaying receipts."""
import asyncio
import hashlib
from datetime import datetime, timedelta, timezone


def _target(name):
    # The game parser is not JSON. Fail closed for escape/control syntax.
    if (not isinstance(name, str) or not name or len(name.encode('utf-8')) > 256
            or any(ord(c) < 32 or ord(c) == 127 or c in '"\\' for c in name)):
        raise ValueError('unsafe player name')
    return '@a[name="' + name + '"]'


class Operator:
    def __init__(self, ctx):
        self.ctx = ctx
        self._lock = asyncio.Lock()

    async def inspect(self, uuid, name):
        """Read a new host observation, requiring unambiguous observed identity."""
        _target(name)
        if not isinstance(uuid, str) or not uuid:
            return None
        stream, snapshot = await self.ctx.observe_players()
        try:
            if snapshot.get('roster_observed') is not True:
                return None
            players = snapshot['players']
            matching = [p for p in players if p['list']['uuid'] == uuid]
            named = [p for p in players if p['list']['username'] == name]
            if len(matching) != 1 or len(named) != 1 or matching[0] != named[0]:
                return None
            player = matching[0]
            entry, ability = player['list'], player.get('ability')
            if not ability or ability['entity_unique_id'] != entry['entity_unique_id']:
                return None
            if sum(p['list']['entity_unique_id'] == entry['entity_unique_id'] for p in players) != 1:
                return None
            pp, cp = ability['player_permissions'], ability['command_permissions']
            if pp == 2 and cp >= 1:
                is_op = True
            elif pp in (0, 1) and cp == 0:
                is_op = False
            else:
                return None
            layers = [layer for layer in (ability.get('layers') or []) if layer['type'] == 1]
            if len(layers) != 1:
                return None
            # RequestPermissions controls only the low eight ability bits.
            # Other observed flags can change independently of OP restoration.
            original_permissions = layers[0]['values'] & layers[0]['abilities'] & 0xff
            return dict(permission_level=pp, requested_permissions=original_permissions, uuid=uuid, name=name, entity_unique_id=entry['entity_unique_id'],
                        epoch=snapshot['epoch'], revision=snapshot['revision'], is_op=is_op)
        finally:
            await stream.close()

    async def set_permission(self, identity, grant, receipt_key, *, reconcile_only=False):
        """Never infer action success from admission; require fresh permission readback.

        A reservation survives a lost commit reply. Its absence is the only path
        which may prepare a new action. Unknown reservations are never replayed.
        The caller owns the job lease and must persist owned=True before deop.
        """
        if type(grant) is not bool or not isinstance(receipt_key, str) or not receipt_key:
            raise ValueError('invalid permission request')
        _target(identity.get('name'))
        if not grant and identity.get('owned') is not True:
            return dict(state='failed', reason='permission_not_owned')
        digest = hashlib.sha256(receipt_key.encode()).hexdigest()
        key, commit_id = 'fatalder.op.' + digest, 'fatalder_op_' + digest
        binding = {k: identity[k] for k in ('uuid', 'name', 'entity_unique_id', 'epoch', 'permission_level', 'requested_permissions')}
        binding['grant'] = grant
        async with self._lock:
            try:
                tx = await self.ctx.storage.transaction()
                previous = tx.get(key)
                if previous is not None:
                    if previous.get('binding') != binding:
                        return dict(state='failed', reason='receipt_identity_mismatch')
                    return await self._reconcile(identity, grant, commit_id, reconcile_only=reconcile_only)
                if reconcile_only:
                    return dict(state='succeeded', changed=False)
                current = await self.inspect(identity['uuid'], identity['name'])
                if not self._same(identity, current):
                    return dict(state='failed', reason='identity_unavailable')
                if current['is_op'] == grant:
                    return dict(state='succeeded', changed=False, identity=current)
                tx.set(key, dict(binding=binding, phase='reserved', commit_id=commit_id))
                await tx.save()
                # Recheck after reservation; a crash from here stays unknown.
                current = await self.inspect(identity['uuid'], identity['name'])
                if not self._same(identity, current):
                    return dict(state='unknown', reason='identity_changed')
                if current['is_op'] == grant:
                    return dict(state='unknown', reason='permission_changed_externally')
                tx = await self.ctx.storage.transaction(commit_id=commit_id)
                deadline = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat().replace('+00:00', 'Z')
                # RequestPermissions addresses the observed entity ID, so queue
                # delay cannot retarget a same-name replacement player.
                intent = self.ctx.operations.prepare('framework.packet.send_json',
                    {'packet_id': 185, 'packet': {
                        'EntityUniqueID': identity['entity_unique_id'],
                        'PermissionLevel': 2 if grant else identity['permission_level'],
                        'RequestedPermissions': (identity['requested_permissions'] | 255) if grant else identity['requested_permissions']}},
                    idempotency_key=commit_id, deadline=deadline)
                tx.set(key, dict(binding=binding, phase='submitted', commit_id=commit_id))
                tx.action(intent)
                await tx.save()
                return await self._reconcile(identity, grant, commit_id, reconcile_only=reconcile_only)
            except asyncio.CancelledError:
                raise
            except Exception:
                # No server text, secrets, or speculative retry is exposed here.
                return dict(state='unknown', reason='permission_receipt_unresolved')

    @staticmethod
    def _same(expected, current):
        return current is not None and all(expected.get(k) == current.get(k)
            for k in ('uuid', 'name', 'entity_unique_id', 'epoch'))

    async def _reconcile(self, identity, grant, commit_id, *, reconcile_only=False):
        receipt = await self.ctx.storage.receipt(commit_id)
        if receipt is None or len(receipt['operation_ids']) != 1:
            return dict(state='unknown', reason='commit_receipt_unavailable')
        operation_id = receipt['operation_ids'][0]
        row = await self.ctx.operations.get(operation_id)
        state = row['state']
        if state in ('accepted', 'dispatching', 'running'):
            return dict(state='pending', operation_id=operation_id)
        if state in ('failed', 'cancelled'):
            if reconcile_only and grant:
                current = await self.inspect(identity['uuid'], identity['name'])
                if self._same(identity, current) and not current['is_op']:
                    return dict(state='succeeded', changed=False, operation_id=operation_id)
            return dict(state='failed', operation_id=operation_id)
        if state != 'succeeded':
            return dict(state='unknown', operation_id=operation_id)
        current = await self.inspect(identity['uuid'], identity['name'])
        if not self._same(identity, current):
            return dict(state='unknown', reason='identity_unavailable', operation_id=operation_id)
        if current['is_op'] != grant or (not grant and any(current[k] != identity[k] for k in ('permission_level', 'requested_permissions'))):
            return dict(state='pending', reason='permission_readback_pending', operation_id=operation_id,
                        readback=dict(
                            expected=dict(is_op=grant,
                                          permission_level=2 if grant else identity['permission_level'],
                                          requested_permissions=255 if grant else identity['requested_permissions']),
                            observed={k: current[k] for k in ('is_op', 'permission_level', 'requested_permissions')}))
        return dict(state='succeeded', changed=True, identity=current, operation_id=operation_id)
