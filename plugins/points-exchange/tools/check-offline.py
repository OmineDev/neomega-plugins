"""Small SDK-level simulation; no Host or game connection and no test framework."""
import asyncio
import copy
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from main import PointsExchange, Settings, business_id, inspect, redeem
from neomega_runtime.plugin import Event
from neomega_runtime.business import Business
from neomega_runtime.game import Commands
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import Operations
from neomega_runtime.storage import Storage, CommitUncertain


class Peer:
    """Only the IPC contracts used here; JSON disk state models process restart."""
    def __init__(self, path):
        self.path = path
        self.data = json.loads(path.read_text()) if path.exists() else dict(values={}, commits={}, operations={}, revision=0)
        self.lose_reply = False

    async def call(self, method, body, **kwargs):
        data = self.data
        if method == 'state.get':
            return dict(values=copy.deepcopy({k: v for k, v in data['values'].items() if k in body.get('keys', data['values'])}), revision=data['revision'], session='offline')
        if method == 'state.receipt':
            return copy.deepcopy(data['commits'].get(body['commit_id'], dict(commit_id=body['commit_id'], found=False)))
        if method == 'operations.get':
            return copy.deepcopy(data['operations'][body['operation_id']])
        assert method in ('state.commit', 'events.commit'), method
        if body['expected_state_revision'] != data['revision']:
            raise IPCRejected('revision_conflict', 'request', False)
        for mutation in body['mutations']:
            data['values'][mutation['key']] = copy.deepcopy(mutation['value'])
        operation_ids = []
        for intent in body['actions']:
            operation_id = 'op_' + str(len(data['operations']) + 1)
            data['operations'][operation_id] = dict(operation_id=operation_id, state='accepted', can_retry=False, result=None, intent=intent)
            operation_ids.append(operation_id)
        data['revision'] += 1
        receipt = dict(commit_id=body['commit_id'], found=True, revision=data['revision'], operation_ids=operation_ids)
        data['commits'][body['commit_id']] = receipt
        self.save()
        if self.lose_reply:
            self.lose_reply = False
            raise TimeoutError()
        return dict(receipt, duplicate=False)

    def save(self):
        self.path.write_text(json.dumps(self.data))

    def finish(self, operation_id, state):
        self.data['operations'][operation_id].update(state=state, result=dict(state=state, reason='offline_fixture'))
        self.save()


def context(peer):
    storage = Storage(peer)
    operations = Operations(peer, 'offline')
    return SimpleNamespace(config=Settings(), storage=storage, business=Business(storage), operations=operations, commands=Commands(operations))


async def check(path):
    player = dict(uuid='00000000-0000-0000-0000-000000000001', name='Offline Player')
    peer = Peer(path)
    ctx = context(peer)
    balance = 'points/' + player['uuid']
    for index, state in enumerate(('succeeded', 'failed', 'unknown'), 1):
        request_id = state
        receipt = await redeem(ctx, player, request_id)
        operation_id, = receipt['operation_ids']
        assert (await inspect(ctx, business_id(player['uuid'], request_id)))['status'] == 'pending'
        peer.finish(operation_id, state)
        result = await inspect(ctx, business_id(player['uuid'], request_id))
        assert result['status'] == state and result['operation']['result']['state'] == state
        # Recreate peer + all SDK objects from persisted state, then repeat purchase.
        peer = Peer(path)
        ctx = context(peer)
        again = await redeem(ctx, dict(player, name='Renamed Player'), request_id)
        assert again == receipt
        assert len(peer.data['operations']) == index
        assert peer.data['values'][balance] == 100 - 10 * index
        assert (await inspect(ctx, business_id(player['uuid'], request_id)))['status'] == state
    # A lost commit reply must not cause a second debit or reward on redelivery.
    peer.lose_reply = True
    try:
        await redeem(ctx, player, 'lost_reply')
        raise AssertionError('expected unresolved commit')
    except CommitUncertain:
        pass
    peer = Peer(path)
    ctx = context(peer)
    await redeem(ctx, player, 'lost_reply')
    assert len(peer.data['operations']) == 4 and peer.data['values'][balance] == 60
    await ctx.storage.set(balance, 0)
    receipt = await redeem(ctx, player, 'empty')
    assert receipt['result']['status'] == 'insufficient_points' and not receipt['operation_ids']
    assert len(peer.data['operations']) == 4
    # Exercise the real event handler: mismatched XUID is ACKed without purchase.
    async def players():
        return [dict(player, xuid='123')]
    ctx.players = players
    ctx.config = Settings(player_uuids=[player['uuid']])
    plugin = PointsExchange()
    await plugin.on_start(ctx)
    payload = dict(kind='chat.received', subscription_id='chat', event_id='chat_1',
                   delivery_token='delivery_1', payload=dict(player=player['name'], xuid='456', message='!兑换 event01'))
    await plugin.on_event(ctx, Event(ctx, payload))
    assert len(peer.data['operations']) == 4
    await ctx.storage.set(balance, 100)
    payload['payload']['xuid'] = '123'
    for token in ('delivery_2', 'delivery_3'):
        payload['delivery_token'] = token
        await plugin.on_event(ctx, Event(ctx, payload))
    rewards = [o for o in peer.data['operations'].values() if o['intent']['arguments']['cmd'].startswith('give ')]
    assert len(rewards) == 5 and peer.data['values'][balance] == 90
    print('PASS: normal/failed/unknown, persisted duplicate after restart, lost commit reply, insufficient points, event identity/ACK; no replay/refund')


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='points-exchange-') as folder:
        asyncio.run(check(Path(folder) / 'fixture.json'))
