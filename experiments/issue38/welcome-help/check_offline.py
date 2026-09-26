"""Direct offline exercise with real SDK transactions and an in-memory IPC peer."""
import asyncio
import copy
import json
import logging
from types import SimpleNamespace

from neomega_runtime import Event, PlayerNotFound
from neomega_runtime.notifications import Notifications
from neomega_runtime.operations import Operations
from neomega_runtime.storage import Storage
from main import WelcomeHelp, WELCOME, HELP


class Peer:
    def __init__(self):
        self.values, self.actions, self.acks = {}, [], []
        self.revision = 0

    async def call(self, method, body, **kwargs):
        if method == 'state.get':
            return dict(revision=self.revision, values=copy.deepcopy(self.values), session='offline')
        assert method == 'events.commit', method
        assert body['expected_state_revision'] == self.revision
        for mutation in body['mutations']:
            self.values[mutation['key']] = mutation['value']
        first = len(self.actions)
        self.actions.extend(body['actions'])
        self.acks.append(body['delivery_token'])
        self.revision += 1
        return dict(commit_id=body['commit_id'], revision=self.revision, duplicate=False,
                    operation_ids=[str(i) for i in range(first, len(self.actions))])


async def check():
    peer = Peer()
    operations = Operations(peer, 'offline')
    states = []

    async def wait(operation_id, **kwargs):
        states.append(operation_id)
        return {'state': 'unknown'}  # Still must not resubmit uncertain sends.

    operations.wait = wait

    async def resolve_player(name, xuid):
        if name == 'left':
            raise PlayerNotFound('left')
        assert name == 'Alex'
        return {'name': name, 'xuid': 'offline-xuid'}

    ctx = SimpleNamespace(storage=Storage(peer), operations=operations,
                          notifications=Notifications(operations), resolve_player=resolve_player,
                          log=logging.getLogger('offline'))
    def event(kind, event_id, token, **payload):
        return Event(ctx, dict(kind=kind, event_id=event_id, delivery_token=token,
                               subscription_id='offline-sub', payload=payload))

    plugin = WelcomeHelp()
    await plugin.on_event(ctx, event('player.list_added', 'join1', 'd1', name='Alex'))
    await plugin.on_event(ctx, event('chat.received', 'help1', 'd2', player='Alex', message='!帮助'))
    before = len(peer.actions)
    assert before == 2
    # New plugin object models process-memory loss; durable state survives.
    plugin = WelcomeHelp()
    await plugin.on_event(ctx, event('player.list_added', 'join1', 'd3', name='Alex'))
    await plugin.on_event(ctx, event('chat.received', 'help1', 'd4', player='Alex', message='!帮助'))
    for i, message in enumerate(('你好', '!help', '!帮助 extra', ' !帮助')):
        await plugin.on_event(ctx, event('chat.received', 'other'+str(i), 'o'+str(i), player='Alex', message=message))
    await plugin.on_event(ctx, event('player.list_added', 'stale', 's1', name='left'))
    assert len(peer.actions) == before and len(peer.acks) == 9
    messages = [json.loads(a['arguments']['cmd'].split('] ', 1)[1])['rawtext'][0]['text']
                for a in peer.actions]
    assert messages == [WELCOME, HELP] and len(messages[1].splitlines()) == 3
    # Concurrent redelivery of a genuinely new join yields one additional send.
    await asyncio.gather(*(plugin.on_event(ctx, event('player.list_added', 'join2', 'c'+str(i), name='Alex'))
                           for i in range(2)))
    assert len(peer.actions) == 3 and len(states) == 3
    print('PASS: welcome, exact 3-line help, unrelated chat, stale player, restart/redelivery/concurrent dedup, unknown no-retry')


if __name__ == '__main__':
    asyncio.run(check())
