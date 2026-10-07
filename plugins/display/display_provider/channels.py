"""Shared ephemeral screen leases, with durable command receipts and ownership."""
import asyncio
import copy
import hashlib
import json
import math
import uuid
from datetime import timedelta

from neomega_runtime.players import player_target

from .provider import owner, identifier, reject, utcnow, instant, stamp

KEY = 'display/channels/v1'
TERMINAL = {'succeeded', 'failed', 'unknown', 'cancelled', 'expired', 'rejected'}


class Channels:
    def __init__(self, ctx, *, clock=utcnow):
        self.ctx, self.clock = ctx, clock
        self.lock = asyncio.Lock()

    def initial(self):
        return dict(session=self.ctx.operations.session, leases={}, slots={}, requests={}, rates={}, sequence=0)

    @staticmethod
    def ttl(value):
        if type(value) not in (int, float) or not math.isfinite(value) or not 1 <= value <= 60:
            reject('invalid_ttl')
        return value

    async def call(self, method, args, call):
        who = owner(call)
        request_key = None
        if method != 'channel_get':
            request = identifier(args['request_id'])
            request_key = KEY + '/request/' + hashlib.sha256(json.dumps([who, request], sort_keys=True).encode()).hexdigest()
        async with self.lock:
            tx = await self.ctx.storage.transaction(keys=[KEY] + ([request_key] if request_key else []))
            state = tx.get(KEY, self.initial())
            now = self.clock()
            if method == 'channel_get':
                lease = state['leases'].get(identifier(args['lease_id']))
                if not lease or lease['owner'] != who:
                    return {'found': False}
                slot = state['slots'].get(json.dumps([lease['player'], lease['channel']]), {})
                return {'found': True, 'lease': copy.deepcopy(lease),
                        'selected': slot.get('shown') == lease['lease_id'],
                        'delivery': 'unknown' if slot.get('frozen') else ('pending' if slot.get('pending') else 'settled'),
                        'evidence': copy.deepcopy(slot.get('evidence')), 'client_visible': None}
            if call.deadline <= now:
                reject('service_deadline_expired')
            if state['session'] != self.ctx.operations.session:
                reject('session_changed')
            self.collect(state, tx, now)
            signature = hashlib.sha256(json.dumps([method, args], sort_keys=True, allow_nan=False).encode()).hexdigest()
            previous = tx.get(request_key) if request_key in state['requests'] else None
            if previous:
                if previous['signature'] != signature:
                    reject('request_id_conflict')
                return previous['result']
            if len(state['requests']) >= 512 and method != 'channel_release':
                reject('request_ledger_full')
            # Closed leases no longer occupy the active quota; their receipts stay queryable.
            active = [r for r in state['leases'].values() if r['state'] == 'active' and instant(r['expires_at']) > now]
            rate_key = json.dumps(who, sort_keys=True)
            rates = [v for v in state['rates'].get(rate_key, []) if v > now.timestamp() - 60]
            if len(rates) >= 120 and method != 'channel_release':
                reject('rate_limit')
            if method == 'channel_acquire':
                if set(args) != {'request_id', 'player', 'channel', 'text', 'priority', 'ttl_seconds'}:
                    reject('invalid_request')
                player, channel, text, priority = (args[k] for k in ('player', 'channel', 'text', 'priority'))
                try:
                    player_target(player)
                except ValueError:
                    reject('invalid_player')
                if channel not in ('actionbar', 'title') or not isinstance(text, str) or len(text.encode('utf-8')) > 2048:
                    reject('invalid_channel_content')
                if type(priority) is not int or not 0 <= priority <= 100:
                    reject('invalid_priority')
                if len(set(state['slots']) | {json.dumps([r['player'], r['channel']]) for r in active} | {json.dumps([player, channel])}) > 32:
                    reject('channel_slot_quota')
                if len(active) >= 128 or sum(r['owner'] == who for r in active) >= 16:
                    reject('channel_quota')
                if len(state['leases']) >= 4096:
                    reject('lease_ledger_full')
                expires = call.accepted_at + timedelta(seconds=self.ttl(args['ttl_seconds']))
                if expires <= now:
                    reject('lease_expired')
                state['sequence'] += 1
                lease = dict(lease_id=uuid.uuid4().hex, owner=who, player=player, channel=channel,
                             text=text, priority=priority, sequence=state['sequence'], revision=1,
                             state='active', expires_at=stamp(expires))
                state['leases'][lease['lease_id']] = lease
            else:
                expected = {'request_id', 'lease_id', 'revision'} | ({'ttl_seconds'} if method == 'channel_renew' else set())
                if method not in ('channel_renew', 'channel_release') or set(args) != expected:
                    reject('invalid_request')
                lease = state['leases'].get(identifier(args['lease_id']))
                if not lease or lease['owner'] != who:
                    reject('lease_not_found')
                if type(args['revision']) is not int or args['revision'] != lease['revision']:
                    reject('revision_conflict')
                if method == 'channel_renew':
                    if lease['state'] != 'active' or instant(lease['expires_at']) <= now:
                        reject('lease_expired')
                    expires = call.accepted_at + timedelta(seconds=self.ttl(args['ttl_seconds']))
                    if expires <= now:
                        reject('lease_expired')
                    lease['expires_at'] = stamp(expires)
                else:
                    if lease['state'] == 'released':
                        reject('lease_closed')
                    lease['state'] = 'released'
                lease['revision'] += 1
            state['rates'][rate_key] = (rates + [now.timestamp()])[-120:]
            result = {'lease': copy.deepcopy(lease), 'delivery': 'queued', 'client_visible': None}
            # Release remains available after the acquisition budget is exhausted.
            state['requests'][request_key] = dict(expires=now.timestamp() + 300, lease_id=lease['lease_id'])
            # Leave room under the Host default 256 KiB commit boundary for cleanup/actions.
            if method != 'channel_release' and len(json.dumps(state, ensure_ascii=True).encode()) > 160 * 1024:
                reject('channel_state_budget')
            tx.set(request_key, dict(signature=signature, result=result))
            tx.set(KEY, state)
            await tx.save()
            return result

    @staticmethod
    def collect(state, tx, now):
        retained = set()
        frozen = set()
        for slot in state['slots'].values():
            if slot.get('frozen'):
                frozen.update(v for v in (slot.get('shown'), slot.get('frozen_lease')) if v)
            retained.update(v for v in (slot.get('shown'), slot.get('frozen_lease'), (slot.get('pending') or {}).get('lease_id')) if v)
        for lease_id, lease in list(state['leases'].items()):
            if lease['state'] == 'active' and instant(lease['expires_at']) <= now:
                lease['state'] = 'expired'
            if lease['state'] != 'active':
                lease.setdefault('terminal_at', now.timestamp())
                if lease_id not in retained:
                    lease.pop('text', None)
                    if lease['terminal_at'] + 300 <= now.timestamp():
                        del state['leases'][lease_id]
        for key, receipt in list(state['requests'].items()):
            if receipt['expires'] <= now.timestamp() and receipt['lease_id'] not in frozen:
                tx.delete(key)
                del state['requests'][key]
        state['rates'] = {key: fresh for key, values in state['rates'].items()
                          if (fresh := [v for v in values if v > now.timestamp() - 60])}
        for key, slot in list(state['slots'].items()):
            if not slot['frozen'] and not slot['pending'] and not slot['shown']:
                del state['slots'][key]

    async def recover(self):
        async with self.lock:
            tx = await self.ctx.storage.transaction(keys=[KEY])
            state = tx.get(KEY, self.initial())
            for lease in state['leases'].values():
                if lease['state'] == 'active':
                    lease['state'] = 'provider_stopped'
            if state['session'] != self.ctx.operations.session:
                # Never clear a screen belonging to a different game session.
                state['slots'] = {}
                state['session'] = self.ctx.operations.session
            tx.set(KEY, state)
            await tx.save()

    async def tick(self):
        async with self.lock:
            tx = await self.ctx.storage.transaction(keys=[KEY])
            state = tx.get(KEY, self.initial())
            if state['session'] != self.ctx.operations.session:
                return
            now = self.clock()
            winners = {}
            for lease in state['leases'].values():
                if lease['state'] == 'active' and instant(lease['expires_at']) <= now:
                    lease['state'] = 'expired'
                if lease['state'] != 'active':
                    continue
                key = json.dumps([lease['player'], lease['channel']])
                winner = winners.get(key)
                if winner is None or (-lease['priority'], lease['sequence']) < (-winner['priority'], winner['sequence']):
                    winners[key] = lease
            submitted = 0
            for key in sorted(set(state['slots']) | set(winners), key=lambda k: state['slots'].get(k, {}).get('refreshed', 0)):
                slot = state['slots'].setdefault(key, dict(shown=None, pending=None, frozen=False, refreshed=0))
                if slot['pending']:
                    receipt = await self.ctx.storage.receipt(slot['pending']['commit_id'])
                    if receipt is None or len(receipt['operation_ids']) <= slot['pending']['action_index']:
                        slot['frozen'] = True
                        slot['frozen_lease'] = slot['pending']['lease_id']
                        slot['pending'] = None
                    else:
                        operation_id = receipt['operation_ids'][slot['pending']['action_index']]
                        operation = await self.ctx.operations.get(operation_id)
                        if operation['state'] not in TERMINAL:
                            continue
                        parsed = self.ctx.commands.result(operation)
                        slot['evidence'] = dict(operation_id=operation_id, state=operation['state'], reason=parsed.reason)
                        slot['frozen'] = operation['state'] == 'unknown' or (operation['state'] == 'succeeded' and parsed.output is None)
                        if slot['frozen']:
                            slot['frozen_lease'] = slot['pending']['lease_id']
                        if parsed.is_success:
                            slot['shown'] = slot['pending']['lease_id']
                            slot['refreshed'] = now.timestamp()
                        elif not slot['frozen']:
                            failed_id = slot['pending']['lease_id']
                            if failed_id is not None:
                                failed = state['leases'][failed_id]
                                failed['state'] = 'delivery_failed'
                                failed['evidence'] = copy.deepcopy(slot['evidence'])
                            else:
                                # A definitely failed clear is not an uncertain mutation.
                                prior = state['leases'].get(slot['shown'])
                                if prior is not None:
                                    prior['clear_evidence'] = copy.deepcopy(slot['evidence'])
                                slot['shown'] = None
                        slot['pending'] = None
                if slot['frozen'] or slot['pending']:
                    continue
                winner = winners.get(key)
                if winner is not None and winner['state'] != 'active':
                    winner = None
                selected = winner['lease_id'] if winner else None
                # Actionbar/title naturally time out: refresh while the lease lives.
                if selected == slot['shown'] and (selected is None or now.timestamp() - slot['refreshed'] < 1):
                    continue
                if submitted >= 8:
                    continue
                player, channel = json.loads(key)
                selector = player_target(player)
                raw = json.dumps({'rawtext': [{'text': winner['text'] if winner else ''}]}, ensure_ascii=False)
                command = f'titleraw {selector} {channel} {raw}'
                # Action indices match the ordered operation IDs in the durable commit receipt.
                intent = self.ctx.commands.prepare(command, idempotency_key='channel_' + tx.commit_id + '_' + str(submitted),
                    deadline=stamp(now + timedelta(seconds=5)), timeout=5)
                slot['pending'] = dict(commit_id=tx.commit_id, action_index=submitted, lease_id=selected)
                tx.action(intent)
                submitted += 1
            self.collect(state, tx, now)
            tx.set(KEY, state)
            await tx.save()
