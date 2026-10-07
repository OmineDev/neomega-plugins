"""Durable display intents; the existing Host owns admission and execution.

No command is submitted outside a state transaction. A lost commit response
leaves its ID in the object row, allowing read-only reconciliation after restart.
"""
import asyncio
import copy
import hashlib
import math
import re
import uuid
from datetime import datetime, timedelta, timezone

from neomega_runtime import ServiceRejected
from neomega_runtime.business import BusinessConflict
from neomega_runtime.storage import CommitUncertain

META = 'display/meta'
PREFIX = 'display/object/'
SPEC_FIELDS = {'profile', 'item', 'anchor', 'scale', 'motion', 'ttl_seconds'}
DEFAULTS = {'max_objects': 8, 'max_owner_objects': 4, 'max_ttl_seconds': 60,
            'max_requests_per_minute': 60, 'max_retained_objects': 128,
            'max_retained_requests': 4096, 'max_revisions': 8,
            'tick_seconds': 0.25}


def utcnow():
    return datetime.now(timezone.utc)


def stamp(value):
    return value.isoformat()


def instant(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def reject(code):
    raise ServiceRejected(code)


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,96}', value):
        reject('invalid_identifier')
    return value


def owner(call):
    if not call or not call.installation_id or type(call.generation) is not int:
        reject('service_context_required')
    return {'installation_id': call.installation_id, 'generation': call.generation}


def business_id(call, request_id):
    raw = f'{call.installation_id}\0{call.generation}\0{identifier(request_id)}'
    return 'display_' + hashlib.sha256(raw.encode()).hexdigest()


class AdmissionLock:
    """One writer boundary shared by objects, channels and maintenance."""
    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = False
        self.token = None
        self.uncertain = set()

    def locked(self):
        return self.lock.locked()

    async def __aenter__(self):
        await self.lock.acquire()
        return self

    async def __aexit__(self, kind, value, traceback):
        if kind is not None and issubclass(kind, (CommitUncertain, asyncio.CancelledError)):
            intent = getattr(value, 'intent', None)
            if intent is not None:
                self.uncertain.add(intent.payload()['commit_id'])
        self.lock.release()

    def admit(self):
        if self.sealed:
            reject('busy')


class Provider:
    def __init__(self, ctx, profile, *, clock=utcnow, limits=None):
        self.ctx, self.profile, self.clock = ctx, profile, clock
        self.limits = dict(DEFAULTS, **(limits or {}))
        self.lock = AdmissionLock()
        self.active_ids = None

    async def describe(self):
        return dict(self.profile.describe(), limits=self.limits,
                    visibility='world', client_visible=None,
                    channels={'kinds': ['actionbar', 'title'], 'max_slots': 32,
                              'max_leases': 128, 'max_owner_leases': 16, 'max_ttl_seconds': 60,
                              'priority': 'highest_first_fifo_ties', 'max_commands_per_tick': 8},
                    ttl_origin='trusted_service_accepted_at',
                    generation_policy='temporary_no_takeover',
                    refresh_policy='initialization_and_explicit_update_only')

    def validate_spec(self, args):
        spec = {key: args[key] for key in SPEC_FIELDS if key in args}
        if set(spec) != SPEC_FIELDS:
            reject('invalid_spec')
        self.ttl(spec['ttl_seconds'])
        try:
            return self.profile.validate(spec)
        except (TypeError, ValueError, KeyError) as error:
            raise ServiceRejected('invalid_spec') from error

    def ttl(self, value):
        if type(value) not in (int, float) or not math.isfinite(value) or not 1 <= value <= self.limits['max_ttl_seconds']:
            reject('invalid_ttl')
        return value

    def public(self, row):
        value = {k: copy.deepcopy(v) for k, v in row.items()
                 if k not in {'plan', 'cursor', 'pending', 'steps'}}
        # Service replies have a 64 KiB budget, including consumer wrappers that
        # repeat this payload in their transport receipt. Completed command
        # intents remain in storage; operation IDs retain the original evidence.
        value['steps'] = [{k: copy.deepcopy(step[k]) for k in
                           ('operation_id', 'commit_id', 'state', 'is_success', 'reason', 'evidence')
                           if k in step} for step in row.get('steps', [])]
        value['inflight'] = copy.deepcopy(row.get('pending'))
        return value

    async def get(self, args, call):
        who = owner(call)
        if set(args) == {'request_id'}:
            receipt = await self.ctx.business.get(business_id(call, args['request_id']))
            return {'found': False} if receipt is None else receipt['result']
        if set(args) != {'object_id'}:
            reject('invalid_request')
        row = await self.ctx.storage.get(PREFIX + identifier(args['object_id']))
        if row is None or row['owner'] != who:
            return {'found': False}
        return {'found': True, 'object': self.public(row)}

    async def mutate(self, kind, args, call):
        async with self.lock:
            self.lock.admit()
            return await self._mutate(kind, args, call)

    async def _mutate(self, kind, args, call):
        who, now = owner(call), self.clock()
        if call.deadline <= now:
            reject('service_deadline_expired')
        expected = {'request_id', 'object_key'} | SPEC_FIELDS if kind == 'create' else {'request_id', 'object_id', 'revision'}
        if kind == 'update':
            expected |= SPEC_FIELDS
        elif kind == 'renew':
            expected |= {'ttl_seconds'}
        if set(args) != expected:
            reject('invalid_request')
        request_id = identifier(args['request_id'])
        bid = business_id(call, request_id)
        oid = uuid.uuid4().hex if kind == 'create' else identifier(args['object_id'])
        key = PREFIX + oid
        try:
            tx = await self.ctx.business.begin(bid, {'kind': kind, 'arguments': args}, keys=[META, key])
        except BusinessConflict:
            reject('request_id_conflict')
        if tx.receipt is not None:
            return tx.receipt['result']
        meta = tx.state.get(META, {'objects': [], 'requests': 0, 'rates': {}})
        if meta['requests'] >= self.limits['max_retained_requests'] or (kind != 'close' and meta['requests'] >= self.limits['max_retained_requests'] - self.limits['max_retained_objects']):
            reject('request_ledger_full')
        meta['rates'] = {k: [v for v in values if v > now.timestamp() - 60]
                         for k, values in meta['rates'].items()
                         if any(v > now.timestamp() - 60 for v in values)}
        rate_key = hashlib.sha256(str(who).encode()).hexdigest()
        rate = [v for v in meta['rates'].get(rate_key, []) if v > now.timestamp() - 60]
        if len(rate) >= self.limits['max_requests_per_minute'] and kind != 'close':
            reject('rate_limit')
        spec = self.validate_spec(args) if kind in ('create', 'update') else None
        if kind == 'create':
            object_key = identifier(args['object_key'])
            if len(meta['objects']) >= self.limits['max_retained_objects']:
                reject('object_ledger_full')
            rows = [await self.ctx.storage.get(PREFIX + value) for value in meta['objects']]
            if any(r['owner'] == who and r['object_key'] == object_key
                   and r['physical_state'] != 'closed' for r in rows):
                reject('object_key_conflict')
            active = [r for r in rows if r['physical_state'] != 'closed']
            if len(active) >= self.limits['max_objects'] or sum(r['owner'] == who for r in active) >= self.limits['max_owner_objects']:
                reject('object_quota')
            expires = call.accepted_at + timedelta(seconds=spec['ttl_seconds'])
            if expires <= now:
                reject('lease_expired')
            carrier = 'nd_' + oid
            row = dict(object_id=oid, object_key=object_key, owner=who,
                       session=self.ctx.operations.session, carrier=carrier,
                       revision=1, spec=spec, expires_at=stamp(expires),
                       desired_state='present', physical_state='queued',
                       cleanup_pending=False, client_visible=None,
                       operation_ids=[], steps=[], cursor=0, pending=None,
                       plan=self.profile.create_commands(carrier, spec))
            meta['objects'].append(oid)
        else:
            row = tx.state.get(key)
            if row is None or row['owner'] != who:
                reject('object_not_found')
            if type(args['revision']) is not int or args['revision'] != row['revision']:
                reject('revision_conflict')
            if row['physical_state'] == 'unknown':
                reject('object_frozen_unknown')
            if row['session'] != self.ctx.operations.session:
                reject('session_changed')
            if row['pending'] or row['plan']:
                reject('object_busy')
            if kind != 'close' and (row['desired_state'] != 'present' or instant(row['expires_at']) <= now):
                reject('lease_expired')
            if kind != 'close' and row['revision'] >= self.limits['max_revisions']:
                reject('revision_limit')
            row['revision'] += 1
            if kind in ('update', 'renew'):
                ttl = spec['ttl_seconds'] if spec else self.ttl(args['ttl_seconds'])
                expires = call.accepted_at + timedelta(seconds=ttl)
                if expires <= now:
                    reject('lease_expired')
                row['expires_at'] = stamp(expires)
            if kind == 'update':
                if spec['anchor']['dimension'] != row['spec']['anchor']['dimension']:
                    reject('dimension_change_unsupported')
                row['spec'] = spec
                row['plan'] = self.profile.update_commands(row['carrier'], spec)
                row['cursor'], row['physical_state'] = 0, 'queued'
            elif kind == 'renew':
                row['plan'] = self.profile.renew_commands(row['carrier'], row['spec'])
                row['cursor'], row['physical_state'] = 0, 'queued'
            elif kind == 'close' and row['physical_state'] != 'closed':
                self.close_intent(row)
        meta['requests'] += 1
        meta['rates'][rate_key] = (rate + [now.timestamp()])[-self.limits['max_requests_per_minute']:]
        row['last_request_id'] = request_id
        row['last_call_id'] = call.call_id
        row['last_accepted_at'] = stamp(call.accepted_at)
        tx.state.set(key, row)
        tx.state.set(META, meta)
        result = {'object': self.public(row), 'request': dict(request_id=request_id,
                  call_id=call.call_id, accepted_at=stamp(call.accepted_at), commit_id=tx.state.commit_id)}
        try:
            saved = (await tx.commit(result))['result']
        except (CommitUncertain, asyncio.CancelledError):
            # A durable create may exist despite a lost reply. Rebuild from the
            # original ledger; do not allocate or submit another business intent.
            self.active_ids = None
            raise
        if self.active_ids is not None and row['physical_state'] not in ('closed', 'unknown', 'session_changed', 'cleanup_failed'):
            self.active_ids.add(oid)
        return saved

    def close_intent(self, row):
        row['desired_state'], row['cleanup_pending'] = 'closed', True
        if row['physical_state'] == 'unknown' or row['pending']:
            return
        # Never-created queued objects need no physical delete.
        if not row['steps']:
            row.update(physical_state='closed', cleanup_pending=False, plan=[], cursor=0)
        else:
            row.update(plan=self.profile.close_commands(row['carrier'], row['spec']), cursor=0,
                       physical_state='cleanup_pending')

    async def recover(self):
        async with self.lock:
            if self.lock.sealed:
                return
            meta = await self.ctx.storage.get(META, {'objects': []})
            for oid in meta['objects']:
                tx = await self.ctx.storage.transaction(keys=[PREFIX + oid])
                row = tx.get(PREFIX + oid)
                if row['physical_state'] != 'closed':
                    self.close_intent(row)
                    if row['session'] != self.ctx.operations.session:
                        row['physical_state'] = 'session_changed'
                        row['cleanup_pending'] = True
                    tx.set(PREFIX + oid, row)
                    await tx.save()

    async def tick(self):
        async with self.lock:
            if self.lock.sealed:
                return
            if self.active_ids is None:
                meta = await self.ctx.storage.get(META, {'objects': []})
                self.active_ids = set(meta['objects'])
            for oid in tuple(self.active_ids):
                await self.advance(oid)

    async def advance(self, oid):
        key = PREFIX + oid
        tx = await self.ctx.storage.transaction(keys=[key])
        row = tx.get(key)
        if row['physical_state'] in ('closed', 'unknown', 'session_changed', 'cleanup_failed'):
            if self.active_ids is not None:
                self.active_ids.discard(oid)
            return
        if row['desired_state'] != 'closed' and instant(row['expires_at']) <= self.clock():
            self.close_intent(row)
        if row['pending']:
            pending = row['pending']
            receipt = await self.ctx.storage.receipt(pending['commit_id'])
            if receipt is None or len(receipt['operation_ids']) != 1:
                row['physical_state'] = 'unknown'
                row['cleanup_pending'] = True
            else:
                opid = receipt['operation_ids'][0]
                op = await self.ctx.operations.get(opid)
                if opid not in row['operation_ids']:
                    row['operation_ids'].append(opid)
                if op['state'] not in ('succeeded', 'failed', 'unknown', 'cancelled', 'expired', 'rejected'):
                    tx.set(key, row)
                    await tx.save()
                    return
                parsed = self.ctx.commands.result(op)
                row['steps'].append({'operation_id': opid, 'state': op['state'],
                    'is_success': parsed.is_success, 'evidence': parsed.evidence,
                    'reason': parsed.reason, 'command': pending['command'],
                    'commit_id': pending['commit_id'], 'intent': pending['intent']})
                row['pending'] = None
                if op['state'] == 'unknown' or (op['state'] == 'succeeded' and not parsed.is_success):
                    row['physical_state'], row['cleanup_pending'] = 'unknown', True
                elif not parsed.is_success:
                    if pending['cleanup']:
                        row.update(physical_state='cleanup_failed', cleanup_pending=True, plan=[])
                    else:
                        self.close_intent(row)
                else:
                    row['cursor'] += 1
                    if row['desired_state'] == 'closed' and not pending['cleanup']:
                        self.close_intent(row)
                    elif row['cursor'] >= len(row['plan']):
                        row.update(plan=[], cursor=0, physical_state='closed' if pending['cleanup'] else 'succeeded', cleanup_pending=False)
            tx.set(key, row)
            await tx.save()
            return
        if row['session'] != self.ctx.operations.session:
            row.update(physical_state='session_changed', cleanup_pending=True)
        elif row['plan']:
            cleanup = row['desired_state'] == 'closed'
            deadline = self.clock() + timedelta(seconds=10)
            if not cleanup:
                deadline = min(deadline, instant(row['expires_at']))
            command = row['plan'][row['cursor']]
            intent = self.ctx.commands.prepare(command, idempotency_key='display_' + tx.commit_id,
                                               deadline=stamp(deadline), timeout=5)
            row['pending'] = {'commit_id': tx.commit_id, 'command': command,
                              'cleanup': cleanup, 'intent': intent.payload()}
            row['physical_state'] = 'inflight'
            tx.action(intent)
        elif row['physical_state'] not in ('closed', 'cleanup_pending'):
            return
        tx.set(key, row)
        try:
            await tx.save()
        except CommitUncertain:
            # Worker fails conservatively. Recovery reads the original commit,
            # never sends another command to guess whether this one happened.
            raise
