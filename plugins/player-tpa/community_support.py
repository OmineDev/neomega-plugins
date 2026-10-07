"""Shared native-plugin primitives, maintained here and explicitly synchronized.

No storage writes, event acknowledgements or world-action submissions live here.
Roster callbacks are ordered and backpressure polling; keep them short. Read
receipts are evidence, never authorization to retry an uncertain world action.
"""
import asyncio
import copy
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from neomega_runtime.player_state import PlayerState
from neomega_runtime.players import player_target


@dataclass(frozen=True)
class Identity:
    uuid: str
    name: str
    xuid: str
    entity_unique_id: int
    epoch: str
    revision: int


class IdentityUnavailable(ValueError):
    """No unique, current observed identity authorizes this request."""


class ObservedRoster:
    """One supervised poll owner; no name fallback or invented join events.

    start installs the initial snapshot and awaits its callback before returning.
    close drains any in-flight poll, then closes the stream without cancellation.
    Callback/source failures invalidate all identities and fail the owned task.
    A failed instance cannot restart; construct a fresh roster instead.
    """

    def __init__(self, *, on_observation=None):
        self._callback = on_observation
        self._state = None
        self._rows = ()
        self._stream = None
        self._task = None
        self._stopping = False
        self._started = False
        self._healthy = False
        self._last_confirmed_ns = 0

    @property
    def last_confirmed_ns(self):
        return self._last_confirmed_ns

    async def _notify(self, healthy):
        stamp = time.monotonic_ns()
        wall = datetime.now(timezone.utc)
        if healthy:
            self._last_confirmed_ns = stamp
        if self._callback is not None:
            await self._callback(self._rows, stamp, wall, healthy)

    def _project(self):
        snapshot = self._state.snapshot()
        rows = []
        seen_uuid, seen_name, seen_xuid, seen_entity = set(), set(), set(), set()
        for player in snapshot['players']:
            entry = player['list']
            uuid, name, xuid = entry['uuid'], entry['username'], entry['xuid']
            unique = entry['entity_unique_id']
            # Empty XUID is observed data, not authorization. Preserve it for
            # online accounting; resolve_chat/require_current reject it.
            if not uuid or not name:
                raise IdentityUnavailable('empty observed UUID or player name')
            if (uuid in seen_uuid or name in seen_name or unique in seen_entity
                    or (xuid and xuid in seen_xuid)):
                raise IdentityUnavailable('conflicting observed player identity')
            seen_uuid.add(uuid)
            seen_name.add(name)
            seen_entity.add(unique)
            if xuid:
                seen_xuid.add(xuid)
            rows.append(Identity(uuid, name, xuid, unique,
                                 snapshot['epoch'], snapshot['revision']))
        self._rows = tuple(rows)
        self._healthy = True

    async def _invalidate(self):
        self._healthy = False
        self._rows = ()
        self._state = None
        await self._notify(False)

    async def start(self, ctx):
        if self._started:
            raise RuntimeError('roster already started')
        self._started = True
        try:
            self._stream, snapshot = await ctx.observe_players()
            self._state = PlayerState(snapshot)
            self._project()
            await self._notify(True)
            self._task = ctx.spawn(self._run(), name='community-player-observation')
        except BaseException:
            try:
                await self._invalidate()
            finally:
                if self._stream is not None:
                    await self._stream.close()
            raise

    async def _run(self):
        try:
            while not self._stopping:
                # Shield only the bounded IPC read so worker cancellation does
                # not abandon its admitted response slot. Drain before retiring.
                read = asyncio.create_task(self._stream.poll())
                interrupted = False
                while not read.done():
                    try:
                        await asyncio.shield(read)
                    except asyncio.CancelledError:
                        interrupted = True
                    except Exception:
                        break
                update = read.result()
                if interrupted:
                    raise asyncio.CancelledError
                if update is not None:
                    self._state.apply(update, epoch=self._state.epoch)
                    self._project()
                await self._notify(True)
                await asyncio.sleep(0)
        except BaseException:
            await self._invalidate()
            raise
        finally:
            try:
                await self._stream.close()
            finally:
                self._healthy = False
                self._rows = ()
                self._state = None

    async def close(self):
        self._stopping = True
        # Worker drain cancels and joins owned tasks before on_drain. A task
        # already cancelled by that lifecycle is not cancellation of this caller.
        # Shield active tasks; caller cancellation still propagates unchanged.
        try:
            if self._task is not None:
                if not self._task.done():
                    await asyncio.shield(self._task)
                elif not self._task.cancelled():
                    self._task.result()
        finally:
            # A child cancelled before its first instruction has no finally
            # execution. Clear projection and explicitly release its stream here.
            if self._task is None or self._task.done():
                self._healthy = False
                self._rows = ()
                self._state = None
                if self._stream is not None:
                    await self._stream.close()

    def snapshot(self):
        if not self._healthy:
            raise IdentityUnavailable('player observation unavailable')
        return self._rows

    def resolve_chat(self, payload):
        name, xuid = payload.get('player'), payload.get('xuid')
        if not isinstance(name, str) or not name or not isinstance(xuid, str) or not xuid:
            raise IdentityUnavailable('chat requires a reliable nonempty XUID')
        matches = [row for row in self.snapshot() if row.xuid == xuid]
        if len(matches) != 1 or matches[0].name != name:
            raise IdentityUnavailable('chat does not match a unique current player')
        return self.require_current(matches[0])

    def require_current(self, identity):
        matches = [row for row in self.snapshot() if row.uuid == identity.uuid]
        if len(matches) != 1:
            raise IdentityUnavailable('player no longer observed')
        current = matches[0]
        if (not current.xuid or (current.name, current.xuid, current.entity_unique_id, current.epoch)
                != (identity.name, identity.xuid, identity.entity_unique_id, identity.epoch)):
            raise IdentityUnavailable('player identity changed or is unverified')
        return current


def exact_target(name):
    return player_target(name)


def config_fingerprint(value):
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False,
                         separators=(',', ':'), allow_nan=False).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()


def receipt_state(operation):
    """Classify raw SDK receipt without replacing its evidence."""
    if not isinstance(operation, dict):
        return 'unknown'
    result = operation.get('result')
    if isinstance(result, dict) and result.get('partial_effects'):
        return 'partial'
    state = operation.get('state')
    if state in ('accepted', 'dispatching', 'running'):
        return 'pending'
    if state == 'succeeded':
        return 'succeeded' if isinstance(result, dict) and result.get('state') == state else 'unknown'
    if state in ('failed', 'cancelled'):
        return 'failed' if isinstance(result, dict) and result.get('state') == state else 'unknown'
    return 'unknown'


def _aggregate(actions):
    states = {action['state'] for action in actions}
    if not states or states == {'succeeded'}:
        return 'succeeded'
    if 'partial' in states or ('succeeded' in states and ('failed' in states or 'unknown' in states)):
        return 'partial'
    if 'unknown' in states:
        return 'unknown'
    if 'pending' in states:
        return 'pending'
    return 'failed'


async def recover_actions(ctx, record):
    """Return a copy; caller owns persistence and CAS, never replay this record.

    Record: {business_key, commit_id, actions: [{intent: intent.payload(),
    operation_id: None, receipt: None, state: 'pending'}], state: 'pending'}.
    actions MUST contain every action of that commit in tx.action order.
    A reconciliation_error is an unresolved read, not ordinary pending work.
    """
    updated = copy.deepcopy(record)
    actions = updated['actions']
    updated.pop('reconciliation_error', None)
    if any(not action.get('operation_id') for action in actions):
        try:
            receipt = await ctx.storage.receipt(updated['commit_id'])
            if receipt is None:
                raise LookupError('commit_receipt_not_found')
            ids = receipt['operation_ids']
            if len(ids) != len(actions):
                raise ValueError('commit_action_count_mismatch')
            if any(action.get('operation_id') not in (None, ids[index])
                   for index, action in enumerate(actions)):
                raise ValueError('commit_action_identity_mismatch')
            for action, operation_id in zip(actions, ids):
                action['operation_id'] = operation_id
        except Exception as error:
            # Expose failure category without copying potentially secret RPC
            # messages into public plugin logs or storage.
            updated['reconciliation_error'] = ('commit_receipt_not_found'
                if isinstance(error, LookupError) else type(error).__name__)
            for action in actions:
                if not action.get('operation_id'):
                    action['state'] = 'unknown'
    for action in actions:
        operation_id = action.get('operation_id')
        if not operation_id:
            continue
        retained = action.get('receipt')
        known = receipt_state(retained)
        if (isinstance(retained, dict) and retained.get('operation_id') == operation_id
                and known in ('succeeded', 'failed', 'partial')):
            # A later read outage cannot erase a previously confirmed result.
            action['state'] = known
            action.pop('reconciliation_error', None)
            continue
        try:
            receipt = await ctx.operations.get(operation_id)
        except Exception as error:
            action['reconciliation_error'] = type(error).__name__
            action['state'] = 'unknown'
            updated['reconciliation_error'] = 'operation_lookup_failed'
        else:
            action.pop('reconciliation_error', None)
            action['receipt'] = receipt
            action['state'] = receipt_state(receipt)
    updated['state'] = _aggregate(actions)
    if updated.get('reconciliation_error') and updated['state'] not in ('unknown', 'partial'):
        updated['state'] = 'unknown'
    return updated
