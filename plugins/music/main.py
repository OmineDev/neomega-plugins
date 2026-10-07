"""Caller-owned, bounded one-shot MIDI playback service."""
import asyncio
import base64
import binascii
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import hashlib
import math
import re
import time
import uuid
from neomega_runtime import Plugin, service, ServiceRejected, PlayerNotFound
from neomega_runtime.players import player_target
from neomega_midi import parse, sound_for, pitch_multiplier


@dataclass(frozen=True)
class Settings:
    sounds_per_second: int = 40
    max_playbacks: int = 16
    max_targets: int = 16
    history_limit: int = 128
    max_lateness: float = 0.25

    def __post_init__(self):
        for field in ('sounds_per_second', 'max_playbacks', 'max_targets', 'history_limit'):
            if type(getattr(self, field)) is not int or not 1 <= getattr(self, field) <= 1024:
                raise ValueError(field + ' must be 1..1024')
        if not math.isfinite(self.max_lateness) or not 0.01 <= self.max_lateness <= 5:
            raise ValueError('max_lateness must be 0.01..5')


class Music(Plugin):
    config_type = Settings

    def __init__(self):
        self.jobs = {}
        self.budget = deque()
        self.sealed = None
        self.admission = asyncio.Lock()

    def view(self, row):
        elapsed = min(row['duration'], max(0, time.monotonic() - row['started']) * row['speed'])
        if row['state'] != 'playing':
            elapsed = row['elapsed']
        return {k: row[k] for k in ('playback_id', 'state', 'duration', 'emitted', 'dropped', 'error', 'operation_id', 'operation_receipt', 'operation_intent')} | {
            'position': elapsed, 'progress': elapsed / row['duration'] if row['duration'] else 1.0}

    def owned(self, args, caller):
        row = self.jobs.get(args.get('playback_id'))
        if row is None or row['owner'] != (caller.installation_id, caller.generation):
            raise ServiceRejected('playback_not_found')
        return row

    @service('play', with_context=True)
    async def play(self, ctx, args, caller):
        async with self.admission:
            return await self.admit(ctx, args, caller)

    async def admit(self, ctx, args, caller):
        digest = hashlib.sha256(json.dumps(args, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
        for row in self.jobs.values():
            if row['call_id'] == caller.call_id and row['owner'] == (caller.installation_id, caller.generation):
                if row['digest'] != digest:
                    raise ServiceRejected('idempotency_conflict')
                return self.view(row)
        if self.sealed is not None:
            raise ServiceRejected('maintenance_sealed')
        targets = args.get('targets')
        speed, volume = args.get('speed', 1.0), args.get('volume', 1.0)
        sound = args.get('sound')
        encoded = args.get('midi_base64')
        if (not isinstance(targets, list) or not 1 <= len(targets) <= ctx.config.max_targets
                or any(not isinstance(t, str) or not t or len(t) > 64 or any(ord(c) < 32 for c in t) for t in targets)
                or type(speed) not in (int, float) or not 0.25 <= speed <= 4
                or type(volume) not in (int, float) or not 0 <= volume <= 1
                or (sound is not None and (not isinstance(sound, str) or not re.fullmatch(r'[a-z0-9_.:]{1,80}', sound)))
                or not isinstance(encoded, str) or len(encoded) > 60000):
            raise ServiceRejected('invalid_arguments')
        try:
            sequence = parse(base64.b64decode(encoded, validate=True), max_bytes=45000)
        except (ValueError, binascii.Error):
            raise ServiceRejected('invalid_midi') from None
        if sum(r['state'] in ('playing', 'unknown') for r in self.jobs.values()) >= ctx.config.max_playbacks:
            raise ServiceRejected('playback_capacity')
        # Bound completed history without evicting active jobs or another caller's control.
        completed = [k for k, r in self.jobs.items() if r['state'] not in ('playing', 'unknown')]
        for key in completed[:max(0, len(completed) - ctx.config.history_limit + 1)]:
            del self.jobs[key]
        try:
            targets = [player_target((await ctx.resolve_player(name=name))['name']) for name in dict.fromkeys(targets)]
        except (ValueError, PlayerNotFound):
            raise ServiceRejected('invalid_target') from None
        key = uuid.uuid4().hex
        row = dict(playback_id=key, owner=(caller.installation_id, caller.generation), call_id=caller.call_id, digest=digest, state='playing', duration=sequence.duration,
                   started=time.monotonic(), elapsed=0.0, speed=speed, emitted=0, dropped=0, error='', operation_id=None, operation_receipt=None, operation_intent=None)
        self.jobs[key] = row
        row['task'] = ctx.spawn(self.play_sequence(ctx, row, sequence, list(dict.fromkeys(targets)), volume, sound), name='music-' + key)
        return self.view(row)

    async def play_sequence(self, ctx, row, sequence, targets, volume, sound):
        try:
            for index, note in enumerate(sequence):
                due = row['started'] + note.time / row['speed']
                await asyncio.sleep(max(0, due - time.monotonic()))
                for target_index, target in enumerate(targets):
                    now = time.monotonic()
                    while self.budget and now - self.budget[0] >= 1:
                        self.budget.popleft()
                    if now - due > ctx.config.max_lateness or len(self.budget) >= ctx.config.sounds_per_second:
                        row['dropped'] += 1
                        continue
                    self.budget.append(now)
                    player = target
                    instrument = sound or sound_for(note.program, note.channel, note.pitch)
                    command = (f'execute as {player} at @s run playsound {instrument} @s ~ ~ ~ '
                               f'{volume * note.velocity / 127:.5f} {pitch_multiplier(note.pitch):.5f}')
                    intent = ctx.commands.prepare(command,
                        idempotency_key=f"music_{row['playback_id']}_{index}_{target_index}",
                        deadline=(datetime.now(timezone.utc) + timedelta(seconds=3)).isoformat(),
                        timeout=2)
                    row['operation_intent'] = intent.payload()
                    row['operation_id'], row['operation_receipt'] = None, None
                    try:
                        receipt = await ctx.operations.execute(intent, timeout=3, submit_timeout=2)
                    except (Exception, asyncio.CancelledError) as exc:
                        row['operation_id'] = getattr(exc, 'operation_id', None)
                        row['state'], row['error'] = 'unknown', type(exc).__name__
                        return
                    row['operation_id'], row['operation_receipt'] = receipt['operation_id'], receipt
                    if receipt['state'] != 'succeeded':
                        row['state'] = 'unknown' if receipt['state'] in ('unknown', 'pending', 'running') else 'failed'
                        row['error'] = 'sound_operation_' + receipt['state']
                        return
                    result = ctx.commands.result(receipt)
                    if not result.is_success:
                        row['state'], row['error'] = 'failed', result.reason or 'sound_command_unsuccessful'
                        return
                    row['emitted'] += 1
            await asyncio.sleep(max(0, row['started'] + sequence.duration / row['speed'] - time.monotonic()))
            row['state'] = 'completed'
        except asyncio.CancelledError:
            row['state'] = 'stopped'
            raise
        except Exception as exc:
            # The playback fails; managed worker remains available for status/control.
            row['state'], row['error'] = 'failed', type(exc).__name__
            ctx.log.warning('music playback failed: %s', type(exc).__name__)
        finally:
            row['elapsed'] = min(row['duration'], max(0, time.monotonic() - row['started']) * row['speed'])

    @service('status', with_context=True)
    async def status(self, ctx, args, caller):
        async with self.admission:
            row = self.owned(args, caller)
            if row['state'] == 'unknown' and row['operation_id']:
                row['operation_receipt'] = await ctx.operations.get(row['operation_id'])
                state = row['operation_receipt']['state']
                if state == 'succeeded':
                    result = ctx.commands.result(row['operation_receipt'])
                    if result.is_success:
                        row['state'], row['error'] = 'stopped', 'reconciled_without_resuming'
                        row['emitted'] += 1
                    else:
                        row['state'], row['error'] = 'failed', result.reason or 'sound_command_unsuccessful'
                elif state in ('failed', 'rejected', 'cancelled'):
                    row['state'], row['error'] = 'failed', 'sound_operation_' + state
            return self.view(row)

    @service('stop', with_context=True)
    async def stop(self, ctx, args, caller):
        row = self.owned(args, caller)
        if row['state'] == 'playing':
            row['task'].cancel()
            await asyncio.gather(row['task'], return_exceptions=True)
            if row['state'] == 'playing':
                row['state'] = 'stopped'
        return self.view(row)

    async def on_maintenance(self, ctx, request):
        async with self.admission:
            token = request['token']
            if request['operation'] == 'seal':
                if self.sealed not in (None, token) or any(r['state'] in ('playing', 'unknown') for r in self.jobs.values()):
                    return {'status': 'busy', 'token': token}
                self.sealed = token
                return {'status': 'sealed', 'token': token}
            if request['operation'] == 'release' and self.sealed in (None, token):
                self.sealed = None
                return {'status': 'released', 'token': token}
            return {'status': 'busy', 'token': token}

    async def on_stop(self, ctx):
        for row in self.jobs.values():
            if row['state'] == 'playing':
                row['task'].cancel()
        await asyncio.gather(*(r['task'] for r in self.jobs.values()), return_exceptions=True)


plugin = Music()

if __name__ == '__main__':
    plugin.run()
