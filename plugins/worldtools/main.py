"""Durable region/NBT tasks; every world action belongs to one retained commit."""
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import time
import uuid

from neomega_runtime import Plugin
from neomega_runtime.artifacts import ArtifactRetentionUncertain
from neomega_runtime.http import ProtocolError
from neomega_runtime.managed import IPCError, IPCRejected
from neomega_runtime.services import ServiceRejected, service
from neomega_runtime.storage import CommitUncertain
from neomega_world import Region, container_slots, digest, position

from fatalder_adapter import FatalderTasks
from snapshot_tasks import SnapshotTasks, atomic_json


def deadline():
    return (datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat()


@dataclass(frozen=True)
class Settings:
    max_tasks: int = 256
    max_blocks: int = 262144
    max_steps: int = 1024
    max_state_bytes: int = 33554432

    def __post_init__(self):
        if (type(self.max_tasks) is not int or not 1 <= self.max_tasks <= 4096
                or type(self.max_blocks) is not int or not 1 <= self.max_blocks <= 1048576
                or type(self.max_steps) is not int or not 1 <= self.max_steps <= 1024):
            raise ValueError('invalid task limits')
        if type(self.max_state_bytes) is not int or not 1048576 <= self.max_state_bytes <= 268435456:
            raise ValueError('state byte budget must be 1..256 MiB')


SINGLE_STEP_KINDS = (
    'command_block',
    'sign',
    'item',
    'enhanced_item',
    'structure_block',
    'pick_block',
    'place_block',
    'held_on_block',
    'item_frame',
    'inspect',
)
WORKSPACE_EXCLUSIVE_KINDS = (
    'container',
    'item',
    'enhanced_item',
    'pick_block',
    'place_block',
    'sign',
    'held_on_block',
    'item_frame',
)
CONTAINER_BLOCKS = ('minecraft:chest', 'minecraft:barrel', 'minecraft:shulker_box')


class WorldTools(SnapshotTasks, FatalderTasks, Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = None
        self.tick_cursor = 0
        self.tasks = {}
        self._submitting = None
        self._fatalder_in_flight = set()

    def _task_bytes(self, task):
        return len(json.dumps(task, ensure_ascii=False).encode())

    def check_budget(self, ctx, *, extra_tasks=()):
        total = sum(self._task_bytes(t) for t in self.tasks.values())
        total += sum(self._task_bytes(t) for t in extra_tasks)
        if total > ctx.config.max_state_bytes:
            raise ServiceRejected('state_byte_budget')

    def check_task_mutation_budget(self, ctx, task, *, inserted_steps):
        old_size = self._task_bytes(task)
        cursor = task['cursor']
        prospective = dict(task)
        prospective['steps'] = (
            task['steps'][:cursor + 1]
            + list(inserted_steps)
            + task['steps'][cursor + 1:]
        )
        new_size = self._task_bytes(prospective)
        total = sum(self._task_bytes(t) for t in self.tasks.values()) - old_size + new_size
        if total > ctx.config.max_state_bytes:
            raise ServiceRejected('state_byte_budget')

    async def on_maintenance(self, ctx, request):
        token = request['token']
        operation = request['operation']
        if operation != 'seal':
            async with self.lock:
                if self.sealed not in (None, token):
                    return {'status': 'busy', 'token': token}
                self.sealed = None
                return {'status': 'released', 'token': token}

        async with self.lock:
            if self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            if self._submitting is not None or self._fatalder_in_flight:
                return {'status': 'busy', 'token': token}
            fatalder_tasks = [t for t in self.tasks.values() if t.get('remote_calls')]
            world_ids = [
                t['task_id']
                for t in self.tasks.values()
                if t['kind'] != 'fatalder' and t['state'] in ('running', 'unknown', 'cancelling')
            ]

        for task in fatalder_tasks:
            try:
                await self.reconcile_fatalder(ctx, task)
            except Exception:
                return {'status': 'busy', 'token': token}
        for tid in world_ids:
            try:
                await self.reconcile(ctx, tid)
            except Exception:
                return {'status': 'busy', 'token': token}

        async with self.lock:
            if self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            if self._submitting is not None or self._fatalder_in_flight:
                return {'status': 'busy', 'token': token}
            for task in self.tasks.values():
                if task.get('remote_calls') and any(self.unresolved_remote(r) for r in task['remote_calls']):
                    await self.save(ctx)
                    return {'status': 'busy', 'token': token}
                if task['kind'] == 'fatalder':
                    # A replied service call is not a finished remote world job.
                    remote = task.get('remote') or {}
                    if (task['state'] not in ('remote_succeeded', 'remote_failed', 'remote_cancelled')
                            or remote.get('lease_pending') or remote.get('recovery_required')):
                        await self.save(ctx)
                        return {'status': 'busy', 'token': token}
                elif task['state'] in ('running', 'unknown', 'cancelling'):
                    await self.save(ctx)
                    return {'status': 'busy', 'token': token}
            await self.save(ctx)
            self.sealed = token
            return {'status': 'sealed', 'token': token}

    def writable(self):
        if self.sealed is not None:
            raise ServiceRejected('maintenance_sealed')
        if any(step['state'] == 'unknown' and not step['operation_ids']
               for task in self.tasks.values() for step in task['steps']):
            raise ServiceRejected('admission_unknown')

    async def on_start(self, ctx):
        self.root = ctx.data_dir / 'world_records'
        self.root.mkdir(parents=True, exist_ok=True)
        self.tasks = {p.name.split('.')[0]: json.loads(p.read_text()) for p in self.root.glob('*.task.json')}
        self.persisted = {k: digest(v) for k, v in self.tasks.items()}
        for task in self.tasks.values():
            version = task.get('schema_version', 1)
            if type(version) is not int or version not in (1, 2):
                raise ValueError('unsupported world task schema_version')
            if version == 1:
                # Only metadata changes: retained commit IDs and unknown actions stay intact.
                task.update(schema_version=2, revision=0)
            elif type(task.get('revision')) is not int or task['revision'] < 1:
                raise ValueError('invalid world task revision')
        await self.save(ctx)
        ctx.every(0.5, lambda: self.tick(ctx), name='world-task-pump')

    async def save(self, ctx):
        self.check_budget(ctx)
        for key, task in self.tasks.items():
            checksum = digest(task)
            if getattr(self, 'persisted', {}).get(key) != checksum:
                old_revision = task.get('revision', 0)
                task.update(schema_version=2, revision=old_revision + 1)
                try:
                    atomic_json(self.root / (key + '.task.json'), task)
                except Exception:
                    task['revision'] = old_revision
                    raise
                self.persisted[key] = digest(task)

    def owned(self, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        task_id = args.get('task_id')
        if not isinstance(task_id, str) or not task_id:
            raise ServiceRejected('invalid_argument')
        task = self.tasks.get(task_id)
        if task is None or task['owner'] != call.installation_id:
            raise ServiceRejected('task_not_found')
        return task

    @staticmethod
    def check_revision(task, args):
        if 'expected_revision' not in args or type(args.get('expected_revision')) is not int:
            raise ServiceRejected('invalid_argument')
        if args['expected_revision'] != task['revision']:
            raise ServiceRejected('revision_conflict')

    @staticmethod
    def public(task, offset=0, limit=2):
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 2:
            raise ServiceRejected('invalid_page')
        result = {
            k: v for k, v in task.items()
            if k not in ('owner', 'steps', 'args', 'regions', 'remote_calls', 'losses')
        }
        result.update(
            losses=task.get('losses', [])[offset:offset + 32],
            loss_count=len(task.get('losses', [])),
            loss_offset=offset,
        )
        if 'remote_calls' in task:
            result['remote_call_count'] = len(task['remote_calls'])
            result['remote_calls'] = [
                {k: v for k, v in r.items() if k not in ('intent', 'receipt')}
                for r in task['remote_calls'][offset:offset + limit]
            ]
        steps = []
        for step in task['steps'][offset:offset + limit]:
            visible = dict(step)
            if 'receipts' in visible:
                visible['receipts'] = [
                    dict(
                        r,
                        result=(
                            {'sha256': digest(r['result']), 'detail': 'operation_readback'}
                            if len(json.dumps(r.get('result')).encode()) > 8192
                            else r.get('result')
                        ),
                    )
                    for r in visible['receipts']
                ]
            if len(json.dumps(visible['args']).encode()) > 4096:
                visible['args'] = {'sha256': digest(step['args']), 'detail': 'original_request'}
            steps.append(visible)
        result.update(
            steps=steps,
            step_count=len(task['steps']),
            step_offset=offset,
            region_count=len(task['regions']),
            regions=task['regions'][offset:offset + limit],
        )
        return result

    @staticmethod
    def append(steps, method, args):
        steps.append({
            'method': method,
            'args': args,
            'state': 'prepared',
            'commit_id': uuid.uuid4().hex,
            'operation_ids': [],
        })

    async def epoch(self, ctx):
        try:
            reply = await ctx.peer.call('capabilities.get', {})
        except (IPCError, TimeoutError, asyncio.TimeoutError):
            raise ServiceRejected('world_epoch_unavailable') from None
        data = reply.get('data') if isinstance(reply, dict) else None
        world_epoch = data.get('world_epoch') if isinstance(data, dict) else None
        if not isinstance(world_epoch, str) or not world_epoch:
            raise ServiceRejected('world_epoch_unavailable')
        return world_epoch

    async def create(self, ctx, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        if 'request_key' not in args or 'kind' not in args or 'args' not in args:
            raise ServiceRejected('invalid_argument')
        request_key = args['request_key']
        if not isinstance(request_key, str) or not 1 <= len(request_key) <= 128:
            raise ServiceRejected('request_key_required')
        kind = args['kind']
        spec = args['args']
        if not isinstance(kind, str) or not isinstance(spec, dict):
            raise ServiceRejected('invalid_argument')
        dimension = spec.get('dimension')
        if dimension is not None and (type(dimension) is not int or not -(1 << 31) <= dimension < (1 << 31)):
            raise ServiceRejected('invalid_argument')
        try:
            fingerprint = digest(args)
        except (TypeError, ValueError):
            raise ServiceRejected('invalid_argument') from None
        ident = hashlib.sha256((call.installation_id + '\0' + request_key).encode()).hexdigest()

        async with self.lock:
            self.writable()
            if ident in self.tasks:
                task = self.tasks[ident]
                if task['fingerprint'] != fingerprint:
                    raise ServiceRejected('request_key_conflict')
                return self.public(task)
            if len(self.tasks) >= ctx.config.max_tasks:
                raise ServiceRejected('task_limit')

        epoch = await self.epoch(ctx)

        async with self.lock:
            self.writable()
            if ident in self.tasks:
                task = self.tasks[ident]
                if task['fingerprint'] != fingerprint:
                    raise ServiceRejected('request_key_conflict')
                return self.public(task)
            if len(self.tasks) >= ctx.config.max_tasks:
                raise ServiceRejected('task_limit')

            steps, regions, losses = [], [], []
            portable = False
            if kind == 'backup':
                mode = spec.get('mode', 'snapshot')
                if mode not in ('snapshot', 'structure'):
                    raise ServiceRejected('invalid_argument')
                portable = mode == 'snapshot'

            if portable:
                if 'pos' not in spec or 'size' not in spec:
                    raise ServiceRejected('invalid_argument')
                try:
                    region = Region(spec['pos'], spec['size'])
                except (TypeError, ValueError):
                    raise ServiceRejected('invalid_argument') from None
                if region.volume > ctx.config.max_blocks:
                    raise ServiceRejected('region_too_large')
                losses = self.portable_plan(spec, ident, epoch, steps, regions, max_steps=ctx.config.max_steps)
            elif kind == 'restore' and spec.get('mode', 'snapshot') == 'snapshot':
                losses, dimension = self.restore_plan(spec, call, steps, regions, max_steps=ctx.config.max_steps)
            elif kind in ('backup', 'copy'):
                if 'pos' not in spec or 'size' not in spec:
                    raise ServiceRejected('invalid_argument')
                try:
                    region = Region(spec['pos'], spec['size'])
                except (TypeError, ValueError):
                    raise ServiceRejected('invalid_argument') from None
                if region.volume > ctx.config.max_blocks:
                    raise ServiceRejected('region_too_large')
                regions.append(region.as_dict())
                destination = None
                if kind == 'copy':
                    if 'destination' not in spec:
                        raise ServiceRejected('invalid_argument')
                    try:
                        destination = position(spec['destination'])
                        regions.append(Region(destination, region.size).as_dict())
                    except (TypeError, ValueError):
                        raise ServiceRejected('invalid_argument') from None
                tile_count = ((region.size[0] + 15) // 16) * ((region.size[1] + 15) // 16) * ((region.size[2] + 15) // 16)
                if tile_count * 2 > ctx.config.max_steps:
                    raise ServiceRejected('step_limit')
                tiles = list(region.tiles())
                # Capture all source tiles before loading any destination; overlap is safe.
                for i, tile in enumerate(tiles):
                    name = 'nw_' + ident[:24] + '_' + str(i)
                    self.append(steps, 'save_structure', {'name': name, 'pos': list(tile.origin), 'size': list(tile.size)})
                    if kind == 'backup':
                        self.append(steps, 'export_bdx', {'pos': list(tile.origin), 'size': list(tile.size)})
                if destination:
                    for i, tile in enumerate(tiles):
                        target = [d + t - o for d, t, o in zip(destination, tile.origin, region.origin)]
                        self.append(steps, 'load_structure', {'name': 'nw_' + ident[:24] + '_' + str(i), 'pos': target})
                losses = ['entities', 'scheduled_ticks']
                if kind == 'backup':
                    losses += [
                        'portable_bdx_excludes_air_clearing',
                        'portable_bdx_excludes_background_layers',
                        'portable_bdx_excludes_non_command_block_entity_nbt',
                    ]
            elif kind == 'restore':
                backup_id = spec.get('backup_id')
                if not isinstance(backup_id, str) or not backup_id:
                    raise ServiceRejected('invalid_argument')
                backup = self.owned({'task_id': backup_id}, call)
                if backup['kind'] != 'backup' or backup['state'] != 'succeeded':
                    raise ServiceRejected('backup_not_complete')
                mode = spec.get('mode', 'structure')
                if mode not in ('structure', 'bdx') or backup.get('portable'):
                    raise ServiceRejected('invalid_restore_mode')
                try:
                    origin = position(backup['args']['pos'])
                    destination = position(spec['destination']) if 'destination' in spec else origin
                    region = Region(destination, backup['args']['size'])
                except (KeyError, TypeError, ValueError):
                    raise ServiceRejected('invalid_argument') from None
                regions.append(region.as_dict())
                if mode == 'structure' and backup['epoch'] != epoch:
                    raise ServiceRejected('structure_epoch_expired')
                for step in backup['steps']:
                    target = [d + t - o for d, t, o in zip(destination, step['args']['pos'], origin)]
                    if mode == 'structure' and step['method'] == 'save_structure':
                        self.append(steps, 'load_structure', {'name': step['args']['name'], 'pos': target})
                    if mode == 'bdx' and step['method'] == 'export_bdx':
                        receipts = step.get('receipts')
                        if not isinstance(receipts, list) or not receipts or not isinstance(receipts[0], dict):
                            raise ServiceRejected('invalid_backup_receipt')
                        completion = receipts[0].get('result')
                        payload = completion.get('result') if isinstance(completion, dict) else None
                        artifact = payload.get('artifact') if isinstance(payload, dict) else None
                        artifact_id = artifact.get('artifact_id') if isinstance(artifact, dict) else None
                        if not isinstance(artifact_id, str) or not artifact_id:
                            raise ServiceRejected('invalid_backup_receipt')
                        self.append(steps, 'bdx_artifact', {'artifact_id': artifact_id, 'pos': target})
                losses = backup['losses'] if mode == 'bdx' else ['entities', 'scheduled_ticks']
            elif kind == 'container':
                for req_key in ('slots', 'pos', 'anvil_pos', 'workspace_pos'):
                    if req_key not in spec:
                        raise ServiceRejected('invalid_argument')
                if 'block' in spec and spec['block'] not in CONTAINER_BLOCKS:
                    raise ServiceRejected('invalid_argument')
                second_raw = spec.get('second_pos')
                try:
                    slots = container_slots(spec['slots'], double_chest=bool(second_raw))
                    first = position(spec['pos'])
                    second = position(second_raw) if second_raw is not None else None
                    anvil_pos = position(spec['anvil_pos'])
                    workspace_pos = position(spec['workspace_pos'])
                except (TypeError, ValueError):
                    raise ServiceRejected('invalid_argument') from None
                if not slots:
                    raise ServiceRejected('invalid_argument')
                for offset, pos in ((0, first), (27, second)):
                    if pos is None:
                        continue
                    local = {str(int(k) - offset): v for k, v in slots.items() if offset <= int(k) < offset + 27}
                    items = list(local.items())
                    for i in range(0, len(items), 9):
                        fields = {
                            'pos': list(pos),
                            'slots': dict(items[i:i + 9]),
                            'anvil_pos': list(anvil_pos),
                            'workspace_pos': list(workspace_pos),
                        }
                        if dimension is not None:
                            fields['dimension'] = dimension
                        if i == 0 and 'block' in spec:
                            fields['block'] = spec['block']
                        self.append(steps, 'container', fields)
                    regions.append(Region(pos, (1, 1, 1)).as_dict())
                losses = ['unmentioned_slots_unchanged']
            elif kind in SINGLE_STEP_KINDS:
                if kind == 'inspect':
                    if 'pos' not in spec or set(spec) - {'pos', 'dimension'}:
                        raise ServiceRejected('invalid_argument')
                    try:
                        pos = position(spec['pos'])
                    except ValueError:
                        raise ServiceRejected('invalid_argument') from None
                    step_args = {'pos': list(pos)}
                    if dimension is not None:
                        step_args['dimension'] = dimension
                    self.append(steps, 'block', step_args)
                    regions.append(Region(pos, (1, 1, 1)).as_dict())
                else:
                    step_args = dict(spec)
                    self.append(steps, kind, step_args)
                    if 'pos' in spec:
                        try:
                            regions.append(Region(spec['pos'], (1, 1, 1)).as_dict())
                        except (TypeError, ValueError):
                            raise ServiceRejected('invalid_argument') from None
            else:
                raise ServiceRejected('unsupported_task_kind')

            for field in ('workspace_pos', 'anvil_pos'):
                if field in spec:
                    try:
                        regions.append(Region(spec[field], (1, 1, 1)).as_dict())
                    except (TypeError, ValueError):
                        raise ServiceRejected('invalid_argument') from None
            if not steps or len(steps) > ctx.config.max_steps:
                raise ServiceRejected('step_limit')
            # Validate all SDK intent arguments before storing a runnable plan.
            for step in steps:
                try:
                    self.intent(ctx, step)
                except (TypeError, ValueError):
                    raise ServiceRejected('invalid_argument') from None
            task = {
                'task_id': ident,
                'owner': call.installation_id,
                'fingerprint': fingerprint,
                'kind': kind,
                'args': spec,
                'epoch': epoch,
                'state': 'prepared',
                'created_at': time.time(),
                'steps': steps,
                'regions': regions,
                'losses': losses,
                'cursor': 0,
                'cancel_requested': False,
                'portable': portable,
                'dimension': dimension,
            }
            self.check_budget(ctx, extra_tasks=[task])
            self.tasks[ident] = task
            try:
                await self.save(ctx)
            except Exception:
                self.tasks.pop(ident, None)
                raise
            return self.public(task)

    @staticmethod
    def intent(ctx, step):
        method = 'region' if step['method'] in ('_snapshot_read', '_restore_read') else step['method']
        facade = ctx.queries if method in ('block', 'region') else ctx.world
        return getattr(facade, method)(**step['args'], idempotency_key=step['commit_id'], deadline=deadline())

    @service('prepare', with_context=True)
    async def prepare(self, ctx, args, call):
        return await self.create(ctx, args, call)

    @service('inspect', with_context=True)
    async def inspect(self, ctx, args, call):
        if not isinstance(args, dict) or 'pos' not in args or 'request_key' not in args:
            raise ServiceRejected('invalid_argument')
        if set(args) - {'request_key', 'pos', 'dimension'}:
            raise ServiceRejected('invalid_argument')
        spec = {'pos': args['pos']}
        if 'dimension' in args and args['dimension'] is not None:
            spec['dimension'] = args['dimension']
        return await self.create(ctx, {'kind': 'inspect', 'args': spec, 'request_key': args['request_key']}, call)

    def acquire(self, task):
        for other in self.tasks.values():
            if other['task_id'] == task['task_id'] or other['state'] not in ('running', 'unknown', 'cancelling'):
                continue
            # One robot hotbar/workspace writer at a time, plus overlapping regions.
            if task['kind'] in WORKSPACE_EXCLUSIVE_KINDS or other['kind'] in WORKSPACE_EXCLUSIVE_KINDS:
                raise ServiceRejected('workspace_busy')
            if any(Region(a['origin'], a['size']).overlaps(Region(b['origin'], b['size']))
                   for a in task['regions'] for b in other['regions']):
                raise ServiceRejected('region_busy')

    @service('execute', with_context=True)
    async def execute(self, ctx, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        async with self.lock:
            self.writable()
            task = self.owned(args, call)
            self.check_revision(task, args)
            expected_rev = task['revision']
            if task['state'] != 'prepared':
                return self.public(task)
        current_epoch = await self.epoch(ctx)
        async with self.lock:
            self.writable()
            task = self.owned(args, call)
            if task['revision'] != expected_rev:
                raise ServiceRejected('revision_conflict')
            if task['state'] == 'prepared':
                if task['epoch'] != current_epoch:
                    raise ServiceRejected('world_epoch_expired')
                self.acquire(task)
                task['state'] = 'running'
                try:
                    await self.save(ctx)
                except Exception:
                    task['state'] = 'prepared'
                    raise
            return self.public(task)

    @service('status', with_context=True)
    async def status(self, ctx, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        async with self.lock:
            task = self.owned(args, call)
            task_id = task['task_id']
            should_reconcile = task['state'] in ('running', 'unknown', 'cancelling')
        if should_reconcile:
            try:
                await self.reconcile(ctx, task_id)
            except (IPCError, ProtocolError, TimeoutError, asyncio.TimeoutError):
                raise ServiceRejected('reconcile_unavailable') from None
        async with self.lock:
            task = self.owned(args, call)
            await self.save(ctx)
            return self.public(task, args.get('offset', 0), args.get('limit', 2))

    @service('cancel', with_context=True)
    async def cancel(self, ctx, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        async with self.lock:
            self.writable()
            task = self.owned(args, call)
            self.check_revision(task, args)
            old_state = task['state']
            old_cancel = task['cancel_requested']
            task['cancel_requested'] = True
            if task['state'] == 'prepared':
                task['state'] = 'cancelled'
            elif task['state'] == 'running':
                task['state'] = 'cancelling'
            # Already submitted actions finish/reconcile; no rollback claim.
            try:
                await self.save(ctx)
            except Exception:
                task['state'] = old_state
                task['cancel_requested'] = old_cancel
                raise
            return self.public(task)

    @service('resume', with_context=True)
    async def resume(self, ctx, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        async with self.lock:
            self.writable()
            task = self.owned(args, call)
            self.check_revision(task, args)
            task_id = task['task_id']
            expected_rev = task['revision']
        try:
            await self.reconcile(ctx, task_id)
        except (IPCError, ProtocolError, TimeoutError, asyncio.TimeoutError):
            raise ServiceRejected('reconcile_unavailable') from None

        async with self.lock:
            self.writable()
            task = self.owned(args, call)
            if task['revision'] != expected_rev:
                raise ServiceRejected('revision_conflict')
            can_resume = (
                task['state'] == 'cancelled'
                and task['cursor'] < len(task['steps'])
                and task['steps'][task['cursor']]['state'] == 'prepared'
            )

        current_epoch = await self.epoch(ctx) if can_resume else None

        async with self.lock:
            self.writable()
            task = self.owned(args, call)
            if task['revision'] != expected_rev:
                raise ServiceRejected('revision_conflict')
            # Unknown and failed actions are never replaced or resubmitted.
            if (task['state'] == 'cancelled'
                    and task['cursor'] < len(task['steps'])
                    and task['steps'][task['cursor']]['state'] == 'prepared'):
                if task['epoch'] != current_epoch:
                    raise ServiceRejected('world_epoch_expired')
                self.acquire(task)
                task.update(state='running', cancel_requested=False)
            await self.save(ctx)
            return self.public(task)

    async def reconcile(self, ctx, task_or_id):
        task_id = task_or_id['task_id'] if isinstance(task_or_id, dict) else task_or_id
        async with self.lock:
            task = self.tasks.get(task_id) if not isinstance(task_or_id, dict) else self.tasks.get(task_id, task_or_id)
            if task is None or task['cursor'] >= len(task['steps']):
                return
            cursor = task['cursor']
            step = task['steps'][cursor]
            if step['state'] == 'prepared':
                if task['cancel_requested']:
                    task['state'] = 'cancelled'
                return
            commit_id = step['commit_id']
            op_ids = list(step['operation_ids'])

        if not op_ids:
            found = await ctx.storage.receipt(commit_id)
            if found is None:
                async with self.lock:
                    task = self.tasks.get(task_id) if not isinstance(task_or_id, dict) else self.tasks.get(task_id, task_or_id)
                    if task is not None and task['cursor'] == cursor and not task['steps'][cursor]['operation_ids']:
                        task['state'] = 'unknown'
                return
            op_ids = list(found['operation_ids'])
        receipts = [await ctx.operations.get(opid) for opid in op_ids]

        async with self.lock:
            task = self.tasks.get(task_id) if not isinstance(task_or_id, dict) else self.tasks.get(task_id, task_or_id)
            if task is None or task['cursor'] != cursor or cursor >= len(task['steps']):
                return
            step = task['steps'][cursor]
            if not step['operation_ids']:
                step['operation_ids'] = op_ids
            step['receipts'] = receipts
            states = [r['state'] for r in receipts]
            if 'unknown' in states:
                step['state'] = task['state'] = 'unknown'
            elif 'failed' in states or 'cancelled' in states:
                step['state'] = task['state'] = 'failed'
            elif states and all(s == 'succeeded' for s in states):
                try:
                    await self.snapshot_completed(ctx, task, step, receipts[0])
                except Exception as exc:
                    step['state'] = task['state'] = 'failed'
                    step['error'] = str(exc)
                    return
                step['state'] = 'succeeded'
                task['cursor'] += 1
                task['state'] = (
                    'succeeded' if task['cursor'] == len(task['steps'])
                    else ('cancelled' if task['cancel_requested'] else 'running')
                )
            else:
                task['state'] = 'cancelling' if task['cancel_requested'] else 'running'

    async def _safe_save(self, ctx):
        try:
            await self.save(ctx)
        except (ServiceRejected, OSError):
            pass

    async def tick(self, ctx):
        async with self.lock:
            if self.sealed is not None:
                return
            candidate_ids = [
                tid for tid, t in self.tasks.items()
                if t['kind'] != 'fatalder' and t['state'] in ('running', 'unknown', 'cancelling')
            ]

        for tid in candidate_ids:
            try:
                await self.reconcile(ctx, tid)
            except (IPCError, ProtocolError, TimeoutError, asyncio.TimeoutError):
                continue

        async with self.lock:
            if self.sealed is not None:
                return
            if any(s['state'] == 'unknown' and not s['operation_ids']
                   for t in self.tasks.values() for s in t['steps']):
                await self._safe_save(ctx)
                return
            selected = None
            for t in self.tasks.values():
                if (t['kind'] != 'fatalder' and t['state'] == 'running'
                        and t['cursor'] < len(t['steps'])
                        and t['steps'][t['cursor']]['state'] == 'prepared'):
                    selected = (t['task_id'], t['cursor'], t['epoch'])
                    break
            if selected is None:
                await self._safe_save(ctx)
                return
            tid, cursor, task_epoch = selected

        try:
            current_epoch = await self.epoch(ctx)
        except ServiceRejected:
            async with self.lock:
                await self._safe_save(ctx)
            return

        async with self.lock:
            if self.sealed is not None:
                return
            task = self.tasks.get(tid)
            if (task is None or task['state'] != 'running' or task['cursor'] != cursor
                    or cursor >= len(task['steps'])):
                await self._safe_save(ctx)
                return
            step = task['steps'][cursor]
            if step['state'] != 'prepared':
                await self._safe_save(ctx)
                return
            if task_epoch != current_epoch:
                task['state'] = 'epoch_expired'
                await self._safe_save(ctx)
                return
            try:
                intent = self.intent(ctx, step)
            except (TypeError, ValueError) as exc:
                step['state'] = task['state'] = 'failed'
                step['error'] = str(exc)
                await self._safe_save(ctx)
                return
            need_retain = step['method'] == 'bdx_artifact' and not step.get('artifact_retained')
            artifact_id = step['args']['artifact_id'] if need_retain else None
            commit_id = step['commit_id']

        if need_retain:
            try:
                await ctx.artifacts.retain(artifact_id, commit_id)
            except IPCRejected as exc:
                async with self.lock:
                    task = self.tasks.get(tid)
                    if task is not None and task['cursor'] == cursor:
                        task['steps'][cursor]['state'] = task['state'] = 'failed'
                        task['steps'][cursor]['error'] = exc.code
                    await self._safe_save(ctx)
                return
            except (ArtifactRetentionUncertain, IPCError, ProtocolError, TimeoutError, asyncio.TimeoutError) as exc:
                async with self.lock:
                    task = self.tasks.get(tid)
                    if task is not None and task['cursor'] == cursor:
                        task['steps'][cursor]['state'] = task['state'] = 'failed'
                        task['steps'][cursor]['error'] = type(exc).__name__
                    await self._safe_save(ctx)
                return
            async with self.lock:
                if self.sealed is not None:
                    return
                task = self.tasks.get(tid)
                if (task is None or task['state'] != 'running' or task['cursor'] != cursor
                        or task['steps'][cursor]['state'] != 'prepared'):
                    await self._safe_save(ctx)
                    return
                task['steps'][cursor]['artifact_retained'] = True

        try:
            tx = await ctx.storage.transaction(commit_id=commit_id, keys=[])
        except (IPCError, ProtocolError, TimeoutError, asyncio.TimeoutError):
            async with self.lock:
                await self._safe_save(ctx)
            return

        async with self.lock:
            if self.sealed is not None:
                return
            task = self.tasks.get(tid)
            if (task is None or task['state'] != 'running' or task['cursor'] != cursor
                    or task['steps'][cursor]['state'] != 'prepared'):
                await self._safe_save(ctx)
                return
            step = task['steps'][cursor]
            try:
                tx.set('world_action_' + commit_id, {'task_id': tid, 'cursor': cursor, 'state': 'admitted'})
                tx.action(intent)
                tx.prepare()
            except (ValueError, TypeError) as exc:
                step['state'] = task['state'] = 'failed'
                step['error'] = str(exc)
                await self._safe_save(ctx)
                return
            self._submitting = (tid, cursor)

        try:
            try:
                receipt = await tx.save()
            except IPCRejected as exc:
                async with self.lock:
                    task = self.tasks.get(tid)
                    if task is not None and task['cursor'] == cursor:
                        step = task['steps'][cursor]
                        if exc.code == 'revision_conflict':
                            pass
                        elif exc.code == 'world_epoch_expired':
                            task['state'] = 'epoch_expired'
                        else:
                            step['state'] = task['state'] = 'failed'
                            step['error'] = exc.code
            except (CommitUncertain, IPCError, ProtocolError, TimeoutError, asyncio.TimeoutError) as exc:
                async with self.lock:
                    task = self.tasks.get(tid)
                    if task is not None and task['cursor'] == cursor:
                        step = task['steps'][cursor]
                        step['state'] = task['state'] = 'unknown'
                        step['error'] = type(exc).__name__
            else:
                async with self.lock:
                    task = self.tasks.get(tid)
                    if task is not None and task['cursor'] == cursor:
                        step = task['steps'][cursor]
                        step['operation_ids'] = receipt['operation_ids']
                        step['state'] = 'submitted'
        finally:
            async with self.lock:
                self._submitting = None
                await self._safe_save(ctx)


if __name__ == '__main__':
    WorldTools().run()
