"""Durable region/NBT tasks; every world action belongs to one retained commit."""
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import time
import uuid

from neomega_runtime import Plugin
from neomega_runtime.services import service, ServiceRejected
from neomega_world import Region, position, digest, container_slots
from snapshot_tasks import SnapshotTasks, atomic_json
from fatalder_adapter import FatalderTasks


def deadline():
    return (datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat()


@dataclass(frozen=True)
class Settings:
    max_tasks: int = 256
    max_blocks: int = 262144
    max_steps: int = 1024
    max_state_bytes: int = 33554432

    def __post_init__(self):
        if not 1 <= self.max_tasks <= 4096 or not 1 <= self.max_blocks <= 1048576 or not 1 <= self.max_steps <= 1024:
            raise ValueError('invalid task limits')
        if not 1048576 <= self.max_state_bytes <= 268435456:
            raise ValueError('state byte budget must be 1..256 MiB')


class WorldTools(SnapshotTasks, FatalderTasks, Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = None
        self.tick_cursor = 0
        self.tasks = {}

    async def on_maintenance(self, ctx, request):
        async with self.lock:
            token = request['token']
            if self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            if request['operation'] == 'seal':
                for task in self.tasks.values():
                    if task.get('remote_calls'):
                        await self.reconcile_fatalder(ctx,task)
                    if task.get('remote_calls') and any(r['method'] != 'status' and r['state'] in ('unknown','pending') for r in task['remote_calls']):
                        return {'status':'busy','token':token}
                    if task['state'] in ('running', 'unknown', 'cancelling'):
                        await self.reconcile(ctx, task)
                        if task['cursor'] < len(task['steps']) and task['steps'][task['cursor']]['state'] in ('unknown', 'submitted'):
                            return {'status': 'busy', 'token': token}
                await self.save(ctx)
                self.sealed = token
                return {'status': 'sealed', 'token': token}
            self.sealed = None
            return {'status': 'released', 'token': token}

    def writable(self):
        if self.sealed is not None:
            raise ServiceRejected('maintenance_sealed')
        if any(step['state']=='unknown' and not step['operation_ids'] for task in self.tasks.values() for step in task['steps']):
            raise ServiceRejected('admission_unknown')

    async def on_start(self, ctx):
        self.root = ctx.data_dir / 'world_records'
        self.root.mkdir(parents=True,exist_ok=True)
        self.tasks = {p.name.split('.')[0]:json.loads(p.read_text()) for p in self.root.glob('*.task.json')}
        self.persisted = {k:digest(v) for k,v in self.tasks.items()}
        ctx.every(0.5, lambda: self.tick(ctx), name='world-task-pump')

    async def save(self, ctx):
        if sum(len(json.dumps(t,ensure_ascii=False).encode()) for t in self.tasks.values()) > ctx.config.max_state_bytes:
            raise ServiceRejected('state_byte_budget')
        for key,task in self.tasks.items():
            checksum = digest(task)
            if getattr(self,'persisted',{}).get(key) != checksum:
                atomic_json(self.root / (key+'.task.json'),task)
                self.persisted[key] = checksum

    def owned(self, args, call):
        task = self.tasks.get(args['task_id'])
        if task is None or task['owner'] != call.installation_id:
            raise ServiceRejected('task_not_found')
        return task

    @staticmethod
    def public(task, offset=0, limit=2):
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 2:
            raise ServiceRejected('invalid_page')
        result = {k: v for k, v in task.items() if k not in ('owner', 'steps', 'args', 'regions', 'remote_calls','losses')}
        result.update(losses=task.get('losses',[])[offset:offset+32],loss_count=len(task.get('losses',[])),loss_offset=offset)
        if 'remote_calls' in task:
            result['remote_call_count'] = len(task['remote_calls'])
            result['remote_calls'] = [{k:v for k,v in r.items() if k not in ('intent','receipt')} for r in task['remote_calls'][offset:offset+limit]]
        steps = []
        for step in task['steps'][offset:offset+limit]:
            visible = dict(step)
            if 'receipts' in visible:
                visible['receipts'] = [dict(r, result=({'sha256':digest(r['result']),'detail':'operation_readback'} if len(json.dumps(r.get('result')).encode())>8192 else r.get('result'))) for r in visible['receipts']]
            if len(json.dumps(visible['args']).encode()) > 4096:
                visible['args'] = {'sha256': digest(step['args']), 'detail': 'original_request'}
            steps.append(visible)
        result.update(steps=steps, step_count=len(task['steps']),
                      step_offset=offset, region_count=len(task['regions']), regions=task['regions'][offset:offset+limit])
        return result

    @staticmethod
    def append(steps, method, args):
        steps.append({'method': method, 'args': args, 'state': 'prepared',
                      'commit_id': uuid.uuid4().hex, 'operation_ids': []})

    async def epoch(self, ctx):
        data = (await ctx.peer.call('capabilities.get', {})).get('data', {})
        if not data.get('world_epoch'):
            raise ServiceRejected('world_epoch_unavailable')
        return data['world_epoch']

    async def create(self, ctx, args, call):
        self.writable()
        request_key = args.get('request_key')
        if not isinstance(request_key, str) or not 1 <= len(request_key) <= 128:
            raise ServiceRejected('request_key_required')
        ident = hashlib.sha256((call.installation_id + '\0' + request_key).encode()).hexdigest()
        fingerprint = digest(args)
        if ident in self.tasks:
            task = self.tasks[ident]
            if task['fingerprint'] != fingerprint:
                raise ServiceRejected('request_key_conflict')
            return self.public(task)
        if len(self.tasks) >= ctx.config.max_tasks:
            raise ServiceRejected('task_limit')
        kind, spec = args['kind'], args.get('args', {})
        epoch = await self.epoch(ctx)
        steps, regions, losses = [], [], []
        portable = kind == 'backup' and spec.get('mode','snapshot') == 'snapshot'
        dimension = spec.get('dimension')
        if portable:
            if Region(spec['pos'],spec['size']).volume > ctx.config.max_blocks:
                raise ServiceRejected('region_too_large')
            losses = self.portable_plan(spec,ident,epoch,steps,regions)
        elif kind == 'restore' and spec.get('mode','snapshot') == 'snapshot':
            losses, dimension = self.restore_plan(spec,call,steps,regions)
        elif kind in ('backup', 'copy'):
            region = Region(spec['pos'], spec['size'])
            if region.volume > ctx.config.max_blocks:
                raise ServiceRejected('region_too_large')
            regions.append(region.as_dict())
            destination = position(spec['destination']) if kind == 'copy' else None
            if destination:
                regions.append(Region(destination, region.size).as_dict())
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
                losses += ['portable_bdx_excludes_air_clearing', 'portable_bdx_excludes_background_layers', 'portable_bdx_excludes_non_command_block_entity_nbt']
        elif kind == 'restore':
            backup = self.owned({'task_id': spec['backup_id']}, call)
            if backup['kind'] != 'backup' or backup['state'] != 'succeeded':
                raise ServiceRejected('backup_not_complete')
            origin = position(backup['args']['pos'])
            destination = position(spec.get('destination', origin))
            region = Region(destination, backup['args']['size'])
            regions.append(region.as_dict())
            mode = spec.get('mode', 'structure')
            if mode not in ('structure', 'bdx'):
                raise ServiceRejected('invalid_restore_mode')
            if mode == 'structure' and backup['epoch'] != epoch:
                raise ServiceRejected('structure_epoch_expired')
            for step in backup['steps']:
                target = [d + t - o for d, t, o in zip(destination, step['args']['pos'], origin)]
                if mode == 'structure' and step['method'] == 'save_structure':
                    self.append(steps, 'load_structure', {'name': step['args']['name'], 'pos': target})
                if mode == 'bdx' and step['method'] == 'export_bdx':
                    result = step['receipts'][0]['result']
                    artifact = result['artifact']['artifact_id']
                    self.append(steps, 'bdx_artifact', {'artifact_id': artifact, 'pos': target})
            losses = backup['losses'] if mode == 'bdx' else ['entities', 'scheduled_ticks']
        elif kind == 'container':
            slots = container_slots(spec['slots'], double_chest=bool(spec.get('second_pos')))
            first = position(spec['pos'])
            second = position(spec['second_pos']) if spec.get('second_pos') else None
            for offset, pos in ((0, first), (27, second)):
                if pos is None:
                    continue
                local = {str(int(k)-offset): v for k, v in slots.items() if offset <= int(k) < offset+27}
                items = list(local.items())
                for i in range(0, len(items), 9):
                    fields = {'pos': list(pos), 'slots': dict(items[i:i+9]),
                              'anvil_pos': list(position(spec['anvil_pos'])), 'workspace_pos': list(position(spec['workspace_pos']))}
                    if 'block' in spec:
                        fields['block'] = spec['block']
                    self.append(steps, 'container', fields)
                regions.append(Region(pos, (1,1,1)).as_dict())
            losses = ['unmentioned_slots_unchanged']
        elif kind in ('command_block', 'sign', 'item', 'enhanced_item', 'structure_block', 'pick_block', 'place_block', 'held_on_block', 'item_frame', 'inspect'):
            method = 'block' if kind == 'inspect' else kind
            self.append(steps, method, dict(spec))
            if 'pos' in spec:
                regions.append(Region(spec['pos'], (1,1,1)).as_dict())
        else:
            raise ServiceRejected('unsupported_task_kind')
        for field in ('workspace_pos', 'anvil_pos'):
            if field in spec:
                regions.append(Region(spec[field], (1,1,1)).as_dict())
        if not steps or len(steps) > ctx.config.max_steps:
            raise ServiceRejected('step_limit')
        # Validate all SDK intent arguments before storing a runnable plan.
        for step in steps:
            self.intent(ctx, step)
        task = {'task_id': ident, 'owner': call.installation_id, 'fingerprint': fingerprint,
                'kind': kind, 'args': spec, 'epoch': epoch, 'state': 'prepared',
                'created_at': time.time(), 'steps': steps, 'regions': regions, 'losses': losses,
                'cursor': 0, 'cancel_requested': False, 'portable':portable, 'dimension':dimension}
        self.tasks[ident] = task
        await self.save(ctx)
        return self.public(task)

    @staticmethod
    def intent(ctx, step):
        method = 'region' if step['method'] in ('_snapshot_read','_restore_read') else step['method']
        facade = ctx.queries if method in ('block','region') else ctx.world
        return getattr(facade, method)(**step['args'], idempotency_key=step['commit_id'], deadline=deadline())

    @service('prepare', with_context=True)
    async def prepare(self, ctx, args, call):
        async with self.lock:
            return await self.create(ctx, args, call)

    @service('inspect', with_context=True)
    async def inspect(self, ctx, args, call):
        async with self.lock:
            return await self.create(ctx, {'kind': 'inspect', 'args': {'pos': args['pos']},
                                           'request_key': args['request_key']}, call)

    def acquire(self, task):
        for other in self.tasks.values():
            if other['task_id'] == task['task_id'] or other['state'] not in ('running', 'unknown', 'cancelling'):
                continue
            # One robot hotbar/workspace writer at a time, plus overlapping regions.
            if task['kind'] in ('container','item','enhanced_item','pick_block','place_block','sign','held_on_block','item_frame') or other['kind'] in ('container','item','enhanced_item','pick_block','place_block','sign','held_on_block','item_frame'):
                raise ServiceRejected('workspace_busy')
            if any(Region(a['origin'], a['size']).overlaps(Region(b['origin'], b['size'])) for a in task['regions'] for b in other['regions']):
                raise ServiceRejected('region_busy')

    @service('execute', with_context=True)
    async def execute(self, ctx, args, call):
        async with self.lock:
            self.writable()
            task = self.owned(args, call)
            if task['state'] == 'prepared':
                if task['epoch'] != await self.epoch(ctx):
                    raise ServiceRejected('world_epoch_expired')
                self.acquire(task)
                task['state'] = 'running'
                await self.save(ctx)
            return self.public(task)

    @service('status', with_context=True)
    async def status(self, ctx, args, call):
        async with self.lock:
            task = self.owned(args, call)
            if task['state'] in ('running', 'unknown', 'cancelling'):
                await self.reconcile(ctx, task)
                await self.save(ctx)
            return self.public(task, args.get('offset', 0), args.get('limit', 2))

    @service('cancel', with_context=True)
    async def cancel(self, ctx, args, call):
        async with self.lock:
            task = self.owned(args, call)
            task['cancel_requested'] = True
            if task['state'] == 'prepared':
                task['state'] = 'cancelled'
            elif task['state'] == 'running':
                task['state'] = 'cancelling'
            # Already submitted actions finish/reconcile; no rollback claim.
            await self.save(ctx)
            return self.public(task)

    @service('resume', with_context=True)
    async def resume(self, ctx, args, call):
        async with self.lock:
            self.writable()
            task = self.owned(args, call)
            await self.reconcile(ctx, task)
            # Unknown and failed actions are never replaced or resubmitted.
            if task['state'] == 'cancelled' and task['cursor'] < len(task['steps']) and task['steps'][task['cursor']]['state'] == 'prepared':
                if task['epoch'] != await self.epoch(ctx):
                    raise ServiceRejected('world_epoch_expired')
                self.acquire(task)
                task.update(state='running', cancel_requested=False)
            await self.save(ctx)
            return self.public(task)

    async def reconcile(self, ctx, task):
        if task['cursor'] >= len(task['steps']):
            return
        step = task['steps'][task['cursor']]
        if step['state'] == 'prepared':
            if task['cancel_requested']:
                task['state'] = 'cancelled'
            return
        if not step['operation_ids']:
            found = await ctx.storage.receipt(step['commit_id'])
            if found is None:
                task['state'] = 'unknown'
                return
            step['operation_ids'] = found['operation_ids']
        step['receipts'] = [await ctx.operations.get(opid) for opid in step['operation_ids']]
        states = [r['state'] for r in step['receipts']]
        if 'unknown' in states:
            step['state'] = task['state'] = 'unknown'
        elif 'failed' in states or 'cancelled' in states:
            step['state'] = task['state'] = 'failed'
        elif states and all(s == 'succeeded' for s in states):
            try:
                await self.snapshot_completed(ctx,task,step,step['receipts'][0])
            except Exception as exc:
                step['state'] = task['state'] = 'failed'
                step['error'] = str(exc)
                return
            step['state'] = 'succeeded'
            task['cursor'] += 1
            task['state'] = 'succeeded' if task['cursor'] == len(task['steps']) else ('cancelled' if task['cancel_requested'] else 'running')
        else:
            task['state'] = 'cancelling' if task['cancel_requested'] else 'running'

    async def tick(self, ctx):
        async with self.lock:
            if self.sealed is not None:
                return
            for task in self.tasks.values():
                if task['state'] not in ('running', 'unknown', 'cancelling'):
                    continue
                await self.reconcile(ctx, task)
                if any(s['state']=='unknown' and not s['operation_ids'] for t in self.tasks.values() for s in t['steps']):
                    continue
                if task['state'] != 'running' or task['cursor'] >= len(task['steps']):
                    continue
                step = task['steps'][task['cursor']]
                if step['state'] != 'prepared':
                    continue
                if task['epoch'] != await self.epoch(ctx):
                    task['state'] = 'epoch_expired'
                    continue
                intent = self.intent(ctx, step)
                if step['method'] == 'bdx_artifact':
                    await ctx.artifacts.retain(step['args']['artifact_id'], step['commit_id'])
                # Persist an uncertain marker before admission; a crash can never replay it.
                step['state'] = 'unknown'
                await self.save(ctx)
                tx = await ctx.storage.transaction(commit_id=step['commit_id'],keys=[])
                tx.set('world_action_'+step['commit_id'],{'task_id':task['task_id'],'cursor':task['cursor'],'state':'admitted'})
                tx.action(intent)
                try:
                    receipt = await tx.save()
                except Exception as exc:
                    task['state'] = 'unknown'
                    step['error'] = type(exc).__name__
                else:
                    step['operation_ids'] = receipt['operation_ids']
                    step['state'] = 'submitted'
                break
            await self.save(ctx)


if __name__ == '__main__':
    WorldTools().run()
