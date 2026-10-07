"""Portable exact-coverage snapshots, paginated manifests and differential restore."""
import json
import os
from pathlib import Path

from neomega_runtime.services import service, ServiceRejected
from neomega_world import Region, region_snapshot, verify_snapshot, restore_difference, digest


def atomic_json(path, value):
    encoded = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()
    temporary = path.with_suffix('.pending')
    with temporary.open('wb') as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class SnapshotTasks:
    def snapshot_path(self, task_id, index):
        return self.root / (task_id + '.tile.' + str(index) + '.json')

    def portable_plan(self, spec, task_id, epoch, steps, regions):
        dimension = spec['dimension']
        if type(dimension) is not int:
            raise ServiceRejected('dimension_required')
        region = Region(spec['pos'], spec['size'])
        regions.append(region.as_dict())
        for index, tile in enumerate(region.tiles(edge=2)):
            self.append(steps, '_snapshot_read', {'pos':list(tile.origin), 'size':list(tile.size), 'dimension':dimension})
            steps[-1]['snapshot_index'] = index
        return ['entities','scheduled_ticks','unsupported_nbt_fields_reported_per_tile']

    def restore_plan(self, spec, caller, steps, regions):
        backup = self.owned({'task_id':spec['backup_id']}, caller)
        if backup['kind'] != 'backup' or backup['state'] != 'succeeded' or not backup.get('portable'):
            raise ServiceRejected('portable_backup_not_complete')
        dimension = spec.get('dimension', backup['dimension'])
        if type(dimension) is not int:
            raise ServiceRejected('dimension_required')
        source = Region(backup['args']['pos'],backup['args']['size'])
        destination = Region(spec.get('destination',source.origin),source.size)
        regions.append(destination.as_dict())
        losses = set(['entities','scheduled_ticks'])
        for step in backup['steps']:
            if step['method'] != '_snapshot_read':
                continue
            saved = json.loads(self.snapshot_path(backup['task_id'],step['snapshot_index']).read_text())
            body = verify_snapshot(saved)
            losses.update(body['losses'])
            if any(c.get('nbt') and c['nbt']['kind']=='container' and c['nbt']['slots'] for c in body['cells'].values()) and not all(k in spec for k in ('anvil_pos','workspace_pos')):
                raise ServiceRejected('container_workspace_required')
            target = [d+s-o for d,s,o in zip(destination.origin,body['region']['origin'],source.origin)]
            self.append(steps,'_restore_read',{'pos':target,'size':body['region']['size'],'dimension':dimension})
            steps[-1].update(backup_id=backup['task_id'], snapshot_index=step['snapshot_index'], snapshot_sha256=saved['sha256'])
        return sorted(losses), dimension

    async def snapshot_completed(self, ctx, task, step, receipt):
        if step['method'] not in ('_snapshot_read','_restore_read'):
            return
        observed = region_snapshot(receipt['result'], dimension=step['args']['dimension'],epoch=task['epoch'])
        if step['method'] == '_snapshot_read':
            atomic_json(self.snapshot_path(task['task_id'],step['snapshot_index']),observed)
            step['snapshot'] = {k:v for k,v in observed.items() if k != 'cells'}
            task['losses'] = sorted(set(task['losses']) | set(observed['losses']))
        else:
            saved = json.loads(self.snapshot_path(step['backup_id'],step['snapshot_index']).read_text())
            body = verify_snapshot(saved)
            if saved['sha256'] != step['snapshot_sha256']:
                raise ValueError('prepared snapshot changed')
            changes = []
            for key, cell in body['cells'].items():
                current = observed['cells'][key]
                if not restore_difference(cell,current):
                    continue
                relative = [int(v) for v in key.split(',')]
                pos = [a+b for a,b in zip(step['args']['pos'],relative)]
                dimension = step['args']['dimension']
                nbt = cell['nbt']
                # Clearing first is necessary for exact empty-slot and absent-NBT recovery.
                reset = nbt is not None and nbt['kind']=='container' or ('nbt_absent' in cell['coverage'] and current['raw_entity_data'] is not None)
                if reset:
                    self.append(changes,'set_block',{'pos':pos,'block':'minecraft:air','states':{},'dimension':dimension})
                if reset or cell['foreground'] != current['foreground']:
                    self.append(changes,'set_block',{'pos':pos,'block':cell['foreground']['name'],'states':cell['foreground']['states'],'dimension':dimension})
                if nbt and nbt['kind']=='container' and nbt['slots']:
                    if not all(k in task['args'] for k in ('anvil_pos','workspace_pos')):
                        raise ServiceRejected('container_workspace_required')
                    slots = list(nbt['slots'].items())
                    for offset in range(0,len(slots),9):
                        self.append(changes,'container',{'pos':pos,'slots':dict(slots[offset:offset+9]),
                            'anvil_pos':task['args']['anvil_pos'],'workspace_pos':task['args']['workspace_pos'], 'dimension':dimension})
                elif nbt and nbt['kind']=='command_block':
                    self.append(changes,'command_block',dict(nbt['fields'],pos=pos,dimension=dimension))
            if len(task['steps']) + len(changes) > ctx.config.max_steps:
                raise ServiceRejected('restore_step_limit')
            task['steps'][task['cursor']+1:task['cursor']+1] = changes
            step['difference'] = {'changed_operations':len(changes),'compared_cells':len(body['cells']),
                                  'source_sha256':saved['sha256'],'observed_sha256':observed['sha256']}
        # Persist large readbacks in immutable tile files, not Host state or service output.
        receipt['result'] = {'snapshot_sha256':observed['sha256'],'region':observed['region'],
                             'losses':observed['losses'],'cell_count':len(observed['cells'])}

    @service('snapshot', with_context=True)
    async def snapshot_read(self, ctx, args, call):
        async with self.lock:
            task = self.owned(args,call)
            index = args.get('index',0)
            if type(index) is not int or index < 0:
                raise ServiceRejected('invalid_page')
            path = self.snapshot_path(task['task_id'],index)
            if not path.is_file():
                raise ServiceRejected('snapshot_not_found')
            value = json.loads(path.read_text())
            verify_snapshot(value)
            # Native NBT may exceed one service response; expose bounded JSON byte slices.
            raw = json.dumps(value,ensure_ascii=False,separators=(',',':')).encode()
            offset = args.get('offset',0)
            if type(offset) is not int or not 0 <= offset <= len(raw):
                raise ServiceRejected('invalid_page')
            import base64
            return {'task_id':task['task_id'],'index':index,'offset':offset,'length':len(raw),
                    'sha256':value['sha256'],'data_base64':base64.b64encode(raw[offset:offset+16384]).decode()}
