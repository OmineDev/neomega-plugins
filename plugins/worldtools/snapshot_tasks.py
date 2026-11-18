"""Portable exact-coverage snapshots, paginated manifests and differential restore."""
import base64
import json
import os

from neomega_runtime.services import service, ServiceRejected
from neomega_world import (
    Region,
    container_slots,
    position,
    region_snapshot,
    restore_difference,
    validate_command_block_fields,
    verify_snapshot,
)


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

    def portable_plan(self, spec, task_id, epoch, steps, regions, max_steps=None):
        dimension = spec.get('dimension')
        if type(dimension) is not int or not -(1 << 31) <= dimension < (1 << 31):
            raise ServiceRejected('dimension_required')
        if 'pos' not in spec or 'size' not in spec:
            raise ServiceRejected('invalid_argument')
        try:
            region = Region(spec['pos'], spec['size'])
        except (TypeError, ValueError):
            raise ServiceRejected('invalid_argument') from None
        tile_count = ((region.size[0] + 1) // 2) * ((region.size[1] + 1) // 2) * ((region.size[2] + 1) // 2)
        if max_steps is not None and tile_count > max_steps:
            raise ServiceRejected('step_limit')
        regions.append(region.as_dict())
        for index, tile in enumerate(region.tiles(edge=2)):
            self.append(steps, '_snapshot_read', {'pos': list(tile.origin), 'size': list(tile.size), 'dimension': dimension})
            steps[-1]['snapshot_index'] = index
        return ['entities', 'scheduled_ticks', 'unsupported_nbt_fields_reported_per_tile']

    def restore_plan(self, spec, caller, steps, regions, max_steps=None):
        backup_id = spec.get('backup_id')
        if not isinstance(backup_id, str) or not backup_id:
            raise ServiceRejected('invalid_argument')
        backup = self.owned({'task_id': backup_id}, caller)
        if backup['kind'] != 'backup' or backup['state'] != 'succeeded' or not backup.get('portable'):
            raise ServiceRejected('portable_backup_not_complete')
        dimension = spec.get('dimension', backup['dimension'])
        if type(dimension) is not int or not -(1 << 31) <= dimension < (1 << 31):
            raise ServiceRejected('dimension_required')
        try:
            source = Region(backup['args']['pos'], backup['args']['size'])
            dest_origin = position(spec['destination']) if 'destination' in spec else source.origin
            destination = Region(dest_origin, source.size)
            for key in ('anvil_pos', 'workspace_pos'):
                if key in spec:
                    position(spec[key])
        except (KeyError, TypeError, ValueError):
            raise ServiceRejected('invalid_argument') from None
        regions.append(destination.as_dict())
        losses = {'entities', 'scheduled_ticks'}
        for step in backup['steps']:
            if step['method'] != '_snapshot_read':
                continue
            try:
                saved = json.loads(self.snapshot_path(backup['task_id'], step['snapshot_index']).read_text())
                body = verify_snapshot(saved)
            except FileNotFoundError:
                raise ServiceRejected('snapshot_not_found') from None
            except (OSError, ValueError, KeyError, TypeError):
                raise ServiceRejected('invalid_snapshot') from None
            losses.update(body['losses'])
            for cell in body['cells'].values():
                nbt = cell.get('nbt') if isinstance(cell, dict) else None
                if not isinstance(nbt, dict):
                    continue
                if nbt.get('kind') == 'container' and nbt.get('slots'):
                    try:
                        container_slots(nbt['slots'])
                    except ValueError:
                        raise ServiceRejected('invalid_snapshot') from None
                    if not all(k in spec for k in ('anvil_pos', 'workspace_pos')):
                        raise ServiceRejected('container_workspace_required')
                elif nbt.get('kind') == 'command_block':
                    try:
                        validate_command_block_fields(nbt.get('fields'))
                    except ValueError:
                        raise ServiceRejected('invalid_snapshot') from None
            target = [d + s - o for d, s, o in zip(destination.origin, body['region']['origin'], source.origin)]
            self.append(steps, '_restore_read', {'pos': target, 'size': body['region']['size'], 'dimension': dimension})
            steps[-1].update(backup_id=backup['task_id'], snapshot_index=step['snapshot_index'], snapshot_sha256=saved['sha256'])
            if max_steps is not None and len(steps) > max_steps:
                raise ServiceRejected('step_limit')
        return sorted(losses), dimension

    async def snapshot_completed(self, ctx, task, step, receipt):
        if step['method'] not in ('_snapshot_read', '_restore_read'):
            return
        completion = receipt.get('result') if isinstance(receipt, dict) else None
        payload = completion.get('result') if isinstance(completion, dict) else None
        if not isinstance(payload, dict):
            raise ValueError('malformed operation receipt')
        observed = region_snapshot(payload, dimension=step['args']['dimension'], epoch=task['epoch'])
        if step['method'] == '_snapshot_read':
            atomic_json(self.snapshot_path(task['task_id'], step['snapshot_index']), observed)
            step['snapshot'] = {k: v for k, v in observed.items() if k != 'cells'}
            task['losses'] = sorted(set(task['losses']) | set(observed['losses']))
        else:
            saved = json.loads(self.snapshot_path(step['backup_id'], step['snapshot_index']).read_text())
            body = verify_snapshot(saved)
            if saved['sha256'] != step['snapshot_sha256']:
                raise ValueError('prepared snapshot changed')
            if observed['region'] != body['region']:
                raise ValueError('snapshot region mismatch')
            changes = []
            for key, cell in body['cells'].items():
                if key not in observed['cells']:
                    raise ValueError('snapshot cell mismatch')
                current = observed['cells'][key]
                if not restore_difference(cell, current):
                    continue
                relative = [int(v) for v in key.split(',')]
                pos = [a + b for a, b in zip(step['args']['pos'], relative)]
                dimension = step['args']['dimension']
                nbt = cell['nbt']
                # Clearing first is necessary for exact empty-slot and absent-NBT recovery.
                reset = (
                    (nbt is not None and nbt['kind'] == 'container')
                    or ('nbt_absent' in cell['coverage'] and current['raw_entity_data'] is not None)
                )
                is_air = cell['foreground']['name'] in ('air', 'minecraft:air') and not cell['foreground'].get('states')
                if reset:
                    self.append(changes, 'set_block', {'pos': pos, 'block': 'minecraft:air', 'states': {}, 'dimension': dimension})
                if (reset and not is_air) or (not reset and cell['foreground'] != current['foreground']):
                    self.append(changes, 'set_block', {
                        'pos': pos,
                        'block': cell['foreground']['name'],
                        'states': cell['foreground']['states'],
                        'dimension': dimension,
                    })
                if nbt and nbt['kind'] == 'container' and nbt['slots']:
                    if not all(k in task['args'] for k in ('anvil_pos', 'workspace_pos')):
                        raise ServiceRejected('container_workspace_required')
                    slots = list(nbt['slots'].items())
                    for offset in range(0, len(slots), 9):
                        self.append(changes, 'container', {
                            'pos': pos,
                            'slots': dict(slots[offset:offset + 9]),
                            'anvil_pos': task['args']['anvil_pos'],
                            'workspace_pos': task['args']['workspace_pos'],
                            'dimension': dimension,
                        })
                elif nbt and nbt['kind'] == 'command_block':
                    validate_command_block_fields(nbt['fields'])
                    self.append(changes, 'command_block', dict(nbt['fields'], pos=pos, dimension=dimension))
            if len(task['steps']) + len(changes) > ctx.config.max_steps:
                raise ServiceRejected('restore_step_limit')
            if changes:
                self.check_task_mutation_budget(ctx, task, inserted_steps=changes)
            task['steps'][task['cursor'] + 1:task['cursor'] + 1] = changes
            step['difference'] = {
                'changed_operations': len(changes),
                'compared_cells': len(body['cells']),
                'source_sha256': saved['sha256'],
                'observed_sha256': observed['sha256'],
            }
        # Persist large readbacks in immutable tile files, not Host state or service output.
        completion['result'] = {
            'snapshot_sha256': observed['sha256'],
            'region': observed['region'],
            'losses': observed['losses'],
            'cell_count': len(observed['cells']),
        }

    @service('snapshot', with_context=True)
    async def snapshot_read(self, ctx, args, call):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')
        async with self.lock:
            task = self.owned(args, call)
            index = args.get('index', 0)
            if type(index) is not int or index < 0:
                raise ServiceRejected('invalid_page')
            path = self.snapshot_path(task['task_id'], index)
            if not path.is_file():
                raise ServiceRejected('snapshot_not_found')
            try:
                value = json.loads(path.read_text())
                verify_snapshot(value)
            except (OSError, ValueError, KeyError, TypeError):
                raise ServiceRejected('invalid_snapshot') from None
            # Native NBT may exceed one service response; expose bounded JSON byte slices.
            raw = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()
            offset = args.get('offset', 0)
            if type(offset) is not int or not 0 <= offset <= len(raw):
                raise ServiceRejected('invalid_page')
            return {
                'task_id': task['task_id'],
                'index': index,
                'offset': offset,
                'length': len(raw),
                'sha256': value['sha256'],
                'data_base64': base64.b64encode(raw[offset:offset + 16384]).decode(),
            }
