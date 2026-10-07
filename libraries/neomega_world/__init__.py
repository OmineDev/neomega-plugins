"""Pure bounded world models: no connection, permissions or global state."""
import hashlib
import json
from dataclasses import dataclass


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def position(value):
    if not isinstance(value, (list, tuple)) or len(value) != 3 or any(type(x) is not int or not -(2**31) <= x < 2**31 for x in value):
        raise ValueError('position must be three int32 values')
    return tuple(value)


@dataclass(frozen=True)
class Region:
    origin: tuple
    size: tuple

    def __post_init__(self):
        object.__setattr__(self, 'origin', position(self.origin))
        object.__setattr__(self, 'size', position(self.size))
        if any(x <= 0 for x in self.size):
            raise ValueError('region size must be positive')
        position([a + b - 1 for a, b in zip(self.origin, self.size)])

    @property
    def volume(self):
        x, y, z = self.size
        return x * y * z

    def tiles(self, edge=16):
        if type(edge) is not int or not 1 <= edge <= 16:
            raise ValueError('tile edge must be 1..16')
        for x in range(0, self.size[0], edge):
            for y in range(0, self.size[1], edge):
                for z in range(0, self.size[2], edge):
                    offset = (x, y, z)
                    yield Region(tuple(a + b for a, b in zip(self.origin, offset)),
                                 tuple(min(edge, n - o) for n, o in zip(self.size, offset)))

    def overlaps(self, other):
        return all(a < b + m and b < a + n for a, n, b, m in zip(self.origin, self.size, other.origin, other.size))

    def as_dict(self):
        return {'origin': list(self.origin), 'size': list(self.size), 'volume': self.volume}


def container_slots(slots, *, double_chest=False):
    """Validate canonical slot map. Double chests use 0..26 left, 27..53 right."""
    if not isinstance(slots, dict):
        raise ValueError('slots must be an object')
    limit = 54 if double_chest else 27
    result = {}
    for raw, item in slots.items():
        try:
            slot = int(raw)
        except (ValueError, TypeError):
            raise ValueError('invalid container slot') from None
        if str(slot) != str(raw) or not 0 <= slot < limit or not isinstance(item, dict):
            raise ValueError('invalid container slot or item')
        canonical(item)
        result[str(slot)] = item
    return result


def differences(before, after):
    """Compare explicitly observed position-keyed cells; absence stays unknown."""
    result = []
    for key in sorted(before.keys() | after.keys()):
        if key not in before or key not in after:
            result.append({'position': key, 'state': 'unknown', 'before': before.get(key), 'after': after.get(key)})
        elif canonical(before[key]) != canonical(after[key]):
            result.append({'position': key, 'state': 'changed', 'before': before[key], 'after': after[key]})
    return result


def snapshot(region, cells, *, dimension, epoch, losses=()):
    body = {'format': 'neomega.world.snapshot.v1', 'region': region.as_dict(),
            'dimension': dimension, 'epoch': epoch, 'cells': cells, 'losses': list(losses)}
    return dict(body, sha256=digest(body))


def verify_snapshot(value):
    body = {k: v for k, v in value.items() if k != 'sha256'}
    if body.get('format') != 'neomega.world.snapshot.v1' or digest(body) != value.get('sha256'):
        raise ValueError('snapshot format or digest mismatch')
    Region(body['region']['origin'], body['region']['size'])
    return body


def normalize_block(entry):
    """Record raw block data plus the exact subset supported by native writers."""
    foreground = entry.get('foreground')
    if not isinstance(foreground, dict) or not isinstance(foreground.get('name'), str):
        raise ValueError('unobserved foreground cannot be backed up as air')
    cell = {'foreground': {'name': foreground['name'], 'states': foreground.get('states') or {}},
            'raw_entity_data': entry.get('entity_data'), 'background': entry.get('background'),
            'coverage': ['foreground'], 'missing': [], 'nbt': None}
    background = entry.get('background')
    if background and background.get('name') not in ('air', 'minecraft:air'):
        cell['missing'].append('background')
    raw = entry.get('entity_data')
    if raw is None:
        cell['coverage'].append('nbt_absent')
        return cell
    if not isinstance(raw, dict):
        cell['missing'].append('entity_data')
        return cell
    name = foreground['name'].removeprefix('minecraft:')
    if name in ('chest', 'trapped_chest', 'barrel') and isinstance(raw.get('Items'), list):
        slots = {}
        unsupported = False
        for item in raw['Items']:
            if not isinstance(item, dict) or set(item) - {'Name','Count','Damage','Slot','tag','WasPickedUp'} or item.get('tag'):
                unsupported = True
                continue
            if type(item.get('Damage',0)) is not int or not 0 <= item.get('Damage',0) <= 32767 or not isinstance(item.get('Name'), str) or type(item.get('Count')) is not int or not 1 <= item['Count'] <= 64 or type(item.get('Slot')) is not int or not 0 <= item['Slot'] < 27:
                unsupported = True
                continue
            slots[str(item['Slot'])] = {'item': {'name': item['Name'], 'val': item.get('Damage', 0)}, 'count': item['Count']}
        # Partial inventories never clear/replace a container; only complete supported lists do.
        if unsupported:
            cell['missing'].append('container_items')
        else:
            cell['nbt'] = {'kind': 'container', 'slots': slots}
            cell['coverage'].append('container_items')
        extras = set(raw) - {'Items','id','x','y','z','isMovable','pairx','pairz','pairlead'}
        if extras:
            cell['missing'].extend('entity_data.'+k for k in sorted(extras))
        if 'pairx' in raw or 'pairz' in raw:
            cell['missing'].append('container_pair_topology')
            if 'container_items' in cell['coverage']:
                cell['coverage'].remove('container_items')
            cell['nbt'] = None
            if 'container_items' not in cell['missing']:
                cell['missing'].append('container_items')
    elif name in ('command_block','repeating_command_block','chain_command_block'):
        states = foreground.get('states') or {}
        fields = {'command': raw.get('Command',''), 'mode': {'command_block':0,'repeating_command_block':1,'chain_command_block':2}[name],
                  'facing': states.get('facing_direction',0), 'conditional': bool(states.get('conditional_bit',False)),
                  'need_redstone': not bool(raw.get('auto',False)), 'tick_delay': raw.get('TickDelay',0),
                  'name': raw.get('CustomName',''), 'track_output': bool(raw.get('TrackOutput',False)),
                  'execute_on_first_tick': bool(raw.get('ExecuteOnFirstTick',False))}
        cell['nbt'] = {'kind':'command_block','fields':fields}
        cell['coverage'].append('command_configuration')
        ignored = {'id','x','y','z','isMovable','Command','auto','TickDelay','CustomName','TrackOutput','ExecuteOnFirstTick'}
        cell['missing'].extend('entity_data.'+k for k in sorted(set(raw)-ignored))
    else:
        cell['missing'].append('entity_data')
    return cell


def region_snapshot(dump, *, dimension, epoch):
    region = Region(dump['pos'], dump['size'])
    cells = {}
    missing = set()
    for entry in dump['blocks']:
        absolute = position(entry['abs'])
        relative = tuple(a-b for a,b in zip(absolute,region.origin))
        if any(not 0 <= v < size for v,size in zip(relative,region.size)):
            raise ValueError('cell outside snapshot region')
        key = ','.join(str(v) for v in relative)
        if key in cells:
            raise ValueError('duplicate snapshot cell')
        cells[key] = normalize_block(entry)
        missing.update(cells[key]['missing'])
    if len(cells) != region.volume:
        raise ValueError('incomplete region observation')
    return snapshot(region,cells,dimension=dimension,epoch=epoch,losses=sorted(missing))


def restore_difference(saved, current):
    """Only compare fully covered fields; preserve missing-field disclosure."""
    if saved['foreground'] != current['foreground']:
        return True
    for field in ('container_items','command_configuration'):
        if field in saved['coverage'] and (field not in current['coverage'] or saved['nbt'] != current['nbt']):
            return True
    return 'nbt_absent' in saved['coverage'] and 'nbt_absent' not in current['coverage']
