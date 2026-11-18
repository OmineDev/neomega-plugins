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
        if type(raw) is int:
            slot = raw
        elif type(raw) is str and raw.isascii() and raw.isdigit() and (raw == '0' or not raw.startswith('0')):
            slot = int(raw)
        else:
            raise ValueError('invalid container slot')
        if not 0 <= slot < limit or not isinstance(item, dict):
            raise ValueError('invalid container slot or item')
        key = str(slot)
        if key in result:
            raise ValueError('duplicate container slot')
        canonical(item)
        result[key] = item
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
    if not isinstance(value, dict):
        raise ValueError('snapshot must be an object')
    body = {k: v for k, v in value.items() if k != 'sha256'}
    if body.get('format') != 'neomega.world.snapshot.v1' or digest(body) != value.get('sha256'):
        raise ValueError('snapshot format or digest mismatch')
    region = body.get('region')
    if (not isinstance(region, dict) or 'origin' not in region or 'size' not in region
            or not isinstance(body.get('cells'), dict) or not isinstance(body.get('losses'), list)):
        raise ValueError('snapshot structure invalid')
    Region(region['origin'], region['size'])
    return body


_COMMAND_MODES = {'command_block': 0, 'repeating_command_block': 1, 'chain_command_block': 2}
MAX_COMMAND_BYTES = 32768
MAX_CUSTOM_NAME_BYTES = 1024
MAX_INT32 = 2147483647


def _flag(mapping, key):
    if key not in mapping:
        return False
    val = mapping[key]
    if type(val) is bool:
        return val
    if type(val) is int and val in (0, 1):
        return bool(val)
    raise ValueError('invalid boolean flag')


def validate_command_block_fields(fields):
    """Validate command block fields against Host world schema bounds."""
    if not isinstance(fields, dict):
        raise ValueError('command block fields must be an object')
    command = fields.get('command')
    mode = fields.get('mode')
    facing = fields.get('facing')
    tick_delay = fields.get('tick_delay')
    name = fields.get('name')
    if (type(command) is not str or len(command.encode('utf-8')) > MAX_COMMAND_BYTES
            or any(ch in command for ch in '\x00\r\n')):
        raise ValueError('invalid command string')
    if type(mode) is not int or not 0 <= mode <= 2:
        raise ValueError('invalid command block mode')
    if type(facing) is not int or not 0 <= facing <= 5:
        raise ValueError('invalid facing_direction')
    if type(tick_delay) is not int or not 0 <= tick_delay <= MAX_INT32:
        raise ValueError('invalid TickDelay')
    if type(name) is not str or len(name.encode('utf-8')) > MAX_CUSTOM_NAME_BYTES:
        raise ValueError('invalid CustomName')
    for flag_key in ('conditional', 'need_redstone', 'track_output', 'execute_on_first_tick'):
        if type(fields.get(flag_key)) is not bool:
            raise ValueError('invalid boolean command block field')
    return fields


def normalize_block(entry):
    """Record raw block data plus the exact subset supported by native writers."""
    if not isinstance(entry, dict):
        raise ValueError('block entry must be an object')
    foreground = entry.get('foreground')
    if not isinstance(foreground, dict) or not isinstance(foreground.get('name'), str):
        raise ValueError('unobserved foreground cannot be backed up as air')
    states_raw = foreground.get('states')
    if states_raw is not None and not isinstance(states_raw, dict):
        raise ValueError('foreground states must be an object')
    cell = {'foreground': {'name': foreground['name'], 'states': states_raw or {}},
            'raw_entity_data': entry.get('entity_data'), 'background': entry.get('background'),
            'coverage': ['foreground'], 'missing': [], 'nbt': None}
    background = entry.get('background')
    if background and isinstance(background, dict) and background.get('name') not in ('air', 'minecraft:air'):
        cell['missing'].append('background')
    elif background is not None and not isinstance(background, dict):
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
            slot_key = str(item['Slot'])
            if slot_key in slots:
                unsupported = True
                continue
            slots[slot_key] = {'item': {'name': item['Name'], 'val': item.get('Damage', 0)}, 'count': item['Count']}
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
    elif name in _COMMAND_MODES:
        states = states_raw if states_raw is not None else {}
        try:
            fields = {
                'command': raw['Command'] if 'Command' in raw else '',
                'mode': _COMMAND_MODES[name],
                'facing': states['facing_direction'] if 'facing_direction' in states else 0,
                'conditional': _flag(states, 'conditional_bit'),
                'need_redstone': not _flag(raw, 'auto'),
                'tick_delay': raw['TickDelay'] if 'TickDelay' in raw else 0,
                'name': raw['CustomName'] if 'CustomName' in raw else '',
                'track_output': _flag(raw, 'TrackOutput'),
                'execute_on_first_tick': _flag(raw, 'ExecuteOnFirstTick'),
            }
            validate_command_block_fields(fields)
        except ValueError:
            cell['missing'].append('command_configuration')
        else:
            cell['nbt'] = {'kind': 'command_block', 'fields': fields}
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
