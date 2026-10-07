"""Pure text and geometry helpers; no runtime, network or world side effects."""
from dataclasses import dataclass
import json
import math
import re
import string

__version__ = '1.0.0'
STYLES = {'info': '§b', 'warn': '§e', 'success': '§a', 'failure': '§c',
          'error': '§4', 'input': '§d', 'select': '§6'}
BIG_CHARACTERS = {chr(code): chr(code + 0xFEE0) for code in range(33, 127)}


def escape_text(value):
    """Remove Minecraft styling/control codes from untrusted inserted text."""
    return re.sub(r'§.', '', str(value)).replace('§', '').replace('\r', '').replace('\x00', '')


def template(pattern, values, *, escape=True):
    """Named placeholders only; no attribute traversal or implicit missing values."""
    output = []
    for literal, name, spec, conversion in string.Formatter().parse(pattern):
        output.append(literal)
        if name is not None:
            if not name.isidentifier() or spec or conversion:
                raise ValueError('only simple named placeholders are supported')
            value = values[name]
            output.append(escape_text(value) if escape else str(value))
    return ''.join(output)


def styled(kind, text, *, theme=None):
    styles = STYLES if theme is None else {**STYLES, **theme}
    return styles[kind] + escape_text(text) + '§r'


def rawtext(text):
    return {'rawtext': [{'text': str(text)}]}


def render(text, *, channel='chat'):
    if channel not in ('chat', 'actionbar', 'title'):
        raise ValueError('unsupported text channel')
    return json.dumps(rawtext(text), ensure_ascii=False, separators=(',', ':'))


def big_text(text, *, mapping=None):
    """Fullwidth ASCII by default; unknown Unicode is preserved unchanged."""
    table = BIG_CHARACTERS if mapping is None else {**BIG_CHARACTERS, **mapping}
    return ''.join(table.get(char, char) for char in text)


def item_name(identifier, *, names=None):
    """Use an explicitly supplied localization table; preserve unknown item IDs."""
    if not isinstance(identifier, str) or not re.fullmatch(r'[a-z0-9_.-]+:[a-z0-9_./-]+', identifier):
        raise ValueError('expected namespaced item identifier')
    return (names or {}).get(identifier, identifier)


@dataclass(frozen=True)
class Vec3:
    x: float
    y: float
    z: float

    def __post_init__(self):
        if any(not math.isfinite(v) for v in (self.x, self.y, self.z)):
            raise ValueError('coordinates must be finite')

    def __add__(self, other):
        return Vec3(self.x + other.x, self.y + other.y, self.z + other.z)

    def __sub__(self, other):
        return Vec3(self.x - other.x, self.y - other.y, self.z - other.z)

    def distance(self, other):
        delta = self - other
        return math.sqrt(delta.x ** 2 + delta.y ** 2 + delta.z ** 2)


@dataclass(frozen=True)
class Region:
    minimum: Vec3
    maximum: Vec3
    dimension: str = 'overworld'

    def __post_init__(self):
        if any(a > b for a, b in zip(vars(self.minimum).values(), vars(self.maximum).values())):
            raise ValueError('region minimum exceeds maximum')

    def contains(self, point):
        return all(a <= p <= b for a, p, b in zip(vars(self.minimum).values(), vars(point).values(), vars(self.maximum).values()))

    def intersection(self, other):
        if self.dimension != other.dimension:
            return None
        low = Vec3(*(max(a, b) for a, b in zip(vars(self.minimum).values(), vars(other.minimum).values())))
        high = Vec3(*(min(a, b) for a, b in zip(vars(self.maximum).values(), vars(other.maximum).values())))
        return None if any(a > b for a, b in zip(vars(low).values(), vars(high).values())) else Region(low, high, self.dimension)


def yaw_index(yaw, count):
    if not math.isfinite(yaw) or type(count) is not int or count < 1:
        raise ValueError('invalid yaw or option count')
    return int(((yaw % 360) / 360) * count) % count


def operation_result(receipt):
    """Preserve Host state; acceptance and dispatch never imply success."""
    state = receipt['state']
    return {'state': state, 'completed': state == 'succeeded',
            'operation_id': receipt.get('operation_id'), 'error': receipt.get('error'),
            'result': receipt.get('result')}
