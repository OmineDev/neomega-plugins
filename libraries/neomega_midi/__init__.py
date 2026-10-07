"""Bounded Standard MIDI File parser and Bedrock note mapping (no IO)."""
from dataclasses import dataclass
import struct


@dataclass(frozen=True)
class Note:
    time: float
    duration: float
    pitch: int
    velocity: int
    channel: int
    program: int


@dataclass(frozen=True)
class Sequence:
    notes: tuple[Note, ...]
    duration: float
    ticks_per_beat: int

    def __iter__(self):
        return iter(self.notes)


def sound_for(program: int, channel: int = 0, pitch: int = 60) -> str:
    """Approximate GM instrument families using vanilla note-block sounds."""
    if not 0 <= program <= 127 or not 0 <= channel <= 15 or not 0 <= pitch <= 127:
        raise ValueError('invalid MIDI instrument')
    if channel == 9:
        return 'note.bd' if pitch < 38 else ('note.snare' if pitch < 44 else 'note.hat')
    return ('note.harp', 'note.bell', 'note.flute', 'note.guitar',
            'note.bass', 'note.chime', 'note.flute', 'note.flute',
            'note.flute', 'note.flute', 'note.pling', 'note.chime',
            'note.harp', 'note.bell', 'note.hat', 'note.pling')[program // 8]


def pitch_multiplier(pitch: int) -> float:
    if not 0 <= pitch <= 127:
        raise ValueError('invalid MIDI pitch')
    return 2.0 ** ((pitch - 66) / 12.0)


def parse(data: bytes, *, max_bytes=1048576, max_events=100000, max_duration=3600) -> Sequence:
    """Parse SMF formats 0/1, PPQN and SMPTE clocks; reject asynchronous format 2.

    Note durations reflect explicit note-offs. Sustain/controller envelopes are not
    synthesized: Bedrock note-block sounds are one-shots.
    """
    if not isinstance(data, bytes) or len(data) > max_bytes or data[:4] != b'MThd' or len(data) < 14:
        raise ValueError('invalid MIDI header or size')
    size = int.from_bytes(data[4:8], 'big')
    if size < 6 or 8 + size > len(data):
        raise ValueError('truncated MIDI header')
    fmt, count, division = struct.unpack('>HHH', data[8:14])
    if fmt not in (0, 1) or not 1 <= count <= 256 or (fmt == 0 and count != 1) or division == 0:
        raise ValueError('unsupported MIDI format or timing')
    pos, events, order = 8 + size, [], 0
    for track in range(count):
        if data[pos:pos + 4] != b'MTrk' or pos + 8 > len(data):
            raise ValueError('missing MIDI track')
        length = int.from_bytes(data[pos + 4:pos + 8], 'big')
        pos += 8
        end = pos + length
        if end > len(data):
            raise ValueError('truncated MIDI track')
        tick, running, ended = 0, None, False
        def take(n):
            nonlocal pos
            if pos + n > end:
                raise ValueError('truncated MIDI event')
            result = data[pos:pos + n]
            pos += n
            return result
        def vlq():
            value = 0
            for _ in range(4):
                byte = take(1)[0]
                value = (value << 7) | (byte & 127)
                if byte < 128:
                    return value
            raise ValueError('invalid MIDI variable integer')
        while pos < end:
            tick += vlq()
            first = take(1)[0]
            if first < 128:
                if running is None:
                    raise ValueError('missing MIDI running status')
                status = running
                pos -= 1
            else:
                status = first
            if status == 255:
                kind = take(1)[0]
                body = take(vlq())
                if kind == 81:
                    if len(body) != 3 or int.from_bytes(body, 'big') == 0:
                        raise ValueError('invalid MIDI tempo')
                    events.append((tick, order, 'tempo', int.from_bytes(body, 'big')))
                if kind == 47:
                    if body:
                        raise ValueError('invalid end-of-track')
                    events.append((tick, order, 'end', track))
                    ended = True
                    pos = end
            elif status in (240, 247):
                running = None
                take(vlq())
            elif 128 <= status < 240:
                running = status
                body = take(1 if status >> 4 in (12, 13) else 2)
                if any(x >= 128 for x in body):
                    raise ValueError('invalid MIDI data byte')
                events.append((tick, order, status, body))
            else:
                raise ValueError('unsupported MIDI system event')
            order += 1
            if order > max_events:
                raise ValueError('MIDI event limit exceeded')
        if not ended:
            raise ValueError('missing MIDI end-of-track')
    if pos != len(data):
        raise ValueError('unexpected trailing MIDI bytes')
    if division & 32768:
        fps = 256 - (division >> 8)
        subticks = division & 255
        if fps not in (24, 25, 29, 30) or not subticks:
            raise ValueError('invalid SMPTE clock')
        fixed = 1 / ((30000 / 1001 if fps == 29 else fps) * subticks)
    else:
        fixed = None
    now, previous, tempo = 0.0, 0, 500000
    programs, active, notes = [0] * 16, {}, []
    for tick, _, status, body in sorted(events, key=lambda e: (e[0], e[1])):
        now += (tick - previous) * (fixed if fixed else tempo / 1000000 / division)
        previous = tick
        if now > max_duration:
            raise ValueError('MIDI duration limit exceeded')
        if status == 'tempo':
            tempo = body
            continue
        if status == 'end':
            continue
        channel, kind = status & 15, status >> 4
        if kind == 12:
            programs[channel] = body[0]
        elif kind in (8, 9):
            key = (channel, body[0])
            if kind == 9 and body[1]:
                active.setdefault(key, []).append((now, body[1], programs[channel]))
            elif active.get(key):
                start, velocity, program = active[key].pop(0)
                notes.append(Note(start, now - start, body[0], velocity, channel, program))
    for (channel, pitch), pending in active.items():
        for start, velocity, program in pending:
            notes.append(Note(start, now - start, pitch, velocity, channel, program))
    return Sequence(tuple(sorted(notes, key=lambda n: n.time)), now, division)
