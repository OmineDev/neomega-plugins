"""Structured display descriptions. Profile-specific limits belong to the provider."""
from dataclasses import dataclass, field
import math
import re


def positive(value: float, name: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError(f'{name} must be finite and positive')
    return value


def identifier(value: str, name: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,96}', value):
        raise ValueError(f'{name} must contain 1..96 ASCII letters, digits, underscore, dot, colon or hyphen')
    return value


@dataclass(frozen=True)
class WorldPosition:
    """World anchor in block coordinates, not renderer-local offsets."""
    dimension: str
    x: float
    y: float
    z: float

    def __post_init__(self):
        identifier(self.dimension, 'dimension')
        for value in (self.x, self.y, self.z):
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError('world coordinates must be finite numbers')

    def to_dict(self) -> dict:
        return dict(dimension=self.dimension, x=self.x, y=self.y, z=self.z)


@dataclass(frozen=True)
class Static:
    def to_dict(self) -> dict:
        return {'kind': 'static'}


@dataclass(frozen=True)
class Spin:
    degrees_per_second: float = 30

    def __post_init__(self):
        if (type(self.degrees_per_second) not in (int, float)
                or not math.isfinite(self.degrees_per_second)):
            raise ValueError('spin speed must be finite')

    def to_dict(self) -> dict:
        return dict(kind='spin', degrees_per_second=self.degrees_per_second)


@dataclass(frozen=True)
class DisplaySpec:
    """One ordinary item, world anchor, scalar scale and bounded lease.

    Select profile from describe(); this type does not assert client validation.
    """
    profile: str
    item: str
    anchor: WorldPosition
    scale: float = 1.0
    motion: Static | Spin = field(default_factory=Static)
    ttl_seconds: float = 30

    def __post_init__(self):
        identifier(self.profile, 'profile')
        if (not isinstance(self.item, str)
                or not re.fullmatch(r'[a-z0-9_]+:[a-z0-9_]+', self.item)):
            raise ValueError('item must be an ordinary namespaced item identifier')
        if not isinstance(self.anchor, WorldPosition):
            raise ValueError('anchor must be WorldPosition')
        if not isinstance(self.motion, (Static, Spin)):
            raise ValueError('motion must be Static or Spin')
        positive(self.scale, 'scale')
        positive(self.ttl_seconds, 'ttl_seconds')

    def to_dict(self) -> dict:
        return dict(profile=self.profile, item=self.item, anchor=self.anchor.to_dict(),
                    scale=self.scale, motion=self.motion.to_dict(), ttl_seconds=self.ttl_seconds)
