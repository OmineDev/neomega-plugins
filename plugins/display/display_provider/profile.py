"""Experimental ordinary-item FMBE renderer; command generation only.

This is an independently written, yaw-only specialization of the ordinary-item
transform described by szea-ll14 (Discussion #5, 2024-09-26), read 2026-09-29:
https://github.com/szea-ll14/mcbe-cmd-memo/discussions/5
It fixes all translation/base offsets to zero, extension scale to one, extension
pitch to -90 degrees and extension yaw to zero. It does NOT combine Wiki/map
profiles. The factual ordinary-item calibration is 17/8, 11/29, 31/37, 8/15 and
25 degrees. With c=cos(yaw), s=sin(yaw), the composed rotation is
[[c,s,0], [0,0,1], [s,-c,0]]. The equations below are its reduced form.

No map, texture, model, or original long expression is redistributed. Third-party
material's redistribution license was not established; the source URL documents
the algorithm/calibration provenance, not a grant to redistribute its assets.
The fixed five animation identifiers consume original client bones; neither
their presence in a particular NetEase client nor final visual calibration is
proven by generating these commands.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from typing import Any

PROFILE_ID = "fmbe:ordinary-item-yaw-v1"
ITEMS = frozenset("minecraft:" + item for item in (
    "diamond", "emerald", "iron_ingot", "gold_ingot", "paper", "stick",
))
DIMENSIONS = frozenset(("overworld", "nether", "the_end"))
_CARRIER = re.compile(r"nd_[0-9a-f]{32}\Z")
_SOURCE = "https://github.com/szea-ll14/mcbe-cmd-memo/discussions/5"
_DOCS = "https://learn.microsoft.com/en-us/minecraft/creator/commands/commands/"
_SIN25, _COS25 = math.sin(math.radians(25)), math.cos(math.radians(25))
_HEAD_ROTATION = (
    math.degrees(math.atan2(_COS25, _SIN25 ** 2)),
    math.degrees(math.asin(-_COS25 * _SIN25)),
    math.degrees(math.atan2(_SIN25, _COS25 ** 2)),
)


def describe() -> dict[str, Any]:
    return {
        "profile": PROFILE_ID,
        "status": "experimental",
        "items": sorted(ITEMS),
        "anchor": {"dimensions": sorted(DIMENSIONS), "units": "blocks",
                   "meaning": "server carrier position; item center uncalibrated"},
        "scale": {"min": 0.1, "max": 4.0, "uniform": True},
        "motion": {"kinds": ["static", "spin"], "axis": "y",
                   "degrees_per_second": {"min": -180, "max": 180},
                   "phase": "client entity lifetime times rate; updates may jump"},
        "visibility": "world-visible; no observer filtering",
        "renderer_layers": 5,
        "refresh": {"required": "unknown", "suggested_min_interval_seconds": 10,
                    "commands_per_object": 6,
                    "note": "optional bounded reinstallation; no tick-rate promise"},
        "carrier_effects": {"duration_seconds": 120, "renew_commands": 2,
                            "renewal_required": "refresh finite effects when extending the lease"},
        "verification": {"command_syntax": "official documentation checked",
                         "target_commands": "unverified", "client_visible": False,
                         "target_version": None, "resource_pack_requirements": "unverified",
                         "item_center": "unverified", "rotation_scale": "unverified",
                         "ai_disabled": False, "invulnerable": False,
                         "collision_removed": False, "refresh_persistence": "unverified"},
        "limitations": [
            "Ordinary fox AI, gravity, collisions, damage and name visibility remain.",
            "Held items may be dropped on death or replaced through fox behavior; economy isolation is not guaranteed.",
            "Clearing the held item before normal cleanup does not prevent earlier drops.",
            "Slowness and periodic teleport do not guarantee a stationary carrier.",
            "Cleanup only targets loaded entities; unload requires reconciliation.",
            "Animation channel evaluation order and cadence require client observation.",
        ],
        "provenance": {"algorithm": _SOURCE, "retrieved": "2026-09-29",
                       "implementation": "independent yaw-only algebraic specialization",
                       "assets_copied": False, "upstream_asset_license": "not established",
                       "official_commands": [_DOCS + x + "?view=minecraft-bedrock-stable"
                                             for x in ("summon", "playanimation", "replaceitem")]},
    }


def _number(value: Any, label: str, lower: float, upper: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    try:
        result = float(value)
    except OverflowError as exc:
        raise ValueError(f"{label} must be a finite number") from exc
    if not math.isfinite(result) or not lower <= result <= upper:
        raise ValueError(f"{label} must be within [{lower}, {upper}]")
    return result


def _fields(value: Any, required: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != required:
        raise ValueError(f"{label} requires exactly {sorted(required)}")
    return value


def validate(spec: dict[str, Any]) -> dict[str, Any]:
    """Validate all inputs before constructing any command, returning a fresh value."""
    data = _fields(spec, {"profile", "item", "anchor", "scale", "motion", "ttl_seconds"}, "spec")
    if data["profile"] != PROFILE_ID:
        raise ValueError("unsupported profile")
    if not isinstance(data["item"], str) or data["item"] not in ITEMS:
        raise ValueError("item is outside the ordinary-item allowlist")
    anchor = _fields(data["anchor"], {"dimension", "x", "y", "z"}, "anchor")
    if not isinstance(anchor["dimension"], str) or anchor["dimension"] not in DIMENSIONS:
        raise ValueError("unsupported dimension")
    motion = data["motion"]
    if not isinstance(motion, Mapping):
        raise ValueError("motion must be structured")
    kind = motion.get("kind")
    if kind == "static":
        _fields(motion, {"kind"}, "motion")
        normalized_motion = {"kind": "static"}
    elif kind == "spin":
        _fields(motion, {"kind", "degrees_per_second"}, "motion")
        normalized_motion = {"kind": "spin", "degrees_per_second": _number(
            motion["degrees_per_second"], "degrees_per_second", -180, 180)}
    else:
        raise ValueError("unsupported motion")
    return {
        "profile": PROFILE_ID, "item": data["item"],
        "anchor": {"dimension": anchor["dimension"],
                   "x": _number(anchor["x"], "x", -30_000_000, 30_000_000),
                   "y": _number(anchor["y"], "y", -64, 320),
                   "z": _number(anchor["z"], "z", -30_000_000, 30_000_000)},
        "scale": _number(data["scale"], "scale", 0.1, 4),
        "motion": normalized_motion,
        "ttl_seconds": _number(data["ttl_seconds"], "ttl_seconds", 1, 60),
    }


def _selector(carrier: str) -> str:
    if not isinstance(carrier, str) or not _CARRIER.fullmatch(carrier):
        raise ValueError("carrier must be nd_ followed by 32 lowercase hex digits")
    # A spatial qualifier keeps selection tied to the execute-in origin; the
    # zero lower radius does not impose a cleanup distance or nearest heuristic.
    return f'@e[type=minecraft:fox,name="{carrier}",rm=0]'


def _num(value: float) -> str:
    # Plain decimals avoid version-dependent exponent parsing in commands/Molang.
    return f"{value:.10f}".rstrip("0").rstrip(".") or "0"


def _prefix(spec: dict[str, Any]) -> str:
    return f'execute in {spec["anchor"]["dimension"]} run '


def _position(spec: dict[str, Any]) -> str:
    return " ".join(_num(spec["anchor"][axis]) for axis in ("x", "y", "z"))


def _render(selector: str, spec: dict[str, Any]) -> list[str]:
    rate = spec["motion"].get("degrees_per_second", 0)
    angle = "0" if rate == 0 else f"query.life_time*({_num(rate)})"
    # Five complete layers from one ordinary-item transform. No user expressions.
    layers = [
        ("player.sleeping", f"v.nd_yaw={angle};v.nd_scale={_num(spec['scale'])};",
         "controller.animation.fox.move"),
        ("creeper.swelling", "v.nd_root=math.sqrt(2.125*v.nd_scale);"
         "v.swelling_scale1=v.nd_root;v.swelling_scale2=v.nd_root;", "nd.scale"),
        ("ender_dragon.neck_head_movement",
         "t.nd_x=-math.cos(v.nd_yaw)+(11/29)*v.nd_scale;"
         "t.nd_y=-math.sin(v.nd_yaw)-(8/15)*v.nd_scale;"
         "v.head_position_x=(16/v.nd_root)*(t.nd_x*math.cos(25)-t.nd_y*math.sin(25));"
         "v.head_position_y=(16/v.nd_root)*(-1/128+(31/37)*v.nd_scale);"
         "v.head_position_z=(16/v.nd_root)*(t.nd_x*math.sin(25)+t.nd_y*math.cos(25));"
         + "".join(f"v.head_rotation_{axis}={_num(value)};"
                   for axis, value in zip("xyz", _HEAD_ROTATION)), "nd.head"),
        ("warden.move", "v.body_x_rot=90;v.body_z_rot=115+v.nd_yaw;", "nd.body"),
        ("player.attack.rotations", "v.attack_body_rot_y=0;", "nd.rotation"),
    ]
    return [f'{_prefix(spec)}playanimation {selector} animation.{animation} none 0 "{expression}" {controller}'
            for animation, expression, controller in layers]


def refresh_commands(carrier: str, spec: dict[str, Any]) -> list[str]:
    """Optional bounded reinstallation. Does not summon or reset accumulated state."""
    data = validate(spec)
    selector = _selector(carrier)
    return [f"{_prefix(data)}tp {selector} {_position(data)} 0 0"] + _render(selector, data)


def create_commands(carrier: str, spec: dict[str, Any]) -> list[str]:
    data = validate(spec)
    selector, prefix = _selector(carrier), _prefix(data)
    return [
        # The recovery name is attached in the first physical mutation.
        f'{prefix}summon minecraft:fox "{carrier}" {_position(data)}',
        f"{prefix}event entity {selector} minecraft:ageable_grow_up",
    ] + renew_commands(carrier, data) + [
        f"{prefix}replaceitem entity {selector} slot.weapon.mainhand 0 {data['item']} 1",
    ] + refresh_commands(carrier, data)


def update_commands(carrier: str, spec: dict[str, Any]) -> list[str]:
    data = validate(spec)
    selector = _selector(carrier)
    return renew_commands(carrier, data) + [f"{_prefix(data)}replaceitem entity {selector} slot.weapon.mainhand 0 {data['item']} 1"] + refresh_commands(carrier, data)


def renew_commands(carrier: str, spec: dict[str, Any]) -> list[str]:
    """Extend finite carrier effects without replacing animation controllers."""
    data = validate(spec)
    selector, prefix = _selector(carrier), _prefix(data)
    return [f"{prefix}effect {selector} invisibility 120 0 true",
            f"{prefix}effect {selector} slowness 120 255 true"]


def close_commands(carrier: str, spec: dict[str, Any]) -> list[str]:
    """Remove the carried item before killing; provider retains per-step evidence.

    Cleanup validates only identity/dimension so obsolete appearance data cannot
    prevent recovery. Never retry an unknown result without reconciliation.
    """
    selector = _selector(carrier)
    anchor = spec.get("anchor") if isinstance(spec, Mapping) else None
    dimension = anchor.get("dimension") if isinstance(anchor, Mapping) else None
    if not isinstance(dimension, str) or dimension not in DIMENSIONS:
        raise ValueError("unsupported cleanup dimension")
    prefix = f"execute in {dimension} run "
    return [f"{prefix}replaceitem entity {selector} slot.weapon.mainhand 0 minecraft:air",
            f"{prefix}kill {selector}"]
