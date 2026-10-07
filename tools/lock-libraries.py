#!/usr/bin/env python3
"""Explicitly refresh vendored source hashes; packaging never silently updates locks."""
import argparse
from pathlib import Path
from library_bundle import write_lock

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('plugins', nargs='+', type=Path)
args = parser.parse_args()
for plugin in args.plugins:
    lock = write_lock(plugin)
    print(f'{plugin}: {len(lock["libraries"])} locked libraries')
