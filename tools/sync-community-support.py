#!/usr/bin/env python3
"""Check vendored support bytes; --write explicitly synchronizes existing plugins."""
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PLUGINS = ('online-time', 'daily-signin', 'player-tpa', 'scheduled-commands', 'personal-homes')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    source = (ROOT / 'tools/community_support.py').read_bytes()
    mismatches = []
    for name in PLUGINS:
        directory = ROOT / 'plugins' / name
        if not directory.is_dir():
            continue
        target = directory / 'community_support.py'
        if args.write:
            target.write_bytes(source)
        if not target.is_file() or target.read_bytes() != source:
            mismatches.append(name)
    if mismatches:
        parser.exit(1, 'Support copies differ: ' + ', '.join(mismatches) + '\n')
    print('Community support copies match canonical source.')


if __name__ == '__main__':
    main()
