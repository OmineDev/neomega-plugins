#!/usr/bin/env python3
"""Validate every submitted community plugin, then run its isolated test suite."""
import importlib.util
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('packager', ROOT / 'tools/package-plugin.py')
packager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packager)
subprocess.run([sys.executable, str(ROOT / 'tools/sync-community-support.py')], check=True)
seen = set()
for plugin in sorted((ROOT / 'plugins').iterdir()):
    if not plugin.is_dir():
        continue
    _, manifest = packager.validate(plugin)
    if manifest['id'] in seen:
        raise ValueError('duplicate plugin ID: ' + manifest['id'])
    seen.add(manifest['id'])
    paths = [str(plugin), str(ROOT / 'libraries')]
    if os.environ.get('PYTHONPATH'):
        paths.append(os.environ['PYTHONPATH'])
    env = dict(os.environ, PYTHONPATH=os.pathsep.join(paths), PYTHONDONTWRITEBYTECODE='1')
    if (plugin / 'tests').is_dir():
        subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', str(plugin / 'tests')], env=env, check=True)
    if (plugin / 'tools/check-offline.py').is_file():
        subprocess.run([sys.executable, str(plugin / 'tools/check-offline.py')], env=env, check=True)
    print('Validated ' + manifest['id'])
