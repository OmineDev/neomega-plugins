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
seen = set()
for plugin in sorted((ROOT / 'plugins').iterdir()):
    if not plugin.is_dir():
        continue
    _, manifest = packager.validate(plugin)
    if manifest['id'] in seen:
        raise ValueError('duplicate plugin ID: ' + manifest['id'])
    seen.add(manifest['id'])
    if (plugin / 'tests').is_dir():
        env = dict(os.environ, PYTHONPATH=str(plugin), PYTHONDONTWRITEBYTECODE='1')
        subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', str(plugin / 'tests')], env=env, check=True)
    print('Validated ' + manifest['id'])
