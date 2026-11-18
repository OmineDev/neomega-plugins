#!/usr/bin/env python3
"""Validate every submitted community plugin, then run its isolated test suite."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('packager', ROOT / 'tools/package-plugin.py')
packager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packager)
seen = set()
for plugin in sorted((ROOT / 'plugins').iterdir()):
    if not plugin.is_dir():
        continue
    meta, manifest = packager.validate(plugin)
    if manifest['id'] in seen:
        raise ValueError('duplicate plugin ID: ' + manifest['id'])
    seen.add(manifest['id'])
    example = plugin / 'config.example.json'
    if example.is_file() and not isinstance(json.loads(example.read_text()), dict):
        raise ValueError('config.example.json must be a JSON object: ' + plugin.name)
    with tempfile.TemporaryDirectory(prefix='neomega-check-') as staged:
        paths = [str(plugin)]
        if meta.get('libraries'):
            for rel, raw in packager.bundled_files(plugin, meta).items():
                dest = Path(staged) / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(raw)
            paths.append(staged)
        if os.environ.get('PYTHONPATH'):
            paths.append(os.environ['PYTHONPATH'])
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(paths), PYTHONDONTWRITEBYTECODE='1')
        if (plugin / 'tests').is_dir():
            subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', str(plugin / 'tests')], env=env, check=True)
        if (plugin / 'tools/check-offline.py').is_file():
            subprocess.run([sys.executable, str(plugin / 'tools/check-offline.py')], env=env, check=True)
    print('Validated ' + manifest['id'])
for library in sorted((ROOT / 'libraries').iterdir()):
    if (library / 'tests').is_dir():
        paths = [str(ROOT / 'libraries')] + ([os.environ['PYTHONPATH']] if os.environ.get('PYTHONPATH') else [])
        env = dict(os.environ, PYTHONPATH=os.pathsep.join(paths), PYTHONDONTWRITEBYTECODE='1')
        subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', str(library / 'tests')], env=env, check=True)
        print('Validated library ' + library.name)
