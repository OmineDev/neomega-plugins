#!/usr/bin/env python3
"""Build the five explicit Go sample packages; never run Workers or publish assets."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TARGETS = (('linux', 'amd64'), ('linux', 'arm64'), ('windows', 'amd64'),
           ('darwin', 'amd64'), ('darwin', 'arm64'))
spec = importlib.util.spec_from_file_location('packager', ROOT / 'tools/package-plugin.py')
packager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packager)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    source = ROOT / 'plugins/go-state'
    template = json.loads((source / 'manifest.json').read_text())
    metadata = json.loads((source / 'release.json').read_text())
    records = []
    for target_os, target_arch in TARGETS:
        plugin_id = template['id'] + '-' + target_os + '-' + target_arch
        entrypoint = 'worker.exe' if target_os == 'windows' else 'worker'
        manifest = {**template, 'id': plugin_id, 'target_os': target_os,
                    'target_arch': target_arch, 'entrypoint': entrypoint}
        with tempfile.TemporaryDirectory(prefix='go-state-', dir=args.output) as temporary:
            package = Path(temporary)
            files = [name for name in metadata['files'] if name not in ('worker', 'worker.exe')]
            for name in files:
                destination = package / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source / name, destination)
            (package / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
            package_meta = {**metadata, 'files': files + [entrypoint]}
            (package / 'release.json').write_text(json.dumps(package_meta, ensure_ascii=False, indent=2) + '\n')
            env = {**os.environ, 'GOOS': target_os, 'GOARCH': target_arch, 'CGO_ENABLED': '0', 'GOWORK': 'off'}
            subprocess.run(['go', 'build', '-trimpath', '-buildvcs=false', '-o', str(package / entrypoint), '.'],
                           cwd=source / 'source', env=env, check=True)
            filename = plugin_id + '-' + manifest['version'] + '.zip'
            digest = packager.build(package, args.output / filename)
            records.append({'plugin_id': plugin_id, 'version': manifest['version'], 'filename': filename,
                            'sha256': digest, 'target_os': target_os, 'target_arch': target_arch,
                            'source_path': 'plugins/go-state'})
    (args.output / 'go-packages.json').write_text(json.dumps(records, indent=2) + '\n')
    (args.output / 'GO-SHA256SUMS').write_text(''.join(item['sha256'] + '  ' + item['filename'] + '\n' for item in records))
    print(json.dumps(records, indent=2))


if __name__ == '__main__':
    main()
