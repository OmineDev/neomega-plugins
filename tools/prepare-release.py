#!/usr/bin/env python3
"""Prepare one reviewed plugin release and a candidate catalog without network access."""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('packager', ROOT / 'tools/package-plugin.py')
packager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packager)


def prepare(directory, output):
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]*', directory):
        raise ValueError('plugin directory must be a single directory name')
    root = ROOT / 'plugins' / directory
    meta, manifest = packager.validate(root)
    index = json.loads((ROOT / 'catalog/index.json').read_text())
    if any(e['plugin_id'] == manifest['id'] and e['version'] == manifest['version'] for e in index['plugins']):
        raise ValueError('version already catalogued; increment the version')
    output.mkdir(parents=True, exist_ok=True)
    filename = f"{manifest['id']}-{manifest['version']}.zip"
    tag = f"{manifest['id']}-v{manifest['version']}"
    digest = packager.build(root, output / filename)
    (output / 'SHA256SUMS').write_text(f'{digest}  {filename}\n')
    (output / 'index.json').write_text(json.dumps(index, ensure_ascii=False, indent=2) + '\n')
    purposes = output / 'permission-purposes.json'
    purposes.write_text(json.dumps(manifest.get('permissions', {}).get('purposes', {})))
    subprocess.run([sys.executable, str(ROOT / 'tools/catalog-release.py'), '--package', str(output / filename),
                    '--plugin', manifest['id'], '--version', manifest['version'], '--name', meta['name'],
                    '--index', str(output / 'index.json'), '--permission-purposes', str(purposes)], check=True)
    (output / 'release.env').write_text(f'tag={tag}\nfilename={filename}\n')
    (output / 'release-notes.md').write_text(f'{meta["name"]} {manifest["version"]}\n\n'
        '固定审核制品；配置 schema、权限用途和许可证见 ZIP。\n'
        '自动打包和静态检查不代表实服验收；安装和运行仍需服主授权。\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plugin_directory')
    parser.add_argument('--output', type=Path, default=Path('dist/release'))
    args = parser.parse_args()
    try:
        prepare(args.plugin_directory, args.output)
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        parser.exit(1, f'prepare-release: {error}\n')
