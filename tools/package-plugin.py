#!/usr/bin/env python3
"""Validate community metadata and build only explicit, reviewed source files."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import zipfile


def validate(root):
    root = Path(root)
    meta = json.loads((root / 'release.json').read_text())
    names = meta['files']
    if not isinstance(meta.get('name'), str) or not meta['name'].strip():
        raise ValueError('release name is required')
    if not isinstance(names, list) or not names or not all(isinstance(n, str) for n in names):
        raise ValueError('files must be an explicit filename list')
    if len(names) != len(set(names)):
        raise ValueError('duplicate packaged path')
    for name in names:
        path = PurePosixPath(name)
        if path.is_absolute() or '..' in path.parts or str(path) != name or '\\' in name:
            raise ValueError('unsafe packaged path: ' + name)
        if any(part.startswith('.') or part in {'__pycache__', 'node_modules'} for part in path.parts):
            raise ValueError('private/generated packaged path: ' + name)
        if path.suffix in {'.log', '.db', '.sqlite', '.pyc'} or path.name in {'config.json', 'release.json'}:
            raise ValueError('private/generated packaged file: ' + name)
        candidate = root
        for part in path.parts:
            candidate = candidate / part
            if candidate.is_symlink():
                raise ValueError('symlinks cannot be packaged: ' + name)
        if not candidate.is_file():
            raise ValueError('missing packaged file: ' + name)
    notice = meta.get('license_notice', 'LICENSE')
    if notice not in ('LICENSE', 'NOTICE'):
        raise ValueError('license_notice must name LICENSE or NOTICE')
    if not {'manifest.json', 'config.schema.json', 'README.md', notice} <= set(names):
        raise ValueError('manifest, schema, README and explicit license/notice must be packaged')
    manifest = json.loads((root / 'manifest.json').read_text())
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', manifest['id']):
        raise ValueError('invalid plugin ID')
    if not re.fullmatch(r'\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?', manifest['version']):
        raise ValueError('explicit semantic version required')
    if manifest.get('manifest_version') != 1 or manifest.get('runtime') not in ('python', 'go'):
        raise ValueError('unsupported manifest version/runtime')
    if manifest.get('runtime') == 'go':
        if (manifest.get('target_os'), manifest.get('target_arch')) not in {
            ('linux', 'amd64'), ('linux', 'arm64'), ('windows', 'amd64'),
            ('darwin', 'amd64'), ('darwin', 'arm64'),
        }:
            raise ValueError('unsupported Go worker platform')
        if manifest.get('host_api', {}).get('min_minor', -1) < 9:
            raise ValueError('Go workers require Host API 1.9')
    if manifest.get('entrypoint') not in names:
        raise ValueError('entrypoint must be packaged')
    schema = json.loads((root / 'config.schema.json').read_text())
    if not isinstance(schema, dict) or schema.get('type') != 'object':
        raise ValueError('configuration schema must describe an object')
    permissions = manifest.get('permissions', {})
    purposes = meta.get('permission_purposes', permissions.get('purposes', {}))
    if not isinstance(purposes, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in purposes.items()):
        raise ValueError('permission purposes must be a string map')
    required = []
    for field, prefix in [('operations', 'operation'), ('services', 'service'), ('players', 'players'), ('packet_send_ids', 'packet_send')]:
        values = permissions.get(field, [])
        if not isinstance(values, list):
            raise ValueError('permission lists required')
        required.extend(f'{prefix}:{value}' for value in values)
    required.extend('event:' + sub['kind'] for sub in manifest.get('subscriptions', []))
    for permission in required:
        if not isinstance(purposes.get(permission), str) or not purposes[permission].strip():
            raise ValueError('missing permission purpose: ' + permission)
    return meta, manifest


def build(root, destination):
    meta, manifest = validate(root)
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Preserve explicit ordering so existing released packages retain their hashes.
    with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name in meta['files']:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            mode = 0o100755 if manifest['runtime'] == 'go' and name == manifest['entrypoint'] else 0o100644
            info.external_attr = mode << 16
            archive.writestr(info, (Path(root) / name).read_bytes())
    if destination.stat().st_size > 32 << 20:
        destination.unlink()
        raise ValueError('package exceeds 32 MiB')
    return hashlib.sha256(destination.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plugin', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    try:
        validate(args.plugin)
        if args.output:
            print(build(args.plugin, args.output))
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        parser.exit(1, f'package-plugin: {error}\n')


if __name__ == '__main__':
    main()
