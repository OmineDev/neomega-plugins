#!/usr/bin/env python3
"""Validate community metadata and build only explicit, reviewed source files."""
import argparse
from email.parser import Parser
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import zipfile
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from library_bundle import bundled_files
from wheel_targets import validate_lock, validate_runtime_target


def validate(root):
    root = Path(root)
    meta = json.loads((root / 'release.json').read_text())
    names = meta['files']
    if 'description' in meta and (not isinstance(meta['description'], str) or len(meta['description']) > 2000):
        raise ValueError('description must be a string of at most 2000 characters')
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
    if not {'manifest.json', 'config.schema.json', 'README.md', 'LICENSE'} <= set(names):
        raise ValueError('manifest, schema, README and LICENSE must be packaged')
    readme = (root / 'README.md').read_bytes()
    if len(readme) > 256 * 1024:
        raise ValueError('README exceeds 256 KiB')
    readme.decode('utf-8')
    manifest = json.loads((root / 'manifest.json').read_text())
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', manifest['id']):
        raise ValueError('invalid plugin ID')
    if not re.fullmatch(r'\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?', manifest['version']):
        raise ValueError('explicit semantic version required')
    if manifest.get('manifest_version') != 1 or manifest.get('runtime') != 'python':
        raise ValueError('unsupported manifest version/runtime')
    if manifest.get('entrypoint') not in names:
        raise ValueError('entrypoint must be packaged')
    schema = json.loads((root / 'config.schema.json').read_text())
    if not isinstance(schema, dict) or schema.get('type') != 'object':
        raise ValueError('configuration schema must describe an object')
    permissions = manifest.get('permissions', {})
    purposes = meta.get('permission_purposes', permissions.get('purposes', {}))
    if not isinstance(purposes, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in purposes.items()):
        raise ValueError('permission purposes must be a string map')
    for permission, purpose in permissions.get('purposes', {}).items():
        if permission in purposes and purposes[permission] != purpose:
            raise ValueError('conflicting permission purpose: ' + permission)
    required = []
    for field, prefix in [('operations', 'operation'), ('services', 'service'), ('players', 'players'), ('packet_send_ids', 'packet_send'), ('packet_observe_ids', 'packet_observe')]:
        values = permissions.get(field, [])
        if not isinstance(values, list):
            raise ValueError('permission lists required')
        if field in {'packet_send_ids', 'packet_observe_ids'} and any(type(value) is not int or not 0 <= value <= 0xffffffff for value in values):
            raise ValueError('packet permission IDs must be uint32')
        required.extend(f'{prefix}:{value}' for value in values)
    required.extend('event:' + sub['kind'] for sub in manifest.get('subscriptions', []))
    for permission in required:
        if not isinstance(purposes.get(permission), str) or not purposes[permission].strip():
            raise ValueError('missing permission purpose: ' + permission)
    runtime_target = json.loads((root / 'runtime-target.json').read_text()) if 'runtime-target.json' in names else None
    for lock_name in (name for name in names if PurePosixPath(name).name == 'wheels.lock.json'):
        wheel_lock = json.loads((root / lock_name).read_text())
        prefix = PurePosixPath(lock_name).parent
        for relative, expected in wheel_lock['files'].items():
            file_name = str(prefix / relative)
            if file_name not in names or hashlib.sha256((root / file_name).read_bytes()).hexdigest() != expected:
                raise ValueError('wheel lock differs from packaged source: ' + file_name)
        # Check the metadata actually included in the ZIP, not just the lock claim.
        for wheel in wheel_lock['wheels']:
            distribution, version = wheel['wheel'].split('-')[:2]
            metadata_name = distribution + '-' + version + '.dist-info/WHEEL'
            if metadata_name not in wheel_lock['files']:
                raise ValueError('wheel WHEEL metadata missing from locked files: ' + metadata_name)
            metadata = Parser().parsestr((root / prefix / metadata_name).read_text())
            wheel['tags'] = metadata.get_all('Tag', [])
            package_metadata_name = distribution + '-' + version + '.dist-info/METADATA'
            if package_metadata_name not in wheel_lock['files']:
                raise ValueError('wheel METADATA missing from locked files')
            package_metadata = Parser().parsestr((root / prefix / package_metadata_name).read_text())
            wheel['requires_python'] = package_metadata.get('Requires-Python')
        validate_lock(wheel_lock, manifest)
        validate_runtime_target(wheel_lock, runtime_target)
    bundled_files(root, meta)
    return meta, manifest


def build(root, destination):
    meta, manifest = validate(root)
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Preserve explicit ordering so existing released packages retain their hashes.
    with zipfile.ZipFile(destination, 'x', zipfile.ZIP_DEFLATED) as archive:
        files = {name: (Path(root) / name).read_bytes() for name in meta['files']}
        for name, raw in bundled_files(root, meta).items():
            if name in files:
                raise ValueError('bundled library overwrites plugin file: ' + name)
            files[name] = raw
        for name, raw in files.items():
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, raw)
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
