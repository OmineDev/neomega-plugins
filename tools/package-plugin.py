#!/usr/bin/env python3
"""Validate community metadata and build only explicit, reviewed source files."""
import argparse
from email.parser import Parser
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import unicodedata
import zipfile
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
from library_bundle import bundled_files
from wheel_targets import validate_lock, validate_runtime_target

try:
    from neomega_runtime.config_schema import check_config_schema
except ImportError:
    check_config_schema = None

SEMVER_RE = re.compile(r'^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$')
WINDOWS_RESERVED = {'CON', 'PRN', 'AUX', 'NUL'} | {f'COM{i}' for i in range(10)} | {f'LPT{i}' for i in range(10)}


def _check_safe_relative_path(name: str):
    path = PurePosixPath(name)
    if not name or path.is_absolute() or '..' in path.parts or str(path) != name or '\\' in name or ':' in name or '\x00' in name:
        raise ValueError('unsafe packaged path: ' + name)
    for part in path.parts:
        if part in {'.', '..'} or part.strip() != part or part.endswith('.'):
            raise ValueError('unsafe packaged path: ' + name)
        if any(ord(ch) < 32 or ch in '<>"|?*' for ch in part):
            raise ValueError('unsafe packaged path: ' + name)
        if part.split('.', 1)[0].upper() in WINDOWS_RESERVED:
            raise ValueError('Windows reserved packaged path: ' + name)


def validate(root):
    root = Path(root)
    meta = json.loads((root / 'release.json').read_text())
    names = meta['files']
    if 'description' in meta and (not isinstance(meta['description'], str) or len(meta['description']) > 2000):
        raise ValueError('description must be a string of at most 2000 characters')
    if meta.get('kind', 'app') not in ('app', 'provider'):
        raise ValueError('kind must be app or provider')
    if not isinstance(meta.get('name'), str) or not meta['name'].strip():
        raise ValueError('release name is required')
    if not isinstance(names, list) or not names or not all(isinstance(n, str) for n in names):
        raise ValueError('files must be an explicit filename list')
    if len(names) != len(set(names)):
        raise ValueError('duplicate packaged path')
    for name in names:
        _check_safe_relative_path(name)
        path = PurePosixPath(name)
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
    # <namespace>.<name> is accepted unchanged by Host, Polaris and the catalog.
    if not re.fullmatch(r'[a-z][a-z0-9-]*\.[a-z][a-z0-9-]*', manifest['id']) or len(manifest['id']) > 80:
        raise ValueError('plugin ID must be <namespace>.<name> in lowercase letters, digits and hyphens, at most 80 characters')
    if manifest['id'].split('.')[0] in {'community', 'example', 'test'}:
        raise ValueError('reserved plugin namespace: ' + manifest['id'])
    if not SEMVER_RE.fullmatch(manifest['version']):
        raise ValueError('explicit semantic version required')
    if manifest.get('manifest_version') != 1 or manifest.get('runtime') != 'python':
        raise ValueError('unsupported manifest version/runtime')
    if manifest.get('target_os') is not None or manifest.get('target_arch') is not None:
        raise ValueError('target_os/target_arch require Go runtime')
    _check_safe_relative_path(str(manifest.get('entrypoint', '')))
    if manifest.get('entrypoint') not in names:
        raise ValueError('entrypoint must be packaged')
    schema = json.loads((root / 'config.schema.json').read_text())
    if not isinstance(schema, dict) or schema.get('type') != 'object':
        raise ValueError('configuration schema must describe an object')
    if check_config_schema is not None:
        check_config_schema(schema)
    permissions = manifest.get('permissions', {})
    purposes = meta.get('permission_purposes', permissions.get('purposes', {}))
    if not isinstance(purposes, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in purposes.items()):
        raise ValueError('permission purposes must be a string map')
    for permission, purpose in permissions.get('purposes', {}).items():
        if permission in purposes and purposes[permission] != purpose:
            raise ValueError('conflicting permission purpose: ' + permission)
    required = []
    for field, prefix in [
        ('operations', 'operation'),
        ('services', 'service'),
        ('players', 'players'),
        ('packet_send_ids', 'packet_send'),
        ('packet_observe_ids', 'packet_observe'),
        ('packet_preview_ids', 'packet_preview'),
        ('raw_packet_directions', 'packet_raw'),
    ]:
        values = permissions.get(field, [])
        if not isinstance(values, list):
            raise ValueError('permission lists required')
        if field in {'packet_send_ids', 'packet_observe_ids', 'packet_preview_ids'}:
            if len(values) > 256 or len(set(values)) != len(values):
                raise ValueError(f'{field} must contain at most 256 unique IDs')
            upper = 1023 if field == 'packet_observe_ids' else 0xffffffff
            if any(type(value) is not int or not 1 <= value <= upper for value in values):
                raise ValueError(f'{field} must contain integers in 1..{upper}')
        if field == 'raw_packet_directions':
            if len(set(values)) != len(values) or any(v not in ('inbound', 'outbound') for v in values):
                raise ValueError('raw_packet_directions must contain unique inbound/outbound values')
        required.extend(f'{prefix}:{value}' for value in values)
    required.extend('event:' + sub['kind'] for sub in manifest.get('subscriptions', []))
    required_set = set(required)
    for permission in required:
        if not isinstance(purposes.get(permission), str) or not purposes[permission].strip():
            raise ValueError('missing permission purpose: ' + permission)
    for permission, purpose in purposes.items():
        if permission not in required_set:
            raise ValueError('unrequested permission purpose: ' + permission)
        if not purpose.strip() or len(purpose) > 512 or any(unicodedata.category(ch)[0] == 'C' for ch in purpose):
            raise ValueError('invalid permission purpose: ' + permission)
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
    bundled = bundled_files(root, meta)
    overlap = set(names) & set(bundled)
    if overlap:
        raise ValueError('bundled library overwrites plugin file: ' + sorted(overlap)[0])
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
