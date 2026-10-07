#!/usr/bin/env python3
"""Download hash-locked wheels in a private temporary build and stage licensed modules."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tempfile
import zipfile
from email.parser import Parser
from wheel_targets import validate_lock, validate_runtime_target, python_version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--requirements', type=Path, required=True, help='Pinned requirements with --hash=sha256 entries')
    parser.add_argument('--output', type=Path, required=True, help='Fresh staging directory; never a shared environment')
    parser.add_argument('--platform', default='any')
    parser.add_argument('--python-version', default='311')
    parser.add_argument('--abi', default='none')
    parser.add_argument('--license-map', type=Path, required=True, help='JSON mapping wheel distribution names to reviewed SPDX licenses')
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('output must not exist; choose a fresh staging directory')
    licenses = json.loads(args.license_map.read_text())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='neomega-wheels-', dir=args.output.parent) as temporary:
        staging = Path(temporary)
        wheels = staging / 'wheels'
        wheels.mkdir()
        subprocess.run([sys.executable, '-m', 'pip', 'download', '--no-cache-dir', '--require-hashes', '--only-binary=:all:',
                        '--platform', args.platform, '--python-version', args.python_version, '--implementation', 'cp',
                        '--abi', args.abi, '--dest', str(wheels), '-r', str(args.requirements.resolve())], check=True)
        payload = staging / 'payload'
        payload.mkdir()
        records = []
        version = python_version(args.python_version)
        runtime_target = {'schema_version': 1, 'implementation': 'cpython', 'python_version': '.'.join(map(str, version)), 'abi': args.abi if args.abi.startswith('cp') else 'cp' + ''.join(map(str, version)), 'platform': args.platform}
        for wheel in sorted(wheels.glob('*.whl')):
            distribution = wheel.name.split('-')[0]
            license_id = licenses.get(distribution)
            if not isinstance(license_id, str) or not license_id.strip():
                raise ValueError('reviewed license missing for ' + distribution)
            with zipfile.ZipFile(wheel) as archive:
                names = archive.namelist()
                metadata_paths = [name for name in names if name.endswith('.dist-info/WHEEL')]
                if len(metadata_paths) != 1:
                    raise ValueError('wheel requires exactly one WHEEL metadata file')
                wheel_tags = Parser().parsestr(archive.read(metadata_paths[0]).decode('utf-8')).get_all('Tag', [])
                record = {'wheel': wheel.name, 'sha256': hashlib.sha256(wheel.read_bytes()).hexdigest(), 'license': license_id, 'tags': wheel_tags}
                package_metadata_paths = [name for name in names if name.endswith('.dist-info/METADATA')]
                if len(package_metadata_paths) != 1:
                    raise ValueError('wheel requires exactly one METADATA file')
                record['requires_python'] = Parser().parsestr(archive.read(package_metadata_paths[0]).decode('utf-8')).get('Requires-Python')
                validate_runtime_target({'platform': args.platform, 'python_version': args.python_version, 'abi': args.abi, 'wheels': [record]}, runtime_target)
                validate_lock({'platform': args.platform, 'python_version': args.python_version, 'abi': args.abi, 'wheels': [record]})
                if not any('license' in n.lower() or 'copying' in n.lower() for n in names):
                    raise ValueError('wheel must include license text: ' + wheel.name)
                for member in archive.infolist():
                    path = PurePosixPath(member.filename)
                    if path.is_absolute() or '..' in path.parts or '\\' in member.filename or (member.external_attr >> 16) & 0o170000 == 0o120000:
                        raise ValueError('unsafe wheel member')
                    if member.is_dir():
                        continue
                    if '.data' in path.parts[0]:
                        raise ValueError('wheel data relocation is unsupported: ' + wheel.name)
                    destination = payload / member.filename
                    if destination.exists():
                        raise ValueError('wheel module collision: ' + member.filename)
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(archive.read(member))
            records.append(record)
        (payload / 'runtime-target.json').write_text(json.dumps(runtime_target, indent=2) + '\n')
        (payload / 'wheels.lock.json').write_text(json.dumps({'schema_version': 1, 'platform': args.platform, 'python_version': args.python_version, 'abi': args.abi, 'wheels': records, 'files': {p.relative_to(payload).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(payload.rglob('*')) if p.is_file()}}, indent=2) + '\n')
        payload.rename(args.output)
    print(args.output)


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError, zipfile.BadZipFile) as error:
        sys.exit('vendor-wheels: ' + str(error))
