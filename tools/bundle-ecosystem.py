#!/usr/bin/env python3
"""Build the complete local release set with hashes and a compatibility matrix; no upload."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import subprocess
import zipfile
import shutil
from library_bundle import ROOT, resolve

spec = importlib.util.spec_from_file_location('packager', ROOT / 'tools/package-plugin.py')
packager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packager)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'dist/ecosystem')
    parser.add_argument('--sdk-source', type=Path, help='SDK source directory used by validation; record content/revision, never local path')
    parser.add_argument('--sdk-license', type=Path, help='Existing SDK license text; omit when upstream has not declared a license')
    parser.add_argument('--host-binary', type=Path, action='append', help='Locally built Host executable to include without publishing')
    parser.add_argument('--host-target', help='Actual built GOOS-GOARCH, for example linux-amd64')
    parser.add_argument('--host-revision', help='Host source commit corresponding to the supplied binary')
    parser.add_argument('--checker', type=Path, help='Optional local native checker; record receipts without suppressing artifacts')
    parser.add_argument('--plugin', action='append', help='Directory selector; default includes every plugin')
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError('output must not exist; existing release artifacts are immutable')
    args.output.mkdir(parents=True)
    entries, matrix, library_entries, components = [], [], [], []
    revision = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], capture_output=True, text=True)
    status = subprocess.run(['git', '-C', str(ROOT), 'status', '--porcelain'], capture_output=True, text=True)
    provenance = {'revision': revision.stdout.strip() if revision.returncode == 0 else None, 'worktree_dirty': bool(status.stdout.strip()) if status.returncode == 0 else None}
    sdk = {'package': 'neomega_runtime', 'version': None, 'source_revision': None, 'source_sha256': None}
    if args.sdk_source:
        source = args.sdk_source / 'neomega_runtime'
        if not source.is_dir():
            raise ValueError('SDK source must contain neomega_runtime')
        sdk_paths = [p for p in sorted(source.rglob('*')) if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc']
        if any(p.is_symlink() for p in sdk_paths):
            raise ValueError('SDK files must not be symlinks')
        sdk_files = {p.relative_to(source).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sdk_paths}
        sdk['source_sha256'] = hashlib.sha256(json.dumps(sdk_files, sort_keys=True).encode()).hexdigest()
        revision = subprocess.run(['git', '-C', str(source), 'rev-parse', 'HEAD'], capture_output=True, text=True)
        sdk['source_revision'] = revision.stdout.strip() if revision.returncode == 0 else None
        status = subprocess.run(['git', '-C', str(source), 'status', '--porcelain', '--', '.'], capture_output=True, text=True)
        sdk['worktree_dirty'] = bool(status.stdout.strip()) if status.returncode == 0 else None
        sdk['license_status'] = 'included' if args.sdk_license else 'not_declared'
        filename = 'neomega-runtime-sdk-' + (sdk['source_revision'] or 'source')[:12] + '-' + sdk['source_sha256'][:12] + '.zip'
        with zipfile.ZipFile(args.output / filename, 'w', zipfile.ZIP_DEFLATED) as archive:
            payload = {'neomega_runtime/' + p.relative_to(source).as_posix(): p.read_bytes() for p in sdk_paths}
            if (args.sdk_source / 'README.md').is_file():
                payload['README.md'] = (args.sdk_source / 'README.md').read_bytes()
            payload['source.lock.json'] = json.dumps(sdk | {'files': sdk_files}, sort_keys=True, indent=2).encode()
            payload['LICENSE' if args.sdk_license else 'NOTICE.txt'] = args.sdk_license.read_bytes() if args.sdk_license else b'Upstream source has not declared a license in this build. This archive does not grant additional rights.\n'
            for name, raw in sorted(payload.items()):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                info.external_attr = 0o100644 << 16
                archive.writestr(info, raw)
        components.append(sdk | {'kind': 'sdk', 'artifact': filename, 'sha256': hashlib.sha256((args.output / filename).read_bytes()).hexdigest(), 'size': (args.output / filename).stat().st_size})
    for host_binary in args.host_binary or []:
        if not args.host_target or not all(c.isalnum() or c == '-' for c in args.host_target):
            raise ValueError('Host binary requires explicit GOOS-GOARCH target')
        digest = hashlib.sha256(host_binary.read_bytes()).hexdigest()
        filename = host_binary.name + '-' + args.host_target + '-' + digest[:16]
        shutil.copyfile(host_binary, args.output / filename)
        (args.output / filename).chmod(0o755)
        components.append({'kind': 'host', 'executable': host_binary.name, 'target': args.host_target, 'source_revision': args.host_revision, 'artifact': filename, 'sha256': digest, 'size': (args.output / filename).stat().st_size})
    sources = [ROOT / 'plugins' / name for name in args.plugin] if args.plugin else sorted((ROOT / 'plugins').iterdir())
    seen = set()
    for source in sources:
        if not source.is_dir():
            raise ValueError('missing plugin directory: ' + str(source))
        meta, manifest = packager.validate(source)
        plugin_id, version = manifest['id'], manifest['version']
        if plugin_id in seen:
            raise ValueError('duplicate plugin ID: ' + plugin_id)
        seen.add(plugin_id)
        filename = f'{plugin_id}-{version}.zip'
        digest = packager.build(source, args.output / filename)
        raw = (source / 'README.md').read_bytes()
        text = raw.decode('utf-8')
        description = meta.get('description', next((line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith(('#','```','|','![','<'))), ''))[:2000]
        entry = {'plugin_id': plugin_id, 'version': version, 'name': meta['name'], 'artifact': filename,
                 'sha256': digest, 'size': (args.output / filename).stat().st_size, 'manifest_sha256': hashlib.sha256((source / 'manifest.json').read_bytes()).hexdigest(), 'description': description,
                 'readme': {'path': 'README.md', 'sha256': hashlib.sha256(raw).hexdigest(), 'size': len(raw)}}
        for field in ('permissions','subscriptions','runtime','host_api','worker_protocol','dependencies','target_os','target_arch','min_host_version'):
            if field in manifest:
                entry[field] = manifest[field]
        entry['permission_purposes'] = meta.get('permission_purposes', manifest.get('permissions', {}).get('purposes', {}))
        if (source / 'library.lock.json').is_file():
            entry['library_lock_sha256'] = hashlib.sha256((source / 'library.lock.json').read_bytes()).hexdigest()
        entries.append(entry)
        wheel_targets = [json.loads((source / name).read_text()) for name in meta['files'] if Path(name).name == 'wheels.lock.json']
        runtime_target = json.loads((source / 'runtime-target.json').read_text()) if 'runtime-target.json' in meta['files'] else None
        matrix.append({key: entry[key] for key in ('plugin_id','version','runtime','host_api','worker_protocol','dependencies','target_os','target_arch','min_host_version') if key in entry} | {'runtime_target': runtime_target, 'runtime_compatibility': 'not_probed', 'wheel_targets': [{key: lock[key] for key in ('platform', 'python_version', 'abi')} for lock in wheel_targets], 'libraries': meta.get('libraries', []), 'exports': [{'name': service['name'], 'major': service['major']} for service in manifest.get('exports', [])], 'artifact': filename, 'sha256': digest, 'size': entry['size']})
        if args.checker:
            receipt = subprocess.run([str(args.checker.resolve()), '--sha256', digest, str((args.output / filename).resolve())], capture_output=True, text=True)
            matrix[-1]['native_check'] = {'exit_code': receipt.returncode, 'receipt': json.loads(receipt.stdout) if receipt.stdout.strip() else None, 'stderr': receipt.stderr.strip()}

    for library in resolve([p.name for p in sorted((ROOT / 'libraries').iterdir()) if p.is_dir()])['libraries'] if (ROOT / 'libraries').is_dir() else []:
        filename = f"{library['name']}-{library['version']}-py3-none-any.zip"
        with zipfile.ZipFile(args.output / filename, 'w', zipfile.ZIP_DEFLATED) as archive:
            for name in library['files']:
                info = zipfile.ZipInfo(library['name'] + '/' + name, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                info.external_attr = 0o100644 << 16
                archive.writestr(info, (ROOT / 'libraries' / library['name'] / name).read_bytes())
            info = zipfile.ZipInfo('library.lock.json', date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, json.dumps({'schema_version': 1, 'abi': 'py3-none-any', 'libraries': [library]}, ensure_ascii=False, indent=2).encode())
        library_entries.append(library | {'artifact': filename, 'size': (args.output / filename).stat().st_size, 'sha256': hashlib.sha256((args.output / filename).read_bytes()).hexdigest()})
    args.output.joinpath('bundle.json').write_text(json.dumps({'schema_version': 1, 'publication_state': 'local-only', 'source_provenance': provenance, 'plugins': entries, 'libraries': library_entries, 'components': components}, ensure_ascii=False, indent=2) + '\n')
    args.output.joinpath('compatibility.json').write_text(json.dumps({'schema_version': 1, 'sdk': sdk, 'plugins': matrix, 'libraries': library_entries, 'components': components}, ensure_ascii=False, indent=2) + '\n')
    args.output.joinpath('SHA256SUMS').write_text(''.join(f'{entry["sha256"]}  {entry["artifact"]}\n' for entry in entries + library_entries + components))
    print(f'Built {len(entries)} plugin artifacts, {len(library_entries)} library archives and {len(components)} Host/SDK components in {args.output}; no release URLs created')


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, KeyError, TypeError) as error:
        sys.exit('bundle-ecosystem: ' + str(error))
