"""Locked, explicitly licensed Python sources bundled into each consumer ZIP."""
import hashlib
import json
from pathlib import Path, PurePosixPath
import re

ROOT = Path(__file__).resolve().parents[1]


def safe_file(root, name):
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or str(path) != name or '\\' in name:
        raise ValueError('unsafe library path: ' + name)
    current = root
    for part in path.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError('library symlink: ' + name)
    if not current.is_file() or any(p.startswith('.') or p == '__pycache__' for p in path.parts):
        raise ValueError('missing/private library file: ' + name)
    if path.suffix in {'.so', '.pyd', '.dll', '.dylib', '.pyc', '.db', '.sqlite'}:
        raise ValueError('native/private file requires a target-specific wheel build: ' + name)
    return current


def resolve(names):
    result = []
    visited, active = set(), set()
    def visit(name):
        if not isinstance(name, str) or not re.fullmatch(r'[a-z][a-z0-9_]*', name):
            raise ValueError('invalid library name')
        if name in active:
            raise ValueError('library dependency cycle: ' + name)
        if name in visited:
            return
        active.add(name)
        root = ROOT / 'libraries' / name
        if root.is_symlink():
            raise ValueError('library directory must not be a symlink: ' + name)
        meta = json.loads((root / 'library.json').read_text())
        if meta.get('name') != name or not re.fullmatch(r'\d+\.\d+\.\d+', meta.get('version', '')):
            raise ValueError('library identity/version mismatch: ' + name)
        if not isinstance(meta.get('license'), str) or not meta['license'].strip():
            raise ValueError('library license required: ' + name)
        files = meta.get('files')
        if not isinstance(files, list) or not files or len(set(files)) != len(files) or not {'LICENSE', '__init__.py'} <= set(files):
            raise ValueError('explicit unique library files including LICENSE and top-level __init__.py required: ' + name)
        for dependency in meta.get('dependencies', []):
            visit(dependency)
        hashes = {f: hashlib.sha256(safe_file(root, f).read_bytes()).hexdigest() for f in sorted(files)}
        result.append({'name': name, 'version': meta['version'], 'license': meta['license'], 'files': hashes})
        active.remove(name)
        visited.add(name)
    for name in names:
        visit(name)
    return {'schema_version': 1, 'abi': 'py3-none-any', 'libraries': result}


def write_lock(plugin):
    plugin = Path(plugin)
    meta = json.loads((plugin / 'release.json').read_text())
    lock = resolve(meta.get('libraries', []))
    (plugin / 'library.lock.json').write_text(json.dumps(lock, ensure_ascii=False, indent=2) + '\n')
    return lock


def bundled_files(plugin, meta):
    if not meta.get('libraries'):
        return {}
    lock_path = Path(plugin) / 'library.lock.json'
    lock = json.loads(lock_path.read_text())
    if lock != resolve(meta['libraries']):
        raise ValueError('library lock differs from sources; run tools/lock-libraries.py explicitly')
    files = {'library.lock.json': lock_path.read_bytes()}
    notices = []
    for library in lock['libraries']:
        root = ROOT / 'libraries' / library['name']
        for name in library['files']:
            files[library['name'] + '/' + name] = (root / name).read_bytes()
        notices.append(f"{library['name']} {library['version']} ({library['license']})\n" + (root / 'LICENSE').read_text())
    files['THIRD_PARTY_NOTICES.txt'] = '\n\n'.join(notices).encode()
    return files
