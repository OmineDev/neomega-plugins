"""Validate locked wheel tags without depending on the build machine's interpreter."""
import itertools
import re
from pathlib import PurePosixPath


def python_version(value):
    match = re.fullmatch(r'(\d)\.?([0-9]+)', str(value))
    if not match:
        raise ValueError('wheel python_version must be an explicit major/minor')
    return tuple(map(int, match.groups()))


def tags(filename):
    if PurePosixPath(filename).name != filename or not filename.endswith('.whl'):
        raise ValueError('invalid locked wheel filename')
    parts = filename[:-4].rsplit('-', 3)
    if len(parts) != 4:
        raise ValueError('invalid locked wheel tags')
    return set(itertools.product(*(part.split('.') for part in parts[1:])))


def platform_target(platform):
    if platform == 'any':
        return None
    aliases = {'win32': ('windows', '386'), 'win_amd64': ('windows', 'amd64'), 'win_arm64': ('windows', 'arm64')}
    if platform in aliases:
        return aliases[platform]
    match = re.fullmatch(r'(linux|manylinux(?:1|2010|2014|_[0-9]+_[0-9]+)|musllinux_[0-9]+_[0-9]+)_(x86_64|aarch64|i686|armv7l|ppc64le|s390x)', platform)
    if match:
        return 'linux', {'x86_64': 'amd64', 'aarch64': 'arm64', 'i686': '386', 'armv7l': 'arm'}.get(match[2], match[2])
    match = re.fullmatch(r'macosx_[0-9]+_[0-9]+_(x86_64|arm64|universal2)', platform)
    if match:
        return 'darwin', {'x86_64': 'amd64', 'arm64': 'arm64', 'universal2': 'universal2'}[match[1]]
    raise ValueError('unsupported wheel platform: ' + platform)


def platform_compatible(wheel, target):
    if wheel == 'any' or wheel == target:
        return True
    legacy = {'manylinux1': 'manylinux_2_5', 'manylinux2010': 'manylinux_2_12', 'manylinux2014': 'manylinux_2_17'}
    for old, new in legacy.items():
        if wheel.startswith(old + '_'):
            wheel = new + wheel[len(old):]
        if target.startswith(old + '_'):
            target = new + target[len(old):]
    pattern = r'(manylinux|musllinux|macosx)_([0-9]+)_([0-9]+)_(.+)'
    left, right = re.fullmatch(pattern, wheel), re.fullmatch(pattern, target)
    if not left or not right or left[1] != right[1]:
        return False
    architecture_ok = left[4] == right[4] or left[1] == 'macosx' and left[4] == 'universal2' and right[4] in {'x86_64', 'arm64'}
    return architecture_ok and (int(left[2]), int(left[3])) <= (int(right[2]), int(right[3]))


def compatible(tag, version, abi, platform):
    interpreter, wheel_abi, wheel_platform = tag
    current = ''.join(map(str, version))
    if not platform_compatible(wheel_platform, platform):
        return False
    if wheel_abi == 'none':
        if interpreter == 'cp' + current:
            return True
        if interpreter == 'py' + str(version[0]):
            return True
        match = re.fullmatch(r'py(\d)(\d+)', interpreter)
        return bool(match and int(match[1]) == version[0] and int(match[2]) <= version[1])
    match = re.fullmatch(r'cp(\d)(\d+)', interpreter)
    if wheel_abi == 'abi3':
        # CPython's stable ABI is forward compatible, except free-threaded builds.
        return bool(match and version[0] == int(match[1]) == 3 and 2 <= int(match[2]) <= version[1] and abi in {'abi3', 'cp' + current})
    return interpreter == 'cp' + current and wheel_abi == abi == 'cp' + current or interpreter == 'cp' + current and wheel_abi == abi == 'cp' + current + 't'


def validate_lock(lock, manifest=None):
    version = python_version(lock['python_version'])
    abi, platform = lock['abi'], lock['platform']
    current = ''.join(map(str, version))
    if abi not in {'none', 'abi3', 'cp' + current, 'cp' + current + 't'}:
        raise ValueError('wheel ABI does not match locked Python version')
    target = platform_target(platform)
    if target and manifest is not None:
        oses, arches = manifest.get('target_os'), manifest.get('target_arch')
        oses = [oses] if isinstance(oses, str) else oses
        arches = [arches] if isinstance(arches, str) else arches
        allowed_arches = {'amd64', 'arm64'} if target[1] == 'universal2' else {target[1]}
        if not oses or not arches or set(oses) != {target[0]} or not set(arches) <= allowed_arches:
            raise ValueError('wheel platform differs from manifest target_os/target_arch')
    for wheel in lock['wheels']:
        candidates = tags(wheel['wheel'])
        if not any(compatible(tag, version, abi, platform) for tag in candidates):
            raise ValueError('wheel tags do not support locked Python/ABI/platform: ' + wheel['wheel'])
        if any(tag[2] == 'any' and tag[1] != 'none' for tag in candidates):
            raise ValueError('platform-independent wheel must use ABI none')
        if 'tags' in wheel and {tuple(tag.split('-')) for tag in wheel['tags']} != candidates:
            raise ValueError('wheel WHEEL tags differ from filename: ' + wheel['wheel'])


def validate_runtime_target(lock, target):
    """Bind native vendored code to the contract enforced by Host before startup."""
    restricted = any(('py3', 'none', 'any') not in tags(wheel['wheel']) or wheel.get('requires_python') for wheel in lock['wheels'])
    if target is None:
        if restricted:
            raise ValueError('Python/platform-specific wheels require packaged runtime-target.json')
        return
    if set(target) != {'schema_version', 'implementation', 'python_version', 'abi', 'platform'} or type(target['schema_version']) is not int or target['schema_version'] != 1 or target['implementation'] != 'cpython':
        raise ValueError('unsupported runtime-target.json contract')
    if not re.fullmatch(r'[0-9]+\.[0-9]+', target['python_version']):
        raise ValueError('runtime Python target must use major.minor')
    version = python_version(target['python_version'])
    expected_abi = 'cp' + ''.join(map(str, version))
    if target['abi'] not in {expected_abi, expected_abi + 't'}:
        raise ValueError('runtime target must identify the actual CPython ABI')
    platform_target(target['platform'])
    if version != python_version(lock['python_version']):
        raise ValueError('wheel lock Python differs from runtime target')
    if lock['platform'] != 'any' and lock['platform'] != target['platform']:
        raise ValueError('wheel lock platform differs from runtime target')
    if lock['abi'] not in {'none', target['abi']} and not (lock['abi'] == 'abi3' and target['abi'] == expected_abi):
        raise ValueError('wheel lock ABI differs from runtime target')
    for wheel in lock['wheels']:
        validate_python_requirement(wheel.get('requires_python'), version)
        if not any(compatible(tag, version, target['abi'], target['platform']) for tag in tags(wheel['wheel'])):
            raise ValueError('wheel tags do not support declared runtime target')


def validate_python_requirement(specifier, version):
    """Accept requirements that cover the whole declared interpreter minor.

    A patch-specific restriction cannot be guaranteed by a major.minor runtime
    contract; reject it instead of claiming an unverified interpreter compatible.
    """
    if not specifier:
        return
    for part in specifier.split(','):
        match = re.fullmatch(r'\s*(>=|<=|==|!=|~=|<|>)\s*([0-9]+)(?:\.([0-9]+))?(?:\.(\*))?\s*', part)
        if not match:
            raise ValueError('Requires-Python needs an unsupported patch-specific or complex constraint: ' + part)
        op, major, minor, wildcard = match.groups()
        bound = (int(major), int(minor or 0))
        if wildcard and op not in {'==', '!='}:
            raise ValueError('invalid Requires-Python wildcard')
        same = version == bound if minor else version[0] == bound[0]
        if op in {'==', '!='}:
            # Without .* equality selects patch zero, not the entire minor.
            if not wildcard:
                raise ValueError('exact patch Requires-Python cannot use a minor-only runtime target')
            accepted = same if op == '==' else not same
        elif op == '>=':
            accepted = version >= bound
        elif op == '<':
            accepted = version < bound
        elif op == '>':
            accepted = version > bound
        elif op == '<=':
            accepted = version < bound
        else:
            if minor is None:
                raise ValueError('Requires-Python ~= needs a minor version')
            accepted = version >= bound and version[0] == bound[0]
        if not accepted:
            raise ValueError('Requires-Python does not cover the declared runtime minor: ' + part)
