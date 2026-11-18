"""Exercise package bytes and rejection of private or incomplete submissions."""
import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest
import json
import zipfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT / 'tools'))
import wheel_targets
spec = importlib.util.spec_from_file_location('community_package', ROOT / 'tools/package-plugin.py')
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)
legacy_spec = importlib.util.spec_from_file_location('legacy_package', ROOT / 'plugins/fatalder-cloud-import/tools/package.py')
legacy = importlib.util.module_from_spec(legacy_spec)
legacy_spec.loader.exec_module(legacy)


class CommunityPackageTests(unittest.TestCase):
    def test_preserves_released_package_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            old, new = Path(folder) / 'old.zip', Path(folder) / 'new.zip'
            legacy.build(old)
            package.build(ROOT / 'plugins/fatalder-cloud-import', new)
            self.assertEqual(old.read_bytes(), new.read_bytes())

    def test_new_author_and_rejections(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / 'plugin'
            shutil.copytree(ROOT / 'plugins/fatalder-cloud-import', root)
            manifest = json.loads((root / 'manifest.json').read_text())
            manifest['id'] = 'another-author.example'
            (root / 'manifest.json').write_text(json.dumps(manifest))
            output = Path(folder) / 'new.zip'
            package.build(root, output)
            with zipfile.ZipFile(output) as archive:
                self.assertEqual(json.loads(archive.read('manifest.json'))['id'], 'another-author.example')
                self.assertNotIn('release.json', archive.namelist())
            for invalid in ['Another.example', 'single', 'a.b.c', 'example.demo', 'community.demo', '1author.demo']:
                (root / 'manifest.json').write_text(json.dumps(dict(manifest, id=invalid)))
                with self.subTest(id=invalid), self.assertRaises(ValueError):
                    package.build(root, output)
            (root / 'manifest.json').write_text(json.dumps(manifest))
            meta = json.loads((root / 'release.json').read_text())
            for unsafe in ['../secret', '/tmp/secret', '.env', 'config.json', 'logs/run.log']:
                changed = dict(meta, files=meta['files'] + [unsafe])
                (root / 'release.json').write_text(json.dumps(changed))
                with self.subTest(path=unsafe), self.assertRaises(ValueError):
                    package.build(root, output)
            (root / 'release.json').write_text(json.dumps(meta))
            (root / 'main.py').unlink()
            (root / 'main.py').symlink_to(ROOT / 'plugins/fatalder-cloud-import/main.py')
            with self.assertRaisesRegex(ValueError, 'symlinks'):
                package.build(root, output)
            (root / 'main.py').unlink()
            (root / 'main.py').write_text('pass\n')
            manifest['permissions']['purposes'] = {}
            (root / 'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'missing permission purpose'):
                package.build(root, output)

    def test_strict_manifest_schema_reserved_paths_and_bundled_collision(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder) / 'plugin'
            shutil.copytree(ROOT / 'plugins/fatalder-cloud-import', root)
            manifest = json.loads((root / 'manifest.json').read_text())
            meta = json.loads((root / 'release.json').read_text())

            # Prerelease / leading-zero semver rejected
            for bad_ver in ('1.0.0-alpha.1', '01.0.0', '1.02.0'):
                (root / 'manifest.json').write_text(json.dumps(dict(manifest, version=bad_ver)))
                with self.subTest(version=bad_ver), self.assertRaises(ValueError):
                    package.validate(root)

            # target_os / target_arch rejected for non-Go runtime
            (root / 'manifest.json').write_text(json.dumps(dict(manifest, target_os='linux')))
            with self.assertRaisesRegex(ValueError, 'Go runtime'):
                package.validate(root)

            # Unrequested purpose key & control chars rejected
            bad_perms = copy_dict(manifest['permissions'])
            bad_perms['purposes']['operation:framework.unused'] = 'nope'
            (root / 'manifest.json').write_text(json.dumps(dict(manifest, permissions=bad_perms)))
            with self.assertRaisesRegex(ValueError, 'unrequested permission purpose'):
                package.validate(root)

            bad_perms = copy_dict(manifest['permissions'])
            first_key = next(iter(bad_perms['purposes']))
            bad_perms['purposes'][first_key] = 'line1\nline2'
            (root / 'manifest.json').write_text(json.dumps(dict(manifest, permissions=bad_perms)))
            with self.assertRaisesRegex(ValueError, 'invalid permission purpose'):
                package.validate(root)

            # Packet observe ID bounds (0 and >1023 rejected)
            bad_perms = copy_dict(manifest['permissions'])
            bad_perms['packet_observe_ids'] = [1024]
            bad_perms['purposes']['packet_observe:1024'] = 'too high'
            (root / 'manifest.json').write_text(json.dumps(dict(manifest, permissions=bad_perms)))
            with self.assertRaisesRegex(ValueError, 'packet_observe_ids'):
                package.validate(root)
            (root / 'manifest.json').write_text(json.dumps(manifest))

            # Windows reserved filename rejected
            (root / 'CON.py').write_text('x = 1\n')
            (root / 'release.json').write_text(json.dumps(dict(meta, files=meta['files'] + ['CON.py'])))
            with self.assertRaisesRegex(ValueError, 'Windows reserved'):
                package.validate(root)
            (root / 'release.json').write_text(json.dumps(meta))

            # check_config_schema rejects invalid config.schema.json
            (root / 'config.schema.json').write_text(json.dumps({
                'type': 'object',
                'properties': {'x': {'type': 'integer', 'minimum': 10, 'maximum': 5}},
            }))
            with self.assertRaises(ValueError):
                package.validate(root)
            shutil.copy2(ROOT / 'plugins/fatalder-cloud-import/config.schema.json', root / 'config.schema.json')

            # bundled_files collision caught during validate()
            shutil.copytree(ROOT / 'plugins/menu', Path(folder) / 'menu')
            menu_root = Path(folder) / 'menu'
            menu_meta = json.loads((menu_root / 'release.json').read_text())
            (menu_root / 'neomega_menu').mkdir()
            (menu_root / 'neomega_menu/LICENSE').write_text('clash\n')
            (menu_root / 'release.json').write_text(json.dumps(dict(menu_meta, files=menu_meta['files'] + ['neomega_menu/LICENSE'])))
            with self.assertRaisesRegex(ValueError, 'bundled library overwrites'):
                package.validate(menu_root)

    def test_wheel_targets_contract(self):
        # <= operator accepts equal version
        wheel_targets.validate_python_requirement('<=3.11', (3, 11))
        with self.assertRaises(ValueError):
            wheel_targets.validate_python_requirement('<=3.10', (3, 11))

        # Python manifest with platform wheel does not require target_os/target_arch (and rejects them)
        lock = {
            'platform': 'manylinux_2_17_x86_64',
            'python_version': '311',
            'abi': 'cp311',
            'wheels': [{'wheel': 'demo-1.0-cp311-cp311-manylinux_2_17_x86_64.whl'}],
        }
        wheel_targets.validate_lock(lock, {'runtime': 'python'})
        with self.assertRaisesRegex(ValueError, 'Go runtime'):
            wheel_targets.validate_lock(lock, {'runtime': 'python', 'target_os': 'linux', 'target_arch': 'amd64'})

        # runtime-target.json platform cannot be 'any'
        with self.assertRaisesRegex(ValueError, 'cannot be any'):
            wheel_targets.validate_runtime_target(
                {'platform': 'any', 'python_version': '311', 'abi': 'none',
                 'wheels': [{'wheel': 'demo-1.0-py3-none-any.whl', 'requires_python': '>=3.8'}]},
                {'schema_version': 1, 'implementation': 'cpython', 'python_version': '3.11',
                 'abi': 'cp311', 'platform': 'any'},
            )


def copy_dict(obj):
    return json.loads(json.dumps(obj))


class PrepareReleaseTests(unittest.TestCase):
    def test_prepares_new_version_and_refuses_catalogued_version(self):
        spec = importlib.util.spec_from_file_location('prepare', ROOT / 'tools/prepare-release.py')
        prepare = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(prepare)
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            shutil.copytree(ROOT / 'tools', root / 'tools')
            shutil.copytree(ROOT / 'plugins', root / 'plugins')
            shutil.copytree(ROOT / 'catalog', root / 'catalog')
            with patch.object(prepare, 'ROOT', root):
                manifest_path = root / 'plugins/fatalder-cloud-import/manifest.json'
                manifest = json.loads(manifest_path.read_text())
                catalog = json.loads((root / 'catalog/index.json').read_text())
                manifest['version'] = next(entry['version'] for entry in reversed(catalog['plugins']) if entry['plugin_id'] == manifest['id'])
                manifest_path.write_text(json.dumps(manifest))
                with self.assertRaisesRegex(ValueError, 'already catalogued'):
                    prepare.prepare('fatalder-cloud-import', root / 'out')
                manifest['version'] = '1.2.1'
                manifest_path.write_text(json.dumps(manifest))
                prepare.prepare('fatalder-cloud-import', root / 'out')
                original = json.loads((root / 'catalog/index.json').read_text())
                candidate = json.loads((root / 'out/index.json').read_text())
                added = [entry for entry in candidate['plugins'] if entry not in original['plugins']]
                self.assertEqual([entry for entry in candidate['plugins'] if entry in original['plugins']], original['plugins'])
                self.assertEqual([(entry['plugin_id'], entry['version']) for entry in added], [('fatalder.cloud-import', '1.2.1')])
                self.assertEqual(added[0]['permissions'], manifest['permissions'])
                self.assertTrue((root / 'out/SHA256SUMS').read_text().endswith('  fatalder.cloud-import-1.2.1.zip\n'))
