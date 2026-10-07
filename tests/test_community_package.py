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

    def test_merged_examples_preserve_manifests_and_use_reviewed_purposes(self):
        with tempfile.TemporaryDirectory() as folder:
            for name in ['points-exchange', 'starter-service']:
                root = ROOT / 'plugins' / name
                output = Path(folder) / (name + '.zip')
                package.build(root, output)
                with zipfile.ZipFile(output) as archive:
                    self.assertEqual(archive.read('manifest.json'), (root / 'manifest.json').read_bytes())
                    self.assertIn('config.example.json', archive.namelist())
            spec = importlib.util.spec_from_file_location('points_packager', ROOT / 'plugins/points-exchange/tools/package.py')
            old = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(old)
            old.build(Path(folder) / 'old.zip')
            self.assertEqual((Path(folder) / 'old.zip').read_bytes(), (Path(folder) / 'points-exchange.zip').read_bytes())

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
                manifest['version'] = '1.0.5'
                manifest_path.write_text(json.dumps(manifest))
                prepare.prepare('fatalder-cloud-import', root / 'out')
                original = json.loads((root / 'catalog/index.json').read_text())
                candidate = json.loads((root / 'out/index.json').read_text())
                self.assertEqual(candidate['plugins'][:-1], original['plugins'])
                self.assertEqual(candidate['plugins'][-1]['version'], '1.0.5')
                self.assertEqual(candidate['plugins'][-1]['permissions'], manifest['permissions'])
                self.assertTrue((root / 'out/SHA256SUMS').read_text().endswith('  fatalder.cloud-import-1.0.5.zip\n'))
