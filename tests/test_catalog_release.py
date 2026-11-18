"""Explicit release publication preparation through its command line."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

ROOT=Path(__file__).resolve().parents[1]
class ReleaseTests(unittest.TestCase):
    def test_explicit_version_generates_pinned_release_and_preserves_existing_versions(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);package=root/'test.demo-1.0.0.zip';index=root/'index.json'
            with zipfile.ZipFile(package,'w') as archive:
                archive.writestr('manifest.json',json.dumps({'id':'test.demo','version':'1.0.0','permissions':{'operations':[],'services':[]}}))
            args=[sys.executable,str(ROOT/'tools/catalog-release.py'),'--package',str(package),'--plugin','test.demo','--version','1.0.0','--name','示例','--index',str(index)]
            result=subprocess.run(args,capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
            entry=json.loads(index.read_text())['plugins'][0]
            self.assertEqual(entry['url'],'https://github.com/OmineDev/neomega-plugins/releases/download/test.demo-v1.0.0/test.demo-1.0.0.zip')
            self.assertEqual(entry['sha256'],hashlib.sha256(package.read_bytes()).hexdigest())
            self.assertEqual(entry['kind'],'app')
            before=index.read_bytes()
            args[args.index('--version')+1]='2.0.0'
            self.assertNotEqual(subprocess.run(args,capture_output=True).returncode,0)
            self.assertEqual(index.read_bytes(),before)

    def test_rejects_prerelease_preserves_manifest_purposes_and_sorts_semver(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            index = root / 'index.json'
            for ver in ('1.10.0', '1.2.0'):
                pkg = root / f'test.demo-{ver}.zip'
                with zipfile.ZipFile(pkg, 'w') as archive:
                    archive.writestr('manifest.json', json.dumps({
                        'id': 'test.demo',
                        'version': ver,
                        'permissions': {
                            'operations': ['framework.command.execute'],
                            'services': [],
                            'purposes': {'operation:framework.command.execute': '执行命令'},
                        },
                    }))
                res = subprocess.run([
                    sys.executable, str(ROOT / 'tools/catalog-release.py'),
                    '--package', str(pkg), '--plugin', 'test.demo', '--version', ver,
                    '--name', '示例', '--index', str(index),
                ], capture_output=True, text=True)
                self.assertEqual(res.returncode, 0, res.stderr)

            plugins = json.loads(index.read_text())['plugins']
            self.assertEqual([e['version'] for e in plugins], ['1.2.0', '1.10.0'])
            self.assertEqual(plugins[0]['permission_purposes'], {'operation:framework.command.execute': '执行命令'})

            bad_pkg = root / 'test.demo-1.2.0-rc.1.zip'
            with zipfile.ZipFile(bad_pkg, 'w') as archive:
                archive.writestr('manifest.json', json.dumps({'id': 'test.demo', 'version': '1.2.0-rc.1', 'permissions': {}}))
            bad = subprocess.run([
                sys.executable, str(ROOT / 'tools/catalog-release.py'),
                '--package', str(bad_pkg), '--plugin', 'test.demo', '--version', '1.2.0-rc.1',
                '--name', '示例', '--index', str(index),
            ], capture_output=True, text=True)
            self.assertNotEqual(bad.returncode, 0)
