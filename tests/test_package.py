"""Check the released ZIP through the public packaging command."""
import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / 'plugins/fatalder-cloud-import'


class PackageTests(unittest.TestCase):
    def test_portable_allowlisted_reproducible_package(self):
        with tempfile.TemporaryDirectory() as folder:
            first, second = Path(folder) / 'first.zip', Path(folder) / 'second.zip'
            for output in (first, second):
                subprocess.run([sys.executable, str(PLUGIN / 'tools/package.py'), str(output)], check=True, capture_output=True)
            self.assertEqual(hashlib.sha256(first.read_bytes()).digest(), hashlib.sha256(second.read_bytes()).digest())
            with zipfile.ZipFile(first) as archive:
                self.assertIn('LICENSE', archive.namelist())
                self.assertIn('manifest.json', archive.namelist())
                self.assertNotIn('tests/test_controller.py', archive.namelist())
                self.assertTrue(all(info.date_time == (1980, 1, 1, 0, 0, 0) for info in archive.infolist()))
                self.assertFalse(any('..' in Path(name).parts or name.startswith('/') for name in archive.namelist()))


if __name__ == '__main__':
    unittest.main()
