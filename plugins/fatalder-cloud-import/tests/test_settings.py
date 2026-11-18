import dataclasses
import json
from pathlib import Path
import tempfile
import unittest

from neomega_runtime.config import load_config
from neomega_runtime.config_errors import ConfigError
from neomega_runtime.config_schema import check_config_schema, schema_for_config

from fatalder_plugin.settings import Settings

ROOT = Path(__file__).resolve().parents[1]


class SettingsTests(unittest.TestCase):
    def base(self, **overrides):
        data = {
            'worker_url': 'https://worker.example.com',
            'api_key': 'secret:api_key',
            'target_server_id': 'srv-1',
            'rental_server_code': '123456',
            'admin_uuids': ['00000000-0000-4000-8000-000000000001'],
        }
        data.update(overrides)
        return data

    def test_schema_and_example_match_settings(self):
        schema = json.loads((ROOT / 'config.schema.json').read_text())
        check_config_schema(schema)
        self.assertEqual(schema, schema_for_config(Settings))
        loaded = load_config(Settings, ROOT / 'config.example.json')
        self.assertEqual(loaded.target_server_id, 'example')
        self.assertEqual(loaded.rental_server_code, '123456')
        self.assertFalse(loaded.revoke_operator_on_completion)

    def test_rejects_invalid_settings(self):
        bad_cases = [
            {'worker_url': 'http://worker.example.com'},
            {'worker_url': 'https://user:pass@worker.example.com'},
            {'worker_url': 'https://worker.example.com/v2'},
            {'worker_url': 'https://worker.example.com?q=1'},
            {'target_server_id': ''},
            {'rental_server_code': 'abc'},
            {'admin_uuids': []},
            {'admin_uuids': ['not-a-uuid']},
            {'files_directory': '/etc'},
            {'files_directory': '../outside'},
            {'files_directory': ''},
        ]
        for bad in bad_cases:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                Settings(**self.base(**bad))

        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / 'bad.json'
            p.write_text(json.dumps(self.base(poll_seconds=0)))
            with self.assertRaises((ConfigError, ValueError)):
                load_config(Settings, p)

    def test_manifest_version_is_bumped(self):
        manifest = json.loads((ROOT / 'manifest.json').read_text())
        self.assertEqual(manifest['id'], 'fatalder.cloud-import')
        self.assertEqual(manifest['version'], '1.2.1')


if __name__ == '__main__':
    unittest.main()
