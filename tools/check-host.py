#!/usr/bin/env python3
"""Check a release ZIP using the existing offline Host and public admin CLI."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--host', type=Path, required=True)
p.add_argument('--agent', type=Path, required=True, help='neomega-agent checkout with scripts and sdk/python')
p.add_argument('package', type=Path)
a = p.parse_args()
with tempfile.TemporaryDirectory(prefix='plugin-host-', dir=os.environ.get('TMPDIR')) as folder:
    state = Path(folder)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env = dict(os.environ, NEOMEGA_AGENT_STATE_DIR=folder,
        NEOMEGA_AGENT_TOKEN_FILE=str(state / 'token'), NEOMEGA_AGENT_ADDR='127.0.0.1',
        NEOMEGA_AGENT_PORT=str(port), NEOMEGA_AGENT_INSTANCE_DIR=str(state / 'instances'),
        NEOMEGA_RUNTIME_PYTHON=sys.executable, NEOMEGA_RUNTIME_SDK=str(a.agent.resolve() / 'sdk/python'),
        NEOMEGA_RUNTIME_AUTOSTART='0')
    child = subprocess.Popen([str(a.host.resolve())], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        end = time.monotonic() + 10
        while True:
            assert child.poll() is None, 'offline Host exited'
            try:
                opener.open(f'http://127.0.0.1:{port}/healthz', timeout=.2).close()
                break
            except OSError:
                if time.monotonic() > end:
                    raise AssertionError('offline Host startup timed out')
                time.sleep(.02)
        def admin(action, *args):
            result = subprocess.run([sys.executable, str(a.agent.resolve() / 'scripts/runtime-admin.py'),
                '--url', f'http://127.0.0.1:{port}', '--token-file', str(state / 'token'),
                '--json', action, 'importer', *map(str, args)], capture_output=True, text=True, timeout=30)
            if result.returncode and action != 'config-validate':
                raise AssertionError(f'{action} failed: {result.stderr}')
            return json.loads(result.stdout)
        installed = admin('install', a.package.resolve())
        assert installed['state'] == 'stopped'
        assert installed['manifest']['id'] == 'fatalder.cloud-import'
        config = {'worker_url': 'https://worker.example.invalid', 'api_key': 'secret:worker_api',
            'target_server_id': 'offline-fixture', 'rental_server_code': '12345678',
            'admin_uuids': ['00000000-0000-0000-0000-000000000001']}
        file = state / 'candidate.json'
        file.write_text(json.dumps(config))
        secret = state / 'fixture-secret'
        secret.write_text('offline-fixture-only')
        admin('secret-set', '--secret-name', 'worker_api', '--secret-file', secret)
        validation = admin('config-validate', '--config-file', file)
        assert validation['valid'], validation
        current = admin('config-get')
        applied = admin('config-apply', '--config-file', file, '--expected-revision', current['revision'])
        assert applied['saved'], applied
        saved = admin('config-get')
        assert saved['config']['api_key'] == 'secret:worker_api'
        assert 'offline-fixture-only' not in json.dumps(saved)
        config['admin_uuids'] = False
        file.write_text(json.dumps(config))
        invalid = admin('config-validate', '--config-file', file)
        assert not invalid['valid'], invalid
        assert admin('config-get') == saved, 'failed validation changed saved configuration'
        print('PASS: release ZIP installed stopped; valid config saved; secret reference retained; invalid config rejected without changes')
    finally:
        if child.poll() is None:
            child.terminate()
        try:
            child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=10)
