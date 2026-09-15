import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from fatalder_plugin.client import WorkerClient, WorkerError


class ClientTest(unittest.TestCase):
    def setUp(self):
        self.seen = []
        self.payload = b"building data"
        self.ref = {"object_id": "obj-1", "sha256": "sha256:" + hashlib.sha256(self.payload).hexdigest(),
                    "size_bytes": len(self.payload), "display_name": "house.bdx"}
        self.mode = "normal"
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                owner.seen.append((self.command, self.path, dict(self.headers)))
                if owner.mode == "redirect":
                    self.send_response(302)
                    self.send_header("Location", owner.url + "/v2/stolen")
                    self.end_headers()
                    return
                if owner.mode == "error":
                    self.send_json({"error": {"code": "secret-KEY", "message": "secret-KEY"}}, 403)
                    return
                if self.path == "/v2/objects/policy":
                    self.send_json({"version": 5, "max_object_bytes": 1024})
                elif self.path.startswith("/v2/objects/lookup?"):
                    self.send_json({"error": {"code": "not_found"}}, 404)
                elif self.path.startswith("/v2/objects/"):
                    data = b"wrong content" if owner.mode == "bad_hash" else owner.payload
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                elif self.path.endswith("/events"):
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    if owner.mode == "long_event":
                        self.wfile.write(b"data: " + b"a" * 65536 + b"\n\n")
                    else:
                        self.wfile.write(b': ping\n\ndata: {"seq":1}\n\ndata: {"seq":2}\n')
                else:
                    self.send_json({"targets": []})

            def do_POST(self):
                owner.seen.append((self.command, self.path, dict(self.headers)))
                owner.uploaded = self.rfile.read(int(self.headers["Content-Length"]))
                self.send_json({"object": owner.ref, "reused": False}, 201)

            def send_json(self, value, status=200):
                self.send_response(status)
                self.end_headers()
                self.wfile.write(json.dumps(value).encode())

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = "http://127.0.0.1:" + str(self.server.server_port)
        self.client = WorkerClient(self.url, lambda: "secret-KEY", allow_loopback_http=True)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_url_policy(self):
        for url in (self.url, "http://example.com", "https://user:key@example.com",
                    "https://example.com?key=secret", "https://example.com/#key", "https://example.com/v2"):
            with self.assertRaises(WorkerError):
                WorkerClient(url, lambda: "key")
        with self.assertRaises(WorkerError):
            WorkerClient("http://example.com", lambda: "key", allow_loopback_http=True)

    def test_redirect_never_forwards_key(self):
        self.mode = "redirect"
        with self.assertRaises(WorkerError) as error:
            self.client.request("GET", "/v2/targets")
        self.assertEqual(error.exception.code, "redirect_refused")
        self.assertEqual(len(self.seen), 1)

    def test_error_body_not_exposed_and_key_rotates(self):
        self.mode = "error"
        with self.assertRaises(WorkerError) as error:
            self.client.request("GET", "/v2/targets")
        self.assertEqual(str(error.exception), "http_error")
        self.assertEqual(error.exception.status, 403)
        self.assertNotIn("secret", repr(error.exception.__dict__))
        self.mode = "normal"
        key = ["first"]
        client = WorkerClient(self.url, lambda: key[0], allow_loopback_http=True)
        client.request("GET", "/v2/targets")
        key[0] = "second"
        client.request("GET", "/v2/targets")
        self.assertEqual([item[2]["X-Fatalder-API-Key"] for item in self.seen],
                         ["secret-KEY", "first", "second"])

    def test_proxy_environment_is_ignored(self):
        with patch.dict("os.environ", {"HTTP_PROXY": "http://127.0.0.1:1", "http_proxy": "http://127.0.0.1:1"}):
            self.assertEqual(self.client.request("GET", "/v2/targets"), {"targets": []})

    def test_upload_stream_and_download(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "城堡.bdx"
            source.write_bytes(self.payload)
            self.assertEqual(self.client.upload(source), self.ref)
            self.assertEqual(self.uploaded, self.payload)
            self.assertEqual(self.seen[-1][2]["Content-Type"], "application/octet-stream")
            target = Path(directory) / "out.bdx"
            self.assertEqual(self.client.download(self.ref, target), target)
            self.assertEqual(target.read_bytes(), self.payload)

    def test_oversize_upload_stops_before_post(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "big.bdx"
            source.write_bytes(b"x" * 1025)
            with self.assertRaises(WorkerError) as error:
                self.client.upload(source)
            self.assertEqual(error.exception.code, "upload_too_large")
            self.assertEqual(len(self.seen), 1)

    def test_bad_download_preserves_old_file_and_cleans_partial(self):
        self.mode = "bad_hash"
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "out.bdx"
            target.write_bytes(b"previous")
            with self.assertRaises(WorkerError) as error:
                self.client.download(self.ref, target)
            self.assertEqual(error.exception.code, "object_hash_mismatch")
            self.assertEqual(target.read_bytes(), b"previous")
            self.assertEqual(list(Path(directory).iterdir()), [target])

    def test_sse_disconnect_replay_and_no_foreground(self):
        stop = threading.Event()
        self.assertEqual(list(self.client.events("job-one", 0, stop)), [{"seq": 1}])
        self.assertEqual(list(self.client.events("job-one", 1, stop)), [{"seq": 1}])
        self.assertEqual(self.seen[-1][2]["Last-Event-ID"], "1")
        self.assertNotIn("X-Fatalder-Task-Lifecycle", self.seen[-1][2])
        stop.set()
        self.assertEqual(list(self.client.events("job-one", 1, stop)), [])

    def test_sse_line_bounded(self):
        self.mode = "long_event"
        with self.assertRaises(WorkerError) as error:
            list(self.client.events("job-one", 0, threading.Event()))
        self.assertEqual(error.exception.code, "event_too_large")


if __name__ == "__main__":
    unittest.main()
