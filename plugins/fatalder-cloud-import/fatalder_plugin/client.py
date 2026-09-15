"""Bounded synchronous Worker transport; callers own retries and durable cursors."""
from __future__ import annotations

import hashlib
import http.client
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import socket
import stat
import tempfile
import threading
from urllib.parse import quote, urlencode, urlsplit


class WorkerError(Exception):
    """Safe error metadata: never retains response bodies or credentials."""

    def __init__(self, code: str, status: int | None = None):
        self.code = code
        self.status = status
        super().__init__(code)


_ERROR_CODES = frozenset({
    "invalid_request", "invalid_query", "object_hash_mismatch", "object_size_mismatch",
    "not_found", "idempotency_conflict", "invalid_state", "quote_mismatch",
    "quote_expired", "recovery_required", "task_local_gate_conflict",
    "storage_quota_exceeded", "unauthorized", "forbidden", "rate_limited",
})
_JSON_LIMIT = 4 << 20
_LINE_LIMIT = 64 << 10
_EVENT_LIMIT = 1 << 20
_CHUNK = 64 << 10


class WorkerClient:
    def __init__(self, base_url, api_key, timeout=60, *, max_upload_bytes=100 << 20,
                 allow_loopback_http=False):
        try:
            parsed = urlsplit(base_url)
            host = parsed.hostname
            loopback = host == "localhost"
            if host and not loopback:
                try:
                    loopback = ipaddress.ip_address(host).is_loopback
                except ValueError:
                    pass
            if (not host or parsed.username is not None or parsed.password is not None
                    or parsed.query or parsed.fragment or parsed.path not in ("", "/")
                    or any(ord(c) <= 32 or ord(c) == 127 for c in base_url)
                    or (parsed.scheme != "https" and not
                        (parsed.scheme == "http" and allow_loopback_http and loopback))):
                raise ValueError
            port = parsed.port
            if not math.isfinite(timeout) or timeout <= 0 or type(max_upload_bytes) is not int or max_upload_bytes < 1:
                raise ValueError
        except (TypeError, ValueError):
            raise WorkerError("invalid_worker_url") from None
        self._host, self._port, self._scheme = host, port, parsed.scheme
        self._api_key = api_key
        self.timeout = timeout
        self.max_upload_bytes = max_upload_bytes

    def _open(self, method, path, body=None, headers=None):
        if (not isinstance(path, str) or not path.startswith("/v2/")
                or "#" in path or "\\" in path
                or any(ord(c) <= 32 or ord(c) >= 127 for c in path)):
            raise WorkerError("invalid_api_path")
        try:
            key = self._api_key()
        except Exception:
            raise WorkerError("secret_unavailable") from None
        if not isinstance(key, str) or not key or any(ord(c) <= 32 or ord(c) >= 127 for c in key):
            raise WorkerError("secret_unavailable")
        outgoing = {"X-Fatalder-API-Key": key, "Accept": "application/json"}
        outgoing.update(headers or {})
        cls = http.client.HTTPSConnection if self._scheme == "https" else http.client.HTTPConnection
        connection = cls(self._host, self._port, timeout=self.timeout)
        try:
            connection.request(method, path, body=body, headers=outgoing)
            response = connection.getresponse()
            if not 200 <= response.status < 300:
                status = response.status
                code = "http_error"
                if 300 <= status < 400:
                    code = "redirect_refused"
                else:
                    try:
                        data = response.read(_JSON_LIMIT + 1)
                        if len(data) <= _JSON_LIMIT:
                            candidate = json.loads(data).get("error", {}).get("code")
                            if isinstance(candidate, str) and candidate in _ERROR_CODES:
                                code = candidate
                    except (ValueError, AttributeError, OSError, http.client.HTTPException):
                        pass
                raise WorkerError(code, status)
            return connection, response
        except WorkerError:
            connection.close()
            raise
        except (OSError, ValueError, http.client.HTTPException):
            connection.close()
            raise WorkerError("transport_error") from None

    @staticmethod
    def _json(response):
        try:
            data = response.read(_JSON_LIMIT + 1)
            if len(data) > _JSON_LIMIT:
                raise WorkerError("response_too_large")
            if response.status == 204:
                return None
            return json.loads(data)
        except (ValueError, UnicodeError):
            raise WorkerError("invalid_response") from None
        except (OSError, http.client.HTTPException):
            raise WorkerError("transport_error") from None

    def request(self, method, path, body=None):
        try:
            data = None if body is None else json.dumps(body, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError):
            raise WorkerError("invalid_request") from None
        connection, response = self._open(method, path, data, {"Content-Type": "application/json"})
        try:
            return self._json(response)
        finally:
            connection.close()

    @staticmethod
    def _ref(value):
        if (not isinstance(value, dict) or not isinstance(value.get("object_id"), str)
                or not value["object_id"] or len(value["object_id"]) > 512
                or not isinstance(value.get("sha256"), str)
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", value["sha256"])
                or type(value.get("size_bytes")) is not int or value["size_bytes"] < 0):
            raise WorkerError("invalid_object_ref")
        return dict(value)

    def upload(self, path):
        policy = self.request("GET", "/v2/objects/policy")
        limit = policy.get("max_object_bytes") if isinstance(policy, dict) else None
        if type(limit) is not int or limit <= 0:
            raise WorkerError("invalid_response")
        try:
            with Path(path).open("rb") as source:
                metadata = os.fstat(source.fileno())
                if not stat.S_ISREG(metadata.st_mode):
                    raise WorkerError("invalid_upload_file")
                size = metadata.st_size
                if size > min(limit, self.max_upload_bytes):
                    raise WorkerError("upload_too_large")
                digest = hashlib.sha256()
                while chunk := source.read(_CHUNK):
                    digest.update(chunk)
                    if source.tell() > size:
                        raise WorkerError("upload_file_changed")
                sha = "sha256:" + digest.hexdigest()
                query = urlencode({"sha256": sha, "size_bytes": size})
                try:
                    result = self.request("GET", "/v2/objects/lookup?" + query)
                except WorkerError as exc:
                    if exc.status != 404:
                        raise
                    result = None
                if result is None:
                    source.seek(0)
                    # UTF-8 bytes preserve Chinese filenames in Go's raw header contract.
                    name = Path(path).name
                    if any(ord(c) < 32 or ord(c) == 127 for c in name):
                        raise WorkerError("invalid_upload_file")
                    conn, response = self._open("POST", "/v2/objects", source, {
                        "Content-Type": "application/octet-stream", "Content-Length": str(size),
                        "X-Object-SHA256": sha, "X-Object-Size": str(size),
                        "X-Object-Name": name.encode("utf-8"),
                    })
                    try:
                        result = self._json(response)
                    finally:
                        conn.close()
                ref = self._ref(result.get("object") if isinstance(result, dict) else None)
                if ref["sha256"] != sha or ref["size_bytes"] != size:
                    raise WorkerError("object_hash_mismatch")
                return ref
        except OSError:
            raise WorkerError("upload_io_error") from None

    def download(self, ref, destination):
        ref = self._ref(ref)
        if ref["size_bytes"] > self.max_upload_bytes:
            raise WorkerError("download_too_large")
        path = "/v2/objects/" + quote(ref["object_id"], safe="") + "?" + urlencode({
            "sha256": ref["sha256"], "size_bytes": ref["size_bytes"],
        })
        conn, response = self._open("GET", path)
        temporary = None
        try:
            destination = Path(destination)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=destination.parent, prefix=".fatalder-", delete=False) as output:
                temporary = Path(output.name)
                digest, count = hashlib.sha256(), 0
                while chunk := response.read(_CHUNK):
                    count += len(chunk)
                    if count > ref["size_bytes"]:
                        raise WorkerError("object_size_mismatch")
                    output.write(chunk)
                    digest.update(chunk)
                if count != ref["size_bytes"]:
                    raise WorkerError("object_size_mismatch")
                if "sha256:" + digest.hexdigest() != ref["sha256"]:
                    raise WorkerError("object_hash_mismatch")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, destination)
            return destination
        except (OSError, http.client.HTTPException):
            raise WorkerError("download_io_error") from None
        finally:
            conn.close()
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def events(self, job_id, after_seq, stop: threading.Event):
        if type(after_seq) is not int or after_seq < 0:
            raise WorkerError("invalid_sequence")
        if stop.is_set():
            return
        path = "/v2/jobs/" + quote(job_id, safe="") + "/events"
        conn, response = self._open("GET", path, headers={
            "Accept": "text/event-stream", "Last-Event-ID": str(after_seq),
        })
        try:
            data, length = [], 0
            while not stop.is_set():
                line = response.readline(_LINE_LIMIT + 1)
                if len(line) > _LINE_LIMIT:
                    raise WorkerError("event_too_large")
                if not line:  # An incomplete final event must be replayed after reconnect.
                    return
                length += len(line)
                if length > _EVENT_LIMIT:
                    raise WorkerError("event_too_large")
                line = line.rstrip(b"\r\n")
                if not line:
                    if data:
                        try:
                            event = json.loads(b"\n".join(data))
                        except (ValueError, UnicodeError):
                            raise WorkerError("invalid_event") from None
                        if not isinstance(event, dict):
                            raise WorkerError("invalid_event")
                        yield event
                    data, length = [], 0
                elif line.startswith(b"data:"):
                    data.append(line[5:].removeprefix(b" "))
        except (socket.timeout, TimeoutError):
            return
        except (OSError, http.client.HTTPException):
            raise WorkerError("transport_error") from None
        finally:
            conn.close()
