"""Thin online-service client: retain evidence, never retry uncertain mutations."""
import asyncio
import hashlib
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import time
import uuid

from neomega_runtime.managed import IPCRejected
from neomega_runtime.services import ServiceIntent

from .types import DisplaySpec, identifier, positive

PROVIDER_ID = 'neomega.display'
SERVICE_MAJOR = 1
MIN_PROVIDER_VERSION = '1.0.0'


@dataclass
class DisplayCall:
    """Retain or persist to_dict() before discarding an uncertain caller.

    Online call receipts are temporary; request_id queries address the provider's
    business record. An accepted request does not prove client visibility.
    """
    request_id: str | None
    method: str
    intent: ServiceIntent
    call_id: str | None = None
    state: str = 'prepared'
    receipt: dict | None = None
    _signature: str = field(default='', repr=False)

    def to_dict(self) -> dict:
        return deepcopy(dict(request_id=self.request_id, method=self.method,
                             intent=self.intent.payload(), call_id=self.call_id,
                             state=self.state, receipt=self.receipt))


class DisplayCallError(Exception):
    def __init__(self, message: str, call: DisplayCall):
        self.call = call
        self.request_id = call.request_id
        self.call_id = call.call_id
        super().__init__(message)


class DisplayUncertain(DisplayCallError):
    """Query the original request/call; do not replace its request ID and replay."""


class DisplayRejected(DisplayCallError):
    def __init__(self, code: str, call: DisplayCall):
        self.code = code
        super().__init__(code, call)


@dataclass(frozen=True)
class DisplayReceipt:
    """Provider payload plus transport evidence; preserve both."""
    payload: dict
    call: DisplayCall

    @property
    def object(self) -> dict | None:
        return deepcopy(self.payload.get('object'))

    @property
    def object_id(self) -> str | None:
        return (self.payload.get('object') or {}).get('object_id')

    @property
    def revision(self) -> int | None:
        return (self.payload.get('object') or {}).get('revision')

    @property
    def operation_ids(self) -> tuple[str, ...]:
        return tuple((self.payload.get('object') or {}).get('operation_ids', ()))

    def to_dict(self) -> dict:
        return dict(result=deepcopy(self.payload), call=self.call.to_dict())


class DisplayClient:
    """Use DisplayClient(ctx). Mutations require caller-owned stable request IDs.

    A repeated mutation in this client reuses its original completed receipt or
    raises DisplayUncertain without another submit. Across client/Host restarts,
    persist DisplayCall.to_dict() and reconcile with get(request_id=...). No
    automatic resume/replay API is provided. Keep one client per plugin instance.
    """
    def __init__(self, ctx, *, timeout: float = 5, max_retained_requests: int = 4096):
        positive(timeout, 'timeout')
        if timeout > 30:
            raise ValueError('service timeout must not exceed 30 seconds')
        if type(max_retained_requests) is not int or max_retained_requests < 1:
            raise ValueError('max_retained_requests must be a positive integer')
        self.services = ctx.services
        self.timeout = timeout
        self.max_retained_requests = max_retained_requests
        self._calls: dict[str, DisplayCall] = {}

    async def describe(self) -> DisplayReceipt:
        return await self._call('describe', {})

    async def create(self, *, request_id: str, object_key: str,
                     spec: DisplaySpec) -> DisplayReceipt:
        identifier(object_key, 'object_key')
        return await self._call('create', dict(request_id=request_id,
                               object_key=object_key, **self._spec(spec)))

    async def get(self, object_id: str | None = None, *,
                  request_id: str | None = None) -> DisplayReceipt:
        if (object_id is None) == (request_id is None):
            raise ValueError('provide exactly one of object_id or request_id')
        args = ({'object_id': identifier(object_id, 'object_id')} if object_id is not None
                else {'request_id': identifier(request_id, 'request_id')})
        return await self._call('get', args)

    async def update(self, object_id: str, *, request_id: str, revision: int,
                     spec: DisplaySpec) -> DisplayReceipt:
        return await self._call('update', dict(request_id=request_id,
                               **self._target(object_id, revision), **self._spec(spec)))

    async def renew(self, object_id: str, *, request_id: str, revision: int,
                    ttl_seconds: float) -> DisplayReceipt:
        positive(ttl_seconds, 'ttl_seconds')
        return await self._call('renew', dict(request_id=request_id,
                               **self._target(object_id, revision), ttl_seconds=ttl_seconds))

    async def close(self, object_id: str, *, request_id: str,
                    revision: int) -> DisplayReceipt:
        return await self._call('close', dict(request_id=request_id,
                               **self._target(object_id, revision)))

    async def channel_acquire(self, *, request_id: str, player: str, channel: str,
                              text: str, priority: int = 50, ttl_seconds: float = 10) -> DisplayReceipt:
        return await self._call('channel_acquire', dict(request_id=request_id, player=player,
            channel=channel, text=text, priority=priority, ttl_seconds=ttl_seconds))

    async def channel_renew(self, lease_id: str, *, request_id: str, revision: int,
                            ttl_seconds: float) -> DisplayReceipt:
        return await self._call('channel_renew', dict(lease_id=lease_id, request_id=request_id,
            revision=revision, ttl_seconds=ttl_seconds))

    async def channel_release(self, lease_id: str, *, request_id: str, revision: int) -> DisplayReceipt:
        return await self._call('channel_release', dict(lease_id=lease_id, request_id=request_id, revision=revision))

    async def channel_get(self, lease_id: str) -> DisplayReceipt:
        return await self._call('channel_get', dict(lease_id=lease_id))

    @staticmethod
    def _spec(spec: DisplaySpec) -> dict:
        if not isinstance(spec, DisplaySpec):
            raise ValueError('spec must be DisplaySpec')
        return spec.to_dict()

    @staticmethod
    def _target(object_id: str, revision: int) -> dict:
        identifier(object_id, 'object_id')
        if type(revision) is not int or revision < 1:
            raise ValueError('revision must be a positive integer')
        return dict(object_id=object_id, revision=revision)

    @staticmethod
    def _result(call: DisplayCall) -> DisplayReceipt:
        return DisplayReceipt(deepcopy(call.receipt['result']), call)

    async def _call(self, method: str, arguments: dict) -> DisplayReceipt:
        mutation = method in ('create', 'update', 'renew', 'close', 'channel_acquire', 'channel_renew', 'channel_release')
        request_id = identifier(arguments['request_id'], 'request_id') if mutation else None
        signature = json.dumps([method, arguments], sort_keys=True, allow_nan=False)
        if request_id in self._calls:
            previous = self._calls[request_id]
            if previous._signature != signature:
                raise ValueError('request_id already belongs to a different mutation')
            if previous.state == 'replied':
                return self._result(previous)
            if previous.state == 'rejected':
                code = (previous.receipt or {}).get('rejection', {}).get('code', 'admission_rejected')
                raise DisplayRejected(code, previous)
            raise DisplayUncertain('original request is unresolved; query its evidence', previous)
        if mutation and len(self._calls) >= self.max_retained_requests:
            raise ValueError('retained request limit reached; persist evidence before replacing client')
        transport_key = ('display_' + hashlib.sha256(request_id.encode('ascii')).hexdigest()
                         if mutation else uuid.uuid4().hex)
        intent = self.services.prepare(f'plugin.{PROVIDER_ID}.{method}', arguments,
            major=SERVICE_MAJOR, idempotency_key=transport_key,
            deadline=(datetime.now(timezone.utc) + timedelta(seconds=self.timeout)).isoformat())
        call = DisplayCall(request_id, method, intent, _signature=signature)
        if mutation:
            self._calls[request_id] = call
        end = time.monotonic() + self.timeout
        try:
            call.state = 'submitting'
            call.call_id = await self.services.submit(intent, timeout=self.timeout)
            call.state = 'waiting'
            remaining = end - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('wait budget exhausted')
            call.receipt = await self.services.wait(call.call_id, timeout=remaining)
            if call.receipt['state'] != 'replied':
                rejection = call.receipt.get('rejection')
                if rejection is not None:
                    call.state = 'rejected'
                    raise DisplayRejected(rejection['code'], call)
                raise RuntimeError('service ended without a reply; inspect receipt')
            if not isinstance(call.receipt['result'], dict):
                raise ValueError('provider result must be an object')
            call.state = 'replied'
            return self._result(call)
        except DisplayRejected:
            raise
        except IPCRejected as exc:
            if call.call_id is None:
                call.state = 'rejected'
                raise DisplayRejected('admission_rejected', call) from exc
            call.state = 'unknown'
            raise DisplayUncertain('receipt access rejected after admission; retain original call', call) from exc
        except asyncio.CancelledError as exc:
            call.state = 'unknown'
            exc.display_call = call
            raise
        except Exception as exc:
            call.state = 'unknown'
            raise DisplayUncertain('display call unresolved; query original request before further writes', call) from exc
