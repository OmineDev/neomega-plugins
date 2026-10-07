"""Installation-isolated persistence and trusted caller authorization."""
import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from neomega_runtime import Plugin, ServiceRejected
from neomega_runtime.business import BusinessConflict
from neomega_runtime.storage import CommitUncertain
from neomega_runtime.managed import IPCRejected

@dataclass(frozen=True)
class Namespace:
    name: str
    owner: str
    readers: list[str] = field(default_factory=list)
    writers: list[str] = field(default_factory=list)

@dataclass(frozen=True)
class Binding:
    namespace: str
    objective: str
    game_objective: str

@dataclass(frozen=True)
class Settings:
    shared_namespaces: list[Namespace] = field(default_factory=list)
    sync_bindings: list[Binding] = field(default_factory=list)
    def __post_init__(self):
        if len({x.name for x in self.shared_namespaces}) != len(self.shared_namespaces):
            raise ValueError('duplicate namespace')
        if len({(x.namespace, x.objective) for x in self.sync_bindings}) != len(self.sync_bindings):
            raise ValueError('duplicate sync binding')
        if len({x.game_objective for x in self.sync_bindings}) != len(self.sync_bindings):
            raise ValueError('game objectives must have one virtual owner')

def reject(code):
    raise ServiceRejected(code)

def ident(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_.:-]{1,96}', value):
        reject('invalid_identifier')
    return value

def key(*parts):
    return 'data:' + hashlib.sha256('\x00'.join(parts).encode()).hexdigest()

def utc():
    return datetime.now(timezone.utc).isoformat()

class Provider(Plugin):
    config_type = Settings
    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = None
        self.pending = None
        self.pending_path = None
    async def on_start(self, ctx):
        self.pending_path = ctx.data_dir / 'governance-pending.json'
        if self.pending_path.exists():
            self.pending = json.loads(self.pending_path.read_text())['commit_id']
        if not await self.reconcile_pending(ctx):
            return
        tx = await ctx.storage.transaction()
        version = tx.get('schema_version')
        if version is None:
            if tx._values:
                raise ValueError('schema_version missing from existing state; explicit migration required')
            tx.set('schema_version', 1)
            tx.prepare()
            self.mark_pending(tx.commit_id)
            try:
                await tx.save()
            except IPCRejected:
                self.clear_pending()
                raise
            self.clear_pending()
        elif type(version) is not int or version != 1:
            raise ValueError('unsupported schema_version')
    def sync_directory(self):
        fd = os.open(self.pending_path.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    def mark_pending(self, commit_id, business_id=None):
        self.pending = commit_id
        if self.pending_path is not None:
            temporary = self.pending_path.with_suffix('.tmp')
            with temporary.open('w') as stream:
                json.dump({'commit_id': commit_id, 'business_id': business_id}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.pending_path)
            self.sync_directory()
    def clear_pending(self):
        if self.pending_path is not None:
            self.pending_path.unlink(missing_ok=True)
            self.sync_directory()
        self.pending = None
    async def reconcile_pending(self, ctx):
        if self.pending is None:
            return True
        if await ctx.storage.receipt(self.pending) is None:
            return False
        self.clear_pending()
        return True
    async def on_maintenance(self, ctx, request):
        async with self.lock:
            token = request['token']
            if not await self.reconcile_pending(ctx):
                return {'status': 'busy', 'token': token}
            if request['operation'] == 'seal':
                if self.sealed not in (None, token):
                    return {'status': 'busy', 'token': token}
                self.sealed = token
                return {'status': 'sealed', 'token': token}
            if self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            self.sealed = None
            return {'status': 'released', 'token': token}
    def namespace(self, ctx, args, call, write=False):
        if call is None:
            reject('permission_denied')
        if self.sealed:
            reject('maintenance_sealed')
        ns = args.get('namespace', call.installation_id)
        if not isinstance(ns, str) or not 1 <= len(ns.encode()) <= 256:
            reject('invalid_namespace')
        config = next((x for x in ctx.config.shared_namespaces if x.name == ns), None)
        allowed = ({config.owner, *config.writers, *([] if write else config.readers)} if config else {ns})
        if call.installation_id not in allowed:
            reject('namespace_forbidden')
        return ns
    async def player(self, ctx, value):
        if not isinstance(value, str) or not value:
            reject('invalid_player')
        result = await ctx.services.call('plugin.neomega.players.get', {'player_id': value})
        if result.get('status') != 'resolved' or not result.get('player'):
            reject('player_not_found')
        return result['player']
    async def begin(self, ctx, args, call, method, data_key):
        if not await self.reconcile_pending(ctx):
            reject('commit_unknown')
        request_id = ident(args.get('request_id'))
        bid = key(call.installation_id, method, request_id).replace(':', '_')
        try:
            business = await ctx.business.begin(bid, args, keys=[data_key])
        except BusinessConflict:
            reject('request_conflict')
        return business
    def cas(self, args, row):
        if type(args.get('expected_revision')) is not int or args['expected_revision'] != row['revision']:
            reject('revision_conflict')
    async def commit(self, business, row, data_key, args, call, method, result=None):
        row['revision'] += 1
        business.state.set(data_key, row)
        result = dict(result or {}, revision=row['revision'], request_id=args['request_id'], status='saved')
        audit = dict(method=method, caller=call.installation_id, at=utc(), result=result)
        business.state.set('audit:' + business.business_id, audit)
        self.mark_pending(business.state.commit_id, business.business_id)
        try:
            receipt = await business.commit(result)
        except CommitUncertain:
            # Lookup only: never resubmit an uncertain mutation or game action.
            previous = await business.business.get(business.business_id)
            if previous is None:
                reject('commit_unknown')
            receipt = previous
        except (ValueError, TypeError):
            self.clear_pending()
            raise
        except IPCRejected as exc:
            self.clear_pending()
            if exc.code == 'revision_conflict':
                reject('revision_conflict')
            raise
        self.clear_pending()
        return dict(receipt['result'], operation_ids=receipt['operation_ids'])
    @staticmethod
    def previous(business):
        return dict(business.receipt['result'], operation_ids=business.receipt['operation_ids'])
