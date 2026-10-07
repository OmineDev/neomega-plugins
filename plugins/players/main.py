"""Host-observed identities with consumer-owned, revisioned profile fields."""
import asyncio
import hashlib
import json
import os
import time
from dataclasses import dataclass

from neomega_runtime import Plugin, service
from neomega_runtime.services import ServiceRejected
from neomega_runtime.storage import CommitUncertain
from neomega_runtime.managed import IPCRejected


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def text(value, maximum=256):
    if not isinstance(value, str) or not value or len(value.encode()) > maximum:
        raise ServiceRejected('invalid_argument')
    return value


def index_key(kind, value):
    return 'identity:' + kind + ':' + digest(value)


@dataclass(frozen=True)
class Settings:
    observation_interval: float = 30.0
    max_players: int = 16384
    max_names: int = 64
    max_field_bytes: int = 16384

    def __post_init__(self):
        if not 5 <= self.observation_interval <= 3600 or not 1 <= self.max_players <= 100000:
            raise ValueError('invalid observation interval or player capacity')
        if not 1 <= self.max_names <= 128 or not 128 <= self.max_field_bytes <= 32768:
            raise ValueError('invalid profile bounds')


class Players(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = None
        self.uncertain = None
        self.pending_path = None

    def owner(self, call):
        if call is None:
            raise ServiceRejected('permission_denied')
        return call.installation_id

    def clear_pending(self):
        self.uncertain = None
        if self.pending_path is not None:
            self.pending_path.unlink(missing_ok=True)
            fd = os.open(self.pending_path.parent, os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    async def reconcile(self, ctx):
        if self.uncertain is None:
            return True
        if await ctx.storage.receipt(self.uncertain) is None:
            return False
        self.clear_pending()
        return True

    async def writable(self, ctx):
        if self.sealed:
            raise ServiceRejected('busy')
        if not await self.reconcile(ctx):
            raise ServiceRejected('unknown')

    async def save(self, tx):
        tx.prepare()
        self.uncertain = tx.commit_id
        if self.pending_path is not None:
            temporary = self.pending_path.with_suffix('.tmp')
            with temporary.open('w') as stream:
                json.dump({'commit_id': self.uncertain}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.pending_path)
            fd = os.open(self.pending_path.parent, os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        try:
            result = await tx.save()
        except IPCRejected:
            self.clear_pending()
            raise
        except CommitUncertain:
            raise ServiceRejected('unknown') from None
        # Cancellation deliberately preserves the durable original commit ID.
        self.clear_pending()
        return result

    async def on_start(self, ctx):
        self.pending_path = ctx.data_dir / 'players-pending.json'
        if self.pending_path.exists():
            self.uncertain = json.loads(self.pending_path.read_text())['commit_id']
        version = await ctx.storage.get('schema_version')
        if version is not None and (type(version) is not int or version != 1):
            raise ValueError('unsupported players state schema')
        if await self.reconcile(ctx):
            if version is None:
                tx = await ctx.storage.transaction(keys=['schema_version'])
                tx.set('schema_version', 1)
                await self.save(tx)
            await self.observe(ctx)
        ctx.every(ctx.config.observation_interval, lambda: self.observe(ctx), name='players-observe')

    async def observe(self, ctx):
        roster = await ctx.players()
        async with self.lock:
            if self.sealed or not await self.reconcile(ctx):
                return
            await self.writable(ctx)
            for identity in roster:
                uuid, xuid, name = (identity.get(k, '') for k in ('uuid', 'xuid', 'name'))
                if not uuid and not xuid:
                    continue
                candidates = set()
                for kind, value in (('uuid', uuid), ('xuid', xuid)):
                    if value:
                        candidates.update(await ctx.storage.get(index_key(kind, value), []))
                if len(candidates) > 1:
                    ctx.log.warning('identity conflict; refusing implicit profile merge')
                    continue
                pid = next(iter(candidates)) if candidates else ('uuid:' + uuid if uuid else 'xuid:' + xuid)
                key = 'player:' + digest(pid)
                record = await ctx.storage.get(key)
                if record and any(record.get(k) and identity.get(k) and record[k] != identity[k] for k in ('uuid', 'xuid')):
                    ctx.log.warning('identity conflict; preserving existing stable identity')
                    continue
                keys = [key, 'count', 'bucket:' + digest(pid)[:2]]
                indexes = {index_key(k, v) for k, v in (('uuid', uuid), ('xuid', xuid), ('name', name)) if v}
                keys.extend(indexes)
                tx = await ctx.storage.transaction(keys=keys)
                record = tx.get(key)
                now = time.time()
                if record is None:
                    count = tx.get('count', 0)
                    if count >= ctx.config.max_players:
                        ctx.log.warning('player profile capacity reached')
                        continue
                    record = dict(player_id=pid, uuid=uuid, xuid=xuid, name=name, names=[], first_seen=now, revision=0)
                    bucket = tx.get(keys[2], [])
                    bucket.append(pid)
                    tx.set(keys[2], bucket).set('count', count + 1)
                if name and (not record['names'] or record['names'][-1]['name'] != name):
                    record['names'].append({'name': name, 'observed_at': now})
                    record['names'] = record['names'][-ctx.config.max_names:]
                record.update(uuid=uuid or record.get('uuid', ''), xuid=xuid or record.get('xuid', ''), name=name,
                              last_seen=now, source='host.players', generation=ctx.operations.session,
                              revision=record['revision'] + 1)
                tx.set(key, record)
                for idx in indexes:
                    values = tx.get(idx, [])
                    if pid not in values:
                        values.append(pid)
                    if len(values) > 256:
                        raise ServiceRejected('busy')
                    tx.set(idx, values)
                await self.save(tx)

    async def record(self, ctx, pid):
        pid = text(pid)
        value = await ctx.storage.get('player:' + digest(pid))
        if value is None:
            raise ServiceRejected('not_found')
        return value

    @service('resolve', with_context=True)
    async def resolve(self, ctx, args, call):
        self.owner(call)
        if args.get('player_id'):
            return {'status': 'resolved', 'player': await self.record(ctx, args['player_id'])}
        criteria = {key: text(args[key]) for key in ('uuid', 'xuid', 'name') if key in args}
        if not criteria:
            raise ServiceRejected('invalid_argument')
        matches = None
        for kind, value in criteria.items():
            ids = set(await ctx.storage.get(index_key(kind, value), []))
            matches = ids if matches is None else matches & ids
        if not matches:
            raise ServiceRejected('not_found')
        if len(matches) != 1:
            raise ServiceRejected('conflict')
        return {'status': 'resolved', 'player': await self.record(ctx, next(iter(matches)))}

    @service('get', with_context=True)
    async def get(self, ctx, args, call):
        owner = self.owner(call)
        player = await self.record(ctx, args.get('player_id'))
        fields = await ctx.storage.get('fields:' + digest(owner + '\0' + player['player_id']), {'revision': 0, 'fields': {}})
        return {'status': 'resolved', 'player': player, 'fields': fields['fields'], 'fields_revision': fields['revision']}

    @service('history', with_context=True)
    async def history(self, ctx, args, call):
        self.owner(call)
        record = await self.record(ctx, args.get('player_id'))
        return {'player_id': record['player_id'], 'names': record['names'], 'first_seen': record['first_seen'],
                'last_seen': record['last_seen'], 'revision': record['revision'], 'retained_name_limit': ctx.config.max_names}

    @service('patch', with_context=True)
    async def patch(self, ctx, args, call):
        owner = self.owner(call)
        pid = text(args.get('player_id'))
        request = text(args.get('request_id'), 128)
        expected = args.get('expected_revision')
        fields = args.get('fields')
        if type(expected) is not int or expected < 0 or not isinstance(fields, dict):
            raise ServiceRejected('invalid_argument')
        if any(not isinstance(k, str) or not k or len(k.encode()) > 128 for k in fields):
            raise ServiceRejected('invalid_argument')
        try:
            encoded = json.dumps(fields, ensure_ascii=False, allow_nan=False, sort_keys=True)
        except (ValueError, TypeError):
            raise ServiceRejected('invalid_argument') from None
        if len(encoded.encode()) > ctx.config.max_field_bytes:
            raise ServiceRejected('invalid_argument')
        fingerprint = digest(json.dumps(args, sort_keys=True, ensure_ascii=False, allow_nan=False))
        receipt_key = 'request:' + digest(owner + '\0' + request)
        field_key = 'fields:' + digest(owner + '\0' + pid)
        async with self.lock:
            await self.writable(ctx)
            await self.record(ctx, pid)
            tx = await ctx.storage.transaction(keys=[field_key, receipt_key])
            receipt = tx.get(receipt_key)
            if receipt:
                if receipt['fingerprint'] != fingerprint:
                    raise ServiceRejected('conflict')
                return receipt['result']
            row = tx.get(field_key, {'revision': 0, 'fields': {}})
            if row['revision'] != expected:
                raise ServiceRejected('conflict')
            for key, value in fields.items():
                if value is None:
                    row['fields'].pop(key, None)
                else:
                    row['fields'][key] = value
            if len(json.dumps(row['fields'], ensure_ascii=False).encode()) > ctx.config.max_field_bytes:
                raise ServiceRejected('invalid_argument')
            row['revision'] += 1
            row['updated_at'] = time.time()
            result = dict(status='saved', player_id=pid, request_id=request, revision=row['revision'], fields=row['fields'])
            tx.set(field_key, row).set(receipt_key, {'fingerprint': fingerprint, 'result': result})
            await self.save(tx)
            return result

    async def page(self, ctx, args, call):
        owner = self.owner(call)
        limit = args.get('limit', 50)
        cursor = args.get('cursor', '0:0')
        if type(limit) is not int or not 1 <= limit <= 100 or not isinstance(cursor, str):
            raise ServiceRejected('invalid_argument')
        try:
            bucket, offset = map(int, cursor.split(':'))
            if not 0 <= bucket <= 256 or offset < 0:
                raise ValueError()
        except ValueError:
            raise ServiceRejected('invalid_argument') from None
        items = []
        response_bytes = 1024
        while bucket < 256 and len(items) < limit:
            ids = await ctx.storage.get('bucket:' + format(bucket, '02x'), [])
            while offset < len(ids) and len(items) < limit:
                item = await self.get(ctx, {'player_id': ids[offset]}, call)
                item_bytes = len(json.dumps(item, ensure_ascii=True).encode()) + 2
                if response_bytes + item_bytes > 512 * 1024:
                    return {'items': items, 'next_cursor': f'{bucket}:{offset}',
                            'namespace': owner, 'consistency': 'live_paginated'}
                items.append(item)
                response_bytes += item_bytes
                offset += 1
            if offset >= len(ids):
                bucket, offset = bucket + 1, 0
        return {'items': items, 'next_cursor': f'{bucket}:{offset}' if bucket < 256 else None,
                'namespace': owner, 'consistency': 'live_paginated'}

    @service('query', with_context=True)
    async def query(self, ctx, args, call):
        return await self.page(ctx, args, call)

    @service('export', with_context=True)
    async def export(self, ctx, args, call):
        return await self.page(ctx, args, call)

    async def on_maintenance(self, ctx, request):
        async with self.lock:
            token = request['token']
            if request['operation'] == 'seal':
                if not await self.reconcile(ctx) or self.sealed not in (None, token):
                    return {'status': 'busy', 'token': token}
                self.sealed = token
                return {'status': 'sealed', 'token': token}
            if request['operation'] == 'release' and self.sealed in (None, token):
                self.sealed = None
                return {'status': 'released', 'token': token}
            return {'status': 'busy', 'token': token}


if __name__ == '__main__':
    Players().run()
