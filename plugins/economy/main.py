"""Installation-owned atomic, integer economy. No world-side money mutation."""
import asyncio
import hashlib
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from neomega_runtime.storage import CommitUncertain
from neomega_runtime.managed import IPCRejected
from neomega_runtime import Plugin
from neomega_runtime.services import service, ServiceRejected


@dataclass(frozen=True)
class Settings:
    currencies: list[str] = field(default_factory=lambda: ['coin'])
    allowed_callers: list[str] = field(default_factory=list)
    mint_callers: list[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.currencies or any(not isinstance(x, str) or not x or len(x) > 64 for x in self.currencies):
            raise ValueError('currencies require nonempty names up to 64 characters')


def key(*parts):
    return 'economy:' + hashlib.sha256(json.dumps(parts, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()).hexdigest()


def text(args, name):
    value = args.get(name)
    if not isinstance(value, str) or not value or len(value.encode()) > 256:
        raise ServiceRejected('invalid_' + name)
    return value


class Economy(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.sealed = None
        self.pending = None
        self.pending_path = None

    async def on_start(self, ctx):
        self.pending_path = ctx.data_dir / 'economy-pending.json'
        if self.pending_path.exists():
            self.pending = json.loads(self.pending_path.read_text())['commit_id']
        async with self.lock:
            if not await self.reconcile(ctx):
                return  # Retain read access; writes and maintenance seal stay busy.
            tx = await ctx.storage.transaction(keys=['economy:schema_version'])
            version = tx.get('economy:schema_version')
            if version is None:
                tx.set('economy:schema_version', 1)
                await self.save(ctx, tx)
            elif type(version) is not int or version != 1:
                raise RuntimeError('unsupported economy schema_version; migration required')

    def clear_pending(self):
        if self.pending_path is not None:
            self.pending_path.unlink(missing_ok=True)
        self.pending = None

    async def reconcile(self, ctx):
        if self.pending is None:
            return True
        if await ctx.storage.receipt(self.pending) is None:
            return False
        self.clear_pending()
        return True

    async def save(self, ctx, tx):
        # Write the uncertain barrier before crossing the IPC boundary. The
        # marker survives a worker crash; absence of receipt is never failure.
        tx.prepare()
        self.pending = tx.commit_id
        if self.pending_path is not None:
            temporary = self.pending_path.with_suffix('.tmp')
            with temporary.open('w') as stream:
                json.dump({'commit_id': self.pending}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(self.pending_path)
            fd = os.open(self.pending_path.parent, os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        try:
            await tx.save()
        except IPCRejected:
            self.clear_pending()
            raise
        except (CommitUncertain, asyncio.CancelledError):
            raise
        self.clear_pending()

    @staticmethod
    def compare_revision(args, field, row, required=True):
        if field not in args and not required:
            return
        expected = args.get(field)
        if type(expected) is not int or expected < 0:
            raise ServiceRejected('invalid_' + field)
        if expected != row['revision']:
            raise ServiceRejected('revision_conflict')

    async def on_maintenance(self, ctx, request):
        async with self.lock:
            token = request['token']
            if not await self.reconcile(ctx) or self.sealed not in (None, token):
                return {'status': 'busy', 'token': token}
            sealing = request['operation'] == 'seal'
            self.sealed = token if sealing else None
            return {'status': 'sealed' if sealing else 'released', 'token': token}

    def authorize(self, ctx, caller, mint=False):
        if caller is None:
            raise ServiceRejected('trusted_caller_required')
        if caller.installation_id not in ctx.config.allowed_callers:
            raise ServiceRejected('caller_denied')
        if mint and caller.installation_id not in ctx.config.mint_callers:
            raise ServiceRejected('mint_denied')
        if self.sealed is not None:
            raise ServiceRejected('maintenance_sealed')
        return caller.installation_id

    def currency(self, ctx, args):
        currency = text(args, 'currency')
        if currency not in ctx.config.currencies:
            raise ServiceRejected('unknown_currency')
        return currency

    @service('balance', with_context=True)
    async def balance(self, ctx, args, caller):
        async with self.lock:
            self.authorize(ctx, caller)
            currency, account = self.currency(ctx, args), text(args, 'account')
            row = await ctx.storage.get(key('account', currency, account), {'balance': 0, 'held': 0, 'revision': 0})
            return dict(row, available=row['balance'] - row['held'], account=account, currency=currency)

    async def mutate(self, ctx, args, caller, operation):
        async with self.lock:
            owner = self.authorize(ctx, caller, operation == 'post')
            if not await self.reconcile(ctx):
                raise ServiceRejected('commit_unresolved')
            currency, account = self.currency(ctx, args), text(args, 'account')
            request_id = text(args, 'request_id')
            request_key = key('request', owner, request_id)
            fingerprint = key(operation, args)
            account_key = key('account', currency, account)
            keys = [request_key, account_key, 'economy:sequence', 'economy:schema_version']
            target_key = None
            if operation == 'transfer':
                target = text(args, 'target')
                if target == account:
                    raise ServiceRejected('same_account')
                target_key = key('account', currency, target)
                keys.append(target_key)
            hold_key = key('hold', owner, currency, account, text(args, 'hold_id')) if operation in ('hold', 'release') else None
            if hold_key:
                keys.append(hold_key)
            tx = await ctx.storage.transaction(keys=keys)
            if tx.get('economy:schema_version') != 1:
                raise ServiceRejected('schema_migration_required')
            previous = tx.get(request_key)
            if previous:
                if previous['fingerprint'] != fingerprint:
                    raise ServiceRejected('idempotency_conflict')
                return previous['result']
            row = tx.get(account_key, {'balance': 0, 'held': 0, 'revision': 0})
            self.compare_revision(args, 'expected_revision', row)
            before = dict(row)
            amount = args.get('amount')
            if operation != 'release' and (type(amount) is not int or amount == 0 or abs(amount) > 2**53 - 1):
                raise ServiceRejected('invalid_amount')
            if operation in ('transfer', 'hold') and amount < 0:
                raise ServiceRejected('invalid_amount')
            target_row = None
            if operation == 'post':
                row['balance'] += amount
            elif operation == 'transfer':
                row['balance'] -= amount
                target_row = tx.get(target_key, {'balance': 0, 'held': 0, 'revision': 0})
                self.compare_revision(args, 'target_expected_revision', target_row, required=False)
                target_row['balance'] += amount
                target_row['revision'] += 1
                tx.set(target_key, target_row)
            elif operation == 'hold':
                if tx.get(hold_key) is not None:
                    raise ServiceRejected('hold_exists')
                row['held'] += amount
                tx.set(hold_key, {'amount': amount, 'state': 'held'})
            else:
                hold = tx.get(hold_key)
                if not hold or hold['state'] != 'held':
                    raise ServiceRejected('hold_not_active')
                amount = hold['amount']
                row['held'] -= amount
                capture = args.get('capture', False)
                if type(capture) is not bool:
                    raise ServiceRejected('invalid_capture')
                if capture:
                    row['balance'] -= amount
                tx.set(hold_key, dict(hold, state='captured' if capture else 'released'))
            if row['balance'] < row['held'] or row['held'] < 0:
                raise ServiceRejected('insufficient_funds')
            if row['balance'] > 2**53 - 1 or (target_row and target_row['balance'] > 2**53 - 1):
                raise ServiceRejected('balance_limit')
            row['revision'] += 1
            sequence = tx.get('economy:sequence', 0) + 1
            result = {'sequence': sequence, 'account': account, 'currency': currency, **row, 'available': row['balance'] - row['held']}
            entry = {'sequence': sequence, 'caller': owner, 'request_id': request_id, 'operation': operation,
                     'currency': currency, 'account': account, 'amount': amount, 'before': before, 'after': row,
                     'arguments': args, 'time': datetime.now(timezone.utc).isoformat()}
            if target_row is not None:
                entry['target_after'] = target_row
            tx.set(account_key, row).set('economy:sequence', sequence).set(key('entry', sequence), entry)
            tx.set(request_key, {'fingerprint': fingerprint, 'result': result})
            await self.save(ctx, tx)
            return result

    @service('post', with_context=True)
    async def post(self, ctx, args, caller):
        return await self.mutate(ctx, args, caller, 'post')

    @service('transfer', with_context=True)
    async def transfer(self, ctx, args, caller):
        return await self.mutate(ctx, args, caller, 'transfer')

    @service('hold', with_context=True)
    async def hold(self, ctx, args, caller):
        return await self.mutate(ctx, args, caller, 'hold')

    @service('release', with_context=True)
    async def release(self, ctx, args, caller):
        return await self.mutate(ctx, args, caller, 'release')

    @service('receipt', with_context=True)
    async def receipt(self, ctx, args, caller):
        async with self.lock:
            owner = self.authorize(ctx, caller)
            row = await ctx.storage.get(key('request', owner, text(args, 'request_id')))
            return {'status': 'committed' if row else 'unknown', 'result': row['result'] if row else None}

    @service('ledger', with_context=True)
    async def ledger(self, ctx, args, caller):
        async with self.lock:
            owner = self.authorize(ctx, caller)
            after, limit = args.get('after', 0), args.get('limit', 20)
            if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 20:
                raise ServiceRejected('invalid_pagination')
            last = await ctx.storage.get('economy:sequence', 0)
            end = min(after + limit, last)
            if end <= after:
                return {'entries': [], 'cursor': after, 'has_more': False}
            keys = [key('entry', i) for i in range(after + 1, end + 1)]
            tx = await ctx.storage.transaction(keys=keys)
            entries = [tx.get(k) for k in keys]
            return {'entries': [e for e in entries if e['caller'] == owner], 'cursor': end, 'has_more': end < last}


if __name__ == '__main__':
    Economy().run()
