"""Durable virtual scores with explicit receipt-backed game synchronization."""
from datetime import datetime, timedelta, timezone
from neomega_runtime import service
from support import Provider, ident, key, reject

class Scores(Provider):
    @service('objective', with_context=True)
    async def objective(self, ctx, args, call):
        async with self.lock:
            ns = self.namespace(ctx, args, call, True)
            oid = ident(args.get('objective'))
            k = key(ns, oid)
            b = await self.begin(ctx, args, call, 'objective', k)
            if b.receipt: return self.previous(b)
            row = b.state.get(k, {'revision': 0, 'active': False, 'scores': {}})
            self.cas(args, row)
            action = args.get('action')
            if action == 'create':
                if row['active']: reject('objective_exists')
                row.update(active=True, name=args.get('name', oid), scores={})
            elif action == 'rename':
                if not row['active']: reject('objective_not_found')
                row['name'] = args.get('name')
            elif action == 'remove':
                if not row['active']: reject('objective_not_found')
                row.update(active=False, scores={})
            else: reject('invalid_action')
            if not isinstance(row.get('name'), str) or not 1 <= len(row['name']) <= 128: reject('invalid_name')
            return await self.commit(b, row, k, args, call, 'objective')

    @service('mutate', with_context=True)
    async def mutate(self, ctx, args, call):
        async with self.lock:
            ns = self.namespace(ctx, args, call, True)
            k = key(ns, ident(args.get('objective')))
            b = await self.begin(ctx, args, call, 'mutate', k)
            if b.receipt: return self.previous(b)
            row = b.state.get(k)
            if row is None or not row['active']: reject('objective_not_found')
            self.cas(args, row)
            player = await self.player(ctx, args.get('player_id'))
            pid = player['player_id']
            action = args.get('action')
            if action == 'reset': row['scores'].pop(pid, None)
            else:
                amount = args.get('value')
                if type(amount) is not int: reject('invalid_value')
                old = row['scores'].get(pid, 0)
                if action == 'set': value = amount
                elif action == 'add': value = old + amount
                elif action == 'sub': value = old - amount
                else: reject('invalid_action')
                if not -(1 << 31) <= value < (1 << 31): reject('score_overflow')
                row['scores'][pid] = value
                if len(row['scores']) > 512: reject('objective_capacity')
            return await self.commit(b, row, k, args, call, 'mutate', {'player_id': pid, 'value': row['scores'].get(pid)})

    @service('query', with_context=True)
    async def query(self, ctx, args, call):
        async with self.lock:
            ns = self.namespace(ctx, args, call)
            row = await ctx.storage.get(key(ns, ident(args.get('objective'))))
            if row is None or not row['active']: return {'found': False, 'revision': row['revision'] if row else 0}
            result = dict(found=True, revision=row['revision'], name=row['name'], count=len(row['scores']))
            if args.get('player_id'):
                p = await self.player(ctx, args['player_id'])
                result.update(player_id=p['player_id'], value=row['scores'].get(p['player_id']))
            return result

    @service('rank', with_context=True)
    async def rank(self, ctx, args, call):
        async with self.lock:
            ns = self.namespace(ctx, args, call)
            row = await ctx.storage.get(key(ns, ident(args.get('objective'))))
            if row is None or not row['active']: reject('objective_not_found')
            if 'expected_revision' in args: self.cas(args, row)
            offset, limit = args.get('offset', 0), args.get('limit', 50)
            if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100: reject('invalid_page')
            ordered = sorted(row['scores'].items(), key=lambda x: (-x[1], x[0]))
            return {'revision': row['revision'], 'total': len(ordered), 'items': [{'rank': i + offset + 1, 'player_id': p, 'value': v} for i, (p,v) in enumerate(ordered[offset:offset+limit])]}

    @service('sync', with_context=True)
    async def sync(self, ctx, args, call):
        async with self.lock:
            ns = self.namespace(ctx, args, call, True)
            if args.get('action') == 'receipt':
                bid = key(call.installation_id, 'sync', ident(args.get('request_id'))).replace(':', '_')
                receipt = await ctx.business.get(bid)
                if receipt is None: return {'found': False}
                operations = [await ctx.operations.get(oid) for oid in receipt['operation_ids']]
                return dict(found=True, result=receipt['result'], operations=operations)
            oid = ident(args.get('objective'))
            binding = next((x for x in ctx.config.sync_bindings if x.namespace == ns and x.objective == oid), None)
            if binding is None: reject('sync_not_authorized')
            k = key(ns, oid)
            b = await self.begin(ctx, args, call, 'sync', k)
            if b.receipt: return self.previous(b)
            row = b.state.get(k)
            if row is None or not row['active']: reject('objective_not_found')
            self.cas(args, row)
            p = await self.player(ctx, args.get('player_id'))
            roster = await ctx.players()
            identities = {field: p[field] for field in ('uuid', 'xuid') if p.get(field)}
            if not identities: reject('player_identity_unknown')
            matches = [candidate for candidate in roster
                       if any(candidate.get(field) == value for field, value in identities.items())]
            if not matches: reject('player_offline')
            if len(matches) != 1: reject('player_identity_conflict')
            current = matches[0]
            if any(current.get(field) and current[field] != value
                   for field, value in identities.items()):
                reject('player_identity_conflict')
            name = current.get('name')
            if not name: reject('player_name_unknown')
            if sum(candidate.get('name') == name for candidate in roster) != 1:
                reject('player_identity_conflict')
            pid = p['player_id']
            deadline = (datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat()
            params = dict(idempotency_key=b.business_id, deadline=deadline)
            if pid in row['scores']:
                intent = ctx.scoreboard.prepare_set(name, binding.game_objective, row['scores'][pid], **params)
            else:
                intent = ctx.scoreboard.prepare_reset(name, binding.game_objective, **params)
            b.state.action(intent)
            return await self.commit(b, row, k, args, call, 'sync', {'player_id': pid, 'sync_state': 'queued'})

if __name__ == '__main__': Scores().run()
