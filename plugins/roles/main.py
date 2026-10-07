"""Scoped business roles. Does not confer Host grants or game operator status."""
from datetime import datetime, timezone
from neomega_runtime import service
from support import Provider, ident, key, reject

class Roles(Provider):
    @service('define', with_context=True)
    async def define(self, ctx, args, call):
        async with self.lock:
            ns = self.namespace(ctx, args, call, True)
            k = key(ns, 'roles')
            b = await self.begin(ctx, args, call, 'define', k)
            if b.receipt: return self.previous(b)
            row = b.state.get(k, {'revision': 0, 'roles': {}, 'members': {}})
            self.cas(args, row)
            rid = ident(args.get('role'))
            if args.get('action', 'set') == 'remove':
                row['roles'].pop(rid, None)
                row['members'] = {k:v for k,v in row['members'].items() if v['role'] != rid}
            elif args.get('action', 'set') == 'set':
                caps = args.get('capabilities')
                if not isinstance(caps, list) or len(caps) > 128: reject('invalid_capabilities')
                caps = sorted(set(ident(x) for x in caps))
                row['roles'][rid] = {'capabilities': caps}
                if len(row['roles']) > 128: reject('role_capacity')
            else: reject('invalid_action')
            return await self.commit(b, row, k, args, call, 'define')

    @service('assign', with_context=True)
    async def assign(self, ctx, args, call):
        return await self.membership(ctx, args, call, False)

    @service('revoke', with_context=True)
    async def revoke(self, ctx, args, call):
        return await self.membership(ctx, args, call, True)

    async def membership(self, ctx, args, call, remove):
        async with self.lock:
            ns = self.namespace(ctx, args, call, True)
            k = key(ns, 'roles')
            method = 'revoke' if remove else 'assign'
            b = await self.begin(ctx, args, call, method, k)
            if b.receipt: return self.previous(b)
            row = b.state.get(k, {'revision': 0, 'roles': {}, 'members': {}})
            self.cas(args, row)
            rid = ident(args.get('role'))
            if rid not in row['roles']: reject('role_not_found')
            pid = (await self.player(ctx, args.get('player_id')))['player_id']
            scope = ident(args.get('scope', 'global'))
            membership = key(pid, rid, scope)
            if remove: row['members'].pop(membership, None)
            else:
                expiry = args.get('expires_at')
                if expiry is not None:
                    try:
                        end = datetime.fromisoformat(expiry.replace('Z', '+00:00'))
                        if end.tzinfo is None or end <= datetime.now(timezone.utc): reject('invalid_expiry')
                        expiry = end.astimezone(timezone.utc).isoformat()
                    except (ValueError, TypeError, AttributeError): reject('invalid_expiry')
                row['members'][membership] = dict(player_id=pid, role=rid, scope=scope, expires_at=expiry)
                if len(row['members']) > 512: reject('membership_capacity')
            return await self.commit(b, row, k, args, call, method, {'player_id': pid})

    @staticmethod
    def active(member):
        return member['expires_at'] is None or datetime.fromisoformat(member['expires_at']) > datetime.now(timezone.utc)

    @service('check', with_context=True)
    async def check(self, ctx, args, call):
        async with self.lock:
            ns = self.namespace(ctx, args, call)
            row = await ctx.storage.get(key(ns, 'roles'), {'revision': 0, 'roles': {}, 'members': {}})
            pid = (await self.player(ctx, args.get('player_id')))['player_id']
            capability, scope = ident(args.get('capability')), ident(args.get('scope', 'global'))
            matches = [m['role'] for m in row['members'].values() if m['player_id'] == pid and m['scope'] in ('global', scope) and self.active(m) and capability in row['roles'][m['role']]['capabilities']]
            return {'allowed': bool(matches), 'roles': sorted(set(matches)), 'revision': row['revision'], 'scope': scope}

    @service('list', with_context=True)
    async def list_roles(self, ctx, args, call):
        async with self.lock:
            ns = self.namespace(ctx, args, call)
            row = await ctx.storage.get(key(ns, 'roles'), {'revision': 0, 'roles': {}, 'members': {}})
            if 'expected_revision' in args: self.cas(args, row)
            offset, limit = args.get('offset', 0), args.get('limit', 50)
            if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 100: reject('invalid_page')
            if args.get('player_id'):
                pid = (await self.player(ctx, args['player_id']))['player_id']
                items = [dict(m, active=self.active(m)) for _, m in sorted(row['members'].items()) if m['player_id'] == pid]
            else: items = [dict(role=r, **v) for r,v in sorted(row['roles'].items())]
            return {'revision': row['revision'], 'total': len(items), 'items': items[offset:offset+limit]}

if __name__ == '__main__': Roles().run()
