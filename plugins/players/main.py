"""玩家档案：记录名字、uuid、xuid 的对应关系，让其他插件按名字找到不在线的玩家。

玩家进服时只做一次存储提交：档案、名字索引、xuid 索引和事件确认一起写入。
提交结果不明时异常上抛、插件重启，未确认的进服事件会重新投递并照常处理。

存储键：
- p:<玩家 ID>：当前名字、uuid、xuid、最近 5 个曾用名、首次和最近进服时间
- n:<名字.casefold()>：玩家 ID；曾用名也保留索引，挤出最近 5 个后删除
- x:<xuid>：玩家 ID，只为有 uuid 的玩家写入
"""
from dataclasses import dataclass
from datetime import datetime, timezone

from neomega_runtime import (AmbiguousPlayer, Event, Plugin, PluginContext, PlayerNotFound,
                             ServiceRejected, service)

MAX_NAMES = 5


@dataclass(frozen=True)
class Settings:
    pass


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def name_key(name: str) -> str:
    return 'n:' + name.casefold()


class Players(Plugin):
    config_type = Settings

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    async def on_event(self, ctx: PluginContext, event: Event):
        if event.kind == 'player.list_added':
            joined = event.payload.get('payload') or {}
            name = joined.get('name') if isinstance(joined.get('name'), str) else ''
            uuid = joined.get('uuid') if isinstance(joined.get('uuid'), str) else ''
            xuid = joined.get('xuid') if isinstance(joined.get('xuid'), str) else ''
            if name or uuid or xuid:
                try:
                    kwargs = {'name': name or None, 'xuid': xuid or None}
                    if not name and not xuid:
                        kwargs['uuid'] = uuid
                    player = await ctx.resolve_player(**kwargs)
                except (PlayerNotFound, AmbiguousPlayer, ValueError):
                    pass  # 玩家已经离开：只确认事件。
                else:
                    await self.record(ctx, event, player)
                    return
        await event.ack()

    async def record(self, ctx, event, player):
        pid, name, xuid = player_id(player), player['name'], player['xuid']
        # 先读一次档案，才知道这次要读写哪些曾用名的索引；只有本插件写这些键，事件串行处理。
        known = await ctx.storage.get('p:' + pid) or {'names': []}
        keys = {'p:' + pid, name_key(name)} | {name_key(old) for old in known['names'] + [known.get('name', name)]}
        if player['uuid'] and xuid:
            keys.add('x:' + xuid)
        tx = await event.transaction(keys=sorted(keys))

        now = self.now()
        record = tx.get('p:' + pid)
        if record is None:
            record = {'name': name, 'uuid': player['uuid'], 'xuid': xuid, 'names': [],
                      'first_seen': now, 'last_seen': now}
        elif record['name'] == name and record['xuid'] == xuid:
            record['last_seen'] = now
        else:
            if record['name'] != name:
                names = [old for old in record['names'] if old not in (name, record['name'])]
                names.append(record['name'])
                kept = {old.casefold() for old in names[-MAX_NAMES:]} | {name.casefold()}
                for old in names[:-MAX_NAMES]:
                    if old.casefold() not in kept and tx.get(name_key(old)) == pid:
                        tx.delete(name_key(old))
                record['names'] = names[-MAX_NAMES:]
            record.update(name=name, xuid=xuid, last_seen=now)
        tx.set('p:' + pid, record)

        # 当前名字总是指向这位玩家：别人用过同一个名字时覆盖。
        if tx.get(name_key(name)) != pid:
            tx.set(name_key(name), pid)
        if player['uuid'] and xuid and tx.get('x:' + xuid) != pid:
            tx.set('x:' + xuid, pid)
        await tx.save()

    @service('resolve', major=1)
    async def resolve(self, ctx: PluginContext, arguments):
        if not isinstance(arguments, dict) or len(arguments) != 1:
            raise ServiceRejected('invalid_argument')
        (field, value), = arguments.items()
        if field not in ('name', 'uuid', 'xuid') or not isinstance(value, str) or not value:
            raise ServiceRejected('invalid_argument')
        if field == 'uuid':
            pid = value
            lookup_key = 'p:' + pid
        elif field == 'name':
            pid = None
            lookup_key = name_key(value)
        else:
            pid = None
            lookup_key = 'x:' + value
        if len(lookup_key.encode('utf-8')) > 256 or (field == 'xuid' and len(('p:xuid-' + value).encode('utf-8')) > 256):
            raise ServiceRejected('invalid_argument')
        if pid is None:
            pid = await ctx.storage.get(lookup_key)
            # 没有 uuid 的玩家不写 xuid 索引，他们的 ID 本身就带着 xuid。
            if pid is None and field == 'xuid':
                pid = 'xuid-' + value
        if not isinstance(pid, str) or not pid or len(('p:' + pid).encode('utf-8')) > 256:
            raise ServiceRejected('not_found')
        record = await ctx.storage.get('p:' + pid)
        if record is None or (field != 'name' and record.get(field) != value):
            raise ServiceRejected('not_found')
        return {'id': pid, 'name': record['name'], 'uuid': record['uuid'], 'xuid': record['xuid'],
                'names': record['names'], 'last_seen': record['last_seen']}


if __name__ == '__main__':
    Players().run()
