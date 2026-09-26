"""Welcome and exact Chinese help command, using durable native events."""
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256

from neomega_runtime import Event, Plugin, PluginContext, PlayerNotFound

HELP = "服务器帮助\n请友善交流，遵守服务器规则。\n遇到问题请联系管理员。"
WELCOME = "欢迎来到服务器！输入 !帮助 查看服务器帮助。"


@dataclass(frozen=True)
class Settings:
    pass


class WelcomeHelp(Plugin):
    config_type = Settings

    def __init__(self):
        self._lock = asyncio.Lock()

    async def on_event(self, ctx: PluginContext, event: Event):
        async with self._lock:
            body = event.payload['payload']
            if event.kind == 'player.list_added':
                name, text = body.get('name'), WELCOME
            elif event.kind == 'chat.received' and body.get('message') == '!帮助':
                name, text = body.get('player'), HELP
            else:
                await event.ack()
                return
            # Stable across redelivery tokens and worker restarts.
            key = 'welcome_' + sha256(event.payload['event_id'].encode()).hexdigest()
            tx = await event.transaction()
            if tx.get(key, False):
                await tx.save()
                return
            try:
                player = await ctx.resolve_player(name=name, xuid=body.get('xuid') or None)
            except PlayerNotFound:
                await event.ack()
                return
            plan = ctx.notifications.prepare(
                player['name'], text, idempotency_key=key,
                deadline=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat())
            for part in plan.parts:
                tx.action(part.intent)
            tx.set(key, True)
            # Marker, notification intents and ACK are committed together.
            receipt = await tx.save()
            for operation_id in receipt['operation_ids']:
                result = await ctx.operations.wait(operation_id, timeout=35)
                ctx.log.info('notification state=%s operation_id=%s', result['state'], operation_id)
                # Unknown/failure remains inspectable; never submit another send.


if __name__ == '__main__':
    WelcomeHelp().run()
