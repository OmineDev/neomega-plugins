"""Small service adapted from neomega-agent's native-gameplay example."""
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import uuid

from neomega_runtime import Event, Plugin, PluginContext, PlayerNotFound


def deadline():
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


@dataclass(frozen=True)
class Settings:
    welcome_text: str = '欢迎 {name}！输入 !help 查看帮助。'
    help_text: str = '服务帮助：输入 !help 查看这条说明。有问题请联系管理员。'
    announcement_text: str = '请友善交流，输入 !help 查看帮助。'
    announcement_interval: float = 300.0

    def __post_init__(self):
        if not math.isfinite(self.announcement_interval) or self.announcement_interval < 10:
            raise ValueError('announcement_interval must be at least 10 seconds')
        for text in (self.welcome_text, self.help_text, self.announcement_text):
            if not text.strip() or len(text.encode('utf-8')) > 200:
                raise ValueError('message must contain 1..200 UTF-8 bytes')


class StarterService(Plugin):
    config_type = Settings

    def __init__(self):
        self._transactions = asyncio.Lock()

    async def report(self, ctx, receipt):
        for operation_id in receipt['operation_ids']:
            result = await ctx.operations.wait(operation_id, timeout=35)
            ctx.log.info('notification state=%s', result['state'],
                         extra={'operation_id': operation_id})
            if result['state'] != 'succeeded':
                # Do not retry uncertain or failed world actions with new keys.
                raise RuntimeError('notification did not succeed; inspect operation ' + operation_id)

    async def reply(self, ctx, event, name, text):
        key = 'starter_' + event.payload['delivery_token']
        plan = ctx.notifications.prepare(name, text, idempotency_key=key, deadline=deadline())
        tx = await event.transaction(commit_id=key)
        for part in plan.parts:
            tx.action(part.intent)
        # Persist notification intents and event ACK together before waiting.
        await self.report(ctx, await tx.save())

    async def announce(self, ctx):
        async with self._transactions:
            key = uuid.uuid4().hex
            text = json.dumps({'rawtext': [{'text': ctx.config.announcement_text}]}, ensure_ascii=False)
            tx = await ctx.storage.transaction(commit_id=key)
            tx.action(ctx.commands.prepare('tellraw @a ' + text,
                      idempotency_key=key, deadline=deadline()))
            await self.report(ctx, await tx.save())

    async def on_start(self, ctx: PluginContext):
        @ctx.chat.command('help')
        async def help_command(command):
            await self.reply(ctx, command.event, command.player['name'], ctx.config.help_text)

        ctx.every(ctx.config.announcement_interval, lambda: self.announce(ctx), name='announcement')
        # Worker cancels and joins this managed task before on_stop; no scheduler to close.

    async def on_event(self, ctx: PluginContext, event: Event):
        # Timers and event callbacks share one storage revision. Serialize commits.
        async with self._transactions:
            await self.handle_event(ctx, event)

    async def handle_event(self, ctx, event):
        if event.kind == 'player.list_added':
            body = event.payload['payload']
            try:
                player = await ctx.resolve_player(name=body['name'], xuid=body.get('xuid') or None)
            except PlayerNotFound:
                # A durable join may arrive after the player has left.
                await event.ack()
                return
            await self.reply(ctx, event, player['name'],
                             ctx.config.welcome_text.replace('{name}', player['name']))
        elif event.kind == 'chat.received':
            try:
                handled = await ctx.chat.dispatch(event)
            except (ValueError, PlayerNotFound):
                # Bad command syntax or a sender who already left: no reply.
                ctx.log.info('ignored invalid or stale chat command')
                await event.ack()
                return
            if not handled:
                await event.ack()
        else:
            await event.ack()


if __name__ == '__main__':
    StarterService().run()
