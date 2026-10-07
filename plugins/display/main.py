"""Installable Neomega display capability; no separate scheduler or database."""
import asyncio
from dataclasses import dataclass, field

from neomega_runtime import Plugin, service
from display_provider import profile
from display_provider.provider import Provider
from display_provider.channels import Channels


@dataclass
class Settings:
    max_objects: int = field(default=8, metadata={'minimum': 1, 'maximum': 8})
    max_owner_objects: int = field(default=4, metadata={'minimum': 1, 'maximum': 4})
    max_ttl_seconds: int = field(default=60, metadata={'minimum': 1, 'maximum': 60})
    tick_seconds: float = field(default=0.25, metadata={'minimum': 0.1, 'maximum': 5.0})


class DisplayPlugin(Plugin):
    config_type = Settings

    async def on_start(self, ctx):
        self.provider = Provider(ctx, profile, limits=vars(ctx.config))
        await self.provider.recover()
        self.channels = Channels(ctx)
        self.channels.lock = self.provider.lock
        await self.channels.recover()
        ctx.every(ctx.config.tick_seconds, self.channels.tick, name='display-channels')
        ctx.every(ctx.config.tick_seconds, self.provider.tick, name='display-reconcile')

    async def on_stop(self, ctx):
        if not hasattr(self, 'provider'):
            return
        await self.provider.recover()
        await self.channels.recover()
        # Best effort only. Unknown/session-changed remain durable and frozen.
        for _ in range(20):
            await self.provider.tick()
            await self.channels.tick()
            await asyncio.sleep(0.1)

    @service('describe', with_context=True)
    async def describe(self, ctx, args, call_context):
        return await self.provider.describe()

    @service('get', with_context=True)
    async def get(self, ctx, args, call_context):
        return await self.provider.get(args, call_context)

    @service('create', with_context=True)
    async def create(self, ctx, args, call_context):
        return await self.provider.mutate('create', args, call_context)

    @service('update', with_context=True)
    async def update(self, ctx, args, call_context):
        return await self.provider.mutate('update', args, call_context)

    @service('renew', with_context=True)
    async def renew(self, ctx, args, call_context):
        return await self.provider.mutate('renew', args, call_context)

    @service('close', with_context=True)
    async def close(self, ctx, args, call_context):
        return await self.provider.mutate('close', args, call_context)

    @service('channel_acquire', with_context=True)
    async def channel_acquire(self, ctx, args, call_context):
        return await self.channels.call('channel_acquire', args, call_context)

    @service('channel_renew', with_context=True)
    async def channel_renew(self, ctx, args, call_context):
        return await self.channels.call('channel_renew', args, call_context)

    @service('channel_release', with_context=True)
    async def channel_release(self, ctx, args, call_context):
        return await self.channels.call('channel_release', args, call_context)

    @service('channel_get', with_context=True)
    async def channel_get(self, ctx, args, call_context):
        return await self.channels.call('channel_get', args, call_context)


if __name__ == '__main__':
    DisplayPlugin().run()
