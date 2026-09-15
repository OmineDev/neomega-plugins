"""Native neomega service plugin; credentials are resolved only when needed."""
from neomega_runtime import Plugin
from fatalder_plugin.settings import Settings
from fatalder_plugin.client import WorkerClient
from fatalder_plugin.operator import Operator
from fatalder_plugin.controller import Controller


class CloudImport(Plugin):
    config_type = Settings

    async def on_start(self, ctx):
        client = WorkerClient(ctx.config.worker_url, lambda: ctx.secrets.get(ctx.config.api_key),
                              timeout=30, max_upload_bytes=ctx.config.max_upload_bytes)
        self.controller = Controller(ctx, client, Operator(ctx))
        await self.controller.start()

    async def on_event(self, ctx, event):
        await self.controller.handle(event)

    async def on_maintenance(self, ctx, request):
        if not hasattr(self, 'controller'):
            return {'status': 'unsupported'}
        return await self.controller.maintenance(request)

    async def on_stop(self, ctx):
        if hasattr(self, 'controller'):
            await self.controller.stop()


if __name__ == '__main__':
    CloudImport().run()
