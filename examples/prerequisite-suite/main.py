"""Read-only runnable consumer: inspect provider data without a game session."""
from dataclasses import dataclass
from neomega_runtime import Plugin
from neomega_clients import PlayersClient, SchedulerClient


@dataclass(frozen=True)
class Settings:
    poll_seconds: float = 60.0

    def __post_init__(self):
        if self.poll_seconds < 10:
            raise ValueError('poll_seconds must be at least 10')


class PrerequisiteConsumer(Plugin):
    config_type = Settings

    async def on_start(self, ctx):
        self.players = PlayersClient(ctx.services)
        self.scheduler = SchedulerClient(ctx.services)
        await self.inspect(ctx)
        ctx.every(ctx.config.poll_seconds, lambda: self.inspect(ctx), name='prerequisite-inspect')

    async def inspect(self, ctx):
        players = await self.players.query({'limit': 10})
        schedules = await self.scheduler.query({})
        ctx.log.info('前置服务已连接：档案数=%s，当前消费者任务数=%s',
                     len(players.get('items', [])), len(schedules.get('schedules', [])))


if __name__ == '__main__':
    PrerequisiteConsumer().run()
