"""每日签到：玩家发送 !签到 领取当天奖励，连续签到满指定天数再领额外奖励。

一次签到只做一次存储提交：玩家记录、奖励命令、回复和事件确认一起写入，
要么全部生效，要么全部不生效。提交结果不明时异常上抛、插件重启：已生效的
提交连同事件确认一起落盘，不会再投递；未生效的事件会重新投递并照常处理。
所以不需要额外的回执或去重记录。
"""
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, player_target)


@dataclass(frozen=True)
class Milestone:
    days: int = field(metadata={'title': '连续签到天数', 'minimum': 2})
    commands: list[str] = field(default_factory=list, metadata={
        'title': '额外奖励命令', 'description': '{player} 会替换为签到的玩家'})
    message: str = field(default='', metadata={'title': '额外奖励提示'})


@dataclass(frozen=True)
class Settings:
    utc_offset_hours: int = field(default=8, metadata={
        'title': '时区', 'description': '与 UTC 相差的小时数，北京时间填 8', 'minimum': -12, 'maximum': 14})
    commands: list[str] = field(default_factory=lambda: ['give {player} bread 3'], metadata={
        'title': '每日奖励命令', 'description': '{player} 会替换为签到的玩家；留空则只记录签到'})
    message: str = field(default='签到成功，获得 3 个面包。', metadata={'title': '签到成功提示'})
    milestones: list[Milestone] = field(default_factory=lambda: [
        Milestone(7, ['give {player} diamond 1'], '连续签到 7 天，额外获得 1 颗钻石！'),
    ], metadata={'title': '连续签到奖励'})

    def __post_init__(self):
        if any(not command.strip() for command in self.commands):
            raise ValueError('reward commands must not be empty')
        if len({milestone.days for milestone in self.milestones}) != len(self.milestones):
            raise ValueError('milestone days must be unique')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_key(player) -> str:
    return 'player:' + (player['uuid'] or 'xuid-' + player['xuid'])


class DailySignin(Plugin):
    config_type = Settings

    async def on_start(self, ctx: PluginContext):
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('签到', aliases=('qd', 'signin'), description='领取今日签到奖励')(
                lambda command: self.sign(ctx, command))
            router.command('签到信息', aliases=('qdxx',), description='查看连续签到天数')(
                lambda command: self.info(ctx, command))

    async def on_event(self, ctx: PluginContext, event: Event):
        if event.kind == 'chat.received':
            try:
                for router in self.routers:
                    if await router.dispatch(event):
                        return
            except (ValueError, PlayerNotFound, AmbiguousPlayer):
                # 引号不配对，或玩家已经下线：不回复，只确认事件。
                pass
        await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    def today(self, ctx) -> date:
        offset = timedelta(hours=ctx.config.utc_offset_hours)
        return (datetime.now(timezone.utc) + offset).date()

    async def sign(self, ctx, command):
        player, key = command.player, player_key(command.player)
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[key])
        record = tx.get(key, {'day': '', 'streak': 0, 'total': 0})
        today = self.today(ctx)
        if record['day'] == today.isoformat():
            self.reply(ctx, tx, player, f'今天已经签到过了，已连续签到 {record["streak"]} 天，明天再来吧。')
            await tx.save()
            return

        continued = record['day'] == (today - timedelta(days=1)).isoformat()
        streak = record['streak'] + 1 if continued else 1
        record = {'day': today.isoformat(), 'streak': streak, 'total': record['total'] + 1}
        tx.set(key, record)

        commands, lines = list(ctx.config.commands), [ctx.config.message]
        for milestone in ctx.config.milestones:
            if milestone.days == streak:
                commands += milestone.commands
                if milestone.message:
                    lines.append(milestone.message)
        lines.append(f'已连续签到 {streak} 天，累计 {record["total"]} 天。')

        target = player_target(player['name'])
        for index, line in enumerate(commands):
            tx.action(ctx.commands.prepare(line.strip().removeprefix('/').replace('{player}', target),
                                           idempotency_key=f'{tx.commit_id}_reward_{index}',
                                           deadline=deadline()))
        self.reply(ctx, tx, player, '\n'.join(lines))
        await tx.save()

    async def info(self, ctx, command):
        player, key = command.player, player_key(command.player)
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[key])
        record = tx.get(key)
        if record is None:
            text = '你还没有签到过，发送 !签到 领取今日奖励。'
        else:
            yesterday = (self.today(ctx) - timedelta(days=1)).isoformat()
            streak = record['streak'] if record['day'] >= yesterday else 0
            signed = '今天已签到' if record['day'] == self.today(ctx).isoformat() else '今天还没签到'
            text = f'{signed}，当前连续 {streak} 天，累计 {record["total"]} 天。'
        self.reply(ctx, tx, player, text)
        await tx.save()

    def reply(self, ctx, tx, player, text):
        plan = ctx.notifications.prepare(player['name'], text,
                                         idempotency_key=tx.commit_id + '_reply', deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)


if __name__ == '__main__':
    DailySignin().run()
