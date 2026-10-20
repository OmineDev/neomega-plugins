"""欢迎与公告：玩家进服时私聊欢迎语，第一次进服另发新手礼包；定时向全服轮播公告。

一次进服只做一次存储提交：首次进服标记、礼包命令、欢迎语和事件确认一起写入，
要么全部生效，要么全部不生效。Host 重建玩家列表时会对在线玩家再投递一次进服，
靠 seen 标记不重复发礼包；欢迎语可能再发一次。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, player_target)
from neomega_runtime.managed import IPCRejected
from neomega_runtime.storage import CommitUncertain


@dataclass(frozen=True)
class Settings:
    welcome_text: str = field(default='欢迎回来，{name}！输入 !帮助 查看可用命令。', metadata={
        'title': '欢迎语', 'description': '玩家进服时私聊发送，{name} 会替换为玩家名'})
    first_join_text: str = field(default='欢迎 {name} 第一次来到服务器！送你一份新手礼包。', metadata={
        'title': '首次进服欢迎语', 'description': '玩家第一次进服时改发这句；留空则发普通欢迎语'})
    first_join_commands: list[str] = field(default_factory=lambda: ['give {player} bread 16'], metadata={
        'title': '新手礼包命令', 'description': '玩家第一次进服时执行，{player} 会替换为该玩家；留空则不发礼包'})
    help_text: str = field(default='可用命令：!签到、!回家、!tpa 玩家名。有问题请联系管理员。', metadata={
        'title': '帮助内容', 'description': '玩家发送 !帮助 时私聊发送'})
    announcements: list[str] = field(default_factory=lambda: ['请文明交流，祝你玩得开心！'], metadata={
        'title': '公告', 'description': '按顺序轮流向全服广播；留空则不发公告'})
    announcement_minutes: int = field(default=10, metadata={
        'title': '公告间隔（分钟）', 'description': '每隔多少分钟广播下一条公告', 'minimum': 1})

    def __post_init__(self):
        if any(not command.strip() for command in self.first_join_commands):
            raise ValueError('first join commands must not be empty')
        if any(not text.strip() for text in self.announcements):
            raise ValueError('announcements must not be empty')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def seen_key(player) -> str:
    return 'seen:' + (player['uuid'] or 'xuid-' + player['xuid'])


class Welcome(Plugin):
    config_type = Settings

    def __init__(self):
        # 定时公告和事件处理共用同一个全局 revision，串行提交。
        self.lock = asyncio.Lock()
        self.next_announcement = 0

    async def on_start(self, ctx: PluginContext):
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('帮助', aliases=('help', 'bz'), description='查看服务器帮助')(
                lambda command: self.help(ctx, command))
        if ctx.config.announcements:
            ctx.every(ctx.config.announcement_minutes * 60, lambda: self.announce(ctx), name='announcement')

    async def on_event(self, ctx: PluginContext, event: Event):
        async with self.lock:
            if event.kind == 'player.list_added' and await self.welcome(ctx, event):
                return
            if event.kind == 'chat.received':
                try:
                    for router in self.routers:
                        if await router.dispatch(event):
                            return
                except (ValueError, PlayerNotFound, AmbiguousPlayer):
                    # 引号不配对，或玩家已经下线：不回复，只确认事件。
                    pass
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    async def welcome(self, ctx, event) -> bool:
        body = event.payload['payload']
        try:
            player = await ctx.resolve_player(name=body['name'], xuid=body.get('xuid') or None)
        except (PlayerNotFound, AmbiguousPlayer):
            # 进服事件送达时玩家已经离开：只确认事件。
            return False
        key, name = seen_key(player), player['name']
        tx = await ctx.storage.transaction(event=event.payload, keys=[key])
        text = ctx.config.welcome_text
        if not tx.get(key, False):
            tx.set(key, True)
            text = ctx.config.first_join_text or text
            target = player_target(name)
            for index, line in enumerate(ctx.config.first_join_commands):
                tx.action(ctx.commands.prepare(line.strip().removeprefix('/').replace('{player}', target),
                                               idempotency_key=f'{tx.commit_id}_gift_{index}',
                                               deadline=deadline()))
        self.reply(ctx, tx, name, text.replace('{name}', name))
        await tx.save()
        return True

    async def help(self, ctx, command):
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
        self.reply(ctx, tx, command.player['name'], ctx.config.help_text)
        await tx.save()

    async def announce(self, ctx):
        announcements = ctx.config.announcements
        text = announcements[self.next_announcement % len(announcements)]
        self.next_announcement = (self.next_announcement + 1) % len(announcements)
        rawtext = json.dumps({'rawtext': [{'text': text}]}, ensure_ascii=False, separators=(',', ':'))
        async with self.lock:
            try:
                tx = await ctx.storage.transaction(keys=[])
                tx.action(ctx.commands.prepare('tellraw @a ' + rawtext,
                                               idempotency_key=tx.commit_id + '_announce', deadline=deadline()))
                await tx.save()
            except (CommitUncertain, IPCRejected) as error:
                # 没有事件可以重投，这一条就跳过，下一轮照常广播下一条。
                ctx.log.warning('announcement not committed: %s', type(error).__name__)

    def reply(self, ctx, tx, name, text):
        plan = ctx.notifications.prepare(name, text, idempotency_key=tx.commit_id + '_reply',
                                         deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)


if __name__ == '__main__':
    Welcome().run()
