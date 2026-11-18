"""欢迎与公告：玩家进服时私聊欢迎语，第一次进服另发新手礼包；定时向全服轮播公告。

一次进服确认事件并记录待确认状态后，在后台核对礼包与欢迎语命令回执：全部确认送达后才写入
seen 标记并清除待确认记录；遇到玩家实体尚未生成（noTargetMatch 等明确失败）且玩家仍在线时，
带退避在后台有限次重试，避免永久漏发或重复发放。Host 重建玩家列表时会对在线玩家再投递一次
进服，靠 seen 标记与进行中的状态不重复发礼包；欢迎语可能再发一次。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json

from neomega_runtime import (AmbiguousPlayer, ChatRouter, CommandResult, Event, Plugin,
                             PluginContext, PlayerNotFound, ProtocolError, player_target)
from neomega_runtime.managed import IPCError, IPCRejected
from neomega_runtime.operations import OperationWaitTimeout
from neomega_runtime.storage import CommitUncertain

RETRY_DELAYS = (1.0, 2.0, 4.0, 8.0)
COMMAND_WAIT_TIMEOUT = 35.0


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


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def seen_key(player) -> str:
    return 'seen:' + player_id(player)


def pending_key(pid: str) -> str:
    return 'pending:' + pid


class Welcome(Plugin):
    config_type = Settings

    def __init__(self):
        # 定时公告和事件处理共用同一个全局 revision，串行提交。
        self.lock = asyncio.Lock()
        self.next_announcement = 0
        self._verifying: dict[str, asyncio.Task] = {}
        self._stopping = False

    async def on_start(self, ctx: PluginContext):
        self._stopping = False
        self._verifying = {}
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('帮助', aliases=('help', 'bz'), description='查看服务器帮助')(
                lambda command: self.help(ctx, command))
        if ctx.config.announcements:
            ctx.every(ctx.config.announcement_minutes * 60, lambda: self.announce(ctx), name='announcement')

    async def on_stop(self, ctx: PluginContext):
        self._stopping = True

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
            await event.ack()

    async def _resolve_joined(self, ctx: PluginContext, event: Event):
        body = event.payload.get('payload') or {}
        name = body.get('name') if isinstance(body.get('name'), str) else ''
        uuid = body.get('uuid') if isinstance(body.get('uuid'), str) else ''
        xuid = body.get('xuid') if isinstance(body.get('xuid'), str) else ''
        if not (name or uuid or xuid):
            return None
        kwargs = {'name': name or None, 'xuid': xuid or None}
        if not name and not xuid:
            kwargs['uuid'] = uuid
        try:
            return await ctx.resolve_player(**kwargs)
        except (PlayerNotFound, AmbiguousPlayer, ValueError):
            return None

    def _prepare_delivery(self, ctx: PluginContext, tx, name: str, pending: dict) -> list[tuple[str, int | None]]:
        slots: list[tuple[str, int | None]] = []
        target = player_target(name)
        for index, raw_cmd in enumerate(pending['commands']):
            if pending['delivered'][index]:
                continue
            cmd = raw_cmd.strip().removeprefix('/').replace('{player}', target)
            tx.action(ctx.commands.prepare(
                cmd, idempotency_key=f'{tx.commit_id}_gift_{index}', deadline=deadline()))
            slots.append(('cmd', index))
        if not pending['welcome_delivered']:
            parts = self.reply(ctx, tx, name, pending['welcome_text'].replace('{name}', name))
            slots.extend(('welcome', None) for _ in range(parts))
        return slots

    async def welcome(self, ctx, event) -> bool:
        player = await self._resolve_joined(ctx, event)
        if player is None:
            # 进服事件送达时玩家已经离开或身份缺失：只确认事件。
            return False
        pid, name = player_id(player), player['name']
        key, p_key = seen_key(player), pending_key(pid)
        tx = await event.transaction(keys=[key, p_key])
        if tx.get(key) is True:
            self.reply(ctx, tx, name, ctx.config.welcome_text.replace('{name}', name))
            await tx.save()
            return True
        if pid in self._verifying:
            await tx.save()
            return True
        try:
            player_target(name)
        except ValueError:
            ctx.log.warning('player name cannot be used in welcome commands: %r', name)
            await tx.save()
            return True

        pending = tx.get(p_key)
        if not (isinstance(pending, dict) and isinstance(pending.get('commands'), list)
                and isinstance(pending.get('delivered'), list)
                and len(pending['commands']) == len(pending['delivered'])
                and isinstance(pending.get('welcome_text'), str)
                and isinstance(pending.get('welcome_delivered'), bool)):
            pending = {
                'name': name,
                'commands': list(ctx.config.first_join_commands),
                'delivered': [False] * len(ctx.config.first_join_commands),
                'welcome_text': ctx.config.first_join_text or ctx.config.welcome_text,
                'welcome_delivered': False,
            }
        else:
            pending = {
                'name': name,
                'commands': list(pending['commands']),
                'delivered': list(pending['delivered']),
                'welcome_text': pending['welcome_text'],
                'welcome_delivered': pending['welcome_delivered'],
            }

        slots = self._prepare_delivery(ctx, tx, name, pending)
        tx.set(p_key, pending)
        receipt = await tx.save()
        op_ids = receipt.get('operation_ids') or []
        if not slots:
            # Everything already marked delivered in pending; finalize seen immediately.
            fin_tx = await ctx.storage.transaction(keys=[key, p_key])
            fin_tx.set(key, True)
            fin_tx.delete(p_key)
            await fin_tx.save()
            return True
        task = ctx.spawn(
            self._verify_and_retry(ctx, pid, key, p_key, name, pending, op_ids, slots),
            name=f'welcome-{pid}',
        )
        self._verifying[pid] = task
        return True

    async def _wait_operation(self, ctx: PluginContext, op_id: str) -> tuple[bool, bool]:
        """Return (succeeded, definite_failure) for one submitted operation."""
        try:
            row = await ctx.operations.wait(op_id, timeout=COMMAND_WAIT_TIMEOUT)
            res = CommandResult(row)
        except asyncio.CancelledError:
            raise
        except (OperationWaitTimeout, IPCError, ProtocolError, TimeoutError, ConnectionError, OSError):
            return False, False
        if res.is_success:
            return True, False
        definite = row['state'] in ('failed', 'cancelled') or (
            res.state == 'succeeded' and res.evidence == 'acknowledged'
        )
        return False, definite

    async def _verify_and_retry(self, ctx: PluginContext, pid: str, key: str, p_key: str,
                                name: str, pending: dict, op_ids: list[str],
                                slots: list[tuple[str, int | None]]):
        current = asyncio.current_task()
        try:
            for attempt in range(len(RETRY_DELAYS) + 1):
                if self._stopping:
                    return
                outcomes = await asyncio.gather(*(self._wait_operation(ctx, op_id) for op_id in op_ids))
                retryable_failure = False
                welcome_attempted = False
                welcome_ok = True
                for (kind, index), (succeeded, definite_failure) in zip(slots, outcomes):
                    if kind == 'cmd':
                        if succeeded:
                            pending['delivered'][index] = True
                        elif definite_failure:
                            retryable_failure = True
                    else:
                        welcome_attempted = True
                        if not succeeded:
                            welcome_ok = False
                            if definite_failure:
                                retryable_failure = True
                if welcome_attempted and welcome_ok:
                    pending['welcome_delivered'] = True

                all_done = all(pending['delivered']) and pending['welcome_delivered']
                async with self.lock:
                    if all_done:
                        tx = await ctx.storage.transaction(keys=[key, p_key])
                        tx.set(key, True)
                        tx.delete(p_key)
                        await tx.save()
                        return
                    tx = await ctx.storage.transaction(keys=[p_key])
                    tx.set(p_key, pending)
                    await tx.save()

                if not retryable_failure or attempt >= len(RETRY_DELAYS) or self._stopping:
                    ctx.log.warning('first-join delivery incomplete for %s (retryable=%s)', name, retryable_failure)
                    return

                await asyncio.sleep(RETRY_DELAYS[attempt])
                if self._stopping:
                    return
                try:
                    online = await ctx.players()
                except asyncio.CancelledError:
                    raise
                except (IPCError, ProtocolError, TimeoutError, ConnectionError, OSError):
                    return
                match = next((p for p in online if player_id(p) == pid), None)
                if match is None:
                    # 玩家已离线：保留 pending 状态，等下次进服继续补发。
                    return
                name = match['name']
                pending['name'] = name

                async with self.lock:
                    tx = await ctx.storage.transaction(keys=[key, p_key])
                    if tx.get(key) is True:
                        return
                    try:
                        slots = self._prepare_delivery(ctx, tx, name, pending)
                    except ValueError:
                        return
                    if not slots:
                        tx.set(key, True)
                        tx.delete(p_key)
                        await tx.save()
                        return
                    tx.set(p_key, pending)
                    receipt = await tx.save()
                    op_ids = receipt.get('operation_ids') or []
        except asyncio.CancelledError:
            raise
        except (CommitUncertain, IPCError, ProtocolError, TimeoutError, ConnectionError, OSError) as error:
            ctx.log.warning('welcome delivery verification failed for %s: %s', name, type(error).__name__)
        finally:
            async with self.lock:
                if self._verifying.get(pid) is current:
                    self._verifying.pop(pid, None)

    async def help(self, ctx, command):
        tx = await command.event.transaction(keys=[])
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

    def reply(self, ctx, tx, name, text) -> int:
        plan = ctx.notifications.prepare(name, text, idempotency_key=tx.commit_id + '_reply',
                                         deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)
        return len(plan.parts)


if __name__ == '__main__':
    Welcome().run()
