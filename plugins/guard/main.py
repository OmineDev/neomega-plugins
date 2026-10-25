"""服务器守卫：进服门槛（等级、昵称、人数）和聊天防刷屏，不满足时先提示原因再踢出。

框架只能在玩家进服后处置，也不能撤回聊天：进服事件到达后检查，不通过就先私聊并
显示标题说明原因，等几秒再踢出；刷屏先警告，再犯才处罚。机器人自己和管理员永远豁免。

一次处置只做一次存储提交：处置记录、提示、处罚命令和事件确认一起写入。延后的踢出
没有事件可以重投，单独提交，失败只记日志，玩家下次进服或再刷屏时会重新检查。
刷屏计数、警告次数和进服先后只保存在内存里，插件重启后清空。
"""
import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import re
import time
from uuid import uuid4

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, player_target)
from neomega_runtime.command_queries import CommandQueries
from neomega_runtime.game import CommandResult
from neomega_runtime.http import ProtocolError
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import OperationWaitTimeout
from neomega_runtime.storage import CommitUncertain

RECORDS = 'records'
WAIT = 35  # 查询动作期限 30 秒，多等几秒回应。
BEIJING = timezone(timedelta(hours=8))
RULES = {'level': '等级不足', 'name': '昵称违规', 'full': '人数已满', 'spam': '刷屏'}
ACTIONS = {'kick': '踢出', 'warn': '警告', 'punish': '处罚'}
TITLES = {'kick': '§c即将被请出服务器', 'warn': '§e警告', 'punish': '§c处罚'}


@dataclass(frozen=True)
class Settings:
    admins: list[str] = field(default_factory=list, metadata={
        'title': '管理员', 'description': '这些玩家不受任何检查，并且可以用 !守卫记录 查看处置记录；填玩家名，不区分大小写'})
    level_check: bool = field(default=False, metadata={
        'title': '启用最低等级', 'description': '玩家进服时检查经验等级，不够就请出'})
    min_level: int = field(default=10, metadata={
        'title': '最低等级', 'description': '玩家经验等级至少要达到多少级', 'minimum': 1, 'maximum': 24791})
    name_check: bool = field(default=False, metadata={
        'title': '启用昵称黑名单', 'description': '玩家名字含有下面的关键词或匹配正则时请出'})
    name_keywords: list[str] = field(default_factory=list, metadata={
        'title': '昵称关键词', 'description': '名字里含有任意一个词就请出，不区分大小写'})
    name_patterns: list[str] = field(default_factory=list, metadata={
        'title': '昵称正则（高级）', 'description': '用正则表达式匹配名字，不区分大小写；不会写可以留空，只用关键词'})
    player_limit_check: bool = field(default=False, metadata={
        'title': '启用人数上限', 'description': '在线玩家（不含机器人）超过上限时，请出最新进服的玩家'})
    max_players: int = field(default=20, metadata={
        'title': '人数上限', 'description': '最多允许多少名玩家同时在线，不含机器人', 'minimum': 1})
    player_limit_exempt: list[str] = field(default_factory=list, metadata={
        'title': '人数上限白名单', 'description': '填玩家名或 UUID；这些玩家满员也能进，但仍计入在线人数'})
    spam_check: bool = field(default=True, metadata={
        'title': '启用防刷屏', 'description': '短时间内发太多消息或重复同一句话时，先警告，再犯就处罚'})
    spam_window_seconds: int = field(default=10, metadata={
        'title': '刷屏统计时间（秒）', 'description': '统计最近多少秒内的消息', 'minimum': 1, 'maximum': 300})
    spam_max_messages: int = field(default=6, metadata={
        'title': '最多消息条数', 'description': '统计时间内超过这么多条就算刷屏', 'minimum': 1})
    spam_repeat_limit: int = field(default=3, metadata={
        'title': '重复消息上限', 'description': '统计时间内同一句话发到这么多次就算刷屏；填 0 不检查重复', 'minimum': 0})
    spam_warnings: int = field(default=1, metadata={
        'title': '警告次数', 'description': '先警告几次，再犯才处罚；填 0 第一次就处罚', 'minimum': 0, 'maximum': 10})
    spam_forgive_minutes: int = field(default=10, metadata={
        'title': '警告有效期（分钟）', 'description': '这么久没有再刷屏，警告次数清零', 'minimum': 1})
    spam_punish_commands: list[str] = field(default_factory=list, metadata={
        'title': '刷屏处罚命令', 'description': '处罚时改为执行这些命令，{player} 会替换为该玩家；留空则踢出'})
    kick_delay_seconds: int = field(default=3, metadata={
        'title': '踢出前等待（秒）', 'description': '先显示原因，等几秒再踢出，让玩家看清提示', 'minimum': 0, 'maximum': 30})
    level_message: str = field(default='需要达到 {level} 级才能进入本服务器，升级后再来吧。', metadata={
        'title': '等级不足提示', 'description': '也作为踢出原因，{level} 会替换为最低等级'})
    name_message: str = field(default='你的名字含有本服务器不允许的内容，请改名后再来。', metadata={
        'title': '昵称违规提示', 'description': '也作为踢出原因'})
    full_message: str = field(default='服务器人数已满（上限 {max} 人），请稍后再来。', metadata={
        'title': '人数已满提示', 'description': '也作为踢出原因，{max} 会替换为人数上限'})
    spam_warning_message: str = field(default='请不要刷屏，再犯会被处罚。', metadata={
        'title': '刷屏警告', 'description': '警告时私聊发送'})
    spam_punish_message: str = field(default='你因刷屏被请出服务器。', metadata={
        'title': '刷屏处罚提示', 'description': '处罚时私聊发送；踢出时也作为踢出原因'})
    record_limit: int = field(default=100, metadata={
        'title': '保留处置记录条数', 'description': '只保留最近这么多条，旧的自动删除', 'minimum': 10, 'maximum': 500})

    def __post_init__(self):
        lists = (self.admins, self.name_keywords, self.name_patterns, self.player_limit_exempt,
                 self.spam_punish_commands)
        if any(not entry.strip() for entries in lists for entry in entries):
            raise ValueError('list entries must not be empty')
        for pattern in self.name_patterns:
            try:
                re.compile(pattern)
            except re.error as error:
                raise ValueError(f'invalid name pattern {pattern!r}: {error}') from None
        messages = (self.level_message, self.name_message, self.full_message,
                    self.spam_warning_message, self.spam_punish_message)
        if any(not text.strip() for text in messages):
            raise ValueError('messages must not be empty')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def listed(entries, player) -> bool:
    """名单里写的是玩家名（不区分大小写）或 UUID。"""
    wanted = {entry.strip().casefold() for entry in entries}
    return player['name'].casefold() in wanted or bool(player.get('uuid')) and player['uuid'].casefold() in wanted


def normalize(text) -> str:
    return ' '.join(text.split()).casefold()


def kick_reason(text) -> str:
    # 踢出原因是一行纯文本；@ 会被服务器展开成选择器，换成全角。
    return ' '.join(text.split()).replace('@', '＠')[:300]


def below_level(receipt) -> bool:
    """只有服务器明确回复“没有匹配的目标”才算等级不足；其他结果都不能下结论。"""
    command = CommandResult(receipt)
    return (command.state == 'failed' and command.evidence == 'acknowledged'
            and command.output is not None and command.output.success_count == 0
            and len(command.messages) == 1 and not command.messages[0].success
            and command.messages[0].message == 'commands.generic.noTargetMatch')


class Guard(Plugin):
    config_type = Settings

    def __init__(self):
        # 事件处理和延后踢出共用同一个全局 revision，串行提交。
        self.lock = asyncio.Lock()
        self.bot_name = None
        self.messages: dict[str, deque] = {}   # 玩家名 -> 最近消息 (时间, 规整后的内容)
        self.strikes: dict[str, deque] = {}    # 玩家名 -> 有效期内的违规时间
        self.joined: dict[str, int] = {}       # 玩家 ID -> 进服先后序号
        self.seq = 0
        self.leaving: dict[str, float] = {}    # 已决定踢出的玩家名 -> 截止时间：不计人数，不再处置

    async def on_start(self, ctx: PluginContext):
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('守卫记录', aliases=('guard',), description='管理员查看最近的处置记录',
                           usage='[条数]', max_args=1)(
                lambda command: self.show(ctx, command))

    async def on_event(self, ctx: PluginContext, event: Event):
        try:
            if event.kind == 'player.list_added' and await self.join(ctx, event):
                return
            if event.kind == 'chat.received' and await self.chat(ctx, event):
                return
        except (ValueError, PlayerNotFound, AmbiguousPlayer):
            # 名字不能用作选择器、引号不配对，或玩家已经下线：不处置，只确认事件。
            pass
        async with self.lock:
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    def now(self) -> float:
        return time.monotonic()

    async def robot(self, ctx):
        """机器人名字；读取失败时沿用上次读到的，从没读到过时返回 None。"""
        try:
            name = (await ctx.robot())['bot_name']
        except (IPCRejected, ProtocolError, ConnectionError, TimeoutError) as error:
            ctx.log.warning('robot status unavailable: %s', type(error).__name__)
            name = None
        if name:
            self.bot_name = name
        return self.bot_name

    def is_leaving(self, name) -> bool:
        now = self.now()
        self.leaving = {k: v for k, v in self.leaving.items() if v > now}
        return name in self.leaving

    # ---- 进服门槛 ----

    async def join(self, ctx, event) -> bool:
        config = ctx.config
        if not (config.level_check or config.name_check or config.player_limit_check):
            return False
        body = event.payload['payload']
        player = await ctx.resolve_player(name=body['name'], xuid=body.get('xuid') or None)
        bot = await self.robot(ctx)
        if bot is None:
            # 认不出机器人时宁可不查，也不能把机器人自己踢掉。
            ctx.log.warning('robot name unknown, join checks skipped')
            return False
        if player['name'] == bot or listed(config.admins, player) or self.is_leaving(player['name']):
            return False
        roster = [p for p in await ctx.players() if p['name'] != bot]
        if all(player_id(p) != player_id(player) for p in roster):
            roster.append(player)
        self.track(roster, player)
        verdict = self.check_name(config, player) or self.check_full(config, roster, player)
        if verdict is None:
            verdict = await self.check_level(ctx, player)
        if verdict is None:
            return False
        rule, message, detail = verdict
        await self.dispose(ctx, event.payload, player, rule, 'kick', message, detail)
        return True

    def track(self, roster, player):
        """记录进服先后。插件启动前就在线的玩家没有序号，算作最早进服。"""
        online = {player_id(p) for p in roster}
        self.joined = {k: v for k, v in self.joined.items() if k in online}
        if player_id(player) not in self.joined:
            self.seq += 1
            self.joined[player_id(player)] = self.seq

    def check_name(self, config, player):
        if not config.name_check:
            return None
        name = player['name']
        for keyword in config.name_keywords:
            if keyword.strip().casefold() in name.casefold():
                return 'name', config.name_message, f'名字含有关键词“{keyword.strip()}”'
        for pattern in config.name_patterns:
            if re.search(pattern, name, re.IGNORECASE):
                return 'name', config.name_message, f'名字匹配正则 {pattern}'
        return None

    def check_full(self, config, roster, player):
        if not config.player_limit_check or listed(config.player_limit_exempt, player):
            return None
        staying = [p for p in roster if p['name'] not in self.leaving]
        excess = len(staying) - config.max_players
        if excess <= 0:
            return None
        # 只请出超出的人数，从最新进服的非豁免玩家开始；机器人重连后重投进服事件也不会多踢。
        candidates = [p for p in staying
                      if not listed(config.admins, p) and not listed(config.player_limit_exempt, p)]
        candidates.sort(key=lambda p: self.joined.get(player_id(p), 0), reverse=True)
        if player_id(player) not in {player_id(p) for p in candidates[:excess]}:
            return None
        return ('full', config.full_message.replace('{max}', str(config.max_players)),
                f'在线 {len(staying)} 人，上限 {config.max_players} 人')

    async def check_level(self, ctx, player):
        config = ctx.config
        if not config.level_check:
            return None
        # lm 是最低经验等级选择器：能选中说明等级够了。
        selector = player_target(player['name'])[:-1] + f',lm={config.min_level}]'
        intent = ctx.command_queries.testfor(selector, idempotency_key=uuid4().hex, deadline=deadline())
        try:
            receipt = await ctx.operations.execute(intent, timeout=WAIT)
        except (OperationWaitTimeout, IPCRejected) as error:
            ctx.log.warning('level check unavailable, %s allowed: %s', player['name'], type(error).__name__)
            return None
        if CommandQueries.parse_testfor(receipt).known:
            return None
        if below_level(receipt):
            return ('level', config.level_message.replace('{level}', str(config.min_level)),
                    f'等级低于 {config.min_level} 级')
        ctx.log.warning('level check inconclusive, %s allowed: %s', player['name'], receipt['state'])
        return None

    # ---- 防刷屏 ----

    async def chat(self, ctx, event) -> bool:
        # 先计数再分派命令：以 ! 开头的消息同样算刷屏。
        if await self.spam(ctx, event):
            return True
        for router in self.routers:
            if await router.dispatch(event):
                return True
        return False

    async def spam(self, ctx, event) -> bool:
        config = ctx.config
        if not config.spam_check:
            return False
        body = event.payload['payload']
        name, text = body.get('player'), body.get('message')
        if not isinstance(name, str) or not name or not isinstance(text, str):
            return False
        if listed(config.admins, {'name': name}) or self.is_leaving(name):
            return False
        bot = await self.robot(ctx)
        if bot is None or name == bot:
            return False

        now, window = self.now(), config.spam_window_seconds
        self.forget(config, now)
        recent = self.messages.setdefault(name, deque())
        recent.append((now, normalize(text)))
        while recent[0][0] <= now - window:
            recent.popleft()
        repeats = sum(1 for _, line in recent if line == recent[-1][1])
        if config.spam_repeat_limit and repeats >= config.spam_repeat_limit:
            detail = f'{window} 秒内同一句话发了 {repeats} 次'
        elif len(recent) > config.spam_max_messages:
            detail = f'{window} 秒内发了 {len(recent)} 条消息'
        else:
            return False

        player = await ctx.resolve_player(name=name, xuid=body.get('xuid') or None)
        del self.messages[name]   # 违规后重新计数，下一次违规要再刷一轮
        strikes = self.strikes.setdefault(name, deque())
        strikes.append(now)
        if len(strikes) <= config.spam_warnings:
            action, message = 'warn', config.spam_warning_message
            detail += f'，第 {len(strikes)} 次警告'
        else:
            del self.strikes[name]
            action, message = ('punish' if config.spam_punish_commands else 'kick'), config.spam_punish_message
        await self.dispose(ctx, event.payload, player, 'spam', action, message, detail)
        return True

    def forget(self, config, now):
        """清掉过期的消息和警告，内存只保留最近活跃的玩家。"""
        window, forgive = config.spam_window_seconds, config.spam_forgive_minutes * 60
        self.messages = {k: v for k, v in self.messages.items() if v and v[-1][0] > now - window}
        for strikes in self.strikes.values():
            while strikes and strikes[0] <= now - forgive:
                strikes.popleft()
        self.strikes = {k: v for k, v in self.strikes.items() if v}

    # ---- 处置与记录 ----

    async def dispose(self, ctx, event_payload, player, rule, action, message, detail):
        config, name = ctx.config, player['name']
        target = player_target(name)
        delay = config.kick_delay_seconds
        async with self.lock:
            tx = await ctx.storage.transaction(event=event_payload, keys=[RECORDS])
            records = tx.get(RECORDS, [])
            records.append({'at': datetime.now(timezone.utc).isoformat(timespec='seconds'),
                            'player': name, 'uuid': player['uuid'], 'rule': rule,
                            'action': action, 'detail': detail})
            tx.set(RECORDS, records[-config.record_limit:])
            self.reply(ctx, tx, name, '[服务器守卫] ' + message)
            for part, text in (('subtitle', message), ('title', TITLES[action])):
                rawtext = json.dumps({'rawtext': [{'text': text}]}, ensure_ascii=False, separators=(',', ':'))
                tx.action(ctx.commands.prepare(f'titleraw {target} {part} {rawtext}',
                                               idempotency_key=f'{tx.commit_id}_{part}', deadline=deadline()))
            if action == 'punish':
                for index, line in enumerate(config.spam_punish_commands):
                    tx.action(ctx.commands.prepare(line.strip().removeprefix('/').replace('{player}', target),
                                                   idempotency_key=f'{tx.commit_id}_punish_{index}',
                                                   deadline=deadline()))
            if action == 'kick' and delay == 0:
                tx.action(self.kick_intent(ctx, name, message, tx.commit_id))
            await tx.save()
        if action == 'kick':
            self.leaving[name] = self.now() + delay + 30
            if delay:
                commit_id = tx.commit_id
                ctx.later(delay, lambda: self.kick(ctx, name, message, commit_id), name='guard-kick')

    def kick_intent(self, ctx, name, message, commit_id):
        return ctx.player_actions.kick(name, kick_reason(message), idempotency_key=commit_id + '_kick',
                                       deadline=deadline())

    async def kick(self, ctx, name, message, commit_id):
        async with self.lock:
            try:
                tx = await ctx.storage.transaction(keys=[])
                tx.action(self.kick_intent(ctx, name, message, commit_id))
                await tx.save()
            except (CommitUncertain, IPCRejected) as error:
                # 没有事件可以重投；玩家下次进服或再刷屏时会重新检查。
                ctx.log.warning('kick not committed: %s', type(error).__name__)

    async def show(self, ctx, command):
        name = command.player['name']
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=[RECORDS])
            if not listed(ctx.config.admins, command.player):
                text = '只有管理员可以查看处置记录。'
            elif command.args and not (command.args[0].isdigit() and 1 <= int(command.args[0]) <= 20):
                text = '条数请填 1 到 20 之间的数字，例如 !守卫记录 10'
            else:
                count = int(command.args[0]) if command.args else 5
                records = tx.get(RECORDS, [])[-count:]
                lines = [self.describe(record) for record in reversed(records)]
                text = '\n'.join([f'最近 {len(lines)} 条处置记录（北京时间）：'] + lines) if lines else '还没有处置记录。'
            self.reply(ctx, tx, name, text)
            await tx.save()

    @staticmethod
    def describe(record) -> str:
        at = datetime.fromisoformat(record['at']).astimezone(BEIJING).strftime('%m-%d %H:%M')
        return f'{at} {record["player"]} {RULES[record["rule"]]}，{ACTIONS[record["action"]]}：{record["detail"]}'

    def reply(self, ctx, tx, name, text):
        plan = ctx.notifications.prepare(name, text, idempotency_key=tx.commit_id + '_reply',
                                         deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)


if __name__ == '__main__':
    Guard().run()
