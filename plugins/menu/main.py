"""自定义菜单：服主配置菜单项，玩家发送 !菜单 在聊天菜单里按编号执行，或直接用快捷指令。

每个菜单项是一组命令，由机器人执行；可选以玩家身份执行（execute as 玩家）、
花费金币、冷却、仅管理员和分组。免费快捷指令在 on_event 里一次提交：冷却记录、
命令、回复和事件确认一起写入。打开菜单或使用收费项目时先确认事件，再在
ctx.spawn 里等待玩家回复；等待期间不持锁，只有存储提交持锁，和 on_event 的
确认共用一个全局 revision。

金币由可选的经济插件管理。每次收费生成一个随机 request_id 交给经济插件扣款；
等待超时或结果不明时用同一个 request_id 再扣一次，经济插件按它去重。没装经济
插件时收费项目不能使用，免费项目照常执行。

可选的 !帮助 / !指令 汇总菜单命令、玩家能用的快捷指令和服主勾选的官方插件命令。
"""
import asyncio
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta, timezone
import math
import time
import unicodedata
import uuid

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ServiceRejected, player_target)
from neomega_runtime.managed import IPCRejected
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from neomega_runtime.storage import CommitUncertain
import neomega_menu as menu

ECONOMY = 'plugin.neomega.economy.'
# 结果不明的扣款可以用同一个 request_id 安全重试；明确的业务拒绝不在其中。
UNSETTLED = (ServiceWaitTimeout, ServiceUncertain, ServiceFailed)
# !指令 一直可用；!帮助 和欢迎插件重名，只有打开 help_takeover 时才注册。
HELP_COMMANDS = ('指令', 'zl')
HELP_TAKEOVER_COMMANDS = ('帮助', 'help', 'bz')
NEXT_WORDS = frozenset({'+', 'n', '下', '下一页'})
PREVIOUS_WORDS = frozenset({'-', 'p', '上', '上一页'})
MAX_ATTEMPTS = 3
UNCERTAIN_CHARGE = '扣款结果暂时不确定，请用 !流水 查看；没有扣款可以重新使用。'


def command_name(text: str) -> str:
    """去掉可选的 ! 或 ！ 前缀；名字只能含文字、数字、_ 和 -，与 ChatRouter 的规则一致。"""
    name = text.strip().removeprefix('!').removeprefix('！')
    if (not 1 <= len(name) <= 64
            or any(c not in '_-' and unicodedata.category(c)[0] not in 'LN' for c in name)):
        raise ValueError('invalid command name')
    return name


@dataclass(frozen=True)
class Item:
    name: str = field(metadata={'title': '显示名', 'description': '菜单里显示的名字，不能重复'})
    commands: list[str] = field(metadata={
        'title': '执行的命令', 'description': '按顺序执行，{player} 会替换为使用的玩家'})
    trigger: str = field(default='', metadata={
        'title': '快捷指令', 'description': '可选，例如 主城，玩家发送 !主城 直接使用；留空则只能从菜单里选'})
    as_player: bool = field(default=False, metadata={
        'title': '以玩家身份执行', 'description': '打开后用 execute as 让命令以该玩家为执行者、在玩家位置执行；命令仍由机器人发出'})
    cost: int = field(default=0, metadata={
        'title': '花费金币', 'description': '需要安装经济插件；填 0 免费', 'minimum': 0})
    cooldown_seconds: int = field(default=0, metadata={
        'title': '冷却秒数', 'description': '同一玩家两次使用的最短间隔；填 0 不限制', 'minimum': 0})
    admin_only: bool = field(default=False, metadata={
        'title': '仅管理员', 'description': '只有管理员名单里的玩家能看到和使用'})
    group: str = field(default='', metadata={
        'title': '分组', 'description': '可选；有分组时菜单先列出分组'})
    message: str = field(default='', metadata={
        'title': '执行后提示', 'description': '留空则提示“已执行「显示名」”'})


@dataclass(frozen=True)
class HelpPlugins:
    signin: bool = field(default=False, metadata={'title': '每日签到'})
    economy: bool = field(default=False, metadata={'title': '经济'})
    shop: bool = field(default=False, metadata={'title': '积分商店'})
    homes: bool = field(default=False, metadata={'title': '个人传送点'})
    tpa: bool = field(default=False, metadata={'title': '玩家互传'})
    online_time: bool = field(default=False, metadata={'title': '在线时长'})


# 官方插件的玩家命令，和各插件 README 一致；服主在 help_plugins 里勾选本服装了的。
KNOWN_COMMANDS = {
    'signin': ('!签到 — 领取每日签到奖励', '!签到信息 — 查看连续签到天数'),
    'economy': ('!余额 — 查看自己的金币', '!转账 玩家 数量 — 把金币转给其他玩家', '!流水 — 查看最近收支'),
    'shop': ('!商店 — 打开积分商店',),
    'homes': ('!设家 [名字] — 把当前位置记为家', '!回家 [名字] — 传送回家',
              '!删家 名字 — 删除这个家', '!家列表 — 查看自己所有的家'),
    'tpa': ('!tpa 玩家 — 请求传送到对方身边', '!tpahere 玩家 — 请求把对方拉过来',
            '!同意 / !拒绝 — 回应传送请求'),
    'online_time': ('!在线时长 — 查看在线时长', '!在线排行 — 查看在线时长排行'),
}


@dataclass(frozen=True)
class Settings:
    title: str = field(default='服务器菜单', metadata={'title': '菜单标题'})
    menu_commands: list[str] = field(default_factory=lambda: ['菜单', 'cd', 'menu'], metadata={
        'title': '打开菜单的指令', 'description': '至少一个，例如 菜单 表示玩家发送 !菜单'})
    page_size: int = field(default=8, metadata={
        'title': '每页条数', 'description': '菜单项多于这个数时分页显示', 'minimum': 3, 'maximum': 20})
    admins: list[str] = field(default_factory=list, metadata={
        'title': '管理员', 'description': '玩家名，能看到和使用“仅管理员”的菜单项'})
    items: list[Item] = field(default_factory=lambda: [
        Item('回主城', ['tp {player} 0 100 0'], trigger='主城', cooldown_seconds=30, group='传送'),
        Item('领取面包', ['give {player} bread 8'], cost=5, cooldown_seconds=300, group='福利'),
        Item('设为白天', ['time set day'], admin_only=True, group='管理'),
    ], metadata={'title': '菜单项', 'description': '至少一项，显示名和快捷指令不能重复'})
    help_takeover: bool = field(default=False, metadata={
        'title': '接管 !帮助', 'description': '打开后 !帮助 也列出本服命令；装了欢迎插件时两边都会回复，请只开一边'})
    help_plugins: HelpPlugins = field(default_factory=HelpPlugins, metadata={
        'title': '帮助里列出的其他插件', 'description': '勾选本服已安装的官方插件，!指令 会列出它们的命令'})
    help_extra: list[str] = field(default_factory=list, metadata={
        'title': '帮助附加行', 'description': '原样加在帮助末尾，例如其他插件的命令或联系方式'})

    def __post_init__(self):
        if not self.items:
            raise ValueError('menu needs at least one item')
        if not self.menu_commands:
            raise ValueError('menu needs at least one command')
        if len({item.name for item in self.items}) != len(self.items):
            raise ValueError('item names must be unique')
        if any(not item.name.strip() for item in self.items):
            raise ValueError('item names must not be empty')
        if any(not item.commands or any(not line.strip() for line in item.commands) for item in self.items):
            raise ValueError('every item needs nonempty commands')
        if any(not name.strip() for name in self.admins):
            raise ValueError('admin names must not be empty')
        if any(not line.strip() for line in self.help_extra):
            raise ValueError('help lines must not be empty')
        reserved = [command_name(name) for name in self.menu_commands] + list(HELP_COMMANDS)
        if self.help_takeover:
            reserved += HELP_TAKEOVER_COMMANDS
        triggers = [command_name(item.trigger) for item in self.items if item.trigger.strip()]
        if len(set(reserved + triggers)) != len(reserved) + len(triggers):
            raise ValueError('menu commands and item triggers must not repeat')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def cooldown_key(player) -> str:
    return 'cooldown:' + player_id(player)


def item_commands(item, target) -> list[str]:
    commands = []
    for line in item.commands:
        command = line.strip().removeprefix('/').replace('{player}', target)
        if item.as_player:
            command = f'execute as {target} at @s run {command}'
        commands.append(command)
    return commands


class Menu(Plugin):
    config_type = Settings

    async def on_start(self, ctx: PluginContext):
        self.lock = asyncio.Lock()
        config = ctx.config
        menu_names = [command_name(name) for name in config.menu_commands]
        help_names = list(HELP_COMMANDS) + (list(HELP_TAKEOVER_COMMANDS) if config.help_takeover else [])
        self.shortcuts = {command_name(item.trigger): item for item in config.items if item.trigger.strip()}
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command(menu_names[0], aliases=tuple(menu_names[1:]), description='打开服务器菜单',
                           max_args=1)(lambda command: self.open(ctx, command))
            router.command(help_names[0], aliases=tuple(help_names[1:]), description='查看本服命令')(
                lambda command: self.help(ctx, command))
            for trigger, item in self.shortcuts.items():
                router.command(trigger, description=item.name)(
                    lambda command, item=item: self.shortcut(ctx, command, item))

    async def on_event(self, ctx: PluginContext, event: Event):
        async with self.lock:
            try:
                # 菜单回复由 feed 确认事件。
                if await menu.feed(ctx, event):
                    return
                if event.kind == 'chat.received':
                    for router in self.routers:
                        if await router.dispatch(event):
                            return
            except (ValueError, PlayerNotFound, AmbiguousPlayer):
                # 引号不配对、参数太多，或玩家已经下线：不回复，只确认事件。
                pass
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    def now(self) -> float:
        return time.time()

    def is_admin(self, ctx, player) -> bool:
        return player['name'] in ctx.config.admins

    def visible(self, ctx, player) -> list[Item]:
        admin = self.is_admin(ctx, player)
        return [item for item in ctx.config.items if admin or not item.admin_only]

    # ---- 聊天命令 ----

    async def open(self, ctx, command):
        await (await ctx.storage.transaction(event=command.event.payload, keys=[])).save()
        ctx.spawn(self.browse(ctx, command.player, command.args[0] if command.args else None))

    async def help(self, ctx, command):
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
        self.reply(ctx, tx, command.player['name'], self.help_text(ctx, command.player))
        await tx.save()

    async def shortcut(self, ctx, command, item):
        player = command.player
        if item.admin_only and not self.is_admin(ctx, player):
            tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
            self.reply(ctx, tx, player['name'], f'「{item.name}」只有管理员可以使用。')
            await tx.save()
            return
        if item.cost:
            # 收费项目要等玩家确认，先确认事件，再在后台流程里扣款执行。
            await (await ctx.storage.transaction(event=command.event.payload, keys=[])).save()
            ctx.spawn(self.use(ctx, player, item))
            return
        key = cooldown_key(player)
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[key])
        wait = self.cooldown_left(tx.get(key, {}), item)
        if wait:
            self.reply(ctx, tx, player['name'], f'「{item.name}」还要等 {wait} 秒才能再用。')
        else:
            self.execute(ctx, tx, player, item, f'已执行「{item.name}」。')
        await tx.save()

    def help_text(self, ctx, player) -> str:
        config = ctx.config
        names = [command_name(name) for name in config.menu_commands]
        lines = ['本服可用命令：', f'!{names[0]} — 打开{config.title}']
        lines += [f'!{command_name(item.trigger)} — {item.name}' for item in self.visible(ctx, player)
                  if item.trigger.strip()]
        for option in fields(HelpPlugins):
            if getattr(config.help_plugins, option.name):
                lines += KNOWN_COMMANDS[option.name]
        lines += config.help_extra
        return '\n'.join(lines)

    # ---- 菜单流程（在 ctx.spawn 里运行） ----

    async def browse(self, ctx, player, group=None):
        items = self.visible(ctx, player)
        if not items:
            await menu.say(ctx, player, '菜单里还没有你能用的项目。')
            return
        groups = list(dict.fromkeys(item.group.strip() for item in items if item.group.strip()))
        title = ctx.config.title
        if group is not None:
            if group not in groups:
                await menu.say(ctx, player, f'没有「{group}」这个分组。')
                return
        elif groups:
            loose = [item for item in items if not item.group.strip()]
            index = await self.pick(ctx, player, title, [f'[{name}]' for name in groups] +
                                    [self.label(item) for item in loose])
            if index is None:
                return
            if index >= len(groups):
                await self.use(ctx, player, loose[index - len(groups)])
                return
            group = groups[index]
        if group is not None:
            items = [item for item in items if item.group.strip() == group]
            title = f'{title} - {group}'
        index = await self.pick(ctx, player, title, [self.label(item) for item in items])
        if index is not None:
            await self.use(ctx, player, items[index])

    def label(self, item) -> str:
        text = item.name + (f' — {item.cost} 金币' if item.cost else '')
        return text + (f'（!{command_name(item.trigger)}）' if item.trigger.strip() else '')

    async def pick(self, ctx, player, title, options) -> int | None:
        """编号菜单，条目多时分页；编号全局连续，任何一页都能直接输入编号。"""
        size = ctx.config.page_size
        pages = math.ceil(len(options) / size)
        page, attempts, error = 0, 0, ''
        while attempts < MAX_ATTEMPTS:
            start = page * size
            if pages == 1:
                head, hint = title, f'输入编号 1-{len(options)} 选择，输入 0 取消。'
            else:
                head, hint = f'{title}（第 {page + 1}/{pages} 页）', '输入编号选择，+ 下一页，- 上一页，0 取消。'
            lines = [error] if error else []
            lines += [head, *(f'{start + n}. {option}' for n, option in enumerate(options[start:start + size], 1)), hint]
            text = await menu.ask(ctx, player, '\n'.join(lines))
            if text is None:
                return None
            word = text.lower()
            if word in NEXT_WORDS or word in PREVIOUS_WORDS:
                page = (page + (1 if word in NEXT_WORDS else -1)) % pages
                error = ''
                continue
            if text.isdigit() and 1 <= int(text) <= len(options):
                return int(text) - 1
            attempts += 1
            error = f'没有这个选项，请输入 1-{len(options)} 之间的编号。'
        await menu.say(ctx, player, '输入错误次数过多，菜单已关闭。')
        return None

    async def use(self, ctx, player, item):
        """菜单选中或收费快捷指令：检查冷却，收费项目确认并扣款，然后执行。"""
        key = cooldown_key(player)
        wait = self.cooldown_left(await ctx.storage.get(key, {}), item)
        if wait:
            await menu.say(ctx, player, f'「{item.name}」还要等 {wait} 秒才能再用。')
            return
        done, request_id = f'已执行「{item.name}」。', None
        if item.cost:
            installed, balance, coin = await self.wallet(ctx, player)
            if not installed:
                await menu.say(ctx, player, f'本服没有启用经济插件，「{item.name}」暂时不能使用。')
                return
            have = '' if balance is None else f'（你有 {balance} {coin}）'
            if not await menu.confirm(ctx, player, f'「{item.name}」需要 {item.cost} {coin}{have}，确认吗？'):
                return
            arguments = {'player_id': player_id(player), 'amount': item.cost,
                         'reason': item.name[:32], 'request_id': uuid.uuid4().hex}
            try:
                result = await self.charge(ctx, arguments)
            except IPCRejected:
                await menu.say(ctx, player, f'本服没有启用经济插件，「{item.name}」暂时不能使用。')
                return
            except ServiceRejected as error:
                if error.code == 'player_offline':
                    return  # 玩家在确认后下线，没有扣款。
                if error.code == 'unavailable':
                    await menu.say(ctx, player, UNCERTAIN_CHARGE)
                    return
                if error.code != 'insufficient_funds':
                    raise
                _, balance, coin = await self.wallet(ctx, player)
                await menu.say(ctx, player, f'{coin}不足，需要 {item.cost} {coin}。' if balance is None
                               else f'{coin}不足，需要 {item.cost} {coin}，你现在有 {balance} {coin}。')
                return
            except UNSETTLED:
                await menu.say(ctx, player, UNCERTAIN_CHARGE)
                return
            # duplicate=True 表示这笔扣款之前已经成功，同样执行。
            coin, request_id = result.get('currency', coin), arguments['request_id']
            done = f'已执行「{item.name}」，花费 {item.cost} {coin}，剩余 {result["balance"]} {coin}。'
        async with self.lock:
            tx = await ctx.storage.transaction(keys=[key])
            self.execute(ctx, tx, player, item, done)
            try:
                await tx.save()
                return
            except CommitUncertain:
                # 收费项目已经扣款，不退款；日志里的 request_id 对应经济插件流水，供管理员核对。
                ctx.log.warning('menu execution uncertain player=%s item=%s request_id=%s',
                                player['name'], item.name, request_id)
        await menu.say(ctx, player, '执行结果不确定，请联系管理员。' if request_id
                       else '执行结果不确定，请稍后查看效果再决定是否重试。')

    # ---- 执行、冷却与经济 ----

    def execute(self, ctx, tx, player, item, text):
        """把命令、冷却记录和回复放进同一个存储提交。"""
        key = cooldown_key(player)
        target = player_target(player['name'])
        for index, line in enumerate(item_commands(item, target)):
            tx.action(ctx.commands.prepare(line, idempotency_key=f'{tx.commit_id}_cmd_{index}',
                                           deadline=deadline()))
        now = self.now()
        record = {name: until for name, until in tx.get(key, {}).items() if until > now}
        if item.cooldown_seconds:
            record[item.name] = math.ceil(now) + item.cooldown_seconds
        if record:
            tx.set(key, record)
        elif tx.get(key) is not None:
            tx.delete(key)
        self.reply(ctx, tx, player['name'], item.message or text)

    def cooldown_left(self, record, item) -> int:
        if not item.cooldown_seconds:
            return 0
        return max(0, math.ceil(record.get(item.name, 0) - self.now()))

    async def charge(self, ctx, arguments):
        """扣款；结果不明（含经济插件回 unavailable）时用同一个 request_id 再试一次。"""
        for retry in (False, True):
            try:
                return await ctx.services.call(ECONOMY + 'pay', arguments, timeout=5)
            except ServiceRejected as error:
                if error.code != 'unavailable' or retry:
                    raise
            except UNSETTLED:
                if retry:
                    raise

    async def wallet(self, ctx, player):
        """(是否装了经济插件, 余额或 None, 货币名)。查询失败时仍按已安装处理，扣款时再判断。"""
        try:
            result = await ctx.services.call(ECONOMY + 'balance', {'player_id': player_id(player)}, timeout=5)
        except IPCRejected:
            return False, None, '金币'
        except (ServiceRejected, *UNSETTLED):
            return True, None, '金币'
        return True, result['balance'], result.get('currency', '金币')

    def reply(self, ctx, tx, name, text):
        plan = ctx.notifications.prepare(name, text, idempotency_key=tx.commit_id + '_reply',
                                         deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)


if __name__ == '__main__':
    Menu().run()
