"""兑换码：服主在设置里配好兑换码，玩家发送 !兑换 <码> 领取奖励。

一次兑换只做一次存储提交：码的总使用次数、玩家的使用次数、奖励命令、回复和
事件确认一起写入，要么全部生效，要么全部不生效。提交带全局 revision 校验，读到
的次数在提交前被别的提交改过就整体拒绝；插件内再用一把锁让兑换逐个处理，所以
总次数和每人次数都不会超发。

提交结果不明时记日志、提醒玩家，再把异常上抛让插件重启：已生效的提交连同事件
确认一起落盘，不会再投递；未生效的事件会重新投递并照常处理。

金币交给可选的经济插件，在提交前调用。request_id 由兑换码和玩家第几次兑换决定，
事件重新投递时经济插件按它去重，不会重复发放。没装经济插件时不发金币。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import re
import unicodedata
import uuid

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ServiceRejected, player_target)
from neomega_runtime.managed import IPCRejected
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from neomega_runtime.storage import CommitUncertain

USAGE = '用法：!兑换 <兑换码>，例如 !兑换 ABC123'


@dataclass(frozen=True)
class Code:
    code: str = field(metadata={
        'title': '兑换码', 'description': '玩家输入时不区分大小写；只能用字母、数字、汉字、- 和 _，最长 32 个字'})
    commands: list[str] = field(default_factory=list, metadata={
        'title': '奖励命令', 'description': '{player} 会替换为兑换的玩家'})
    coins: int = field(default=0, metadata={
        'title': '金币', 'description': '需要安装经济插件，没装时不发；填 0 不发金币', 'minimum': 0})
    total_uses: int = field(default=0, metadata={
        'title': '总可用次数', 'description': '所有玩家合计能兑换几次，填 0 不限', 'minimum': 0})
    per_player_uses: int = field(default=1, metadata={
        'title': '每人可用次数', 'description': '同一个玩家能兑换几次，填 0 不限', 'minimum': 0})
    expires: str = field(default='', metadata={
        'title': '过期日期', 'description': '例如 2026-12-31，这一天过完后失效；留空则不过期'})
    message: str = field(default='', metadata={
        'title': '兑换成功提示', 'description': '留空时显示“兑换成功！”'})
    note: str = field(default='', metadata={'title': '备注', 'description': '只给自己看，玩家看不到'})


@dataclass(frozen=True)
class Settings:
    utc_offset_hours: int = field(default=8, metadata={
        'title': '时区', 'description': '与 UTC 相差的小时数，北京时间填 8', 'minimum': -12, 'maximum': 14})
    codes: list[Code] = field(default_factory=list, metadata={
        'title': '兑换码', 'description': '每个码写奖励和次数限制；码不能重复（不区分大小写）'})

    def __post_init__(self):
        if len({normalize(code.code) for code in self.codes}) != len(self.codes):
            raise ValueError('codes must be unique ignoring case')
        for code in self.codes:
            if not valid_code(code.code):
                raise ValueError('invalid code text')
            if any(not command.strip() for command in code.commands):
                raise ValueError('reward commands must not be empty')
            if not code.commands and not code.coins:
                raise ValueError('a code needs a reward command or coins')
            if code.expires and not valid_date(code.expires):
                raise ValueError('expiry must be a YYYY-MM-DD date')


def normalize(text: str) -> str:
    return text.strip().casefold()


def valid_code(text: str) -> bool:
    return (1 <= len(text) <= 32
            and all(c in '-_' or unicodedata.category(c)[0] in 'LN' for c in text))


def valid_date(text: str) -> bool:
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', text):
        return False
    try:
        date.fromisoformat(text)
    except ValueError:
        return False
    return True


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


class RedeemCodes(Plugin):
    config_type = Settings

    async def on_start(self, ctx: PluginContext):
        # Host 逐个投递事件；锁让并发调用同样逐个处理，配合存储的 revision 校验不超发。
        self.lock = asyncio.Lock()
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('兑换', aliases=('cdk', 'CDK'), description='使用兑换码领取奖励')(
                lambda command: self.redeem(ctx, command))

    async def on_event(self, ctx: PluginContext, event: Event):
        async with self.lock:
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

    async def redeem(self, ctx, command):
        player = command.player
        if len(command.args) != 1 or not normalize(command.args[0]):
            return await self.answer(ctx, command, USAGE)
        key = normalize(command.args[0])
        entry = next((code for code in ctx.config.codes if normalize(code.code) == key), None)
        if entry is None:
            return await self.answer(ctx, command, '兑换码不存在，请检查是否输错。')
        if entry.expires and self.today(ctx).isoformat() > entry.expires:
            return await self.answer(ctx, command, f'这个兑换码已经过期了（有效期到 {entry.expires}）。')

        code_key, use_key = 'code:' + key, f'use:{key}:{player_id(player)}'
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[code_key, use_key])
        used, mine = tx.get(code_key, 0), tx.get(use_key, 0)
        limit = entry.per_player_uses
        if limit and mine >= limit:
            self.reply(ctx, tx, player, '你已经领过这个兑换码了。' if limit == 1
                       else f'这个兑换码每人只能用 {limit} 次，你已经用完了。')
            await tx.save()
            return
        if entry.total_uses and used >= entry.total_uses:
            self.reply(ctx, tx, player, '这个兑换码已经被领完了。')
            await tx.save()
            return

        mine += 1
        tx.set(code_key, used + 1)
        tx.set(use_key, mine)
        lines = [entry.message or '兑换成功！']
        if entry.coins:
            lines += await self.grant(ctx, player, entry, key, mine)
        if limit > 1 and mine < limit:
            lines.append(f'这个兑换码你还能再用 {limit - mine} 次。')

        target = player_target(player['name'])
        for index, line in enumerate(entry.commands):
            tx.action(ctx.commands.prepare(line.strip().removeprefix('/').replace('{player}', target),
                                           idempotency_key=f'{tx.commit_id}_reward_{index}',
                                           deadline=deadline()))
        self.reply(ctx, tx, player, '\n'.join(lines))
        try:
            await tx.save()
        except CommitUncertain:
            # 事件确认和这次提交绑在一起，重启后要么已生效、要么重新投递，不会重复发放。
            ctx.log.warning('redeem commit uncertain player=%s code=%s commit_id=%s',
                            player['name'], key, tx.commit_id)
            await self.notify(ctx, player, '兑换结果暂时不确定，请稍等片刻看看是否收到奖励；没有收到请联系管理员。')
            raise
        # 奖励命令由 Host 在提交后执行；日志供管理员核对和补发。
        ctx.log.info('redeemed player=%s code=%s use=%d commit_id=%s', player['name'], key, mine, tx.commit_id)

    async def answer(self, ctx, command, text):
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
        self.reply(ctx, tx, command.player, text)
        await tx.save()

    async def grant(self, ctx, player, entry, key, use) -> list[str]:
        arguments = {'player_id': player_id(player), 'amount': entry.coins,
                     'reason': ('兑换码 ' + entry.code)[:32], 'request_id': f'cdk-{use}-{key}'[:64]}
        try:
            result = await ctx.services.call('plugin.neomega.economy.grant', arguments, timeout=5)
        except IPCRejected:
            return []  # 没有安装或没有启动经济插件。
        except (ServiceWaitTimeout, ServiceUncertain):
            return ['金币奖励结果暂时不确定，可以用 !流水 查看。']
        except ServiceRejected as error:
            if error.code == 'unavailable':  # 经济插件已记账，只是没等到计分板回应。
                return ['金币奖励结果暂时不确定，可以用 !流水 查看。']
            return ['金币奖励暂时没有发出。']
        except ServiceFailed:
            return ['金币奖励暂时没有发出。']
        coin = result.get('currency', '金币')  # 经济插件 2.0.0 不返回货币名
        return [f'获得 {entry.coins} {coin}，现有 {result["balance"]} {coin}。']

    def reply(self, ctx, tx, player, text):
        plan = ctx.notifications.prepare(player['name'], text,
                                         idempotency_key=tx.commit_id + '_reply', deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)

    async def notify(self, ctx, player, text):
        """提交之外的私聊，只用于提醒结果不明；发不出去不影响后续重启处理。"""
        plan = ctx.notifications.prepare(player['name'], text,
                                         idempotency_key='notice_' + uuid.uuid4().hex, deadline=deadline())
        try:
            for part in plan.parts:
                await ctx.operations.submit(part.intent)
        except Exception:
            ctx.log.warning('redeem notice not sent player=%s', player['name'])


if __name__ == '__main__':
    RedeemCodes().run()
