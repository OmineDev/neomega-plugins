"""玩家互传：!tpa 请求传送到对方身边，!tpahere 请求把对方拉过来，对方 !同意 后执行 tp。

请求和冷却只保存在内存里，插件重启后清空；这个插件不写任何存储键。
每条命令只做一次存储提交：tp 命令、给双方的消息和事件确认一起提交给 Host。
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import math
import time

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, player_target)

GO, BRING = 'go', 'bring'   # 发起人去对方身边 / 把对方拉到发起人身边


@dataclass(frozen=True)
class Settings:
    request_timeout_seconds: int = field(default=60, metadata={
        'title': '请求有效时间（秒）', 'description': '超过这个时间没有同意，请求自动失效', 'minimum': 5})
    cooldown_seconds: int = field(default=10, metadata={
        'title': '发起冷却（秒）', 'description': '同一玩家两次发起请求之间至少间隔的秒数，填 0 不限制',
        'minimum': 0})


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def match(names, query):
    """完全相同的名字优先，否则取唯一的不区分大小写前缀匹配；返回匹配到的名字列表。"""
    if query in names:
        return [query]
    return [name for name in names if name.casefold().startswith(query.casefold())]


class Tpa(Plugin):
    config_type = Settings

    async def on_start(self, ctx: PluginContext):
        # 被请求人 -> {发起人: (类型, 过期时间)}，字典顺序即发起顺序，最后一条最新。
        self.requests: dict[str, dict[str, tuple[str, float]]] = {}
        self.last_sent: dict[str, float] = {}
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('tpa', description='请求传送到某位玩家身边',
                           usage='<玩家>', min_args=1, max_args=1)(
                lambda command: self.ask(ctx, command, GO))
            router.command('tpahere', description='请求把某位玩家拉到身边',
                           usage='<玩家>', min_args=1, max_args=1)(
                lambda command: self.ask(ctx, command, BRING))
            router.command('同意', aliases=('tpaccept', 'ty'), description='同意传送请求',
                           usage='[玩家]', max_args=1)(
                lambda command: self.answer(ctx, command, True))
            router.command('拒绝', aliases=('tpdeny', 'jj'), description='拒绝传送请求',
                           usage='[玩家]', max_args=1)(
                lambda command: self.answer(ctx, command, False))

    async def on_event(self, ctx: PluginContext, event: Event):
        if event.kind == 'chat.received':
            try:
                for router in self.routers:
                    if await router.dispatch(event):
                        return
            except (ValueError, PlayerNotFound, AmbiguousPlayer):
                # 引号不配对、参数个数不对，或玩家已经下线：不回复，只确认事件。
                pass
        await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    def now(self) -> float:
        return time.monotonic()

    def pending(self, name):
        """name 收到的未过期请求；顺手清掉过期的。"""
        now = self.now()
        for target in list(self.requests):
            alive = {k: v for k, v in self.requests[target].items() if v[1] > now}
            if alive:
                self.requests[target] = alive
            else:
                del self.requests[target]
        return self.requests.get(name, {})

    async def ask(self, ctx, command, kind):
        me = command.player['name']
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
        wait = self.last_sent.get(me, -math.inf) + ctx.config.cooldown_seconds - self.now()
        names = [player['name'] for player in await ctx.players()] if wait <= 0 else []
        found = match(names, command.args[0])
        if wait > 0:
            self.reply(ctx, tx, me, f'请求太频繁了，请 {math.ceil(wait)} 秒后再试。')
        elif not found:
            self.reply(ctx, tx, me, f'没有找到在线玩家“{command.args[0]}”。')
        elif len(found) > 1:
            self.reply(ctx, tx, me, f'有多位玩家匹配“{command.args[0]}”：{"、".join(found)}，请输入更完整的名字。')
        elif found[0] == me:
            self.reply(ctx, tx, me, '不能向自己发送传送请求。')
        else:
            target, seconds = found[0], ctx.config.request_timeout_seconds
            want = '想传送到你身边' if kind == GO else '想把你拉到身边'
            self.reply(ctx, tx, me, f'已向 {target} 发送请求，等待对方同意（{seconds} 秒内有效）。')
            self.reply(ctx, tx, target, f'{me} {want}，发送 !同意 接受，!拒绝 拒绝（{seconds} 秒内有效）。',
                       'notify')
            await tx.save()
            requests = self.pending(target)
            requests.pop(me, None)   # 新请求替换旧请求，并排到最新
            requests[me] = (kind, self.now() + seconds)
            self.requests[target] = requests
            self.last_sent[me] = self.now()
            self.last_sent = {k: v for k, v in self.last_sent.items()
                              if v + ctx.config.cooldown_seconds > self.now()}
            return
        await tx.save()

    async def answer(self, ctx, command, accept):
        me = command.player['name']
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
        requests = self.pending(me)
        found = match(list(requests), command.args[0]) if command.args else list(requests)[-1:]
        if not found:
            self.reply(ctx, tx, me, f'没有来自“{command.args[0]}”的传送请求。' if command.args
                       else '你没有待处理的传送请求。')
        elif len(found) > 1:
            self.reply(ctx, tx, me, f'有多位玩家的请求匹配“{command.args[0]}”：{"、".join(found)}，请输入更完整的名字。')
        else:
            sender = found[0]
            kind, _ = requests.pop(sender)
            online = [player['name'] for player in await ctx.players()]
            if sender not in online:
                self.reply(ctx, tx, me, f'{sender} 已经下线，这条请求已失效。')
            elif not accept:
                self.reply(ctx, tx, me, f'已拒绝 {sender} 的传送请求。')
                self.reply(ctx, tx, sender, f'{me} 拒绝了你的传送请求。', 'notify')
            else:
                mover, place = (sender, me) if kind == GO else (me, sender)
                tx.action(ctx.commands.prepare(f'tp {player_target(mover)} {player_target(place)}',
                                               idempotency_key=tx.commit_id + '_tp', deadline=deadline()))
                self.reply(ctx, tx, me, f'已同意 {sender} 的传送请求。')
                self.reply(ctx, tx, sender, f'{me} 同意了你的传送请求。', 'notify')
        await tx.save()

    def reply(self, ctx, tx, name, text, tag='reply'):
        plan = ctx.notifications.prepare(name, text, idempotency_key=f'{tx.commit_id}_{tag}',
                                         deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)


if __name__ == '__main__':
    Tpa().run()
