"""在线时长：每分钟给在线玩家记 1 分钟，当天在线满指定分钟数发放奖励，并维护总时长排行。

一轮统计只做一次存储提交（人多时每 255 人一次）：玩家记录、排行、奖励命令和
私聊一起写入，要么全部生效，要么全部不生效。提交结果不明时只记日志，下一轮
重新读取：Host 对整个安装做全局版本校验，同一份快照不可能提交两次，所以不会
重复记时或重复发奖。定时器和聊天命令共用一把锁，避免两边读到同一版本互相冲突。

金币交给可选的经济插件，在提交前调用。request_id 由日期和当天第几次决定，
本轮没能保存时下一轮重算同一分钟，经济插件按它去重，不会重复发放。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ServiceRejected, player_target)
from neomega_runtime.managed import IPCRejected
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout
from neomega_runtime.storage import CommitUncertain

BATCH = 255  # 每批玩家数；再加上排行键，正好是一次最多读取的 256 个键。
TOP = 10


@dataclass(frozen=True)
class Reward:
    minutes: int = field(metadata={'title': '当天在线分钟数', 'minimum': 1})
    commands: list[str] = field(default_factory=list, metadata={
        'title': '奖励命令', 'description': '{player} 会替换为获奖的玩家'})
    message: str = field(default='', metadata={'title': '奖励提示', 'description': '私聊发给获奖的玩家'})


@dataclass(frozen=True)
class Settings:
    utc_offset_hours: int = field(default=8, metadata={
        'title': '时区', 'description': '与 UTC 相差的小时数，北京时间填 8', 'minimum': -12, 'maximum': 14})
    rewards: list[Reward] = field(default_factory=lambda: [
        Reward(60, ['give {player} bread 5'], '今天已在线 1 小时，送你 5 个面包！'),
    ], metadata={'title': '每日在线奖励', 'description': '当天在线时长正好达到设定分钟数时发放一次'})
    coin_interval_minutes: int = field(default=30, metadata={
        'title': '金币间隔分钟数', 'description': '当天在线每满这么多分钟发一次金币', 'minimum': 1})
    coins_per_interval: int = field(default=5, metadata={
        'title': '每次金币', 'description': '需要安装经济插件，没装时不发；填 0 不发金币', 'minimum': 0})
    daily_coin_limit: int = field(default=0, metadata={
        'title': '每日金币上限', 'description': '当天最多发这么多金币，最后一次按剩余额度发；填 0 不限', 'minimum': 0})

    def __post_init__(self):
        if any(not command.strip() for reward in self.rewards for command in reward.commands):
            raise ValueError('reward commands must not be empty')
        if len({reward.minutes for reward in self.rewards}) != len(self.rewards):
            raise ValueError('reward minutes must be unique')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def duration(minutes: int) -> str:
    hours, minutes = divmod(minutes, 60)
    if not hours:
        return f'{minutes} 分钟'
    return f'{hours} 小时' + (f' {minutes} 分钟' if minutes else '')


class OnlineTime(Plugin):
    config_type = Settings

    async def on_start(self, ctx: PluginContext):
        self.lock = asyncio.Lock()
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('在线时长', aliases=('zxsc', 'online'), description='查看今日和累计在线时长')(
                lambda command: self.info(ctx, command))
            router.command('在线排行', aliases=('zxph',), description='查看累计在线时长前 10 名')(
                lambda command: self.ranking(ctx, command))
        ctx.every(60, lambda: self.tick(ctx), name='online-time')

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

    async def tick(self, ctx):
        async with self.lock:
            try:
                robot = (await ctx.robot())['bot_name']
                online = {player_id(p): p for p in await ctx.players() if p['name'] != robot}
                ids = list(online)
                for start in range(0, len(ids), BATCH):
                    await self.count(ctx, [online[i] for i in ids[start:start + BATCH]])
            except (CommitUncertain, IPCRejected) as exc:
                ctx.log.warning('在线时长本轮未能保存，下一轮重新统计：%s', exc)

    async def count(self, ctx, players):
        tx = await ctx.storage.transaction(keys=['player:' + player_id(p) for p in players] + ['top'])
        today, number, config, grants = self.today(ctx).isoformat(), 0, ctx.config, []
        top = {entry['id']: entry for entry in tx.get('top', [])}
        for player in players:
            key = 'player:' + player_id(player)
            record = tx.get(key, {'name': player['name'], 'total': 0, 'day': today, 'today': 0})
            same_day = record['day'] == today
            minutes = (record['today'] if same_day else 0) + 1
            record = {'name': player['name'], 'total': record['total'] + 1, 'day': today, 'today': minutes,
                      'coins_today': record.get('coins_today', 0) if same_day else 0}
            tx.set(key, record)
            top[player_id(player)] = {'id': player_id(player), 'name': player['name'], 'minutes': record['total']}

            coins = config.coins_per_interval
            if config.daily_coin_limit:
                coins = min(coins, config.daily_coin_limit - record['coins_today'])
            if coins > 0 and minutes % config.coin_interval_minutes == 0:
                grants.append((player, record, coins, f'online-{today}-{minutes // config.coin_interval_minutes}'))

            rewards = [reward for reward in config.rewards if reward.minutes == minutes]
            if not rewards:
                continue
            try:
                target = player_target(player['name'])
            except ValueError:
                # 名字无法安全写进命令时只记时长；否则每轮都会在这里失败，所有人都停止计时。
                ctx.log.warning('玩家名不能用于命令，跳过在线奖励：%r', player['name'])
                continue
            for reward in rewards:
                for line in reward.commands:
                    tx.action(ctx.commands.prepare(line.strip().removeprefix('/').replace('{player}', target),
                                                   idempotency_key=f'{tx.commit_id}_reward_{number}',
                                                   deadline=deadline()))
                    number += 1
                if reward.message:
                    self.tell(ctx, tx, player, reward.message, f'{tx.commit_id}_message_{number}')
                    number += 1

        results = await asyncio.gather(*(self.grant(ctx, player, coins, request_id)
                                         for player, _, coins, request_id in grants))
        for (player, record, _, _), (given, text) in zip(grants, results):
            if given:
                tx.set('player:' + player_id(player), {**record, 'coins_today': record['coins_today'] + given})
            if text:
                self.tell(ctx, tx, player, f'今天已在线 {duration(record["today"])}，{text}',
                          f'{tx.commit_id}_message_{number}')
                number += 1
        tx.set('top', sorted(top.values(), key=lambda entry: -entry['minutes'])[:TOP])
        await tx.save()

    async def info(self, ctx, command):
        player, key = command.player, 'player:' + player_id(command.player)
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[key])
        record = tx.get(key, {'total': 0, 'day': '', 'today': 0})
        today = record['today'] if record['day'] == self.today(ctx).isoformat() else 0
        self.tell(ctx, tx, player, f'今天在线 {duration(today)}，累计 {duration(record["total"])}。',
                  tx.commit_id + '_reply')
        await tx.save()

    async def ranking(self, ctx, command):
        tx = await ctx.storage.transaction(event=command.event.payload, keys=['top'])
        top = tx.get('top', [])
        lines = ['在线时长排行：'] + [f'{rank}. {entry["name"]}  {duration(entry["minutes"])}'
                                      for rank, entry in enumerate(top, 1)]
        self.tell(ctx, tx, command.player, '\n'.join(lines) if top else '还没有人上榜，多在线一会儿吧。',
                  tx.commit_id + '_reply')
        await tx.save()

    async def grant(self, ctx, player, coins, request_id) -> tuple[int, str]:
        """返回计入今日上限的金币数和提示；结果不明时也计入，保证不超过上限。"""
        arguments = {'player_id': player_id(player), 'amount': coins, 'reason': '在线奖励', 'request_id': request_id}
        try:
            result = await ctx.services.call('plugin.neomega.economy.grant', arguments, timeout=5)
        except IPCRejected:
            return 0, ''  # 没有安装或没有启动经济插件。
        except (ServiceWaitTimeout, ServiceUncertain):
            return coins, '金币奖励结果暂时不确定，可以用 !流水 查看。'
        except ServiceRejected as error:
            if error.code == 'unavailable':  # 经济插件已记账，只是没等到计分板回应。
                return coins, '金币奖励结果暂时不确定，可以用 !流水 查看。'
            return 0, '金币奖励暂时没有发出。'
        except ServiceFailed:
            return 0, '金币奖励暂时没有发出。'
        currency = result['currency']
        if result['pending']:
            return coins, f'获得 {coins} {currency}，下次上线时到账。'
        return coins, f'获得 {coins} {currency}，现有 {result["balance"]} {currency}。'

    def tell(self, ctx, tx, player, text, key):
        plan = ctx.notifications.prepare(player['name'], text, idempotency_key=key, deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)


if __name__ == '__main__':
    OnlineTime().run()
