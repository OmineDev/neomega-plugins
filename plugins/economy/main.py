"""经济：整数余额、单一货币。其他插件通过服务发奖和扣款，玩家可以查余额、转账、看流水。

每个账户一个键，只保留最近 20 条收支：它既是玩家能查的流水，也是去重窗口。
调用方用同一个 request_id 重试时直接返回当前余额，不会重复记账。
服务处理器和聊天命令都会提交存储，共用一把锁串行化；每次处理只提交一次。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ServiceRejected, service)
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout

RECENT = 20
UNAVAILABLE = '暂时查不到玩家，请稍后再试'


@dataclass(frozen=True)
class Settings:
    currency_name: str = field(default='金币', metadata={'title': '货币名称'})
    starting_balance: int = field(default=0, metadata={
        'title': '初始余额', 'description': '玩家账户第一次有收支时的金额', 'minimum': 0})
    allow_transfer: bool = field(default=True, metadata={
        'title': '允许玩家转账', 'description': '关闭后玩家不能用 !转账，其他插件的发奖和扣款不受影响'})


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def seen(record, request_id) -> bool:
    return any(entry['id'] == request_id for entry in record['recent'])


def post(record, entry_id, delta, reason):
    record['balance'] += delta
    entry = {'id': entry_id, 'delta': delta, 'reason': reason,
             'at': datetime.now(timezone.utc).isoformat()}
    record['recent'] = [*record['recent'], entry][-RECENT:]


class Economy(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()

    async def on_start(self, ctx: PluginContext):
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('余额', aliases=('ye', 'balance'), description='查看余额')(
                lambda command: self.show_balance(ctx, command))
            router.command('转账', aliases=('zz', 'pay'), description='转给其他玩家',
                           usage='<玩家> <数量>', min_args=2, max_args=2)(
                lambda command: self.transfer_command(ctx, command))
            router.command('流水', aliases=('ls',), description='查看最近 10 条收支')(
                lambda command: self.history(ctx, command))

    async def on_event(self, ctx: PluginContext, event: Event):
        if event.kind == 'chat.received':
            try:
                for router in self.routers:
                    if await router.dispatch(event):
                        return
            except (ValueError, PlayerNotFound, AmbiguousPlayer):
                # 引号不配对、参数个数不对，或玩家已经下线：不回复，只确认事件。
                pass
        async with self.lock:
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    def account(self, ctx, tx, pid):
        return tx.get('a:' + pid, {'balance': ctx.config.starting_balance, 'recent': []})

    def move(self, tx, ids, records, amount, entry_id, reasons):
        for pid, record, delta, reason in zip(ids, records, (-amount, amount), reasons):
            post(record, entry_id, delta, reason)
            tx.set('a:' + pid, record)

    # ---- 服务 ----

    @service('balance', major=1)
    async def balance(self, ctx, args):
        tx = await ctx.storage.transaction(keys=['a:' + args['player_id']])
        return {'balance': self.account(ctx, tx, args['player_id'])['balance']}

    @service('grant', major=1)
    async def grant(self, ctx, args):
        return await self.change(ctx, args, args['amount'])

    @service('pay', major=1)
    async def pay(self, ctx, args):
        return await self.change(ctx, args, -args['amount'])

    async def change(self, ctx, args, delta):
        key = 'a:' + args['player_id']
        async with self.lock:
            tx = await ctx.storage.transaction(keys=[key])
            record = self.account(ctx, tx, args['player_id'])
            if seen(record, args['request_id']):
                return {'balance': record['balance'], 'duplicate': True}
            if record['balance'] + delta < 0:
                raise ServiceRejected('insufficient_funds')
            post(record, args['request_id'], delta, args['reason'])
            tx.set(key, record)
            await tx.save()
        return {'balance': record['balance'], 'duplicate': False}

    @service('transfer', major=1)
    async def transfer(self, ctx, args):
        ids = (args['from_id'], args['to_id'])
        if ids[0] == ids[1]:
            raise ServiceRejected('invalid_argument')
        async with self.lock:
            tx = await ctx.storage.transaction(keys=['a:' + pid for pid in ids])
            records = [self.account(ctx, tx, pid) for pid in ids]
            if any(seen(record, args['request_id']) for record in records):
                return {'balance': records[0]['balance'], 'duplicate': True}
            if records[0]['balance'] < args['amount']:
                raise ServiceRejected('insufficient_funds')
            self.move(tx, ids, records, args['amount'], args['request_id'], (args['reason'],) * 2)
            await tx.save()
        return {'balance': records[0]['balance'], 'duplicate': False}

    # ---- 玩家命令 ----

    async def show_balance(self, ctx, command):
        pid = player_id(command.player)
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=['a:' + pid])
            balance = self.account(ctx, tx, pid)['balance']
            self.tell(ctx, tx, command.player['name'], f'你有 {balance} {ctx.config.currency_name}')
            await tx.save()

    async def history(self, ctx, command):
        pid = player_id(command.player)
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=['a:' + pid])
            recent = self.account(ctx, tx, pid)['recent'][-10:][::-1]
            lines = [f'{entry["delta"]:+d} {entry["reason"]}' for entry in recent]
            text = '最近的收支：\n' + '\n'.join(lines) if lines else '还没有收支记录。'
            self.tell(ctx, tx, command.player['name'], text)
            await tx.save()

    async def transfer_command(self, ctx, command):
        name, amount = command.args
        if not ctx.config.allow_transfer:
            return await self.answer(ctx, command, '服务器已关闭转账')
        if not amount.isdecimal() or int(amount) < 1:
            return await self.answer(ctx, command, '用法：!转账 <玩家> <数量>，数量是正整数')
        amount = int(amount)
        try:
            target_id, target_name = await self.find(ctx, name)
        except ServiceRejected as error:
            return await self.answer(ctx, command, f'找不到玩家 {name}' if error.code == 'not_found' else UNAVAILABLE)
        except (ServiceFailed, ServiceWaitTimeout, ServiceUncertain):
            return await self.answer(ctx, command, UNAVAILABLE)
        payer, coin = command.player, ctx.config.currency_name
        ids = (player_id(payer), target_id)
        if ids[0] == ids[1]:
            return await self.answer(ctx, command, '不能转账给自己')

        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=['a:' + pid for pid in ids])
            records = [self.account(ctx, tx, pid) for pid in ids]
            if records[0]['balance'] < amount:
                self.tell(ctx, tx, payer['name'], f'{coin}不足，你只有 {records[0]["balance"]} {coin}')
            else:
                self.move(tx, ids, records, amount, tx.commit_id,
                          (f'转给 {target_name}', f'收到 {payer["name"]} 的转账'))
                self.tell(ctx, tx, payer['name'],
                          f'已转给 {target_name} {amount} {coin}，剩余 {records[0]["balance"]} {coin}')
                # 收款方不在线时，这条私聊会被丢弃。
                self.tell(ctx, tx, target_name,
                          f'{payer["name"]} 转给你 {amount} {coin}，现有 {records[1]["balance"]} {coin}', 'payee')
            await tx.save()

    async def find(self, ctx, name):
        """先在在线玩家里精确匹配名字，再请玩家档案按名字查离线玩家。"""
        for player in await ctx.players():
            if player['name'] == name:
                return player_id(player), player['name']
        found = await ctx.services.call('plugin.neomega.players.resolve', {'name': name}, timeout=5)
        return found['id'], found['name']

    async def answer(self, ctx, command, text):
        async with self.lock:
            tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
            self.tell(ctx, tx, command.player['name'], text)
            await tx.save()

    def tell(self, ctx, tx, name, text, tag='reply'):
        plan = ctx.notifications.prepare(name, text, idempotency_key=f'{tx.commit_id}_{tag}',
                                         deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)


if __name__ == '__main__':
    Economy().run()
