"""经济：游戏计分板就是余额。其他插件通过服务发奖和扣款，玩家可以查余额、转账、看流水。

计分板只能读写在线玩家，所以账本只记两样：最近 20 条收支（玩家能查的流水，也是
request_id 的去重窗口）和离线期间的待入账。加减分和它的流水在同一次提交里落盘，
再等服务器回应：失败时收入改记待入账、扣款撤销；结果不明时不重放，按已成功处理。
服务、命令和进服入账共用一把锁串行化。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import re
from uuid import uuid4

from neomega_runtime import (AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext,
                             PlayerNotFound, ServiceRejected, service)
from neomega_runtime.http import ProtocolError
from neomega_runtime.managed import IPCRejected
from neomega_runtime.operations import OperationWaitTimeout
from neomega_runtime.services import ServiceFailed, ServiceUncertain, ServiceWaitTimeout

RECENT = 20
WAIT = 35  # 动作期限 30 秒，多等几秒回应。
LOOKUP_FAILED = '暂时查不到玩家，请稍后再试'
SCORE_FAILED = '暂时查不到余额，请稍后再试'


@dataclass(frozen=True)
class Settings:
    currency_name: str = field(default='金币', metadata={
        'title': '货币名称', 'description': '回复玩家时显示的货币名，也是新建计分板的显示名'})
    objective: str = field(default='coin', metadata={
        'title': '计分板', 'description': '存放余额的计分板 ID，只能用英文字母、数字和 _ . -，最长 16 个字符'})
    create_objective: bool = field(default=True, metadata={
        'title': '自动创建计分板', 'description': '启动时计分板不存在就新建一个'})
    display_slot: str = field(default='', metadata={
        'title': '显示位置', 'description': '留空不显示；sidebar 侧边栏，list 玩家列表，belowname 名字下方'})
    starting_balance: int = field(default=0, metadata={
        'title': '初始余额', 'description': '玩家第一次进服时加到计分板上，每人只发一次', 'minimum': 0})
    allow_transfer: bool = field(default=True, metadata={
        'title': '允许玩家转账', 'description': '关闭后玩家不能用 !转账，其他插件的发奖和扣款不受影响'})

    def __post_init__(self):
        if re.fullmatch(r'[A-Za-z0-9_.-]{1,16}', self.objective) is None:
            raise ValueError('objective must match [A-Za-z0-9_.-]{1,16}')
        if self.display_slot not in ('', 'sidebar', 'list', 'belowname'):
            raise ValueError('display_slot must be empty, sidebar, list or belowname')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def once() -> dict:
    """读取和设置类动作每次都是新动作。"""
    return {'idempotency_key': uuid4().hex, 'deadline': deadline()}


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def account(tx, pid):
    record = tx.get('a:' + pid, {'pending': 0, 'recent': [], 'started': False})
    if 'balance' in record:  # 2.0.0 的记录：余额以计分板为准，不迁移。
        record = {'pending': 0, 'recent': record['recent'], 'started': True}
    return record


def seen(record, request_id) -> bool:
    return any(entry['id'] == request_id for entry in record['recent'])


def post(record, entry_id, delta, reason):
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
        ctx.spawn(self.prepare_objective(ctx), name='objective')

    async def prepare_objective(self, ctx):
        """建计分板、设显示位置。放在后台，不挡事件；失败只记日志。"""
        config, board = ctx.config, ctx.scoreboard
        try:
            if config.create_objective:
                listed = await self.execute(ctx, board.prepare_objective_list(**once()))
                if listed and config.objective not in {o['name'] for o in listed['objectives']}:
                    await self.execute(ctx, board.prepare_objective_add(
                        config.objective, display_name=config.currency_name, **once()))
            if config.display_slot:
                await self.execute(ctx, board.prepare_objective_setdisplay(
                    config.display_slot, config.objective, **once()))
        except (IPCRejected, ProtocolError, TimeoutError, ValueError) as error:
            ctx.log.warning('objective setup failed: %s', type(error).__name__)

    async def on_event(self, ctx: PluginContext, event: Event):
        async with self.lock:
            if event.kind == 'player.list_added' and await self.joined(ctx, event):
                return
            if event.kind == 'chat.received':
                try:
                    for router in self.routers:
                        if await router.dispatch(event):
                            return
                except (ValueError, PlayerNotFound, AmbiguousPlayer):
                    # 引号不配对、参数个数不对，或玩家已经下线：不回复，只确认事件。
                    pass
            await (await ctx.storage.transaction(event=event.payload, keys=[])).save()

    # ---- 计分板 ----

    async def execute(self, ctx, intent):
        """执行一个读取或设置动作，返回服务器回应；没有成功时记日志并返回 None。"""
        try:
            row = await ctx.operations.execute(intent, timeout=WAIT)
        except OperationWaitTimeout:
            row = {'state': 'unknown', 'result': {}}
        if row['state'] == 'succeeded':
            return row['result']['result']
        ctx.log.warning('scoreboard action %s: %s', row['state'], row['result'].get('reason'))
        return None

    async def online(self, ctx, pid):
        """在线玩家的当前名字，就是他在计分板上的 holder；不在线时为 None。"""
        return next((p['name'] for p in await ctx.players() if player_id(p) == pid), None)

    async def score(self, ctx, name):
        found = await self.execute(ctx, ctx.scoreboard.prepare_get(name, ctx.config.objective, **once()))
        if found is None:
            raise ServiceRejected('unavailable')
        return found['value'] if found['found'] else 0

    async def apply(self, ctx, tx, pid, record, name, delta, label):
        """流水和加减分一起提交，等回应，返回 (改了分数, 新分数)。结果不明时不重放，按已改处理。"""
        tx.action(ctx.scoreboard.prepare_add(name, ctx.config.objective, delta,
                                             idempotency_key=tx.commit_id + '_score', deadline=deadline()))
        tx.set('a:' + pid, record)
        receipt = await tx.save()
        try:
            row = await ctx.operations.wait(receipt['operation_ids'][-1], timeout=WAIT)
        except OperationWaitTimeout:
            row = {'state': 'unknown'}
        if row['state'] == 'succeeded':
            return True, row['result']['result']['value']
        if row['state'] in ('failed', 'cancelled'):  # cancelled：没发出就过期了，同样没改分数。
            return False, None
        ctx.log.warning('scoreboard result unknown, treated as applied: %s', label)
        return True, None

    async def amend(self, ctx, pid, change):
        tx = await ctx.storage.transaction(keys=['a:' + pid])
        record = account(tx, pid)
        change(record)
        await tx.set('a:' + pid, record).save()

    async def change(self, ctx, tx, pid, delta, reason, request_id):
        """记一条收支并改分数，返回新余额。离线玩家的收入或加分失败记进待入账，返回 None；
        扣款只对在线玩家，扣分失败（玩家刚好下线）时撤销这条流水并拒绝。"""
        name = await self.online(ctx, pid)
        if delta < 0 and name is None:
            raise ServiceRejected('player_offline')
        if delta < 0 and await self.score(ctx, name) < -delta:
            raise ServiceRejected('insufficient_funds')
        record = account(tx, pid)
        post(record, request_id, delta, reason)
        if name is None:
            record['pending'] += delta
            await tx.set('a:' + pid, record).save()
            return None
        applied, value = await self.apply(ctx, tx, pid, record, name, delta, request_id)
        if not applied and delta > 0:
            await self.amend(ctx, pid, lambda r: r.update(pending=r['pending'] + delta))
            return None
        if not applied:
            await self.amend(ctx, pid, lambda r: r.update(
                recent=[entry for entry in r['recent'] if entry['id'] != request_id]))
            raise ServiceRejected('player_offline')
        return await self.score(ctx, name) if value is None else value

    async def move(self, ctx, ids, amount, reasons, request_id):
        """先扣付款方再给收款方，流水同一个 id；返回 (付款方余额, 收款方余额或 None, 是否重复)。
        两步之间中断时，用同一个 request_id 重试会跳过已扣的款，接着给收款方。"""
        keys = ['a:' + pid for pid in ids]
        tx = await ctx.storage.transaction(keys=keys)
        payer, payee = (account(tx, pid) for pid in ids)
        if seen(payee, request_id):
            return (await self.repeat(ctx, ids[0], income=False))['balance'], None, True
        balance = None
        if not seen(payer, request_id):
            try:
                balance = await self.change(ctx, tx, ids[0], -amount, reasons[0], request_id)
            except ServiceRejected as error:
                # 扣款已经记账、只是回读余额失败：照样给收款方，不能停在一半。
                tx = await ctx.storage.transaction(keys=keys)
                if error.code != 'unavailable' or not seen(account(tx, ids[0]), request_id):
                    raise
        payee = await self.change(ctx, await ctx.storage.transaction(keys=keys[1:]),
                                  ids[1], amount, reasons[1], request_id)
        if balance is None:
            balance = (await self.repeat(ctx, ids[0], income=False))['balance']
        return balance, payee, False

    async def repeat(self, ctx, pid, income):
        """同一个 request_id 再来一次：不记账，只报当前余额。离线玩家的收入当作还在待入账。"""
        name = await self.online(ctx, pid)
        if name is None and not income:
            raise ServiceRejected('player_offline')
        balance = 0 if name is None else await self.score(ctx, name)
        return self.result(ctx, balance, duplicate=True, pending=name is None)

    def result(self, ctx, balance, duplicate=False, pending=False):
        return {'balance': balance, 'duplicate': duplicate, 'pending': pending,
                'currency': ctx.config.currency_name}

    # ---- 服务 ----

    @service('balance', major=1)
    async def balance(self, ctx, args):
        name = await self.online(ctx, args['player_id'])
        if name is None:
            raise ServiceRejected('player_offline')
        return {'balance': await self.score(ctx, name), 'pending': False, 'currency': ctx.config.currency_name}

    @service('grant', major=1)
    async def grant(self, ctx, args):
        return await self.book(ctx, args, args['amount'])

    @service('pay', major=1)
    async def pay(self, ctx, args):
        return await self.book(ctx, args, -args['amount'])

    async def book(self, ctx, args, delta):
        pid, request_id = args['player_id'], args['request_id']
        async with self.lock:
            tx = await ctx.storage.transaction(keys=['a:' + pid])
            if seen(account(tx, pid), request_id):
                return await self.repeat(ctx, pid, income=delta > 0)
            balance = await self.change(ctx, tx, pid, delta, args['reason'], request_id)
        return self.result(ctx, 0 if balance is None else balance, pending=balance is None)

    @service('transfer', major=1)
    async def transfer(self, ctx, args):
        ids = (args['from_id'], args['to_id'])
        if ids[0] == ids[1]:
            raise ServiceRejected('invalid_argument')
        async with self.lock:
            balance, payee, duplicate = await self.move(
                ctx, ids, args['amount'], (args['reason'],) * 2, args['request_id'])
        return self.result(ctx, balance, duplicate, pending=payee is None and not duplicate)

    # ---- 进服入账 ----

    async def joined(self, ctx, event) -> bool:
        body = event.payload['payload']
        try:
            player = await ctx.resolve_player(name=body['name'], xuid=body.get('xuid') or None)
        except (PlayerNotFound, AmbiguousPlayer):
            return False  # 进服事件送达时玩家已经离开：只确认事件。
        pid, name = player_id(player), player['name']
        tx = await ctx.storage.transaction(event=event.payload, keys=['a:' + pid])
        record = account(tx, pid)
        start, pending = (0 if record['started'] else ctx.config.starting_balance), record['pending']
        if record['started'] and not pending:
            return False
        if start:
            post(record, tx.commit_id, start, '初始金币')
        if pending:
            post(record, tx.commit_id, pending, '离线期间收入')
            self.tell(ctx, tx, name, f'你离线期间收到 {pending} {ctx.config.currency_name}')
        record.update(started=True, pending=0)
        amount = start + pending
        if not amount:
            await tx.set('a:' + pid, record).save()
        elif not (await self.apply(ctx, tx, pid, record, name, amount, 'join ' + pid))[0]:
            await self.amend(ctx, pid, lambda r: r.update(pending=r['pending'] + amount))
        return True

    # ---- 玩家命令 ----

    async def show_balance(self, ctx, command):
        try:
            text = f'你有 {await self.score(ctx, command.player["name"])} {ctx.config.currency_name}'
        except ServiceRejected:
            text = SCORE_FAILED
        await self.answer(ctx, command, text)

    async def history(self, ctx, command):
        pid = player_id(command.player)
        tx = await ctx.storage.transaction(event=command.event.payload, keys=['a:' + pid])
        recent = account(tx, pid)['recent'][-10:][::-1]
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
            return await self.answer(ctx, command, f'找不到玩家 {name}' if error.code == 'not_found' else LOOKUP_FAILED)
        except (ServiceFailed, ServiceWaitTimeout, ServiceUncertain):
            return await self.answer(ctx, command, LOOKUP_FAILED)
        payer, coin = command.player, ctx.config.currency_name
        ids = (player_id(payer), target_id)
        if ids[0] == ids[1]:
            return await self.answer(ctx, command, '不能转账给自己')

        reasons = (f'转给 {target_name}', f'收到 {payer["name"]} 的转账')
        try:
            # 事件重新投递时 event_id 不变，这笔转账按同一个 request_id 去重或接着做完。
            balance, payee, duplicate = await self.move(ctx, ids, amount, reasons,
                                                        'zz-' + command.event.payload['event_id'])
        except ServiceRejected as error:
            text = f'{coin}不足，转不出 {amount} {coin}' if error.code == 'insufficient_funds' else SCORE_FAILED
            return await self.answer(ctx, command, text)
        text = f'已转给 {target_name} {amount} {coin}，剩余 {balance} {coin}'
        if payee is None and not duplicate:
            text += f'\n{target_name} 不在线，{coin}会在他下次进服时到账'
        tx = await ctx.storage.transaction(event=command.event.payload, keys=[])
        self.tell(ctx, tx, payer['name'], text)
        if payee is not None:
            self.tell(ctx, tx, target_name, f'{payer["name"]} 转给你 {amount} {coin}，现有 {payee} {coin}', 'payee')
        await tx.save()

    async def find(self, ctx, name):
        """先在在线玩家里精确匹配名字，再请玩家档案按名字查离线玩家。"""
        for player in await ctx.players():
            if player['name'] == name:
                return player_id(player), player['name']
        found = await ctx.services.call('plugin.neomega.players.resolve', {'name': name}, timeout=5)
        return found['id'], found['name']

    async def answer(self, ctx, command, text):
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
