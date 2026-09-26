"""Teaching example: one apple for ten test points, using Host business receipts."""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from uuid import UUID

from neomega_runtime import Plugin, find_player, player_target
from neomega_runtime.players import PlayerNotFound, AmbiguousPlayer


@dataclass(frozen=True)
class Settings:
    player_uuids: list[str] = field(default_factory=list)
    initial_points: int = field(default=100, metadata={'minimum': 0, 'maximum': 10000})

    def __post_init__(self):
        if any(str(UUID(value)) != value for value in self.player_uuids):
            raise ValueError('player_uuids must contain canonical UUIDs')


def business_id(player_uuid, request_id):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,32}', request_id):
        raise ValueError('request ID must contain 1..32 letters, digits, _ or -')
    return 'exchange_' + hashlib.sha256(f'{player_uuid}/{request_id}'.encode()).hexdigest()


def deadline():
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


async def redeem(ctx, player, request_id):
    identity = business_id(player['uuid'], request_id)
    balance_key = 'points/' + player['uuid']
    # Host Business stores the result, debit and single reward intent atomically.
    # The name is a current command target, not part of durable request identity.
    request = dict(player_uuid=player['uuid'], item='minecraft:apple', count=1, price=10)
    business = await ctx.business.begin(identity, request, keys=[balance_key])
    if business.receipt is not None:
        return business.receipt
    tx = business.state
    balance = tx.get(balance_key, ctx.config.initial_points)
    if type(balance) is not int or balance < 0:
        raise ValueError('invalid persisted test balance')
    result = dict(request_id=request_id, **request, balance=balance, status='insufficient_points')
    if balance >= 10:
        intent = ctx.commands.prepare(
            'give ' + player_target(player['name']) + ' minecraft:apple 1',
            idempotency_key=identity, deadline=deadline())
        tx.action(intent)
        balance -= 10
        result.update(balance=balance, status='pending', intent=intent.payload())
    tx.set(balance_key, balance)
    return await business.commit(result)


async def inspect(ctx, identity):
    business = await ctx.business.get(identity)
    if business is None:
        return {'status': 'not_found'}
    result = dict(business['result'], business_id=identity)
    if not business['operation_ids']:
        return result
    operation_id, = business['operation_ids']
    receipt = await ctx.operations.get(operation_id)
    state = receipt['state']
    # Keep raw evidence and exact terminal state. Admission is not game success.
    result.update(operation_id=operation_id, operation=receipt,
                  status=state if state in ('succeeded', 'failed', 'unknown', 'cancelled') else 'pending')
    await ctx.storage.set('exchange_status/' + identity, result)
    return result


class PointsExchange(Plugin):
    config_type = Settings

    async def on_start(self, ctx):
        self.lock = asyncio.Lock()

    async def on_event(self, ctx, event):
        async with self.lock:
            body = event.payload.get('payload', {})
            parts = body.get('message', '').split()
            if event.kind != 'chat.received' or not parts or parts[0] != '!兑换':
                await event.ack()
                return
            try:
                player = find_player(await ctx.players(), name=body.get('player', ''),
                                     xuid=body.get('xuid') or None)
            except (PlayerNotFound, AmbiguousPlayer, ValueError):
                await event.ack()
                return
            if player['uuid'] not in ctx.config.player_uuids:
                await event.ack()
                return
            # Reject unsafe names before any debit or reply intent is prepared.
            target = player_target(player['name'])
            if (len(parts) == 2 or len(parts) == 3 and parts[1] == '状态') and re.fullmatch(r'[A-Za-z0-9_-]{1,32}', parts[-1]):
                request_id = parts[-1]
                if len(parts) == 2:
                    await redeem(ctx, player, request_id)
                result = await inspect(ctx, business_id(player['uuid'], request_id))
                status = result['status']
                message = f'{request_id}: {status}'
                if status == 'unknown':
                    message += '；待人工核对，请勿换请求号重发。'
                if result.get('operation_id'):
                    message += ' operation=' + result['operation_id']
            else:
                message = '!兑换 请求号：10 测试积分换 1 苹果；!兑换 状态 请求号：查询原回执。'
            # ACK and notification are one existing event transaction. If the
            # business commit succeeded before a crash, redelivery reads it.
            key = 'exchange_reply_' + hashlib.sha256(event.payload['delivery_token'].encode()).hexdigest()
            tx = await event.transaction(commit_id=key)
            tx.action(ctx.commands.prepare(
                'tellraw ' + target + ' ' + json.dumps({'rawtext': [{'text': message}]}, ensure_ascii=False),
                idempotency_key=key, deadline=deadline()))
            await tx.save()


if __name__ == '__main__':
    PointsExchange().run()
