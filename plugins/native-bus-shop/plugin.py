"""Persist a demo order and its typed delivery intent in one transaction."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from neomega_runtime import Call, OpIntent, Plugin, PluginContext, f64, u32, u64
from neomega_runtime.operations import OperationWaitTimeout


@dataclass
class BuyRequest:
    business_key: str
    player: str
    item: str
    count: u32


@dataclass
class BuyResult:
    business_key: str
    order_number: u64
    commit_id: str
    operation_ids: list[str]
    duplicate: bool
    state: str
    receipt_json: str


@dataclass
class CommandArgs:
    # This is the Host's command.execute contract, including nullable fields.
    cmd: str
    identity: str
    wait: bool | None
    timeout: f64 | None


_ITEMS = {'stone': 'minecraft:stone', 'bread': 'minecraft:bread'}
_PLAYER = re.compile(r'[A-Za-z0-9_]{1,16}\Z')
_BUSINESS_KEY = re.compile(r'[A-Za-z0-9_-]{1,128}\Z')
_ORDER_COUNT = 'shop/order_count'


class ShopPlugin(Plugin):
    async def on_init(self, ctx: PluginContext):
        self.ctx = ctx
        self._purchase_lock = asyncio.Lock()
        ctx.bus.provide(self.buy, name='shop.buy', kind='service', major=1)

    async def buy(self, request: BuyRequest, call: Call) -> BuyResult:
        if call.cancelled:
            raise asyncio.CancelledError
        if not _BUSINESS_KEY.fullmatch(request.business_key):
            raise ValueError('business_key must contain 1..128 ASCII letters, digits, _ or -')
        def prepare(tx):
            # Existing purchases skip preparation; CAS remains the durable
            # guard even though this provider serializes its own purchases.
            if not _PLAYER.fullmatch(request.player):
                raise ValueError('player must contain 1..16 ASCII letters, digits or _')
            if request.item not in _ITEMS or not 1 <= request.count <= 64:
                raise ValueError('item must be stone or bread; count must be 1..64')
            # The trusted installation scopes keys, never a request field.
            scope = json.dumps([call.caller.installation, request.business_key],
                               ensure_ascii=False, separators=(',', ':'))
            delivery_key = 'give_' + hashlib.sha256(scope.encode()).hexdigest()
            deadline = (datetime.now(timezone.utc) + timedelta(seconds=15)).isoformat()
            intent = OpIntent(delivery_key, tx.session, deadline)
            command = f'give {request.player} {_ITEMS[request.item]} {request.count}'
            tx.action(self.ctx.bus.prepare_op(
                'command.execute', CommandArgs(command, 'ws', True, None), intent=intent,
            ))
            order_number = tx.increment(_ORDER_COUNT)
            order = dict(business_key=request.business_key, order_number=order_number,
                         caller_installation=call.caller.installation,
                         player=request.player, item=request.item, count=request.count,
                         step='delivery_requested', commit_id=tx.commit_id)
            tx.set(f'shop/orders/{order_number}', order)
            return order

        # Different callers share this provider's storage revision and counter.
        # Coordinate only preparation/commit, never wait for delivery here.
        async with self._purchase_lock:
            purchase = await self.ctx.business.once(
                request.business_key, prepare, keys=[_ORDER_COUNT],
            )
        result = purchase['result']
        operation_ids = purchase['operation_ids']
        # An accepted response never claims delivery. Preserve the original ID
        # on wait timeout; a later same-key call only reads the retained order.
        state, receipt_json = 'accepted', ''
        remaining = (call.deadline - datetime.now(timezone.utc)).total_seconds() - .1
        if remaining > 0:
            try:
                receipt = await self.ctx.operations.wait(operation_ids[0], timeout=min(1, remaining))
            except OperationWaitTimeout:
                pass
            else:
                state = receipt['state']
                receipt_json = json.dumps(receipt, ensure_ascii=False, allow_nan=False,
                                          sort_keys=True, separators=(',', ':'))
        return BuyResult(result['business_key'], result['order_number'], purchase['commit_id'],
                         operation_ids, purchase['duplicate'], state, receipt_json)


if __name__ == '__main__':
    ShopPlugin().run()
