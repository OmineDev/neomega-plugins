"""Consumer integration; retain the request ID before calling a mutation."""
async def create_expiry(ctx, business_id, due_at, player_id):
    return await ctx.services.call('plugin.neomega.scheduler.schedule', {
        'request_id': 'create-' + business_id, 'schedule_id': business_id,
        'due_at': due_at, 'timezone': 'Asia/Shanghai', 'missed': 'coalesce',
        'payload': {'player_id': player_id}})


async def consume(ctx, durable_request_id, apply_once):
    reply = await ctx.services.call('plugin.neomega.scheduler.claim', {
        'request_id': durable_request_id, 'limit': 16})
    for trigger in reply['triggers']:
        # apply_once persists trigger_id with the business transaction. Unknown
        # effects return 'unknown'; they must not be repeated on a lease timeout.
        outcome = await apply_once(trigger['trigger_id'], trigger['payload'])
        await ctx.services.call('plugin.neomega.scheduler.ack', {
            'request_id': 'ack-' + trigger['token'], 'schedule_id': trigger['schedule_id'],
            'trigger_id': trigger['trigger_id'], 'token': trigger['token'], 'outcome': outcome})
