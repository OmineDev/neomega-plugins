"""Call from an authorized consumer Plugin.on_start or managed callback."""
async def send_notice(ctx, request_id: str):
    routes = await ctx.services.call('plugin.neomega.messaging.routes', {})
    if not routes['routes']:
        raise RuntimeError('configure a route ACL for this installation')
    receipt = await ctx.services.call('plugin.neomega.messaging.send', {
        'route_id': routes['routes'][0]['route_id'],
        'text': '服务器活动已经开始。', 'request_id': request_id})
    # Keep delivery_id; query instead of submitting again after uncertainty.
    return await ctx.services.call('plugin.neomega.messaging.status', {
        'delivery_id': receipt['delivery_id']})


async def read_messages(ctx, cursor=0):
    return await ctx.services.call('plugin.neomega.messaging.subscribe', {'after': cursor, 'limit': 50})
