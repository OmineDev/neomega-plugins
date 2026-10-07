"""Use inside a consumer Plugin; declare each called service in manifest."""
async def register_shop(ctx):
    route = await ctx.services.call('plugin.neomega.cbbridge.register', {'prefix': '[cb:shop]', 'priority': 10, 'consume': True})
    subscription = await ctx.services.call('plugin.neomega.cbbridge.subscribe', {'route_id': route['id']})
    return await ctx.services.call('plugin.neomega.cbbridge.poll', {'route_id': route['id'], 'cursor': subscription['cursor']})
