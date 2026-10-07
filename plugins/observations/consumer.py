"""Call from an activated consumer Plugin with declared service permissions."""
async def observe(ctx):
    subscription = await ctx.services.call('plugin.neomega.observations.subscribe', {'kinds': ['position', 'death', 'gap']})
    snapshot = await ctx.services.call('plugin.neomega.observations.refresh', {'kinds': ['position']})
    page = await ctx.services.call('plugin.neomega.observations.poll', {'subscription_id': subscription['subscription_id'], 'cursor': subscription['cursor']})
    await ctx.services.call('plugin.neomega.observations.unsubscribe', {'subscription_id': subscription['subscription_id']})
    return snapshot, page
