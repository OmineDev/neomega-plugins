"""Consumer plugin callback; declare the scores service permissions."""
async def example(ctx, player_id):
    await ctx.services.call('plugin.neomega.scores.objective', {'objective':'points','action':'create','name':'积分','request_id':'setup-1','expected_revision':0})
    return await ctx.services.call('plugin.neomega.scores.mutate', {'objective':'points','player_id':player_id,'action':'add','value':10,'request_id':'award-1','expected_revision':1})
