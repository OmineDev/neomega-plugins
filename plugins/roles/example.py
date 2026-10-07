"""Consumer plugin callback; declare roles service permissions."""
async def example(ctx, player_id):
    await ctx.services.call('plugin.neomega.roles.define', {'role':'builder','capabilities':['land.build'],'request_id':'role-1','expected_revision':0})
    await ctx.services.call('plugin.neomega.roles.assign', {'role':'builder','player_id':player_id,'scope':'island-1','request_id':'assign-1','expected_revision':1})
    return await ctx.services.call('plugin.neomega.roles.check', {'player_id':player_id,'capability':'land.build','scope':'island-1'})
