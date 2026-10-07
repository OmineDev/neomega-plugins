"""Call from a consumer PluginContext with declared service permissions."""
async def example(ctx, name):
    resolved = await ctx.services.call('plugin.neomega.players.resolve', {'name': name})
    player_id = resolved['player']['player_id']
    profile = await ctx.services.call('plugin.neomega.players.get', {'player_id': player_id})
    return await ctx.services.call('plugin.neomega.players.patch', {
        'player_id': player_id, 'request_id': 'onboarding-1',
        'expected_revision': profile['fields_revision'], 'fields': {'onboarded': True}})
