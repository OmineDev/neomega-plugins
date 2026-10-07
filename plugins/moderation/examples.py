"""Call from an installation allowed by writer_installations."""
async def ban_player(ctx, player_id, request_id, expires_at=None):
    current = await ctx.services.call('plugin.neomega.moderation.check', {'player_id': player_id})
    args = {'request_id': request_id, 'player_id': player_id,
            'expected_revision': current['revision'], 'reason': '违反服务器规则'}
    if expires_at is not None:
        args['expires_at'] = expires_at
    return await ctx.services.call('plugin.neomega.moderation.ban', args)


async def unban_player(ctx, player_id, request_id):
    current = await ctx.services.call('plugin.neomega.moderation.check', {'player_id': player_id})
    return await ctx.services.call('plugin.neomega.moderation.unban', {
        'request_id': request_id, 'player_id': player_id,
        'expected_revision': current['revision'], 'reason': '解除处罚'})
