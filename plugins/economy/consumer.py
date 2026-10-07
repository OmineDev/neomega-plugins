"""Consumer snippet; declare neomega.economy dependency and service permission."""
async def purchase(ctx, account, merchant, purchase_id, amount, expected_revision):
    # amount is a positive integer in the smallest currency unit.
    return await ctx.services.call('plugin.neomega.economy.transfer', {
        'currency': 'coin', 'account': account, 'target': merchant,
        'amount': amount, 'request_id': purchase_id, 'expected_revision': expected_revision,
    }, idempotency_key=purchase_id)
