"""Consumer example; declare the matching service permissions in its manifest."""
from datetime import datetime, timedelta, timezone
import uuid

async def request_chunk(ctx, dimension, pos):
    return await ctx.services.call('plugin.neomega.chunks.request',
        {'dimension': dimension, 'pos': list(pos)}, idempotency_key=uuid.uuid4().hex, timeout=25)
