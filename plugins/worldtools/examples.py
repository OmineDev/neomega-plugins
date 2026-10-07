"""Prepare a bounded backup; callers explicitly execute the returned task ID."""
from datetime import datetime, timedelta, timezone
import uuid

async def prepare_backup(ctx, request_key, pos, size, dimension=0):
    return await ctx.services.call('plugin.neomega.worldtools.prepare',
        {'request_key': request_key, 'kind': 'backup', 'args': {'dimension':dimension, 'pos': list(pos), 'size': list(size)}},
        idempotency_key=uuid.uuid4().hex, timeout=25)
