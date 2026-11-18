"""A filesystem barrier retains uncertain Host state commits across restarts."""
import json
import os
from pathlib import Path

from neomega_runtime.storage import CommitUncertain


def barrier(ctx):
    return Path(ctx.data_dir) / 'fatalder-state-pending.json'


async def recover_barrier(ctx):
    path = barrier(ctx)
    if not path.exists():
        return
    record = json.loads(path.read_text())
    receipt = await ctx.storage.receipt(record['commit_id'])
    if receipt is None:
        raise RuntimeError('Fatalder state commit unresolved; original receipt required')
    clear(ctx)


def clear(ctx):
    path = barrier(ctx)
    path.unlink(missing_ok=True)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_barrier(ctx, commit_id):
    path = barrier(ctx)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Exclusive creation prevents a second CAS overwriting unresolved evidence.
    with path.open('x') as handle:
        json.dump({'commit_id': commit_id}, handle)
        handle.flush()
        os.fsync(handle.fileno())
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


async def save(ctx, tx, commit_id):
    if barrier(ctx).exists():
        raise RuntimeError('Fatalder state commit unresolved; barrier already present')
    try:
        return await tx.save()
    except CommitUncertain:
        _write_barrier(ctx, commit_id)
        raise
