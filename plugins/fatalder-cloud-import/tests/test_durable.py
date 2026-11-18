import json
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

from neomega_runtime.managed import IPCRejected
from neomega_runtime.storage import CommitUncertain

from fatalder_plugin.durable import barrier, clear, recover_barrier, save


class DurableTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.storage = SimpleNamespace(receipt=AsyncMock(return_value=None))
        self.ctx = SimpleNamespace(data_dir=self.temp.name, storage=self.storage)

    async def test_barrier_not_written_on_success_or_definite_rejection(self):
        tx_ok = SimpleNamespace(save=AsyncMock(return_value={'commit_id': 'c1'}))
        self.assertEqual(await save(self.ctx, tx_ok, 'c1'), {'commit_id': 'c1'})
        self.assertFalse(barrier(self.ctx).exists())

        tx_rej = SimpleNamespace(save=AsyncMock(side_effect=IPCRejected('revision_conflict', 'r1', True)))
        with self.assertRaises(IPCRejected):
            await save(self.ctx, tx_rej, 'c2')
        self.assertFalse(barrier(self.ctx).exists())

        tx_val = SimpleNamespace(save=AsyncMock(side_effect=ValueError('too big')))
        with self.assertRaises(ValueError):
            await save(self.ctx, tx_val, 'c3')
        self.assertFalse(barrier(self.ctx).exists())

    async def test_barrier_written_only_on_commit_uncertain_and_recovered_on_receipt(self):
        tx_uncertain = SimpleNamespace(save=AsyncMock(side_effect=CommitUncertain(SimpleNamespace())))
        with self.assertRaises(CommitUncertain):
            await save(self.ctx, tx_uncertain, 'commit-uncertain')
        self.assertTrue(barrier(self.ctx).exists())
        self.assertEqual(json.loads(barrier(self.ctx).read_text()), {'commit_id': 'commit-uncertain'})

        # Subsequent save while barrier exists fails closed.
        tx_next = SimpleNamespace(save=AsyncMock(return_value={}))
        with self.assertRaises(RuntimeError):
            await save(self.ctx, tx_next, 'c4')
        tx_next.save.assert_not_awaited()

        # Without receipt, recover_barrier refuses startup and keeps barrier.
        self.storage.receipt.return_value = None
        with self.assertRaises(RuntimeError):
            await recover_barrier(self.ctx)
        self.assertTrue(barrier(self.ctx).exists())

        # With receipt, recover_barrier clears barrier cleanly.
        self.storage.receipt.return_value = {'commit_id': 'commit-uncertain', 'revision': 1, 'operation_ids': []}
        await recover_barrier(self.ctx)
        self.assertFalse(barrier(self.ctx).exists())
        clear(self.ctx)


if __name__ == '__main__':
    unittest.main()
