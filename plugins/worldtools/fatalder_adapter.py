"""Optional Fatalder bridge: preserve quotation, confirmation and original job."""
import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import time
import uuid

from neomega_runtime.managed import IPCError, IPCRejected
from neomega_runtime.services import ServiceRejected, service
from neomega_world import digest, position

UNRESOLVED_REMOTE_STATES = ('unknown', 'pending', 'timed_out', 'interrupted')
CONTROL_REJECTION_CODES = (
    'invalid_control_id',
    'control_id_conflict',
    'task_not_active',
    'control_in_progress',
    'control_unresolved',
    'retained_control_limit',
    'caller_not_allowed',
    'maintenance_sealed',
    'persistence_unresolved',
    'task_not_found',
)


def _compact_receipt(receipt):
    return {
        'call_id': receipt.get('call_id'),
        'state': receipt.get('state'),
        'error': receipt.get('error'),
        'rejection': receipt.get('rejection'),
        'result': None,
    }


class FatalderTasks:
    @staticmethod
    def unresolved_remote(row):
        if row['method'] in ('pause', 'resume', 'cancel'):
            # Legacy replied records prove service submission only, not control completion.
            return row.get('control_state', 'unknown') not in ('completed', 'rejected')
        return row['method'] != 'status' and row['state'] in UNRESOLVED_REMOTE_STATES

    @staticmethod
    def observe_rejection(row, receipt):
        # These provider refusals are documented before control admission. A generic
        # failed/timeout receipt can follow a side effect and must remain unknown.
        if (row['method'] in ('pause', 'resume', 'cancel')
                and receipt.get('state') == 'failed'
                and (receipt.get('rejection') or {}).get('code') in CONTROL_REJECTION_CODES):
            row['control_state'] = 'rejected'
            row['rejection'] = receipt['rejection']

    @staticmethod
    def observe_fatalder(task, result):
        # Only exact durable business identifiers prove acceptance; phase alone does not.
        if not isinstance(result, dict) or result.get('request_key') != task['remote_key'] or not result.get('job_id'):
            return
        for row in task.get('remote_calls', []):
            arguments = row['intent']['arguments']
            if row['method'] in ('pause', 'resume', 'cancel'):
                if row.get('control_state') in ('completed', 'rejected'):
                    continue
                control_id = arguments.get('control_id')
                controls = result.get('controls') or {}
                control = controls.get(control_id) if control_id else None
                if control_id and isinstance(result.get('control'), dict) and result['control'].get('control_id') == control_id:
                    control = result['control']
                if (control_id and isinstance(control, dict) and control.get('action') == row['method']
                        and arguments.get('job_id') == result['job_id']):
                    row['control_state'] = control['action_state']
                    row['desired_state_observed'] = control.get('desired_state_observed', False)
                    if row['control_state'] in ('completed', 'rejected'):
                        row['state'] = 'observed' if row['control_state'] == 'completed' else 'rejected'
                        row['evidence'] = {'job_id': result['job_id'], 'control_id': control_id}
                        if row['control_state'] == 'rejected':
                            row['rejection'] = {'code': control.get('error_code', 'control_rejected')}
                continue
            if row['state'] not in UNRESOLVED_REMOTE_STATES:
                continue
            if arguments.get('request_key') != task['remote_key']:
                continue
            prepared = row['method'] == 'prepare'
            confirmed = (row['method'] == 'confirm'
                         and arguments.get('job_id') == result['job_id']
                         and bool(arguments.get('confirmation_key'))
                         and arguments['confirmation_key'] == result.get('accepted_start_key'))
            if prepared or confirmed:
                row['state'] = 'observed'
                row['evidence'] = {
                    key: result.get(key)
                    for key in ('request_key', 'job_id', 'accepted_start_key')
                }

    def _apply_fatalder_receipt(self, ctx, task, row, receipt):
        old_row = dict(row)
        old_remote = task.get('remote')
        old_state = task.get('state')
        row['receipt'] = receipt
        row['state'] = receipt['state']
        self.observe_rejection(row, receipt)
        if receipt['state'] == 'replied':
            result = receipt['result']
            task['remote'] = result
            phase = result.get('phase', 'unknown') if isinstance(result, dict) else 'unknown'
            task['state'] = 'remote_' + str(phase).lower()
            self.observe_fatalder(task, result)
        try:
            self.check_budget(ctx)
        except ServiceRejected:
            # Strip bulky receipt payload if needed; if task['remote'] itself exceeds budget, roll back.
            row['receipt'] = _compact_receipt(receipt)
            try:
                self.check_budget(ctx)
            except ServiceRejected:
                row.clear()
                row.update(old_row)
                task['remote'] = old_remote
                task['state'] = old_state
                raise

    async def reconcile_fatalder(self, ctx, task):
        task_id = task['task_id']
        async with self.lock:
            current = self.tasks.get(task_id)
            if current is None:
                return
            pending = [
                (index, row['call_id'])
                for index, row in enumerate(current.get('remote_calls', []))
                if row['state'] in ('unknown', 'pending') and row.get('call_id')
            ]
        fetched = []
        for index, call_id in pending:
            try:
                receipt = await ctx.services.get(call_id)
                fetched.append((index, call_id, receipt, None))
            except Exception as exc:
                fetched.append((index, call_id, None, type(exc).__name__))
        if not fetched:
            return
        async with self.lock:
            current = self.tasks.get(task_id)
            if current is None:
                return
            calls = current.get('remote_calls', [])
            for index, call_id, receipt, error_name in fetched:
                if index >= len(calls):
                    continue
                row = calls[index]
                if row.get('call_id') != call_id or row['state'] not in ('unknown', 'pending'):
                    continue
                if error_name is not None:
                    # A prior generation may no longer access its online receipt.
                    # Keep uncertainty and let the explicit status path query the original business key.
                    row['lookup_error'] = error_name
                    continue
                self._apply_fatalder_receipt(ctx, current, row, receipt)

    @staticmethod
    def _validate_fatalder_prepare(args):
        for field in ('request_key', 'source_name', 'position', 'dimension'):
            if field not in args:
                raise ServiceRejected('invalid_argument')
        key = args['request_key']
        if not isinstance(key, str) or not 1 <= len(key) <= 128:
            raise ServiceRejected('request_key_required')
        source_name = args['source_name']
        if not isinstance(source_name, str) or not 1 <= len(source_name) <= 256:
            raise ServiceRejected('invalid_argument')
        try:
            pos = position(args['position'])
        except ValueError:
            raise ServiceRejected('invalid_argument') from None
        if any(abs(v) > 30000000 for v in pos):
            raise ServiceRejected('invalid_argument')
        if args['dimension'] not in ('overworld', 'nether', 'the_end'):
            raise ServiceRejected('invalid_argument')
        try:
            fingerprint = digest(args)
        except (TypeError, ValueError):
            raise ServiceRejected('invalid_argument') from None
        return key, fingerprint

    async def fatalder_call(self, ctx, args, call, method):
        if not isinstance(args, dict):
            raise ServiceRejected('invalid_argument')

        if method == 'prepare':
            key, fingerprint = self._validate_fatalder_prepare(args)
            task_id = hashlib.sha256((call.installation_id + '\0fatalder\0' + key).encode()).hexdigest()
            async with self.lock:
                self.writable()
                task = self.tasks.get(task_id)
                if task is not None:
                    if task['fingerprint'] != fingerprint:
                        raise ServiceRejected('request_key_conflict')
                    return self.public(task)
                if len(self.tasks) >= ctx.config.max_tasks:
                    raise ServiceRejected('task_limit')
                remote_key = task_id
                payload = {k: args[k] for k in ('source_name', 'position', 'dimension')}
                payload['request_key'] = remote_key
                intent = ctx.services.prepare(
                    'plugin.fatalder.cloud-import.prepare',
                    payload,
                    idempotency_key=uuid.uuid4().hex,
                    deadline=(datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat(),
                )
                row = {'method': 'prepare', 'intent': intent.payload(), 'state': 'unknown', 'call_id': None}
                task = {
                    'task_id': task_id,
                    'owner': call.installation_id,
                    'fingerprint': fingerprint,
                    'kind': 'fatalder',
                    'args': args,
                    'state': 'remote_pending',
                    'epoch': None,
                    'created_at': time.time(),
                    'steps': [],
                    'regions': [],
                    'cursor': 0,
                    'losses': [],
                    'cancel_requested': False,
                    'remote_key': remote_key,
                    'remote': None,
                    'remote_calls': [row],
                }
                self.check_budget(ctx, extra_tasks=[task])
                self.tasks[task_id] = task
                try:
                    # Stable request/job/confirmation IDs are fsynced before any remote call.
                    await self.save(ctx)
                except Exception:
                    self.tasks.pop(task_id, None)
                    raise
                self._fatalder_in_flight.add(task_id)
                row_index = 0
        else:
            async with self.lock:
                if method != 'status':
                    self.writable()
                task = self.owned(args, call)
                if task['kind'] != 'fatalder':
                    raise ServiceRejected('not_fatalder_task')
                task_id = task['task_id']
                if method != 'status' and task_id in self._fatalder_in_flight:
                    raise ServiceRejected('remote_in_progress')

            await self.reconcile_fatalder(ctx, task)

            async with self.lock:
                if method != 'status':
                    self.writable()
                task = self.owned(args, call)
                if method != 'status' and task_id in self._fatalder_in_flight:
                    raise ServiceRejected('remote_in_progress')
                payload = {'request_key': task['remote_key']}
                if method in ('pause', 'resume', 'cancel'):
                    if 'control_id' not in args:
                        raise ServiceRejected('invalid_argument')
                    control_id = args['control_id']
                    if not isinstance(control_id, str) or not 1 <= len(control_id) <= 128:
                        raise ServiceRejected('control_id_required')
                    for existing in task['remote_calls']:
                        if existing['intent']['arguments'].get('control_id') == control_id:
                            if existing['method'] != method:
                                raise ServiceRejected('control_id_conflict')
                            await self.save(ctx)
                            return self.public(task)
                    payload['control_id'] = control_id
                if method != 'status':
                    remote = task.get('remote') or {}
                    if method != 'recover' and not remote.get('job_id'):
                        raise ServiceRejected('remote_job_unknown')
                    payload['job_id'] = remote.get('job_id')
                    if method == 'confirm':
                        for key in ('confirm_token', 'confirmation_key'):
                            if key not in args or not isinstance(args.get(key), str):
                                raise ServiceRejected('invalid_argument')
                        for key in ('confirm_token', 'confirmation_key'):
                            if args[key] != remote.get(key):
                                raise ServiceRejected('confirmation_mismatch')
                            payload[key] = args[key]
                    elif method == 'recover':
                        payload['confirmation_key'] = remote.get('confirmation_key', '')
                    unresolved = [r for r in task['remote_calls'] if self.unresolved_remote(r)]
                    if unresolved and method != 'recover':
                        raise ServiceRejected('remote_result_unknown')
                    if len(task['remote_calls']) >= 1024:
                        raise ServiceRejected('remote_call_limit')

                intent = ctx.services.prepare(
                    'plugin.fatalder.cloud-import.' + method,
                    payload,
                    idempotency_key=uuid.uuid4().hex,
                    deadline=(datetime.now(timezone.utc) + timedelta(seconds=25)).isoformat(),
                )
                if method == 'status':
                    row_index = None
                    await self.save(ctx)
                else:
                    row = {'method': method, 'intent': intent.payload(), 'state': 'unknown', 'call_id': None}
                    if method in ('pause', 'resume', 'cancel'):
                        row['control_state'] = 'unknown'
                        row['control_id'] = payload['control_id']
                    task['remote_calls'].append(row)
                    try:
                        self.check_budget(ctx)
                        # Stable request/job/confirmation IDs are fsynced before any remote call.
                        await self.save(ctx)
                    except Exception:
                        task['remote_calls'].pop()
                        raise
                    row_index = len(task['remote_calls']) - 1
                    self._fatalder_in_flight.add(task_id)

        if method == 'status':
            try:
                call_id = await ctx.services.submit(intent)
                receipt = await ctx.services.wait(call_id, timeout=15)
            except IPCRejected:
                raise ServiceRejected('remote_unavailable') from None
            except Exception:
                async with self.lock:
                    task = self.tasks[task_id]
                    task['state'] = 'remote_unknown'
                    await self.save(ctx)
                    return self.public(task)
            async with self.lock:
                task = self.tasks[task_id]
                if receipt['state'] == 'replied':
                    old_remote = task.get('remote')
                    old_state = task.get('state')
                    result = receipt['result']
                    task['remote'] = result
                    phase = result.get('phase', 'unknown') if isinstance(result, dict) else 'unknown'
                    task['state'] = 'remote_' + str(phase).lower()
                    try:
                        self.check_budget(ctx)
                    except ServiceRejected:
                        task['remote'] = old_remote
                        task['state'] = old_state
                        raise
                    self.observe_fatalder(task, result)
                else:
                    task['state'] = 'remote_unknown'
                await self.save(ctx)
                return self.public(task)

        try:
            try:
                call_id = await ctx.services.submit(intent)
            except IPCRejected as exc:
                async with self.lock:
                    task = self.tasks[task_id]
                    row = task['remote_calls'][row_index]
                    row['state'] = 'rejected'
                    row['rejection'] = {'code': exc.code}
                    row['error'] = exc.code
                    if method in ('pause', 'resume', 'cancel'):
                        row['control_state'] = 'rejected'
                    if method == 'prepare':
                        task['state'] = 'remote_failed'
                    await self.save(ctx)
                    return self.public(task)
            except Exception as exc:
                async with self.lock:
                    task = self.tasks[task_id]
                    row = task['remote_calls'][row_index]
                    task['state'] = 'remote_unknown'
                    row['error'] = type(exc).__name__
                    await self.save(ctx)
                    return self.public(task)

            async with self.lock:
                task = self.tasks[task_id]
                row = task['remote_calls'][row_index]
                row['call_id'] = call_id
                await self.save(ctx)

            try:
                receipt = await ctx.services.wait(call_id, timeout=15)
            except Exception as exc:
                async with self.lock:
                    task = self.tasks[task_id]
                    row = task['remote_calls'][row_index]
                    task['state'] = 'remote_unknown'
                    row['error'] = type(exc).__name__
                    await self.save(ctx)
                    return self.public(task)

            async with self.lock:
                task = self.tasks[task_id]
                row = task['remote_calls'][row_index]
                self._apply_fatalder_receipt(ctx, task, row, receipt)
                if receipt['state'] != 'replied':
                    task['state'] = 'remote_unknown'
                await self.save(ctx)
                return self.public(task)
        finally:
            self._fatalder_in_flight.discard(task_id)

    @service('fatalder_prepare', with_context=True)
    async def fatalder_prepare(self, ctx, args, call):
        return await self.fatalder_call(ctx, args, call, 'prepare')

    @service('fatalder_status', with_context=True)
    async def fatalder_status(self, ctx, args, call):
        return await self.fatalder_call(ctx, args, call, 'status')

    @service('fatalder_confirm', with_context=True)
    async def fatalder_confirm(self, ctx, args, call):
        return await self.fatalder_call(ctx, args, call, 'confirm')

    @service('fatalder_pause', with_context=True)
    async def fatalder_pause(self, ctx, args, call):
        return await self.fatalder_call(ctx, args, call, 'pause')

    @service('fatalder_resume', with_context=True)
    async def fatalder_resume(self, ctx, args, call):
        return await self.fatalder_call(ctx, args, call, 'resume')

    @service('fatalder_cancel', with_context=True)
    async def fatalder_cancel(self, ctx, args, call):
        return await self.fatalder_call(ctx, args, call, 'cancel')

    @service('fatalder_recover', with_context=True)
    async def fatalder_recover(self, ctx, args, call):
        return await self.fatalder_call(ctx, args, call, 'recover')
