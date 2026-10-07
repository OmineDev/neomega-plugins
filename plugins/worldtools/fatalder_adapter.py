"""Optional Fatalder bridge: preserve quotation, confirmation and original job."""
import hashlib
import time
import uuid
from datetime import datetime,timedelta,timezone
from neomega_runtime.services import service,ServiceRejected
from neomega_world import digest


class FatalderTasks:
    @staticmethod
    def unresolved_remote(row):
        if row['method'] in ('pause', 'resume', 'cancel'):
            # Legacy replied records prove service submission only, not control completion.
            return row.get('control_state', 'unknown') not in ('completed', 'rejected')
        return row['method'] != 'status' and row['state'] in ('unknown', 'pending')

    @staticmethod
    def observe_rejection(row, receipt):
        # These provider refusals are documented before control admission. A generic
        # failed/timeout receipt can follow a side effect and must remain unknown.
        if (row['method'] in ('pause', 'resume', 'cancel')
                and receipt.get('state') == 'failed'
                and (receipt.get('rejection') or {}).get('code') in (
                    'invalid_control_id', 'control_id_conflict', 'task_not_active',
                    'control_in_progress', 'control_unresolved', 'retained_control_limit',
                    'caller_not_allowed', 'maintenance_sealed', 'persistence_unresolved', 'task_not_found')):
            row['control_state'] = 'rejected'
            row['rejection'] = receipt['rejection']

    @staticmethod
    def observe_fatalder(task, result):
        # Only exact durable business identifiers prove acceptance; phase alone does not.
        if result.get('request_key') != task['remote_key'] or not result.get('job_id'):
            return
        for row in task.get('remote_calls', []):
            arguments = row['intent']['arguments']
            if row['method'] in ('pause', 'resume', 'cancel'):
                if row.get('control_state') in ('completed', 'rejected'):
                    continue
                control_id = arguments.get('control_id')
                controls = result.get('controls') or {}
                control = controls.get(control_id) if control_id else None
                if control_id and result.get('control', {}).get('control_id') == control_id:
                    control = result['control']
                if (control_id and control and control.get('action') == row['method']
                        and arguments.get('job_id') == result['job_id']):
                    row['control_state'] = control['action_state']
                    row['desired_state_observed'] = control.get('desired_state_observed', False)
                    if row['control_state'] in ('completed', 'rejected'):
                        row['state'] = 'observed' if row['control_state'] == 'completed' else 'rejected'
                        row['evidence'] = {'job_id': result['job_id'], 'control_id': control_id}
                        if row['control_state'] == 'rejected':
                            row['rejection'] = {'code': control.get('error_code', 'control_rejected')}
                continue
            if row['state'] not in ('unknown', 'pending'):
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
                row['evidence'] = {key: result.get(key) for key in
                                   ('request_key', 'job_id', 'accepted_start_key')}

    async def reconcile_fatalder(self,ctx,task):
        for row in task.get('remote_calls',[]):
            if row['state'] in ('unknown','pending') and row.get('call_id'):
                try:
                    receipt = await ctx.services.get(row['call_id'])
                except Exception as exc:
                    # A prior generation may no longer access its online receipt.
                    # Keep uncertainty and let the explicit status path query the original business key.
                    row['lookup_error'] = type(exc).__name__
                    continue
                row['receipt'] = receipt
                row['state'] = receipt['state']
                self.observe_rejection(row, receipt)
                if receipt['state']=='replied':
                    task['remote'] = receipt['result']
                    task['state'] = 'remote_'+str(receipt['result'].get('phase','unknown')).lower()
                    self.observe_fatalder(task, receipt['result'])

    async def fatalder_call(self,ctx,args,call,method):
        if method != 'status':
            self.writable()
        if method == 'prepare':
            key = args.get('request_key')
            if not isinstance(key,str) or not 1 <= len(key) <= 128:
                raise ServiceRejected('request_key_required')
            task_id = hashlib.sha256((call.installation_id+'\0fatalder\0'+key).encode()).hexdigest()
            task = self.tasks.get(task_id)
            if task is not None:
                if task['fingerprint'] != digest(args):
                    raise ServiceRejected('request_key_conflict')
                return self.public(task)
            if len(self.tasks) >= ctx.config.max_tasks:
                raise ServiceRejected('task_limit')
            remote_key = task_id
            payload = {k:args[k] for k in ('source_name','position','dimension')}
            payload['request_key'] = remote_key
            task = {'task_id':task_id,'owner':call.installation_id,'fingerprint':digest(args),'kind':'fatalder',
                    'args':args,'state':'remote_pending','epoch':None,'created_at':time.time(),
                    'steps':[],'regions':[],'cursor':0,'losses':[],'cancel_requested':False,'remote_key':remote_key,
                    'remote':None,'remote_calls':[]}
            self.tasks[task_id] = task
        else:
            task = self.owned(args,call)
            await self.reconcile_fatalder(ctx,task)
            if task['kind'] != 'fatalder':
                raise ServiceRejected('not_fatalder_task')
            payload = {'request_key':task['remote_key']}
            if method in ('pause', 'resume', 'cancel'):
                control_id = args.get('control_id')
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
                    for key in ('confirm_token','confirmation_key'):
                        if args.get(key) != remote.get(key):
                            raise ServiceRejected('confirmation_mismatch')
                        payload[key] = args[key]
                elif method == 'recover':
                    payload['confirmation_key'] = remote.get('confirmation_key','')
                unresolved = [r for r in task['remote_calls'] if self.unresolved_remote(r)]
                if unresolved and method != 'recover':
                    raise ServiceRejected('remote_result_unknown')
        if len(task['remote_calls']) >= 1024:
            raise ServiceRejected('remote_call_limit')
        key = uuid.uuid4().hex
        intent = ctx.services.prepare('plugin.fatalder.cloud-import.'+method,payload,
             idempotency_key=key,deadline=(datetime.now(timezone.utc)+timedelta(seconds=25)).isoformat())
        row = {'method':method,'intent':intent.payload(),'state':'unknown','call_id':None}
        if method in ('pause', 'resume', 'cancel'):
            row['control_state'] = 'unknown'
            row['control_id'] = payload['control_id']
        task['remote_calls'].append(row)
        # Stable request/job/confirmation IDs are fsynced before any remote call.
        await self.save(ctx)
        try:
            row['call_id'] = await ctx.services.submit(intent)
            await self.save(ctx)
            receipt = await ctx.services.wait(row['call_id'],timeout=15)
            row['receipt'] = receipt
            row['state'] = receipt['state']
            self.observe_rejection(row, receipt)
            if receipt['state']=='replied':
                task['remote'] = receipt['result']
                task['state'] = 'remote_'+str(receipt['result'].get('phase','unknown')).lower()
                self.observe_fatalder(task, receipt['result'])
            else:
                task['state'] = 'remote_unknown'
        except Exception as exc:
            task['state'] = 'remote_unknown'
            row['error'] = type(exc).__name__
        await self.save(ctx)
        return self.public(task)

    @service('fatalder_prepare',with_context=True)
    async def fatalder_prepare(self,ctx,args,call):
        async with self.lock:
            return await self.fatalder_call(ctx,args,call,'prepare')

    @service('fatalder_status',with_context=True)
    async def fatalder_status(self,ctx,args,call):
        async with self.lock:
            return await self.fatalder_call(ctx,args,call,'status')

    @service('fatalder_confirm',with_context=True)
    async def fatalder_confirm(self,ctx,args,call):
        async with self.lock:
            return await self.fatalder_call(ctx,args,call,'confirm')

    @service('fatalder_pause',with_context=True)
    async def fatalder_pause(self,ctx,args,call):
        async with self.lock:
            return await self.fatalder_call(ctx,args,call,'pause')

    @service('fatalder_resume',with_context=True)
    async def fatalder_resume(self,ctx,args,call):
        async with self.lock:
            return await self.fatalder_call(ctx,args,call,'resume')

    @service('fatalder_cancel',with_context=True)
    async def fatalder_cancel(self,ctx,args,call):
        async with self.lock:
            return await self.fatalder_call(ctx,args,call,'cancel')

    @service('fatalder_recover',with_context=True)
    async def fatalder_recover(self,ctx,args,call):
        async with self.lock:
            return await self.fatalder_call(ctx,args,call,'recover')
