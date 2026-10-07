"""Trusted installation adapter; shares the chat controller's single durable slot."""
import hashlib
import json
import secrets

from neomega_runtime.services import ServiceRejected, service
from .controller import TERMINAL, expired
from .durable import barrier


class ServiceAdapter:
    def authorize(self, ctx, call):
        if call is None or call.installation_id not in ctx.config.service_installation_ids:
            raise ServiceRejected('caller_not_allowed')
        if self.controller.persistence_error or barrier(ctx).exists():
            raise ServiceRejected('persistence_unresolved')
        if self.controller.maintenance_token:
            raise ServiceRejected('maintenance_sealed')

    @staticmethod
    def public(state):
        record = state.get('service', {})
        return dict(request_key=record.get('request_key'), job_id=state.get('job_id'),
                    phase=state.get('phase'), quote=state.get('quote'),
                    accepted_start_key=(state.get('start') or {}).get('idempotency_key'),
                    confirm_token=state.get('confirm_token'), confirmation_key=record.get('confirmation_key'),
                    recovery_required=state.get('recovery_required', False),
                    lease_pending=bool(state.get('lease')), task_outcomes=state.get('task_outcomes', {}))

    def owned(self, args, call):
        state = self.controller.state
        record = state.get('service', {})
        if record.get('owner') != call.installation_id or record.get('request_key') != args.get('request_key'):
            raise ServiceRejected('task_not_found')
        if 'job_id' in args and args['job_id'] != state.get('job_id'):
            raise ServiceRejected('task_not_found')
        return state

    @service('prepare', with_context=True)
    async def service_prepare(self, ctx, args, call):
        async with self.controller.lock:
            self.authorize(ctx, call)
            key = args.get('request_key')
            if not isinstance(key, str) or not 1 <= len(key) <= 128:
                raise ServiceRejected('invalid_request_key')
            source, xyz, dimension = args.get('source_name'), args.get('position'), args.get('dimension')
            if (not isinstance(source, str) or not isinstance(xyz, list) or len(xyz) != 3
                    or any(type(v) is not int or abs(v) > 30000000 for v in xyz)
                    or dimension not in ('overworld', 'nether', 'the_end')):
                raise ServiceRejected('invalid_import')
            digest = hashlib.sha256(json.dumps([source, xyz, dimension], separators=(',', ':')).encode()).hexdigest()
            state = self.controller.state
            record = state.get('service', {})
            identity = hashlib.sha256((call.installation_id + '\0' + key).encode()).hexdigest()
            history = dict(state.get('service_history', {}))
            if identity in history:
                old = history[identity]
                if old['fingerprint'] != digest:
                    raise ServiceRejected('request_key_conflict')
                return old['result']
            if record.get('owner') == call.installation_id and record.get('request_key') == key:
                if record['fingerprint'] != digest:
                    raise ServiceRejected('request_key_conflict')
                return self.public(state)
            if state and (state.get('phase') not in TERMINAL or state.get('lease')
                          or state.get('recovery_required') or self.controller.active_controls
                          or any(not task.done() for task in self.controller.control_tasks)):
                raise ServiceRejected('import_slot_occupied')
            if record:
                old_id = hashlib.sha256((record['owner'] + '\0' + record['request_key']).encode()).hexdigest()
                history[old_id] = dict(fingerprint=record['fingerprint'], result=self.public(state))
            if len(history) >= 256:
                raise ServiceRejected('retained_request_limit')
            record = dict(owner=call.installation_id, request_key=key, fingerprint=digest,
                          confirmation_key=secrets.token_hex(16))
            # No chat command or invented player: preparation is a shared primitive.
            await self.controller.prepare_import(None, [source, *map(str, xyz), dimension], record, history)
            return self.public(self.controller.state)

    @service('status', with_context=True)
    async def service_status(self, ctx, args, call):
        async with self.controller.lock:
            self.authorize(ctx, call)
            key = args.get('request_key')
            if not isinstance(key, str):
                raise ServiceRejected('invalid_request_key')
            identity = hashlib.sha256((call.installation_id + '\0' + key).encode()).hexdigest()
            history = self.controller.state.get('service_history', {})
            if identity in history:
                return history[identity]['result']
            return self.public(self.owned(args, call))

    @service('confirm', with_context=True)
    async def service_confirm(self, ctx, args, call):
        async with self.controller.lock:
            self.authorize(ctx, call)
            state = self.owned(args, call)
            if (not args.get('job_id') or args.get('confirmation_key') != state['service']['confirmation_key']
                    or args.get('confirm_token') != state.get('confirm_token')):
                raise ServiceRejected('invalid_confirmation')
            if state.get('start'):
                return self.public(state)
            if state.get('phase') != 'quoted' or expired(state['quote']['expires_at']):
                raise ServiceRejected('quote_expired_or_not_ready')
            state['start'] = dict(version=5, job_id=state['job_id'], quote_id=state['quote']['quote_id'],
                                  idempotency_key=state['service']['confirmation_key'])
            state['phase'] = 'start_pending'
            await self.controller.save()
            self.controller.spawn_control('start', state['start'], name='fatalder-service-start')
            return self.public(state)

    async def control(self, ctx, args, call, action):
        async with self.controller.lock:
            self.authorize(ctx, call)
            state = self.owned(args, call)
            if action == 'recover':
                expected = state['service']['confirmation_key'] if state.get('start') else ''
                if args.get('confirmation_key') != expected:
                    raise ServiceRejected('original_confirmation_required')
                if not state.get('job_id') and state.get('prepare'):
                    await self.controller.prepare()
                    return self.public(state)
                job = await self.controller.request('GET', self.controller.path())
                if job['state'] == 'READY' and state.get('start'):
                    action, body = 'start', state['start']
                elif job.get('recovery_required') and state.get('start'):
                    body = dict(version=5, job_id=state['job_id'])
                else:
                    raise ServiceRejected('recovery_not_required')
            else:
                if not args.get('job_id') or state.get('phase') in TERMINAL:
                    raise ServiceRejected('task_not_active')
                body = {}
            if self.controller.active_controls or any(not t.done() for t in self.controller.control_tasks):
                raise ServiceRejected('control_in_progress')
            self.controller.spawn_control(action, body, name='fatalder-service-control')
            return dict(self.public(state), control='submitted')

    @service('pause', with_context=True)
    async def service_pause(self, ctx, args, call):
        return await self.control(ctx, args, call, 'pause')

    @service('resume', with_context=True)
    async def service_resume(self, ctx, args, call):
        return await self.control(ctx, args, call, 'resume')

    @service('cancel', with_context=True)
    async def service_cancel(self, ctx, args, call):
        return await self.control(ctx, args, call, 'cancel')

    @service('recover', with_context=True)
    async def service_recover(self, ctx, args, call):
        return await self.control(ctx, args, call, 'recover')
