"""One durable import slot, explicit billing confirmation, and connection leases."""
import asyncio
from datetime import datetime, timedelta, timezone
import json
import hashlib
from pathlib import Path
import secrets
import re
import shlex
import threading
from urllib.parse import quote

from .operator import _target
from .durable import save as durable_save, recover_barrier, barrier
from .client import WorkerError

STATE = 'fatalder.controller'
TERMINAL = {'succeeded', 'failed', 'cancelled'}


def now():
    return datetime.now(timezone.utc)


def expired(value):
    # Go emits RFC3339Nano; Python 3.10 accepts only 3 or 6 fractional digits.
    value = re.sub(r'\.(\d+)(?=Z|[+-])', lambda m: '.' + (m[1] + '000000')[:6], value)
    return datetime.fromisoformat(value.replace('Z', '+00:00')) <= now()


class Controller:
    def __init__(self, ctx, client, operator):
        self.ctx, self.client, self.operator = ctx, client, operator
        self.state = {}
        self.lock = asyncio.Lock()
        self.stopped = threading.Event()
        self.reader_job = None
        self.reader_task = None
        self.maintenance_token = None
        self.control_tasks = set()
        self.active_controls = 0
        self.persistence_error = False

    async def maintenance(self, request):
        operation, token = request.get('operation'), request.get('token')
        if operation not in ('seal', 'release') or not isinstance(token, str) or not token:
            return {'status': 'unsupported'}
        # The lock check and seal contain no await: one atomic event-loop turn.
        # Do not acquire an unlocked asyncio.Lock here: queued waiters may own
        # its next turn, which would make maintenance wait behind business I/O.
        if self.persistence_error or barrier(self.ctx).exists() or self.lock.locked():
            return {'status': 'busy', 'token': token}
        if operation == 'release':
            if self.maintenance_token not in (None, token):
                return {'status': 'busy', 'token': token}
            self.maintenance_token = None
            return {'status': 'released', 'token': token}
        if self.maintenance_token == token:
            return {'status': 'sealed', 'token': token}
        outcomes = self.state.get('task_outcomes') or {}
        if (self.maintenance_token or self.active_controls or any(not task.done() for task in self.control_tasks) or
                (self.state and self.state.get('phase') not in TERMINAL) or
                self.state.get('lease') or self.state.get('review') or
                self.state.get('recovery_required') or self.unresolved_controls() or
                any(value.get('state') not in ('succeeded', 'failed', 'skipped')
                    for value in outcomes.values())):
            return {'status': 'busy', 'token': token}
        self.maintenance_token = token
        return {'status': 'sealed', 'token': token}


    async def start(self):
        await recover_barrier(self.ctx)
        self.state = await self.ctx.storage.get(STATE, {})
        if self.state and self.state.get('binding') != self.binding():
            raise ValueError('active slot configuration binding changed')
        changed = False
        for record in self.state.get('controls', {}).values():
            if record['action_state'] == 'accepted':
                record['action_state'] = 'unknown'
                changed = True
        if changed:
            await self.save()
        self.ctx.every(self.ctx.config.poll_seconds, self.tick, name='fatalder-monitor', immediate=True)

    def binding(self):
        c = self.ctx.config
        return [c.worker_url, c.api_key, c.target_server_id, c.rental_server_code]

    async def stop(self):
        # SSE disconnection is observational; never sends cancel/start/recover.
        self.stopped.set()

    async def save(self):
        if self.persistence_error:
            raise RuntimeError('controller persistence unresolved; reload required')
        try:
            commit_id = 'fatalder_state_' + secrets.token_hex(16)
            tx = await self.ctx.storage.transaction(commit_id=commit_id)
            tx.set(STATE, self.state)
            await durable_save(self.ctx, tx, commit_id)
        except BaseException:
            # Do not create another transaction/CAS from uncommitted memory.
            # A restart loads the authoritative persisted slot before observation.
            self.persistence_error = True
            raise

    def path(self, suffix=''):
        return '/v2/jobs/' + quote(self.state['job_id'], safe='') + suffix

    async def request(self, method, path, body=None):
        return await asyncio.to_thread(self.client.request, method, path, body)

    async def tell(self, name, message):
        key = 'fatalder_message_' + secrets.token_hex(16)
        tx = await self.ctx.storage.transaction(commit_id=key)
        tx.action(self.ctx.commands.prepare(
            'tellraw ' + _target(name) + ' ' + json.dumps({'rawtext': [{'text': message}]}, ensure_ascii=False),
            idempotency_key=key, deadline=(now() + timedelta(seconds=30)).isoformat()))
        await durable_save(self.ctx, tx, key)

    async def handle(self, event):
        async with self.lock:
            if self.maintenance_token or self.persistence_error or barrier(self.ctx).exists():
                return
            body = event.payload.get('payload', {})
            message, name = body.get('message', ''), body.get('player', '')
            if event.kind != 'chat.received' or not message.startswith('!导入'):
                await event.ack()
                return
            players = await self.ctx.players()
            matched = [p for p in players if p['name'] == name]
            if len(matched) != 1 or matched[0]['uuid'] not in self.ctx.config.admin_uuids:
                await event.ack()
                return
            # Admission and ACK are atomic. A crash never replays a paid command.
            # The slot records the corresponding prepare/start request before I/O.
            token = event.payload['delivery_token']
            tx = await event.transaction(commit_id='fatalder_chat_' + hashlib.sha256(token.encode()).hexdigest())
            tx.set('fatalder.last_chat', token)
            await durable_save(self.ctx, tx, 'fatalder_chat_' + hashlib.sha256(token.encode()).hexdigest())
            try:
                await self._command(name, shlex.split(message)[1:])
            except asyncio.CancelledError:
                raise
            except Exception:
                await self.tell(name, '操作未确认完成；请用 !导入 状态 检查，勿重复提交。')

    def files_root(self):
        data = Path(self.ctx.data_dir).resolve()
        root = (data / self.ctx.config.files_directory).resolve()
        if not root.is_relative_to(data):
            raise ValueError('files directory escapes data directory')
        root.mkdir(parents=True, exist_ok=True)
        return root

    def source_path(self, name):
        if not name or name in ('.', '..') or Path(name).name != name or '\\' in name:
            raise ValueError('invalid basename')
        root = self.files_root()
        path = root / name
        if path.is_symlink() or not path.resolve().is_relative_to(root) or not path.is_file():
            raise ValueError('invalid source')
        return path

    async def command(self, name, args):
        async with self.lock:
            if self.maintenance_token or self.persistence_error or barrier(self.ctx).exists():
                return
            await self._command(name, args)

    async def _command(self, name, args):
        if not args:
            await self.tell(name, '!导入 列表 | 文件名 x y z overworld/nether/the_end | 确认 token | 状态 | 暂停 | 继续 | 取消 | 恢复 | 核查 | 确认核查 token')
            return
        action = args[0]
        if self.state.get('service') and action not in ('列表', '状态'):
            controls = ('暂停', '继续', '取消', '恢复', '确认', '核查', '确认核查')
            if (action in controls or self.state.get('phase') not in TERMINAL or self.state.get('lease')):
                raise ValueError('service-owned slot; use owning installation')
        if action == '列表':
            names = sorted(p.name for p in self.files_root().iterdir() if p.is_file() and not p.is_symlink())
            await self.tell(name, '可用建筑：' + '、'.join(names[:40]))
            return
        if action in ('核查', '确认核查'):
            await self.review_cleanup(name, args)
            return
        if action == '状态':
            await self.tell(name, '导入状态：' + str(self.state.get('phase', '空闲')) +
                            '；任务：' + str(self.state.get('job_id', '无')) +
                            ('；需要显式恢复' if self.state.get('recovery_required') else '') +
                            ('；存在待核查权限租约' if self.state.get('lease') else '') +
                            '；' + self.result_summary(self.state.get('task_outcomes', {})))
            controls = self.state.get('controls', {})
            if controls:
                latest = next(reversed(controls.values()))
                await self.tell(name, '最近控制：' + latest['control_id'] + ' ' + latest['action'] +
                                '；回执=' + latest['action_state'] +
                                '；观察到目标状态=' + str(latest['desired_state_observed']))
            if self.state.get('phase') == 'quoted':
                await self.show_quote(name)
            return
        if action in ('暂停', '继续', '取消', '恢复', '确认'):
            if not self.state.get('job_id'):
                # A lost Prepare reply can be retried explicitly with the SAME body/key.
                if action == '恢复' and self.state.get('prepare'):
                    await self.prepare()
                    await self.show_quote(name)
                    return
                raise ValueError('no job')
            if action == '确认':
                if (len(args) != 2 or args[1] != self.state.get('confirm_token') or
                        self.state.get('phase') != 'quoted' or expired(self.state['quote']['expires_at'])):
                    raise ValueError('invalid confirmation')
                self.state['start'] = dict(version=5, job_id=self.state['job_id'],
                    quote_id=self.state['quote']['quote_id'], idempotency_key=secrets.token_hex(16))
                self.state['phase'] = 'start_pending'
                await self.save()
                self.spawn_control('start', self.state['start'], name='fatalder-start')
            elif action == '恢复':
                if self.unresolved_controls():
                    raise ValueError('control_unresolved')
                job = await self.request('GET', self.path())
                if job['state'] == 'READY' and self.state.get('start'):
                    self.spawn_control('start', self.state['start'], name='fatalder-start-reconcile')
                    await self.tell(name, '使用原确认回执核对启动；未创建新的扣费请求。')
                    return
                if not job.get('recovery_required'):
                    raise ValueError('recovery not required')
                self.spawn_control('recover', dict(version=5, job_id=self.state['job_id']), name='fatalder-recover')
            else:
                operation = {'暂停': 'pause', '继续': 'resume', '取消': 'cancel'}[action]
                control_id = secrets.token_hex(16)
                await self.admit_control(operation, control_id)
                self.spawn_control(operation, {'control_id': control_id}, name='fatalder-control')
            await self.tell(name, '已提交请求；最终状态以 !导入 状态 为准。')
            return
        await self.prepare_import(name, args)
        await self.show_quote(name)

    async def prepare_import(self, owner, args, service_record=None, service_history=None):
        if len(args) != 5:
            raise ValueError('expected file x y z dimension')
        if self.state and (self.state.get('phase') not in TERMINAL or self.state.get('lease') or self.unresolved_controls()):
            raise ValueError('import slot occupied')
        action = args[0]
        path = self.source_path(action)
        xyz = [int(value) for value in args[1:4]]
        if any(abs(v) > 30000000 for v in xyz) or args[4] not in ('overworld', 'nether', 'the_end'):
            raise ValueError('invalid position')
        if action.endswith('.ref.json'):
            if path.stat().st_size > 16384:
                raise ValueError('reference too large')
            ref = self.client._ref(json.loads(path.read_text()))
            source_name = ref.get('display_name', '')
            if not source_name or Path(source_name).name != source_name:
                raise ValueError('invalid reference name')
        else:
            ref = await asyncio.to_thread(self.client.upload, path)
            source_name = action
        build = json.loads(Path(__file__).with_name('build_defaults.json').read_text())
        build.update(source=ref, source_name=source_name,
            start_position=dict(zip(('x', 'y', 'z'), xyz)), dimension_name=args[4])
        history = service_history if service_history is not None else dict(self.state.get('service_history', {}))
        if service_history is None and self.state.get('service'):
            from .services import ServiceAdapter
            previous = self.state['service']
            ident = hashlib.sha256((previous['owner'] + '\0' + previous['request_key']).encode()).hexdigest()
            history[ident] = dict(fingerprint=previous['fingerprint'], result=ServiceAdapter.public(self.state))
        if len(history) >= 256:
            raise ValueError('retained service request limit')
        self.state = dict(service_history=history, binding=self.binding(), phase='prepare_pending', owner=owner, cursor=0, prepare=dict(version=5,
            idempotency_key=secrets.token_hex(16), spec=dict(display_name=source_name,
                managed_operator=True,
                operator_cleanup_policy='revoke' if self.ctx.config.revoke_operator_on_completion else 'retain',
                target=dict(server_id=self.ctx.config.target_server_id,
                    rental_server_code=self.ctx.config.rental_server_code, account_source='service_center'),
                tasks=[dict(task_key='import', kind='build', build=build)])))
        if service_record is not None:
            self.state['service'] = service_record
        await self.save()
        await self.prepare()

    async def prepare(self):
        response = await self.request('POST', '/v2/jobs/prepare', self.state['prepare'])
        self.state.update(job_id=response['job_id'], quote=response['quote'], phase='quoted',
                          confirm_token=secrets.token_hex(4))
        await self.save()

    async def show_quote(self, name):
        q = self.state['quote']
        await self.tell(name, f"报价：{q['amount']} {q['currency']}，计费量 {q['units']}，有效至 {q['expires_at']}。确认执行：!导入 确认 {self.state['confirm_token']}")

    def unresolved_controls(self):
        return any(r['action_state'] in ('accepted', 'unknown')
                   for r in self.state.get('controls', {}).values())

    async def admit_control(self, action, control_id):
        """Caller holds the lock. Persist intent before any network dispatch."""
        if not isinstance(control_id, str) or not 1 <= len(control_id) <= 128:
            raise ValueError('invalid_control_id')
        records = self.state.setdefault('controls', {})
        if control_id in records:
            if records[control_id]['action'] != action:
                raise ValueError('control_id_conflict')
            return records[control_id]
        if self.unresolved_controls():
            raise ValueError('control_unresolved')
        if len(records) >= 256:
            raise ValueError('retained_control_limit')
        record = dict(control_id=control_id, action=action, action_state='accepted',
                      observed_state=self.state.get('phase'), desired_state_observed=False)
        records[control_id] = record
        await self.save()
        return record

    def observe_controls(self):
        desired = {'pause': 'paused', 'resume': 'running', 'cancel': 'cancelled'}
        for record in self.state.get('controls', {}).values():
            record['observed_state'] = self.state.get('phase')
            record['desired_state_observed'] = self.state.get('phase') == desired[record['action']]

    def spawn_control(self, action, body, *, name):
        task = self.ctx.spawn(self.run_control(action, body), name=name)
        self.control_tasks.add(task)
        task.add_done_callback(self.control_tasks.discard)

    async def run_control(self, action, body):
        async with self.lock:
            if self.maintenance_token or self.persistence_error or barrier(self.ctx).exists():
                return
            record = None
            if action in ('pause', 'resume', 'cancel'):
                control_id = body.get('control_id') or secrets.token_hex(16)
                record = await self.admit_control(action, control_id)
                if record['action_state'] != 'accepted':
                    return
                # Persist uncertain dispatch before I/O; restart never replays it.
                record['action_state'] = 'unknown'
                await self.save()
            elif self.unresolved_controls():
                return
            self.active_controls += 1
        try:
            # Remote start/recovery may await operator ACKs from tick/reader.
            # Keep observation live while the admitted request runs.
            acknowledged = await self._run_control(action, body)
            if record is not None and acknowledged:
                async with self.lock:
                    record['action_state'] = 'rejected' if isinstance(acknowledged, dict) else 'completed'
                    record['evidence'] = 'pre_dispatch_rejection' if isinstance(acknowledged, dict) else 'original_http_response'
                    if isinstance(acknowledged, dict):
                        record['error_code'] = acknowledged['error_code']
                    await self.save()
        finally:
            self.active_controls -= 1

    async def _run_control(self, action, body):
        # Phase GET is observational, never evidence settling a lost control reply.
        path = self.path('/' + action)
        try:
            # Worker runtime controls require no request body, not JSON {}.
            outgoing = None if action in ('pause', 'resume', 'cancel') else dict(body)
            access = getattr(self.ctx.config, 'rental_access', None)
            if action in ('start', 'recover') and access is not None:
                outgoing['rental_server_passcode'] = self.ctx.secrets.get(access.passcode)
            await self.request('POST', path, outgoing)
            return True
        except asyncio.CancelledError:
            raise
        except WorkerError as exc:
            # Worker ServeHTTP rejects 401 before dispatch to the engine. These
            # local client errors likewise occur before connection.request.
            # In contrast, invalid_state/409 can arise AFTER Controller.Pause
            # or Resume, so it must remain unknown even with an HTTP response.
            if (exc.status == 401 or
                    (exc.status is None and exc.code in
                     ('secret_unavailable', 'invalid_api_path', 'invalid_request'))):
                return {'error_code': exc.code}
            self.ctx.log.warning('Fatalder control outcome requires observation')
        except Exception:
            self.ctx.log.warning('Fatalder control outcome requires observation')

    async def tick(self):
        if self.stopped.is_set() or not self.state.get('job_id'):
            return
        try:
            async with self.lock:
                if self.maintenance_token or self.persistence_error or barrier(self.ctx).exists():
                    return
                job = await self.request('GET', self.path())
                if job['state'] == 'READY' and self.state.get('quote') and not self.state.get('start'):
                    self.state['phase'] = 'quoted'
                else:
                    self.state['phase'] = job['state'].lower()
                self.observe_controls()
                self.state['recovery_required'] = job.get('recovery_required', False)
                self.state['task_outcomes'] = job.get('task_outcomes', {})
                self.state['worker_cleanup_policy'] = job.get('spec', {}).get('operator_cleanup_policy') or 'revoke'
                await self.operator_gate(job.get('operator_session'))
                await self.reconcile_retained(job)
                await self.save()
                job_id = self.state['job_id']
                if self.reader_task is None or self.reader_task.done():
                    self.reader_job = job_id
                    self.reader_task = self.ctx.spawn(self.read_events(job_id), name='fatalder-events')
        except asyncio.CancelledError:
            raise
        except Exception:
            self.ctx.log.warning('Fatalder observation unavailable; retaining task and permission receipts')

    @staticmethod
    def result_summary(outcomes):
        if not outcomes:
            return '任务结果未知；内容完整性未知'
        labels = {'succeeded': '成功', 'failed': '失败', 'skipped': '跳过',
                  'unknown': '未知', 'incomplete': '不完整'}
        return '；'.join(str(key) + ' 任务=' + labels.get(value.get('state'), '未知') +
                        ' 内容=' + labels.get(value.get('content'), '未知')
                        for key, value in outcomes.items())

    def matching_session(self, job):
        lease = self.state.get('lease')
        snapshot = job.get('operator_session') or {}
        return bool(lease and job.get('job_id') == self.state.get('job_id') and
                    snapshot.get('session_id') == lease['session_id'] and
                    snapshot.get('bot_uuid') == lease['identity']['uuid'] and
                    snapshot.get('bot_name') == lease['identity']['name'] and
                    snapshot.get('server_id') == self.ctx.config.target_server_id and
                    snapshot.get('rental_server_code') == self.ctx.config.rental_server_code)

    async def reconcile_retained(self, job):
        if (job.get('recovery_required') or
                job.get('spec', {}).get('operator_cleanup_policy') != 'retain' or
                (job.get('operator_session') or {}).get('phase') != 'retained' or
                not self.matching_session(job)):
            return
        lease = self.state.pop('lease')
        self.state['last_operator_session'] = dict(lease, job_id=self.state['job_id'],
            cleanup_outcome='operator_retained', task_outcomes=job.get('task_outcomes', {}))
        await self.save()

    async def review_cleanup(self, name, args):
        if not self.state.get('job_id'):
            await self.tell(name, '没有可核查的原任务。')
            return
        job = await self.request('GET', self.path())
        outcomes = job.get('task_outcomes') or {}
        tasks = job.get('spec', {}).get('tasks', [])
        keys = {task['task_key'] for task in tasks}
        trusted = (keys and keys == set(outcomes) and
                   all(v.get('state') in ('succeeded', 'failed', 'skipped') for v in outcomes.values()))
        if (not self.matching_session(job) or job.get('state', '').lower() not in TERMINAL or
                job.get('recovery_required') or not trusted or
                job['operator_session'].get('phase') != 'cleanup_failed'):
            self.state.pop('review', None)
            await self.save()
            await self.tell(name, '核查证据不足，保留占用；' + self.result_summary(outcomes))
            return
        evidence = dict(job_id=job['job_id'], state=job['state'],
                        session=job['operator_session'], outcomes=outcomes, tasks=tasks)
        digest = hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest()
        if args[0] == '核查':
            token = secrets.token_hex(4)
            self.state['review'] = dict(token=token, digest=digest, owner=name,
                deadline=(now()+timedelta(minutes=5)).isoformat())
            await self.save()
            await self.tell(name, '原任务=' + job['state'] + '；' + self.result_summary(outcomes) +
                '。确认将保留机器人 OP 并释放本地槽位；历史失败不改、不会重建。输入 !导入 确认核查 ' + token)
            return
        review = self.state.get('review') or {}
        if (len(args) != 2 or args[1] != review.get('token') or review.get('owner') != name or
                review.get('digest') != digest or expired(review.get('deadline', '2000-01-01T00:00:00Z'))):
            await self.tell(name, '核查确认无效或证据变化，请重新核查。')
            return
        lease = self.state.pop('lease')
        self.state.update(phase=job['state'].lower(), task_outcomes=outcomes,
            last_operator_session=dict(lease, job_id=job['job_id'], cleanup_outcome='explicit_retention_review',
                review=evidence, confirmed_by=name, confirmed_at=now().isoformat()))
        self.state.pop('review', None)
        await self.save()
        await self.tell(name, '已记录保留 OP 核查并释放槽位；原任务=' + job['state'] + '；' + self.result_summary(outcomes))

    async def operator_gate(self, snapshot):
        if not snapshot:
            return
        lease = self.state.get('lease')
        if snapshot['phase'] == 'cleaned' and lease and lease.get('cleaned') and lease['session_id'] == snapshot['session_id']:
            self.state.pop('lease', None)
            await self.save()
            return
        if snapshot['phase'] not in ('authorize', 'cleanup'):
            return
        if (snapshot['server_id'] != self.ctx.config.target_server_id or
                snapshot['rental_server_code'] != self.ctx.config.rental_server_code or expired(snapshot['deadline'])):
            return
        session = snapshot['session_id']
        lease = self.state.get('lease')
        if snapshot['phase'] == 'authorize':
            if lease is None:
                identity = await self.operator.inspect(snapshot['bot_uuid'], snapshot['bot_name'])
                if identity is None:
                    return
                lease = dict(session_id=session, identity=identity, prior_op=identity['is_op'], owned=False,
                             cleanup_policy=self.state.get('worker_cleanup_policy',
                                 'revoke' if self.ctx.config.revoke_operator_on_completion else 'retain'),
                             grant_key=self.state['job_id'] + ':' + session + ':grant')
                self.state['lease'] = lease
                await self.save()
            if lease['session_id'] != session or lease['identity']['uuid'] != snapshot['bot_uuid'] or lease['identity']['name'] != snapshot['bot_name']:
                return
            if not lease['prior_op']:
                result = await self.operator.set_permission(lease['identity'], True, lease['grant_key'])
                if result['state'] != 'succeeded':
                    return
                # A permission changed by someone else must never become ours.
                lease['owned'] = result.get('changed', False)
                await self.save()
        else:
            if lease is None:
                # No local authorization was issued; nothing to revoke.
                await self.ack_operator(snapshot)
                return
            if lease['session_id'] != session or lease['identity']['uuid'] != snapshot['bot_uuid'] or lease['identity']['name'] != snapshot['bot_name']:
                return
            # Freeze the policy before ACK, including leases created by older
            # versions. A lost reply/reload must not turn retained OP into deop.
            if 'cleanup_policy' not in lease:
                lease['cleanup_policy'] = 'revoke' if self.ctx.config.revoke_operator_on_completion else 'retain'
                await self.save()
            if lease['cleanup_policy'] == 'retain':
                lease['cleaned'] = True
                lease['cleanup_outcome'] = 'operator_retained'
                self.state['last_operator_session'] = dict(lease, job_id=self.state['job_id'])
                await self.save()
                # This ACK closes the managed session; it does not claim that
                # permissions were revoked or an unknown grant was resolved.
                await self.ack_operator(snapshot)
                self.state.pop('lease', None)
                await self.save()
                return
            if not lease['prior_op'] and not lease['owned']:
                # Reconcile a lost grant reply with its durable action receipt.
                result = await self.operator.set_permission(lease['identity'], True, lease['grant_key'], reconcile_only=True)
                if result['state'] != 'succeeded':
                    return
                lease['owned'] = result.get('changed', False)
                await self.save()
            if lease['owned']:
                identity = dict(lease['identity'], owned=True)
                result = await self.operator.set_permission(identity, False,
                    self.state['job_id'] + ':' + session + ':revoke')
                if result['state'] != 'succeeded':
                    return
            # Keep a completed lease until ACK is confirmed; a lost response must
            # reconcile the same revoke receipt instead of creating an action.
            lease['cleaned'] = True
            await self.save()
        await self.ack_operator(snapshot)
        if snapshot['phase'] == 'cleanup':
            self.state.pop('lease', None)
            await self.save()

    async def ack_operator(self, snapshot):
        await self.request('POST', self.path('/operator-ack'),
            {k: snapshot[k] for k in ('session_id', 'attempt_id', 'phase')})

    async def read_events(self, job_id):
        if self.maintenance_token or self.persistence_error or barrier(self.ctx).exists():
            return
        iterator = self.client.events(job_id, self.state.get('cursor', 0), self.stopped)
        try:
            while not self.stopped.is_set():
                event = await asyncio.to_thread(next, iterator, None)
                if event is None:
                    break
                async with self.lock:
                    if self.maintenance_token or self.persistence_error or barrier(self.ctx).exists():
                        return
                    if self.state.get('job_id') != job_id:
                        break
                    seq = event.get('seq', 0)
                    if seq <= self.state.get('cursor', 0):
                        continue
                    payload = event.get('payload') or {}
                    if event.get('kind') == 'target.operator_session' and payload.get('phase') == 'retained':
                        await self.reconcile_retained(dict(job_id=job_id, operator_session=payload,
                            recovery_required=self.state.get('recovery_required', False),
                            spec={'operator_cleanup_policy': self.state.get('worker_cleanup_policy')}))
                    gate = payload.get('gate')
                    if gate:
                        obj = gate.get('object')
                        if obj:
                            output = Path(self.ctx.data_dir) / 'downloads' / (secrets.token_hex(16) + '.artifact')
                            output.parent.mkdir(parents=True, exist_ok=True)
                            await asyncio.to_thread(self.client.download, obj, output)
                        try:
                            await self.request('POST', self.path('/task-local-gate-ack'),
                                dict(gate, task_key=event['task_key']))
                        except WorkerError as exc:
                            if exc.status != 409:
                                raise
                    self.state['cursor'] = seq
                    await self.save()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.ctx.log.warning('Fatalder event stream disconnected; observation will reconnect')
