"""举报与留言：玩家提交举报（附带坐标）与意见留言，管理员查办回复并支持离线进服送达。

每条举报和留言独立保存在存储键 report:<id> 中，紧凑索引 index 仅保存编号、类型、
状态与提交人标识；超出 max_records 时优先淘汰最旧的已处理（resolved）记录，仍超出则
裁剪最旧记录并删除对应的 report:<id>。旧版单键 board 会在首次读写时分批无感迁移。
提交冷却跨重启保存于 cd:<pid>，离线处理通知按条数上限保存于 inbox:<pid>。

每次事件处理只做一次存储提交，状态变更、通知和事件确认一起落盘。位置采样与在线列表
查询放在打开存储事务之前，采样失败降级为无坐标，不阻塞举报提交。
"""
import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import math
import time

from neomega_runtime import AmbiguousPlayer, ChatRouter, Event, Plugin, PluginContext, PlayerNotFound
from neomega_runtime.http import ProtocolError
from neomega_runtime.managed import IPCError

BOARD = 'board'
INDEX = 'index'
MIGRATE_BATCH = 50
MAX_INBOX_ITEMS = 20
MAX_STORAGE_KEY_BYTES = 256
MAX_TEXT_LENGTH = 200
PAGE_SIZE = 5
# 位置查询（querytarget）报的是玩家眼睛高度，脚坐标比它低 1.62 格。
EYE_HEIGHT = 1.62
# 重启或重连后 Host 会补发积压事件，间隔超过 CATCH_UP_GAP（或总时长超过 CATCH_UP_LIMIT）
# 视为实时事件，避免启动补发期间重复向进服管理员刷屏提醒。
CATCH_UP_GAP = 1.0
CATCH_UP_LIMIT = 60.0
BEIJING = timezone(timedelta(hours=8))
DIMENSION_NAMES = {0: '主世界', 1: '下界', 2: '末地'}
KINDS = {'report': '举报', 'feedback': '留言'}
STATUSES = {'open': '待处理', 'resolved': '已处理'}


@dataclass(frozen=True)
class Settings:
    admins: list[str] = field(default_factory=list, metadata={
        'title': '管理员',
        'description': '可以使用 !查举报、!查全部举报、!处理举报 并接收举报提醒的玩家名字，不区分大小写'})
    cooldown_seconds: int = field(default=60, metadata={
        'title': '提交冷却（秒）',
        'description': '同一名玩家两次提交 !举报 或 !留言 之间至少间隔的秒数，填 0 不限制',
        'minimum': 0})
    max_records: int = field(default=200, metadata={
        'title': '最多保留记录数',
        'description': '超出后优先淘汰最旧的已处理记录，仍超出则裁剪最旧记录',
        'minimum': 20, 'maximum': 1000})
    allow_feedback: bool = field(default=True, metadata={
        'title': '允许玩家留言',
        'description': '关闭后玩家无法使用 !留言 命令'})
    notify_admins_on_submit: bool = field(default=True, metadata={
        'title': '提交时提醒在线管理员',
        'description': '玩家提交新举报或留言时，私聊通知当前在线的管理员'})
    notify_admins_on_join: bool = field(default=True, metadata={
        'title': '管理员进服时提醒待处理数',
        'description': '管理员进服且存在待处理记录时，私聊提醒条数'})

    def __post_init__(self):
        if any(not name.strip() for name in self.admins):
            raise ValueError('admin names must not be empty')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def player_id(player) -> str:
    return player['uuid'] or 'xuid-' + player['xuid']


def cd_key(pid: str) -> str:
    return 'cd:' + pid


def inbox_key(pid: str) -> str:
    return 'inbox:' + pid


def report_key(record_id: int) -> str:
    return f'report:{record_id}'


def make_index_entry(record: dict) -> list:
    kind = 'r' if record.get('kind') == 'report' else 'f'
    status = 'x' if record.get('status') == 'resolved' else 'o'
    pid = record.get('reporter_pid') or ''
    owner = pid if pid else 'name:' + str(record.get('reporter', ''))
    return [int(record['id']), kind, status, owner]


def entry_matches_player(entry: list, pid: str, name: str) -> bool:
    owner = entry[3]
    if owner.startswith('name:'):
        return owner[5:] == name
    return owner == pid


def trim_entries(entries: list[list], limit: int) -> tuple[list[list], list[int]]:
    kept = list(entries)
    evicted: list[int] = []
    while len(kept) > limit:
        idx = next((i for i, row in enumerate(kept) if row[2] == 'x'), None)
        if idx is not None:
            evicted.append(kept.pop(idx)[0])
        else:
            drop = len(kept) - limit
            evicted.extend(row[0] for row in kept[:drop])
            kept = kept[drop:]
            break
    return kept, evicted


def coordinate(value) -> str:
    text = f'{float(value):.2f}'.rstrip('0').rstrip('.')
    return '0' if text == '-0' else text


def format_pos(pos: dict | None) -> str:
    if not pos:
        return ''
    dim = DIMENSION_NAMES.get(pos['dimension'], f'维度{pos["dimension"]}')
    return f'{dim} {coordinate(pos["x"])} {coordinate(pos["y"])} {coordinate(pos["z"])}'


def format_time(iso_text: str) -> str:
    return datetime.fromisoformat(iso_text).astimezone(BEIJING).strftime('%m-%d %H:%M')


def extract_feet_pos(entity: dict) -> dict | None:
    if not isinstance(entity, dict):
        return None
    dim, pos = entity.get('dimension'), entity.get('position')
    if type(dim) is not int or not isinstance(pos, dict):
        return None
    coords = {}
    for axis in ('x', 'y', 'z'):
        val = pos.get(axis)
        if type(val) not in (int, float) or not math.isfinite(val):
            return None
        coords[axis] = round(float(val) - (EYE_HEIGHT if axis == 'y' else 0.0), 2)
    return {'dimension': dim, **coords}


def match_entity(player: dict, entities: list[dict]) -> dict | None:
    uuid_val = (player.get('uuid') or '').casefold()
    name_val = player.get('name', '')
    for entity in entities:
        if not isinstance(entity, dict):
            continue
        uid = entity.get('uniqueId')
        euuid = entity.get('uuid')
        if uuid_val and ((isinstance(uid, str) and uid.casefold() == uuid_val)
                         or (isinstance(euuid, str) and euuid.casefold() == uuid_val)):
            return extract_feet_pos(entity)
        if name_val and (entity.get('name') == name_val or (not uuid_val and uid == name_val)):
            return extract_feet_pos(entity)
    return None


def find_online(roster: list[dict], name: str) -> dict | None:
    exact = next((p for p in roster if p['name'] == name), None)
    if exact is not None:
        return exact
    folded = name.casefold()
    return next((p for p in roster if p['name'].casefold() == folded), None)


def trim_records(records: list[dict], limit: int) -> list[dict]:
    kept = list(records)
    while len(kept) > limit:
        idx = next((i for i, row in enumerate(kept) if row.get('status') == 'resolved'), None)
        if idx is not None:
            kept.pop(idx)
        else:
            return kept[-limit:]
    return kept


def check_cooldown(last: float | None, now: float, cooldown: int) -> int:
    if last is None or cooldown <= 0:
        return 0
    wait = min(last + cooldown - now, cooldown)
    return math.ceil(wait) if wait > 0 else 0


def format_admin_record(record: dict) -> str:
    at = format_time(record['created_at'])
    status = STATUSES.get(record['status'], record['status'])
    if record['kind'] == 'report':
        head = f'#{record["id"]} [{status}] 举报 {record["target"]}（提交人：{record["reporter"]}，{at}）'
        lines = [head, f'  原因：{record["reason"]}']
        coords = []
        if record.get('reporter_pos'):
            coords.append('举报人 ' + format_pos(record['reporter_pos']))
        if record.get('target_pos'):
            coords.append('目标 ' + format_pos(record['target_pos']))
        lines.append('  坐标：' + ('；'.join(coords) if coords else '无'))
    else:
        head = f'#{record["id"]} [{status}] 留言（提交人：{record["reporter"]}，{at}）'
        lines = [head, f'  内容：{record["reason"]}']
    if record['status'] == 'resolved':
        rat = format_time(record['resolved_at']) if record.get('resolved_at') else ''
        lines.append(f'  处理（{record.get("resolved_by", "管理员")} {rat}）：{record.get("reply", "")}')
    return '\n'.join(lines)


def format_my_record(record: dict) -> str:
    at = format_time(record['created_at'])
    status = STATUSES.get(record['status'], record['status'])
    label = f'举报 {record["target"]}' if record['kind'] == 'report' else '留言'
    text = f'#{record["id"]} [{status}] {label}（{at}）：{record["reason"]}'
    if record['status'] == 'resolved':
        text += f'\n  回复（{record.get("resolved_by", "管理员")}）：{record.get("reply", "")}'
    return text


def format_inbox_item(item: dict) -> str:
    who = item.get('resolved_by', '管理员')
    reply = item.get('reply', '')
    if item.get('kind') == 'report':
        return f'你的举报 #{item["id"]}（目标：{item.get("target", "")}）已处理（{who}）：{reply}'
    return f'你的留言 #{item["id"]} 已有回复（{who}）：{reply}'


class Reports(Plugin):
    config_type = Settings

    def __init__(self):
        self.lock = asyncio.Lock()
        self.bot_name = None
        self.started = self.last_event = None
        self.caught_up = False

    async def on_start(self, ctx: PluginContext):
        self.bot_name = None
        self.started = self.now()
        # 全角感叹号是中文输入法的默认输出，两个前缀都接受。
        self.routers = (ctx.chat, ChatRouter(prefix='！'))
        for router in self.routers:
            router.command('举报', aliases=('jb', 'report'), description='举报违规玩家',
                           usage='<玩家名> <原因...>')(
                lambda command: self.submit_report(ctx, command))
            router.command('留言', aliases=('ly', 'feedback'), description='向管理员提交建议或留言',
                           usage='<内容...>')(
                lambda command: self.submit_feedback(ctx, command))
            router.command('我的举报', aliases=('wdjb', 'myreports'), description='查看自己最近 5 条举报或留言',
                           max_args=0)(
                lambda command: self.my_reports(ctx, command))
            router.command('查举报', aliases=('cjb', 'reports'), description='管理员分页查看待处理举报或留言',
                           usage='[页码]', max_args=1)(
                lambda command: self.list_reports(ctx, command, only_open=True))
            router.command('查全部举报', aliases=('qbjb', 'allreports'), description='管理员分页查看全部举报和留言',
                           usage='[页码]', max_args=1)(
                lambda command: self.list_reports(ctx, command, only_open=False))
            router.command('处理举报', aliases=('cljb', 'resolve'), description='管理员处理并回复举报或留言',
                           usage='<编号> <回复内容...>')(
                lambda command: self.resolve_report(ctx, command))
        async with self.lock:
            await self._ensure_migrated(ctx)

    async def on_event(self, ctx: PluginContext, event: Event):
        self.watch_backlog()
        try:
            if event.kind == 'player.list_added' and await self.on_join(ctx, event):
                return
            if event.kind == 'chat.received':
                for router in self.routers:
                    if await router.dispatch(event):
                        return
        except (ValueError, PlayerNotFound, AmbiguousPlayer):
            # 引号不配对、参数个数不对，或玩家已经下线：不回复，只确认事件。
            pass
        async with self.lock:
            await (await event.transaction(keys=[])).save()

    async def bot(self, ctx: PluginContext) -> str | None:
        try:
            name = (await ctx.robot()).get('bot_name')
        except (IPCError, ProtocolError, TimeoutError, ConnectionError, OSError):
            return self.bot_name
        if name:
            self.bot_name = name
        return self.bot_name

    async def _migrate_board(self, ctx: PluginContext, legacy: dict) -> dict:
        """把旧版单键 board 分批拆分为 report:<id> 与紧凑索引 index，最后删除 board。"""
        records = [r for r in legacy.get('records', []) if isinstance(r, dict) and type(r.get('id')) is int]
        records = trim_records(records, ctx.config.max_records)
        next_id = max(int(legacy.get('next_id', 1)), max((r['id'] for r in records), default=0) + 1)
        entries = [make_index_entry(r) for r in records]
        index = {'next_id': next_id, 'entries': entries}

        for start in range(0, len(records), MIGRATE_BATCH):
            batch = records[start:start + MIGRATE_BATCH]
            tx = await ctx.storage.transaction(keys=[])
            for row in batch:
                tx.set(report_key(int(row['id'])), row)
            if start + MIGRATE_BATCH >= len(records):
                tx.set(INDEX, index)
                tx.delete(BOARD)
            await tx.save()
        if not records:
            tx = await ctx.storage.transaction(keys=[])
            tx.set(INDEX, index)
            tx.delete(BOARD)
            await tx.save()
        return index

    async def _ensure_migrated(self, ctx: PluginContext) -> dict:
        snap = await ctx.storage.transaction(keys=[INDEX, BOARD])
        legacy = snap.get(BOARD)
        if isinstance(legacy, dict):
            return await self._migrate_board(ctx, legacy)
        return snap.get(INDEX) or {'next_id': 1, 'entries': []}

    def now(self) -> float:
        return time.monotonic()

    def clock(self) -> float:
        return time.time()

    def now_beijing(self) -> datetime:
        return datetime.fromtimestamp(self.clock(), tz=BEIJING)

    def watch_backlog(self):
        """判断启动时补发的积压旧事件是否已经处理完。"""
        now = self.now()
        if not self.caught_up:
            last = self.last_event if self.last_event is not None else self.started
            if last is None or now - last >= CATCH_UP_GAP or now - self.started >= CATCH_UP_LIMIT:
                self.caught_up = True
        self.last_event = now

    def is_admin(self, ctx: PluginContext, player: dict) -> bool:
        wanted = {name.strip().casefold() for name in ctx.config.admins}
        return player['name'].casefold() in wanted or (bool(player.get('uuid')) and player['uuid'].casefold() in wanted)

    @staticmethod
    async def resolve_joined(ctx: PluginContext, name: str, xuid: str):
        """按名字和 XUID 确认进服玩家；兼容在线名单不带 XUID 的 Host。"""
        try:
            return await ctx.resolve_player(name=name, xuid=xuid or None)
        except PlayerNotFound:
            if not xuid:
                raise
            player = await ctx.resolve_player(name=name)
            if player.get('xuid'):
                raise
            return player

    async def sample_positions(self, ctx: PluginContext) -> list[dict]:
        try:
            sample = await ctx.positions.sample(target='players', limit=64)
        except Exception as error:
            ctx.log.warning('position sample unavailable: %s', type(error).__name__)
            return []
        entities = sample.get('entities') if isinstance(sample, dict) else None
        return entities if isinstance(entities, list) else []

    async def _cooldown_wait(self, ctx: PluginContext, pid: str) -> int:
        if ctx.config.cooldown_seconds <= 0:
            return 0
        return check_cooldown(await ctx.storage.get(cd_key(pid)), self.clock(), ctx.config.cooldown_seconds)

    # ---- 玩家命令 ----

    async def submit_report(self, ctx: PluginContext, command):
        if len(command.args) < 2:
            return await self.answer(ctx, command, '用法：!举报 <玩家名> <原因>')
        target_name = command.args[0].strip()
        reason = ' '.join(command.args[1:]).strip()
        if not target_name:
            return await self.answer(ctx, command, '请填写被举报的玩家名。')
        if not reason:
            return await self.answer(ctx, command, f'举报原因不能为空（最多 {MAX_TEXT_LENGTH} 字）。')
        if len(reason) > MAX_TEXT_LENGTH:
            return await self.answer(ctx, command, f'举报原因最多 {MAX_TEXT_LENGTH} 个字（当前 {len(reason)} 字）。')

        wait = await self._cooldown_wait(ctx, player_id(command.player))
        if wait > 0:
            return await self.answer(ctx, command, f'提交还在冷却中，请 {wait} 秒后再试。')

        roster = await ctx.players()
        entities = await self.sample_positions(ctx)
        online_target = find_online(roster, target_name)
        canonical_target = online_target['name'] if online_target is not None else target_name
        reporter_pos = match_entity(command.player, entities)
        target_pos = match_entity(online_target, entities) if online_target is not None else None

        await self._commit_submission(
            ctx, command, kind='report', target=canonical_target, reason=reason,
            reporter_pos=reporter_pos, target_pos=target_pos, roster=roster)

    async def submit_feedback(self, ctx: PluginContext, command):
        if not ctx.config.allow_feedback:
            return await self.answer(ctx, command, '当前未开放留言功能。')
        content = ' '.join(command.args).strip()
        if not content:
            return await self.answer(ctx, command, '用法：!留言 <内容>')
        if len(content) > MAX_TEXT_LENGTH:
            return await self.answer(ctx, command, f'留言内容最多 {MAX_TEXT_LENGTH} 个字（当前 {len(content)} 字）。')

        wait = await self._cooldown_wait(ctx, player_id(command.player))
        if wait > 0:
            return await self.answer(ctx, command, f'提交还在冷却中，请 {wait} 秒后再试。')

        roster = await ctx.players() if ctx.config.notify_admins_on_submit else []
        await self._commit_submission(
            ctx, command, kind='feedback', target='', reason=content,
            reporter_pos=None, target_pos=None, roster=roster)

    async def _commit_submission(self, ctx: PluginContext, command, *, kind: str, target: str,
                                 reason: str, reporter_pos: dict | None, target_pos: dict | None,
                                 roster: list[dict]):
        player = command.player
        pid = player_id(player)
        ckey = cd_key(pid)
        async with self.lock:
            tx = await command.event.transaction(keys=[INDEX, BOARD, ckey])
            if isinstance(tx.get(BOARD), dict):
                await self._migrate_board(ctx, tx.get(BOARD))
                tx = await command.event.transaction(keys=[INDEX, ckey])
            now = self.clock()
            wait = check_cooldown(tx.get(ckey), now, ctx.config.cooldown_seconds)
            if wait > 0:
                self.reply(ctx, tx, player['name'], f'提交还在冷却中，请 {wait} 秒后再试。')
                return await tx.save()

            index = tx.get(INDEX) or {'next_id': 1, 'entries': []}
            record_id = int(index.get('next_id', 1))
            record = {
                'id': record_id,
                'kind': kind,
                'status': 'open',
                'reporter': player['name'],
                'reporter_pid': pid,
                'target': target,
                'reason': reason,
                'created_at': self.now_beijing().isoformat(timespec='seconds'),
                'reporter_pos': reporter_pos,
                'target_pos': target_pos,
            }
            kept, evicted = trim_entries(list(index.get('entries', [])) + [make_index_entry(record)],
                                         ctx.config.max_records)
            tx.set(INDEX, {'next_id': record_id + 1, 'entries': kept})
            tx.set(report_key(record_id), record)
            for old_id in evicted:
                tx.delete(report_key(old_id))
            if ctx.config.cooldown_seconds > 0:
                tx.set(ckey, now)

            if kind == 'report':
                self.reply(ctx, tx, player['name'], f'已提交举报 #{record_id}（目标：{target}），管理员会尽快处理。')
                coords = []
                if reporter_pos:
                    coords.append('举报人 ' + format_pos(reporter_pos))
                if target_pos:
                    coords.append('目标 ' + format_pos(target_pos))
                coord_note = (' | ' + '，'.join(coords)) if coords else ''
                alert = f'[新举报 #{record_id}] {player["name"]} 举报 {target}：{reason}{coord_note}（发 !查举报 查看）'
            else:
                self.reply(ctx, tx, player['name'], f'已提交留言 #{record_id}，感谢你的反馈。')
                alert = f'[新留言 #{record_id}] {player["name"]}：{reason}（发 !查举报 查看）'

            if ctx.config.notify_admins_on_submit:
                self._notify_online_admins(ctx, tx, roster, alert, exclude_pid=pid)
            await tx.save()

    def _notify_online_admins(self, ctx: PluginContext, tx, roster: list[dict], alert: str, *, exclude_pid: str):
        seen_names = set()
        idx = 0
        for member in roster:
            if player_id(member) == exclude_pid or not self.is_admin(ctx, member):
                continue
            name = member['name']
            if name in seen_names:
                continue
            seen_names.add(name)
            try:
                self.reply(ctx, tx, name, alert, suffix=f'_admin_{idx}')
                idx += 1
            except ValueError:
                ctx.log.warning('skipping admin notification for unsafe name')

    async def my_reports(self, ctx: PluginContext, command):
        pid = player_id(command.player)
        name = command.player['name']
        async with self.lock:
            index = await self._ensure_migrated(ctx)
            latest_ids = [row[0] for row in index.get('entries', [])
                          if entry_matches_player(row, pid, name)][-5:]
            tx = await command.event.transaction(keys=[report_key(rid) for rid in latest_ids])
            recent = [row for rid in reversed(latest_ids)
                      if (row := tx.get(report_key(rid))) is not None]
            if not recent:
                text = '你还没有提交过举报或留言。'
            else:
                text = '\n'.join(['你最近的举报与留言（北京时间）：'] + [format_my_record(r) for r in recent])
            self.reply(ctx, tx, name, text)
            await tx.save()

    # ---- 管理员命令 ----

    async def list_reports(self, ctx: PluginContext, command, *, only_open: bool):
        if not self.is_admin(ctx, command.player):
            return await self.answer(ctx, command, '只有管理员可以查看举报列表。')
        page = 1
        if command.args:
            raw = command.args[0].strip()
            if not (raw.isascii() and raw.isdigit() and int(raw) >= 1):
                cmd_name = '!查举报' if only_open else '!查全部举报'
                return await self.answer(ctx, command, f'页码请填写大于 0 的整数，例如 {cmd_name} 1')
            page = int(raw)

        async with self.lock:
            index = await self._ensure_migrated(ctx)
            entries = list(reversed(index.get('entries', [])))
            if only_open:
                entries = [row for row in entries if row[2] == 'o']
            if not entries:
                tx = await command.event.transaction(keys=[])
                text = '当前没有待处理的举报或留言。' if only_open else '目前还没有任何举报或留言记录。'
            else:
                total_pages = math.ceil(len(entries) / PAGE_SIZE)
                if page > total_pages:
                    tx = await command.event.transaction(keys=[])
                    text = f'共 {total_pages} 页（{len(entries)} 条），请查看第 1 到 {total_pages} 页。'
                else:
                    start = (page - 1) * PAGE_SIZE
                    page_ids = [row[0] for row in entries[start:start + PAGE_SIZE]]
                    tx = await command.event.transaction(keys=[report_key(rid) for rid in page_ids])
                    chunk = [row for rid in page_ids if (row := tx.get(report_key(rid))) is not None]
                    title = f'待处理举报与留言（第 {page}/{total_pages} 页，共 {len(entries)} 条）：' if only_open \
                        else f'全部举报与留言（第 {page}/{total_pages} 页，共 {len(entries)} 条）：'
                    lines = [title] + [format_admin_record(r) for r in chunk]
                    if any(r.get('status') == 'open' for r in chunk):
                        lines.append('发送 !处理举报 <编号> <回复内容> 标记为已处理。')
                    text = '\n'.join(lines)
            self.reply(ctx, tx, command.player['name'], text)
            await tx.save()

    async def resolve_report(self, ctx: PluginContext, command):
        if not self.is_admin(ctx, command.player):
            return await self.answer(ctx, command, '只有管理员可以处理举报。')
        if len(command.args) < 2:
            return await self.answer(ctx, command, '用法：!处理举报 <编号> <回复内容>')
        raw_id = command.args[0].strip().lstrip('#')
        if not (raw_id.isascii() and raw_id.isdigit() and int(raw_id) >= 1):
            return await self.answer(ctx, command, '举报编号请填写正整数，例如 !处理举报 1 已核实处理')
        target_id = int(raw_id)
        reply_text = ' '.join(command.args[1:]).strip()
        if not reply_text:
            return await self.answer(ctx, command, '回复内容不能为空。')
        if len(reply_text) > MAX_TEXT_LENGTH:
            return await self.answer(ctx, command, f'回复内容最多 {MAX_TEXT_LENGTH} 个字（当前 {len(reply_text)} 字）。')

        rkey = report_key(target_id)
        async with self.lock:
            await self._ensure_migrated(ctx)
        preview = await ctx.storage.get(rkey)
        if not isinstance(preview, dict):
            return await self.answer(ctx, command, f'没有找到编号 #{target_id} 的记录。')

        reporter_pid = preview.get('reporter_pid', '')
        raw_ikey = inbox_key(reporter_pid) if reporter_pid else None
        ikey = raw_ikey if raw_ikey and len(raw_ikey.encode('utf-8')) <= MAX_STORAGE_KEY_BYTES else None
        roster = await ctx.players()
        keys = [INDEX, rkey, ikey] if ikey else [INDEX, rkey]

        async with self.lock:
            tx = await command.event.transaction(keys=keys)
            record = tx.get(rkey)
            if not isinstance(record, dict):
                self.reply(ctx, tx, command.player['name'], f'没有找到编号 #{target_id} 的记录。')
                return await tx.save()
            if record.get('status') == 'resolved':
                self.reply(ctx, tx, command.player['name'],
                           f'编号 #{target_id} 已经由 {record.get("resolved_by", "管理员")} 处理过了。')
                return await tx.save()

            resolved_at = self.now_beijing().isoformat(timespec='seconds')
            record['status'] = 'resolved'
            record['reply'] = reply_text
            record['resolved_by'] = command.player['name']
            record['resolved_at'] = resolved_at
            tx.set(rkey, record)

            index = tx.get(INDEX) or {'next_id': target_id + 1, 'entries': []}
            entries = list(index.get('entries', []))
            for row in entries:
                if row[0] == target_id:
                    row[2] = 'x'
                    break
            tx.set(INDEX, {'next_id': int(index.get('next_id', target_id + 1)), 'entries': entries})

            item = {
                'id': target_id,
                'kind': record['kind'],
                'target': record.get('target', ''),
                'reply': reply_text,
                'resolved_by': command.player['name'],
                'resolved_at': resolved_at,
            }
            online_reporter = (next((p for p in roster if player_id(p) == reporter_pid), None)
                               if reporter_pid else find_online(roster, record['reporter']))
            if online_reporter is not None:
                try:
                    self.reply(ctx, tx, online_reporter['name'], format_inbox_item(item), suffix='_notify')
                    admin_msg = f'已处理 #{target_id}，并已私聊通知 {online_reporter["name"]}。'
                except ValueError:
                    ctx.log.warning('online reporter notification skipped for unsafe name')
                    admin_msg = f'已处理 #{target_id}。'
            elif ikey:
                inbox = list(tx.get(ikey, []))
                inbox.append(item)
                tx.set(ikey, inbox[-MAX_INBOX_ITEMS:])
                admin_msg = f'已处理 #{target_id}，{record["reporter"]} 当前不在线，将在下次进服时自动通知。'
            else:
                admin_msg = f'已处理 #{target_id}。'

            self.reply(ctx, tx, command.player['name'], admin_msg)
            await tx.save()

    # ---- 进服通知 ----

    async def on_join(self, ctx: PluginContext, event: Event) -> bool:
        body = event.payload.get('payload')
        if not isinstance(body, dict) or not isinstance(body.get('name'), str) or not body['name']:
            return False
        player = await self.resolve_joined(ctx, body['name'], body.get('xuid') or '')
        bot = await self.bot(ctx)
        if bot and player['name'] == bot:
            return False
        pid = player_id(player)
        raw_ikey = inbox_key(pid)
        ikey = raw_ikey if len(raw_ikey.encode('utf-8')) <= MAX_STORAGE_KEY_BYTES else None
        is_live = self.caught_up and event.payload.get('session', ctx.operations.session) == ctx.operations.session
        check_admin = bool(ctx.config.notify_admins_on_join and is_live and self.is_admin(ctx, player))

        async with self.lock:
            index = await self._ensure_migrated(ctx) if check_admin else None
            keys = [ikey] if ikey else []
            tx = await event.transaction(keys=keys)
            messages = []
            inbox = tx.get(ikey, []) if ikey else []
            if inbox:
                messages.extend(format_inbox_item(item) for item in inbox)
            if check_admin and index is not None:
                open_rows = [row for row in index.get('entries', []) if row[2] == 'o']
                if open_rows:
                    reports = sum(1 for row in open_rows if row[1] == 'r')
                    feedbacks = len(open_rows) - reports
                    messages.append(
                        f'当前有 {len(open_rows)} 条待处理记录（举报 {reports} 条、留言 {feedbacks} 条），发送 !查举报 查看。'
                    )
            if messages:
                self.reply(ctx, tx, player['name'], '\n'.join(messages))
            if inbox and ikey:
                tx.delete(ikey)
            await tx.save()
        return True

    # ---- 公用 ----

    async def answer(self, ctx: PluginContext, command, text: str):
        async with self.lock:
            tx = await command.event.transaction(keys=[])
            self.reply(ctx, tx, command.player['name'], text)
            await tx.save()

    def reply(self, ctx: PluginContext, tx, name: str, text: str, *, suffix: str = '_reply'):
        plan = ctx.notifications.prepare(name, text, idempotency_key=tx.commit_id + suffix,
                                         deadline=deadline())
        for part in plan.parts:
            tx.action(part.intent)


if __name__ == '__main__':
    Reports().run()
