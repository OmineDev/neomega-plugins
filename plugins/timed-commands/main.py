"""定时命令：按固定间隔或每天的固定时刻执行命令、向全服广播。

每 10 秒检查一次全部任务，只开一个存储事务：到期任务的新时段、要执行的命令
和广播一起提交，要么全部生效，要么全部不生效。每个任务只保存上一次处理过的
时段起点，同一时段不会执行两次。提交结果不明时只记日志，下一轮重新读取：
Host 对整个安装做全局版本校验，同一份快照不可能提交两次。
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import re

from neomega_runtime import Plugin, PluginContext
from neomega_runtime.managed import IPCRejected
from neomega_runtime.storage import CommitUncertain


@dataclass(frozen=True)
class Task:
    name: str = field(metadata={'title': '任务名', 'description': '每个任务的名字不能重复'})
    every_minutes: int = field(default=0, metadata={
        'title': '间隔分钟', 'description': '每隔多少分钟执行一次；与"每日时刻"二选一，不用时填 0', 'minimum': 0})
    daily_at: str = field(default='', metadata={
        'title': '每日时刻', 'description': '每天在这个时刻执行，格式 HH:MM，例如 20:00；与"间隔分钟"二选一'})
    commands: list[str] = field(default_factory=list, metadata={'title': '执行命令'})
    message: str = field(default='', metadata={'title': '全服广播', 'description': '留空则不广播'})

    def __post_init__(self):
        if not self.name.strip():
            raise ValueError('task name must not be empty')
        if (self.every_minutes > 0) == bool(self.daily_at):
            raise ValueError(f'task {self.name}: set exactly one of every_minutes or daily_at')
        if self.daily_at and not re.fullmatch(r'([01]\d|2[0-3]):[0-5]\d', self.daily_at):
            raise ValueError(f'task {self.name}: daily_at must be HH:MM')
        if any(not command.strip() for command in self.commands):
            raise ValueError(f'task {self.name}: commands must not be empty')
        if not self.commands and not self.message:
            raise ValueError(f'task {self.name}: needs commands or a message')


@dataclass(frozen=True)
class Settings:
    utc_offset_hours: int = field(default=8, metadata={
        'title': '时区', 'description': '与 UTC 相差的小时数，北京时间填 8；用于每日时刻', 'minimum': -12, 'maximum': 14})
    grace_minutes: int = field(default=5, metadata={
        'title': '补执行宽限（分钟）',
        'description': '到点后插件不在线超过这么久，就跳过这一次，不再补执行', 'minimum': 1})
    tasks: list[Task] = field(default_factory=lambda: [
        Task('整点公告', every_minutes=60, message='感谢游玩，祝你玩得开心！'),
    ], metadata={'title': '定时任务', 'description': '最多 64 个'})

    def __post_init__(self):
        if len(self.tasks) > 64:
            raise ValueError('at most 64 tasks')
        if len({task.name for task in self.tasks}) != len(self.tasks):
            raise ValueError('task names must be unique')


def deadline() -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()


def task_key(task) -> str:
    return 'task:' + task.name


def period_start(task, now, utc_offset_hours) -> int:
    """当前时段的起点，以 Unix 纪元起的分钟数表示。"""
    minutes = int(now.timestamp()) // 60
    if task.every_minutes:
        return minutes // task.every_minutes * task.every_minutes
    offset = timedelta(hours=utc_offset_hours)
    local = now + offset
    hour, minute = map(int, task.daily_at.split(':'))
    start = local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if start > local:
        start -= timedelta(days=1)
    return int((start - offset).timestamp()) // 60


def broadcast(message) -> str:
    return 'tellraw @a ' + json.dumps({'rawtext': [{'text': message}]}, ensure_ascii=False)


class TimedCommands(Plugin):
    config_type = Settings

    async def on_start(self, ctx: PluginContext):
        ctx.every(10, lambda: self.check(ctx), name='timed-commands')

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def check(self, ctx):
        tasks, now = ctx.config.tasks, self.now()
        try:
            tx = await ctx.storage.transaction(keys=[task_key(task) for task in tasks])
            due = False
            for index, task in enumerate(tasks):
                start, last = period_start(task, now, ctx.config.utc_offset_hours), tx.get(task_key(task))
                if last is not None and start <= last:
                    continue
                tx.set(task_key(task), start)
                due = True
                # 首次见到这个任务只记下当前时段；错过太久说明插件那时不在线，不补执行。
                if last is None or now.timestamp() - start * 60 > ctx.config.grace_minutes * 60:
                    continue
                lines = [line.strip().removeprefix('/') for line in task.commands]
                if task.message:
                    lines.append(broadcast(task.message))
                for number, line in enumerate(lines):
                    tx.action(ctx.commands.prepare(line, idempotency_key=f'{tx.commit_id}_{index}_{number}',
                                                   deadline=deadline()))
            if due:
                await tx.save()
        except (CommitUncertain, IPCRejected) as exc:
            ctx.log.warning('定时任务本轮未能保存，下一轮重新检查：%s', exc)


if __name__ == '__main__':
    TimedCommands().run()
