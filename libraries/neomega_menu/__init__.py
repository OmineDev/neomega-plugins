"""Numbered chat menus that run inside one plugin, on top of ctx.chat_waiters.

    async def on_event(self, ctx, event):
        async with self.lock:
            if await neomega_menu.feed(ctx, event):
                return
            ...

    async def shop(self, ctx, player):          # started with ctx.spawn(...)
        index = await neomega_menu.choose(ctx, player, '商店', ['面包', '钻石'])

Menus wait for a later chat event, so run them from ctx.spawn, never inside
on_event. feed() acknowledges the reply event; hold the same lock around feed()
and around the menu flow's own storage commits, because both advance the
installation's single storage revision. Prompts are sent directly and are not
stored; a restart simply closes open menus.
"""
from datetime import datetime, timedelta, timezone
import uuid

__all__ = ['feed', 'say', 'ask', 'choose', 'confirm']

CANCEL_WORDS = frozenset({'0', '取消', 'q', 'quit', 'exit'})
YES_WORDS = frozenset({'1', '是', '确认', '确定', 'y', 'yes'})
MAX_ATTEMPTS = 3


async def feed(ctx, event) -> bool:
    """Route a chat reply to its waiting menu. True means it was consumed and acknowledged."""
    if not await ctx.chat_waiters.feed(event):
        return False
    await (await ctx.storage.transaction(event=event.payload, keys=[])).save()
    return True


async def say(ctx, player, text: str) -> None:
    """Private message outside any storage transaction."""
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat()
    plan = ctx.notifications.prepare(player['name'], text, idempotency_key='menu_' + uuid.uuid4().hex,
                                     deadline=deadline)
    for part in plan.parts:
        await ctx.operations.submit(part.intent)


async def ask(ctx, player, question: str, *, timeout: float = 30) -> str | None:
    """Send a question and return the player's next chat line, or None on timeout or cancel."""
    try:
        # Register before prompting so a fast reply cannot arrive first.
        reply = ctx.chat_waiters.wait_for(player, timeout=timeout)
    except ValueError:
        await say(ctx, player, '你还有一个菜单没有完成，请先回复它或输入 0 取消。')
        return None
    await say(ctx, player, question)
    try:
        text = (await reply).message.strip()
    except TimeoutError:
        await say(ctx, player, '太久没有回复，菜单已关闭。')
        return None
    if text.lower() in CANCEL_WORDS:
        await say(ctx, player, '已取消。')
        return None
    return text


async def choose(ctx, player, title: str, options: list[str], *, timeout: float = 30) -> int | None:
    """Show numbered options; return the chosen zero-based index, or None."""
    if not options:
        raise ValueError('menu needs at least one option')
    prompt = '\n'.join([title, *(f'{number}. {option}' for number, option in enumerate(options, 1)),
                        f'输入编号 1-{len(options)} 选择，输入 0 取消。'])
    for _ in range(MAX_ATTEMPTS):
        text = await ask(ctx, player, prompt, timeout=timeout)
        if text is None:
            return None
        if text.isdigit() and 1 <= int(text) <= len(options):
            return int(text) - 1
        prompt = f'没有这个选项，请输入 1-{len(options)} 之间的编号，输入 0 取消。'
    await say(ctx, player, '输入错误次数过多，菜单已关闭。')
    return None


async def confirm(ctx, player, question: str, *, timeout: float = 30) -> bool:
    """Yes/no question; anything other than a yes word counts as no."""
    text = await ask(ctx, player, question + '\n输入 1 确认，输入 0 取消。', timeout=timeout)
    if text is None:
        return False
    if text.lower() not in YES_WORDS:
        await say(ctx, player, '已取消。')
        return False
    return True
