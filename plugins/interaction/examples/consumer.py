"""Run from a consumer Plugin.on_start; register and poll under its own identity."""
import uuid

async def install_menu(ctx):
    menu = {'title': '旅行计划', 'steps': [
        {'id': 'place', 'kind': 'select', 'prompt': '选择目的地', 'options': [
            {'label': '主城', 'value': 'spawn'}, {'label': '矿区', 'value': 'mine'}]},
        {'id': 'confirm', 'kind': 'confirm', 'prompt': '确认旅行？'}]}
    async def register():
        return await ctx.services.call('plugin.neomega.interaction.register', {
            'request_id': uuid.uuid4().hex, 'menu_id': 'travel', 'commands': ['travel', '旅行'],
            'ttl_seconds': 300, 'menu': menu})
    await register()
    ctx.every(240, register, name='travel-menu-lease')
    completed = set()
    async def poll():
        result = await ctx.services.call('plugin.neomega.interaction.status', {})
        for session in result['sessions']:
            if session['state'] == 'completed' and session['session_id'] not in completed:
                completed.add(session['session_id'])
                # Business plugin checks permissions and its durable receipt before acting.
                ctx.log.info('travel selection: %s', session['answers'])
    ctx.every(1, poll, name='travel-menu-results')
