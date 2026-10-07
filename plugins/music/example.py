"""Consumer declares neomega.music dependency and play/status/stop permissions."""
import base64


async def play_file(ctx, midi_bytes, player_name):
    playback = await ctx.services.call('plugin.neomega.music.play', {
        'midi_base64': base64.b64encode(midi_bytes).decode('ascii'),
        'targets': [player_name], 'speed': 1.0, 'volume': 0.8,
    })
    return await ctx.services.call('plugin.neomega.music.status', {
        'playback_id': playback['playback_id'],
    })
