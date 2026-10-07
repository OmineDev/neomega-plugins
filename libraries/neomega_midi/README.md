# neomega_midi

无网络、无线程、无文件读写的 Python 3.10+ MIDI 解析库。

```python
from neomega_midi import parse, sound_for, pitch_multiplier
sequence = parse(midi_bytes)
for note in sequence:
    print(note.time, note.duration, sound_for(note.program, note.channel, note.pitch), pitch_multiplier(note.pitch))
```

`parse(bytes, max_bytes=1048576, max_events=100000, max_duration=3600)` 返回不可变 Sequence(notes, duration, ticks_per_beat)。音符时间和长度为秒，pitch/velocity 0–127、channel 0–15、program 0–127。format 0/1 支持多轨 tempo map、PPQN/SMPTE、running status、program change、note-on/off；格式 2、截断数据、非法事件和超限输入抛 ValueError。关闭前未结束的音符以曲目结束时间截断。SMPTE 模式 ticks_per_beat 保留原始 division 值。

映射为原版 note-block 近似音色，鼓通道按音高选择鼓/军鼓/镲；不是 GM 音源合成器。声音播放、预算、生命周期由 neomega.music 提供，不属于本库。

发行使用 library.json 的显式文件清单；服务发行由统一打包器嵌入本库，不需要运行期 pip 安装。
