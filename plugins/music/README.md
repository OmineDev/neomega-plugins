# 音乐服务

向指定玩家播放 MIDI 音乐，为多个玩法插件共享声音预算。支持播放速度、音量、音色覆盖、进度查询和停止。原版音符盒音色近似 General MIDI 乐器，不要求客户端资源包。

## 调用

依赖 `neomega.music >=1.0.0`，声明所用服务权限。所有接口 major 1：

| 服务 | 参数 | 结果 |
|---|---|---|
| `plugin.neomega.music.play` | `midi_base64`、`targets` 玩家名数组；可选 `speed` 0.25–4、`volume` 0–1、`sound` 声音 ID | 播放状态 |
| `plugin.neomega.music.status` | `playback_id` | 播放状态 |
| `plugin.neomega.music.stop` | `playback_id` | 停止后的状态 |

状态含 `playback_id/state/duration/position/progress/emitted/dropped/error`。duration/position 单位秒，采用原始曲目时间；progress 0–1。state 为 playing/completed/stopped/failed/unknown。示例见 `example.py`。

可信 Host 调用上下文的安装 ID 与 generation 共同确定调用方，只能查询或停止当前代创建的任务，参数不能伪造所有者。重复提交应保留 SDK idempotency_key；未知回执先查询原服务调用，不以新请求补播。

## 配置和限制

默认每秒最多 40 次声音操作、16 个同时播放任务、16 个收件人，保留最近 128 个结束任务。音符晚于计划时间 0.25 秒或预算耗尽时跳过并计入 dropped，避免积压后集中补播。可按 config.schema.json 配置这些限制。

MIDI base64 最多 60000 字符（解码最多 45000 字节，受在线服务 64 KiB 总消息限制），最多 100000 事件、3600 秒。支持 SMF 0/1、多轨 tempo map、PPQN/SMPTE、running status、音符起止、通道与 program change。格式 2 不共享时间线，明确拒绝；音符盒为单发音，不合成持续音、力度曲线、延音踏板或 pitch bend。结束任务历史为内存数据，插件或 Host 重启后停止且不恢复历史音符。

停止会取消未发声音；已被 Host 接受的命令和正在响的短音不能撤回。发生未知或失败声音操作时停止后续音符，error 提供阶段信息，不自动重试。unknown 保留 operation_id、operation_receipt 和 operation_intent，status 在有 ID 时读取原操作；确定成功后标 stopped，仍不恢复或补播。取消在途操作同样保留 unknown。emitted 仅统计 SDK CommandResult.is_success 确认成功的游戏命令回执；Host succeeded 但 success_count 为 0 时标 failed 并保留原回执及 reason，不宣称玩家实际听见。

需要 `framework.command.execute` 和 `players:list` 权限；先解析在线玩家，再使用 SDK 精确名称选择器。插件安装后需按 Host 现有授权方式启动；本服务不改变权限或启动其他插件。

## 维护和重复调用

同一可信 call_id 在任务历史保留窗口内返回原播放任务，参数变化拒绝；窗口随已结束历史清理，重启不保留。维护 seal 在 playing/unknown 时返回 busy，否则阻止新播放并回显 token；release 仅接受同一 token，重复释放幂等。未知操作无 ID 时保留原 intent，不自动重放。
