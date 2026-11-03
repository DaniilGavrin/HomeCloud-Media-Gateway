# KAN-54 — Direct WS lifecycle

The Gateway's HCM1 sender closes its source reader after writing the EOF binary frame, and emits `ENDED` on the **control** connection. It does **not** close the room or direct WS, which remains attached, can respond to control Ping, and can wait for a subsequent playback offer. Gateway-local state moves `PREPARING → BUFFERING → PLAYING → ENDED` and cancels only the playback worker. The room context is preserved and room heartbeat continues independently, including after EOF.

TV's independent decoder EOF, not the Gateway's source EOF, decides when Backend shows the `media.playback.ended` event. STOP notification from Backend calls `Book.Remove(room_id)` (revokes lease and closes direct WS), `StreamControl.RemoveRoom` (immediately invalidates sender routing) and idempotent local `rooms.Manager.Stop` (cancels all remaining workers). Duplicated close messages are harmless. Backend reconnect revokes all tickets and resets sender routing.

**Not implemented yet:** source readers, keyframe-indexed seeking, FFmpeg, and TV decoding. Tests use small, in-memory HCM1 streams and verify real WebSocket survives EOF but closes after STOP.
