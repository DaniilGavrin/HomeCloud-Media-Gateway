# KAN-52 — Bounded pause/resume of current HCM1 sender

A direct HCM1 stream registers its `Sender` under `(room_id, playback_id, generation, connection_epoch)` in a mutex-guarded `StreamControl` table. Control events arrive only on Gateway's existing privileged loopback Backend control WS. Applying `pause` gates **new** source reads in `Sender.waitCredit` and retains the direct WS connection, pending ACK window and source reader. An already-active read/write of up to one 64KiB frame can finish. `resume` reopens the wait condition and continues the same source at the same position; no new playback ID or stream is created.

Gateway returns `media.room.control.ack` only after the pause gate is set. The Gateway rejects replayed/stale command sequences, wrong playback/generation/epoch and requests for a sender that does not exist or has finished EOF. If `OpenStream` has not yet been installed, there is no current sender and the control fails with `NO_ACTIVE_STREAM`; Gateway does not invent a video stream.

An existing `media.room.closed`, STOP, or generation change revokes the direct WS ticket and ends any active stream. This feature does not change KAN-49 binary frames/backpressure.
