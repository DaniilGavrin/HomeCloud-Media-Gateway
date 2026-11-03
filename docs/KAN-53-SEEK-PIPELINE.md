# KAN-53: Gateway seek epoch, safe source opener boundary

Implemented: `roomOffer.seek_position_ms` (nullable; **zero is a valid seek**),
`Grant.SeekPositionMS` / `Lease.SeekPositionMS`, and optional
`App.SetSeekStreamOpener(directws.SeekStreamOpener)`.

An incremented offer generation with unchanged `room_id` + `playback_id`
causes Gateway `rooms.Seek`: previous work context is cancelled; old HCM1
socket is revoked by the new one-use ticket; old payloads cannot be decoded
as the new generation. Each sender retains its existing 4 MiB pending-frame
window and 64 KiB read buffer.

`SeekStreamOpener(ctx, lease) (io.ReadCloser, achievedPositionMS, error)` is a
trusted in-process adapter for future local/remote source readers and FFmpeg
pipelines. It MUST return a reader *already positioned* at the resolved
keyframe/segment. `achievedPositionMS` can differ from the requested time;
report it separately from the TV's presentation position. Reject invalid time
values. The opener must respond to context cancellation and close the reader.

**Safe default:** if a seek ticket arrives without a registered seek-capable
opener, the direct WebSocket emits `SEEK_UNSUPPORTED` and never silently opens
byte zero through the ordinary `OpenStream` callback. The control plane's
participant receipt is not a success report for a media seek.

This does not implement a demuxer, real file offsets, remote range requests,
FFmpeg or TV decoding. Those require MG-17/19 + MG-22–24 and physical Android TV
integration. Do not close KAN-53 until end-to-end seek is verified.
