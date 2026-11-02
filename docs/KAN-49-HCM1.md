# KAN-49 / MG-09 — HCM1 media frames and bounded flow control

Baseline checked against GitHub 2026-11-02:
- `HomeCloud-Media-Gateway` main: `53daba615c20cb839f18cb3b52d667127f288d5b`
- `HomeCloudBackend` main: `4f1a2ba3863fcc416bb45fd38025ec6fb0c6c16b`

Only Media Gateway Go source changes. Backend, TV Player, production YAML,
and real storage content are NOT modified.

## Activation

1. Backend issues a `media.room.offer`, Gateway prepares the one-use ticket.
2. TV connects to Gateway `/ws/stream` with existing `media.hello`, and adds
   `"capabilities":{"hcm1":true}`.
3. After the existing `media.ready`, Gateway sends HCM1 binary WebSocket
   messages **only if the application registered a trusted StreamOpener**.
4. If `StreamOpener` is absent (default), KAN-48's READY-only behavior remains.
5. A StreamOpener returns `io.ReadCloser` for the authorized Lease; the caller
   must never use an unvalidated TV-supplied path or URL as a filesystem source.

`app.SetStreamOpener(opener)` must be called before `app.Run()`. Integration
with the actual Storage Agent/Media Gateway file source and FFmpeg are NOT
part of this patch; do not claim TV video playback works yet.

## Binary message layout (56-byte network-order header)

| Offset | Size | Value |
|---:|---:|---|
| 0 | 4 | ASCII `HCM1` |
| 4 | 1 | `1` version |
| 5 | 1 | flags: 0=data, 1=EOF |
| 6 | 2 | header length `56` |
| 8 | 4 | playback generation, uint32 |
| 12 | 8 | attachment connection_epoch, uint64 |
| 20 | 8 | monotonically increasing sequence, uint64 |
| 28 | 8 | stream offset, uint64 (starts at zero for each playback/epoch) |
| 36 | 4 | payload byte length, uint32 |
| 40 | 16 | first 16 bytes of SHA-256(UTF-8 playback_id) |
| 56 | 0–65536 | raw media bytes |

The HCM1 header is a transport envelope, not a codec container. The receiver
should verify version, fingerprint, generation, epoch, sequence, and offset.
EOF has no payload and does NOT close the room; STOP and switching are managed
through the authenticated control plane. Each WebSocket message contains exactly
one complete frame. No Base64 is used.

## ACK contract

TV sends text on the **same direct media WebSocket**:

```json
{"v":1,"type":"media.ack","room_id":"room-x","playback_id":"play-1","generation":1,"connection_epoch":1,"sequence":0,"received_bytes":65536}
```

- `received_bytes` is the cumulative end offset of the named fully received
  frame; it must match a frame actually sent by the Gateway.
- ACKs with unknown, stale, out-of-window, or inconsistent identities are
  rejected; a byte offset alone never grants credit.
- Duplicate ACK of the most recently accepted frame is idempotent.
- Maximum unacknowledged payload: **4 MiB**.
- Maximum unacknowledged data frames: **64** (including for 1-byte reads).
- ACK advances credit; when the window is full, the source is not read further.
- Source read buffer is at most 64 KiB. No file-sized buffering, no base64.
- The sender is the only data writer. ACK receiver never sends data.
- Invalid TV data closes the session; TV cannot control file selection through
  the media socket.

## Cancellation

The `directws.Book` lease is revoked by STOP, room cleanup, switching, or
re-attaching with a newer epoch. Its Done channel closes the socket. The sender
uses a derived context; cancellation closes the `io.ReadCloser` to unblock
pending source reads. Stale frames are distinguished by playback fingerprint,
generation and connection epoch.

## Verification

```powershell
cd C:\Users\user\IdeaProjects\HomeCloud-Media-Gateway
go test -count=1 -timeout=120s ./internal/mediaframe ./internal/directws ./internal/app
go test -count=1 -timeout=180s ./...
go vet ./...
```

Unit tests include synthetic >10 GiB flow, limited in-flight payload and
frame-count, tampered/stale ACK, epoch fencing, canceled blocking Reader, and
WS integration tests. Complete GitHub Windows/Linux CI and actual TV-client
support must still be verified independently.
