# KAN-50 / MG-10 — Room heartbeat: TV + Gateway

## Repository baselines

- Backend: `4f1a2ba3863fcc416bb45fd38025ec6fb0c6c16b`
- Media Gateway: `990a90d1005d41da9764d27d42f3db1f489e024b`

## Overview

The HC WS Backend is authoritative over room lifetime. No media bytes are
routed through the Backend. Room heartbeat traffic travels solely over the
existing identified control WebSocket `/ws`; direct media HCM1 stays on the
Gateway `/ws/stream` socket.

A room has two independent monotonic timestamps:
`last_seen_tv` and `last_seen_gateway`. Both are initialized at room creation
(or reset at the start of a new playback generation). A room expires when a
member has not been heard from for more than `interval + grace`, based on
`time.Now()`-derived monotonic times. Gateway's single startup configuration
specifies `heartbeat.interval` (20..30s) and `heartbeat.grace` (exactly 10s);
Backend receives and validates these in the privileged Gateway registration.
The current default is **20s + 10s = 30s**.

### Control-plane request from either participant

```json
{
  "v": 1,
  "id": "unique-request-id",
  "type": "command",
  "action": "media.room.heartbeat",
  "data": {
    "room_id": "room-...",
    "playback_id": "play-...",
    "generation": 1,
    "participant": "tv"
  }
}
```

For Gateway heartbeat, `participant` is `gateway`. The Backend accepts it
only from the currently registered Gateway control session. TV heartbeat must
originate from the identified control socket whose `device_id` matches the
room's target and whose client type is `tv` or `television`. Session identity
is checked to reject stale TV sockets, and stale playback generations are
rejected. The control plane's device IDs remain routing identities, **not
cryptographic authentication** (consistent with HC WS v1).

Success uses the existing `{v,id,type:"result",action,data}` envelope and
returns `last_seen_tv` and `last_seen_gateway`. Use the local server clock,
not a client-provided timestamp. Heartbeat is independent of HCM1 ACK and
WebSocket ping/pong and continues when the playback ends or is idle.

### Expiry / notifications

A lightweight Backend checker runs only once the first room is created. Upon
expiry, the Backend records a real `LOST` room transition and publishes:

- `media.room.changed` with `room_state: "LOST"`;
- `media.room.lost` with `participant` and `HEARTBEAT_TIMEOUT_TV` or
  `HEARTBEAT_TIMEOUT_GATEWAY`;
- `media.room.closed` after authoritative cleanup, also queued to both
  participants.

Gateway receives `media.room.closed`, cancels its local room and closes the
one-use media ticket/lease, which interrupts HCM1 streaming. A physical
control socket disconnect may still trigger immediate peer-loss cleanup,
independently of heartbeat deadlines.

### What KAN-50 does NOT do

The current Android TV Player does not yet implement receiving a media room
offer or sending `media.room.heartbeat`. The backend handler and the Gateway
sender are implemented and unit tested; a synthetic TV control client can
exercise the TV heartbeat API. To use it with a **physical** TV in a media
room, a TV Player update must parse `media.room.offer` and send TV heartbeat
while the room is active, including after EOF. This patch deliberately does
not modify TV Player, video decode, or the KAN-49 media transport.

## Tests

- `go test -count=1 ./internal/roomheartbeat ./internal/wsserver ./internal/mediarooms` in Backend
- `go test -count=1 ./internal/control ./internal/rooms ./internal/directws ./internal/mediaframe` in Gateway
- Full `go test -count=1 ./...`, `go vet ./...`, and `go build ./...` in both.

The patch is not automatically committed. KAN-50 remains In Progress pending
both full test suites, successful GitHub Actions and physical TV integration
(or explicit acceptance of the synthetic TV-sender milestone).
