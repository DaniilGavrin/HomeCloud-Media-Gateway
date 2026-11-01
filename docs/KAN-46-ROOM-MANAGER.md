# KAN-46: HomeCloud Media Gateway Room Manager

## Scope

This is a **local, in-memory** state machine owned by the Media Gateway. The main HomeCloud WS Backend remains the source of truth for user-facing rooms, command ordering, subscriptions and history. Gateway state is NOT persisted to YAML/database and must not be reconstructed as active playback after a process restart without Backend reconciliation.

The manager deliberately **does not** implement media streaming, WS control commands, attach tickets, file access, or TV offers. Those are KAN-47, KAN-48 and later. Existing `/ws/stream` remains HTTP 501. No media payload crosses the Backend control WS.

## Lifecycle

```text
ROOM:     CREATING -> CONNECTING -> ACTIVE -> CLOSING -> CLOSED
                                  -> LOST/ERROR -> CLOSING -> CLOSED

PLAYBACK: IDLE -> PREPARING -> BUFFERING -> PLAYING <-> PAUSED
                                       -> ENDED -> IDLE
                              ERROR -> IDLE

Start another file: same room_id and target TV, fresh playback_id, generation+1
Seek: same room_id and playback_id, generation+1, previous worker canceled
EOF: playback ENDED, worker canceled, ROOM remains ACTIVE
STOP: room CLOSED, room and playback contexts canceled, TV slot released
```

Implementation details: Fail(LOST/ERROR) records terminal Closed state with reason and monotonic revisions; transient LOST/CLOSING stages are performed atomically. Snapshot generation/revision is local to Gateway; Backend authoritatively sequences UI commands.

## Package: `internal/rooms`

- `New(maxRooms)` — validates the existing `config.limits.max_rooms` (1–128).
- `CreateAssigned(roomID, Target)` — preferred for future Backend command integration. IDs come from the Backend; caller must authenticate and authorize them before invoking this method.
- `Create(Target)` — local/test convenience, generates a 128-bit random room ID.
- `AdvanceRoom(roomID, Connecting|Active)` — validates room states.
- `StartAssignedPlayback(roomID, playbackID, Source)` — preferred future Backend path; switches file, invalidates old workers and increments generation.
- `StartPlayback(roomID, Source)` — local convenience, generates a 128-bit random playback ID.
- `MovePlayback(Token, PlaybackState)`, `EOF(Token)` — reject callbacks from an outdated playback ID/generation.
- `Seek(Token, positionMS)` — increment generation, cancel previous worker context, return new token/context.
- `Get`, `List`, `Count`, `IsCurrent` — safe concurrent snapshots and current-token check.
- `Stop(roomID)`, `Fail(roomID, LOST|ERROR)` — idempotent close, cancel all contexts, release TV target. Last 256 closed room snapshots are retained for bounded STOP idempotency.
- `Close()` — idempotent shutdown and cancellation of all local rooms.

`Target` includes both `node_id` and `device_id` to disambiguate televisions across HomeCloud nodes; local TV exclusivity prevents two active rooms targeting the same pair.

`Source` holds the **logical file reference** (`storage_node_id`, `file_id`), not a direct OS filesystem path. The storage resolver is planned for KAN-56.

## Callback protocol (critical)

A worker receives `(Snapshot, Token, context.Context)` when playback starts and `Seek` resets the generation. It **must stop work on `ctx.Done()`**. Before applying any asynchronous result, it must pass the original `Token` to `MovePlayback`, `EOF` or use `IsCurrent`. Never update the room using `room_id` alone; a late EOF after switching movies could otherwise end the new one.

## Integration with `internal/app`

`app.New` initializes `rooms.Manager` using `limits.max_rooms`, `app.Rooms()` exposes the local manager for future validated control-plane handlers, and `app.Run` closes the manager on exit. Existing KAN-44 supervisor and KAN-45 control registration remain untouched.

This package intentionally adds **no public room-modifying HTTP endpoint**, avoiding unauthenticated LAN control of the Media Gateway.

## Validation

Run from the Gateway repository root:

```powershell
go test ./...
go test -race ./internal/rooms ./internal/app
go vet ./...
go build ./cmd/media-gateway
git status --short
git diff --check
```

Tests cover correct room/playback separation, EOF vs STOP, multiple playbacks, seek generation fencing, callback staleness, allowed state transitions, configuration room limit, target conflicts, concurrent STOP/callbacks, external immutable snapshots, cancellation and bounded STOP history.

**Note:** Gateway's full module specifies Go 1.26.4; these commands should be run with that toolchain (or the compatible toolchain selected by `go.mod`).

## Follow-up work

KAN-47 will accept ordered `media.rooms.*` commands from the authoritative Backend and route validated room events into this manager. KAN-48 will attach real TV sockets. KAN-49 will use playback generation/sequence to guard media frames. Neither feature is claimed to exist at KAN-46.
