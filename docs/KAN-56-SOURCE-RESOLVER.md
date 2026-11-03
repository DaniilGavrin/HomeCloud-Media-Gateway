# KAN-56 — Source Resolver & HC Storage Service contract (draft v1)

Baseline: `DaniilGavrin/HomeCloud-Media-Gateway` `main` at `7ecc79ce01983cd2df7e6b40acf7f4a7c2b91560` (KAN-54).

## Boundary: implemented vs planned

**Implemented in KAN-56:** gateway-owned typed resolver, trusted node registry interface, local filesystem root/permission/version probe, automatic strategy selection (including exact 1 GiB boundary), bounded byte-range message schema, source reader factory, per-room/playback/generation plan fencing, Gateway offer-path integration when a source coordinator is configured, tests (including race tests).

**Not implemented by KAN-56:** HC Storage Service itself (`KAN-39`), private WS connector to that service, real remote byte transfers (`KAN-58/59`), local actual stream reader (`KAN-57`), disk cache, media inspection and FFmpeg, Android TV decoder. A `ReaderFactory` is *not* registered by default and cannot claim any media playback was successful. This document is a proposed inter-service API to reconcile with HC SS implementation.

## Trust and identity

A Backend-authorized room offer contains `source: {storage_node_id, file_id}`. Only the server-side Gateway Registry resolves `storage_node_id` to an approved HC SS session. There is **no** arbitrary WS URL, physical path, or storage credential supplied by the TV or HomeCloud UI. Backend remains owner of the room and never proxies media bytes. Storage Agent reads its OS-managed filesystem on whitelisted roots; no NFS/SMB implementation or cloud backend is introduced.

HC SS `Stat(file_id)` returns:

```json
{
  "owner_node_id": "node-storage-01",
  "file_id": "opaque-file-identifier",
  "version": "opaque-stable-content-revision",
  "size_bytes": 1073741824,
  "content_type": "video/mp4",
  "range_readable": true,
  "local_hint": {
    "shared_root_id": "disk-data-1",
    "relative_path": "movies/movie.mp4",
    "modified_unix_nano": 1793660000000000000
  }
}
```

`size_bytes: null` means **unknown**; `0` means the empty file. `local_hint` is optional. Do **not** return arbitrary full file paths or expose hints to TV. For same-host operation, Gateway administrator explicitly configures a mapping of `shared_root_id` to a verified absolute path *inside Gateway's filesystem namespace*. The `RootProbe` verifies canonical paths, symlink containment, OS read permission, regular file, size and nanosecond modification time. Same IP is never considered sufficient. This is a provisional filesystem proof: a future KAN-57 reader must reopen and reverify the file to close TOCTOU gaps. The identity and allowed roots are asserted by the **authenticated** Storage Service, not by a client request.

## Range contract for remote sources (planned HC SS private WS)

Gateway requests a bounded range, directly from the authorised Storage Service for that `file_id`:

```json
{"file_id":"opaque-file-identifier","version":"opaque-stable-content-revision","offset":1048576,"length":4194304}
```

`offset` and `length` are **bytes**, never time milliseconds. Length must be positive and at most **4 MiB**, with overflow and stale-version rejection. Response header carries `offset`, `length`, `version`, `eof`, optional `checksum_sha256`. Payload travels as **binary frames** on the HC SS↔Gateway connection, with request correlation, length checking, backpressure and cancellation to be implemented with KAN-39/59. Source revisions must be fenced: if `version` differs, fail with `SOURCE_VERSION_CHANGED` instead of mixing old and new data. `OpenRange` returns a `ReadCloser`: callers MUST close it on seek/STOP/cancel.

This is a Go contract for the Storage Connector, **not an already deployed HC SS API**. Wire command names/frame layout, authentication/session binding and local root namespace identifiers remain subject to final agreement with KAN-39. No JSON/Base64 media data should traverse Backend.

## Automatic selection

```text
HC SS owner Stat succeeds and matches storage_node_id+file_id+version
   │
   ├─ local_hint + explicitly mapped shared root + canonical regular file,
   │  exact size+mtime + OS-readable?             → LOCAL_DIRECT
   │
   └─ otherwise authenticated HC SS range reads available?
      ├─ known size <= 1,073,741,824 bytes       → REMOTE_PREFETCH (on DISK)
      ├─ known size > 1,073,741,824 bytes        → REMOTE_BLOCK_STREAM
      └─ unknown size                             → REMOTE_BLOCK_STREAM
```

If remote range access is not available, return `RANGE_UNSUPPORTED`; never fabricate media readiness. KAN-58 is responsible for free disk space, quota, atomic `.part` commit, checksum and cleanup, and may produce an explicit `ENOSPC` or a separately agreed block-stream fallback. KAN-57 and 59 implement real readers. Common `ReaderFactory.Open(ctx, plan, byteOffset)` keeps direct/remux/FFmpeg unaware of whether bytes were local, disk-prefetched or fetched by WS. Seek by *time* is resolved only at the media pipeline layer (keyframes/timeline); the resolver must not assume time↔byte offsets.

## Integration and lifecycle

`App.SetSourceCoordinator(coordinator)` registers the trusted HC SS resolver and per-room plan ledger **before** `App.Run`. When present, the Gateway control client resolves metadata for `media.room.offer` before ACK `APPLIED`, rejecting invalid/no-owner metadata via existing `FAILED` path. Plans are keyed by `room_id+playback_id+generation` to reject stale callbacks. `media.room.closed` deletes a room plan; control reconnect resets all plans. No coordinator is installed by default, preserving KAN-54 control-only behavior until HC SS exists.

A plan only means source strategy selection succeeded, **not** that a disk cache/remote reader/TV decoder has started. Actual `PLAYING` still requires independent TV+Gateway observation and is outside KAN-56.

## Tests

`go test -race -count=1 ./internal/source ./internal/control`

`go test -count=1 ./...`, `go vet ./...`, `go build ./...` before commit, then check GitHub Actions. No physical TV or remote HC SS availability claimed.
