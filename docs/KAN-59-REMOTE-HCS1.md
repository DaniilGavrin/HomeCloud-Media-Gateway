# KAN-57 / KAN-59 — Gateway readers for local and remote HC SS sources

Storage Service is authoritative for media file IDs, version, permission and
file metadata. HomeCloudBackend controls room creation and generation. The
TV cannot pass arbitrary paths or HCS1 endpoints to the Gateway.

## Configuration

The Gateway's startup-only `config/config.yaml` has a new optional section:

```yaml
storage:
  source_nodes:
    storage-node-id: "ws://192.168.0.10:8092/ws/source"
  local_roots: {}
  cache_dir: "data/media-cache"
  prefetch_limit_bytes: 1073741824
  max_cache_bytes: 10737418240
```

This is an example: use the actual HC SS node ID and exact private storage
IP. DNS names, wildcard/public addresses, query strings and credentials are
rejected. Use `wss://` or Tailscale for encryption where appropriate.
Storage Service must enable HCS1, allow the Gateway source IP, and allow reads
only on the intended root. Default `source_nodes: {}` means no source
connector or reader is registered.

When both services see the **same real files**, optional shared roots can be
mapped explicitly:

```yaml
  local_roots:
    "folder:media": "/mnt/data/Media"
```

The local reader reopens files with `os.OpenRoot`, verifies the same
file identity as the authorization probe, size and mtime before delivering
bytes. Never assume that two hosts share files simply because their IPs
match or a path string looks similar.

## Runtime

1. Backend supplies a trusted `media.room.offer` with `source.storage_node_id`
   and `source.file_id` from `storage.fs.list/stat`.
2. Gateway resolves only the configured node, requests HCS1 `source.stat` and
   checks node ID, file ID, version, size and optional local file proof.
3. A plan is fenced by `room_id + playback_id + generation`.
4. At a valid TV direct attach the Gateway obtains `ReaderFactory.Open` for
   that plan and streams HCM1 frames with the existing ACK/4MiB flow control.
5. For remote files the reader issues HCS1 ranged reads of **256KiB**, validates
   each block's offset, size, version and SHA-256, then returns bytes to HCM1.
   Cancellation closes pending requests. No whole-file RAM allocation.

The KAN-56 <=1GiB `REMOTE_PREFETCH` strategy temporarily uses the SAME
bounded range streaming reader as `REMOTE_BLOCK_STREAM`. It is **not** an
on-disk prefetch cache, despite the planner's strategy name. KAN-58 disk
caching remains a separate item; disk quota config is not yet applied.

## Important boundary

HCS1 produces raw file bytes and HCM1 transports them to TV. Neither format
is a codec decoder. WebView `<video>` cannot play arbitrary chunks directly
from WS without a compatible container pipeline, Media3, or HTTP Range source.
This PR is tested for byte correctness in Go CI, not playback on physical TV.
Missing: Android TV decoder/source binding, seek by timeline/keyframe and
FFmpeg remux/transcode for unsupported source codecs.

`go test ./... && go vet ./...` and the PR CI verify the reader and wire
client (using a mock HCS1 server). HC SS has separate actual-disk WS tests.
No cryptographic service authentication is present; use a private trusted
network and explicit allowlists only.
