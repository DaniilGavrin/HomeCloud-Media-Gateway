# KAN-57 — verified local source reader, stage 1

Adds source.LocalReaderFactory, a read-only streaming io.ReadCloser for
LOCAL_DIRECT decisions from KAN-56. It works for audio and video bytes
without claiming any codec/container decoding.

The source must first be authorized by Coordinator.Prepare, which asks
the trusted HC SS owner for file metadata. A TV-supplied path or URL
is never an acceptable input.

The reader uses Go os.OpenRoot to anchor the file open within an explicit
admin-configured shared root. It verifies size, modification time and file
identity versus the previous local probe, rejects invalid byte offsets,
and returns a closable file descriptor.

App.SetSourceReaders installs trusted factories before Run. Only when
the source Coordinator AND reader factory are configured does the
Gateway HCM1 sender open real bytes for the granted room/playback/generation.
Default executable does NOT currently register a Storage Service connector
or any reader, preserving READY-only behavior until explicitly configured.

New tests check local disk audio and video byte delivery through HCM1/ACK.
They use synthetic payloads; this is a transport test, NOT a decoder test.

Still required for end-to-end playback:
- HC SS stable file IDs, trusted Stat and private HCS1 binary range reads.
- Explicit Gateway shared-root mapping at startup. Never infer shared
  filesystem access from matching IPs.
- Android TV HCM1 receiver with decoder/remux (raw file chunks alone
  do not form a MediaSource-compatible fragmented MP4).
- Media type negotiation and physical TV tests.

Checks: go test -count=1 ./...; go vet ./...
