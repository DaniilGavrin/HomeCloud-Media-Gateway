# KAN-61 — Media Inspector V1

## Contract and authority

HomeCloud Backend owns rooms; HC Storage Service owns allowed files; Gateway
owns source selection, inspection and media transfer. A TV never supplies a
file path, source URL or storage endpoint to ffprobe. The Inspector only gets a
room/playback/generation-fenced Source Plan previously validated by HC SS.

The codec decision is advisory. KAN-61 **does not** implement transcoding,
remuxing, track filtering, or replacement of the actual HTTP Range media URL.
The current KAN-59 direct media delivery remains unchanged.

## Activation

The existing YAML schema is backwards compatible; no `inspector` stanza means
inspection is disabled and the existing player continues working.

```yaml
inspector:
  enabled: true
  binary: "ffprobe"
```

Install ffprobe on the **Gateway host** (for Ubuntu: `apt install ffmpeg`).
`exec.LookPath` resolves the administrator-configured binary; the file path
can never come from TV/UI/Backend commands. No external internet connection,
new server or new dependency is needed at runtime. Missing ffprobe returns
`UNAVAILABLE` and **must not stop a previously usable direct media stream**.

## Bounded remote-source access

1. HC SS `source.stat` returns metadata and stable source-version fence.
2. Gateway Source Resolver chooses LOCAL_DIRECT / REMOTE_PREFETCH /
   REMOTE_BLOCK_STREAM.
3. An opt-in Inspector starts a loopback-only HTTP server for exactly the
   current approved source, listening on `127.0.0.1:0` and requiring a
   cryptographically random 128-bit opaque path. The server is not advertised
   to TV/UI and exists only while probing.
4. `http.ServeContent` implements seekable Range requests. Reads are backed by
   the existing source reader factories and version-checked plans (including
   HCS1). No full file is downloaded, and a tail seek is possible.
5. ffprobe is a subprocess with context timeout <=8 seconds, total source
   read budget <=32 MiB, stdout <=256 KiB, and at most two concurrent probes.
   The result is cached for <=5 minutes by room/playback/generation and file
   version, with <=64 cached records. Failure is safe UNKNOWN.
6. STOP, switch and server shutdown cancel the inspector context, close the
   HTTP listener and reader, and fence late results by direct WS lease.

## Control / direct WS messages

The already-existing `media.hello` adds an **optional** decoder declaration
inside `capabilities`:

```json
{
  "v":1, "type":"media.hello", "room_id":"room-...",
  "playback_id":"play-...", "generation":1, "device_id":"tv-...",
  "attach_ticket":"<opaque one-use ticket>",
  "capabilities": {
    "http_range":true, "http_switch":true, "media_inspection":true,
    "decoder":{
      "version":1, "source":"webview_can_play_type",
      "preferred_audio_language":"rus",
      "formats":[
        {"container":"mp4","video_codec":"h264","audio_codec":"aac",
         "certainty":"maybe"}
      ]
    }
  }
}
```

The Gateway first replies with the **unchanged** `media.ready`, so slow
inspection does not hold up playback. Later, on the same direct WS, it may
send:

```json
{
  "v":1, "type":"media.inspection", "room_id":"room-...",
  "playback_id":"play-...", "generation":1, "connection_epoch":1,
  "report":{
    "status":"READY",
    "media":{
      "container":"mp4", "duration_ms":123400,
      "tracks":[{"index":0,"kind":"video","codec":"h264","profile":"High",
                 "level":40,"width":1920,"height":1080},
                {"index":1,"kind":"audio","codec":"aac","default":true}]
    },
    "decision":{"mode":"UNKNOWN","reason":"TV_CAPABILITIES_NOT_CONFIRMED",
                "audio_stream_index":1}
  }
}
```

Only clients that explicitly send `capabilities.media_inspection=true`
receive this asynchronous event. This leaves the HCM1 binary-only protocol
untouched for legacy clients.

If the WebView supports `navigator.mediaCapabilities.decodingInfo()`, TV
tests the **inspected file's** codec/profile/level, dimensions, bitrate and
frame rate; if it has enough information, it sends one optional
`media.decoder.report` on the same ticket-authorized direct WS:

```json
{
  "v":1, "type":"media.decoder.report",
  "room_id":"room-...", "playback_id":"play-...",
  "generation":1, "device_id":"tv-...",
  "capabilities":{"decoder":{
    "version":1,"source":"media_capabilities_api",
    "formats":[{"container":"mp4","video_codec":"h264","audio_codec":"aac",
      "video_profiles":["High"],"max_level":40,"max_width":1920,
      "max_height":1080,"certainty":"probably"}]
  }}
}
```

Gateway validates active lease identity and returns an updated
`media.inspection` decision **without rerunning ffprobe**. This decision
is a browser capability estimate, not proof of smooth physical playback.

Messages and decisions are fenced by room_id/playback_id/generation and
current direct lease epoch. An old-generation result must never appear as the
new playback, even if ffprobe finishes after a switch.

Legacy clients ignore `media.inspection`; legacy Gateways ignore the extra
capabilities object. There is no change to the Backend v1 control envelope.

## Decision semantics

- `DIRECT` — **capability-based candidate**: the TV explicitly reported
  probably for the exact container, codec, declared profile, level and
  resolution. Actual media playback success requires the HTML decoder.
- `ADAPTIVE` — a reported codec, track selection, profile, level or size is
  unsupported and a future remux/transcode pipeline is needed (KAN-63/64).
- `UNKNOWN` — no reliable report, unknown limits/codecs, missing media probe,
  or ambiguous WebView decoder result. No optimistic direct compatibility.
- `audio_stream_index` — **requested/planned** audio track, not a promise
  that the original HTTP Range file was remuxed or its audio changed.
  Actual track switching belongs to KAN-63.

Default audio index: requested index, otherwise preferred language, otherwise
default track, otherwise first audio track. Unavailable explicit index => UNKNOWN.

## Tests

`go test ./...`
`go test -race -count=10 ./internal/directws ./internal/mediahttp`
`go vet ./...`

Coverage includes parsers, malformed input, MIME vs real format, H264/AAC and
HEVC/DTS decisions, audio preference, EOF/switch generation fencing, seekable
remote-reader budget and a generated one-second WAV file inspected by real
ffprobe when installed.

## Outstanding physical acceptance

Install the updated Gateway and TV signed APK, enable ffprobe after confirming
its executable is available on the host, then test MP3/MKV, audio track
metadata, correct decoder measurements, network disconnect and STOP. This is
not satisfied merely by a green CI. Related: KAN-55, KAN-62, KAN-63, KAN-64,
KAN-66, KAN-69 and KAN-75.
