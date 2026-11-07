# KAN-63 — On-the-fly audio selection and fragmented MP4 remux (V1)

## Architecture and backward compatibility

Existing **Backend control WS** and **TV↔Gateway direct WS + HTTP Range**
remain unchanged. On a current signed TV app, KAN-63 adds an OPTIONAL
`audio_selection=true` capability on `media.hello`. Gateway advertises
`audio_selection=true` in `media.ready` **only if** Inspector and an
FFmpeg executable are both available. Older clients never enter this mode.

TV displays inspected audio tracks from `media.inspection`. On the viewer's
explicit selection, it checks the **potential output** MP4/AAC format
independently via `MediaCapabilities.decodingInfo()`, including inspected
H.264/HEVC profile, level, resolution, bitrate, and frame rate. If capability
is unknown or not `probably`, it does not invoke conversion.

A valid selection is sent on the **already ticket-authorized direct WS**:

```json
{"v":1,"type":"media.audio.select","room_id":"room-...",
 "playback_id":"play-...","generation":1,"device_id":"tv-...",
 "audio_index":2,"capabilities":{"decoder":{
   "version":1,"source":"media_capabilities_api","formats":[
     {"container":"mp4","video_codec":"h264","audio_codec":"aac",
      "video_profiles":["High"],"max_level":40,
      "max_width":1920,"max_height":1080,"certainty":"probably"}
   ]
 }}}
```

Gateway rejects unauthenticated/wrong room/playback/device/generation,
nonexistent track, unsupported output codec/profile, missing FFmpeg,
stale/STOPped source plans, or media metadata that cannot establish
compatibility. No public file path, arbitrary URL or FFmpeg command is
accepted from TV.

If accepted, a session-specific, peer-IP/lease-bound
`/media/convert/<opaque-64-hex>` endpoint is offered as
`media.convert.ready`, with `seek_supported=false`. The existing
`/media/session/<opaque>` URL stays valid as Direct Play fallback.

## Pipeline

- HC Storage Service owns and verifies the file ID. Gateway opens only the
  authorized Source Plan (LOCAL_DIRECT or remote HCS1) and reads bounded
  byte ranges as needed. A random HTTP Range input facade is bound to
  127.0.0.1 on an ephemeral port **within the Gateway process**.
- FFmpeg gets exactly that seekable loopback URL and an explicitly selected
  numeric input audio stream index. Video codec is **copied** (H.264/HEVC
  only if the TV declared the matching MP4 profile/level/resolution).
- Matching AAC audio is copied; other selected audio codecs are encoded to
  **AAC-LC stereo 160 kbps**, with at most one FFmpeg conversion at a time.
- Output is a fragmented MP4 (`empty_moov + frag_keyframe`) streamed
  directly through an HTTP 200 response with bounded buffers. There is no
  intermediate whole-file conversion and **no original-file size advertised
  for converted bytes**.
- FFmpeg stderr is capped. Request cancellation, room STOP, source switch,
  or audio reselection cancels the child process and closes its input listener.
  Previous converted URL is revoked when the audio choice changes.
- The conversion URL is never published via Backend, never exposes a local
  file path, and requires the same peer IP and current room lease.

## Known limitations (not implicit promises)

- This is **sequential HTTP streaming**, not HTTP byte-range output.
  Arbitrary `Range` requests on converted bytes get HTTP 416; only the
  initial `bytes=0-` request can be answered with 200.
  **Seek within a converted stream is disabled** in V1. Native WebView
  support for progressive fragmented MP4 must be physically verified.
- Restarting the output at another time offset / preserving playback
  position during track switching requires a subsequent ticketed protocol
  extension. Selection starts the converted output at its beginning.
- No video transcoding. Unknown video compatibility or an output format
  other than supported MP4+AAC is rejected.
- Capability results are advisory, not guaranteed decode success. If Android
  WebView does not implement `MediaCapabilities.decodingInfo()` or
  ffprobe bitrate/framerate fields are missing, selection is unavailable.
- This first implementation does not change `media.rooms.seek`, remux
  streams do not support arbitrary Range and cannot claim seek success.
- No disk HLS cache, manifests, HLS native decoder promises, or full
  KAN-64 video conversion are included.
- Feature requires optional Gateway `inspector.enabled=true` and **both**
  `ffprobe` and `ffmpeg` installed on the Gateway host.
  Missing binaries leave legacy Direct Play fully functional.

## QA

Run Gateway PR CI (`go test ./...`, real FFmpeg WAV and MKV/H264 copy tests,
race detector) and signed TV APK CI. Then use a **noncritical test media**
on the real Android TV. Compare playback position, audio language, CPU/RAM
and logs, attempt a second audio selection, STOP and a file switch. Assert
that no FFmpeg subprocess or converted session survives STOP/switch and that
an unrelated peer cannot fetch converted media. Retest ordinary MP3/MKV
Direct Play, HTTP Range seek and Pause/Resume.

Do not mark KAN-63 Done or deploy as default until physical E2E passes.
