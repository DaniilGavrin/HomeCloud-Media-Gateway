# KAN-65 — FFmpeg admission control (first slice)

The previous KAN-63 Runner limited active child processes but made additional
requests wait for an available slot. The HTTP handler had already returned
`200 video/mp4`, even if no process could run. This was misleading and could
retain many blocked media clients on resource-constrained HomeCloud hosts.

This slice uses a nonblocking per-Gateway reservation:
- At most the existing **one** FFmpeg conversion is admitted.
- A second conversion gets `503 Service Unavailable` with
  `Retry-After: 2` and `REMUX_BUSY` **before HTTP streaming starts**.
- A permit is released on success, cancellation, FFmpeg error or STOP.
- The rejected request never creates a subprocess, local Range listener or
  claims to be valid MP4. There is no unbounded wait queue.
- Existing raw HTTP Range Direct Play is unaffected.

Further KAN-65 work still required: platform-specific CPU/RAM/IO enforcement,
actual running-process telemetry and failure injection for slow clients.
This initial patch deliberately does not impose unreliable artificial
CPU/RAM metrics or Linux-only limits on the Windows Gateway build.

Test coverage: capacity saturation, idempotent release, no use-after-release,
and real Gateway HTTP 503 on exhaustion. Keep KAN-65 in progress until
stress/load measurements from the target hardware are available.
