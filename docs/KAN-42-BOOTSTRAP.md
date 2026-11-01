# KAN-42 — Media Gateway bootstrap

This package adds startup-only YAML settings, strict parser, HTTP liveness check
and graceful shutdown. It does **not** implement WS media transport yet.

Run from the repository root:

```powershell
go get go.yaml.in/yaml/v3
go mod tidy
go test ./...
go run ./cmd/media-gateway -config config/config.yaml
```

In a second PowerShell terminal:

```powershell
Invoke-RestMethod http://127.0.0.1:8091/healthz
```

`/ws/stream` deliberately returns HTTP 501 until MG-08/MG-09.

`config/config.yaml` contains *only* startup settings. Rooms, playback state,
TV identities and dynamic data belong in the appropriate runtime service,
not YAML. A loopback listener cannot be reached by a TV on another machine:
configure an explicit private address when the media WS is implemented.

`storage.prefetch_limit_bytes` is the future *disk prefetch* threshold of exactly
1 GiB, **not a RAM buffer**. Neither prefetch nor cache quota is enforced yet.

Dependencies are resolved by `go mod tidy` in the existing project; this ZIP
intentionally contains **no `go.mod`, `go.sum`, `.gitignore` or `data/`**.
