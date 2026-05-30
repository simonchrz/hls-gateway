# hls-gateway-go — strangler-fig front for service.py

A Go reverse-proxy that takes the port Caddy points at, serves **migrated**
routes natively, and proxies everything else to Flask. Migrating a route =
add a native handler in `main.go`; nothing changes for unmigrated routes.

    Caddy(:8443) → hls-gateway-go(:8080) → Flask(:8081, unmigrated)
                                         → tv-receiver(:9983) live/EPG/DVR

## Migrated so far
- `GET /healthz` — byte-faithful to the Flask version (verified).

## Build / run side-by-side (no cutover)
    GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o hls-gateway-go .
    # on the Pi, parallel test against the live Flask:
    ./hls-gateway-go -addr :8090 -flask http://127.0.0.1:8080 -tv-receiver http://127.0.0.1:9983
    # compare :8090/<route> vs :8080/<route> until transparent, then cut over.

## Cutover (later, one careful step)
Move Flask to :8081, run this on :8080 (-flask http://127.0.0.1:8081). Caddy
config unchanged. Roll back = stop Go, move Flask back to :8080.

## Migration notes
Even "simple" endpoints carry real Flask logic — e.g. `/api/channels` filters
to the favourites (`.favorites.json`), it is NOT a raw tv-receiver passthrough.
Read the Flask handler before porting; verify key-by-key against the live
Flask response.

## Redeploy after a Go change
    GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o /tmp/hls-gateway-go-arm64 .
    scp /tmp/hls-gateway-go-arm64 pi:hls-gateway/hls-gateway-go
    ssh pi 'cd ~/hls-gateway && docker compose up -d'   # or docker restart hls-gateway

## CUTOVER DONE 2026-05-30
Live: Caddy(:8443) → Go(:8080) → Flask(:8081). gateway-entrypoint.sh supervises
both (either exits → container restarts). Revert: drop the `command:` line in
docker-compose.yml. NOTE: service.py reload is now `docker restart hls-gateway`
(or HUP the Flask pid) — a `docker kill -s HUP` now hits the bash wrapper, not Flask.
