#!/usr/bin/bash
# Pure-Flask entrypoint — go-front (the Go strangler-fig front) RETIRED 2026-06-01.
#
# Why go-front is gone: Caddy's handle blocks already route every migrated path
# directly — recorder paths (/api/internal/*, /api/learning/*, /api/recordings,
# /api/recording/*, /api/series, /api/search, /api/poster/*, /api/bumper/*,
# /recording/*) → tv-recorder :9984, and live-TV paths (live-ads(-stream),
# active-channels, warm-status) → tv-receiver :9983 — all BEFORE the catch-all.
# So go-front's ~60 native routes had become dead duplicates; only the Flask
# HTML leftovers + /api/internal/duplicate-recordings ever reached :8080. go-front
# had nothing left to route, so it leaves the data path. Flask now binds :8080
# itself (where Caddy points, unchanged — no Caddy edit needed for this cutover).
#
# /healthz is served by Flask too (byte-faithful to the old go-front native one),
# so the container healthcheck (:8080/healthz) keeps working.
#
# Rollback: restore gateway-entrypoint.sh.bak-pre-stage4 + `docker compose up -d`.
# The hls-gateway-go binary is still in the image (Dockerfile unchanged), so the
# old strangler entrypoint works again as-is.
set -u

echo "[entrypoint] pure Flask on :8080 (go-front retired 2026-06-01)"

# GATEWAY_PORT unset → service.py defaults to 8080 (see service.py:17817).
exec python3 -u /app/service.py
