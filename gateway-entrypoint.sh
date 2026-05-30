#!/usr/bin/bash
# Strangler-fig cutover entrypoint.
#
# Runs the Go gateway on :8080 (where Caddy points, unchanged) in front of
# the Flask service.py backend on :8081. The Go gateway serves migrated
# routes natively and proxies everything else to Flask.
#
# Supervises BOTH: if either process exits, this exits non-zero so Docker's
# restart:unless-stopped recreates the container cleanly (rather than leaving
# a half-dead gateway — e.g. Go up but Flask crashed). The container
# healthcheck (:8080/healthz, Go-native) only confirms Go + tv-receiver; Flask
# liveness is covered here by `wait -n`.
#
# Revert to pure Flask: set the compose `command:` back to
#   python3 -u /app/service.py
# and drop GATEWAY_PORT (defaults to 8080).
set -u

echo "[entrypoint] strangler-fig: Flask backend :8081 + Go gateway :8080"

GATEWAY_PORT=8081 python3 -u /app/service.py &
flask_pid=$!

/app/hls-gateway-go \
    -addr :8080 \
    -flask http://127.0.0.1:8081 \
    -tv-receiver http://127.0.0.1:9983 &
go_pid=$!

# Wait for whichever exits first, then take the container down.
wait -n
echo "[entrypoint] a child exited (flask=$flask_pid go=$go_pid) — stopping the other + exiting for restart"
kill "$flask_pid" "$go_pid" 2>/dev/null
exit 1
