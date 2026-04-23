# What's intentionally NOT done & what's on the backlog

## Intentionally NOT done

- **MediaMTX / go2rtc fallback** — extensively tested, abandoned (see
  project memory for details). The go2rtc docker-compose and its
  Caddy routes were removed 2026-04-21; `/home/simon/go2rtc/` still
  exists on the Pi but the container isn't running. Caddy no longer
  references `:1984`, `/live/*.m3u8`, `/live/hls/*`, `/api/ws`,
  `/api/webrtc*`, `/video-rtc.js`, or `/video-stream.js`.
- **Per-channel audio silence / scene-change analysis for show
  boundary detection** (comskip-for-live) — not worth the CPU and
  still not sekundengenau. We use Now-Next APIs + per-channel lead-in
  instead.
- **ZDF GraphQL introspection beyond the two persisted queries**
  (`VideoByCanonical`, `getEpg`) — those give us what we need;
  reverse-engineering more risks breakage on their next bundle push.
- **Reverse-engineering arte's private OPA v3 API for past-show
  lookup.** Not publicly accessible. User opted to skip for now.
- **ML model training** for ad detection — proposed as "Stufe 2/3" of
  an ad-detection overhaul. Skipped because (a) the problem is Pi
  CPU, not detection accuracy, and (b) the obvious label source
  would be comskip's own output, capping the ML ceiling at comskip's
  accuracy. An ML model on the Pi would replace one CPU load with
  another (likely worse, no NPU/TPU on Pi5). If detection quality
  ever becomes the issue, start with per-channel `comskip.ini`
  tuning and ground-truth hand-labels — not a comskip-imitating
  model.
- **Full stack migration to Mac** — considered 2026-04-21. Blockers
  that *do* apply: Docker Desktop's `network_mode: host` is gimped
  on macOS (all three stack containers use it, plus SAT>IP discovery
  over multicast breaks in the Docker-VM), and macOS is not a
  graceful 24/7 server OS (update-required reboots mid-recording).
  Blockers that *don't* apply for this specific user: tuner is
  FritzBox SAT>IP (network, not USB-bound to Pi), and pihole+HA stay
  on Pi regardless. Net: the CPU motive is better addressed by the
  live-scan offload (see [mac-handlers.md](mac-handlers.md)) than by
  moving the stack.
- **Live-transcode offload to Mac** — considered 2026-04-23.
  Architecturally feasible (Mac reads tvheadend HTTP stream, ffmpeg
  transcodes, output back to Pi), but every option for the output
  path has a worse trade-off than Pi-local: SMB writes can't keep up
  with realtime 1 s segments, Mac-served HLS adds cross-origin/route
  complexity, and Caddy-proxied Mac segments add per-segment latency.
  Plus the always-warm pin requires 24/7 uptime and the Mac sleeps;
  if Mac goes off-LAN the live TV path dies entirely. Pi-local
  ffmpeg on the Pi5 with `usb_max_current_enable=1` and SuperSpeed
  USB handles 3-4 streams at ~100% CPU comfortably (4 cores
  available).

## Backlog (user mentioned, not built)

- Always-warm list for some specific channels independent of viewing
  behaviour (partially supported via LRU already)
- Scene-based chapter marker tuning (would need arte.tv `scene[]`
  data which is editorially curated — absent for most shows)
