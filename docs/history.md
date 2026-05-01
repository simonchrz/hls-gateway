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
- ~~**ML model training** for ad detection~~ — UPDATE: this entry is
  now obsolete. The Mac-offload path freed enough CPU that we DID
  build the ML stack — `tv-detect` with backbone.onnx + linear
  head.bin, nightly retrain via train-head.py, ads_user.json as
  ground-truth label source. Per-Show IoU now reported on /learning.
  The original concerns (a) Pi CPU bottleneck, (b) label source
  capped at comskip — both addressed by running on Mac with user-
  reviewed labels.
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
- **Whisper-Feature Stage 4 — full Go-side NN integration** (deferred
  2026-05-01). Currently Whisper runs as daemon-side post-processor
  with WHISPER_ENABLE=1 (rules in `tv-whisper-eval.py`, validated
  +5.4% mean Block-IoU on n=9). Stage 4 would add `whisper-prob` as
  a 6th NN-feature column (dim ≥5160, currently 5132 with logo+audio)
  so the head learns optimal weight per frame instead of hand-coded
  rule thresholds. Deferred reasons:
    1. **N too small for new feature** (memory `feature_ceiling_n50`
       puts the threshold at N≥150; we are at N=161, just barely).
       Audio-RMS was added recently and we have no stable per-show
       IoU yet to confirm it didn't already saturate the head.
    2. **Architecture inversion**: NN-feature needs whisper BEFORE
       detect (= input to inference), current post-processor runs
       AFTER. Sequencing change adds ~50 s latency unless run in
       parallel.
    3. **Loss of pass-through-safety**: post-processor falls back
       cleanly on any error; NN-feature is hard-wired and any broken
       whisper.json would silently bias the head's frame predictions.
    4. **Less interpretable**: post-processor logs which rule fired
       (`−1fp, ⤇0ext, +0new`); NN-feature would be opaque.

  Re-evaluate when: (a) corpus N ≥ 250-300, (b) post-processor's
  +5.4% gain has plateaued (= rule-tuning yields no further IoU
  gain over 2-3 weeks), (c) we have separated per-show IoU
  before/after WHISPER_ENABLE so Stage 4's marginal effect is
  measurable. Estimated effort if/when revived: 6-8h
  (Go feature in `signals/`, train-head.py wiring, daemon CLI flag,
  head retraining + smoke).
