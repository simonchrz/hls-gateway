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

- **piped-backend resilience: a YouTube IP-block must not take down the
  WHOLE backend** (deferred 2026-05-29; HIGH value, do when the IP isn't
  throttled so it can be tested without re-triggering the block). Observed
  failure: under a googlevideo per-IP block every `/streams`/synth-hls
  resolve waits on a YT timeout; the request threads all block, the
  AsyncServlet pool starves, and then EVEN the YT-independent
  `/healthcheck` gets no thread → the entire piped-backend (:8881) hangs
  (HTTP 000), not just YouTube. Immediate recovery is `docker restart
  piped-backend piped-bg-helper` (clears stuck threads; verified back to
  healthy + `/healthcheck` 200 in ms), but it re-hangs under load until
  the IP recovers (~1-12h). Two complementary robustness fixes:
  1. ~~**Thread-pool isolation**~~ **DONE 2026-05-29 (commit 9e3b624 in
     simonchrz/Piped-Backend `ios-streaming-patches`):** `/streams` +
     `/synth-hls` capped at `availableProcessors()/2` (=2 on Pi5) concurrent
     resolves via a `Semaphore` in `ServerLauncher.java`; `tryAcquire(500ms)`
     else fast-reject 503. Hung resolves now pin ≤2 carriers, so
     `/healthcheck` + non-YT routes never starve. Validated without YouTube
     (3rd concurrent resolve → 503 in 0.5s; `/healthcheck` 200 in 1.6ms in
     parallel). A YT IP-block can no longer take the whole backend down.
  2. ~~**Fail-fast resolve timeouts**~~ **DONE 2026-05-29 (commit 75b0c57):**
     per-call timeouts already existed (Downloader 10s, HEAD 2s/3s, Android
     cascade 6s), but a throttled resolve STACKS the sequential fallbacks
     (Android 6s + WebEmbed ~20s + HEAD 5s + force-WebEmbed retry ~20s) into
     ~30-50s. Added a total per-resolve budget in `ServerLauncher` (12s) that
     wraps `streamsResponse` + the SynthHls playlist builds; on timeout it
     cancels + throws so the handler returns a fast error and frees the
     semaphore slot. Normal resolves ~2-4s, well under budget (verified).
  3. ~~**De-pin virtual threads**~~ **DONE 2026-05-29 (commit ffd99ef):**
     `SynthHlsHandlers` resolve section converted from `synchronized
     (streamsCache)` to a `ReentrantLock` — synchronized + blocking I/O pins
     the vthread carrier (Java 21); ReentrantLock lets it unmount during the
     resolve. (StreamHandlers had no synchronized around its resolve.)
  NOTE: none of this makes videos PLAY under a hard IP block (only IP
  recovery does) — they keep the backend alive + fail fast. Root cause of
  the block is resolve VOLUME (backend tuned for minimal resolves; pre-warm
  doubled it, since reverted). See project memory `piped_synth_hls_youtube`
  + `synth_hls_cache_ttl_cpn_throttle`. Don't test by hammering the same
  resolve — it re-triggers the block.

- **tv-receiver EPG: extend horizon to 7+ days / cover the missing ~32
  channels** (was a dangling `epg-horizon-ausweiten` "backlog memory"
  reference in tv-receiver/CLAUDE.md that never existed — folded in here).
  EPG currently covers ~7100 events across 63 of 95 slugs, ~3 days ahead
  (epgshare01 DE1 XMLTV feed). The missing ~32 are mostly shopping/niche.
  Follow-up: a longer-horizon / more-complete XMLTV source (or a second
  feed merged in). Low priority.

- **hls-gateway → Go rewrite** (roadmap, not started; full rationale in
  project memory `hls_gateway_go_rewrite_roadmap`). `service.py` is
  ~22.7k lines of Flask with the UI built as f-string HTML. Approach if
  ever started: strangler-fig — stand up a Go gateway, proxy unmigrated
  routes to Flask, migrate the data endpoints first (shared Go packages
  with tv-receiver: fan-out, per-channel ffmpeg), HTML pages last. Big
  lift, no concrete trigger yet. Near-term sub-item already noted in that
  memory: add a `/healthz` route — DONE 2026-05-29 (it exists now).

- ~~**Channel-logo fallback still points at dead tvh imagecache**~~ **DONE
  2026-05-29 (commit d07e1a5):** `_channel_logo_url` now returns `""` for any
  `imagecache/<id>` fallback (tvh's :9981 is gone, tv-receiver has none), so
  logo-less channels show the client placeholder instead of a dead 404/502
  URL. Curated `/static/ch-logos/<slug>` overrides still win (all 24 favorite
  channels have one). Optional future nicety (not done, marginal): a generated
  initials-placeholder or a real icon source for niche channels.

- **synth-hls: first-segment warmup (cold-tap latency)** (app-dev finding
  2026-05-29, after the resolve-reuse win brought cold-tap to ~1.7s). The
  biggest remaining server-side chunk is the **first segment fetch ~846ms**:
  piped-proxy (:8882) synchronously pulls the first googlevideo chunk when
  the player requests it. A naive prefetch/warmup does NOT help and is
  HARMFUL here: piped-proxy is a streaming proxy with NO cache (the cached
  yt-proxy is deliberately bypassed — `rewriteToYtProxy` returns the URL
  unchanged because googlevideo per-video-throttles the yt-proxy upstream),
  so a warmup fetch + the player's fetch = TWO googlevideo fetches of the
  same chunk = extra YT load + the same IP-block risk we just tamed. Proper
  version needs a **first-chunk cache** in piped-backend: at variant-build
  (the Pi already knows the first segment URL + byte-range) async-pull the
  first chunk ONCE into a small cache, point the first segment URL at a
  Pi-served endpoint that reads it, so it's a single fetch started early and
  overlapped with master/variant/mpv-start (~30ms) — the 846ms then drops
  out of perceived latency. Moderate effort; gated on getting caching right
  without re-triggering the throttle. (The other remaining ~320ms is VT
  first-frame decode — app/decode-side, not server.)

- **Proactive monitoring / alerting** (idea 2026-05-29; still collecting).
  This session was all reactive firefighting — disk at 90%, the YouTube
  IP-block, slow recording downloads were each only found once someone
  noticed. Building blocks already exist: `/healthz` on both services, the
  `/` dashboard, `/learning`, and Home Assistant runs on the Pi anyway.
  Idea: a few HA sensors + an automation that PUSHES before it hurts —
  disk `>85%`, piped-backend unhealthy, a DVR recording failed, detect-drain
  backing up. Reuses HA (no new system). Turns "user notices it broke" into
  "system warns first". Highest real-world value.

- **Extend YT-resolve isolation to ALL resolve routes** (idea 2026-05-29).
  The `Semaphore` only guards `/streams` + `/synth-hls`. But `/channel`,
  `/c`, `/user`, `/clips`, `/sponsors`, `/dearrow` also resolve YouTube and
  can starve carriers under an IP-block if the app browses/searches during
  one. Put them on a shared resolve semaphore — cheap, low-risk, closes the
  gap left by the playback-only cap (commit 9e3b624).

- **Investigate the yt-proxy throttle (root cause of the first-segment
  problem)** (idea 2026-05-29). The cached yt-proxy is bypassed because
  googlevideo per-video-throttles it (`rewriteToYtProxy` is a no-op), which
  is *why* there's no segment caching → the 846ms first-segment + why the
  warmup item above is hard. If we find WHY the yt-proxy gets throttled
  (URL pattern? missing header/cpn vs the working piped-proxy path?) and fix
  it, segment caching unlocks → solves the first-segment latency cleanly AND
  cuts repeat-segment YT load. Deeper/research, but the upstream lever.

- **Smoke test for the Piped-Backend fork** (idea 2026-05-29). A lot landed
  in the fork this session (semaphore, resolve budget, auto-WebEmbed
  fallback, resolve-reuse, ReentrantLock) — all validated manually. A ~10-line
  smoke (resolve a known video → master 200 + segment 206 + semaphore-503
  behaviour) wired into the build would catch regressions. Small; insurance
  now that the fork is complex enough that a silent break hurts.

- **Back up tv-receiver state (DVR schedules / autorec / channel map)** (idea
  2026-05-29; HIGH value, cheap). Confirmed gap: `tv-backup-labels.sh` backs
  up the ML labels (ads_user.json + models) but NOT tv-receiver's state in
  `~/bin/`: `dvr.json` (~480 schedules + the uuid→recording registry!),
  `autorec.json` (19 rules), `channels.json` (95-channel slug→freq/pids map).
  No script backs these up. If the NVMe dies (it has a history — APST hang,
  memory `nvme_controller_hang_recovery`), all schedules + autorec rules + the
  recording mapping are lost (cf. the migrator wipe that cost 485 dirs). Add
  them to the existing daily backup (or a small rsync to the labels-backup
  repo). `epg.json` (3.9 MB) is regenerable from the XMLTV feed → skip/optional.

- **Recording-failure detection + alert** (idea 2026-05-29). DVR recordings
  can come out as junk (~5 MB, stuck subs / mux re-tune mid-record — see
  memories `never_restart_tvh_during_recordings`, `stuck_warm_channel_kills_dvr`).
  A check that flags recordings far below expected size/duration → alert
  (+ optional autorec re-schedule). Feeds the monitoring idea above.

- **Episode summaries / auto-chapters from whisper transcripts** (idea
  2026-05-29; nice-to-have, speculative). The `whisper.json` transcripts
  already exist (FTS5 search + ad-classify). Could generate per-episode
  summaries or chapter markers from them — `video_analyzer_reference` memory
  noted this as a future template. Feature, not infra.

- **Housekeeping: prune dead dirs/config** (idea 2026-05-29; low value).
  Stale leftovers: `~/MPVKit-fork-stale`, `~/go2rtc` (container not running),
  the disabled tvh-config-snapshot remnants, and the `~/caddy/Caddyfile` vs
  git-copy drift (a deploy-from-git or drift-check would prevent recurrence).
