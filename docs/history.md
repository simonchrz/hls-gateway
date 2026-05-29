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

## Done (shipped)

- **tv-receiver: GOP-aligned replay for snappy live-TV channel change**
  (shipped 2026-05-29, commit `bcc3ef6` in tv-receiver). App-dev idea:
  keyframe-cache-on-join. tv-receiver already replayed the slot's last
  ~3s ring to a newly-attached consumer (so the decoder finds a keyframe
  without waiting for the next live IDR), but that replay began mid-GOP
  — mpv demuxed ~1-1.5s of pre-keyframe junk first. `gopAlignReplay`
  (keyframe.go) now trims the snapshot to start at the channel's most
  recent video IDR (PUSI + adaptation random_access_indicator on the
  PMT-derived video PID) with the latest PAT+PMT prepended → consumer's
  first bytes are PAT→PMT→IDR. No CC-rewrite needed (cuts on whole
  ring-element boundaries; only the 2 prepended PSI packets step CC,
  which demuxers tolerate). Safe fallback to full blind replay when no
  keyframe boundary is found, so no channel regresses. Verified live:
  warm prosieben tap trimmed 4780→382 chunks (-92% pre-keyframe replay).
  NOTE: only helps WARM muxes (ring must hold a keyframe); cold-tune
  first-frame is still bounded by the next live IDR — prewarm covers that.
  Follow-ups same day: (a) packet-precise cut (commit in tv-receiver) so the
  first replayed video bytes ARE the IDR packet — dropped the ≤7
  pre-keyframe TS packets that shared the IDR's RTP payload (= the "mid-GOP
  before keyframe" the app-dev saw causing brief h264 errors at join).
  (b) Verified detection is correct, NOT too liberal: on prosieben
  random_access_indicator marks IDRs exactly (ffprobe 14 keyframes = 14
  PUSI+RAI hits over the same 14s capture; GOP ~1.1s, well inside the 3s
  ring). The app-dev's "long GOP exceeds buffer" hypothesis was wrong.
  The residual cold-start "~7s" is the iOS HLS segment floor (~6 segments),
  a separate layer GOP-align doesn't touch — prewarm is the lever there.

- **tv-receiver: autorec cross-UUID dedup** (shipped 2026-05-29). Recurring
  shows double-recorded because the native autorec engine (UUID
  dvr-<slug>-<evStart>) didn't dedup against tvh-migrated timers (UUID
  dvr-<slug>-<paddedStart>) for the same airing. Added
  ScheduleStore.CoveringSchedule (same slug+title, start within 300s) as a
  pre-Add guard. Cleaned up 10 existing dupe clusters = 14.25 GB reclaimed
  (all the redundant copies were migrated-from-tvh; native autorec copy
  kept). Consecutive same-title episodes (≥~20 min apart) are NOT merged.

- **tv-receiver: green-bottom-on-join fix** (shipped 2026-05-29, commit
  `4718499`). Switching channels showed ~1-2s of green macroblocks in the
  bottom of the picture on many channels. Cause: gopAlignReplay started at
  the MOST RECENT keyframe = often the IDR still being transmitted at attach
  (only leading slices in the ring) → shallow-buffer decoder renders a
  half-complete IDR → bottom rows never fill. Fix: replay from the
  SECOND-to-last keyframe (guaranteed complete: whole AU + full trailing GOP
  already in the ring). Ring bumped 6→12 MB so ≥2 keyframes are reliably
  cached; replay stays bounded to ~2 GOPs by the trim. Verified: the
  green-causing `error while decoding MB` count dropped to 0 on
  comedy-central/kabel-eins/prosieben (remaining `missing/mmco` are harmless
  discarded open-GOP B-frames, not green).

- **Adjacency-aware dynamic prewarm (A+B)** (shipped 2026-05-29; tv-receiver
  `5ba1955`, hls-gateway `d7fb38e`). (A) channels.json regrouped by mux so
  the favourites surf-order is mux-contiguous (favourite cold-crossings
  17→7). (B) on every channel switch hls-gateway computes the first
  different-mux favourite in each surf direction and POSTs them to the new
  tv-receiver `POST /api/prewarm` (runtime-updatable Prewarmer), keeping the
  ≤2 neighbouring muxes warm + their replay rings populated for instant
  GOP-aligned switching. Mid-block neighbours are same-mux (already warm) →
  0 extra tuner; only block edges spend one. Baseline `PREWARM_BASE=vox`
  keeps mux 546 warm even when idle. Hook is the live-ads SSE subscribe (the
  raw-TS player path doesn't hit the m3u8 handler). DEPENDENCY: the app must
  surf in /api/channels order for the neighbour computation to match.
  Verified end-to-end (SSE prosieben→[vox,comedy-central], nitro→[vox,3sat-hd]).

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

- ~~**Proactive monitoring / alerting**~~ **DONE 2026-05-29.** HA package
  `/home/simon/homeassistant/packages/tv_monitoring.yaml` (packages enabled
  via a `homeassistant: packages:` line in configuration.yaml). HA is
  host-network so it hits the stack on localhost. 3 REST sensors +
  3 automations → push to `notify.mobile_app_iphone_17_pro`:
    - `sensor.tv_disk_free` ← gateway `/api/warm-status` `disk_free_gb`;
      alert when `<50 GB` for 15 min.
    - `sensor.tv_piped_backend` ← `:8881/healthcheck`; alert when
      `unavailable` for 3 min (YouTube backend down).
    - `sensor.tv_gateway_backend` ← gateway `/healthz`; alert when
      `unavailable` for 5 min (503 = tv-receiver down).
  Validated (`check_config` clean, HA restarted, sensors+automations
  registered, no rest errors). Thresholds are tunable in the package. Could
  extend later: recording-failure + detect-drain signals (see below).

- ~~**Extend YT-resolve isolation to ALL resolve routes**~~ **DOWNGRADED
  2026-05-29 — not worth doing.** On closer look the observed starvation was
  the PLAYER-API path (`/streams`, `/synth-hls`), already capped (commit
  9e3b624) — and with those carriers freed, the browse routes (`/channel`,
  `/c`, `/user`, search) keep working *during* a player-block (they only hung
  as collateral of full carrier-starvation). They'd only need a cap if
  YouTube blocked the browse Innertube path *separately* (never observed),
  and capping ~15 browse routes on a shared Semaphore(2) would cause frequent
  503s during normal browsing (UX cost for speculative gain). `/sponsors` +
  `/dearrow` are external APIs (not YouTube) — irrelevant. Only genuine
  remnant was `/clips` (resolveClipId = a player-resolve, same risk) — **DONE
  2026-05-29 (commit `153d690` in simonchrz/Piped-Backend): wrapped in the
  same ytResolveAcquire()→503 + withResolveBudget(12s) + release pattern.
  Deployed + smoke-verified (limiter 2×200+1×503 intact).** Resolve-isolation
  is now complete across all player-resolve routes.

- ~~**Investigate the yt-proxy throttle (root cause)**~~ **ROOT CAUSE FOUND
  2026-05-29 (empirical).** Measured a real `c=ANDROID_VR` googlevideo URL
  (has cpn, NO `n`/`ratebypass`): bounded **Range requests stream at
  ~1.4 MB/s, 10 rapid same-cpn ranges all 206 (no 403, no count-cap)**, but
  the **no-Range full GET is throttled to ~31 KB/s** (googlevideo anti-
  download). `YtProxyHandlers` does exactly the no-Range pull — its doc
  comment ("no Range → avoids per-cpn rate-limit") is INVERTED, and the
  per-range-403 it warns about does NOT occur on current ANDROID_VR URLs.
  At 31 KB/s < bitrate the cache fills slower than realtime → useless →
  that's why `rewriteToYtProxy` was disabled. See memory
  `googlevideo_throttle_noRange_not_cpn`. **FIX (now actionable, not yet
  built):** rewrite `startDownloader` to fetch sequential BOUNDED range
  chunks (`Range: bytes=s-e`) instead of one no-Range pull → full speed →
  re-enable `rewriteToYtProxy` → segment caching returns → unlocks the
  first-segment pre-fetch (846 ms) + cuts repeat-segment YT load. Extend
  smoke-test with a yt-proxy segment 206 at full speed.

- ~~**Smoke test for the Piped-Backend fork**~~ **DONE 2026-05-29 (commit
  `8c2729e` in simonchrz/Piped-Backend `ios-streaming-patches`).**
  `smoke-test.sh` at the fork root: validates the resolve → synth-hls
  master → variant → segment(206) chain AND the YT_RESOLVE_LIMITER (3
  concurrent /streams → ≥1× 503). Run after `docker compose up`
  (`bash ~/piped-backend-src/smoke-test.sh`, default video "Me at the zoo").
  One-shot — does ~2-3 YT resolves, do NOT loop (re-triggers the throttle).
  Verified green on the live backend (2×200+1×503, segment 206). A failure
  during an active IP-block is expected (resolve can't complete), not a fork
  regression.

- ~~**Back up tv-receiver state (DVR schedules / autorec / channel map)**~~
  **DONE 2026-05-29.** `tv-backup-labels.sh` now also rsyncs
  `~/bin/{dvr,autorec,channels}.json` from the Pi into
  `~/tv-labels-backup/tv-receiver-state/` (daily 04:30 launchd job →
  GitHub). `epg.json` skipped (regenerable from the XMLTV feed). Verified:
  the three files committed (snapshot 0e8870f). Restore is documented in the
  script header (`rsync ~/tv-labels-backup/tv-receiver-state/ pi:/home/simon/bin/`
  + `docker restart tv-receiver`). Closes the disaster-recovery gap (schedules
  + autorec rules + the uuid→recording registry survive an SSD death now).

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

- ~~**Housekeeping: prune dead dirs/config**~~ **DONE 2026-05-29.**
  - `~/go2rtc` (16 K leftover, no container, only referenced in stale
    `Caddyfile.bak*`) — removed.
  - `~/tvheadend` (9.8 M old tvh config + `docker-compose.yml.disabled`, no
    container mounts it) — archived to `~/tvheadend-decommissioned-2026-05-27.tar.gz`
    (4.9 M) then dir removed.
  - `~/MPVKit-fork-stale` — didn't exist on Pi or Mac; stale backlog ref.
  - Caddyfile drift: the authoritative pair (deployed `~/caddy/Caddyfile` +
    the git copy) were already in sync; only the Pi's UNUSED
    `~/hls-gateway/caddy/Caddyfile` checkout was ~31 lines stale — synced it
    to deployed (drift=0 everywhere now). Caddy reads `~/caddy/Caddyfile`;
    the git copy is the record. (A deploy-from-git / drift-check to *prevent*
    recurrence wasn't built — low value; the copies match now.)
