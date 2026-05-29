# Notes for future Claude sessions

This file is project context for Claude Code. Read it before
suggesting changes.

## What this is

Flask-based HLS gateway that sits in front of tvheadend on a Raspberry
Pi 5 + bridges live TV + recorded DVR + Mediathek streams to clients
(iOS app, web UI, mpv, AVPlayer, browsers). The single `service.py`
(~21 000 lines) hosts the entire orchestration:

- Live HLS spawning + warm-pool management per channel
- DVR recording → HLS-VOD remux + thumbnail extraction (offloaded to
  a Mac daemon to keep Pi CPU free)
- Ad-detection orchestration (tv-detect with NN head) + smart-merge
  of user reviews
- Mediathek-live passthru for public broadcasters (ARD/ZDF/Arte/etc)
- EPG archive + Now-Next + Bibliothek/Recordings/Search HTML pages
- App API (`/api/recordings`, `/api/series`, `/api/channels`,
  `/api/app/live/<slug>/*`)

Sister repos:
- `simonchrz/tvheadend` — tvh config snapshots + mac-daemon scripts +
  ops docs (was hls-gateway's parent until 2026-05-22 when hls-gateway
  graduated to its own repo)
- `simonchrz/tv-detect` — Go ad-detection binary + Python train-head.py
  ML pipeline

## Where the code runs

`/home/simon/hls-gateway/` on `raspberrypi5lan` (Debian/Bookworm).
Container runs via `docker compose up -d` with `network_mode: host`.
The git repo IS at that path — Pi pushes to GitHub directly via SSH
key. Mac clone at `~/src/hls-gateway/` is read-only / pull-for-review;
authoritative edits happen on Pi.

Three-process pipeline per service:
```
Caddy :8443 (TLS)  →  Flask :8080 (service.py)  →  tvheadend :9981
```
plus ffmpeg subprocesses spawned per active channel (live HLS feed)
and per recording (HLS-VOD remux, but the recording-side mostly
offloads to the Mac daemon now).

## Deploy workflow

Two equivalent paths, both end up at the same git state:

**Edit-on-Pi (most common):**
```sh
ssh simon@raspberrypi5lan
cd /home/simon/hls-gateway
# edit service.py
docker compose restart hls-gateway   # OR send SIGHUP — see below
git add service.py && git commit -m "..." && git push
```

**Edit-on-Mac:**
```sh
# edit ~/src/hls-gateway/service.py
scp ~/src/hls-gateway/service.py simon@raspberrypi5lan:/home/simon/hls-gateway/service.py
ssh simon@raspberrypi5lan 'cd /home/simon/hls-gateway && docker compose restart hls-gateway && git add service.py && git commit && git push'
```

**Hot-reload via SIGHUP** (preserves ffmpeg subprocesses):
service.py has a file-watcher loop (`_file_watcher_loop`) that polls
its own mtime every 0.5 s. When the file changes AND `compile()` passes,
the process self-execs (`os.execv`) so child ffmpegs (started with
`start_new_session=True`) survive the reload. `docker compose restart`
kills them; SIGHUP keeps live viewers running. Workflow:
```sh
scp service.py ...   # triggers self-exec on Pi
# no restart needed
```

**Don't ever** kill ffmpeg processes or restart docker during active
recordings — `never_restart_tvh_during_recordings` in your auto-memory
covers why (epgdb snapshot triggers mux-deletes mid-air, recordings
become 5 MB junk).

## Code organisation in service.py

`service.py` is one giant file. Search by route or by section comment:

| Section | Approx line | What |
|---|---|---|
| Constants + locks | 80-300 | env vars, HLS_DIR, ALWAYS_WARM, MAX_WARM_STREAMS, etc |
| `slugify`, `_normalize_title` | 430+ | string utilities |
| `stop_channel`, `ensure_running` | 754-800 | warm-pool + ffmpeg lifecycle |
| `_app_track` + `_app_idle_loop` | ~820 | single-tuner-per-app session tracking |
| `idle_killer_loop` | ~870 | reaps stale warm channels |
| `_cors`, `index()` | 1029+ | CORS helper + status dashboard |
| `/playlist.m3u`, `/hls/<slug>/*` | 1486-1700 | live HLS endpoints (shared pool) |
| `/api/app/live/<slug>/*` | ~1700 | app-scoped live HLS (single-tuner cap, 60 s idle) |
| `fetch_epg`, EPG archive | 1670+ | EPG fetch + persist + Now-Next |
| `/api/now/<slug>` | 7766 | current EPG show + show-poster |
| `/api/channels` | ~7790 | channel listing (icon + via + current show) |
| `/api/internal/*` | many spots | Mac-daemon coordination endpoints |
| `/recording/<uuid>/*` (player + assets) | ~11800 | HLS playlist + segments + ads + thumbs + poster |
| `/api/recording/<uuid>/{watched,playposition}` | 8784, 8830 | writable resume state (POST) |
| `/api/recordings` | ~13150 | list + filters + delta-sync (`?since=`) |
| `/api/recordings/<uuid>` | ~13230 | single-recording detail |
| `/recording/<uuid>/poster.jpg` | ~17050 | 302 → TMDB/fernsehserien show poster |
| `_show_poster_url`, `_recording_app_schema` | helpers | shared schema for /api/recordings + /api/series |
| `_bib_bucket_completed`, `_bib_tiles_data` | ~20300 | bibliothek grouping + tile-building (reused by /api/series) |
| `/api/series` | ~20420 | aggregated per-show JSON for app |
| `/bibliothek` HTML page | ~20440 | web-UI tile grid (still its own renderer; tile-data helper extracted but page not refactored to use it yet) |
| `/learning` HTML page | ~4344 | active-learning + per-show drift + failure-mode analyse |
| `_file_watcher_loop`, `_self_exec` | ~20230 | hot-reload via SIGHUP |
| Thread launches | ~21250 | main() spawns idle_killer + app_idle + warmer + train-watcher etc. |

Line numbers drift as the file grows — use the route decorator or
function name as anchor, not the literal line number.

## The app API (added 2026-05-22+)

External app clients (iOS app at `simonchrz/tv-app`) use a parallel set
of `/api/*` endpoints — schema documented in
`hls-gateway/docs/app-api.md` (TODO) or the commit-history of
`a6f5d60..HEAD`. Quick reference:

- **`GET /api/recordings`** — playable recordings (Bibliothek filter),
  with `?since=`, `?watched=`, `?limit=`, `?include_recording=`
- **`GET /api/recordings/<uuid>`** — single recording (= refresh
  detail-view without re-fetching list)
- **`POST /api/recording/<uuid>/playposition`** — body `{position: N}`
  writes `playposition` via tvh idnode/save. Cross-device resume.
- **`POST /api/recording/<uuid>/watched`** — body `{watched: bool}`,
  sets `playcount`. Auto-cleanup deletes watched after 7 days.
- **`GET /api/series`** — per-show grouping (autorec + user-groups +
  orphan-by-(title,channel) + cross-channel merge), with TMDB/
  fernsehserien posters
- **`GET /api/channels`** — live-TV channel list, sorted by recent
  watch-time. Includes `app_live_url` + `current` EPG + `via` field
  ("mediathek" | "tuner")
- **`GET /api/now/<slug>`** — current show + poster
- **`GET /api/app/live/<slug>/index.m3u8`** + `dvr.m3u8` —
  app-scoped live HLS with single-tuner-cap (kill-on-switch,
  APP_IDLE_TIMEOUT=60s). Falls slug in MEDIATHEK_LIVE → delegated to
  `/mediathek-passthru/<slug>/master.m3u8` (= zero tuner usage for the
  6 public broadcasters)
- **`GET /recording/<uuid>/poster.jpg`** — 302 redirect to show-poster
  (TMDB/fernsehserien); thumb-frame as fallback
- **`GET /recording/<uuid>/index.m3u8`** + `seg_*.ts` — HLS playlist
  with RFC 8216 §4.1 relative URIs; segments served under the same
  prefix (= `/recording/<uuid>/seg_*.ts`)
- **`GET /recording/<uuid>/ads`** — smart-merged ad blocks (auto +
  user edits + deletions)

CORS-enabled on all `/api/*`. Most recording URLs return absolute
URLs prefixed with `HOST_URL` env var (= `https://raspberrypi5lan:8443`).

## Two distinct live-stream pools

- **Shared warm pool** (`/hls/<slug>/*`): MAX_WARM_STREAMS=3,
  WARM_TTL_SECONDS=180. Used by web UI, /playlist.m3u (VLC etc),
  /bibliothek browser-player. LRU eviction when 4th channel
  requested; ALWAYS_WARM channels never evicted.
- **App single-tuner pool** (`/api/app/live/<slug>/*`):
  APP_IDLE_TIMEOUT=60. Switching channels stops the previous app
  channel immediately (unless web UI is also watching it via shared
  pool, detected via 10 s last-seen window). One concurrent app
  channel max system-wide. Tracked via `_app_session` dict.

For mediathek-equipped channels (ARD, ZDF, 3sat, Arte, KiKA,
Tagesschau24): `/api/app/live/<slug>/*` delegates to
`/mediathek-passthru/<slug>/master.m3u8` so no tuner is used at all.
DVR + private-broadcaster app streams get full tuner capacity even
while the app shows ÖR live TV.

## Mac-daemon offload pipeline

The Pi's heavy work (HLS remux, thumbs, ad-detect) is offloaded to
`~/bin/tv-thumbs-daemon.py` on the Mac via HTTP-only protocol (NO SMB,
TCC restrictions kill it). Pi writes `.requested` marker files;
daemon polls `/api/internal/{thumbs,hls,detect}-pending`, runs work
locally, PUTs results back.

**Pi-local HLS fallback is DISABLED (`HLS_FALLBACK_S=0`, 2026-05-28).**
The Pi 5 has no HW H.264 encoder; any local libx264 remux spikes load
to 80+ and starves live recordings, so the Mac is the sole ffmpeg host.
Markers wait until the Mac picks them up (daemon `HLS_PARALLEL=2`,
always-on). Related guards added the same day:
- `REMUX_MAX_PARALLEL=2` semaphore caps Pi-local remux IF ever
  re-enabled.
- Prewarm skips queueing a remux when the source `.ts` is missing
  (dead schedule entries → endless 404 loop otherwise).
- Blackframe-snap in `/ads` is gated behind `BLACKFRAME_EXTEND=1`
  (default off) — it spawned an unbounded ffmpeg blackdetect storm per
  uncached poll.
- comskip-parser derives true fps from frame-count/duration for
  interlaced SD (field-rate inflation placed ad blocks past the
  recording end); `/ads` also clamps blocks to playable duration.
- Latin-1 filenames auto-renamed → UTF-8 at startup.

Key internal endpoints:
- `GET /api/internal/{thumbs,hls,detect}-pending` — work queue
- `GET /recording/<uuid>/source` — Range-streamed .ts (no SMB)
- `PUT /api/internal/hls-segment/<uuid>/<fname>` — per-segment upload
- `POST /api/internal/hls-done/<uuid>` — finalise
- `POST /api/internal/cutlist-uploaded/<uuid>` — ad-detect cutlist
- `GET /api/internal/detect-config/<uuid>` — per-job config bundle
- `GET /api/internal/detect-models/...` — model file fetch
- `GET /api/internal/training-snapshot` — bulk metadata dump for
  `tv-train-head.py` (replaces SMB-mount glob)

Full architecture documented in `simonchrz/tvheadend/docs/mac-handlers.md`.

## Bibliothek + Series (consumer-side)

`/bibliothek` is the watch-focused view (separate from `/recordings`
which is admin-side). Tile grid per show, click → episode list →
player. Same grouping logic powers `/api/series` for the app:

1. **Bucketing**: user-groups → autorec → orphan-by-(title, channel)
2. **Cross-channel merge** by `_normalize_title` (SpongeBob on
   Nick+COMEDY CENTRAL collapses to ONE bucket)
3. **Playability filter**: sched_status in
   `(completed, completedError)` AND (tvh .ts ≥ 50 MB OR HLS-VOD with
   ≥ 20 segments) — second path covers Pi-disk dedup'd recordings
   where the .ts is gone but the HLS bundle stays
4. **Movie vs Series classification**: user-group → movie; autorec →
   series; EPG-fallback titles like `"VOX (06:48)"` → series;
   multi-rec with ` - <subtitle>` → series (= no Staying-Alive FP);
   TMDB `kind=movie` + ≥ 80 min → movie; ≥ 80 min without TMDB → movie
5. **Poster source**: fernsehserien.de first (German shows), TMDB
   always (for `kind` + portrait `tmdb_poster`), TVmaze last fallback.
   Films pick `tmdb_poster` at render-time; series pick whatever
   landed in `meta["poster"]`

`_bib_tiles_data()` returns the structured list; `/api/series` uses
it as-is. `/bibliothek` HTML page has its own (slightly older) copy
of the tile-building logic — eventual cleanup is to refactor the page
to use the helper too.

## Mediathek passthru

`MEDIATHEK_LIVE` dict (constant ~line 7570) maps 6 public-broadcaster
slugs to `(upstream_HLS_url, restart_window_seconds)`:
```python
"das-erste-hd":    "https://daserste-live.ard-mcdn.de/...",     7200
"tagesschau24-hd": "https://tagesschau-live.ard-mcdn.de/...",   7200
"zdf-hd":          "https://zdf-hls-15.akamaized.net/...",     10800
"3sat-hd":         "https://zdf-hls-18.akamaized.net/...",     10800
"arte-hd":         "https://artesimulcast.akamaized.net/...",   1800
"kika-hd":         "https://kikageohls.akamaized.net/...",      7200
```

`/mediathek-passthru/<slug>/master.m3u8` fetches the upstream master,
parses the top-bandwidth rendition, rewrites variant URIs through
`/mediathek-passthru/<slug>/pl.m3u8?u=<encoded>` so all playlist
origins stay on the gateway (= iOS cross-origin sidestep).

`/api/channels` exposes `via: "mediathek" | "tuner"` + `mediathek_window`
so the app can render badges. App-live endpoints
(`/api/app/live/<slug>/*`) auto-route to passthru when slug is in
MEDIATHEK_LIVE; `_app_track(None)` releases any held tuner.

## Channel logos

`static/ch-logos/<slug>.{png,svg}` — 25 PNGs + 23 SVGs, curated per
channel. PNG was added 2026-05-22 because iOS UIImage can't render
SVG natively. `_channel_logo_url(slug, fallback, ext_priority=...)`
returns the best match; `ext_priority=("svg", "png", "jpg")` is the
default (web UI wins), but `/api/recordings` + `/api/series` +
`/api/channels` override to `("png", "svg", "jpg")` for iOS.

Unknown channels (slug not in static/ch-logos/) fall back to tvh's
`imagecache/<id>` URL via Caddy's `/imagecache/*` reverse-proxy.

PNGs are generated from SVGs via `rsvg-convert -h 192`. When you add
a new SVG, also generate the PNG:
```sh
cd static/ch-logos/
rsvg-convert -h 192 -o <slug>.png <slug>.svg
```

## Hot gotchas (from session memory)

- **HLS playlist URIs**: ffmpeg writes segment URIs as URL-paths like
  `/hls/_rec_<uuid>/seg_*.ts` (= absolute path, NOT bare filename).
  For app endpoints the `/recording/<uuid>/index.m3u8` handler
  rewrites these to bare filenames so `seg_*.ts` resolves relative to
  the playlist URL → `/recording/<uuid>/seg_*.ts` (= separate Flask
  route in this same service.py).
- **f-string + JS comments**: in service.py's embedded JS (which
  lives inside Python f-strings), use `/* */` comments INSIDE
  f-strings and `#` comments BETWEEN them. `//` inside an f-string
  kills the JS at that line; `/* */` between f-strings kills the
  Python parser. (auto-memory entry: `no_inline_comments_in_fstring_js`)
- **`Path(uri).name` for FS, raw URI for HTTP**: HLS playlist URIs
  inside index.m3u8 are URL-paths like `/hls/_rec_<uuid>/seg_*.ts`,
  not relative filenames. For filesystem ops use `Path(uri).name`;
  for HTTP responses keep the URI as-is. (auto-memory:
  `hls_playlist_uri_basename`)
- **Never cat ads.json or whisper.json directly**: they're lazily
  regenerated from `.txt` + `ads_user.json`. Call the
  `/recording/<uuid>/ads` endpoint instead. (auto-memory:
  `never_cat_gateway_caches`)
- **Bash `$VAR` word-splitting in nohup**: don't use unquoted env
  expansion inside background-spawned shell commands. Use a bash
  array `"${ARR[@]}"` or explicit args. (auto-memory:
  `bash_var_word_split_in_nohup`)
- **tvh dvr grid pagination**: don't hardcode `?limit=N` for fetches
  that need to be complete. The library has grown to 500+ entries;
  filter-by-uuid > pagination > hardcoded-limit. (auto-memory:
  `tvh_grid_limit_pagination_bomb`)
- **Source-cache stub trap**: get_source historically accepted
  truncated TS stubs from partial HTTP fetches and stalled ffmpeg at
  rc=234. Threshold is now 100 MB + Content-Length sanity check.
  (auto-memory: `source_cache_stub_trap`, `source_cache_truncation_silent`)

## Pi5 ops context

- **NVMe APST bug fixed 2026-05-22**: WD_BLACK SN770M had recurring
  PCIe-link-disconnects (`Identify namespace failed`, `Disabling
  device after reset failure: -19`). Root cause was the classic
  Linux+NVMe APST power-saving bug. Fix: kernel cmdline flags in
  `/boot/firmware/cmdline.txt`:
  ```
  nvme_core.default_ps_max_latency_us=0 pcie_aspm=off pcie_port_pm=off
  ```
  Stable since deployment. If you see those errors again, check
  cmdline first. (auto-memory: `nvme_controller_hang_recovery`)
- **Persistent journal**: Pi-OS ships a vendor drop-in
  `/usr/lib/systemd/journald.conf.d/40-rpi-volatile-storage.conf`
  that forces `Storage=volatile`. Override with a same-named file in
  `/etc/systemd/journald.conf.d/` + `journalctl --flush`. Without
  this, `journalctl -b -1` returns nothing after a crash — no
  post-mortem possible. (auto-memory: `pi5_no_persistent_journal`)
- **piped-backend mem_limit**: `docker-compose.yml` for piped has
  `mem_limit: 2g`. Without it, a Liquibase DB migration can pump
  memory until the kernel OOM-killer picks a victim — observed
  2026-05-21 crashing the Pi mid-recording. cgroup_memory=1 in the
  kernel cmdline is required for this to take effect (already set).

## Useful invariants

- **`HOST_URL`** env var = `https://raspberrypi5lan:8443` (public
  Caddy/TLS URL). Use it for any absolute URL the gateway emits —
  `request.host_url` sees the internal `http://internal:8080` view
  because Caddy strips the scheme.
- **`HLS_DIR`** = `/data/hls` inside container = `/mnt/tv/hls` on Pi
  filesystem. Recording dirs: `_rec_<uuid>/` with `index.m3u8`,
  `seg_*.ts`, `<title>.txt` (cutlist), `ads_user.json`, `ads.json`,
  `thumbs/t<NNNNN>.jpg`.
- **`TVH_BASE`** = `http://raspberrypi5lan:9981` — tvheadend HTTP API.
  Most metadata queries go through `/api/dvr/entry/grid*` or
  `/api/idnode/save`. tvh's grid endpoints used: `grid` (all),
  `grid_finished` (completed only), `grid_upcoming` (scheduled +
  recording).
- **CORS**: all `/api/*` routes pass through `_cors()` which sets
  `Access-Control-Allow-Origin: *`. Don't return raw `Response()`
  from new app endpoints — wrap.

## Things deliberately not done

- **`?since=` doesn't track deletions**. The delta-sync cursor in
  `/api/recordings` / `/api/series` reports new entries but never
  reports deletes. Clients should periodically full-sync (e.g.
  once/day) to reconcile, OR call `/api/recordings/<uuid>` and
  treat 404 as deletion.
- **App single-tuner cap is global**, not per-client. Two app
  clients sharing the same `_app_session` slot will fight. For
  current single-user setup that's fine.
- **`/bibliothek` HTML page still has its own tile-building code**
  even though `_bib_tiles_data` (used by `/api/series`) extracted
  the logic. Refactor pending; both produce the same data, just
  different render output.
- **Per-device playposition** — tvh's `playposition` field is global
  per recording. Cross-device resume works but two devices playing
  simultaneously overwrite each other. Per-device tracking would
  need a separate SQLite-backed table in the gateway. Deferred until
  multi-user becomes a thing.
