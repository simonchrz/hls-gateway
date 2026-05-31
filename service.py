#!/usr/bin/env python3
"""HLS Gateway on-demand for tvheadend. Spawns ffmpeg per channel on request,
stops after idle timeout. Serves an HLS playlist with 2h DVR window."""
import os, re, sys, signal, json, time, shutil, subprocess, threading, urllib.request, sqlite3
import urllib.parse
import datetime
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from flask import Flask, send_from_directory, send_file, abort, Response, request, redirect

HLS_DIR        = Path("/data/hls")
COMSKIP_INI    = HLS_DIR / ".comskip.ini"
COMSKIP_INI_TEXT = """\
; Overrides for comskip defaults, written at service startup.
; Tuned for German private TV (RTL/VOX/Sat.1/ProSieben/Kabel Eins/etc.).

; --- Block-length ceilings ---
; DE-private Werbepausen are often 10-15 min with station promos
; inside; default 10-min ceiling splits one break into two entries.
max_commercialbreak=900
max_commercial_size=150

; --- Minimum thresholds, prevent sponsor cards / promos / single
;     teaser interruptions being mis-detected as ad breaks ---
min_commercialbreak=60       ; default 20 — a real break is >= 60 s total
min_commercial_size=20       ; default 4  — a real spot is >= 20 s
min_show_segment_length=120  ; merge breaks separated by < 2 min of show

; --- Detection methods ---
; default detect_method=47 (Black + Logo + Scene + Resolution + AR).
; Drop AR (32) — every DE channel runs 16:9 since ~2010, AR detection
; produces false positives on dark Letterbox film scenes.
detect_method=15

; Confirm block boundaries with audio silence — DE-private ads have
; a reliable audio dip at start/end.
validate_silence=1

; --- Intro/outro grace ---
; First/last 60 s of each show are excluded from detection — show
; intros (Vorspann) often contain blackframes that look ad-like.
intro_max_seconds=60
outro_max_seconds=60

; --- Logo sensitivity ---
; Default 0.80 = block boundary needs 80 % logo-absence confidence.
; That confidence builds slowly when the logo fades out gradually
; (rtlzwei does this), pushing detected ad-start 15-20 s past the
; visual logo-disappear. 0.60 is more responsive at slight cost in
; false-positives; the .scanning + min_commercialbreak=60 pipelines
; on top filter those out anyway.
logo_threshold=0.60

; --- Logo search area ---
; Constrain logo training to the TOP HALF of the frame. Without this,
; comskip's stable-edge-detection can latch onto a news ticker /
; sponsor strip at the bottom (witnessed on rtlzwei: cached template
; was y=475-553 on a 576-tall frame = bottom-of-screen banner, not
; the actual channel logo). Every DE private/public TV logo is in a
; top corner — the bottom-half scan is pure false-positive surface.
subtitles=1
"""


# Per-channel ini overrides, merged on top of COMSKIP_INI_TEXT at
# comskip invocation time. Comskip's ini parser is first-match-wins,
# so per-channel values are PREPENDED to the base ini (not appended).
# Add entries here when a channel needs a tighter threshold, a
# different logo position, etc.
# Per-channel logo position constraints. Comskip only knows three
# logo-position flags (comskip.c:798-800):
#   subtitles=1       → search top half only
#   logo_at_bottom=1  → search bottom half only
#   logo_at_side=1    → search right half only
# There's no _at_top / _at_left / _at_right, so:
#   top-left   logos → subtitles=1 only (global default; no x flag possible)
#   top-right  logos → subtitles=1 + logo_at_side=1   = TR quadrant
#   bottom-... logos → subtitles=0 + logo_at_bottom=1 (+ logo_at_side
#                      for BR) — must override the global subtitles=1
COMSKIP_INI_PER_CHANNEL = {
    # rtlzwei: bottom-right logo. Override global top-half restriction.
    # Soft logo fade also makes even 0.60 occasionally late — tighter
    # threshold + aggressive_logo_rejection to catch logo-loss faster.
    "rtlzwei": {
        "subtitles": "0",
        "logo_at_bottom": "1",
        "logo_at_side": "1",
        "logo_threshold": "0.50",
        "aggressive_logo_rejection": "1",
    },
    # Top-right corner — combined with global subtitles=1 → top-right
    # quadrant only. ~2× faster logo learning, fewer false positives
    # from left-side promo bugs / DOG watermarks.
    "prosieben":  {"logo_at_side": "1"},
    "kabel-eins": {"logo_at_side": "1"},
    "sixx":       {"logo_at_side": "1"},
    # Top-left corner (vox, rtl, kika-hd, toggo-plus, nitro) gets the
    # global subtitles=1 only — comskip has no logo_at_left flag, so
    # no per-channel entry needed.
}


TVH_BASE       = os.environ.get("TVH_BASE", "http://localhost:9981")
# DVR_BACKEND chooses where /api/dvr/* + /api/autorec + /api/idnode/save
# calls are routed:
#   "tvh"    — original tvh container (default)
#   "local"  — tv-receiver's tvh-compat shim (= our own DVR engine)
DVR_BACKEND    = os.environ.get("DVR_BACKEND", "tvh")


def dvr_base():
    """Base URL for DVR/autorec/idnode/dvrfile + EPG-load HTTP calls.
    Honors DVR_BACKEND. Anything that has a tvh-compat shim in tv-receiver
    routes through here so a single flag flip swaps the whole backend."""
    if DVR_BACKEND == "local":
        return TV_RECEIVER_BASE
    return TVH_BASE
HOST_URL       = os.environ.get("HOST_URL", "http://raspberrypi5lan:8080")
IDLE_TIMEOUT   = int(os.environ.get("IDLE_TIMEOUT", "120"))
MAX_WARM_STREAMS = int(os.environ.get("MAX_WARM_STREAMS", "3"))
WARM_TTL_SECONDS = int(os.environ.get("WARM_TTL_SECONDS", "180"))
# APP_IDLE_TIMEOUT / _app_session / _app_lock removed 2026-05-30 (slice 5):
# external-app live sessions are tracked by tv-receiver now (it serves
# /api/app/live directly).
# Watched recordings older than this get auto-deleted by the cleanup
# loop. tvheadend's `watched` field is read-only/computed; the user-
# settable proxy is `playcount` (>0 means watched). Player auto-marks
# at AUTO_WATCHED_THRESHOLD playback fraction.
WATCHED_AUTO_DELETE_DAYS = int(os.environ.get("WATCHED_AUTO_DELETE_DAYS", "7"))
AUTO_WATCHED_THRESHOLD   = 0.90
# Channels to keep warm permanently. Initial seed from env var, then
# persisted to disk so UI toggles survive restarts. LRU eviction never
# touches these; a background loop re-spawns ffmpeg if they die. Cap
# is auto-raised so viewing still has a free slot.
ALWAYS_WARM = {s.strip() for s in
               os.environ.get("ALWAYS_WARM", "").split(",") if s.strip()}
always_warm_lock = threading.Lock()
FAV_TAG_UUID   = os.environ.get("FAV_TAG_UUID", "ed43d130b6d7f8e56b063db6de8d2b06")
SEGMENT_TIME   = 1                                    # shorter = faster first-load
WINDOW_SECONDS = 2 * 3600
LIST_SIZE      = WINDOW_SECONDS // SEGMENT_TIME       # 7200 segments (2h)
CODEC_CACHE_FILE = HLS_DIR / ".codec_cache.json"
STATS_FILE       = HLS_DIR / ".usage_stats.json"
EPG_ARCHIVE_FILE = HLS_DIR / ".epg_archive.jsonl"
ALWAYS_WARM_FILE = HLS_DIR / ".always_warm.json"
FAVORITES_FILE   = HLS_DIR / ".favorites.json"  # {"slugs": [...]} — replaces tvh's FAV_TAG_UUID tag-based favorites
EPG_META_FILE    = HLS_DIR / ".epg_meta.json"  # tvmaze poster + rating cache
USER_GROUPS_FILE = HLS_DIR / ".user-groups.json"  # manual recording groups (= cross-title franchise grouping like Rocky/Asterix)
AUTO_SCHED_LOG   = HLS_DIR / ".tvd-models" / "auto-schedule-log.jsonl"  # one JSON-line per auto-schedule decision (success or skip)
AUTO_SCHED_PAUSE = HLS_DIR / ".tvd-models" / ".auto-schedule-paused"   # presence = paused; default-paused on first install
AUTO_SCHED_MAX_PER_DAY = 3   # hard cap: auto-creates max N scheduled entries per daily run
AUTO_SCHED_MAX_ACTIVE  = 8   # hard cap: paused once this many auto-scheduled entries sit in the queue
EPG_META_TTL_S   = 30 * 86400                  # re-fetch each title monthly
CH_LOGO_DIR      = Path(__file__).parent / "static" / "ch-logos"
TMDB_API_KEY     = os.environ.get("TMDB_API_KEY", "").strip()  # optional: better DE show coverage
PIN_HARD_MAX = 3   # tuner-driven upper bound; minus active/scheduled DVR jobs
TUNER_TOTAL = int(os.environ.get("TUNER_TOTAL", "4"))   # FRITZ!Box DVB-C tuners
EPG_SNAPSHOT_INTERVAL = 1800  # 30 min — fan-out hits all ~24 channels via /api/epg/events/grid in parallel; tvheadend's table parser overflowed at 10 min when it overlapped with active recordings + DVR API queries
EPG_ARCHIVE_KEEP_DAYS = 14

# How long the sponsor/"Präsentiert von ..."-Einblendung typically is
# per channel. The blackframe-extender adds this to the detected
# transition blackframe after each comskip ad block. Channels not
# listed here use SPONSOR_DURATION_DEFAULT; set to 0 to disable the
# extension entirely for a channel.
SPONSOR_DURATION_DEFAULT = 20.0
SPONSOR_DURATION_BY_CHANNEL = {
    "vox":        25.0,
    "rtl":        20.0,
    "rtlzwei":    20.0,
    "prosieben":  20.0,
    "sat-1":      20.0,
    "kabel-eins": 20.0,
    "sixx":       20.0,
    "super-rtl":  20.0,
    "nitro":      20.0,
    "ntv":        20.0,
    "sport1":     20.0,
}

# Per-channel minimum backward extension when blackframe-extend
# finds nothing usable. Channels with soft logo fades (logo dims
# rather than hard-cuts) leave comskip 15-20 s behind the visual
# ad-start; without a blackframe to snap to we'd otherwise show
# the skip button that late.
START_LAG_FALLBACK = {
    "rtlzwei": 20.0,
}

app = Flask(__name__)


@app.after_request
def _no_cache_dynamic(resp):
    """All dynamically-rendered responses (HTML pages + JSON API
    endpoints) should bypass browser cache. iOS Safari is
    particularly aggressive — without explicit headers it caches:
      - HTML pages → users keep running stale JS for days
      - JSON API responses → polling endpoints like
        /api/warm-status return stale state right after a POST,
        causing the just-toggled pin button to flicker back to its
        old class because refreshWarm sees the cached "still
        pinned" answer.
    Static segments etc. are served by Caddy directly and aren't
    routed through Flask, so they're unaffected."""
    ct = resp.headers.get("Content-Type", "")
    if ct.startswith("text/html") or ct.startswith("application/json"):
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
    return resp


import http.client

class _MediathekUpstreamPool:
    """Per-host HTTP/1.1 keep-alive pool for mediathek-passthru upstream
    GETs. Saves the TLS handshake on each subsequent call to the same
    Akamai/ARD CDN host — the dominant cost on a Mediathek cold-tap.

    Thread-safe via a single lock around dict mutations. Connections are
    rented (= removed from the pool) before each request and returned on
    clean response. On any exception the connection is dropped, not
    returned, so a broken socket doesn't poison subsequent calls.

    Cap at MAX_PER_HOST per host to bound memory + avoid CDN throttling.
    """
    MAX_PER_HOST = 4

    def __init__(self):
        self._pools = {}   # (host, port, scheme) -> list[HTTPConnection]
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def _key(self, parsed):
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        return (parsed.hostname, port, parsed.scheme)

    def _new(self, key, timeout):
        host, port, scheme = key
        if scheme == "https":
            return http.client.HTTPSConnection(host, port, timeout=timeout)
        return http.client.HTTPConnection(host, port, timeout=timeout)

    def _pool_alive_check(self, conn):
        """Quick liveness test for a rented keep-alive connection. Returns
        True if the underlying socket appears alive, False if the server
        has closed it. ~10us syscall — cheap enough to do on every rent.

        Method: non-blocking MSG_PEEK of 1 byte. EAGAIN/BlockingIOError =
        socket idle and alive. b'' return = server sent FIN, socket is dead.
        Any OS error = socket is broken. Other data = unexpected pipelining,
        treat as broken (caller will get fresh).
        """
        import socket as _socket
        sock = getattr(conn, "sock", None)
        if sock is None:
            return False
        try:
            sock.setblocking(False)
            try:
                data = sock.recv(1, _socket.MSG_PEEK)
            except (BlockingIOError, InterruptedError):
                return True  # EAGAIN — socket idle, healthy
            except (OSError, ConnectionError):
                return False
            finally:
                sock.setblocking(True)
            # data == b'' means server closed (FIN received).
            # any unexpected bytes mean weird pipeline state, treat as bad.
            return bool(data)
        except Exception:
            return False

    def urlopen(self, url, timeout=8, headers=None):
        """GET url, reusing an existing keep-alive connection if any.
        Returns body bytes. Raises on non-2xx or transport error.

        Robustness (Step-7a): liveness-check on rent + retry-once on
        connection-reset errors for reused conns.
        """
        import http.client as _httpc
        parsed = urllib.parse.urlparse(url)
        key = self._key(parsed)
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query

        # Rent a live connection, or open new
        conn = None
        is_reused = False
        while True:
            with self._lock:
                bucket = self._pools.get(key)
                candidate = bucket.pop() if bucket else None
            if candidate is None:
                conn = self._new(key, timeout)
                is_reused = False
                break
            if self._pool_alive_check(candidate):
                conn = candidate
                is_reused = True
                break
            # dead rented conn — discard, try next
            try: candidate.close()
            except Exception: pass

        req_headers = {
            "Host": parsed.hostname,
            "Connection": "keep-alive",
            "Accept": "*/*",
            "User-Agent": "hls-gateway/mediathek-passthru",
        }
        if headers:
            req_headers.update(headers)

        retry_eligible_excs = (
            _httpc.RemoteDisconnected, _httpc.BadStatusLine,
            BrokenPipeError, ConnectionResetError, ConnectionAbortedError,
        )

        attempt = 0
        while True:
            try:
                conn.request("GET", path, headers=req_headers)
                resp = conn.getresponse()
                body = resp.read()
                status = resp.status
                keep = (status < 500
                        and resp.getheader("Connection", "").lower() != "close")
                if keep:
                    with self._lock:
                        bucket = self._pools.setdefault(key, [])
                        if len(bucket) < self.MAX_PER_HOST:
                            bucket.append(conn)
                        else:
                            conn.close()
                else:
                    conn.close()
                with self._lock:
                    if is_reused: self._hits += 1
                    else: self._misses += 1
                if status < 200 or status >= 300:
                    raise IOError(f"{url}: HTTP {status}")
                return body
            except retry_eligible_excs as e:
                try: conn.close()
                except Exception: pass
                if attempt == 0 and is_reused:
                    # The peek said alive but server tore down between peek
                    # and request. Open fresh and try once more — this is
                    # the user-visible 502 we observed in step-4 curl-sim.
                    attempt += 1
                    conn = self._new(key, timeout)
                    is_reused = False
                    continue
                raise
            except Exception:
                try: conn.close()
                except Exception: pass
                raise

    def stats(self):
        with self._lock:
            return {"hits": self._hits, "misses": self._misses,
                    "hosts": {f"{k[0]}:{k[1]}": len(v)
                              for k, v in self._pools.items()}}

_mediathek_upstream_pool = _MediathekUpstreamPool()


class _MediathekManifestCache:
    """LRU+TTL cache for Mediathek upstream manifest bodies (= the actual
    response from Akamai/ARD CDN, NOT our rewritten proxy version). Each
    entry is keyed by upstream URL.

    Two TTLs:
      MASTER_TTL  = 30s    — master.m3u8 only lists variants, very stable
      VARIANT_TTL = 1.5s   — sub-playlist updates each ~2s TARGETDURATION;
                              keep just under that to avoid serving stale

    Speculative variant prefetch: on master cache-miss, the freshly-fetched
    master.m3u8 is parsed via the existing _parse_master() and the picked
    video+audio variant URLs are submitted to a small background pool.
    By the time mpv requests them (~50ms later), they're cached. This is
    the structural fix for split-stream channels (ZDF/ARD) where mpv would
    otherwise sequentially fetch master → video-variant → audio-variant.
    """

    MASTER_TTL = 30.0
    VARIANT_TTL = 1.5
    MAX_ENTRIES = 64

    def __init__(self):
        import collections
        self._lock = threading.Lock()
        self._cache = collections.OrderedDict()  # url -> (expires_at, body)
        # Small pool — 4 threads handle parallel video+audio variant fetches
        # for 2 simultaneous master fetches comfortably.
        from concurrent.futures import ThreadPoolExecutor
        self._prefetch_pool = ThreadPoolExecutor(
            max_workers=4, thread_name_prefix="mediathek-prefetch")
        self._hits = 0
        self._misses = 0
        self._speculative_filled = 0
        self._evicted = 0

    def _peek(self, url):
        """Cache lookup. Returns body bytes or None. Does NOT track stats
        — caller distinguishes hits/misses based on the return value."""
        now = time.time()
        with self._lock:
            entry = self._cache.get(url)
            if entry is None:
                return None
            expires_at, body = entry
            if now >= expires_at:
                del self._cache[url]
                self._evicted += 1
                return None
            self._cache.move_to_end(url)
            return body

    def _put(self, url, body, ttl, speculative=False):
        expires_at = time.time() + ttl
        with self._lock:
            if url in self._cache:
                self._cache.move_to_end(url)
            self._cache[url] = (expires_at, body)
            if speculative:
                self._speculative_filled += 1
            while len(self._cache) > self.MAX_ENTRIES:
                self._cache.popitem(last=False)
                self._evicted += 1

    def get_or_fetch_master(self, url, fetch_fn):
        """Returns master.m3u8 body bytes. Cache-hit serves immediately;
        miss fetches via fetch_fn, stores, and schedules background
        prefetch of the variants this master points at."""
        cached = self._peek(url)
        if cached is not None:
            with self._lock: self._hits += 1
            return cached
        with self._lock: self._misses += 1
        body = fetch_fn(url)
        self._put(url, body, self.MASTER_TTL)
        # Speculative variant prefetch
        try:
            picked = _parse_master(url, body.decode())
        except Exception:
            picked = None
        if picked:
            video_url, audio_url, _ = picked
            for variant_url in (video_url, audio_url):
                if variant_url:
                    self._prefetch_pool.submit(
                        self._speculative_fetch_variant, variant_url, fetch_fn)
        return body

    def get_or_fetch_variant(self, url, fetch_fn):
        """Returns variant playlist body bytes. Cache-hit is the common
        case after master-fetch's speculative prefetch."""
        cached = self._peek(url)
        if cached is not None:
            with self._lock: self._hits += 1
            return cached
        with self._lock: self._misses += 1
        body = fetch_fn(url)
        self._put(url, body, self.VARIANT_TTL)
        return body

    def _speculative_fetch_variant(self, url, fetch_fn):
        """Background-fetch a variant we expect mpv to request soon. No
        stats-tracking on hit-check (= we don't want speculative double-
        check to inflate miss counter). On error: silent — speculative
        is best-effort, real fetch will retry."""
        if self._peek(url) is not None:
            return
        try:
            body = fetch_fn(url)
            self._put(url, body, self.VARIANT_TTL, speculative=True)
        except Exception:
            pass

    def stats(self):
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total) if total else 0.0
            return {
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(hit_rate, 3),
                "speculative_filled": self._speculative_filled,
                "evicted": self._evicted,
                "size": len(self._cache),
            }


_mediathek_manifest_cache = _MediathekManifestCache()



channels     = {}    # slug -> {"process","last_seen","started_at"}
channel_map  = {}    # slug -> {"name","uuid"}
codec_cache  = {}    # slug -> {"video": "h264"|..., "audio": "aac"|...}
stats        = {}    # slug -> {"starts", "watch_seconds", "last_watched"}
cmap_lock    = threading.RLock()
active_lock  = threading.RLock()
codec_lock   = threading.RLock()
stats_lock   = threading.RLock()

# codecs iOS/Safari supports natively (pass-through without transcoding)
SAFE_VIDEO = {"h264", "hevc"}
SAFE_AUDIO = {"aac"}

# Channels whose tvh-IPTV-input has chronic minor cc-errors that libx264's
# default (B-frames + CABAC + multi-ref) amplifies into visible decoder
# corruption via broken reference chains. For these, use an error-
# resilient libx264 config: no B-frames, single reference, CABAC off,
# aggressive keyframe interval. Trade-off: ~15-20% larger output bitrate
# but decoder recovers from packet loss within 0.6s instead of cascading.
# Currently only RTL — its FritzBox-SAT>IP source has ~0.6-1.5% TS cc-
# errors that no amount of buffer-tuning eliminates (2026-05-26).
ERROR_RESILIENT_TRANSCODE = {"rtl"}

# Per-channel override: route the upstream RTSP-RTP-to-HTTP-TS stream
# via tv-receiver (Go binary, gortsplib direct RTSP-client) instead of
# tvh's IPTV-input. tv-receiver uses slug-keyed URLs, not UUIDs.
# 2026-05-26: RTL added after a 15-min direct-RTSP test showed 0%
# continuity-counter errors vs tvh's chronic 0.59-1.5%. tvh's
# IPTV-input is the cause of the corruption — bypass it for affected
# channels. See ~/src/tv-receiver/README.md.
TV_RECEIVER_BASE = os.environ.get("TV_RECEIVER_BASE", "http://localhost:9983")

# Fallback when tv-receiver is unreachable at startup. These three were
# the first channels we routed through tv-receiver (originally to bypass
# tvh's broken IPTV-input for RTL); they're guaranteed-present in any
# sane tv-receiver channels.json.
TV_RECEIVER_FALLBACK_SLUGS = {"rtl", "rtlzwei", "vox"}

def _fetch_tv_receiver_slugs():
    try:
        with urllib.request.urlopen(
            f"{TV_RECEIVER_BASE}/api/channels", timeout=3) as r:
            data = json.load(r)
            slugs = set(data.get("slugs") or [])
            if slugs:
                return slugs
    except Exception as e:
        print(f"tv-receiver /api/channels fetch failed: {e} — "
              f"falling back to hardcoded set", flush=True)
    return TV_RECEIVER_FALLBACK_SLUGS

# Resolved at startup; refresh by restarting the container.
TV_RECEIVER_SLUGS = _fetch_tv_receiver_slugs()
print(f"tv-receiver routes {len(TV_RECEIVER_SLUGS)} channel slugs: "
      f"{sorted(TV_RECEIVER_SLUGS)}", flush=True)

BASE_CSS = """
:root {
    --bg: #fafafa; --fg: #222; --muted: #777;
    --border: #ddd; --stripe: #f0f0f0;
    --link: #0366d6; --code-bg: #f3f3f3;
    /* Dark plaque works for both bright-coloured and white-on-transparent picons */
    --logo-bg: #1f1f1f;
}
@media (prefers-color-scheme: dark) {
    :root {
        --bg: #1a1a1a; --fg: #e4e4e4; --muted: #999;
        --border: #333; --stripe: #242424;
        --link: #79b8ff; --code-bg: #2a2a2a;
        --logo-bg: #1f1f1f;
    }
}
* { box-sizing: border-box; }
body {
    font-family: -apple-system, BlinkMacSystemFont, sans-serif;
    background: var(--bg); color: var(--fg);
    max-width: 720px; margin: 0 auto; padding: 1.2em;
    line-height: 1.5;
}
h1, h2 { color: var(--fg); }
a { color: var(--link); text-decoration: none; }
a:hover { text-decoration: underline; }
code {
    font-size: 0.8em; color: var(--muted);
    background: var(--code-bg);
    padding: 1px 6px; border-radius: 3px;
    word-break: break-all;
}
ul.tools { line-height: 1.8; padding-left: 1.2em; }
ul.channels {
    list-style: none; padding: 0; margin: 0;
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(var(--tile-min, 110px), 1fr));
    gap: 10px;
}
ul.channels .logo {
    width: var(--logo-w, 80px); height: var(--logo-h, 60px);
}
body.tile-sm { --tile-min: 80px; --logo-w: 60px; --logo-h: 45px; }
body.tile-lg { --tile-min: 150px; --logo-w: 110px; --logo-h: 82px; }
ul.channels li {
    position: relative;
    display: flex; flex-direction: column; align-items: center;
    justify-content: flex-start; gap: 6px;
    padding: 12px 6px 8px;
    border: 2px solid var(--mux-color, var(--border));
    border-radius: 8px;
    background: var(--stripe);
    transition: border-left-width .12s, padding-left .12s;
}
ul.channels li:has(.buffer-bar.running) {
    border-left-width: 5px;
    padding-left: 3px;   /* compensate so inner content doesn't shift */
}
.now-title {
    font-size: .72em; line-height: 1.2; color: var(--muted);
    text-align: center;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
    overflow: hidden;
    word-break: break-word;
    max-width: 100%;
    min-height: 0;
}
.now-title:empty { display: none; }
ul.channels .logo {
    display: flex; align-items: center; justify-content: center;
    overflow: hidden;
}
ul.channels .logo img { max-width: 100%; max-height: 100%; }
/* Dark-mode only: mid-grey backdrop behind logos so dark channel
   logos (Das Erste, Eurosport) AND light ones (NITRO, sixx) both
   stay readable on the dark tile body. Light mode page bg is
   already #fafafa, no backdrop needed. */
@media (prefers-color-scheme: dark) {
    ul.channels .logo,
    .ch-cell img {
        background: rgba(120, 120, 120, 0.55);
        border-radius: 4px;
        padding: 4px;
    }
    .ch-cell img { padding: 2px; border-radius: 3px; }
}

/* ===== EPG layout: wrap scrolls h+v, channels column sticky ===== */
.epg-wrap {
    display: flex;
    border: 1px solid var(--border); border-radius: 6px;
    margin: 1em 0;
    max-height: calc(100vh - 170px);
    overflow: auto;
    overscroll-behavior: contain;
}
.epg-channels {
    flex: 0 0 64px;
    background: var(--bg);
    border-right: 1px solid var(--border);
    position: sticky; left: 0; z-index: 2;
}
.epg-tl-scroll {
    flex: 1 1 auto;
    overflow: visible;
}
.epg-tl-inner {
    position: relative;
    /* width is set inline per request */
}
.ch-cell {
    height: 52px;
    display: flex; align-items: center; justify-content: center;
    padding: 4px 6px;
    border-bottom: 1px solid var(--border);
    text-decoration: none; color: var(--fg);
    box-sizing: border-box;
}
.ch-cell:last-child { border-bottom: none; }
.ch-cell img { width: 48px; height: 36px; object-fit: contain;
               flex: 0 0 48px; }
.ch-cell .name { font-weight: 600; font-size: .75em;
                 white-space: nowrap; overflow: hidden;
                 text-overflow: ellipsis; text-align: center; }
.ch-cell .name.fallback { max-width: 52px; }
.ch-cell.header {
    height: 32px;
    background: var(--stripe);
    color: var(--muted);
    font-size: .8em;
}
.tl-row {
    height: 52px;
    position: relative;
    border-bottom: 1px solid var(--border);
    box-sizing: border-box;
}
.tl-row:last-child { border-bottom: none; }
.tl-row.header {
    height: 32px;
    background: var(--stripe);
}
.epg-event {
    position: absolute; top: 4px; bottom: 4px;
    background: var(--code-bg);
    padding: 3px 6px; border-radius: 4px;
    overflow: hidden;
    border: 1px solid var(--border);
    font-size: .82em; line-height: 1.2;
    box-sizing: border-box;
    text-decoration: none; color: inherit;
    -webkit-touch-callout: none;
    -webkit-user-select: none; user-select: none;
}
.epg-event.scheduled::after {
    content: ""; position: absolute;
    top: 4px; right: 4px; width: 8px; height: 8px;
    background: #27ae60; border-radius: 50%;
    box-shadow: 0 0 0 2px var(--code-bg);
}
.epg-event.now.scheduled::after { box-shadow: 0 0 0 2px #1565c0; }
.epg-event.lp-active {
    box-shadow: 0 0 0 2px #e74c3c inset;
    transition: box-shadow .1s;
}
.epg-event .t { font-weight: 600; display: block;
                white-space: nowrap; overflow: hidden;
                text-overflow: ellipsis; }
.epg-event .ts { color: var(--muted); font-size: .75em; }
/* Short events (<10 min) — strip padding, shrink font, hide the
   timestamp line so the title gets every pixel it can. */
.epg-event.tight { padding: 2px 3px; font-size: .66em;
                    font-stretch: condensed; }
.epg-event.tight .ts { display: none; }
.epg-event.tight .t { letter-spacing: -.03em; line-height: 1.1;
                       white-space: normal;
                       display: -webkit-box;
                       -webkit-line-clamp: 3;
                       -webkit-box-orient: vertical;
                       text-overflow: clip;
                       word-break: break-word; }
.epg-event.now {
    background: #1565c0; color: #fff; border-color: #0d47a1;
}
.epg-event.now .ts { color: rgba(255,255,255,.85); }
.epg-event.past { opacity: 0.55; }
.epg-time-marker {
    position: absolute; top: 8px;
    font-size: .75em; color: var(--muted);
    border-left: 1px solid var(--border);
    padding-left: 4px; padding-top: 2px;
}

.epg-now-line {
    position: absolute; top: 0; bottom: 0;
    width: 2px; background: #e74c3c;
    z-index: 4; pointer-events: none;
}
.epg-now-line::before {
    content: ""; position: absolute;
    top: -4px; left: -4px;
    width: 10px; height: 10px;
    background: #e74c3c; border-radius: 50%;
}
.epg-controls {
    display: flex; align-items: center; gap: 1em;
    margin: 1em 0 .4em;
}
.btn-now {
    display: inline-block;
    background: var(--code-bg); color: var(--fg);
    padding: .4em .9em; border-radius: 4px;
    font-size: .9em; font-weight: 600;
    text-decoration: none;
    border: 1px solid var(--border);
}
.btn-now:hover { background: var(--stripe); text-decoration: none; }

table { border-collapse: collapse; width: 100%; }
th, td {
    padding: .5em .7em; text-align: left;
    border-bottom: 1px solid var(--border);
}
tr:nth-child(even) td { background: var(--stripe); }
.rank { color: var(--muted); }
"""


def slugify(name):
    s = name.lower()
    s = re.sub(r'[äöüß]', lambda m: {'ä':'ae','ö':'oe','ü':'ue','ß':'ss'}[m.group()], s)
    s = re.sub(r'[^a-z0-9]+', '-', s).strip('-')
    return s


def _migrate_favorites_from_tvh():
    """First-run favorites migration. Used to pull tvh's FAV_TAG_UUID-tagged
    channels into FAVORITES_FILE. Tvh has been decommissioned (2026-05-27);
    on a fresh install with no .favorites.json, ALL tv-receiver channels
    become favorites and the user can curate via the UI."""
    if FAVORITES_FILE.exists():
        return []
    try:
        with urllib.request.urlopen(
            f"{TV_RECEIVER_BASE}/api/channels", timeout=5) as r:
            data = json.load(r)
        slugs = sorted(data.get("slugs") or [])
        FAVORITES_FILE.write_text(json.dumps({"slugs": slugs}, indent=2))
        print(f"seeded {FAVORITES_FILE} with all {len(slugs)} tv-receiver "
              f"channels (= edit via UI to curate)", flush=True)
        return slugs
    except Exception as e:
        print(f"favorites seed failed: {e}", flush=True)
        return []


def _read_favorites():
    """Favourites now live in tv-receiver (source of truth, GET /api/favorites).
    Fetch them from there and mirror to FAVORITES_FILE so the local copy stays
    a fresh fallback. Fall back to that file (then the tvh-migration seed) only
    if tv-receiver is unreachable or returns an empty set — never let a blip
    silently disable favourite-filtering (= channel_map would balloon to all
    channels)."""
    try:
        with urllib.request.urlopen(
                f"{TV_RECEIVER_BASE}/api/favorites", timeout=4) as r:
            slugs = json.load(r).get("slugs", [])
        if slugs:
            try:
                FAVORITES_FILE.write_text(json.dumps({"slugs": slugs}, indent=2))
            except Exception:
                pass
            return slugs
    except Exception as e:
        print(f"favorites: tv-receiver fetch failed ({e}), using {FAVORITES_FILE}",
              flush=True)
    if not FAVORITES_FILE.exists():
        return _migrate_favorites_from_tvh()
    try:
        return json.loads(FAVORITES_FILE.read_text()).get("slugs", [])
    except Exception as e:
        print(f"favorites file read failed: {e}", flush=True)
        return []


def load_favorites():
    """Build channel_map from tv-receiver's /api/channels for channels in
    FAVORITES_FILE. UUIDs are slug-based stable IDs (= hash(slug) hex
    prefix) since tv-receiver doesn't use tvh's UUID scheme; downstream
    code that uses the uuid for tvh stream-URL fallback now hits
    TV_RECEIVER_BASE/stream/channel/<slug> instead (= start_ffmpeg already
    branches on TV_RECEIVER_SLUGS).

    For channels NOT known to tv-receiver, we fall back to fetching from
    tvh — keeps non-FTA / non-m3u channels (= anything not derived from
    FritzBox tvsd.m3u + tvhd.m3u) reachable until tvh is fully removed."""
    fav_slugs = set(_read_favorites())
    new_map = {}

    # Try tv-receiver first.
    try:
        with urllib.request.urlopen(
            f"{TV_RECEIVER_BASE}/api/channels", timeout=5) as r:
            tvr = json.load(r)
        for ch in tvr.get("channels", []):
            slug = ch["slug"]
            if fav_slugs and slug not in fav_slugs:
                continue
            freq = ch.get("freq", 0)
            new_map[slug] = {
                "name":     ch["name"],
                "uuid":     slug,           # not used when slug routes via tv-receiver
                "icon":     "",
                "mux_uuid": f"freq-{freq}",
                "mux_name": f"{freq}MHz",
            }
    except Exception as e:
        print(f"tv-receiver /api/channels fetch failed: {e}", flush=True)

    # Any favorite slug not covered by tv-receiver's channels.json is just
    # logged + skipped — tvh is gone, so there's no fallback to pull
    # metadata from. The user can either add the slug to channels.json
    # or drop it from .favorites.json.
    missing = fav_slugs - set(new_map.keys()) if fav_slugs else set()
    if missing:
        print(f"favorites not in tv-receiver: {sorted(missing)}", flush=True)

    with cmap_lock:
        channel_map.clear()
        channel_map.update(new_map)
    print(f"loaded {len(new_map)} favorite channels "
          f"(tv-receiver: {len(new_map) - len(missing & set(new_map.keys()))}, "
          f"tvh-fallback: {len(missing & set(new_map.keys()))})", flush=True)


def probe_codecs(slug):
    """ffprobe the channel briefly, return {'video': ..., 'audio': ...} or None."""
    info = channel_map.get(slug)
    if not info:
        return None
    # Probe via tv-receiver (= the only stream source post-tvh-removal).
    src_url = f"{TV_RECEIVER_BASE}/stream/channel/{slug}?profile=pass"
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "error",
             "-analyzeduration", "800000", "-probesize", "500000",
             "-show_entries", "stream=codec_type,codec_name",
             "-of", "json", src_url],
            capture_output=True, timeout=15)
        if r.returncode != 0:
            print(f"[{slug}] probe returncode={r.returncode}", flush=True)
            return None
        data = json.loads(r.stdout.decode() or "{}")
        codecs = {"video": None, "audio": None}
        for s in data.get("streams", []):
            t = s.get("codec_type"); n = s.get("codec_name")
            if t == "video" and not codecs["video"]: codecs["video"] = n
            elif t == "audio" and not codecs["audio"]: codecs["audio"] = n
        return codecs
    except Exception as e:
        print(f"[{slug}] probe exception: {e}", flush=True)
        return None


def load_codec_cache():
    if not CODEC_CACHE_FILE.exists():
        return
    try:
        data = json.loads(CODEC_CACHE_FILE.read_text())
        with codec_lock:
            codec_cache.update(data)
        print(f"Loaded {len(data)} cached codecs from disk", flush=True)
    except Exception as e:
        print(f"load codec cache: {e}", flush=True)


def save_codec_cache():
    try:
        with codec_lock:
            data = dict(codec_cache)
        CODEC_CACHE_FILE.write_text(json.dumps(data, indent=1))
    except Exception as e:
        print(f"save codec cache: {e}", flush=True)


def get_codecs(slug):
    with codec_lock:
        if slug in codec_cache:
            return codec_cache[slug]
    c = probe_codecs(slug)
    if not c:
        c = {"video": "unknown", "audio": "unknown"}
    with codec_lock:
        codec_cache[slug] = c
    save_codec_cache()
    print(f"[{slug}] codecs: v={c['video']} a={c['audio']}", flush=True)
    return c


def load_stats():
    if not STATS_FILE.exists():
        return
    try:
        data = json.loads(STATS_FILE.read_text())
        with stats_lock:
            stats.update(data)
        print(f"Loaded usage stats for {len(data)} channels", flush=True)
    except Exception as e:
        print(f"load stats: {e}", flush=True)


def save_stats():
    try:
        with stats_lock:
            data = dict(stats)
        STATS_FILE.write_text(json.dumps(data, indent=1))
    except Exception as e:
        print(f"save stats: {e}", flush=True)


def record_start(slug):
    with stats_lock:
        s = stats.setdefault(slug, {"starts": 0, "watch_seconds": 0,
                                     "last_watched": None})
        s["starts"] += 1
        s["last_watched"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save_stats()


def record_stop(slug, started_at):
    duration = max(0, time.time() - started_at)
    with stats_lock:
        s = stats.setdefault(slug, {"starts": 0, "watch_seconds": 0,
                                     "last_watched": None})
        s["watch_seconds"] += duration
    save_stats()


def prewarm_codecs():
    """Probe all channels that aren't in cache, sequentially, in background."""
    time.sleep(3)   # let startup settle
    with cmap_lock:
        slugs = sorted(channel_map.keys())
    to_probe = [s for s in slugs if s not in codec_cache]
    if not to_probe:
        print("[prewarm] all channels already cached", flush=True)
        return
    print(f"[prewarm] probing {len(to_probe)} channels in background...", flush=True)
    for slug in to_probe:
        get_codecs(slug)
        time.sleep(0.5)   # be gentle on tuners
    print(f"[prewarm] done", flush=True)


class AdoptedProcess:
    """Popen-compatible stand-in for an ffmpeg process that survived
    a container restart (we spawn with start_new_session=True so the
    children get orphaned from PID 1 instead of killed). We re-attach
    by PID on the next hls-gateway startup."""
    def __init__(self, pid):
        self.pid = pid
    def poll(self):
        # os.kill(pid, 0) treats zombies as alive — they're still in
        # the process table. Read /proc/<pid>/status and treat state
        # 'Z' as dead, and try to reap the zombie so the always-warm
        # loop can respawn cleanly.
        try:
            os.kill(self.pid, 0)
        except OSError:
            return 0
        try:
            with open(f"/proc/{self.pid}/status") as f:
                for line in f:
                    if line.startswith("State:"):
                        if line.split()[1] == "Z":
                            try: os.waitpid(self.pid, os.WNOHANG)
                            except (ChildProcessError, OSError): pass
                            return 0
                        break
        except FileNotFoundError:
            return 0
        except Exception:
            pass
        return None
    def terminate(self):
        try: os.kill(self.pid, 15)
        except OSError: pass
    def kill(self):
        try: os.kill(self.pid, 9)
        except OSError: pass
    def wait(self, timeout=None):
        deadline = time.time() + (timeout or 1e9)
        while time.time() < deadline:
            if self.poll() is not None:
                return 0
            time.sleep(0.1)
        raise subprocess.TimeoutExpired(cmd="", timeout=timeout)


def adopt_surviving_ffmpegs():
    """Look for PID files left behind by ffmpegs from a prior container
    run. For each still-alive ffmpeg, register it in `channels` so the
    HLS buffer continues uninterrupted."""
    if not HLS_DIR.exists():
        return
    adopted = 0
    for ch_dir in HLS_DIR.iterdir():
        if not ch_dir.is_dir() or ch_dir.name.startswith((".", "_")):
            continue
        pid_file = ch_dir / ".ffmpeg.pid"
        if not pid_file.exists():
            continue
        try:
            data = json.loads(pid_file.read_text())
            pid = int(data["pid"])
            started_at = float(data.get("started_at", time.time()))
        except Exception:
            try: pid_file.unlink()
            except Exception: pass
            continue
        stub = AdoptedProcess(pid)
        if stub.poll() is not None:
            try: pid_file.unlink()
            except Exception: pass
            continue
        slug = ch_dir.name
        with cmap_lock:
            if slug not in channel_map:
                continue
        with active_lock:
            channels[slug] = {"process": stub,
                              "last_seen": time.time(),
                              "started_at": started_at}
        adopted += 1
        age = int(time.time() - started_at)
        print(f"[{slug}] adopted ffmpeg pid={pid} "
              f"(buffer age {age}s)", flush=True)
    if adopted:
        print(f"adopted {adopted} surviving ffmpeg processes", flush=True)


def start_ffmpeg(slug):
    info = channel_map.get(slug)
    if not info:
        return None
    ch_dir = HLS_DIR / slug
    if ch_dir.exists():
        shutil.rmtree(ch_dir)
    ch_dir.mkdir(parents=True, exist_ok=True)

    codecs = get_codecs(slug)
    # Live-TV via tvh-IPTV: ALWAYS transcode to libx264, even if source is
    # already h264. Reasons (2026-05-23 RTL incident):
    #   1. Source GOP-intervals are not aligned with hls_time → copy-mode
    #      produces mid-GOP segment cuts → iOS decoder shows green frames
    #      until the next IDR.
    #   2. Anamorphic SD (720x576 SAR 64:45 — RTL) needs scale+setsar for
    #      iOS to render correct aspect; copy can't apply filters.
    #   3. Source SPS/PPS arrive in-stream after a few seconds of "corrupt"-
    #      looking packets that confuse ffmpeg's input demuxer.
    # Transcode neutralises all three: libx264 emits fresh SPS/PPS + IDRs at
    # exact segment boundaries via `-force_key_frames`. ~10 % Pi CPU per
    # stream — acceptable since only tvh-IPTV channels hit this path
    # (HD public broadcasters go through MEDIATHEK_LIVE passthrough).
    # DVR-recording HLS-remux (_rec_hls_spawn_local) still copies h264 — that
    # operates on completed files with predictable GOP structure.
    video_opts = [
        "-vf", "scale=trunc(iw*sar/2)*2:trunc(ih/2)*2,setsar=1",
        "-c:v", "libx264",
        "-preset", "ultrafast",
        "-tune", "zerolatency",
        "-profile:v", "main",
        "-pix_fmt", "yuv420p",
        "-g", "50",                  # keyframe every ~2s
        "-force_key_frames", f"expr:gte(t,n_forced*{SEGMENT_TIME})",
    ]
    if slug in ERROR_RESILIENT_TRANSCODE:
        # Override for channels with chronic source-side TS cc-errors
        # (= RTL via FritzBox-SAT>IP). Defaults: B-frames + CABAC + multi-
        # ref give 15-20% better compression BUT each broken ref-frame
        # cascades into ~0.5-2 s of visible decoder corruption. Disabling
        # B-frames, CABAC and multi-ref means each frame can recover
        # within ~1 keyframe interval (= 0.6s with keyint=15 at 25fps).
        video_opts += [
            "-x264-params",
            "keyint=15:scenecut=0:bframes=0:ref=1:b_pyramid=0:weightp=0:cabac=0",
        ]

    if codecs["audio"] in SAFE_AUDIO:
        audio_opts = ["-c:a", "copy"]
    else:
        audio_opts = ["-c:a", "aac", "-b:a", "192k", "-ac", "2"]

    # Post-tvh-removal (2026-05-27): tv-receiver is the only stream source.
    # If the slug isn't in TV_RECEIVER_SLUGS we can't get the stream at all.
    if slug not in TV_RECEIVER_SLUGS:
        print(f"[{slug}] cannot stream: not in tv-receiver's channels.json", flush=True)
        return None
    src_url = f"{TV_RECEIVER_BASE}/stream/channel/{slug}?profile=pass"
    # No `+discardcorrupt`: even in transcode-mode the h264 decoder needs
    # the initial in-stream SPS/PPS packets to sync. `+discardcorrupt` throws
    # them away before the demuxer can sync — decoder starves indefinitely
    # (RTL incident 2026-05-23: 0 frames decoded → libx264 has nothing to
    # encode → 0 segments → watchdog kill loop).
    # analyzeduration/probesize: reverted to 5MB on 2026-05-27 — the
    # 1MB experiment (= ~1.5s cold-start saving) is suspected to cause
    # periodic mid-stream stutters because ffmpeg sees too little of
    # the source GOP pattern → wrong keyframe-insertion decisions →
    # tiny segments. Restored full 5MB until confirmed otherwise.
    input_opts = ["-fflags", "+genpts",
                  "-analyzeduration", "5000000",
                  "-probesize", "5000000"]
    cmd = [
        "ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "warning",
        *input_opts,
        "-i", src_url,
        "-map", "0:v:0", "-map", "0:a:0",
        *video_opts, *audio_opts,
        "-start_at_zero",
        "-avoid_negative_ts", "make_zero",
        "-f", "hls",
        "-hls_time", str(SEGMENT_TIME),
        "-hls_list_size", str(LIST_SIZE),
        "-hls_flags",
        "delete_segments+append_list+independent_segments+program_date_time",
        "-hls_segment_type", "mpegts",
        "-hls_segment_filename", str(ch_dir / "seg_%06d.ts"),
        str(ch_dir / "index.m3u8"),
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL,
                             start_new_session=True)
    mode = f"transcode v={codecs['video']} a={codecs['audio']}"
    # Record pid + start time so we can re-adopt the ffmpeg if the
    # hls-gateway container restarts (segments keep rolling meanwhile).
    try:
        (ch_dir / ".ffmpeg.pid").write_text(json.dumps({
            "pid": proc.pid, "started_at": time.time(),
        }))
    except Exception as e:
        print(f"[{slug}] pid-file write: {e}", flush=True)
    print(f"[{slug}] ffmpeg pid={proc.pid} ({mode})", flush=True)
    return proc


def stop_channel(slug):
    with active_lock:
        info = channels.pop(slug, None)
    if info:
        if info["process"].poll() is None:
            try:
                info["process"].terminate()
                try: info["process"].wait(timeout=5)
                except Exception: info["process"].kill()
            except Exception: pass
        record_stop(slug, info.get("started_at", time.time()))
        print(f"[{slug}] stopped", flush=True)
    # Best-effort cleanup of the pid file so an orphaned ffmpeg from a
    # prior container run isn't re-adopted after we intentionally stopped
    try: (HLS_DIR / slug / ".ffmpeg.pid").unlink()
    except Exception: pass


def ensure_running(slug):
    """Start ffmpeg for this channel if not already running. Keeps
    recently-watched channels warm (idle ffmpeg still filling DVR
    buffer) so switching back gives instant timeshift. If we hit
    MAX_WARM_STREAMS, evict LRU warm channels first to free tuners."""
    # Any viewer request wakes a dormant pin back up.
    with _dormant_pins_lock:
        _dormant_pins.discard(slug)
    victims = []
    with active_lock:
        info = channels.get(slug)
        if info and info["process"].poll() is None:
            info["last_seen"] = time.time()
            return
        if info:
            record_stop(slug, info.get("started_at", time.time()))
            channels.pop(slug, None)
        others = [(s, i["last_seen"]) for s, i in channels.items()
                  if i["process"].poll() is None and s != slug
                  and s not in ALWAYS_WARM]   # never evict permanent
        others.sort(key=lambda x: x[1])
        while len(others) >= MAX_WARM_STREAMS:
            victims.append(others.pop(0)[0])
        proc = start_ffmpeg(slug)
        if proc:
            now = time.time()
            channels[slug] = {"process": proc, "last_seen": now,
                              "started_at": now}
            record_start(slug)
    for v in victims:
        print(f"[{v}] LRU-evicted to free tuner for {slug}", flush=True)
        stop_channel(v)


# _compute_prewarm_neighbors / _post_prewarm / PREWARM_BASE / _spawn_prewarm_update
# / _app_track / _app_idle_loop removed 2026-05-30 (slice 5). The external app's
# live-TV now goes straight to tv-receiver (/api/app/live → Caddy → :9983), which
# owns app-session tracking, adjacency-prewarm and tight per-app eviction. The
# gateway no longer drives tv-receiver's prewarm — the dual-writer is gone.
# (Warm-pool ensure_running/start_ffmpeg/hls_playlist stay: the web UI uses them.)


def idle_killer_loop():
    """Stop non-pinned warm streams after WARM_TTL_SECONDS of idleness.
    Short TTL (3 min default) frees the DVB tuner + CPU + SSD writes
    quickly when the user genuinely walks away from a channel, while
    still riding through brief cross-channel comparisons or quick
    info-screens that bring the user back within the grace period.
    Pinned (ALWAYS_WARM) channels are exempt and run forever."""
    while True:
        time.sleep(30)
        try:
            now = time.time()
            with active_lock:
                stale = [s for s, i in channels.items()
                         if now - i["last_seen"] > WARM_TTL_SECONDS
                         and s not in ALWAYS_WARM]
            for s in stale:
                print(f"[{s}] warm TTL expired ({WARM_TTL_SECONDS}s)",
                      flush=True)
                stop_channel(s)
        except Exception as e:
            print(f"idle loop: {e}", flush=True)


def load_always_warm():
    global MAX_WARM_STREAMS
    if ALWAYS_WARM_FILE.exists():
        try:
            data = json.loads(ALWAYS_WARM_FILE.read_text())
            with always_warm_lock:
                ALWAYS_WARM.clear()
                ALWAYS_WARM.update(data.get("slugs", []))
        except Exception as e:
            print(f"load always-warm: {e}", flush=True)
    if ALWAYS_WARM:
        MAX_WARM_STREAMS = max(MAX_WARM_STREAMS, len(ALWAYS_WARM) + 1)
    print(f"always-warm: {sorted(ALWAYS_WARM) or '(none)'}  "
          f"max_warm={MAX_WARM_STREAMS}", flush=True)


def save_always_warm():
    try:
        with always_warm_lock:
            payload = {"slugs": sorted(ALWAYS_WARM)}
        ALWAYS_WARM_FILE.write_text(json.dumps(payload, indent=2))
    except Exception as e:
        print(f"save always-warm: {e}", flush=True)


def set_always_warm(slug, on):
    """Toggle a channel's pinned-warm state. Returns True if state
    was changed. Pinning spawns ffmpeg; unpinning keeps the stream
    running but re-subjects it to normal LRU eviction (TTL or
    replaced-by-next-viewed-channel)."""
    global MAX_WARM_STREAMS
    changed = False
    with always_warm_lock:
        if on and slug not in ALWAYS_WARM:
            ALWAYS_WARM.add(slug); changed = True
        elif not on and slug in ALWAYS_WARM:
            ALWAYS_WARM.discard(slug); changed = True
    if changed:
        MAX_WARM_STREAMS = max(
            int(os.environ.get("MAX_WARM_STREAMS", "3")),
            len(ALWAYS_WARM) + 1)
        save_always_warm()
        if on:
            ensure_running(slug)
    return changed


PIN_IDLE_HOURS = 6
_dormant_pins = set()
_dormant_pins_lock = threading.Lock()


def always_warm_loop():
    """Background watchdog for pinned channels. Respawns a dead
    ffmpeg for anything in ALWAYS_WARM unless the channel is dormant
    (hit the idle timeout). Kills a warm ffmpeg after PIN_IDLE_HOURS
    without a viewer — saves a tuner + CPU while still auto-restarting
    on the next tap (ensure_running clears the dormant flag)."""
    time.sleep(15)
    idle_cutoff = PIN_IDLE_HOURS * 3600
    while True:
        try:
            now = time.time()
            for slug in list(ALWAYS_WARM):
                with _dormant_pins_lock:
                    dormant = slug in _dormant_pins
                with active_lock:
                    info = channels.get(slug)
                    alive = info and info["process"].poll() is None
                if alive:
                    idle = now - (info.get("last_seen") or now)
                    if idle > idle_cutoff:
                        print(f"[{slug}] pin idle {idle/3600:.1f}h — "
                              f"stopping, will re-arm on next tap",
                              flush=True)
                        stop_channel(slug)
                        with _dormant_pins_lock:
                            _dormant_pins.add(slug)
                elif not dormant:
                    print(f"[{slug}] always-warm respawn", flush=True)
                    ensure_running(slug)
        except Exception as e:
            print(f"always-warm loop: {e}", flush=True)
        time.sleep(30)


def ffmpeg_watchdog_loop():
    """Check every 30 s that each running ffmpeg is still writing
    segments. If the latest seg file hasn't changed in 90 s, the
    ffmpeg is stuck (tuner lost / SAT>IP timeout / decode freeze):
    kill it and let ensure_running respawn it. Preserves the 2 h
    DVR buffer because old segments stay on disk."""
    time.sleep(45)
    STUCK_TIMEOUT = 90.0
    last_seen = {}   # slug -> (latest_seg_mtime, observed_at)
    while True:
        try:
            now = time.time()
            with active_lock:
                running = [(s, i) for s, i in channels.items()
                           if i["process"].poll() is None]
            for slug, info in running:
                ch_dir = HLS_DIR / slug
                try:
                    segs = sorted(ch_dir.glob("seg_*.ts"))
                    mtime = segs[-1].stat().st_mtime if segs else 0
                except Exception:
                    continue
                prev = last_seen.get(slug)
                if prev is None or prev[0] != mtime:
                    last_seen[slug] = (mtime, now)
                    continue
                if now - prev[1] > STUCK_TIMEOUT:
                    # Only kill if ffmpeg has actually had time to settle.
                    if (now - info.get("started_at", now)) < 60:
                        continue
                    print(f"[{slug}] watchdog: no new segment in "
                          f"{int(now-prev[1])}s — killing + respawning",
                          flush=True)
                    stop_channel(slug)
                    last_seen.pop(slug, None)
                    ensure_running(slug)
        except Exception as e:
            print(f"watchdog loop: {e}", flush=True)
        time.sleep(30)


def state_backup_loop():
    """Copy the important JSON state files from /mnt/tv (SSD) to a
    tiny backup dir inside the container image's writeable mount
    every 10 min. Protects against SSD failure: if the disk drops
    out, .always_warm.json, .mediathek_recordings.json etc. still
    survive and can be restored manually."""
    import shutil as _sh
    targets = [
        ".always_warm.json",
        ".mediathek_recordings.json",
        ".usage_stats.json",
        ".live_ads.json",
        ".codec_cache.json",
    ]
    backup_dir = Path("/state-backup")
    if not backup_dir.exists():
        # Mount not configured — silently no-op instead of crashing.
        return
    time.sleep(60)
    while True:
        try:
            for name in targets:
                src = HLS_DIR / name
                if not src.exists():
                    continue
                try:
                    _sh.copy2(src, backup_dir / name)
                except Exception as e:
                    print(f"[state-backup] {name}: {e}", flush=True)
        except Exception as e:
            print(f"state-backup loop: {e}", flush=True)
        time.sleep(600)


def disk_cleanup_loop():
    """When /mnt/tv falls below DISK_MIN_FREE_PCT of free space, delete
    the oldest `_rec_<uuid>/` HLS-VOD remux directories (LRU by mtime)
    until there's again at least DISK_TARGET_FREE_PCT free. The original
    .ts recordings in /recordings stay — only the remuxed copies go, and
    they're lazy-rebuilt on next playback.

    Percentage-based so the trigger scales with disk size (previously
    hardcoded 8 GB free, which on the 916 GB NVMe = 99.1% used before
    cleanup kicked in — well past the pihole 96%-used alert)."""
    import shutil as _sh
    DISK_MIN_FREE_PCT    = 8.0   # cleanup triggers when free < 8% (= ~92% used)
    DISK_TARGET_FREE_PCT = 15.0  # cleanup until free >= 15%   (= ~85% used)
    time.sleep(90)
    while True:
        try:
            usage = _sh.disk_usage(HLS_DIR)
            free_pct = usage.free / usage.total * 100
            if free_pct < DISK_MIN_FREE_PCT:
                rec_dirs = []
                for p in HLS_DIR.glob("_rec_*"):
                    if p.is_dir():
                        try: rec_dirs.append((p.stat().st_mtime, p))
                        except Exception: pass
                rec_dirs.sort()   # oldest first
                for mtime, p in rec_dirs:
                    usage = _sh.disk_usage(HLS_DIR)
                    cur_free_pct = usage.free / usage.total * 100
                    if cur_free_pct >= DISK_TARGET_FREE_PCT:
                        break
                    try:
                        size_mb = sum(f.stat().st_size
                                      for f in p.rglob("*")
                                      if f.is_file()) / (1024 ** 2)
                        _sh.rmtree(p, ignore_errors=True)
                        print(f"[disk-cleanup] removed {p.name} "
                              f"({size_mb:.0f} MB, free now "
                              f"{usage.free/(1024**3):.1f}GB / "
                              f"{cur_free_pct:.1f}%)", flush=True)
                    except Exception as e:
                        print(f"[disk-cleanup] rm {p}: {e}", flush=True)
        except Exception as e:
            print(f"disk-cleanup loop: {e}", flush=True)
        time.sleep(300)


def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/")
def index():
    now = time.time()
    # Snapshot stats + live sessions (contribute to sort)
    with stats_lock:
        st_snap = {s: dict(v) for s, v in stats.items()}
    with active_lock:
        for s, i in channels.items():
            st_snap.setdefault(s, {"starts": 0, "watch_seconds": 0})
            st_snap[s]["watch_seconds"] = st_snap[s].get("watch_seconds", 0) \
                                           + (now - i.get("started_at", now))

    with cmap_lock:
        items = list(channel_map.items())
    # Sort: pinned first, then watch_seconds DESC, starts DESC, name ASC
    items.sort(key=lambda kv: (
        0 if kv[0] in ALWAYS_WARM else 1,
        -st_snap.get(kv[0], {}).get("watch_seconds", 0),
        -st_snap.get(kv[0], {}).get("starts", 0),
        kv[1]["name"].lower(),
    ))

    # Assign a stable colour per mux transponder so viewers can tell
    # at a glance which channels share a tuner. Only show the dot if
    # the mux has 2+ favourites on it — a solo channel's mux info is
    # noise.
    mux_members = {}
    for s, info in items:
        mu = info.get("mux_uuid") or ""
        if mu:
            mux_members.setdefault(mu, []).append(s)
    # Six hues chosen so every pair stays distinguishable at an 8 px
    # dot: black, green, blue, pink, yellow, brown. No orange (reads
    # as red), no cyan/purple (collapse into green/blue at that size).
    MUX_PALETTE = [
        "#1976d2",   # blue
        "#27ae60",   # green
        "#ff9800",   # orange
        "#8e44ad",   # purple
        "#f1c40f",   # yellow
        "#00bcd4",   # cyan
        "#c0392b",   # red
        "#8bc34a",   # lime
        "#2c3e50",   # slate
        "#795548",   # brown
        "#009688",   # teal
        "#673ab7",   # deep-purple
    ]
    mux_colour = {}
    for i, mu in enumerate(sorted(mux_members.keys())):
        mux_colour[mu] = MUX_PALETTE[i % len(MUX_PALETTE)]

    rows = []
    for s, info in items:
        watch_url = f"{HOST_URL}/watch/{s}"
        dvr_url   = f"{HOST_URL}/hls/{s}/dvr.m3u8"
        icon = _channel_logo_url(s, info.get("icon", ""))
        logo = (f'<img src="{icon}" alt="" loading="lazy">'
                if icon else "")
        st = st_snap.get(s, {})
        starts = st.get("starts", 0)
        hours = st.get("watch_seconds", 0) / 3600
        usage = (f'<span class="usage">{starts}× · {hours:.1f} h</span>'
                 if starts > 0 else
                 '<span class="usage muted">neu</span>')
        mu = info.get("mux_uuid", "")
        mux_dot = ""
        if mu in mux_colour:
            mates = [channel_map[m]["name"] for m in mux_members[mu]
                     if m != s]
            title = (f'{info.get("mux_name","Mux")} — teilt Tuner mit '
                     f'{", ".join(mates)}')
            mux_dot = (f'<span class="mux-dot" '
                       f'style="background:{mux_colour[mu]}" '
                       f'title="{title}"></span>')
        mux_style = (f' style="--mux-color:{mux_colour[mu]}"'
                     if mu in mux_colour else "")
        mux_attr = f' data-mux="{mu}"' if mu else ""
        mt_attr  = ' data-mediathek="1"' if s in MEDIATHEK_LIVE else ""
        rows.append(
            f'<li data-slug="{s}"{mux_attr}{mt_attr} title="{info["name"]}"{mux_style}>'
            f'<button class="pin-btn" data-slug="{s}" title="Dauer-warm">📌</button>'
            f'<a class="logo" href="{watch_url}">{logo}</a>'
            f'<span class="buffer-bar" data-slug="{s}"></span>'
            f'<span class="now-title" data-slug="{s}"></span>'
            f'</li>')
    tools = [
        ("Alle Kanäle als M3U (für IPTV-Apps)", f"{HOST_URL}/playlist.m3u"),
        ("Whisper-Suche (Volltext über alle Aufnahmen)",
                                                f"{HOST_URL}/search"),
        ("Nutzungsstatistik / Top-Sender",      f"{HOST_URL}/stats"),
        ("Status (gerade aktive Streams)",      f"{HOST_URL}/status"),
        ("System-Health (Pi + Container)",      f"{HOST_URL}/health"),
        ("Kanal-Liste neu laden",              f"{HOST_URL}/reload"),
    ]
    tool_rows = "".join(
        f'<li><a href="{url}">{label}</a><br>'
        f'<code style="font-size:0.8em;color:#888">{url}</code></li>'
        for label, url in tools)
    extra_css = (
        ".buffer-bar{position:absolute;left:4px;right:4px;bottom:3px;"
        "height:4px;border-radius:3px;background:transparent;z-index:3}"
        "ul.channels li.scanning::before{content:'🔍';position:absolute;"
        "top:4px;left:6px;font-size:.8em;z-index:2;"
        "animation:scanpulse 1.2s ease-in-out infinite}"
        "@keyframes scanpulse{0%,100%{opacity:.4}50%{opacity:1}}"
        ".buffer-bar.running{background:#00000022;cursor:pointer;"
        "box-shadow:inset 0 0 0 1px #0001}"
        "@media (prefers-color-scheme:dark){"
        ".buffer-bar.running{background:#ffffff33;box-shadow:none}"
        "}"
        ".buffer-bar::before{content:'';display:block;height:100%;"
        "width:var(--pct,0);background:#27ae60;border-radius:3px;"
        "transition:width .4s}"
        # Invisible tap-extender: bottom band of the tile is clickable.
        ".buffer-bar.running::after{content:'';position:absolute;"
        "left:-4px;right:-4px;top:-14px;bottom:-6px}"
        ".pin-btn{position:absolute;top:-9px;right:-8px;"
        "background:none;border:0;cursor:pointer;font-size:1.1em;"
        "opacity:.35;padding:3px;line-height:1;transform:rotate(35deg);"
        "transform-origin:center;"
        "transition:opacity .15s,transform .15s,filter .15s;z-index:2;"
        # Unpinned: grayscale + drop-shadow desaturates the emoji so it
        # reads as "off". opacity alone wasn't enough on iOS — the
        # native red push-pin emoji rendered as full colour even at
        # opacity .2, indistinguishable from the active state.
        "filter:grayscale(1) drop-shadow(0 1px 2px #0008)}"
        # Hover gated on a device with real hover capability —
        # iOS Safari latches :hover after a tap and never clears it
        # without a navigation, leaving the just-tapped pin stuck in
        # the half-grayscale "kind of pinned" middle state until a
        # page reload.
        "@media (hover:hover){"
        ".pin-btn:hover{opacity:.75;transform:rotate(45deg) scale(1.1);"
        "filter:grayscale(.5) drop-shadow(0 1px 2px #0008)}"
        "}"
        # Note: 'filter: none drop-shadow(...)' is invalid CSS — 'none'
        # cancels all filters, can't compose with other functions.
        # Just drop-shadow alone: no grayscale → emoji renders in full
        # red; the implicit reset of grayscale relative to the parent
        # .pin-btn rule is what we want.
        ".pin-btn.active{opacity:1;transform:rotate(45deg);"
        "filter:drop-shadow(0 2px 3px #0009)}"
        "ul.channels li:has(.pin-btn.active){"
        "box-shadow:0 3px 10px #0003;transform:translateY(-1px)}"
        ".pin-btn.dormant{opacity:.6}"
        ".tuner-badge{display:inline-block;font-size:.72em;font-weight:600;"
        "padding:3px 10px;border-radius:12px;"
        "background:#34495e;color:#fff;letter-spacing:.03em}"
        ".tuner-badge:empty{display:none}"
        ".tuner-badge.tight{background:#e67e22}"
        ".tuner-badge.full{background:#c0392b}"
        ".host-badges{display:flex;flex-wrap:wrap;gap:6px;margin:-4px 0 12px}"
        ".channels-head{display:flex;align-items:center;gap:10px}"
        ".tile-size-btn{background:transparent;border:1px solid var(--border);"
        "border-radius:6px;padding:2px 9px;cursor:pointer;font-size:.9em;"
        "color:var(--muted);line-height:1}"
        ".tile-size-btn:hover{background:var(--stripe)}"
        ".quick-links{display:flex;gap:10px;margin:16px 0 24px}"
        ".quick-links a{flex:1;background:var(--stripe);border:1px solid var(--border);"
        "border-radius:10px;padding:14px 16px;display:flex;flex-direction:column;"
        "align-items:flex-start;gap:2px;text-decoration:none;color:var(--fg);"
        "font-weight:600;font-size:1em;transition:background .15s}"
        ".quick-links a:hover{background:var(--code-bg);text-decoration:none}"
        ".quick-links span{font-size:1.6em;line-height:1}"
        ".quick-links small{font-weight:400;color:var(--muted);font-size:.82em}"
        "h2.tools-head{margin-top:2em;font-size:1em;color:var(--muted);"
        "font-weight:500;border-top:1px solid var(--border);padding-top:1em}"
        "ul.tools li{font-size:.9em;opacity:.75}"
        ".mux-dot{display:inline-block;width:8px;height:8px;"
        "border-radius:50%;margin-right:6px;vertical-align:1px;"
        "cursor:help}"
        # Subtle blue accent line at the bottom-inside edge of the tile,
        # marks channels that have a public-broadcaster Mediathek-Live
        # fallback stream (MEDIATHEK_LIVE). Sits below the buffer-bar
        # (which is bottom:3px height:4px) without overlap.
        "ul.channels li[data-mediathek]{"
        "box-shadow:inset 0 -3px 0 #2980b9}"
    )
    js = (
        "let _warmFailures=0;"
        "function setRecoveryBanner(on){"
        "  const b=document.getElementById('recovery-banner');"
        "  if(b)b.style.display=on?'block':'none';"
        "}"
        "async function refreshWarm(){"
        "  try{"
        "    const r=await fetch('/api/warm-status');"
        "    if(!r.ok)throw new Error('http '+r.status);"
        "    const d=await r.json();"
        "    _warmFailures=0;"
        "    setRecoveryBanner(false);"
        "    const W=d.window_seconds||7200;"
        "    const scanSlug=d.adskip_slug||null;"
        "    for(const li of document.querySelectorAll('ul.channels li')){"
        "      li.classList.toggle('scanning',li.dataset.slug===scanSlug);"
        "    }"
        "    for(const nt of document.querySelectorAll('.now-title')){"
        "      const s=nt.dataset.slug;"
        "      const e=d.channels[s];"
        "      nt.textContent=e&&e.now?e.now:'';"
        "    }"
        "    for(const bar of document.querySelectorAll('.buffer-bar')){"
        "      const s=bar.dataset.slug;"
        "      const e=d.channels[s];"
        "      if(!e||!e.running){"
        "        bar.style.setProperty('--pct','0');bar.title='';"
        "        bar.classList.remove('running','pinned');"
        "      } else {"
        "        const bs=e.buffer_seconds;"
        "        const full=bs>=W-30;"
        "        let time;"
        "        if(full)time='2h';"
        "        else if(bs>=3600)time=Math.floor(bs/3600)+'h'+Math.floor((bs%3600)/60)+'m';"
        "        else if(bs>=60)time=Math.floor(bs/60)+'m';"
        "        else time=bs+'s';"
        "        bar.classList.add('running');"
        "        bar.classList.toggle('pinned',!!e.always_warm);"
        "        bar.title=e.always_warm?'Dauer-warm · Puffer '+time+(full?' (voll)':''):"
        "          'Warm-Tuner · Puffer '+time+(full?' (voll)':'')+' · Klick: Tuner freigeben';"
        "        bar.style.setProperty('--pct',Math.min(100,bs*100/W)+'%');"
        "      }"
        "    }"
        "    const budget=(d.pin_budget||0);"
        "    for(const b of document.querySelectorAll('.pin-btn')){"
        "      const s=b.dataset.slug;"
        "      const e=d.channels[s];"
        "      const pinned=e&&e.always_warm;"
        "      const dormant=pinned&&e&&e.dormant;"
        "      b.classList.toggle('active',!!pinned);"
        "      b.classList.toggle('dormant',!!dormant);"
        "      b.textContent=dormant?'💤':'📌';"
        "      if(!pinned&&budget<=0){"
        "        b.style.display='none';"
        "      } else {"
        "        b.style.display='';"
        "      }"
        "      b.title=dormant?'Pin ruht (6h ohne Zugriff) — Tap weckt':"
        "              pinned?'Dauer-warm aktiv — klick zum Deaktivieren':"
        "                     'Kanal dauer-warm halten (max '+(d.pin_limit||0)+' bei '+(d.pin_dvr_reserve||0)+' DVR-Jobs)';"
        "    }"
        "    const tb=document.getElementById('tuner-badge');"
        "    if(tb){"
        "      const u=d.tuners_used,t=d.tuners_total||4,epg=d.tuners_epggrab||0;"
        "      if(u===null||u===undefined){tb.textContent='';tb.className='tuner-badge';}"
        "      else{"
        "        const epgSuffix=epg>0?' ('+epg+'× EPG)':'';"
        "        tb.textContent='📡 '+u+'/'+t+' Tuner'+epgSuffix;"
        "        tb.className='tuner-badge'+(u>=t?' full':u>=t-1?' tight':'');"
        "        tb.title='DVB-C Tuner in Nutzung (FRITZ!Box SAT>IP). '+"
        "          (epg>0?epg+' davon für EPG-OTA-Grab (läuft nach tvheadend-Restart auto durch). ':'')+"
        "          'Kanäle auf demselben Mux teilen sich einen Tuner.';"
        "      }"
        "    }"
        "    const fb=document.getElementById('ffmpeg-badge');"
        "    if(fb){"
        "      const n=d.ffmpeg_count;"
        "      if(n===null||n===undefined){fb.textContent='';fb.className='tuner-badge';}"
        "      else{"
        "        fb.textContent='⚙ '+n+' ffmpeg';"
        "        fb.className='tuner-badge'+(n>=8?' full':n>=5?' tight':'');"
        "        fb.title='Laufende ffmpeg-Prozesse: Live-Streams, Recording-Remuxe, Thumbnails, Ad-Detection.';"
        "      }"
        "    }"
        "    const lb=document.getElementById('load-badge');"
        "    if(lb){"
        "      if(d.load1===null||d.load1===undefined){lb.textContent='';lb.className='tuner-badge';}"
        "      else{"
        "        const c=d.cpu_count||4;"
        "        const ratio=d.load1/c;"
        "        lb.textContent='💻 '+d.load1.toFixed(2)+'/'+c;"
        "        lb.className='tuner-badge'+(ratio>=1.5?' full':ratio>=1.0?' tight':'');"
        "        lb.title='Load-Average über 1 min geteilt durch Kerne.';"
        "      }"
        "    }"
        "    const mb=document.getElementById('mem-badge');"
        "    if(mb&&d.mem_total_mb){"
        "      const used=d.mem_total_mb-d.mem_avail_mb;"
        "      const pct=used/d.mem_total_mb;"
        "      mb.textContent='🧠 '+(used/1024).toFixed(1)+'/'+(d.mem_total_mb/1024).toFixed(1)+'G';"
        "      mb.className='tuner-badge'+(pct>=0.9?' full':pct>=0.75?' tight':'');"
        "      mb.title='RAM genutzt / gesamt (Pi 5 mit '+(d.mem_total_mb/1024).toFixed(0)+' GB).';"
        "    }"
        "    const tb2=document.getElementById('temp-badge');"
        "    if(tb2&&d.cpu_temp_c){"
        "      tb2.textContent='🌡 '+d.cpu_temp_c+'°';"
        "      tb2.className='tuner-badge'+(d.cpu_temp_c>=75?' full':d.cpu_temp_c>=65?' tight':'');"
        "      tb2.title='CPU-Temperatur. Throttling ab ~80 °C.';"
        "    }"
        "    const db=document.getElementById('disk-badge');"
        "    if(db&&d.disk_free_gb!=null){"
        "      const pct=1-d.disk_free_gb/d.disk_total_gb;"
        "      db.textContent='💾 '+d.disk_free_gb+'G frei';"
        "      db.className='tuner-badge'+(d.disk_free_gb<10?' full':d.disk_free_gb<25?' tight':'');"
        "      db.title='/mnt/tv freier Platz auf der SSD ('+d.disk_total_gb+' GB gesamt).';"
        "    }"
        "  }catch(e){"
        "    _warmFailures++;"
        "    if(_warmFailures>=1)setRecoveryBanner(true);"
        "  }"
        "}"
        "function reorderPinned(){"
        "  const ul=document.querySelector('ul.channels');if(!ul)return;"
        "  const items=Array.from(ul.children);"
        "  const pinned=items.filter(li=>li.querySelector('.pin-btn.active'));"
        "  const rest=items.filter(li=>!li.querySelector('.pin-btn.active'));"
        "  for(const li of pinned)ul.appendChild(li);"
        "  for(const li of rest)ul.appendChild(li);"
        "}"
        "function showPinToast(slug, on, name){"
        "  let t=document.getElementById('pin-toast');"
        "  if(!t){t=document.createElement('div');t.id='pin-toast';"
        "    t.style.cssText='position:fixed;top:14px;left:50%;"
        "transform:translateX(-50%);background:#222;color:#fff;"
        "padding:10px 18px;border-radius:24px;font-weight:600;"
        "box-shadow:0 4px 14px #0006;z-index:1000;font-size:.95em;"
        "transition:opacity .3s';document.body.appendChild(t);}"
        "  t.textContent=(on?'📌 ':'📍 ')+name+(on?' angepinnt':' losgelöst');"
        "  t.style.opacity='1';"
        "  clearTimeout(window._pinToastT);"
        "  window._pinToastT=setTimeout(()=>{t.style.opacity='0';},2200);"
        "}"
        "async function togglePin(btn){"
        "  const slug=btn.dataset.slug;"
        "  const on=!btn.classList.contains('active');"
        "  /* Toast immediately — disambiguates which channel the click"
        "     actually hit (the visual list is JS-reordered, easy to"
        "     mis-tap when tiles look similar). */"
        "  const li=btn.closest('li');"
        "  const name=li?(li.title||slug):slug;"
        "  showPinToast(slug, on, name);"
        "  btn.classList.toggle('active',on);"
        "  try{"
        "    const r=await fetch('/api/always-warm/'+slug,{method:'POST',"
        "      headers:{'content-type':'application/json'},"
        "      body:JSON.stringify({on:on})});"
        "    /* Reload to get a fresh server-side sort. JS-side sort"
        "       can't get the unpinned channel back to its natural"
        "       usage position on its own — the captured origOrder"
        "       still has it near the front from when it was pinned"
        "       at page-render time. Reload is ~200-500 ms and"
        "       guarantees the visual matches the server.  Gated on"
        "       r.ok so a 502 during ssd-recovery doesn't put us in"
        "       a reload loop — fetch only throws on network errors,"
        "       not on HTTP 5xx, so we'd otherwise reload forever. */"
        "    if(r.ok)setTimeout(()=>location.reload(),350);"
        "  }catch(e){}"
        "}"
        "document.addEventListener('click',e=>{"
        "  const b=e.target.closest('.pin-btn');"
        "  if(b){e.preventDefault();togglePin(b);return;}"
        "  const w=e.target.closest('.buffer-bar.running');"
        "  if(w&&!w.classList.contains('pinned')){"
        "    e.preventDefault();e.stopPropagation();"
        "    const slug=w.dataset.slug;"
        "    if(!slug)return;"
        "    fetch('/stop/'+slug).then(()=>refreshWarm()).catch(()=>{});"
        "  }"
        "});"
        # Tile-size toggle: cycles through sm/md/lg, persisted.
        "(function(){"
        "  const sizes=['md','sm','lg'];"
        "  const icons={sm:'⊡',md:'⊟',lg:'⊞'};"
        "  let cur=localStorage.getItem('tile-size')||'md';"
        "  const apply=()=>{"
        "    document.body.classList.remove('tile-sm','tile-lg');"
        "    if(cur!=='md')document.body.classList.add('tile-'+cur);"
        "    const btn=document.getElementById('tile-size');"
        "    if(btn)btn.textContent=icons[cur]||'⊟';"
        "  };"
        "  apply();"
        "  document.getElementById('tile-size').addEventListener('click',()=>{"
        "    cur=sizes[(sizes.indexOf(cur)+1)%sizes.length];"
        "    localStorage.setItem('tile-size',cur);apply();"
        "  });"
        "})();"
        # Sort toggle: 'usage' (server default) or 'mux' (group by mux).
        # Exposes window.applyChannelSort so togglePin can re-sort after
        # an unpin (otherwise the un-pinned channel just slides one
        # position down rather than returning to its usage-sorted spot).
        "(function(){"
        "  const ul=document.querySelector('ul.channels');"
        "  const btn=document.getElementById('sort-toggle');"
        "  if(!ul||!btn)return;"
        "  const origOrder=Array.from(ul.children).map(li=>li.dataset.slug);"
        "  let mode=localStorage.getItem('sort-mode')||'usage';"
        "  const icons={usage:'📊',mux:'📡'};"
        "  const labels={usage:'Nutzung',mux:'Nach Mux'};"
        "  const apply=()=>{"
        "    btn.textContent=icons[mode];"
        "    btn.title='Sortierung: '+labels[mode]+' (Klick zum Umschalten)';"
        "    const items=Array.from(ul.children);"
        "    if(mode==='mux'){"
        "      items.sort((a,b)=>{"
        "        const ma=a.dataset.mux||'zzz';const mb=b.dataset.mux||'zzz';"
        "        if(ma!==mb)return ma<mb?-1:1;"
        "        return origOrder.indexOf(a.dataset.slug)-origOrder.indexOf(b.dataset.slug);"
        "      });"
        "    } else {"
        "      items.sort((a,b)=>"
        "        origOrder.indexOf(a.dataset.slug)-origOrder.indexOf(b.dataset.slug));"
        "    }"
        "    for(const li of items)ul.appendChild(li);"
        "    reorderPinned();"
        "  };"
        "  window.applyChannelSort=apply;"
        "  apply();"
        "  btn.addEventListener('click',()=>{"
        "    mode=mode==='usage'?'mux':'usage';"
        "    localStorage.setItem('sort-mode',mode);apply();"
        "  });"
        "})();"
        "refreshWarm();setInterval(refreshWarm,5000);"
    )
    body = (f"<html><head><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'>"
            f"<meta name='color-scheme' content='light dark'>"
            f"<style>{BASE_CSS}{extra_css}</style></head>"
            f"<body>"
            f"<div id='recovery-banner' style='display:none;"
            f"background:#f39c12;color:#000;padding:10px 14px;border-radius:8px;"
            f"margin:0 0 12px;font-weight:600;text-align:center'>"
            f"⚠ Speicher-Recovery läuft — Live-TV in ~15 s wieder verfügbar"
            f"</div>"
            f"<h1>HLS Gateway</h1>"
            f"<div class='host-badges'>"
            f"<span id='tuner-badge' class='tuner-badge'></span>"
            f"<span id='ffmpeg-badge' class='tuner-badge'></span>"
            f"<span id='load-badge' class='tuner-badge'></span>"
            f"<span id='mem-badge' class='tuner-badge'></span>"
            f"<span id='temp-badge' class='tuner-badge'></span>"
            f"<span id='disk-badge' class='tuner-badge'></span>"
            f"</div>"
            f"<div class='quick-links'>"
            f"<a href='{HOST_URL}/epg'><span>📅</span>Programm<small>EPG-Guide + Chapter-Ticks</small></a>"
            f"<a href='{HOST_URL}/bibliothek'><span>🎬</span>Bibliothek<small>Filme + Serien zum Anschauen</small></a>"
            f"<a href='{HOST_URL}/recordings'><span>📼</span>Aufnahmen<small>DVR + Mediathek-Recordings</small></a>"
            f"<a href='{HOST_URL}/learning'><span>🧠</span>Lernfortschritt<small>Modell-Historie + Per-Channel-Tuning</small></a>"
            f"</div>"
            f"<h2 class='channels-head'>Kanäle"
            f" <button id='tile-size' class='tile-size-btn' "
            f"title='Kachelgröße umschalten'>⊟</button>"
            f" <button id='sort-toggle' class='tile-size-btn' "
            f"title='Sortierung umschalten'>📊</button></h2>"
            f"<ul class='channels'>{''.join(rows)}</ul>"
            f"<h2 class='tools-head'>Tools</h2>"
            f"<ul class='tools'>{tool_rows}</ul>"
            f"<script>{js}</script>"
            f"</body></html>")
    return body


@app.route("/playlist.m3u")
def playlist_m3u():
    lines = ["#EXTM3U"]
    with cmap_lock:
        for slug, info in sorted(channel_map.items(),
                                  key=lambda kv: kv[1]["name"].lower()):
            attrs = [f'tvg-id="{slug}"', f'tvg-name="{info["name"]}"']
            logo_url = _channel_logo_url(slug, info.get("icon", ""))
            if logo_url:
                attrs.append(f'tvg-logo="{logo_url}"')
            lines.append(f'#EXTINF:-1 {" ".join(attrs)},{info["name"]}')
            lines.append(f"{HOST_URL}/hls/{slug}/index.m3u8")
    return _cors(Response("\n".join(lines), mimetype="audio/x-mpegurl"))


def _client_min_segments(ua):
    """Adaptive cold-start gate. Different HLS clients have very
    different prebuffer behaviors at the first playlist GET:

    - iOS AVFoundation (AppleCoreMedia, iPhone, iPad, AppleTV):
      stalls or shows a still frame if the initial playlist lists too
      few segments. Empirically 6 segments / 6s works (verified
      2026-05-27 on iPhone Safari). Apple spec says ≥3 is the floor.
    - mpv / libavformat (Lavf/N.N.N UA — covers our Kuckuck app, vlc,
      desktop mpv, ffplay): tolerates 1 segment cleanly because it has
      its own read-ahead demuxer cache and refetches the playlist on a
      tick. Returning early saves ~1s of server-side wait.
    - Everything else (hls.js, browser MSE, unknown): conservative 2.
      hls.js typically wants ≥3 but tolerates 2 with appended segments
      arriving via reload.

    Logged at INFO so we can verify UA-detection in the field."""
    ua_low = (ua or "").lower()
    if any(t in ua_low for t in ("applecoremedia", "iphone", "ipad", "appletv")):
        return 6, "ios"
    if "lavf" in ua_low or "libmpv" in ua_low or "mpv/" in ua_low:
        return 1, "mpv"
    return 2, "default"


@app.route("/hls/<slug>/index.m3u8")
def hls_playlist(slug):
    with cmap_lock:
        if slug not in channel_map:
            abort(404, "unknown channel")
    ensure_running(slug)
    ch_dir = HLS_DIR / slug
    playlist_path = ch_dir / "index.m3u8"
    ua = request.headers.get("User-Agent", "")
    min_segments, _ = _client_min_segments(ua)
    deadline = time.time() + 30
    while time.time() < deadline:
        if playlist_path.exists() and playlist_path.stat().st_size > 100:
            segs = sorted(ch_dir.glob("seg_*.ts"))
            if len(segs) >= min_segments:
                break
        time.sleep(0.15)
    if not playlist_path.exists():
        abort(503, "stream not ready yet")
    # Send the full playlist with an EXT-X-START hint so iOS jumps close
    # to live. DVR range intact for user seek-back.
    content = playlist_path.read_text()
    if "#EXT-X-START" not in content:
        content = content.replace(
            "#EXTM3U",
            "#EXTM3U\n#EXT-X-START:TIME-OFFSET=-6",
            1)
    resp = Response(content, mimetype="application/vnd.apple.mpegurl")
    resp.headers["Cache-Control"] = "no-cache"
    return _cors(resp)


@app.route("/hls/<slug>/dvr.m3u8")
def hls_playlist_dvr(slug):
    """Full 2h DVR playlist (untrimmed), for timeshift playback.

    Inserts `#EXT-X-PLAYLIST-TYPE:EVENT` right after the version tag.
    Without it, AVPlayer/mpv/ffmpeg default to sliding-window semantics
    and refuse to scrub back beyond the player buffer, even though all
    segments are listed. EVENT marks the playlist as append-only — the
    player allows scrubbing from playlist start to live edge. NOT VOD:
    that would tell the player the stream has ended and stop live
    tracking."""
    with cmap_lock:
        if slug not in channel_map:
            abort(404, "unknown channel")
    ensure_running(slug)
    ch_dir = HLS_DIR / slug
    playlist_path = ch_dir / "index.m3u8"
    ua = request.headers.get("User-Agent", "")
    min_segments, _ = _client_min_segments(ua)
    deadline = time.time() + 25
    while time.time() < deadline:
        if playlist_path.exists() and playlist_path.stat().st_size > 100:
            segs = sorted(ch_dir.glob("seg_*.ts"))
            if len(segs) >= min_segments:
                break
        time.sleep(0.15)
    if not playlist_path.exists():
        abort(503, "stream not ready yet")
    content = playlist_path.read_text()
    if "#EXT-X-PLAYLIST-TYPE" not in content:
        # Insert right after the VERSION tag so the playlist-type is
        # known before any media tags are parsed.
        content = re.sub(r"(#EXT-X-VERSION:[0-9]+\s*\n)",
                         r"\1#EXT-X-PLAYLIST-TYPE:EVENT\n",
                         content, count=1)
    resp = Response(content, mimetype="application/vnd.apple.mpegurl")
    resp.headers["Cache-Control"] = "no-cache"
    return _cors(resp)


# app_live_playlist / app_live_dvr / app_live_segment (/api/app/live/<slug>/*)
# removed 2026-05-30 (slice 5). tv-receiver serves these directly now (Caddy
# routes /api/app/live/* + /mediathek-passthru/* → :9983). The app-URL strings
# the gateway still emits (api_app_endpoints) keep pointing at /api/app/live/...,
# which Caddy resolves to tv-receiver. Web UI keeps /hls/<slug>/* below.


@app.route("/hls/<slug>/<filename>")
def hls_segment(slug, filename):
    with cmap_lock:
        if slug not in channel_map:
            abort(404)
    with active_lock:
        if slug in channels:
            channels[slug]["last_seen"] = time.time()
    mime = "video/mp2t" if filename.endswith(".ts") \
           else "application/vnd.apple.mpegurl" if filename.endswith(".m3u8") \
           else "application/octet-stream"
    return _cors(send_from_directory(HLS_DIR / slug, filename, mimetype=mime))


_epg_cache = {"data": None, "expires": 0}
_epg_lock = threading.Lock()

# EPG archive: {(channel_slug, start): {...event...}}
_epg_archive = {}
_epg_archive_lock = threading.Lock()


def load_epg_archive():
    """Load archive, drop expired entries, and compact the file on disk."""
    if not EPG_ARCHIVE_FILE.exists():
        return
    cutoff = time.time() - EPG_ARCHIVE_KEEP_DAYS * 86400
    raw_count = 0
    try:
        with open(EPG_ARCHIVE_FILE) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                raw_count += 1
                try:
                    e = json.loads(line)
                except Exception:
                    continue
                if e.get("stop", 0) < cutoff:
                    continue
                key = (e["slug"], e["start"])
                with _epg_archive_lock:
                    # keep newer version on dup
                    _epg_archive[key] = e
        kept = len(_epg_archive)
        print(f"EPG archive: loaded {kept} (dropped {raw_count - kept} "
              f"expired/duplicate)", flush=True)
        # compact file if we dropped a lot
        if raw_count > kept * 1.3 or raw_count - kept > 500:
            tmp = EPG_ARCHIVE_FILE.with_suffix(".tmp")
            with open(tmp, "w") as f:
                with _epg_archive_lock:
                    for rec in _epg_archive.values():
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            tmp.replace(EPG_ARCHIVE_FILE)
            print(f"EPG archive: compacted to {kept} entries", flush=True)
    except Exception as e:
        print(f"load EPG archive: {e}", flush=True)


def append_epg_archive(events_by_slug):
    """Write newly-seen events to archive file. Skips duplicates."""
    new_lines = []
    with _epg_archive_lock:
        for slug, events in events_by_slug.items():
            for e in events:
                key = (slug, e["start"])
                if key in _epg_archive:
                    continue
                rec = {"slug": slug, "start": e["start"], "stop": e["stop"],
                       "title": e.get("title", ""),
                       "subtitle": e.get("subtitle", ""),
                       "event_id": e.get("event_id")}
                _epg_archive[key] = rec
                new_lines.append(json.dumps(rec, ensure_ascii=False))
    if new_lines:
        try:
            with open(EPG_ARCHIVE_FILE, "a") as f:
                f.write("\n".join(new_lines) + "\n")
        except Exception as e:
            print(f"append archive: {e}", flush=True)


def epg_snapshot_loop():
    """Every N seconds, fetch live EPG and append new events to archive."""
    time.sleep(15)  # let startup settle
    while True:
        try:
            data = fetch_epg(window_before=60, window_after=12 * 3600,
                              force=True)
            append_epg_archive(data["events"])
        except Exception as e:
            print(f"snapshot loop: {e}", flush=True)
        time.sleep(EPG_SNAPSHOT_INTERVAL)

# ---- EPG: now served by tv-receiver /api/epg/* ----
# The XMLTV fetcher + parser previously here was migrated to tv-receiver
# (Go) 2026-05-27 — see tv-receiver/epg.go. hls-gateway now just GETs
# from tv-receiver. Fallback to tvh remains for slugs tv-receiver doesn't
# know (= legacy / non-m3u channels).


def _fetch_channel_events(ch_uuid, now_ts, horizon_ts, slug=None):
    """Returns events list for the channel within [now_ts, horizon_ts).

    Primary source: tv-receiver /api/epg/events/grid?slug=<slug>&from&to.
    Falls back to tvh /api/epg/events/grid (queried by UUID) when
    tv-receiver has no data for this slug or is unreachable."""
    # Try tv-receiver (slug-keyed, our local EPG store)
    if slug:
        try:
            url = (f"{TV_RECEIVER_BASE}/api/epg/grid"
                   f"?slug={urllib.parse.quote(slug)}"
                   f"&from={now_ts}&to={horizon_ts}")
            data = json.loads(urllib.request.urlopen(url, timeout=4).read())
            events = (data.get("events") or {}).get(slug) or []
            if events:
                # Map tv-receiver shape → hls-gateway/tvh-compat shape
                # (= "subtitle" field name matches; "event_id" matches).
                return [{
                    "start":    e["start"],
                    "stop":     e["stop"],
                    "title":    e.get("title", "?"),
                    "subtitle": e.get("subtitle", ""),
                    "event_id": e.get("event_id"),
                } for e in events]
        except Exception as e:
            print(f"tv-receiver epg fetch {slug}: {e}", flush=True)

    # Fallback: tvh's EPG via uuid (= for channels not in tv-receiver)
    try:
        params = urllib.parse.urlencode({
            "limit": 60, "channel": ch_uuid, "sort": "start",
        })
        url = f"{dvr_base()}/api/epg/events/grid?{params}"
        data = json.loads(urllib.request.urlopen(url, timeout=6).read())
        out = []
        for e in data.get("entries", []):
            s = e.get("start", 0); stop = e.get("stop", 0)
            if stop <= now_ts or s >= horizon_ts:
                continue
            out.append({"start": s, "stop": stop,
                        "title": e.get("title", "?"),
                        "subtitle": e.get("subtitle", ""),
                        "event_id": e.get("eventId") or e.get("id")})
        return out
    except Exception as e:
        print(f"epg fetch {ch_uuid[:8]}: {e}", flush=True)
        return []


def fetch_epg(window_before=900, window_after=6 * 3600, force=False):
    """Fetch EPG for all favorite channels within given window.

    Cached ~3 min unless force=True. The returned events list per channel
    is merged with archived events for the past portion of the window.
    """
    now_ts = int(time.time())
    if not force:
        with _epg_lock:
            cached = _epg_cache["data"]
            # Only reuse the cache when its window fully covers what we
            # were asked for — otherwise older/past or farther-future
            # events would be missing (even though the archive has them).
            if (cached and now_ts < _epg_cache["expires"]
                and cached["window_start"] <= now_ts - window_before
                and cached["window_end"]   >= now_ts + window_after):
                return cached
    win_start = now_ts - window_before
    win_end   = now_ts + window_after
    with cmap_lock:
        items = [(s, info) for s, info in channel_map.items()]
    with ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(
            lambda x: _fetch_channel_events(
                x[1]["uuid"], win_start, win_end, slug=x[0]),
            items))
    events_by_slug = dict(zip([s for s, _ in items], results))

    # Merge archived events (covers past — tvheadend deletes those)
    with _epg_archive_lock:
        for (slug, start), rec in _epg_archive.items():
            if slug not in events_by_slug:
                continue
            if rec["stop"] < win_start or start > win_end:
                continue
            # skip if we already have a live event at this start
            if any(e["start"] == start for e in events_by_slug[slug]):
                continue
            events_by_slug[slug].append({
                "start": rec["start"], "stop": rec["stop"],
                "title": rec.get("title", ""),
                "subtitle": rec.get("subtitle", ""),
                "event_id": rec.get("event_id"),
            })
    # sort each channel's events
    for slug in events_by_slug:
        events_by_slug[slug].sort(key=lambda e: e["start"])

    out = {"window_start": win_start, "window_end": win_end,
           "now": now_ts, "events": events_by_slug}
    with _epg_lock:
        _epg_cache["data"] = out
        _epg_cache["expires"] = now_ts + 180
    return out


@app.route("/api/epg.json")
def api_epg_json():
    try:
        hours_back = max(0, min(48, int(request.args.get("back", "1"))))
        hours_fwd  = max(1, min(48, int(request.args.get("fwd",  "12"))))
    except ValueError:
        hours_back, hours_fwd = 1, 12
    data = fetch_epg(window_before=hours_back * 3600,
                     window_after=hours_fwd * 3600)
    return Response(json.dumps(data), mimetype="application/json")


@app.route("/epg")
def epg_grid():
    # Widen window: from 12h ago to 18h ahead — covers "today + tomorrow"
    try:
        hours_back = max(0, min(48, int(request.args.get("back", "12"))))
        hours_fwd  = max(1, min(72, int(request.args.get("fwd",  "18"))))
    except ValueError:
        hours_back, hours_fwd = 12, 18
    data = fetch_epg(window_before=hours_back * 3600,
                     window_after=hours_fwd * 3600)
    now_ts = data["now"]
    win_start = data["window_start"]
    win_end   = data["window_end"]
    px_per_min = 5
    total_min = (win_end - win_start) // 60
    total_px  = total_min * px_per_min

    # time markers every 30 min
    time_markers = []
    # align to next half-hour inside window
    ts = win_start - (win_start % 1800) + 1800
    while ts < win_end:
        offset_px = ((ts - win_start) // 60) * px_per_min
        label = time.strftime("%H:%M", time.localtime(ts))
        time_markers.append((offset_px, label))
        ts += 1800

    with cmap_lock:
        # sort same as main page: pinned first, then usage-based
        items = list(channel_map.items())
    with stats_lock:
        st_snap = {s: dict(v) for s, v in stats.items()}
    items.sort(key=lambda kv: (
        0 if kv[0] in ALWAYS_WARM else 1,
        -st_snap.get(kv[0], {}).get("watch_seconds", 0),
        -st_snap.get(kv[0], {}).get("starts", 0),
        kv[1]["name"].lower()))

    # Map (channel_uuid, start_ts) -> dvr_uuid for already-scheduled entries
    scheduled = {}
    try:
        dvr_data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid_upcoming?limit=500",
            timeout=5).read())
        for d in dvr_data.get("entries", []):
            scheduled[(d.get("channel"), d.get("start"))] = d.get("uuid")
    except Exception:
        pass

    # Build channel column (left, non-scrolling)
    channel_col = ['<div class="ch-cell header"><span class="name">'
                   'Uhrzeit →</span></div>']
    # Build timeline column (right, horizontally scrollable)
    marker_html = "".join(
        f'<div class="epg-time-marker" style="left:{px}px">{lbl}</div>'
        for px, lbl in time_markers)
    tl_rows = [f'<div class="tl-row header">{marker_html}</div>']

    for slug, info in items:
        icon = _channel_logo_url(slug, info.get("icon", ""))
        name_escaped = info["name"].replace('"', "&quot;")
        stream_url = f"{HOST_URL}/watch/{slug}"
        if icon:
            inner = f'<img src="{icon}" alt="{name_escaped}" loading="lazy">'
        else:
            inner = f'<span class="name fallback">{info["name"]}</span>'
        channel_col.append(
            f'<a class="ch-cell" href="{stream_url}" title="{name_escaped}">'
            f'{inner}</a>')

        evts = data["events"].get(slug, [])
        event_html = []
        for e in evts:
            start = max(e["start"], win_start)
            stop  = min(e["stop"],  win_end)
            left_px  = ((start - win_start) // 60) * px_per_min
            width_px = max(int(((stop - start) / 60) * px_per_min) - 2, 40)
            is_now  = e["start"] <= now_ts < e["stop"]
            is_past = e["stop"] <= now_ts
            is_tight = (e["stop"] - e["start"]) < 10 * 60
            cls = "epg-event"
            if is_now: cls += " now"
            elif is_past: cls += " past"
            if is_tight: cls += " tight"
            ts_label = time.strftime("%H:%M", time.localtime(e["start"]))
            title = (e["title"] or "—").replace("<", "&lt;")
            eid = e.get("event_id") or ""
            dvr_uuid = scheduled.get((info["uuid"], e["start"]))
            data_attrs = ""
            if dvr_uuid:
                cls += " scheduled"
                data_attrs += f' data-uuid="{dvr_uuid}"'
            # Use the tvheadend event_id when we have it. Past events
            # that are archive-only (old entries without event_id) get
            # a synthetic "arc_<slug>_<start>" key the lookup endpoint
            # understands — so yesterday's Tatort still gets Mediathek
            # even though tvheadend dropped it from its live EPG.
            # data-slug disambiguates the record-event call: event_id is the
            # start epoch and not unique across channels, so the server needs
            # the slug to schedule the RIGHT channel for same-start events.
            data_attrs += f' data-slug="{slug}"'
            if eid:
                data_attrs += f' data-eid="{eid}"'
            elif is_past:
                data_attrs += f' data-eid="arc_{slug}_{e["start"]}"'
            event_html.append(
                f'<a class="{cls}" '
                f'style="left:{left_px}px;width:{width_px}px" '
                f'href="{stream_url}" aria-label="{title} — {ts_label}"'
                f'{data_attrs}>'
                f'<span class="t">{title}</span>'
                f'<span class="ts">{ts_label}</span></a>')
        tl_rows.append(f'<div class="tl-row">{"".join(event_html)}</div>')

    # Current time marker — position inside timeline only (no offset for channel col)
    now_offset_px = int((now_ts - win_start) / 60 * px_per_min)

    # Next 20:15 (Primetime) within the window
    now_lt = time.localtime(now_ts)
    today_2015 = time.mktime((now_lt.tm_year, now_lt.tm_mon, now_lt.tm_mday,
                              20, 15, 0, 0, 0, -1))
    prime_ts = today_2015 if today_2015 >= now_ts else today_2015 + 86400
    if prime_ts > win_end:
        prime_ts = today_2015
    prime_px = int((prime_ts - win_start) / 60 * px_per_min)
    now_line = (f'<div class="epg-now-line" '
                f'style="left:{now_offset_px}px"></div>')

    grid_html = (f'<div class="epg-wrap" id="tlscroll">'
                 f'<div class="epg-channels">{"".join(channel_col)}</div>'
                 f'<div class="epg-tl-scroll">'
                 f'<div class="epg-tl-inner" style="width:{total_px}px">'
                 f'{"".join(tl_rows)}{now_line}</div>'
                 f'</div></div>')

    body = (f"<html><head><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'>"
            f"<meta name='color-scheme' content='light dark'>"
            f"<style>{BASE_CSS}"
            f"html,body{{height:100%;overflow:hidden}}"
            f"body{{max-width:none;margin:0;padding:0;display:flex;flex-direction:column}}"
            f".epg-wrap{{flex:1 1 auto;min-height:0;max-height:none;margin-bottom:0}}"
            f"@media (pointer:coarse){{"
            f"html,body{{height:auto;overflow:visible}}"
            f"body{{display:block}}"
            f".epg-wrap{{flex:none;max-height:none}}"
            f"}}"
            f".epg-header{{display:flex;align-items:center;flex-wrap:wrap;"
            f"gap:8px 14px;padding:6px 10px;border-bottom:1px solid var(--border);"
            f"background:var(--bg);font-size:.9em}}"
            f".epg-header h1{{margin:0;font-size:1.1em;font-weight:600}}"
            # Bigger tap target on touch — wrap arrow + title in one
            # <a class=home-link>, give it padding + hover-tint.
            f".home-link{{display:inline-flex;align-items:center;gap:8px;"
            f"padding:8px 12px;margin:-6px -8px;border-radius:6px;"
            f"text-decoration:none;color:inherit;-webkit-tap-highlight-color:transparent}}"
            f".home-link .arrow{{font-size:1.4em;line-height:1}}"
            f"@media (hover:hover){{.home-link:hover{{background:var(--stripe)}}}}"
            f".epg-header .meta{{color:var(--muted);font-size:.85em}}"
            f".epg-header .spacer{{flex:1 1 auto}}"
            f".auto-refresh{{display:inline-flex;align-items:center;gap:.35em;"
            f"font-size:.85em;color:var(--muted);cursor:pointer;user-select:none}}"
            f".auto-refresh input{{accent-color:#1565c0;cursor:pointer}}"
            f".epg-search{{padding:4px 10px;border-radius:6px;"
            f"border:1px solid var(--border);background:var(--stripe);"
            f"color:var(--fg);font-size:14px;min-width:160px}}"
            f".epg-event.filter-hidden{{opacity:.15;pointer-events:none}}"
            f".epg-event.filter-match{{outline:2px solid #f1c40f;"
            f"outline-offset:-2px;z-index:5}}"
            f"</style></head><body>"
            f"<div class='epg-header'>"
            f"<a class='home-link' href='{HOST_URL}/'>"
            f"<span class='arrow'>←</span><h1>Programm</h1></a>"
            f"<span class='meta'>Stand {time.strftime('%H:%M', time.localtime(now_ts))}</span>"
            f"<a class='btn-now' href='#' onclick='jumpNow();return false'>"
            f"▶︎ Jetzt</a>"
            f"<a class='btn-now' href='#' onclick='jumpPrime();return false'>"
            f"🕗 20:15</a>"
            f"<input type='search' id='epg-search' placeholder='🔍 Titel filtern…' "
            f"class='epg-search'>"
            f"<label class='auto-refresh' title='Seite alle 60 s neu laden'>"
            f"<input type='checkbox' id='auto-refresh'>"
            f"<span>🔄 60s</span></label>"
            f"<span class='spacer'></span>"
            f"<span class='meta'>"
            f"<a href='?back=6&fwd=12'>6h</a> · "
            f"<a href='?back=12&fwd=18'>12h</a> · "
            f"<a href='?back=24&fwd=24'>24h</a></span>"
            f"</div>"
            f"{grid_html}"
            f"<script>"
            f"const NOW_PX={now_offset_px};"
            f"const PRIME_PX={prime_px};"
            f"function jumpNow(){{"
            f"  const w=document.getElementById('tlscroll');"
            f"  if(w)w.scrollTo({{left:Math.max(0,NOW_PX-100),behavior:'smooth'}});"
            f"}}"
            f"function jumpPrime(){{"
            f"  const w=document.getElementById('tlscroll');"
            f"  if(w)w.scrollTo({{left:Math.max(0,PRIME_PX-100),behavior:'smooth'}});"
            f"}}"
            # Title filter: dim non-matching events, highlight matches.
            f"function filterEpg(q){{"
            f"  q=(q||'').trim().toLowerCase();"
            f"  const evs=document.querySelectorAll('.epg-event');"
            f"  for(const el of evs){{"
            f"    el.classList.remove('filter-hidden','filter-match');"
            f"    if(!q)continue;"
            f"    const t=(el.textContent||'').toLowerCase();"
            f"    if(t.includes(q)){{el.classList.add('filter-match');}}"
            f"    else{{el.classList.add('filter-hidden');}}"
            f"  }}"
            f"}}"
            f"const searchBox=document.getElementById('epg-search');"
            f"if(searchBox){{"
            f"  searchBox.addEventListener('input',ev=>filterEpg(ev.target.value));"
            f"  const url=new URL(location.href);"
            f"  const initial=url.searchParams.get('q');"
            f"  if(initial){{searchBox.value=initial;filterEpg(initial);}}"
            f"}}"
            f"const LP_MS=500, LP_MOVE=10;"
            f"let lpTimer=null,lpX=0,lpY=0,lpEl=null,lpFired=false;"
            f"function lpCancel(){{"
            f"  if(lpTimer){{clearTimeout(lpTimer);lpTimer=null;}}"
            f"  if(lpEl){{lpEl.classList.remove('lp-active');lpEl=null;}}"
            f"}}"
            f"function lpStart(ev){{"
            f"  const el=ev.target.closest('.epg-event');"
            f"  if(!el)return;"
            f"  const hasEid=el.dataset.eid||el.dataset.uuid;"
            f"  if(!hasEid)return;"
            f"  const p=ev.touches?ev.touches[0]:ev;"
            f"  lpX=p.clientX;lpY=p.clientY;lpEl=el;lpFired=false;"
            f"  el.classList.add('lp-active');"
            f"  clearTimeout(lpTimer);"
            f"  lpTimer=setTimeout(()=>{{"
            f"    lpFired=true;"
            f"    try{{navigator.vibrate&&navigator.vibrate(25);}}catch(e){{}}"
            f"    const tgt=lpEl;lpCancel();handleLP(tgt);"
            f"  }},LP_MS);"
            f"}}"
            f"function lpMove(ev){{"
            f"  if(!lpTimer)return;"
            f"  const p=ev.touches?ev.touches[0]:ev;"
            f"  if(Math.abs(p.clientX-lpX)>LP_MOVE||Math.abs(p.clientY-lpY)>LP_MOVE)"
            f"    lpCancel();"
            f"}}"
            # Minimal 3-button modal for long-press. confirm() is binary
            # and can't offer "episode vs series", so we roll our own.
            # pointer-events:none on the host anchor during the dialog
            # absorbs iOS' queued synthesized click, which otherwise
            # navigates the page behind the modal.
            f"function lpDialog(opts){{"
            f"  return new Promise(resolve=>{{"
            f"    const bg=document.createElement('div');"
            f"    bg.style.cssText='position:fixed;inset:0;background:#000a;"
            f"z-index:1000;display:flex;align-items:center;justify-content:center;"
            f"padding:20px';"
            f"    const box=document.createElement('div');"
            f"    box.style.cssText='background:var(--bg,#fff);color:var(--fg,#000);"
            f"padding:18px;border-radius:10px;max-width:320px;width:100%;"
            f"font-size:.95em;box-shadow:0 8px 32px #0008';"
            f"    box.innerHTML='<div style=\"font-weight:600;margin-bottom:14px;"
            f"line-height:1.35\">'+opts.msg+'</div>';"
            f"    const row=document.createElement('div');"
            f"    row.style.cssText='display:flex;flex-direction:column;gap:8px';"
            f"    for(const b of opts.buttons){{"
            f"      const btn=document.createElement('button');"
            f"      btn.textContent=b.label;"
            f"      btn.style.cssText='padding:10px;border-radius:8px;border:0;"
            f"font-size:1em;cursor:pointer;'+(b.primary?'background:#e74c3c;color:#fff;"
            f"font-weight:600':'background:#ccc;color:#000');"
            f"      btn.onclick=()=>{{bg.remove();resolve(b.value);}};"
            f"      row.appendChild(btn);"
            f"    }}"
            f"    box.appendChild(row);bg.appendChild(box);"
            f"    document.body.appendChild(bg);"
            f"  }});"
            f"}}"
            f"/* Refresh the green-dot indicator on every .epg-event"
            f"   cell by querying the DVR upcoming list, indexing by"
            f"   EPG event id, and matching against each cell's"
            f"   data-eid. Idempotent — cells that lose their schedule"
            f"   (e.g. user just cancelled a series) also drop the"
            f"   class. Called after every record-event / record-series"
            f"   so the user gets immediate visual feedback without a"
            f"   page reload. */"
            f"function syncScheduledFromUpcoming(){{"
            f"  fetch('{HOST_URL}/api/internal/scheduled-events')"
            f"    .then(r=>r.json()).then(d=>{{"
            f"      const byEid={{}};"
            f"      for(const e of (d.entries||[])) byEid[String(e.eid)]=e.uuid;"
            f"      document.querySelectorAll('.epg-event[data-eid]').forEach(el=>{{"
            f"        const u=byEid[el.dataset.eid];"
            f"        if(u){{el.classList.add('scheduled');el.dataset.uuid=u;}}"
            f"        else{{el.classList.remove('scheduled');delete el.dataset.uuid;}}"
            f"      }});"
            f"    }}).catch(()=>{{}});"
            f"}}"
            f"function handleLP(el){{"
            f"  const ttl=(el.querySelector('.t')||{{}}).textContent||'diese Sendung';"
            f"  el.style.pointerEvents='none';"
            f"  const release=()=>setTimeout(()=>{{"
            f"    el.style.pointerEvents='';"
            f"  }},400);"
            f"  if(el.dataset.uuid){{"
            f"    lpDialog({{msg:'Geplante Aufnahme entfernen?<br><br>'+ttl,"
            f"      buttons:[{{label:'Entfernen',value:'yes',primary:true}},"
            f"               {{label:'Abbrechen',value:''}}]}}).then(v=>{{"
            f"      release();"
            f"      if(v==='yes')fetch('{HOST_URL}/cancel-recording/'+el.dataset.uuid)"
            f"        .then(r=>r.json()).then(d=>{{"
            f"          if(d.ok){{el.classList.remove('scheduled');"
            f"            delete el.dataset.uuid;}}"
            f"        }}).catch(()=>{{}});"
            f"    }});"
            f"  }} else if(el.dataset.eid){{"
            f"    const isPast=el.classList.contains('past');"
            # Query the Mediathek lookup in parallel. Past events can
            # only be retrieved via Mediathek, so we build a different
            # button list for them (no DVR options).
            f"    const mtFetch=fetch('{HOST_URL}/api/mediathek-lookup/'"
            f"+el.dataset.eid+'?slug='+(el.dataset.slug||'')).then(r=>r.json()).catch(()=>({{match:null}}));"
            f"    const dialogBtns=isPast?[]:[{{label:'Einzelne Episode',"
            f"value:'ep',primary:true}},{{label:'Ganze Serie',value:'series'}}];"
            f"    dialogBtns.push({{label:'Abbrechen',value:''}});"
            f"    mtFetch.then(m=>{{"
            f"      if(m&&m.match){{"
            f"        const avail=new Date(m.match.available_to*1000);"
            f"        const dStr=String(avail.getDate()).padStart(2,'0')+'.'"
            f"+String(avail.getMonth()+1).padStart(2,'0');"
            f"        const srcLabel=m.match.source==='zdf'?'ZDF':'ARD';"
            f"        const mtBtn={{label:'Aus '+srcLabel+' Mediathek (bis '+dStr+')',"
            f"value:'mediathek'}};"
            # Past events: "Jetzt abspielen" (primary, direct playback,
            # nothing persisted) plus "Aus Mediathek speichern" (save
            # a virtual recording for later). Future events: Mediathek
            # option slots between Episode and Serie as before.
            f"        if(isPast){{"
            f"          dialogBtns.splice(dialogBtns.length-1,0,"
            f"            {{label:'Jetzt abspielen',value:'play',primary:true}},"
            f"            mtBtn);"
            f"        }} else {{dialogBtns.splice(1,0,mtBtn);}}"
            f"      }} else if(isPast){{"
            # Past event with no Mediathek match: there's nothing we
            # can do, show a brief note and bail.
            f"        release();"
            f"        lpDialog({{msg:'Sendung ist bereits ausgestrahlt "
            f"und in der Mediathek nicht (mehr) verfügbar.',"
            f"          buttons:[{{label:'OK',value:'',primary:true}}]}});"
            f"        return;"
            f"      }}"
            f"      lpDialog({{msg:'Aufnahme planen?<br><br>'+ttl,"
            f"        buttons:dialogBtns}}).then(v=>{{"
            f"        release();"
            f"        if(v==='ep')fetch('{HOST_URL}/record-event/'+el.dataset.eid+'?slug='+(el.dataset.slug||''))"
            f"          .then(r=>r.json()).then(d=>{{"
            f"            if(d.ok){{"
            f"              /* Update the long-pressed cell immediately"
            f"                 (no wait for the upcoming-sync below). uuid"
            f"                 may be null in rare tvh response shapes — we"
            f"                 still want the green dot, the cancel-flow"
            f"                 will pick up the uuid via syncScheduledFromUpcoming. */"
            f"              el.classList.add('scheduled');"
            f"              if(d.uuid)el.dataset.uuid=d.uuid;"
            f"              syncScheduledFromUpcoming();"
            f"            }}"
            f"          }}).catch(()=>{{}});"
            f"        else if(v==='series')fetch('{HOST_URL}/record-series/'+el.dataset.eid+'?slug='+(el.dataset.slug||''))"
            f"          .then(r=>r.json()).then(d=>{{"
            f"            if(d.ok){{"
            f"              /* Series scheduling can plant green dots on"
            f"                 multiple cells (every future episode). Pull"
            f"                 the full upcoming list and walk the DOM so"
            f"                 ALL matching cells light up, not just the"
            f"                 one that was long-pressed. */"
            f"              syncScheduledFromUpcoming();"
            f"              const n=d.scheduled||0;"
            f"              const ep=n===1?'Folge':'Folgen';"
            f"              const msg=d.already_exists"
            f"                ?'<b>'+d.title+'</b> auf '+d.channel+"
            f"'<br><br>Serien-Aufnahme war bereits aktiv'"
            f"                :'<b>'+d.title+'</b> auf '+d.channel+"
            f"'<br><br>'+n+' '+ep+' aktuell geplant';"
            f"              lpDialog({{msg:msg,"
            f"                buttons:[{{label:'OK',value:'',primary:true}}]}});"
            f"            }}"
            f"          }}).catch(()=>{{}});"
            f"        else if(v==='play')"
            f"          location.href='{HOST_URL}/mediathek-play/'+el.dataset.eid;"
            f"        else if(v==='mediathek')"
            f"          fetch('{HOST_URL}/api/mediathek-schedule/'+el.dataset.eid+'?slug='+(el.dataset.slug||''),"
            f"            {{method:'POST'}})"
            f"            .then(r=>r.json()).then(d=>{{"
            f"              if(d.ok)lpDialog({{msg:'<b>'+d.title+"
            f"'</b><br><br>Aus Mediathek gespeichert. Unter Aufnahmen "
            f"abrufbar.',"
            f"                buttons:[{{label:'OK',value:'',primary:true}}]}});"
            f"              else lpDialog({{msg:'Fehler: '+(d.error||'?'),"
            f"                buttons:[{{label:'OK',value:'',primary:true}}]}});"
            f"            }}).catch(()=>{{}});"
            f"      }});"
            f"    }});"
            f"  }} else {{"
            f"    release();"
            f"  }}"
            f"}}"
            f"document.addEventListener('touchstart',lpStart,{{passive:true}});"
            f"document.addEventListener('touchmove',lpMove,{{passive:true}});"
            f"document.addEventListener('touchend',lpCancel);"
            f"document.addEventListener('touchcancel',lpCancel);"
            f"document.addEventListener('mousedown',ev=>{{"
            f"  if(ev.button===0)lpStart(ev);"
            f"}});"
            f"document.addEventListener('mousemove',lpMove);"
            f"document.addEventListener('mouseup',lpCancel);"
            f"document.addEventListener('mouseleave',lpCancel);"
            # Suppress the anchor click that follows a long-press.
            # Also: for past events a plain tap means "I want to watch
            # this show" — which on DVB means "nope, it's over". Route
            # those taps through the long-press handler so the Mediathek
            # option shows up instead of navigating to the live stream
            # of a now-unrelated current programme.
            f"document.addEventListener('click',ev=>{{"
            f"  const evEl=ev.target.closest('.epg-event');"
            f"  if(!evEl)return;"
            f"  if(lpFired){{"
            f"    ev.preventDefault();ev.stopPropagation();lpFired=false;"
            f"    return;"
            f"  }}"
            f"  if(evEl.classList.contains('past')&&evEl.dataset.eid){{"
            f"    ev.preventDefault();ev.stopPropagation();"
            f"    handleLP(evEl);"
            f"  }}"
            f"}},true);"
            f"document.addEventListener('contextmenu',ev=>{{"
            f"  if(ev.target.closest('.epg-event'))ev.preventDefault();"
            f"}});"
            f"window.addEventListener('load',()=>{{"
            f"  const w=document.getElementById('tlscroll');"
            f"  if(w)w.scrollLeft=Math.max(0,NOW_PX-100);"
            f"  const cb=document.getElementById('auto-refresh');"
            f"  if(cb){{"
            f"    cb.checked=localStorage.getItem('epgAutoRefresh')==='1';"
            f"    let timer=null;"
            f"    function arm(){{"
            f"      if(timer){{clearTimeout(timer);timer=null;}}"
            f"      if(cb.checked)timer=setTimeout(()=>location.reload(),60000);"
            f"    }}"
            f"    cb.addEventListener('change',()=>{{"
            f"      localStorage.setItem('epgAutoRefresh',cb.checked?'1':'0');"
            f"      arm();"
            f"    }});"
            f"    arm();"
            f"  }}"
            f"}});"
            f"</script>"
            f"</body></html>")
    return body


@app.route("/stats")
def usage_stats():
    """Ranking of channels by usage. Also includes ongoing sessions."""
    now = time.time()
    with active_lock:
        live = {s: now - i["started_at"] for s, i in channels.items()}
    with stats_lock:
        snap = {s: dict(v) for s, v in stats.items()}
    for s, secs in live.items():
        snap.setdefault(s, {"starts": 0, "watch_seconds": 0,
                             "last_watched": None})
        snap[s]["watch_seconds"] += secs  # include current live session
    rows = []
    for slug, v in snap.items():
        with cmap_lock:
            name = channel_map.get(slug, {}).get("name", slug)
        rows.append({"slug": slug, "name": name,
                     "starts": v.get("starts", 0),
                     "watch_hours": round(v.get("watch_seconds", 0)/3600, 2),
                     "last_watched": v.get("last_watched")})
    rows.sort(key=lambda r: (r["watch_hours"], r["starts"]), reverse=True)

    if request.args.get("format") != "json":
        html = [f"<html><head><meta name='viewport' "
                f"content='width=device-width,initial-scale=1'>"
                f"<meta name='color-scheme' content='light dark'>"
                f"<style>{BASE_CSS}</style></head><body>"
                f"<h1>Nutzungsstatistik</h1>"
                f"<p><a href='{HOST_URL}/'>← Zurück</a> · "
                f"<a href='{HOST_URL}/stats?format=json'>JSON</a></p>"
                f"<table><tr><th>#</th><th>Sender</th>"
                f"<th>Aufrufe</th><th>Std gesehen</th>"
                f"<th>Zuletzt</th></tr>"]
        for i, r in enumerate(rows, 1):
            html.append(f"<tr><td class='rank'>{i}</td>"
                        f"<td>{r['name']}</td>"
                        f"<td>{r['starts']}</td>"
                        f"<td>{r['watch_hours']}</td>"
                        f"<td>{r['last_watched'] or '—'}</td></tr>")
        html.append("</table>")
        # Audio+visual spot clusters from cross-channel fingerprint
        # matching. Loads async — heavy compute (family rebuild)
        # happens on the spot-fp worker thread; this just renders
        # the cached result. Labelled "Cluster" not "Werbespots"
        # because the matching catches some same-advertiser groupings
        # (shared brand outros) in addition to true exact-spot
        # repeats — refined further by the dHash visual confirmation
        # path but not perfect.
        html.append(
            "<h2 style='margin-top:32px;font-size:1.1em'>"
            "Wiederkehrende Werbe-Cluster "
            "<small style='color:#888;font-weight:400;"
            "font-size:.8em' id='spot-meta'></small></h2>"
            "<p style='color:#888;font-size:.85em;margin:0 0 12px;"
            "max-width:680px;line-height:1.45'>"
            "Audio-Fingerprint (Chromaprint) + Visual-dHash Matching "
            "über alle reviewten Werbeblöcke. Cluster ≈ wiederkehrende "
            "Spots oder same-brand-Spotgruppen. ▶ springt zum "
            "ersten Auftreten. Eine echte 1:1-Spot-Erkennung ist Audio "
            "+ Video noch nicht 100&nbsp;%ig &mdash; same-brand-Outros "
            "(z.&nbsp;B. shared IKEA-Tag) können Cluster aufblähen.</p>"
            "<div id='spot-list'>Lade…</div>"
            "<script>"
            "function fmtAbsTs(s){if(!s)return'—';"
            "const d=new Date(s*1000);"
            "return d.toLocaleDateString('de-DE',{day:'2-digit',"
            "month:'2-digit'})+' '+d.toLocaleTimeString('de-DE',"
            "{hour:'2-digit',minute:'2-digit'});}"
            "function fmtAge(s){if(!s)return'';const d=Math.max(0,"
            "Date.now()/1000-s);if(d<3600)return Math.floor(d/60)+' min';"
            "if(d<86400)return Math.floor(d/3600)+' h';"
            "return Math.floor(d/86400)+' Tg';}"
            "fetch('/api/internal/spot-fingerprints/families?min_size=2&limit=30')"
            ".then(r=>r.json()).then(d=>{"
            "const c=document.getElementById('spot-list');"
            "const m=document.getElementById('spot-meta');"
            "if(d.meta&&d.meta.rebuilt_at){"
            "m.textContent='('+(d.meta.n_fp||0)+' Spot-Fingerprints, '"
            "+(d.meta.n_families||0)+' Cluster · zuletzt '"
            "+fmtAge(parseInt(d.meta.rebuilt_at))+' aufgebaut)';"
            "}"
            "if(!d.families||d.families.length===0){"
            "c.innerHTML='<p style=\"color:#888\">Noch keine Cluster mit "
            "≥ 2 Aufnahmen. Index füllt sich nach jedem ✓ Geprüft.</p>';"
            "return;}"
            "const rows=d.families.map(f=>{"
            "const url='/recording/'+f.first_uuid+'?t='+Math.max(0,"
            "Math.floor(f.first_t_s));"
            "const chans=f.channels.slice(0,4).join(', ')+("
            "f.n_channels>4?(' +'+(f.n_channels-4)):'');"
            "return '<tr><td><a href=\"'+url+'\" title=\"Erste Stelle "
            "anhören\">▶</a></td>'"
            "+'<td><b>'+f.n_airings+'</b></td>'"
            "+'<td>'+f.n_recordings+'</td>'"
            "+'<td>'+f.n_channels+'</td>'"
            "+'<td style=\"font-size:.85em;color:#666\">'+chans+'</td>'"
            "+'<td style=\"font-size:.85em;color:#666\">'"
            "+fmtAbsTs(f.first_rec_ts)+' → '+fmtAbsTs(f.last_rec_ts)+'</td>'"
            "+'</tr>';}).join('');"
            "c.innerHTML='<table><tr>"
            "<th title=\"Springt zur ersten Stelle\"></th>"
            "<th title=\"Anzahl Airings über alle Aufnahmen\">Airings</th>"
            "<th title=\"In wie vielen Aufnahmen detektiert\">Aufn.</th>"
            "<th title=\"Auf wie vielen Sendern gesehen\">Sender</th>"
            "<th>Sender-Liste</th>"
            "<th>Erste – Letzte Erscheinung</th></tr>'+rows+'</table>';"
            "}).catch(e=>{document.getElementById('spot-list')"
            ".textContent='Fehler beim Laden: '+e;});"
            "</script>")
        html.append("</body></html>")
        return "\n".join(html)
    return {"ranking": rows}


def _render_per_show_iou_trend():
    """Render small per-show IoU sparklines from head.per-show-iou.jsonl.

    Sorted by latest-IoU ascending so the worst shows surface at top
    (= visual signal of where the model is regressing). Each sparkline
    is a small SVG (180×40 px) with the most recent N=12 datapoints.
    Color: green if last IoU > 0.85, orange 0.65-0.85, red < 0.65.

    Returns empty string if no snapshot file exists yet.
    """
    p = HLS_DIR / ".tvd-models" / "head.per-show-iou.jsonl"
    if not p.is_file():
        return ""
    series = {}  # show → [(ts, iou_mean, n), ...]
    try:
        for ln in p.read_text().splitlines():
            ln = ln.strip()
            if not ln:
                continue
            r = json.loads(ln)
            series.setdefault(r["show"], []).append(
                (r["ts"], float(r["iou_mean"]), int(r.get("n", 0))))
    except Exception:
        return ""
    if not series:
        return ""
    # Sort each show's series chronologically.
    for s in series.values():
        s.sort(key=lambda x: x[0])
    # Filter singletons (= movies, one-off specials) — per-show IoU
    # for those is meaningful as ONE datapoint but doesn't trend
    # (= broadcaster won't repeat 5×). Hiding declutters the section
    # so series with actual movement are easier to spot.
    show_n = _show_review_counts()
    autorec_t = _autorec_titles()
    ug_t = _user_grouped_titles()
    series = {s: pts for s, pts in series.items()
              if not _is_singleton_title(s, show_n, autorec_t, ug_t)}
    if not series:
        return ""
    # Sort shows by LAST iou ascending (worst first).
    shows = sorted(series.items(), key=lambda kv: kv[1][-1][1])

    def _sparkline(points, w=180, h=40):
        if len(points) < 2:
            return f"<span class='muted'>(nur {len(points)} datenpunkt(e))</span>"
        ious = [p[1] for p in points]
        n = len(points)
        xs = [int(i * (w - 8) / (n - 1)) + 4 for i in range(n)]
        ys = [int((1 - iou) * (h - 8)) + 4 for iou in ious]  # iou=1 → top
        path = "M" + " L".join(f"{x},{y}" for x, y in zip(xs, ys))
        last = ious[-1]
        if last > 0.85:    color = "#27ae60"
        elif last > 0.65:  color = "#f39c12"
        else:              color = "#e74c3c"
        # 0.5 reference line
        ref_y = int((1 - 0.5) * (h - 8)) + 4
        return (f"<svg width='{w}' height='{h}' viewBox='0 0 {w} {h}' "
                f"style='vertical-align:middle'>"
                f"<line x1='0' y1='{ref_y}' x2='{w}' y2='{ref_y}' "
                f"stroke='var(--border)' stroke-dasharray='2,2'/>"
                f"<path d='{path}' fill='none' stroke='{color}' stroke-width='2'/>"
                f"<circle cx='{xs[-1]}' cy='{ys[-1]}' r='3' fill='{color}'/>"
                f"</svg>")

    def _confidence(n):
        """Three-tier sample-size ampel for the per-show IoU mean.
        N=1 → mean is a single point, useless for trend judgement.
        N=2-4 → thin, can move ±15 IoU on next episode.
        N≥5 → robust, mean stable to ±5 within a recurring show.
        """
        if n >= 5:
            return ("🟢", "robust", "#27ae60")
        if n >= 2:
            return ("🟡", "dünn", "#f39c12")
        return ("🔴", "einzeln", "#e74c3c")

    out = ["<h2>Per-Show IoU-Verlauf</h2>",
           "<p class='muted'>letzte 12 Snapshots pro Sendung — sortiert nach "
           "aktueller IoU aufsteigend (Problemkinder oben). Y-Achse: 0 unten, "
           "1 oben; gestrichelte Linie = 0.5. Ampel = Stichprobengröße: "
           "🟢 ≥5 Episoden (robust), 🟡 2-4 (dünn), 🔴 1 (einzeln, IoU "
           "spiegelt nur diese eine Aufnahme).</p>",
           "<table style='font-size:0.9em'>",
           "<tr><th>Show</th><th>n</th><th>IoU jetzt</th><th>Δ vs erste</th>"
           "<th>Trend (letzte 12)</th></tr>"]
    for show, pts in shows:
        last = pts[-1][1]
        first = pts[0][1]
        delta = last - first
        delta_str = f"<span style='color:{('#27ae60' if delta>0.01 else '#e74c3c' if delta<-0.01 else 'var(--muted)')}'>{delta:+.2f}</span>"
        n_recs = pts[-1][2]
        emoji, label, _ = _confidence(n_recs)
        out.append(f"<tr><td>{show}</td>"
                   f"<td title='{label}'>{emoji} {n_recs}</td>"
                   f"<td>{last*100:.0f}%</td><td>{delta_str}</td>"
                   f"<td>{_sparkline(pts[-12:])}</td></tr>")
    out.append("</table>")
    return "\n".join(out)


def _render_status_filter(counts):
    """Render the /recordings status-filter checkbox row. Skips
    statuses with count==0 so empty filters don't add visual noise.
    Each label includes a (N) badge showing the row count."""
    items = [("live",      "● live"),
             ("warming",   "⏳ remux"),
             ("playable",  "▶ abspielbar"),
             ("pending",   "◌ ausstehend"),
             ("failed",    "⚠ fehlgeschlagen"),
             ("scheduled", "⏱ geplant")]
    out = []
    for key, label in items:
        n = counts.get(key, 0)
        if n == 0:
            continue
        out.append(f"<label><input type='checkbox' value='{key}' checked>"
                   f"{label} <span class='cnt'>({n})</span></label>")
    if counts.get("unedited", 0) > 0:
        out.append(f"<label style='margin-left:14px;border-style:dashed'>"
                   f"<input type='checkbox' value='unedited-only'>"
                   f"nur unbearbeitete "
                   f"<span class='cnt'>({counts['unedited']})</span></label>")
    return "".join(out)


def _render_current_metrics(history):
    """Cards showing the currently-deployed model's headline metrics
    (Block-IoU / Test Acc / Train Acc / Last Deploy) with an ampel
    indicator per metric so the user immediately sees whether a value
    is healthy. Quality bands are calibrated against actual per-show
    IoU spread (good shows hit 0.80+, weak shows 0.20s; OVERALL
    weighted average lives in the 0.60-0.65 range right now)."""
    if not history:
        return ""
    last_dep = next((e for e in reversed(history) if e.get("deployed")), None)
    if not last_dep:
        last_dep = history[-1]  # show latest run even if rejected
    iou = last_dep.get("test_iou")
    acc = last_dep.get("test_acc")
    train_acc = last_dep.get("train_acc")
    ts = last_dep.get("ts", "")
    deployed = last_dep.get("deployed", False)
    age_h = None
    if ts:
        try:
            t = time.mktime(time.strptime(ts, "%Y%m%dT%H%M%S"))
            age_h = (time.time() - t) / 3600
        except Exception:
            pass

    # Drift vs 7-day median (deployed-only) — same calculation as
    # /api/learning/summary's drift_vs_7d_median, so the card matches
    # what the daily-summary email mentions.
    deployed_recent = [e.get("test_iou") for e in history
                       if e.get("deployed") and e.get("test_iou") is not None]
    drift = None
    if iou is not None and len(deployed_recent) >= 7:
        prev7 = sorted(deployed_recent[-8:-1])  # exclude current
        if prev7:
            median = prev7[len(prev7) // 2]
            drift = iou - median

    def ampel(value, thresholds, higher_is_better=True):
        """Return (color, label) tuple based on value."""
        if value is None:
            return ("#7f8c8d", "—")
        good, ok = thresholds
        if higher_is_better:
            if value >= good: return ("#27ae60", "sehr gut")
            if value >= ok:   return ("#f39c12", "ok")
            return ("#e74c3c", "schwach")
        if value <= good: return ("#27ae60", "sehr gut")
        if value <= ok:   return ("#f39c12", "ok")
        return ("#e74c3c", "schwach")

    iou_amp = ampel(iou, (0.70, 0.55))
    acc_amp = ampel(acc, (0.92, 0.85))
    # Overfit hint: if train>>test, the head memorised noise.
    overfit_gap = (train_acc - acc) if (train_acc is not None and acc is not None) else None
    train_amp = ("#27ae60", "balanced")
    if overfit_gap is not None and overfit_gap > 0.10:
        train_amp = ("#e74c3c", f"overfit (+{overfit_gap*100:.0f}pp)")
    elif overfit_gap is not None and overfit_gap > 0.05:
        train_amp = ("#f39c12", f"leichter overfit (+{overfit_gap*100:.0f}pp)")
    age_amp = ("#7f8c8d", "—")
    if age_h is not None:
        age_amp = (("#27ae60", f"{age_h:.0f} h alt") if age_h < 30
                   else ("#f39c12", f"{age_h:.0f} h alt") if age_h < 48
                   else ("#e74c3c", f"{age_h:.0f} h alt — Training stockt"))

    drift_html = ""
    if drift is not None:
        sym = "▲" if drift > 0.005 else ("▼" if drift < -0.005 else "→")
        dcol = ("#27ae60" if drift > 0.005 else
                "#e74c3c" if drift < -0.005 else "#7f8c8d")
        drift_html = (f"<div class='metric-drift' style='color:{dcol}'>"
                      f"{sym} {drift:+.3f} vs 7-d Median</div>")

    def card(label, value_str, color, status, extra=""):
        return (f"<div class='metric-card' "
                f"style='border-left:4px solid {color}'>"
                f"<div class='metric-label'>{label}</div>"
                f"<div class='metric-value'>{value_str}</div>"
                f"<div class='metric-status' style='color:{color}'>{status}</div>"
                f"{extra}</div>")

    return (
        "<style>"
        ".metrics-grid{display:grid;grid-template-columns:"
        "repeat(auto-fit,minmax(180px,1fr));gap:10px;margin:6px 0 14px}"
        ".metric-card{background:var(--code-bg);border-radius:4px;"
        "padding:10px 14px;display:flex;flex-direction:column;gap:2px}"
        ".metric-label{font-size:.8em;color:var(--muted);"
        "text-transform:uppercase;letter-spacing:.5px}"
        ".metric-value{font-size:1.7em;font-weight:600;line-height:1.1}"
        ".metric-status{font-size:.85em;font-weight:500}"
        ".metric-drift{font-size:.8em;margin-top:2px}"
        "</style>"
        "<div class='metrics-grid'>"
        + card("Block-IoU (Test)",
               f"{iou*100:.1f}%" if iou is not None else "—",
               iou_amp[0], iou_amp[1], drift_html)
        + card("Test Accuracy",
               f"{acc*100:.1f}%" if acc is not None else "—",
               acc_amp[0], acc_amp[1])
        + card("Train Accuracy",
               f"{train_acc*100:.1f}%" if train_acc is not None else "—",
               train_amp[0], train_amp[1])
        + card("Letzter Deploy",
               ts[:8] + " " + ts[9:11] + ":" + ts[11:13] if len(ts) >= 13 else "—",
               age_amp[0], age_amp[1],
               "" if deployed else
               "<div class='metric-status' style='color:#e74c3c'>letzter Run REJECTED</div>")
        + "</div>"
    )


def _render_history_chart(history, width=860, height=280):
    """Inline SVG line-chart of train_acc / test_acc / test_iou over
    the run history. No external deps — scales sharp at any size,
    works offline, no JS. Rejected runs get an open ring instead of
    a filled dot so the eye picks them out without a legend."""
    if not history:
        return ""
    # Take the last 60 runs max — older drowns the chart visually
    runs = history[-60:]
    n = len(runs)
    pad_l, pad_r, pad_t, pad_b = 38, 12, 14, 26
    plot_w = width - pad_l - pad_r
    plot_h = height - pad_t - pad_b

    def x_at(i): return pad_l + (i / max(1, n - 1)) * plot_w
    def y_at(v): return pad_t + (1 - v) * plot_h  # v in [0,1]

    series = [
        ("test_iou",  "#3498db", "Block-IoU"),
        ("test_acc",  "#27ae60", "Test Acc"),
        ("train_acc", "#9b59b6", "Train Acc"),
    ]
    parts = [f"<svg viewBox='0 0 {width} {height}' "
             f"style='display:block;width:100%;max-width:{width}px;"
             f"background:var(--code-bg);border-radius:6px;margin-top:6px'>"]
    # Y-axis grid + percent labels. Both series (IoU and Acc) are in
    # [0,1] internally, so the same axis serves both — series are
    # disambiguated by colour via the legend, not by a second axis.
    for pct in (0, 25, 50, 75, 100):
        y = y_at(pct / 100)
        parts.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' "
                     f"y2='{y:.1f}' stroke='var(--border)' stroke-width='0.5'/>")
        parts.append(f"<text x='{pad_l-5}' y='{y+3:.1f}' fill='var(--muted)'"
                     f"font-size='10' text-anchor='end'>{pct}%</text>")
    # X-axis: tick every ~10 runs with date label
    step = max(1, n // 6)
    for i in range(0, n, step):
        ts = runs[i].get("ts", "")
        label = ts[4:8] if len(ts) >= 8 else ts  # MMDD slice from YYYYMMDD
        x = x_at(i)
        parts.append(f"<line x1='{x:.1f}' y1='{pad_t}' x2='{x:.1f}' "
                     f"y2='{height-pad_b}' stroke='var(--border)' stroke-width='0.5'/>")
        parts.append(f"<text x='{x:.1f}' y='{height-8}' fill='var(--muted)'"
                     f"font-size='10' text-anchor='middle'>{label}</text>")
    # All-time min/max horizontal references per series (computed
    # from the FULL history, not just the visible window — gives a
    # "best/worst we've ever hit" anchor that doesn't move when older
    # runs scroll off-chart).
    for key, color, _ in series:
        vals = [r.get(key) for r in history if r.get(key) is not None]
        if not vals:
            continue
        for v, tag in ((max(vals), "max"), (min(vals), "min")):
            y = y_at(v)
            parts.append(f"<line x1='{pad_l}' y1='{y:.1f}' "
                         f"x2='{width-pad_r}' y2='{y:.1f}' "
                         f"stroke='{color}' stroke-width='0.5' "
                         f"stroke-dasharray='2,3' opacity='0.5'/>")
            parts.append(f"<text x='{width-pad_r-3}' y='{y-2:.1f}' "
                         f"fill='{color}' font-size='9' "
                         f"text-anchor='end' opacity='0.75'>"
                         f"{tag} {v*100:.1f}%</text>")
    # Series lines + dots
    for key, color, label in series:
        pts = [(x_at(i), y_at(r.get(key) or 0))
               for i, r in enumerate(runs) if r.get(key) is not None]
        if len(pts) >= 2:
            d = "M " + " L ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
            parts.append(f"<path d='{d}' fill='none' stroke='{color}' "
                         f"stroke-width='1.6' opacity='0.85'/>")
        for i, r in enumerate(runs):
            v = r.get(key)
            if v is None:
                continue
            x, y = x_at(i), y_at(v)
            deployed = r.get("deployed", False)
            if deployed:
                parts.append(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2.6' "
                             f"fill='{color}'/>")
            else:
                # Open ring for rejected runs (visual flag without
                # needing a separate legend entry)
                parts.append(f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3.4' "
                             f"fill='none' stroke='{color}' stroke-width='1.4'/>")
    parts.append("</svg>")
    # Legend rendered as HTML ABOVE the SVG so it can never overlap
    # data points (Train Acc lives in the 90-99% band where any
    # in-plot legend always collides). Footnote BELOW the SVG so it
    # can't collide with the X-axis date labels at the right edge.
    legend = ["<div style='display:flex;gap:14px;font-size:.8em;"
              "margin-top:6px;color:var(--fg);align-items:center'>"]
    for key, color, label in series:
        legend.append(f"<span><span style='display:inline-block;"
                      f"width:10px;height:10px;border-radius:5px;"
                      f"background:{color};vertical-align:middle;"
                      f"margin-right:4px'></span>{label}</span>")
    legend.append(f"<span style='margin-left:auto;color:var(--muted);font-size:.85em'>"
                  f"○ rejected · ● deployed · {n} runs</span>")
    legend.append("</div>")
    return "".join(legend) + "".join(parts)


def _learning_health_banner():
    """Inline HTML banner shown atop /recordings when nightly retrain
    has been failing or stuck. Empty string when healthy — quiet by
    default, only nags on real problems."""
    h = _learning_health()
    if h["status"] == "ok":
        return ""
    color = "#f39c12" if h["status"] == "warn" else "#e74c3c"
    msg = (f"<b>NN-Training stockt</b> — letzter erfolgreicher Deploy vor "
           f"{h['last_age_h']} h. Reject-Grund: "
           f"{(h['last_reject'] or '')[:120]}"
           if h["status"] == "fail"
           else f"<b>NN-Training warnt</b> — letzter Deploy vor {h['last_age_h']} h")
    return (f"<div style='background:{color}22;border-left:4px solid {color};"
            f"padding:10px 14px;margin:8px 14px;border-radius:4px;font-size:.9em'>"
            f"⚠ {msg} · <a href='{HOST_URL}/learning' style='color:#fff'>"
            f"Details</a></div>")


def _learning_health():
    """Inspect the recent head.history.json runs and return a status
    summary used by both /learning and the /recordings banner.

    Returns dict with:
      status:       "ok" | "warn" | "fail"
      last_deploy:  ISO-ish timestamp of the most recent deployed run, or None
      last_age_h:   hours since last deploy, or None
      last_reject:  most recent rejected run's reason, or None
      trend:        list of (ts, iou, deployed) for last 10 runs"""
    history_path = HLS_DIR / ".tvd-models" / "head.history.json"
    out = {"status": "ok", "last_deploy": None, "last_age_h": None,
           "last_reject": None, "trend": []}
    try:
        h = json.loads(history_path.read_text())
    except Exception:
        return {**out, "status": "ok"}  # no history yet = healthy first-run state
    if not h:
        return out
    out["trend"] = [(e.get("ts"), e.get("test_iou"), e.get("deployed", False))
                    for e in h[-10:]]
    last_dep = next((e for e in reversed(h) if e.get("deployed")), None)
    if last_dep:
        out["last_deploy"] = last_dep["ts"]
        try:
            ts = time.mktime(time.strptime(last_dep["ts"], "%Y%m%dT%H%M%S"))
            out["last_age_h"] = round((time.time() - ts) / 3600, 1)
        except Exception:
            pass
    # Look for consecutive rejections at the tail.
    consec_rej = 0
    for e in reversed(h):
        if e.get("deployed"):
            break
        consec_rej += 1
    if consec_rej > 0:
        out["last_reject"] = h[-1].get("reason", "?")
    if consec_rej >= 3 or (out["last_age_h"] is not None and out["last_age_h"] > 48):
        out["status"] = "fail"
    elif consec_rej >= 1 or (out["last_age_h"] is not None and out["last_age_h"] > 30):
        out["status"] = "warn"

    # ── B: broken-template check ────────────────────────────────
    # Walk recordings grouped by channel slug; flag channels where
    # the most recent N detections are all 0-block or all >60% ad-rate
    # (= cached logo template silently broken — same failure mode we
    # had for RTL/Sat.1 the first day).
    out["broken_channels"] = []
    by_chan = {}  # slug -> list of (mtime, n_blocks, ad_rate)
    if HLS_DIR.exists():
        for d in HLS_DIR.glob("_rec_*"):
            uuid = d.name[5:]
            slug = _rec_channel_slug(uuid) or ""
            if not slug:
                continue
            ads_p = d / "ads.json"
            if not ads_p.is_file():
                continue
            try:
                ads = json.loads(ads_p.read_text())
                pl = d / "index.m3u8"
                dur = 0.0
                if pl.is_file():
                    for ln in pl.read_text().splitlines():
                        if ln.startswith("#EXTINF:"):
                            try:
                                dur += float(ln.split(":",1)[1].rstrip(","))
                            except Exception:
                                pass
            except Exception:
                continue
            n_blocks = len(ads) if isinstance(ads, list) else 0
            ad_secs = sum(e-s for s,e in ads) if isinstance(ads, list) else 0
            ad_rate = ad_secs / dur if dur > 0 else 0
            by_chan.setdefault(slug, []).append((ads_p.stat().st_mtime,
                                                  n_blocks, ad_rate, dur))
    THRESHOLD_RECS = 5
    for slug, recs in by_chan.items():
        recs.sort(key=lambda r: -r[0])  # newest first
        recent = recs[:THRESHOLD_RECS]
        if len(recent) < THRESHOLD_RECS:
            continue
        all_zero = all(n == 0 for _, n, _, _ in recent)
        # `all_high` was previously `all(rate > 0.60 ... if dur > 0)` —
        # which evaluates to True on an empty generator (= when ALL
        # recent recordings have dur=0, e.g. because their index.m3u8
        # is empty after a tar restore — incident 2026-05-27). Demand
        # at least one valid-duration recording before claiming
        # "high-ad-rate everywhere".
        valid_rates = [rate for _, _, rate, dur in recent if dur > 0]
        all_high = len(valid_rates) >= THRESHOLD_RECS and all(r > 0.60 for r in valid_rates)
        if all_zero or all_high:
            out["broken_channels"].append({
                "slug": slug,
                "kind": "zero-blocks" if all_zero else "all-ad-rate",
                "n": len(recent)})
            if out["status"] == "ok":
                out["status"] = "warn"

    # ── C: trend-based regression check ─────────────────────────
    # Champion-Challenger only compares to the LAST deployed run —
    # 5pp/day drift over 2 weeks would never trip it. Compare
    # current IoU against the median of the last 7 deployed runs.
    # Composition-aware: when the test set grew between the
    # comparison runs (e.g. new user-reviewed recordings entered
    # the test split), the metric isn't apples-to-apples — surface
    # an info marker instead of a regression alert.
    out["trend_drift"] = None
    out["test_composition_changed"] = False
    deployed_runs = [e for e in h
                     if e.get("deployed") and e.get("test_iou") is not None]
    if len(deployed_runs) >= 7:
        last7 = deployed_runs[-7:]
        ious = [e["test_iou"] for e in last7]
        med = sorted(ious)[len(ious)//2]
        cur = last7[-1]
        drift = cur["test_iou"] - med
        if drift < -0.05:
            # Composition-aware: any size mismatch in the comparison
            # window means the test sets aren't apples-to-apples
            # (= even if cur_n equals the mode, when some runs in
            # between had a different size, the recordings rotated
            # in/out and the IoU baseline isn't the same population).
            # Only flag a real regression when ALL runs in the window
            # had the identical test-set size as cur.
            cur_n = cur.get("n_test_recs", 0)
            other_n = [e.get("n_test_recs", 0) for e in last7[:-1]]
            any_differs = any(n != cur_n for n in other_n)
            if any_differs:
                # Find the most recent prior n that differs, for the message.
                prev_n = next((n for n in reversed(other_n) if n != cur_n),
                              cur_n)
                out["test_composition_changed"] = {
                    "drift_pp": round(drift, 3),
                    "from_n": prev_n, "to_n": cur_n,
                    "n_added": cur_n - prev_n}
                # Don't elevate to "warn" — composition change is
                # expected when the user keeps labelling new shows.
            else:
                out["trend_drift"] = round(drift, 3)
                if out["status"] == "ok":
                    out["status"] = "warn"

    return out


# ── Failure-mode taxonomy (feature 10) ─────────────────────────────
# Each user-vs-auto mismatch on a recording slots into one of N
# categories. Aggregating by show + by channel turns vague "bad IoU"
# into concrete "85 % of sixx mismatches are washout — invest in
# audio-RMS, not bumper-template tuning". Read-only over existing
# ads.json/ads_user.json; no schema changes needed.

FAILURE_MODES = [
    ("good",            "🟢 IoU >0.85 (kein echter Mismatch)"),
    ("runaway",         "🔴 State-Machine Runaway (Block >50% Aufnahme)"),
    ("washout",         "🟠 Logo-Washout (kein Auto-Block trotz User-Block)"),
    ("missed_bumper",   "🟡 Bumper-Snap fired nicht (Boundary >10s vom Bumper-Anker)"),
    ("boundary_drift",  "🟡 Boundary-Drift >30s (Block existiert, Grenze daneben)"),
    ("false_positive",  "🟠 False Positive (Auto-Block ohne User-Overlap)"),
    ("false_negative",  "🟠 False Negative (User-Block ohne Auto-Overlap)"),
]


def _block_iou(a, b):
    """IoU between two block lists (each a list of [start,end] pairs).
    Treats blocks as time-intervals; intersection/union are computed
    on the merged interval-set."""
    def total(blocks):
        return sum(max(0, e - s) for s, e in blocks)
    def inter(a, b):
        out = 0.0
        for as_, ae in a:
            for bs_, be in b:
                lo = max(as_, bs_); hi = min(ae, be)
                if hi > lo:
                    out += hi - lo
        return out
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    i = inter(a, b)
    u = total(a) + total(b) - i
    return i / u if u > 0 else 0.0


def _classify_recording_failures(uuid):
    """Classify the (auto, user) mismatch for one recording into the
    primary failure mode. Returns None if the recording lacks user
    review (= no ground truth) OR is currently mid-redetect (auto
    blocks haven't been written yet — would otherwise misclassify as
    "washout" en masse during a bulk re-detect after head.bin
    update). Returns one of FAILURE_MODES keys."""
    rec_dir = HLS_DIR / f"_rec_{uuid}"
    user_p = rec_dir / "ads_user.json"
    auto_p = rec_dir / "ads.json"
    if not user_p.exists():
        return None  # no ground truth, can't classify
    # Re-detect in flight — auto blocks not yet computed for the
    # current head. Skip until the daemon finishes; classifying now
    # would falsely mark every reviewed recording as "washout".
    # Both marker tiers count: low-prio backfill is still pending work.
    if not auto_p.exists() and any(
            (rec_dir / n).exists() for n in
            (".detect-requested", ".detect-requested-low")):
        return None
    try:
        user_raw = json.loads(user_p.read_text())
    except Exception:
        return None
    if not isinstance(user_raw, dict):
        return None
    if not user_raw.get("reviewed_at"):
        return None  # not reviewed — premature to call it a mismatch
    user_blocks = user_raw.get("ads", []) or []
    if auto_p.exists():
        try:
            auto_data = json.loads(auto_p.read_text())
        except Exception:
            auto_data = []
        auto_blocks = (auto_data if isinstance(auto_data, list)
                       else auto_data.get("auto", []))
    else:
        # ads.json missing but .txt cutlist may still be on disk —
        # detection completed (possibly with 0 blocks) but the cache
        # was never warmed because nobody opened /recording/<uuid>.
        # Parse the .txt directly so classification reflects reality
        # (= detect ran, this is its actual output) instead of falsely
        # labelling everything as washout.
        sidecars = (".logo.txt", ".trained.logo.txt", ".cskp.txt", ".tvd.txt")
        txts = [p for p in rec_dir.glob("*.txt")
                if not any(p.name.endswith(s) for s in sidecars)]
        if not txts:
            return None  # never detected, can't classify
        auto_blocks = _rec_parse_comskip(rec_dir) or []

    # Recording duration from playlist (sum of EXTINF lines).
    dur = 0.0
    pl = rec_dir / "index.m3u8"
    if pl.is_file():
        try:
            for ln in pl.read_text().splitlines():
                if ln.startswith("#EXTINF:"):
                    try: dur += float(ln.split(":", 1)[1].rstrip(","))
                    except Exception: pass
        except Exception:
            pass

    # IoU first — anything above 0.85 is a "good" call, not a failure.
    iou = _block_iou(auto_blocks, user_blocks)
    if iou >= 0.85:
        return "good"

    # Runaway: any auto block spans >50% of the recording. The cap
    # introduced today drops these going forward, but historical
    # detections may still show this pattern.
    if dur > 0:
        for a in auto_blocks:
            if (a[1] - a[0]) / dur > 0.5:
                return "runaway"

    # No auto blocks at all but user has them → state machine never
    # opened a candidate. Almost always logo-washout (logo-conf too
    # high throughout, no consecutive-absent stretch met threshold).
    if not auto_blocks and user_blocks:
        return "washout"

    # Auto blocks exist but no overlap with any user block (and user
    # has blocks) → false positives somewhere in the recording.
    def overlaps(x, y):
        return x[0] < y[1] and y[0] < x[1]
    auto_with_user_overlap = [
        a for a in auto_blocks
        if any(overlaps(a, u) for u in user_blocks)]
    if not auto_with_user_overlap and user_blocks:
        return "false_positive"

    # User blocks exist but no auto block overlaps any → state
    # machine emitted some blocks but missed every real one (= miss
    # in the wrong place, not just a drift).
    user_with_auto_overlap = [
        u for u in user_blocks
        if any(overlaps(u, a) for a in auto_blocks)]
    if not user_with_auto_overlap and user_blocks:
        return "false_negative"

    # Bumper-snap miss: did we have bumpers for this channel AND the
    # nearest auto-block-end is >10s from the user-block-end?
    slug = _rec_channel_slug(uuid) or ""
    bdir = _TVD_BUMPER_DIR / slug if slug else None
    has_bumpers = (bdir and bdir.is_dir() and
                   (any(bdir.glob("*.png")) or
                    any((bdir / "end").glob("*.png")) if (bdir / "end").is_dir()
                    else False))
    if has_bumpers:
        for u in user_blocks:
            for a in auto_with_user_overlap:
                if overlaps(a, u) and abs(a[1] - u[1]) > 10:
                    return "missed_bumper"

    # Anything else: blocks exist on both sides with overlap, but
    # boundaries are off by more than the 30s tolerance.
    return "boundary_drift"


# 30 s TTL cache for heavy aggregators on /learning. All of them
# walk every _rec_* dir + read ads.json/ads_user.json/.txt per dir;
# at 125+ recordings that adds 5-7 s to every page load. The caches
# are invalidated by mtime — if any user just edited ads_user.json,
# next request recomputes. TTL also bounds staleness on quiet ticks.
_learning_agg_cache = {"ts": 0, "mtime": 0,
                       "fm": None, "gaps": None,
                       "episodes": None, "fingerprints": None}
_LEARNING_CACHE_TTL_S = 30


def _learning_agg_max_mtime():
    """Cheapest staleness signal: largest mtime across the actual
    files the aggregators read. Stat'ing the DIR mtime alone misses
    in-place writes — e.g. clicking ✓ Geprüft a second time
    overwrites ads_user.json without touching the dir entry, so on
    ext4 the dir mtime doesn't bump and the cache served stale data
    for up to 30s. Stating the JSON files directly catches both
    overwrites and creates."""
    m = 0
    try:
        for d in HLS_DIR.iterdir():
            if not d.name.startswith("_rec_"):
                continue
            for fn in ("ads_user.json", "ads.json",
                       ".detect-requested", ".detect-requested-low"):
                try:
                    ts = (d / fn).stat().st_mtime
                    if ts > m:
                        m = ts
                except Exception:
                    pass
    except Exception:
        pass
    return m


def _aggregate_failure_modes_cached():
    now = time.time()
    cur_mtime = _learning_agg_max_mtime()
    if (_learning_agg_cache["fm"] is not None
            and now - _learning_agg_cache["ts"] < _LEARNING_CACHE_TTL_S
            and cur_mtime == _learning_agg_cache["mtime"]):
        return _learning_agg_cache["fm"]
    result = _aggregate_failure_modes()
    _learning_agg_cache["fm"] = result
    _learning_agg_cache["ts"] = now
    _learning_agg_cache["mtime"] = cur_mtime
    return result


def _aggregate_show_gaps_cached():
    now = time.time()
    cur_mtime = _learning_agg_max_mtime()
    if (_learning_agg_cache["gaps"] is not None
            and now - _learning_agg_cache["ts"] < _LEARNING_CACHE_TTL_S
            and cur_mtime == _learning_agg_cache["mtime"]):
        return _learning_agg_cache["gaps"]
    result = _aggregate_show_gaps()
    _learning_agg_cache["gaps"] = result
    _learning_agg_cache["ts"] = now
    _learning_agg_cache["mtime"] = cur_mtime
    return result


def _compute_show_fingerprints_cached(*args, **kwargs):
    """Cached wrapper around _compute_show_fingerprints. Same TTL +
    mtime-invalidation as the failure-mode and show-gap caches.
    Skip the cache when the caller passes non-default arguments
    (e.g. leave-one-out validation builds custom fingerprints — must
    not poison the shared cache)."""
    if args or kwargs:
        return _compute_show_fingerprints(*args, **kwargs)
    now = time.time()
    cur_mtime = _learning_agg_max_mtime()
    if (_learning_agg_cache["fingerprints"] is not None
            and now - _learning_agg_cache["ts"] < _LEARNING_CACHE_TTL_S
            and cur_mtime == _learning_agg_cache["mtime"]):
        return _learning_agg_cache["fingerprints"]
    result = _compute_show_fingerprints()
    _learning_agg_cache["fingerprints"] = result
    _learning_agg_cache["ts"] = now
    _learning_agg_cache["mtime"] = cur_mtime
    return result


def _aggregate_failure_modes():
    """Walk all reviewed recordings, tally failure modes per show
    AND per channel. Each entry tracks total, mode counts, and the
    UUID list per mode so the UI can drill from the stacked bar
    into the actual broken recordings."""
    per_show = {}
    per_channel = {}
    for d in HLS_DIR.glob("_rec_*"):
        uuid = d.name[5:]
        mode = _classify_recording_failures(uuid)
        if not mode:
            continue
        show = _show_title_for_rec(d) or "(untitled)"
        slug = _rec_channel_slug(uuid) or "(unknown)"
        for bucket, key in ((per_show, show), (per_channel, slug)):
            entry = bucket.setdefault(
                key, {"total": 0, "modes": {}, "uuids": {}})
            entry["total"] += 1
            entry["modes"][mode] = entry["modes"].get(mode, 0) + 1
            entry["uuids"].setdefault(mode, []).append(uuid)
    return per_show, per_channel


def _rec_date_from_filename(rec_dir):
    """Derive the recording date suffix from the cutlist filename
    (`Show $YYYY-MM-DD-HHMM.txt` → `2026-04-30 17:43`). Returns ""
    when no parseable basename is found."""
    for p in rec_dir.glob("*.txt"):
        if any(p.name.endswith(s) for s in
               (".logo.txt", ".cskp.txt", ".tvd.txt",
                ".trained.logo.txt")):
            continue
        if " $" in p.stem:
            stamp = p.stem.split(" $", 1)[1]
            # "2026-04-30-1743" or "2026-04-30-1743-1" (counter suffix)
            parts = stamp.split("-")
            if len(parts) >= 4 and len(parts[3]) >= 4:
                return f"{parts[0]}-{parts[1]}-{parts[2]} {parts[3][:2]}:{parts[3][2:4]}"
        return ""
    return ""


def _render_failure_modes(per_show, per_channel):
    """Two-table render: by-channel rollup first (broader patterns),
    then by-show breakdown (where to focus). Each row shows the
    dominant failure mode + a stacked-mini-bar of all modes."""
    if not per_show and not per_channel:
        return ""

    # Color per failure mode (matches the FAILURE_MODES emoji).
    mode_color = {
        "good":           "#27ae60",
        "runaway":        "#c0392b",
        "washout":        "#e67e22",
        "missed_bumper":  "#f1c40f",
        "boundary_drift": "#f39c12",
        "false_positive": "#e74c3c",
        "false_negative": "#d35400",
    }

    def render_bar(modes, total, row_id):
        """Stacked horizontal bar, 100 % wide split by mode shares.
        Each segment is a clickable button that toggles the per-mode
        recording-list panel beneath the row (= drill-down from bar
        to actual broken recordings)."""
        if total == 0:
            return ""
        parts = []
        for key, _ in FAILURE_MODES:
            n = modes.get(key, 0)
            if n == 0:
                continue
            pct = n / total * 100
            # Skip the click-to-expand for the "good" segment — those
            # don't need fixing. Cursor stays default to signal that.
            if key == "good":
                cursor = "default"
                onclick = ""
            else:
                cursor = "pointer"
                onclick = (f" onclick=\"toggleFailDetails("
                           f"'{row_id}','{key}')\"")
            parts.append(
                f"<div style='background:{mode_color[key]};"
                f"width:{pct:.1f}%;height:14px;display:inline-block;"
                f"cursor:{cursor}' "
                f"title='{n}× {key} (klicken für Details)'"
                f"{onclick}></div>")
        return ("<div style='display:flex;width:100%;height:14px;"
                "border-radius:3px;overflow:hidden'>"
                + "".join(parts) + "</div>")

    def dominant(modes):
        non_good = {k: v for k, v in modes.items() if k != "good"}
        if not non_good:
            return ""
        k = max(non_good, key=non_good.get)
        return f"{k} ({non_good[k]}×)"

    def _bumper_diagnostic(uuid, mode):
        """For missed_bumper / boundary_drift cases, surface the
        per-block deltas (user vs auto) so the user can tell at a
        glance whether the snap-window (±90s default) or the match
        threshold (0.85 default) is the actual limiter:
          • |Δ| > 90s = window too narrow; widen to fix
          • |Δ| < 90s = bumper conf below threshold; lower to fix
                         (or capture more template variants)
        Empty string for non-snap-related modes."""
        if mode not in ("missed_bumper", "boundary_drift"):
            return ""
        d = HLS_DIR / f"_rec_{uuid}"
        try:
            user_raw = json.loads((d / "ads_user.json").read_text())
            user_blocks = user_raw.get("ads", []) or []
        except Exception:
            return ""
        try:
            auto_raw = json.loads((d / "ads.json").read_text())
            auto_blocks = (auto_raw if isinstance(auto_raw, list)
                           else auto_raw.get("auto", []))
        except Exception:
            auto_blocks = _rec_parse_comskip(d) or []
        if not user_blocks or not auto_blocks:
            return ""
        # For each user block, find its closest auto block by
        # midpoint (= which auto block is the candidate match).
        rows = []
        for i, (us, ue) in enumerate(user_blocks):
            best = None  # (auto_idx, mid_dist, as, ae)
            for j, (as_, ae) in enumerate(auto_blocks):
                d_mid = abs((us + ue) / 2 - (as_ + ae) / 2)
                if best is None or d_mid < best[1]:
                    best = (j, d_mid, as_, ae)
            if best is None:
                continue
            j, _, as_, ae = best
            d_start = us - as_   # +ve = auto starts EARLIER than user
            d_end = ue - ae      # +ve = auto ends EARLIER than user
            # Highlight the boundary off by >10s
            def fmt(dx):
                if abs(dx) <= 10:
                    return f"<span style='color:#27ae60'>{dx:+.0f}s</span>"
                if abs(dx) <= 90:
                    return (f"<span style='color:#f39c12' "
                            f"title='innerhalb Snap-Window — "
                            f"Threshold dürfte limitieren'>"
                            f"{dx:+.0f}s</span>")
                return (f"<span style='color:#e74c3c' "
                        f"title='außerhalb ±90s Snap-Window — "
                        f"Window-Vergrößerung würde helfen'>"
                        f"{dx:+.0f}s</span>")
            rows.append(
                f"User [{us:.0f}–{ue:.0f}] vs "
                f"Auto [{as_:.0f}–{ae:.0f}] — "
                f"ΔStart {fmt(d_start)}, ΔEnd {fmt(d_end)}")
        if not rows:
            return ""
        return ("<div style='font-size:.78em;color:var(--muted);"
                "padding:3px 0 0 4px;font-family:monospace'>"
                + "<br>".join(rows) + "</div>")

    def render_recordings_for_mode(uuids, mode):
        """Per-recording mini-list inside an expanded drill-down
        panel. Each row: title • date • action buttons + optional
        per-block snap-delta diagnostic for missed_bumper cases."""
        rows = []
        for uuid in uuids:
            d = HLS_DIR / f"_rec_{uuid}"
            title = _show_title_for_rec(d) or uuid[:8]
            date_s = _rec_date_from_filename(d)
            diag = _bumper_diagnostic(uuid, mode)
            rows.append(
                f"<div class='fm-rec'>"
                f"<div class='fm-rec-main'>"
                f"<a href='{HOST_URL}/recording/{uuid}' "
                f"class='fm-rec-link'>"
                f"<b>{title}</b>"
                f"{(' · ' + date_s) if date_s else ''}</a>"
                f"<span class='fm-rec-actions'>"
                f"<button class='fm-btn redet' "
                f"onclick=\"failModeRedetect('{uuid}',this)\">"
                f"🔄 Re-Detect</button>"
                f"<a class='fm-btn check' "
                f"href='{HOST_URL}/recording/{uuid}'>"
                f"👁 Prüfen</a>"
                f"<button class='fm-btn del' "
                f"onclick=\"failModeDelete('{uuid}',this)\">"
                f"🗑 Löschen</button>"
                f"</span></div>"
                f"{diag}"
                f"</div>")
        return "\n".join(rows)

    def render_table(rows, label):
        scope = label.split(' ')[-1].lower()
        out = [f"<h3 style='margin-top:14px'>{label}</h3>"]
        out.append("<div style='overflow-x:auto;"
                   "-webkit-overflow-scrolling:touch'>")
        out.append("<table style='width:100%;min-width:520px'><tr>"
                   f"<th style='text-align:left'>{label.split(' ')[-1]}</th>"
                   "<th>n</th><th>Dominant</th>"
                   "<th style='width:40%'>Verteilung</th></tr>")
        # Sort by failure-rate descending (most-broken first).
        def fail_rate(entry):
            non_good = sum(v for k, v in entry["modes"].items() if k != "good")
            return non_good / entry["total"] if entry["total"] else 0
        for key in sorted(rows, key=lambda k: -fail_rate(rows[k])):
            r = rows[key]
            row_id = f"{scope}-" + re.sub(r"[^a-z0-9]+", "-", key.lower()).strip("-")
            out.append(f"<tr><td><b>{key}</b></td>"
                       f"<td>{r['total']}</td>"
                       f"<td class='muted' style='white-space:nowrap'>"
                       f"{dominant(r['modes'])}</td>"
                       f"<td>{render_bar(r['modes'], r['total'], row_id)}</td></tr>")
            # Hidden drill-down: one inner panel per non-good mode.
            for mode_key, _ in FAILURE_MODES:
                if mode_key == "good":
                    continue
                uuids = r["uuids"].get(mode_key, [])
                if not uuids:
                    continue
                out.append(
                    f"<tr id='fm-detail-{row_id}-{mode_key}' "
                    f"class='fm-detail' style='display:none'>"
                    f"<td colspan='4' style='padding:8px 12px;"
                    f"background:var(--code-bg)'>"
                    f"<div class='fm-detail-head' "
                    f"style='color:{mode_color[mode_key]};font-weight:600;"
                    f"margin-bottom:6px'>"
                    f"{mode_key} ({len(uuids)} Aufnahme{'n' if len(uuids)!=1 else ''})"
                    f"</div>"
                    f"{render_recordings_for_mode(uuids, mode_key)}"
                    f"</td></tr>")
        out.append("</table></div>")
        return "\n".join(out)

    # Legend + tables
    parts = ["<p class='muted'>Wo das Modell stolpert — pro geprüfter "
             "Aufnahme die häufigste Failure-Kategorie. Zeigt welche "
             "Optimierung wo am meisten bringt: Washout-Cluster auf "
             "einem Sender → Audio-RMS ergänzen; Boundary-Drift überall "
             "→ Bumper-Templates nachpflegen; Runaway noch sichtbar → "
             "MaxBlockFraction-Cap ist neu, nur historische Detektionen "
             "betroffen.</p>"]
    parts.append("<div style='display:flex;flex-wrap:wrap;gap:10px;"
                 "margin:6px 0 10px;font-size:.85em'>")
    for key, label in FAILURE_MODES:
        parts.append(f"<span><span style='display:inline-block;width:12px;"
                     f"height:12px;background:{mode_color[key]};"
                     f"vertical-align:middle;margin-right:4px;"
                     f"border-radius:2px'></span>{label}</span>")
    parts.append("</div>")
    if per_channel:
        parts.append(render_table(per_channel, "Per Channel"))
    if per_show:
        parts.append(render_table(per_show, "Per Show"))
    parts.append(
        "<style>"
        ".fm-rec{display:flex;flex-direction:column;gap:2px;"
        "padding:5px 0;border-bottom:1px solid var(--border)}"
        ".fm-rec:last-child{border-bottom:0}"
        ".fm-rec-main{display:flex;flex-wrap:wrap;align-items:center;gap:6px}"
        ".fm-rec-link{color:var(--fg);text-decoration:none;flex:1 1 auto;"
        "min-width:200px}"
        ".fm-rec-link:hover{text-decoration:underline}"
        ".fm-rec-actions{display:flex;gap:4px;flex:0 0 auto}"
        ".fm-btn{background:#fff2;color:var(--fg);border:0;"
        "padding:3px 8px;border-radius:12px;font-size:.78em;"
        "cursor:pointer;text-decoration:none;display:inline-flex;"
        "align-items:center;line-height:1;white-space:nowrap}"
        ".fm-btn.redet{background:#27ae6033}"
        ".fm-btn.check{background:#3498db33}"
        ".fm-btn.del{background:#e74c3c33;color:#e74c3c}"
        ".fm-btn:hover{filter:brightness(1.2)}"
        ".fm-btn:disabled{opacity:.5;cursor:wait}"
        "</style>"
        "<script>"
        "function toggleFailDetails(rowId, mode){"
        "  /* Close all OTHER drill-downs first so only one is open */"
        "  document.querySelectorAll('.fm-detail').forEach(el=>{"
        "    if(el.id !== 'fm-detail-'+rowId+'-'+mode) el.style.display='none';"
        "  });"
        "  const el = document.getElementById('fm-detail-'+rowId+'-'+mode);"
        "  if(!el) return;"
        "  el.style.display = el.style.display==='none' ? '' : 'none';"
        "}"
        "function failModeRedetect(uuid, btn){"
        "  btn.disabled=true; const old=btn.textContent;"
        "  btn.textContent='⏳ läuft…';"
        f"  fetch('{HOST_URL}/api/recording/'+uuid+'/redetect',"
        "    {method:'POST'}).then(r=>r.json()).then(d=>{"
        "      btn.textContent = d && d.ok ? '✓ markiert' : '✗ Fehler';"
        "      setTimeout(()=>{btn.textContent=old; btn.disabled=false;}, 3000);"
        "  }).catch(()=>{btn.textContent='✗ Netz';"
        "    setTimeout(()=>{btn.textContent=old; btn.disabled=false;}, 3000);});"
        "}"
        "function failModeDelete(uuid, btn){"
        "  if(!confirm('Aufnahme komplett löschen?\\n\\n'+"
        "    'Das löscht .ts + ads_user.json + HLS-Bundle. '+"
        "    'Trainings-Daten gehen verloren.\\n\\nUUID: '+uuid)) return;"
        "  btn.disabled=true; btn.textContent='⏳';"
        f"  fetch('{HOST_URL}/recording/'+uuid+'/delete',{{method:'DELETE'}})"
        "    .then(()=>{"
        "      const row = btn.closest('.fm-rec');"
        "      if(row){row.style.opacity='.4';row.style.textDecoration='line-through';"
        "        btn.textContent='✓ gelöscht';}"
        "      else btn.textContent='✓';"
        "  }).catch(()=>{btn.textContent='✗ Fehler';btn.disabled=false;});"
        "}"
        "</script>"
    )
    return "\n".join(parts)


# ── Show-gap detection (feature 11) ────────────────────────────────
# Surface concrete labelling-leverage opportunities: shows with a
# trailing-N=1 problem (= IoU is statistical noise), shows with many
# unlabelled siblings of an already-confirmed episode (= cheap
# velocity), shows where per-show drift learning could activate
# (≥5 reviewed unlocks _persist_detection_learning_by_show).

LEARNING_MIN_SAMPLES_FOR_DRIFT = 5  # mirrors the drift-learning gate


def _label_yield_score(uuid):
    """Information-gain proxy for picking which unreviewed recording
    is most worth the user's review time. Returns a non-negative
    score; higher = more informative. Two components:

      1. Sum of per-frame model uncertainty from head.uncertain.txt
         (frames where train-head's active-learning surface flagged
         the head as least confident). Recordings the model is
         genuinely confused about score high.
      2. Tiny recency tiebreaker — newer recordings preferred when
         the uncertainty signal is identical (fresher schedule
         patterns are usually more relevant).

    Returns 0 for recordings the active-learning step never saw
    (= bootstrap recordings without cached features). Those still
    get reviewed eventually, just not first."""
    items = _uncertain_for_recording(uuid)
    if not items:
        return 0.0
    # Each entry's uncertainty: 1 at p=0.5, 0 at p=0/1.
    score = sum(1.0 - 2.0 * abs(it["p"] - 0.5) for it in items)
    # Recency: minute granularity is enough to break ties.
    try:
        mtime = (HLS_DIR / f"_rec_{uuid}" / "index.m3u8").stat().st_mtime
        score += mtime / 1e12  # tiny epsilon, only breaks ties
    except Exception:
        pass
    return score


def _autorec_titles() -> set:
    """Set of titles currently covered by an autorec rule. Used by
    the singleton/movie heuristic — if a title has only 1 corpus
    sample BUT user explicitly set up an autorec for it, treat it
    as series-in-the-making, not a one-off film."""
    out = set()
    try:
        d = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/autorec/grid?limit=500",
            timeout=5).read().decode("utf-8", errors="replace"))
        for e in d.get("entries", []):
            t = (e.get("title") or "")
            # autorec stores title-regex like ^Show\\ Name$. Strip
            # the anchors + un-escape backslashes for naive equality.
            if t.startswith("^") and t.endswith("$"):
                t = t[1:-1].replace("\\", "")
            if t:
                out.add(t)
    except Exception:
        pass
    return out


def _user_grouped_titles() -> set:
    """All show titles that the user has manually placed into a
    franchise group via /recordings → Bearbeiten → Neue Gruppe.
    Strong "this is a film series" signal — user wouldn't manually
    bundle Tatort episodes that way."""
    out = set()
    for uuids in _load_user_groups().values():
        for u in uuids:
            d = HLS_DIR / f"_rec_{u}"
            t = _show_title_for_rec(d) or ""
            if t:
                out.add(t)
    return out


def _is_singleton_title(title: str, show_counts: dict,
                        autorec_titles: set,
                        user_group_titles: set = None) -> bool:
    """Is this title a one-off (= movie/special) we shouldn't chase
    for N≥5 stat-floor or auto-schedule N more episodes for?

    Three signals (any True = singleton):
      1. TMDB-fetched `kind` field == "movie" (authoritative)
      2. Title is part of a user-defined franchise group
         (= explicit "this is a movie franchise" act)
      3. Fallback: ≤1 corpus sample AND no autorec rule
         (catches pre-cache movies; false-positive on un-autorec'd
         pilots — accepted cost)

    Counter-signal: TMDB kind == "tv" overrides #2/#3."""
    with _epg_meta_lock:
        meta = _epg_meta.get(_normalize_title(title)) or {}
    kind = meta.get("kind")
    if kind == "movie":
        return True
    if kind == "tv":
        return False
    if user_group_titles and title in user_group_titles:
        return True
    return (show_counts.get(title, 0) <= 1
            and title not in autorec_titles)


def _show_review_counts() -> dict:
    """show_title → number of recordings on disk that user has reviewed
    (= ads_user.json with reviewed_at). Source of truth for the
    auto-scheduler's "how much do we have for this show?" decision."""
    out = {}
    for d in HLS_DIR.glob("_rec_*"):
        if not d.is_dir():
            continue
        show = _show_title_for_rec(d) or ""
        if not show:
            continue
        user_p = d / "ads_user.json"
        if not user_p.is_file():
            continue
        try:
            raw = json.loads(user_p.read_text())
            if isinstance(raw, dict) and raw.get("reviewed_at"):
                out[show] = out.get(show, 0) + 1
        except Exception:
            pass
    return out


def _read_auto_schedule_log(max_age_s: int = 7 * 86400) -> list:
    """Return recent auto-schedule entries, newest first. Each entry is
    one JSON object as written by _auto_schedule_run."""
    if not AUTO_SCHED_LOG.is_file():
        return []
    cutoff = time.time() - max_age_s
    out = []
    try:
        for ln in AUTO_SCHED_LOG.read_text().splitlines():
            if not ln.strip():
                continue
            try:
                e = json.loads(ln)
                if e.get("ts", 0) >= cutoff:
                    out.append(e)
            except Exception:
                pass
    except Exception:
        return []
    out.sort(key=lambda e: -e.get("ts", 0))
    return out


def _count_active_auto_scheduled() -> int:
    """How many auto-scheduled DVR entries are still in the queue
    (= scheduled or recording, not yet completed/cancelled). Cross-
    references the log against tvh's current grid via dvr_uuid."""
    log_entries = _read_auto_schedule_log(max_age_s=14 * 86400)
    log_uuids = {e.get("dvr_uuid") for e in log_entries
                 if e.get("ok") and e.get("dvr_uuid")}
    if not log_uuids:
        return 0
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000",
            timeout=10).read())
    except Exception:
        return 0
    active = 0
    for e in data.get("entries", []):
        if (e.get("uuid") in log_uuids
                and e.get("sched_status") in ("scheduled", "recording")):
            active += 1
    return active


def _auto_schedule_run(max_n: int = AUTO_SCHED_MAX_PER_DAY,
                        dry_run: bool = False) -> dict:
    """Pick the most-needed shows from EPG over the next 7 days and
    auto-schedule them via tvh's create_by_event. Returns a structured
    result for both the daily loop and the manual trigger endpoint.

    Scoring is intentionally simple for v1: priority = (5 - N_reviewed)
    capped at 0; ties broken by earlier start time. Skip shows already
    well-covered (N≥5), already in the active queue, or duplicates of
    something we just scheduled this run."""
    if AUTO_SCHED_PAUSE.exists() and not dry_run:
        return {"ok": True, "paused": True, "scheduled": [],
                "skipped_reason": "auto-scheduler is paused"}
    show_n = _show_review_counts()
    autorec_t = _autorec_titles()  # for the singleton/movie skip below
    ug_t = _user_grouped_titles()
    # Channel filter — only schedule from channels the user has ≥1
    # reviewed recording on. Otherwise the scheduler picks "Frühshoppen
    # on sonnenklar.TV" because nobody's ever recorded a shopping
    # channel, score=5. Implicit interest signal: a channel without
    # any training data → user doesn't care about that channel.
    interested_channels = set()
    for d in HLS_DIR.glob("_rec_*"):
        if (d / "ads_user.json").is_file():
            slug = _rec_channel_slug(d.name[5:]) or ""
            if slug:
                interested_channels.add(slug)
    # Failure-mode rate per show + channel — feeds the score boost so
    # we preferentially fill gaps where the model currently struggles.
    # _aggregate_failure_modes_cached returns (per_show, per_channel)
    # both as {key: {"total": N, "modes": {mode: count}}}.
    try:
        per_show_fm, per_channel_fm = _aggregate_failure_modes_cached()
    except Exception:
        per_show_fm, per_channel_fm = {}, {}
    def _fail_rate(entry):
        if not entry or entry.get("total", 0) == 0:
            return 0.0
        non_good = sum(v for k, v in entry.get("modes", {}).items()
                       if k != "good")
        return non_good / entry["total"]
    # Per-show LATEST IoU from the post-deploy snapshots — shows with
    # weak production-IoU benefit most from another labelled sample.
    # File is JSONL: one entry per (show, ts).
    show_iou = {}
    iou_path = HLS_DIR / ".tvd-models" / "head.per-show-iou.jsonl"
    if iou_path.is_file():
        try:
            for ln in iou_path.read_text().splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    r = json.loads(ln)
                    s = r.get("show")
                    iou = r.get("iou_mean")
                    ts = r.get("ts", "")
                    if not s or iou is None:
                        continue
                    cur = show_iou.get(s)
                    if not cur or ts > cur[0]:
                        show_iou[s] = (ts, float(iou))
                except Exception:
                    continue
        except Exception:
            pass
    # Per-show drift presence — shows with NO drift mark yet benefit
    # from a fresh recording where the user might mark it (= teaches
    # us the EPG-Drift, then auto-padding for next time).
    drift_known = set()
    drift_path = HLS_DIR / ".tvd-models" / "per-show-drift.json"
    if drift_path.is_file():
        try:
            for s in json.loads(drift_path.read_text()).keys():
                drift_known.add(s)
        except Exception:
            pass
    # Empfehlungen-signals (= same data the /learning recommendations
    # bucket uses). Auto-scheduler previously only counted RECORDINGS
    # per show (show_n) but not BLOCKS — a show with 5 recordings × 1
    # block could still need more for the show-prior gate. Plus the
    # test-set confusion (= missed+extra blocks per test recording)
    # was empfehlungen-only. Now both signals feed scheduler scoring.
    chan_user_blocks = {}
    show_user_blocks = {}
    for d in HLS_DIR.glob("_rec_*"):
        au = d / "ads_user.json"
        if not au.is_file():
            continue
        try:
            raw = json.loads(au.read_text())
        except Exception:
            continue
        blocks = raw if isinstance(raw, list) else (raw.get("ads") or [])
        n_blocks = len([b for b in blocks if b and len(b) >= 2])
        slug = _rec_channel_slug(d.name[5:]) or ""
        show = _show_title_for_rec(d)
        if slug:
            chan_user_blocks[slug] = chan_user_blocks.get(slug, 0) + n_blocks
        if show:
            show_user_blocks[show] = show_user_blocks.get(show, 0) + n_blocks
    # Test-set confusion: shows with missed+extra blocks need more
    # data for the test metric specifically. Parse head.confusion.txt
    # the same way /learning page does.
    confusion_miss_extra = {}
    conf_path = HLS_DIR / ".tvd-models" / "head.confusion.txt"
    if conf_path.is_file():
        try:
            cur_title = None
            for ln in conf_path.read_text().splitlines():
                if ln.startswith("## "):
                    cur_title = ln[3:].strip()
                elif cur_title and ln.startswith("  blocks:"):
                    m = re.search(r"missed=(\d+)\s+extra=(\d+)", ln)
                    if m:
                        confusion_miss_extra[cur_title] = (
                            int(m.group(1)) + int(m.group(2)))
        except Exception:
            pass
    active = _count_active_auto_scheduled()
    slots = min(max_n, max(0, AUTO_SCHED_MAX_ACTIVE - active))
    if slots <= 0:
        return {"ok": True, "scheduled": [], "skipped_reason":
                f"hit AUTO_SCHED_MAX_ACTIVE={AUTO_SCHED_MAX_ACTIVE} "
                f"(active={active})"}
    # Walk EPG for the next 7 days. tvh's grid is paginated; one big
    # call covers all channels we receive.
    now_ts = int(time.time())
    horizon = now_ts + 7 * 86400
    try:
        # tvh occasionally returns event titles with stray non-UTF-8
        # bytes (= broadcaster encoded with latin-1 in a UTF-8 EPG
        # field). Decode with errors=replace so one bad title doesn't
        # tank the whole scheduler run.
        raw = urllib.request.urlopen(
            f"{dvr_base()}/api/epg/events/grid?limit=1500",
            timeout=15).read()
        data = json.loads(raw.decode("utf-8", errors="replace"))
    except Exception as e:
        return {"ok": False, "error": f"epg fetch: {e}"}
    # Existing scheduled events to dedup against (= avoid scheduling
    # an EPG event that's already on the calendar manually).
    try:
        sched = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000",
            timeout=10).read())
        existing_eids = {e.get("broadcast")
                         for e in sched.get("entries", [])
                         if e.get("sched_status") in ("scheduled", "recording")
                         and e.get("broadcast")}
    except Exception:
        existing_eids = set()
    # Score each upcoming event
    candidates = []
    seen_titles = set()  # dedup within this run — avoid scheduling
                         # 5 episodes of the same show in one go
    # Length cap: skip events longer than 2h. Long films take huge
    # disk (~3-4 GB at 25 Mbit/s DVB-C) for diminishing training value
    # — the model has already seen the same logo/show patterns thousands
    # of times by frame 30k. 7200s catches all sitcoms (25-50min),
    # reality (45-90min), most movies (90-120min); excludes 3h+ epics.
    MAX_LEN_S = 7200
    for ev in data.get("entries", []):
        title = (ev.get("title") or "").strip()
        if not title:
            continue
        start = ev.get("start", 0)
        stop = ev.get("stop", 0)
        if start <= now_ts or start > horizon:
            continue
        if stop > start and (stop - start) > MAX_LEN_S:
            continue
        if ev.get("eventId") in existing_eids:
            continue
        # Channel-of-interest gate
        ch_slug = slugify(ev.get("channelName") or "")
        if interested_channels and ch_slug not in interested_channels:
            continue
        n_rev = show_n.get(title, 0)
        if n_rev >= 5:
            continue  # already enough labelled examples
        # Singleton/movie skip — chasing N=5 for a one-off film is
        # impossible (broadcaster won't repeat 5×) and just burns
        # disk budget on titles we'll never have a training cluster
        # for. Heuristic: ≤1 corpus sample AND no autorec rule.
        if _is_singleton_title(title, show_n, autorec_t, ug_t):
            continue
        # Base priority by sample count gap
        priority = max(0, 5 - n_rev)
        if priority == 0:
            continue
        # Boost: high failure-rate on this show OR on this channel = the
        # current model struggles here. More data of those = bigger
        # marginal IoU lift per labelled recording. Cap at +2 so the
        # base sample-gap signal still dominates ordering.
        if _fail_rate(per_show_fm.get(title)) > 0.4:
            priority += 1
        if _fail_rate(per_channel_fm.get(ch_slug)) > 0.4:
            priority += 1
        # Phase-6F refinements:
        # - Low per-show production IoU = model already failing here in
        #   prod = next labelled sample is high-leverage. +2 if < 0.5,
        #   +1 if < 0.7. Doesn't fire when no IoU snapshot exists yet.
        # - Unknown EPG-drift = recording it gives the user a chance to
        #   mark show-start, which then permanently improves padding for
        #   that show. +1 only if we have ≥1 reviewed sample (= we know
        #   the show actually exists in our corpus).
        latest = show_iou.get(title)
        if latest:
            iou = latest[1]
            if iou < 0.5:
                priority += 2
            elif iou < 0.7:
                priority += 1
        if n_rev >= 1 and title not in drift_known:
            priority += 1
        # Empfehlungen-derived boosts (= same bucket the /learning page
        # surfaces as "where does labelling help most?"). Three signals:
        #   - per-channel block-prior gap: <5 blocks → channel still
        #     uses fleet-wide prior, more blocks unlock channel-specific
        #     prior. +1 priority. Marginal (channels with literally 0
        #     reviews already get filtered via interested_channels).
        #   - per-show block-prior gap: <5 blocks → same as above for
        #     show-specific prior. +2 priority since show-specific has
        #     larger downstream IoU lift than channel-specific.
        #   - test-set confusion: missed+extra blocks in test (= direct
        #     test-metric impact). +2 for >=3 missed+extra. Sub-signal
        #     of weak_shows; the IoU-boost above captures the symptom
        #     but missed/extra captures the WHICH-FAILURE-MODE detail.
        ch_blocks = chan_user_blocks.get(ch_slug, 0)
        if 0 < ch_blocks < 5:
            priority += 1
        sh_blocks = show_user_blocks.get(title, 0)
        if 0 < sh_blocks < 5:
            priority += 2
        if confusion_miss_extra.get(title, 0) >= 3:
            priority += 2
        candidates.append((priority, start, ev, n_rev))
    # Sort: higher priority first, then earlier start (= sooner data).
    # The picking loop below adds a time-spread constraint on top.
    candidates.sort(key=lambda c: (-c[0], c[1]))
    # Pick top per dedup-by-title PLUS time-of-day spread.
    # SPREAD_S=3600 means subsequent picks must start ≥1 h apart from
    # any earlier pick this run. Avoids 3 events at 12:55 burning all
    # tuners on the same minute (= what v1 did).
    SPREAD_S = 3600
    picked_starts = []
    picked = []
    for prio, start, ev, n_rev in candidates:
        title = ev["title"]
        if title in seen_titles:
            continue
        if any(abs(start - ps) < SPREAD_S for ps in picked_starts):
            continue
        if len(picked) >= slots:
            break
        seen_titles.add(title)
        picked_starts.append(start)
        picked.append((prio, start, ev, n_rev))
    results = []
    for prio, start, ev, n_rev in picked:
        ch_slug = slugify(ev.get("channelName") or "")
        log_entry = {
            "ts": int(time.time()),
            "title": ev.get("title"),
            "channel": ev.get("channelName"),
            "start": start,
            "stop": ev.get("stop", 0),
            "event_id": ev.get("eventId"),
            "score": prio,
            "n_reviewed_before": n_rev,
            "show_fail_rate": round(
                _fail_rate(per_show_fm.get(ev.get("title"))), 2),
            "channel_fail_rate": round(
                _fail_rate(per_channel_fm.get(ch_slug)), 2),
            "ok": False, "dvr_uuid": None, "error": None,
            "dry_run": dry_run,
        }
        if dry_run:
            log_entry["ok"] = True
            log_entry["error"] = "dry_run"
        else:
            try:
                body = urllib.parse.urlencode({
                    "event_id": ev["eventId"],
                    "config_uuid": "",
                    # 5/10 min pre/post padding so broadcasters running
                    # over EPG-listed end don't truncate the recording.
                    # Same value as record_series autorec defaults.
                    "start_extra": 5,
                    "stop_extra": 10,
                }).encode()
                req = urllib.request.Request(
                    f"{dvr_base()}/api/dvr/entry/create_by_event",
                    data=body, method="POST")
                res = urllib.request.urlopen(req, timeout=10).read().decode()
                rd = json.loads(res) if res else {}
                u = rd.get("uuid")
                if isinstance(u, list):
                    u = u[0] if u else None
                log_entry["ok"] = bool(u)
                log_entry["dvr_uuid"] = u
            except Exception as e:
                log_entry["error"] = str(e)
        results.append(log_entry)
    # Append non-dry-run entries to the log
    if not dry_run and results:
        try:
            AUTO_SCHED_LOG.parent.mkdir(parents=True, exist_ok=True)
            with open(AUTO_SCHED_LOG, "a") as f:
                for r in results:
                    f.write(json.dumps(r) + "\n")
        except Exception as e:
            print(f"[auto-sched] log write err: {e}", flush=True)
    return {"ok": True, "scheduled": [r for r in results if r["ok"]],
            "errors": [r for r in results if not r["ok"]],
            "candidates_seen": len(candidates),
            "active_before": active}


def _auto_schedule_loop():
    """Daily 04:30 local — fires _auto_schedule_run() once. Sleeps to
    next-04:30 in between. Default-paused (= AUTO_SCHED_PAUSE marker
    exists on first install); user un-pauses when ready."""
    while True:
        now = time.localtime()
        secs = now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec
        target = 4 * 3600 + 30 * 60
        delay = (target - secs) % 86400
        if delay < 60:
            delay += 86400  # already past today's slot, wait for tomorrow
        time.sleep(delay)
        try:
            r = _auto_schedule_run()
            print(f"[auto-sched] daily fire: {len(r.get('scheduled', []))} "
                  f"scheduled, {len(r.get('errors', []))} errors, "
                  f"paused={r.get('paused', False)}", flush=True)
        except Exception as e:
            print(f"[auto-sched] error: {e}", flush=True)


# ============================================================
# Adaptive end-padding
# Fixed stop_extra=10 still cut off recordings whose broadcaster
# overran the EPG end (e.g. Davina & Shania ran 13 min long, lost
# the cliffhanger). This loop watches every in-progress recording
# in its final 90 s and, if the live ad-scanner sees an active ad
# block on that channel right now, extends the recording by another
# 5 min. Capped at 3 extensions per uuid (= +15 min total) so a
# stuck/silent stream can't keep extending forever.
# ============================================================
ADAPTIVE_PADDING_FILE     = HLS_DIR / ".adaptive_padding.json"
ADAPTIVE_PADDING_MAX_EXT  = 3
ADAPTIVE_PADDING_STEP_MIN = 5
ADAPTIVE_PADDING_WINDOW_S = 90
_adaptive_padding_lock    = threading.Lock()
_adaptive_padding_state   = {}


def _adaptive_padding_load():
    global _adaptive_padding_state
    if not ADAPTIVE_PADDING_FILE.exists():
        return
    try:
        _adaptive_padding_state = json.loads(
            ADAPTIVE_PADDING_FILE.read_text())
    except Exception as e:
        print(f"[adaptive-pad] load err: {e}", flush=True)
        _adaptive_padding_state = {}


def _adaptive_padding_save():
    try:
        ADAPTIVE_PADDING_FILE.write_text(
            json.dumps(_adaptive_padding_state))
    except Exception as e:
        print(f"[adaptive-pad] save err: {e}", flush=True)


def _adaptive_padding_extend_tvh(uuid_str, current_stop_extra_min):
    """Bump tvh's stop_extra (in minutes) for one DVR entry by
    ADAPTIVE_PADDING_STEP_MIN. Returns the new value."""
    new_extra = int(current_stop_extra_min) + ADAPTIVE_PADDING_STEP_MIN
    body = urllib.parse.urlencode({
        "node": json.dumps({"uuid": uuid_str, "stop_extra": new_extra})
    }).encode()
    req = urllib.request.Request(
        f"{dvr_base()}/api/idnode/save",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    urllib.request.urlopen(req, timeout=10).read()
    return new_extra


def _adaptive_padding_check_one(entry, now=None):
    """Decide+apply for one in-progress entry. Returns (extended,
    reason). Throttles to one extension per 60 s per uuid, caps at
    ADAPTIVE_PADDING_MAX_EXT total."""
    if now is None:
        now = time.time()
    uuid_str = entry.get("uuid")
    if not uuid_str:
        return (False, "no uuid")
    # tvh's `stop_real` is the actual scheduled stop wall-clock
    # (= stop + stop_extra*60). When we increase stop_extra,
    # stop_real updates accordingly on the next grid query.
    stop_real = int(entry.get("stop_real") or 0)
    if stop_real <= 0:
        return (False, "no stop_real")
    until_end = stop_real - now
    if until_end < -10 or until_end > ADAPTIVE_PADDING_WINDOW_S:
        return (False, f"not in window ({until_end:.0f}s)")
    with _adaptive_padding_lock:
        st = dict(_adaptive_padding_state.get(uuid_str) or {})
    if st.get("count", 0) >= ADAPTIVE_PADDING_MAX_EXT:
        return (False, "cap reached")
    if (now - (st.get("last_extend_ts") or 0)) < 60:
        return (False, "throttled")
    slug = slugify(entry.get("channelname") or "")
    if not slug:
        return (False, "no channel slug")
    ads = _live_ads_payload(slug).get("ads", [])
    active = None
    for blk in ads:
        if not (isinstance(blk, (list, tuple)) and len(blk) >= 2):
            continue
        s, e = float(blk[0]), float(blk[1])
        # +30 s grace: if the block ends 25 s before scheduled stop,
        # it's still effectively "running into" the recording boundary
        # and the next one is plausibly imminent.
        if s <= now <= e + 30:
            active = (s, e)
            break
    if not active:
        return (False, "no active ad block")
    stop_extra_min = int(entry.get("stop_extra") or 0)
    try:
        new_extra = _adaptive_padding_extend_tvh(uuid_str, stop_extra_min)
    except Exception as e:
        return (False, f"tvh save err: {e}")
    title = (entry.get("disp_title") or "")[:40]
    with _adaptive_padding_lock:
        prev = _adaptive_padding_state.get(uuid_str) or {}
        _adaptive_padding_state[uuid_str] = {
            "count": int(prev.get("count", 0)) + 1,
            "last_extend_ts": now,
            "title": title,
            "channel": slug,
            "stop_extra_min": new_extra,
        }
        _adaptive_padding_save()
    print(f"[adaptive-pad] {uuid_str[:8]} '{title}' @{slug}: "
          f"stop_extra {stop_extra_min}->{new_extra} min "
          f"(active block ends in {int(active[1]-now)}s)", flush=True)
    return (True, f"extended to {new_extra} min")


def _adaptive_padding_loop():
    """Every 60 s: scan in-progress recordings near their scheduled
    end, extend if a live ad block is active on the channel."""
    time.sleep(20)  # let live-ads cache populate after boot
    while True:
        try:
            data = json.loads(urllib.request.urlopen(
                f"{dvr_base()}/api/dvr/entry/grid?limit=200"
                f"&sort=start&dir=DESC", timeout=8).read())
            now = time.time()
            for e in data.get("entries", []):
                if e.get("sched_status") != "recording":
                    continue
                try:
                    _adaptive_padding_check_one(e, now=now)
                except Exception as ex:
                    print(f"[adaptive-pad] check err uuid={e.get('uuid','?')[:8]}: {ex}",
                          flush=True)
        except Exception as e:
            print(f"[adaptive-pad] loop err: {e}", flush=True)
        time.sleep(60)




def _aggregate_show_gaps():
    """Returns a sorted list of (show, slug, n_total, n_reviewed,
    n_unreviewed_playable, suggestion) tuples for shows that would
    benefit from more labelling. Only shows with at least one
    unreviewed playable recording are included — labelling something
    you can't watch isn't actionable.

    Suggestions are tiered:
      "drift_unlock"  — N reviewed < threshold but ≥1 unreviewed exists
                         → labelling 1-2 more activates per-show drift
      "stat_floor"    — N reviewed = 1 (single-test-set sample, IoU is
                         noise; getting to 3+ stabilises metrics)
      "velocity"      — N reviewed ≥ threshold but ≥5 unreviewed sit
                         around (= cheap source of fresh training data)

    For shows with multiple unreviewed siblings, the action-button
    UUID is the one with highest label-yield score (= maximum model
    uncertainty across its frames) — labelling THAT one gives the
    biggest model-update per minute of user time.
    """
    by_show = {}
    for d in HLS_DIR.glob("_rec_*"):
        uuid = d.name[5:]
        show = _show_title_for_rec(d) or ""
        if not show:
            continue
        slug = _rec_channel_slug(uuid) or ""
        playable = (d / "index.m3u8").exists()
        user_p = d / "ads_user.json"
        reviewed = False
        if user_p.exists():
            try:
                raw = json.loads(user_p.read_text())
                if isinstance(raw, dict) and raw.get("reviewed_at"):
                    reviewed = True
            except Exception:
                pass
        entry = by_show.setdefault(show, {
            "slug": slug, "n_total": 0, "n_reviewed": 0,
            "n_unreviewed_playable": 0, "uuids_unreviewed": []})
        entry["n_total"] += 1
        if reviewed:
            entry["n_reviewed"] += 1
        elif playable:
            entry["n_unreviewed_playable"] += 1
            entry["uuids_unreviewed"].append(uuid)
    # Sort each show's unreviewed UUIDs by label-yield score so the
    # action button targets the most informative one.
    for entry in by_show.values():
        entry["uuids_unreviewed"].sort(
            key=_label_yield_score, reverse=True)

    # Singleton/movie heuristic: don't nag the user to review more
    # of a one-off film. show_n.get() uses reviewed counts; for
    # _is_singleton_title we need the per-show TOTAL (reviewed +
    # unreviewed), so feed n_total in via the dict.
    show_total_n = {s: e["n_total"] for s, e in by_show.items()}
    autorec_t = _autorec_titles()
    ug_t = _user_grouped_titles()
    out = []
    for show, e in by_show.items():
        if e["n_unreviewed_playable"] == 0:
            continue  # nothing the user can act on
        n_rev = e["n_reviewed"]
        if n_rev == 0:
            sugg = "stat_floor"
            priority = 0  # unseen → highest
        elif n_rev == 1:
            sugg = "stat_floor"
            priority = 1
        elif n_rev < LEARNING_MIN_SAMPLES_FOR_DRIFT:
            sugg = "drift_unlock"
            priority = 2
        elif e["n_unreviewed_playable"] >= 5:
            sugg = "velocity"
            priority = 3
        else:
            continue  # well-labelled, no slack worth surfacing
        # Skip stat_floor for singletons — chasing N=3 of a movie
        # is futile (= broadcaster won't repeat 5×). drift_unlock
        # already requires n_rev≥2 so singletons can't reach it.
        if sugg == "stat_floor" and _is_singleton_title(
                show, show_total_n, autorec_t, ug_t):
            continue
        out.append((show, e["slug"], e["n_total"], n_rev,
                    e["n_unreviewed_playable"], sugg, priority,
                    e["uuids_unreviewed"]))
    out.sort(key=lambda r: (r[6], -r[4]))  # priority asc, then slack desc
    return out


def _render_show_gaps(gaps):
    """Render the show-gap suggestions as a table with action links
    pointing to the first unreviewed UUID per show (= 'one-click
    onboarding' for the user)."""
    if not gaps:
        return ("<p class='muted'>Keine offenen Label-Lücken — alle "
                "Shows haben genug User-Reviews für stabile Metriken "
                "und Per-Show-Drift-Learning.</p>")
    sugg_label = {
        "stat_floor":  ("🆕", "#e74c3c",
                        "Statistische Stabilität: N=1 ist Rauschen, "
                        "≥3 Reviews machen den Test-IoU verlässlich"),
        "drift_unlock":("🔓", "#f39c12",
                        f"Per-Show-Drift-Learning aktiviert sich bei "
                        f"≥{LEARNING_MIN_SAMPLES_FOR_DRIFT} reviewed Folgen — "
                        f"dann gilt show-spezifische start_lag/end_lag-Korrektur"),
        "velocity":    ("🚀", "#27ae60",
                        "Show ist gut gelabelt. Diese unbearbeiteten "
                        "Folgen wären schnelle Trainings-Daten via "
                        "Smart-Merge-Auto-Labels"),
    }
    out = ["<p class='muted'>Aufnahmen pro Show: wo ein paar weitere "
           "Reviews den größten Hebel hätten. Klick auf eine Aufnahme "
           "öffnet sie direkt im Player. Bei Shows mit mehreren "
           "ungeprüften Folgen wird die <b>informativste</b> als "
           "Top-Pick angeboten — die Aufnahme wo das Modell die "
           "höchste Unsicherheit zeigt (= eine Review dort bringt "
           "dem Modell am meisten Verbesserung pro Minute deiner "
           "Zeit).</p>"]
    # Horizontal scroll wrapper — on mobile the table is wider than
    # the viewport (show titles + action button); without this the
    # outer section gets clipped instead of letting the table scroll
    # within its own bounds.
    out.append("<div style='overflow-x:auto;-webkit-overflow-scrolling:touch'>")
    out.append("<table style='width:100%;min-width:560px'><tr>"
               "<th style='text-align:left'>Show</th>"
               "<th>Sender</th><th>Total</th><th>Geprüft</th>"
               "<th>Offen ⏵</th><th>Empfehlung</th><th>Action</th></tr>")
    for (show, slug, n_total, n_rev, n_open, sugg, _prio,
         uuids_open) in gaps:
        emoji, color, hint = sugg_label[sugg]
        first_uuid = uuids_open[0] if uuids_open else ""
        # Top-Pick badge when there are siblings AND we have a label-
        # yield signal (= the model surfaced uncertain frames for the
        # picked UUID). Without that signal the order is just by
        # recency, so don't oversell it as a "smart pick".
        top_pick = (len(uuids_open) > 1 and first_uuid
                    and _label_yield_score(first_uuid) > 0)
        label = "🎯 Top-Pick prüfen" if top_pick else "➜ prüfen"
        action_title = (
            "Modell zeigt hier die höchste Unsicherheit "
            f"({len(uuids_open)} ungeprüfte Folgen, beste Wahl voraus)"
            if top_pick else
            f"Eine von {len(uuids_open)} ungeprüften Folge(n)")
        action = (f"<a href='{HOST_URL}/recording/{first_uuid}' "
                  f"title='{action_title}' "
                  f"class='pill' style='background:{color};color:#fff;"
                  f"text-decoration:none;padding:3px 8px;font-size:.85em;"
                  f"white-space:nowrap'>"
                  f"{label}</a>"
                  if first_uuid else "")
        out.append(f"<tr><td><b>{show}</b></td>"
                   f"<td>{slug}</td><td>{n_total}</td>"
                   f"<td>{n_rev}</td>"
                   f"<td><b style='color:{color}'>{n_open}</b></td>"
                   f"<td title='{hint}' style='white-space:nowrap'>"
                   f"{emoji} {sugg}</td>"
                   f"<td style='white-space:nowrap'>{action}</td></tr>")
    out.append("</table></div>")
    return "\n".join(out)


@app.route("/learning")
def learning_page():
    """Single-page dashboard for the autonomous training loop.
    Reads head.history.json + head.uncertain.txt + head.confusion.txt +
    .detection_learning.json + .block_length_prior.json — all
    files written by the nightly retrain or the gateway's own
    feedback-stats refresh."""
    models_dir = HLS_DIR / ".tvd-models"

    # 1. History
    history = []
    try:
        history = json.loads((models_dir / "head.history.json").read_text())
    except Exception:
        pass

    # 2. Per-channel learning + prior
    learning = {}
    try:
        learning = json.loads((HLS_DIR / ".detection_learning.json").read_text())
    except Exception:
        pass
    priors = {}
    try:
        priors = json.loads((HLS_DIR / ".block_length_prior.json").read_text())
    except Exception:
        pass
    channel_cfg = {}
    try:
        channel_cfg = json.loads(
            (HLS_DIR / ".channel-config.json").read_text()).get("channels", {})
    except Exception:
        pass
    all_slugs = sorted(set(list(learning) + list(priors) + list(channel_cfg)))

    # 3. Active learning queue. Filter out:
    #    - frames in already-reviewed recordings (user explicitly hit
    #      "Geprüft" → all remaining unsicheren are now confirmed_show)
    #    - frames inside an existing user ad-block (was a known-ad,
    #      uncertainty here is intra-block confusion not actionable)
    uncertain = []
    reviewed_skipped = 0
    try:
        for ln in (models_dir / "head.uncertain.txt").read_text().splitlines():
            if not ln or ln.startswith("#"):
                continue
            parts = ln.split("\t")
            if len(parts) < 4:
                continue
            uuid = parts[0].strip()
            t = float(parts[1])
            # Skip rows whose recording dir no longer exists (user
            # deleted via tvheadend) — head.uncertain.txt is stale
            # until the next nightly retrain re-writes it.
            if not (HLS_DIR / f"_rec_{uuid}").exists():
                continue
            user_cache = HLS_DIR / f"_rec_{uuid}" / "ads_user.json"
            if user_cache.exists():
                try:
                    raw = json.loads(user_cache.read_text())
                except Exception:
                    raw = None
                if isinstance(raw, dict):
                    if raw.get("reviewed_at"):
                        reviewed_skipped += 1
                        continue
                    in_ad = any(s <= t <= e
                                for s, e in raw.get("ads", []) or [])
                    if in_ad:
                        continue
            # source column is optional (5th field, added with
            # --with-minute-prior); old runs without it still parse.
            src = parts[4].strip() if len(parts) > 4 else "unc"
            uncertain.append({
                "uuid": uuid, "t": t,
                "p": float(parts[2]), "title": parts[3].strip(),
                "src": src,
            })
    except Exception:
        pass
    # Sort: divergence-source first (more actionable — model is
    # confidently wrong about a wall-clock-anomalous frame), then
    # uncertainty-source by closeness-to-0.5.
    uncertain.sort(key=lambda u: (0 if u.get("src") in ("div", "both") else 1,
                                    abs(u["p"] - 0.5)))

    # 4. Confusion (last summary block per show)
    confusion = []
    try:
        cur = None
        for ln in (models_dir / "head.confusion.txt").read_text().splitlines():
            if ln.startswith("## "):
                if cur:
                    confusion.append(cur)
                cur = {"title": ln[3:].strip(), "blocks": "", "errors": ""}
            elif cur and ln.startswith("  blocks:"):
                cur["blocks"] = ln.strip().replace("blocks:  ", "")
            elif cur and ln.startswith("  errors:"):
                cur["errors"] = ln.strip().replace("errors:  ", "")
            elif cur and ln.startswith("  frames:"):
                cur["frames"] = ln.strip().replace("frames:  ", "")
        if cur:
            confusion.append(cur)
    except Exception:
        pass

    health = _learning_health()
    ok_color = {"ok": "#27ae60", "warn": "#f39c12", "fail": "#e74c3c"}[health["status"]]

    # ── Build HTML ───────────────────────────────────────────────
    out = []
    out.append("<!doctype html><html lang=de><head><meta charset=utf-8>")
    out.append("<title>tv-detect Lernfortschritt</title>")
    out.append("<meta name=viewport content='width=device-width,initial-scale=1'>")
    # Browser hint: page supports both light + dark modes. Without
    # this, form controls + scrollbars stay light on dark pages.
    out.append("<meta name=color-scheme content='light dark'>")
    out.append("<style>")
    # CSS-variable theme matching BASE_CSS used elsewhere — tagsüber
    # hell (prefers-color-scheme: light), abends dunkel (dark). Was
    # previously hardcoded #1c1c1c forever.
    out.append(":root{--bg:#fafafa;--fg:#222;--muted:#777;"
               "--border:#ddd;--stripe:#f0f0f0;--link:#0366d6;"
               "--code-bg:#f3f3f3}")
    out.append("@media (prefers-color-scheme:dark){:root{"
               "--bg:#1a1a1a;--fg:#e4e4e4;--muted:#999;--border:#333;"
               "--stripe:#242424;--link:#79b8ff;--code-bg:#2a2a2a}}")
    out.append("body{font-family:-apple-system,sans-serif;"
               "background:var(--bg);color:var(--fg);"
               "margin:0;padding:18px;max-width:1200px;"
               "margin-left:auto;margin-right:auto}")
    out.append("h1{font-size:1.5em;margin:0 0 8px}"
               "h2{margin:24px 0 8px;font-size:1.1em;color:var(--fg);"
               "border-bottom:1px solid var(--border);padding-bottom:4px}")
    out.append(f".status{{padding:12px 14px;border-radius:6px;"
               f"background:{ok_color}22;border-left:4px solid {ok_color};"
               f"margin:8px 0 16px}}")
    out.append(".status .dot{display:inline-block;width:10px;height:10px;"
               "border-radius:5px;margin-right:8px;vertical-align:middle}")
    out.append("table{border-collapse:collapse;width:100%;font-size:.85em}")
    out.append("th,td{padding:5px 9px;text-align:left;"
               "border-bottom:1px solid var(--border)}")
    out.append("th{color:var(--muted);font-weight:600}")
    out.append("tr:hover{background:var(--stripe)}")
    out.append(".rej{color:#e74c3c}.dep{color:#27ae60}")
    out.append(".bar{display:inline-block;height:8px;background:#3498db;"
               "border-radius:2px;vertical-align:middle}")
    out.append(".muted{color:var(--muted)}")
    out.append("a{color:var(--link)}a:hover{text-decoration:underline}")
    out.append("nav{margin-bottom:14px}"
               "nav a{margin-right:14px;color:var(--muted)}")
    out.append("</style></head><body>")
    out.append(f"<nav><a href='{HOST_URL}/'>← Home</a> <a href='{HOST_URL}/recordings'>Aufnahmen</a></nav>")
    out.append("<h1>tv-detect Lernfortschritt</h1>")

    # Status banner
    out.append("<div class='status'>")
    out.append(f"<span class='dot' style='background:{ok_color}'></span>")
    if health["status"] == "ok":
        out.append(f"<b>Healthy</b> — letzter Deploy: {health['last_deploy']} "
                   f"(vor {health['last_age_h']} h)")
    elif health["status"] == "warn":
        out.append(f"<b>Achtung</b> — letzter Deploy vor {health['last_age_h']} h. "
                   f"Letzter Reject-Grund: {health['last_reject'] or '–'}")
    else:
        out.append(f"<b>Modell-Training stockt</b> — letzter erfolgreicher Deploy "
                   f"vor {health['last_age_h']} h. Reject-Grund: "
                   f"{health['last_reject'] or '–'}. Logs: ~/Library/Logs/tv-train-head.log")
    # B + C surface as additional rows inside the status banner
    bc = health.get("broken_channels", []) or []
    if bc:
        for entry in bc:
            kind_label = ("alle Detections leer (Logo-Template kaputt?)"
                          if entry["kind"] == "zero-blocks"
                          else "alle Detections > 60 % Werbung (Logo greift überall)")
            out.append(f"<br><b>⚠ {entry['slug']}:</b> letzte {entry['n']} "
                       f"Aufnahmen → {kind_label}")
    if health.get("trend_drift") is not None:
        d = health["trend_drift"]
        out.append(f"<br><b>📉 IoU-Drift</b> {d*100:+.1f}% vs Median der letzten "
                   f"7 deployed Runs — schleichende Regression?")
    cc = health.get("test_composition_changed")
    if cc:
        out.append(f"<br><b>ℹ Test-Set gewachsen</b>: "
                   f"{cc['from_n']}→{cc['to_n']} Recordings "
                   f"({cc['n_added']} neu) · IoU {cc['drift_pp']*100:+.1f}pp "
                   f"vs Median — nicht apples-to-apples, kein echter Drift")
    out.append("</div>")

    # Daemon status banner — Mac-side detect / HLS-remux / thumbs
    # daemon health, queue depth, and an estimated remaining time
    # for the current backlog. Same color-coded box style as the
    # model-status banner above.
    # Daemon poll cadence is 5s normally, but during a long detect
    # job (4-7 min wall) the poll loop pauses until the job finishes.
    # Generous thresholds so a mid-detect daemon shows GREEN with a
    # "busy" hint rather than warning.
    # Source of truth: the .daemon-last-poll file mtime — tv-recorder now
    # serves the {thumbs,detect,hls}-pending polls and touches this file on
    # every daemon poll (touchHeartbeat). Flask's in-memory _daemon_last_poll
    # only ever gets bumped by the few poll routes still on Flask (live-ads),
    # so reading it alone always showed "noch nie gepingt" post-migration.
    last_poll = _daemon_last_poll or 0
    try:
        last_poll = max(last_poll, (HLS_DIR / ".daemon-last-poll").stat().st_mtime)
    except OSError:
        pass
    daemon_age = int(time.time() - last_poll)
    if last_poll == 0:
        d_color = "#e74c3c"; d_label = "noch nie gepingt"
    elif daemon_age <= 30:
        d_color = "#27ae60"; d_label = f"aktiv (letzter Ping vor {daemon_age}s)"
    elif daemon_age <= 600:
        d_color = "#27ae60"; d_label = (f"aktiv, gerade in einem Detect-Job "
                                          f"(letzter Ping vor {daemon_age}s)")
    elif daemon_age <= 1800:
        d_color = "#f39c12"; d_label = (f"⚠ ungewöhnlich lange beschäftigt "
                                          f"({daemon_age//60} min ohne Ping — "
                                          f"sehr großer Detect-Job?)")
    else:
        d_color = "#e74c3c"; d_label = (f"✗ offline (kein Ping seit "
                                          f"{daemon_age//60} min)")
    # Pending counts — re-uses the same logic the daemon polls.
    # V2 splits detect into high (.detect-requested) + low
    # (.detect-requested-low); show both so background-backfill
    # progress is visible during the 1-2 days it takes to drain.
    n_detect = n_detect_low = n_thumbs = n_hls = 0
    def _pending_for(marker_glob):
        n = 0
        for marker in HLS_DIR.glob(marker_glob):
            if any(t.stat().st_size > 0
                   for t in marker.parent.glob("*.txt")
                   if not any(t.name.endswith(s) for s in
                              (".logo.txt", ".cskp.txt", ".tvd.txt", ".trained.logo.txt"))):
                continue
            n += 1
        return n
    try:
        n_detect     = _pending_for("_rec_*/.detect-requested")
        n_detect_low = _pending_for("_rec_*/.detect-requested-low")
        n_thumbs = sum(1 for _ in HLS_DIR.glob("_rec_*/thumbs/.requested"))
        n_hls = sum(1 for _ in HLS_DIR.glob("_rec_*/.hls-requested"))
    except Exception:
        pass
    # Recent detect timing → estimate ETA on the queue. The Mac daemon
    # runs DETECT_PARALLEL detect-jobs concurrently (default 3) and each
    # job now completes in ~3 min wallclock thanks to CoreML NN + ONNX
    # ECAPA. Effective rate ≈ 1 min/detect-equivalent. Override per-knob
    # via env: DAEMON_DETECT_PARALLEL, DAEMON_MIN_PER_DETECT.
    parallel = max(1, int(os.environ.get("DAEMON_DETECT_PARALLEL", "3")))
    min_per_detect = float(os.environ.get("DAEMON_MIN_PER_DETECT", "3"))
    detect_label = (f"{n_detect} detect"
                    + (f" + {n_detect_low} bg" if n_detect_low > 0 else ""))
    out.append(f"<div class='status' style='background:{d_color}22;"
               f"border-left:4px solid {d_color}'>"
               f"<span class='dot' style='background:{d_color}'></span>"
               f"<b>Mac-Daemon</b> — {d_label}<br>"
               f"Queue: {detect_label} · {n_thumbs} thumbs · {n_hls} hls")
    # ETA includes background queue — they share one daemon. High-prio
    # gets pulled first, but the user wants to know total wallclock to
    # corpus-fresh.
    n_total = n_detect + n_detect_low
    if n_total > 0:
        eta_min = int(n_total * min_per_detect / parallel)
        eta_h = eta_min // 60
        eta_rest = eta_min % 60
        eta_str = f"~{eta_h}h {eta_rest}min" if eta_h else f"~{eta_min} min"
        out.append(f" · geschätzte Restlaufzeit {eta_str} "
                   f"(≈{min_per_detect:g} min/detect × {parallel} parallel)")
    out.append("</div>")

    # Collapsible sections via HTML5 <details> — the page used to
    # be one long scroll. Each section is a <details>...</details>
    # whose <summary> wraps the original <h2>. The most diagnostic
    # ones (Verlauf, Per-Show IoU) default open; tabular reference
    # data (Modell-Historie, Per-Channel-Tuning, Bumper-Templates,
    # Confusion, Empfehlungen, Show-Fingerprints, Active-Learning)
    # default closed.
    out.append("""<style>
      details.section { margin: 1.2em 0; border: 1px solid var(--border);
        border-radius: 6px; padding: 0 12px; }
      details.section[open] { padding: 0 12px 12px; }
      details.section > summary { cursor: pointer; padding: 10px 0;
        list-style-position: inside; }
      details.section > summary > h2 { display: inline; margin: 0;
        font-size: 1.1em; }
      details.section > summary:hover { color: var(--link); }
    </style>""")
    _section_open = [False]
    def _section(title, default_open=False):
        if _section_open[0]:
            out.append("</details>")
        # Stable id from title — used by the localStorage script below
        # to persist open/closed state across page reloads. Strip
        # parenthesised numbers (e.g. "Show-Fingerprints (15)") so the
        # id doesn't shift when the count changes.
        clean = re.sub(r"\s*\(.*?\)", "", title)
        sid = re.sub(r"[^a-z0-9]+", "-", clean.lower()).strip("-")
        out.append(f"<details class='section' id='sec-{sid}' "
                   f"data-default-open='{int(default_open)}'>")
        out.append(f"<summary><h2>{title}</h2></summary>")
        _section_open[0] = True

    # Training-active banner — drops in from the very top so the
    # user knows a fresh head is being trained right now (= the
    # current Verlauf metrics will get a new datapoint shortly).
    # Marker file written by tv-train-head.sh on script start, removed
    # via shell trap on exit. Mtime tells us when training started.
    # ETA derived from .training-durations.jsonl (median of recent
    # completed runs); inherently bimodal (cold ~60 min vs warm
    # ~4 min) so the median is approximate — flag it as such.
    train_marker = HLS_DIR / ".tvd-models" / ".training-active"
    if train_marker.exists():
        try:
            mtime = train_marker.stat().st_mtime
            elapsed_s = max(0, int(time.time() - mtime))
            elapsed_min = elapsed_s // 60
            stale_min = elapsed_min > 180  # 3 h is well past worst-case
            # ETA: median of recent successful run durations; only
            # show when we have enough data points (≥3) to make the
            # number meaningful. Cap at 10 most recent so the median
            # tracks current cache state, not ancient cold runs.
            eta_text = ""
            try:
                durs = []
                dp = HLS_DIR / ".tvd-models" / ".training-durations.jsonl"
                for ln in dp.read_text().splitlines()[-10:]:
                    if not ln.strip():
                        continue
                    e = json.loads(ln)
                    if e.get("rc") == 0 and e.get("dur_s"):
                        durs.append(int(e["dur_s"]))
                if len(durs) >= 3:
                    durs.sort()
                    med = durs[len(durs)//2]
                    remaining = max(0, med - elapsed_s)
                    if remaining > 0:
                        eta_text = (f" · ETA ~{remaining // 60} min "
                                    f"(median letzter {len(durs)} Runs: "
                                    f"{med // 60} min)")
                    elif elapsed_s > med * 1.5:
                        eta_text = (f" · läuft länger als üblich "
                                    f"({med // 60} min Median)")
                    else:
                        eta_text = " · gleich fertig"
            except Exception:
                pass
            color = "#e74c3c" if stale_min else "#3498db"
            note = ("⚠ stale marker (>3 h) — script may have crashed; "
                    "delete .training-active manually if no train is running"
                    if stale_min else
                    "Modell wird gerade neu trainiert — neuer Eintrag in "
                    "Verlauf folgt in Kürze. Während des Trainings bleibt "
                    "die deployte head.bin unverändert; Detection läuft "
                    "ungestört weiter.")
            out.append(
                f"<div style='background:{color}22;border-left:4px solid "
                f"{color};padding:10px 14px;margin:0 0 14px;border-radius:4px;"
                f"font-size:.95em'>"
                f"🔄 <b>Training läuft</b> seit {elapsed_min} min{eta_text} · "
                f"{note}</div>")
        except Exception:
            pass

    # Auto-scheduler banner: shows what got auto-planned recently +
    # quick toggle to pause/resume. Sits ABOVE all sections so it's
    # the first thing the user sees after the training-active line.
    auto_log = _read_auto_schedule_log(max_age_s=3 * 86400)
    auto_paused = AUTO_SCHED_PAUSE.exists()
    if auto_log or auto_paused:
        ok_recent = [e for e in auto_log if e.get("ok")]
        err_recent = [e for e in auto_log if not e.get("ok")]
        if auto_paused:
            badge_color = "#7f8c8d"
            head = "🛑 Auto-Scheduler pausiert"
        elif ok_recent:
            badge_color = "#27ae60"
            head = (f"📅 Auto-Scheduler aktiv — letzte 3 Tage: "
                    f"{len(ok_recent)} geplant"
                    + (f", {len(err_recent)} fehlgeschlagen"
                       if err_recent else ""))
        else:
            badge_color = "#3498db"
            head = "📅 Auto-Scheduler aktiv — noch nichts geplant"
        out.append(
            f"<div class='status' style='background:{badge_color}22;"
            f"border-left:4px solid {badge_color};margin:8px 0 16px'>"
            f"<b>{head}</b> "
            f"<button onclick='toggleAutoSchedule()' "
            f"style='float:right;padding:4px 12px;border-radius:4px;"
            f"border:1px solid {badge_color};background:transparent;"
            f"color:var(--fg);cursor:pointer;font-family:inherit;"
            f"font-size:.85em'>"
            f"{'▶ Aktivieren' if auto_paused else '⏸ Pausieren'}</button>")
        if ok_recent[:5]:
            out.append("<details style='margin-top:8px'>"
                       "<summary style='cursor:pointer;font-size:.9em'>"
                       f"Letzte {min(5, len(ok_recent))} geplante Aufnahmen "
                       "anzeigen</summary>")
            out.append("<ul style='margin:6px 0 0 0;padding-left:20px;"
                       "font-size:.88em'>")
            for e in ok_recent[:5]:
                ts = time.strftime("%d.%m %H:%M",
                                   time.localtime(e.get("ts", 0)))
                start = time.strftime("%d.%m %H:%M",
                                      time.localtime(e.get("start", 0)))
                title = (e.get("title") or "?")[:40]
                ch = e.get("channel") or "?"
                n_rev = e.get("n_reviewed_before", 0)
                out.append(f"<li>{ts}: <b>{title}</b> auf {ch} "
                           f"(geplant {start}) — Show hatte {n_rev} reviewed</li>")
            out.append("</ul></details>")
        out.append("</div>")
        out.append("""<script>
async function toggleAutoSchedule() {
  const r = await fetch('/api/learning/auto-schedule-log').then(r=>r.json());
  const now_paused = r.paused;
  const newState = !now_paused;
  await fetch('/api/learning/auto-schedule-pause', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({paused: newState})});
  location.reload();
}</script>""")

    # Auto-confirm banner — analogous to auto-scheduler. Default-paused
    # on first install; user opts-in once a few /recordings badges
    # have been seen and the verdicts look trustworthy.
    ac_paused = AUTO_CONFIRM_PAUSE.exists()
    ac_today_count = 0
    if AUTO_CONFIRM_LOG.is_file():
        try:
            cutoff = time.time() - 86400
            for ln in AUTO_CONFIRM_LOG.read_text().splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                try:
                    r = json.loads(ln)
                    if r.get("ts", 0) >= cutoff:
                        ac_today_count += 1
                except Exception:
                    continue
        except Exception:
            pass
    if ac_paused:
        ac_color = "#7f8c8d"
        ac_head = ("✓ Auto-Confirm pausiert — keine Aufnahmen werden "
                   "automatisch reviewed")
    elif ac_today_count:
        ac_color = "#27ae60"
        ac_head = (f"✓ Auto-Confirm aktiv — letzte 24h: {ac_today_count} "
                   f"Aufnahme{'n' if ac_today_count != 1 else ''} "
                   f"automatisch bestätigt")
    else:
        ac_color = "#3498db"
        ac_head = ("✓ Auto-Confirm aktiv — wartet auf Aufnahmen mit "
                   "Confidence ≥85% (Whisper + Cluster-Anchors nötig)")
    out.append(
        f"<div class='status' style='background:{ac_color}22;"
        f"border-left:4px solid {ac_color};margin:8px 0 16px'>"
        f"<b>{ac_head}</b> "
        f"<button onclick='toggleAutoConfirm()' "
        f"style='float:right;padding:4px 12px;border-radius:4px;"
        f"border:1px solid {ac_color};background:transparent;"
        f"color:var(--fg);cursor:pointer;font-family:inherit;"
        f"font-size:.85em'>"
        f"{'▶ Aktivieren' if ac_paused else '⏸ Pausieren'}</button>"
        f"</div>"
        f"<script>"
        f"async function toggleAutoConfirm() {{"
        f"  const r = await fetch('/api/internal/auto-confirm/status')"
        f"    .then(r => r.json()).catch(() => ({{paused: {str(ac_paused).lower()}}}));"
        f"  const newState = !r.paused;"
        f"  await fetch('/api/internal/auto-confirm/pause', {{"
        f"    method: 'POST', headers: {{'Content-Type': 'application/json'}},"
        f"    body: JSON.stringify({{paused: newState}})}});"
        f"  location.reload();"
        f"}}"
        f"</script>")

    # History chart (visual trend before the table)
    if history:
        _section("Verlauf", default_open=True)
        out.append(_render_current_metrics(history))
        out.append(_render_history_chart(history))

    # History table (last 30)
    _section("Modell-Historie (letzte 30 Runs)")
    out.append("<table><tr><th>Zeit</th><th>Train Acc</th><th>Test Acc</th>"
               "<th>Test IoU</th><th>n Test/Train</th><th>Status</th><th>Reason</th></tr>")
    for e in reversed(history[-30:]):
        ts = e.get("ts", "?")
        ta = e.get("train_acc"); tea = e.get("test_acc"); iou = e.get("test_iou")
        nt = e.get("n_test_recs", 0); ntr = e.get("n_train_recs", 0)
        dep = e.get("deployed", False)
        cls = "dep" if dep else "rej"
        sym = "✓ DEPLOYED" if dep else "✗ REJECTED"
        iou_bar = (f"<span class='bar' style='width:{int((iou or 0)*100)}px'></span> "
                   f"{iou*100:.1f}%" if iou is not None else "–")
        out.append(f"<tr><td>{ts}</td><td>{ta*100:.1f}%</td>"
                   f"<td>{tea*100:.1f}%</td><td>{iou_bar}</td>"
                   f"<td>{nt}/{ntr}</td><td class='{cls}'>{sym}</td>"
                   f"<td class='muted'>{e.get('reason','')}</td></tr>")
    out.append("</table>")

    # Per-channel
    _section("Per-Channel-Tuning")
    out.append("<table><tr><th>Channel</th><th>Logo-Smooth</th>"
               "<th>Start-Lag (gelernt)</th><th>Sponsor-Tail (gelernt)</th>"
               "<th>Block-Länge Prior</th><th>n Samples</th></tr>")
    for s in all_slugs:
        ls = channel_cfg.get(s, {}).get("logo_smooth_s", 0)
        lr = learning.get(s, {})
        sl = lr.get("start_lag", 0); sp = lr.get("sponsor_duration", 0)
        pr = priors.get(s, {})
        prior_str = (f"{pr['min_block_s']:.0f}–{pr['max_block_s']:.0f} s "
                     f"(μ={pr['mean_s']:.0f} σ={pr['std_s']:.0f})"
                     if pr else "<span class='muted'>n &lt; 5 — Defaults</span>")
        n = pr.get("sample_n", lr.get("sample_n", 0))
        out.append(f"<tr><td><b>{s}</b></td><td>{ls or '–'} s</td>"
                   f"<td>{sl or '–'} s</td><td>{sp or '–'} s</td>"
                   f"<td>{prior_str}</td><td>{n}</td></tr>")
    out.append("</table>")

    # Bumper-template coverage per channel — flags slugs where a
    # marked station-id card would help boundary-snap precision.
    # Private-TV channels (RTL/Pro7/SAT.1/sixx/kabel-eins/vox) are the
    # ones that PLAY bumpers; public broadcasters don't have ad blocks
    # at all so we don't list them. Per-show suggestions show up in the
    # active-learning section based on test IoU.
    PRIVATE_SLUGS = ["rtl", "rtlzwei", "prosieben", "prosiebenmaxx",
                     "sat-1", "sixx", "kabel-eins", "kabel-eins-doku",
                     "vox", "vox-up", "tlc", "dmax"]
    _section("Bumper-Templates pro Sender")
    out.append("<p class='muted'>Sender-Bumper sind das stärkste "
               "deterministische Boundary-Signal. Zwei Sorten: "
               "<b>End-Bumper</b> (z.B. sixx „WIE SIXX IST DAS DENN?\", "
               "RTL „Mein RTL\") snappen Werbeblock-Ende; "
               "<b>Start-Bumper</b> (z.B. sixx „WERBUNG\"-Card) snappen "
               "Werbeblock-Anfang. Pro Sender + Sorte 1-5 Templates "
               "reichen meist. Markierung über den Player: "
               "🎯 Werbung → 🎬 Bumper End → 🎬 Bumper Start (3-State-"
               "Toggle), dann ⏵ Start / ⏹ Ende beim Bumper-Frame.</p>")
    bdir = HLS_DIR / ".tvd-bumpers"
    # Only list slugs we actually have recordings on — no point urging
    # the user to mark a kabel-eins-doku bumper if they've never recorded
    # one. One DVR-grid pull covers all uuids.
    have_slugs = set()
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=500", timeout=6).read())
        for e in data.get("entries", []):
            s = slugify(e.get("channelname") or "")
            if s:
                have_slugs.add(s)
    except Exception:
        # If tvh is unreachable, fall back to "show every PRIVATE_SLUG"
        # rather than producing an empty section.
        have_slugs = set(PRIVATE_SLUGS)
    # Collect templates as (kind, path) tuples so the renderer can
    # tag and link each thumb correctly. Channel-root *.png = legacy
    # end-bumpers (captured before the start/end split).
    rows = []
    for slug in PRIVATE_SLUGS:
        if slug not in have_slugs:
            continue
        sd = bdir / slug
        items = []
        if sd.is_dir():
            for p in sorted(sd.glob("*.png")):
                items.append(("end", p))
            end_dir = sd / "end"
            if end_dir.is_dir():
                for p in sorted(end_dir.glob("*.png")):
                    items.append(("end", p))
            start_dir = sd / "start"
            if start_dir.is_dir():
                for p in sorted(start_dir.glob("*.png")):
                    items.append(("start", p))
        rows.append((len(items), slug, items))
    rows.sort(key=lambda r: (r[0] > 0, r[0]))  # empty first, then ascending
    out.append("<style>"
               ".bm-row{margin:8px 0;padding:8px;background:var(--stripe);"
               "border-radius:4px}"
               ".bm-head{display:flex;align-items:baseline;gap:10px;margin-bottom:6px}"
               ".bm-slug{font-weight:bold;font-size:1.05em}"
               ".bm-count{color:var(--muted);font-size:.9em}"
               ".bm-status{margin-left:auto;font-size:.9em}"
               ".bm-thumbs{display:flex;flex-wrap:wrap;gap:6px}"
               ".bm-thumb{position:relative;width:120px;border-radius:3px;overflow:hidden}"
               ".bm-thumb img{width:120px;height:auto;display:block;background:#000}"
               ".bm-del{position:absolute;top:2px;right:2px;width:22px;height:22px;"
               "border:none;background:rgba(0,0,0,.7);color:#fff;cursor:pointer;"
               "border-radius:50%;font-size:14px;line-height:18px;padding:0}"
               ".bm-del:hover{background:#e74c3c}"
               ".bm-name{font-size:.7em;color:var(--muted);padding:3px;"
               "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
               "</style>")
    for n, slug, items in rows:
        n_end = sum(1 for k, _ in items if k == "end")
        n_start = sum(1 for k, _ in items if k == "start")
        if n == 0:
            status = ("<span style='color:#e74c3c'>fehlt — bringt vermutlich "
                      "+0.05–0.13 IoU</span>")
        elif n < 3:
            status = (f"<span style='color:#f39c12'>nur {n} — mehrere "
                      f"Varianten erfassen für bessere Match-Rate</span>")
        else:
            status = "<span style='color:#27ae60'>ausreichend</span>"
        kind_breakdown = (f" <span class='muted' style='font-size:.85em'>"
                          f"({n_end} End, {n_start} Start)</span>"
                          if items else "")
        out.append(f"<div class='bm-row'>")
        out.append(f"<div class='bm-head'><span class='bm-slug'>{slug}</span>"
                   f"<span class='bm-count'>{n} Templates{kind_breakdown}</span>"
                   f"<span class='bm-status'>{status}</span></div>")
        if items:
            out.append("<div class='bm-thumbs'>")
            for kind, p in items:
                fn = p.name
                # URL: kind subdir if file lives under start/ or end/,
                # legacy channel-root path otherwise.
                if p.parent.name in ("start", "end"):
                    img_url = f"/api/internal/detect-bumper/{slug}/{p.parent.name}/{fn}"
                else:
                    img_url = f"/api/internal/detect-bumper/{slug}/{fn}"
                kind_tag = ("<span style='position:absolute;top:2px;left:2px;"
                            "background:rgba(0,0,0,.7);color:#fff;font-size:.7em;"
                            "padding:1px 5px;border-radius:3px'>"
                            f"{'▶ Start' if kind == 'start' else '⏹ End'}</span>")
                out.append(
                    f"<div class='bm-thumb'>"
                    f"<img src='{img_url}' loading='lazy' alt=''/>"
                    f"{kind_tag}"
                    f"<button class='bm-del' onclick=\"deleteBumper('{slug}','{fn}',this)\" "
                    f"title='Template löschen'>×</button>"
                    f"<div class='bm-name' title='{fn}'>{fn}</div>"
                    f"</div>")
            out.append("</div>")
        out.append("</div>")
    out.append(
        "<script>function deleteBumper(slug,fn,btn){"
        "if(!confirm('Template '+fn+' löschen?'))return;"
        "fetch('/api/bumper/'+slug+'/'+fn,{method:'DELETE'})"
        ".then(r=>r.json()).then(d=>{"
        "if(d.ok){btn.closest('.bm-thumb').remove();}"
        "else{alert('Fehler: '+(d.error||'unbekannt'));}"
        "}).catch(e=>alert('Netzwerk-Fehler'));}</script>")

    # Failure-mode taxonomy — concrete categorisation of where the
    # model struggles, broken down per channel + per show. Built from
    # live ads.json/ads_user.json on disk, no precomputation needed.
    _t0 = time.time()
    try:
        per_show_fm, per_chan_fm = _aggregate_failure_modes_cached()
    except Exception as e:
        per_show_fm, per_chan_fm = {}, {}
        print(f"[learning-page] failure-mode aggregation err: {e}",
              flush=True)
    # Surface a banner if a bulk re-detect is in progress — those
    # recordings are intentionally excluded from the classification
    # (would otherwise swamp the washout bucket), and the user
    # should know the analysis below is incomplete until it drains.
    n_pending = sum(1 for _ in HLS_DIR.glob("_rec_*/.detect-requested"))
    _ms = (time.time()-_t0)*1000
    if _ms > 500:
        print(f"[learning-page-prof] failure-mode-agg={_ms:.0f}ms n={n_pending}",
              flush=True)
    _t0 = time.time()
    if per_show_fm or per_chan_fm:
        _section("Failure-Mode-Analyse (wo das Modell stolpert)")
        if n_pending > 0:
            out.append(
                f"<div style='background:#3498db22;border-left:4px solid "
                f"#3498db;padding:8px 12px;margin-bottom:10px;"
                f"border-radius:4px;font-size:.9em'>"
                f"⏳ <b>{n_pending} Aufnahmen werden gerade re-detected</b> "
                f"(Bulk-Re-Detect nach Head-Update läuft) und sind aus "
                f"der Auswertung unten ausgeklammert. Tabelle vervoll"
                f"ständigt sich automatisch sobald der Daemon durch ist."
                f"</div>")
        out.append(_render_failure_modes(per_show_fm, per_chan_fm))
    _ms = (time.time()-_t0)*1000
    if _ms > 500:
        print(f"[learning-page-prof] failure-mode-render={_ms:.0f}ms", flush=True)
    _t0 = time.time()

    # Show-gap detection — proactive labelling recommendations.
    try:
        show_gaps = _aggregate_show_gaps_cached()
    except Exception as e:
        show_gaps = []
        print(f"[learning-page] show-gap aggregation err: {e}",
              flush=True)
    if show_gaps:
        _section("Show-Lücken (wo zusätzliche Reviews am meisten bringen)",
                 default_open=True)
        out.append(_render_show_gaps(show_gaps))
    _ms = (time.time()-_t0)*1000
    if _ms > 500:
        print(f"[learning-page-prof] show-gaps={_ms:.0f}ms", flush=True)
    _t0 = time.time()

    # Deletion candidates — collapsed by default since it's an
    # opt-in housekeeping action. Lazy-loaded via /api/learning/
    # deletion-candidates so the heavy DVR-grid fetch only happens
    # when user opens the section.
    _section("Sichere Löschkandidaten (Disk freigeben)")
    out.append("<div id='del-cand-mount'><p class='muted'>Lade …</p></div>")
    out.append("""<script>
(function(){
  // Look up the parent <details> via the mount node — robust against
  // section-ID slugification quirks (Umlauts in "Löschkandidaten"
  // get replaced by dashes, not "oe", so the title-derived ID
  // doesn't match obvious guesses).
  const mount=document.getElementById('del-cand-mount');
  if(!mount)return;
  const sec=mount.closest('details');
  if(!sec)return;
  let loaded=false;
  sec.addEventListener('toggle',async()=>{
    if(!sec.open||loaded)return;
    loaded=true;
    try{
      const r=await fetch('/api/learning/deletion-candidates').then(r=>r.json());
      const fmt=(mb)=>mb>=1024?(mb/1024).toFixed(1)+' GB':mb+' MB';
      const renderRow=(e,tier)=>'<tr><td><b>'+e.title+'</b><br>'
        +'<span style="color:var(--muted);font-size:.85em">'+e.channel
        +' · '+Math.round(e.age_days)+' Tage alt</span></td>'
        +'<td style="text-align:right">'+fmt(e.filesize_mb)+'</td>'
        +'<td><a href="/recording/'+e.uuid+'" target="_blank" '
        +'style="color:var(--link)">prüfen</a></td>'
        +'<td><button class="del-btn" data-uuid="'+e.uuid+'" '
        +'data-title="'+e.title.replace(/"/g,'&quot;')+'" '
        +'style="padding:4px 10px;border:1px solid #c0392b;'
        +'background:transparent;color:#c0392b;border-radius:4px;'
        +'cursor:pointer">🗑 Löschen</button></td></tr>';
      let html='';
      if(r.tier1.length){
        html+='<h3 style="margin-top:14px">Tier 1 — unreviewed alt ('
          +fmt(r.tier1_total_mb)+' frei machbar)</h3>'
          +'<p class="muted" style="font-size:.85em">Recordings die du in 14+ Tagen'
          +' nicht reviewed hast. Nichts geht im Training verloren.</p>'
          +'<table style="width:100%"><tr><th style="text-align:left">Aufnahme</th>'
          +'<th>Größe</th><th></th><th></th></tr>'
          +r.tier1.slice(0,30).map(e=>renderRow(e,1)).join('')
          +'</table>';
        if(r.tier1.length>30)html+='<p class="muted">… und '
          +(r.tier1.length-30)+' weitere</p>';
      }
      if(r.tier2.length){
        html+='<h3 style="margin-top:14px">Tier 2 — reviewed aber alt ('
          +fmt(r.tier2_total_mb)+' frei machbar)</h3>'
          +'<p class="muted" style="font-size:.85em">≥30 Tage alt, Show hat ≥5 reviewed'
          +' Episoden — Training-Beitrag minimal.</p>'
          +'<table style="width:100%"><tr><th style="text-align:left">Aufnahme</th>'
          +'<th>Größe</th><th></th><th></th></tr>'
          +r.tier2.slice(0,30).map(e=>renderRow(e,2)).join('')
          +'</table>';
        if(r.tier2.length>30)html+='<p class="muted">… und '
          +(r.tier2.length-30)+' weitere</p>';
      }
      if(!r.tier1.length&&!r.tier2.length){
        html='<p>Keine sicheren Löschkandidaten. Test-Set: '+r.test_set_size+' Aufnahmen.</p>';
      }
      mount.innerHTML=html;
      mount.querySelectorAll('.del-btn').forEach(btn=>{
        btn.addEventListener('click',async()=>{
          if(!confirm('Wirklich löschen?\\n\\n'+btn.dataset.title))return;
          btn.disabled=true;btn.textContent='…';
          try{
            // /recording/<uuid>/delete handles dvr/entry/remove +
            // HLS cleanup + ffmpeg kill in one shot. DELETE-only
            // since 2026-05-03 (= no longer a hyperlink target;
            // the GET path was a CSRF risk).
            const dr=await fetch('/recording/'+btn.dataset.uuid+'/delete',
              {method:'DELETE'});
            if(dr.ok){
              btn.closest('tr').style.opacity=.3;
              btn.textContent='✓ gelöscht';
            }else{btn.disabled=false;btn.textContent='✗ Fehler '+dr.status;}
          }catch(e){btn.disabled=false;btn.textContent='✗ '+e.message;}
        });
      });
    }catch(e){
      mount.innerHTML='<p style="color:#e74c3c">Fehler: '+e.message+'</p>';
    }
  });
})();
</script>""")

    # Confusion summary
    if confusion:
        _section("Confusion-Analyse (letzter Test-Set Run)")
        out.append("<table><tr><th>Show</th><th>Frames</th><th>Block-Vergleich</th><th>Fehlertyp</th></tr>")
        for c in confusion:
            out.append(f"<tr><td><b>{c['title']}</b></td>"
                       f"<td class='muted'>{c.get('frames','')}</td>"
                       f"<td>{c.get('blocks','')}</td>"
                       f"<td class='muted'>{c.get('errors','')}</td></tr>")
        out.append("</table>")

    # Active-learning queue
    # ── Recommendation engine: where would more labelling help most? ──
    # Three buckets: (1) channels just below the per-channel-prior gate,
    # (2) shows just below the per-show-prior gate, (3) test-set shows
    # with low IoU (direct test-metric impact). Sorted by gap-to-threshold
    # so the cheapest wins (= 1 more recording flips a channel into having
    # its own prior) appear first.
    _t0 = time.time()
    recommendations = []
    # (1) per-channel gates: count user-confirmed recordings + blocks per slug
    chan_user_recs = {}
    chan_user_blocks = {}
    show_user_blocks = {}
    show_user_recs = {}
    for d in HLS_DIR.glob("_rec_*"):
        uuid = d.name[5:]
        slug = _rec_channel_slug(uuid) or ""
        show = _show_title_for_rec(d)
        user = d / "ads_user.json"
        if not user.is_file():
            continue
        try:
            raw = json.loads(user.read_text())
        except Exception:
            continue
        blocks = raw if isinstance(raw, list) else (raw.get("ads", []) or [])
        n_blocks = len([b for b in blocks if b and len(b) >= 2])
        if slug:
            chan_user_recs[slug] = chan_user_recs.get(slug, 0) + 1
            chan_user_blocks[slug] = chan_user_blocks.get(slug, 0) + n_blocks
        if show:
            show_user_recs[show] = show_user_recs.get(show, 0) + 1
            show_user_blocks[show] = show_user_blocks.get(show, 0) + n_blocks
    for slug, n_blk in sorted(chan_user_blocks.items(), key=lambda x: x[1]):
        if 0 < n_blk < 5:
            need = 5 - n_blk
            recommendations.append({
                "kind": "channel", "key": slug, "n": need,
                "msg": f"<b>{slug}</b> hat {n_blk} user-Block(s) — "
                       f"{need} mehr für eigenen Channel-Block-Prior",
                "priority": need})
    # Singletons (= movies, one-off specials) excluded — chasing
    # +4 user-blocks for "Rocky II" or "Bohemian Rhapsody" is futile
    # since the broadcaster won't repeat it 5 times. show_user_recs
    # is the per-show review count (= same data _is_singleton_title
    # uses). autorec_t fetched once per page render below.
    _show_recs_for_check = {s: n for s, n in show_user_recs.items()}
    _ar_titles_for_check = _autorec_titles()
    _ug_titles_for_check = _user_grouped_titles()
    for show, n_blk in sorted(show_user_blocks.items(), key=lambda x: x[1]):
        if 0 < n_blk < 5:
            if _is_singleton_title(show, _show_recs_for_check,
                                    _ar_titles_for_check,
                                    _ug_titles_for_check):
                continue
            need = 5 - n_blk
            recommendations.append({
                "kind": "show", "key": show, "n": need,
                "msg": f"Show <b>{show}</b> hat {n_blk} user-Block(s) — "
                       f"{need} mehr für eigenen Show-Prior",
                "priority": need + 0.5})
    weak_shows = []
    for c in confusion:
        m = re.search(r"missed=(\d+)\s+extra=(\d+)", c.get("blocks", ""))
        if m:
            missed = int(m.group(1)); extra = int(m.group(2))
            if missed + extra >= 1:
                weak_shows.append((c["title"], missed, extra))
    for title, missed, extra in sorted(weak_shows, key=lambda x: -(x[1] + x[2])):
        # Same singleton-skip — recommending the user "record more
        # Rocky for IoU correction" is just as futile here.
        if _is_singleton_title(title, _show_recs_for_check,
                                _ar_titles_for_check,
                                _ug_titles_for_check):
            continue
        recommendations.append({
            "kind": "test-iou", "key": title, "n": max(1, min(3, missed + extra)),
            "msg": f"Test-Show <b>{title}</b> hat {missed} verfehlt + "
                   f"{extra} extra Block(s) — manuelle Korrektur dort "
                   f"steigert Test-IoU direkt",
            "priority": 10 - missed - extra})
    recommendations.sort(key=lambda r: r["priority"])

    # Subtract already-scheduled future DVR entries that match each
    # recommendation, so clicks across page reloads don't pile up
    # extra recordings. Channel kind matches by channel uuid (slug
    # lookup); show/test-iou kinds match by exact title.
    upcoming_by_slug = {}
    upcoming_by_title = {}
    try:
        up = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid_upcoming?limit=500",
            timeout=10).read())
        uuid_to_slug = {}
        with cmap_lock:
            for s, info in channel_map.items():
                uuid_to_slug[info["uuid"]] = s
        for e in up.get("entries", []):
            slug = uuid_to_slug.get(e.get("channel", ""), "")
            if slug:
                upcoming_by_slug[slug] = upcoming_by_slug.get(slug, 0) + 1
            t = (e.get("disp_title") or "").strip()
            if t:
                upcoming_by_title[t] = upcoming_by_title.get(t, 0) + 1
    except Exception:
        pass
    pruned = []
    for r in recommendations:
        if r["kind"] == "channel":
            already = upcoming_by_slug.get(r["key"], 0)
        else:
            already = upcoming_by_title.get(r["key"], 0)
        remaining = r["n"] - already
        if remaining <= 0:
            continue
        r["n"] = remaining
        pruned.append(r)
    recommendations = pruned
    _ms = (time.time()-_t0)*1000
    if _ms > 500:
        print(f"[learning-page-prof] recommendations={_ms:.0f}ms",
              flush=True)
    _t0 = time.time()

    if recommendations:
        _section("Empfehlungen — wo Labelling am meisten bringt")
        # "Alle annehmen" — fires every individual plan-btn in order,
        # collects results into one summary alert. Useful when there
        # are 5-10 recommendations and the user just wants to accept
        # them in bulk (typical after a model retrain surfaces a fresh
        # set of under-represented shows / channels).
        total_n = sum(min(r["n"], 10) for r in recommendations[:10])
        out.append(
            f"<p style='margin:0 0 10px'>"
            f"<button id='plan-all-btn' "
            f"style='padding:6px 14px;border-radius:4px;border:1px solid #27ae60;"
            f"background:#27ae60;color:#fff;font-size:.9em;cursor:pointer;'>"
            f"📅 alle {len(recommendations[:10])} Empfehlungen annehmen "
            f"(~{total_n} Aufnahmen)</button>"
            f"</p>")
        out.append("<ul style='line-height:1.8;font-size:.9em;list-style:none;"
                   "padding-left:0'>")
        for i, r in enumerate(recommendations[:10]):
            kind_api = "channel" if r["kind"] == "channel" else (
                "show" if r["kind"] == "show" else "test-iou")
            # data-attrs picked up by the JS handler at the end
            out.append(
                f"<li style='display:flex;gap:10px;align-items:baseline;"
                f"padding:6px 0;border-bottom:1px solid #2a2a2a'>"
                f"<span style='flex:1'>{r['msg']}</span>"
                f"<button class='plan-btn' data-kind='{kind_api}' "
                f"data-key='{r['key']}' data-n='{r['n']}' "
                f"style='padding:4px 10px;border-radius:4px;border:1px solid #2980b9;"
                f"background:#2980b9;color:#fff;font-size:.85em;cursor:pointer;"
                f"white-space:nowrap'>📅 alle {r['n']} planen</button>"
                f"</li>")
        out.append("</ul>")
        # Button click handler — confirm + POST + toast
        out.append("""<script>
document.querySelectorAll('.plan-btn').forEach(btn => {
  btn.addEventListener('click', async () => {
    const kind = btn.dataset.kind;
    const key = btn.dataset.key;
    const n = parseInt(btn.dataset.n);
    if (!confirm(`Plane ${n} kommende Aufnahme(n) für ${key}?\\n\\nBestätigen → tvheadend findet die nächsten ${n} EPG-Termine und legt sie als DVR-Einträge an. Konflikte (Tuner belegt) werden gemeldet.`)) return;
    btn.disabled = true; btn.textContent = '…';
    try {
      const r = await fetch('/api/learning/plan', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({kind, key, n})
      });
      const d = await r.json();
      if (!d.ok) throw new Error(d.error || 'unknown');
      let msg = `${d.planned.length}/${n} geplant`;
      if (d.planned.length > 0) {
        msg += ':\\n' + d.planned.map(p => `  ${p.start_iso} ${p.title} (${p.channelname})`).join('\\n');
      }
      if (d.skipped.length > 0) {
        msg += `\\n\\nÜbersprungen (${d.skipped.length}):\\n` + d.skipped.map(s => `  ${s.start_iso} ${s.title}: ${s.reason}`).join('\\n');
      }
      if (d.found === 0) msg = `Keine kommenden EPG-Termine für ${key} in den nächsten 14 Tagen.`;
      alert(msg);
      btn.textContent = '✓ ' + d.planned.length + '/' + n;
    } catch (e) {
      alert('Fehler: ' + e.message);
      btn.disabled = false;
      btn.textContent = `📅 alle ${n} planen`;
    }
  });
});

// "Alle Empfehlungen annehmen" — fires every plan-btn in order and
// aggregates results into a single summary. Sequential (not parallel)
// because tvh autorec writes are not lock-free and back-to-back
// concurrent requests can drop entries.
const planAllBtn = document.getElementById('plan-all-btn');
if (planAllBtn) {
  planAllBtn.addEventListener('click', async () => {
    const btns = Array.from(document.querySelectorAll('.plan-btn:not([disabled])'));
    if (!btns.length) return;
    if (!confirm(`Plane ALLE ${btns.length} Empfehlungen?\n\nFür jede wird tvheadend nach kommenden EPG-Terminen gesucht und neue DVR-Einträge angelegt. Konflikte werden gemeldet.`)) return;
    planAllBtn.disabled = true;
    const orig = planAllBtn.textContent;
    let okN = 0, failN = 0, plannedTotal = 0, skippedTotal = 0;
    for (let i = 0; i < btns.length; i++) {
      planAllBtn.textContent = `läuft ${i+1}/${btns.length} …`;
      const btn = btns[i];
      const kind = btn.dataset.kind, key = btn.dataset.key, n = parseInt(btn.dataset.n);
      btn.disabled = true; btn.textContent = '…';
      try {
        const r = await fetch('/api/learning/plan', {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({kind, key, n})
        });
        const d = await r.json();
        if (!d.ok) throw new Error(d.error || 'unknown');
        plannedTotal += d.planned.length;
        skippedTotal += d.skipped.length;
        btn.textContent = '✓ ' + d.planned.length + '/' + n;
        okN++;
      } catch (e) {
        btn.textContent = '✗ ' + e.message.slice(0, 20);
        failN++;
      }
    }
    planAllBtn.textContent = `${okN} ok · ${failN} err · ${plannedTotal} geplant · ${skippedTotal} übersprungen`;
  });
}
</script>""")

    # ── Show-Fingerprint section ──────────────────────────────────
    # List shows with enough confirmed episodes for a fingerprint, and
    # a button to scan all unreviewed recordings against them. Each
    # match auto-confirms (writes a synthetic ads_user.json) so the
    # recording disappears from the active-learning queue.
    # Per-show IoU trend (one sparkline per show, sorted by current IoU
    # ascending so problem-shows surface at the top). Sourced from
    # head.per-show-iou.jsonl appended after each train-head deploy
    # via /api/internal/snapshot-per-show-iou. Sits next to the other
    # show-aggregate sections (Show-Fingerprints, Active-Learning) so
    # all per-show diagnostics are grouped at the bottom of the page.
    per_show_html = _render_per_show_iou_trend()
    if per_show_html:
        per_show_html = per_show_html.replace(
            "<h2>Per-Show IoU-Verlauf</h2>", "")
        _section("Per-Show IoU-Verlauf", default_open=True)
        out.append(per_show_html)

    # Test-Set membership — read from head.test-set.json sidecar
    # written by train-head.py. Surfaces WHICH user-reviewed
    # recordings count toward the per-show IoU snapshot (= the
    # ones the bulk re-detect targets after a head deploy under
    # the V1 test-set-only invalidation strategy).
    ts_path = HLS_DIR / ".tvd-models" / "head.test-set.json"
    if ts_path.is_file():
        try:
            ts_data = json.loads(ts_path.read_text())
            ts_uuids = ts_data.get("uuids", []) or []
        except Exception:
            ts_uuids = []
        if ts_uuids:
            # Group by show for readability; UUIDs without a show
            # title (= recording dir gone or .txt missing) bucket
            # under "(unknown)".
            by_show = {}
            for u in ts_uuids:
                d = HLS_DIR / f"_rec_{u}"
                # Prefer the cutlist-derived show name (= what
                # train-head actually keys on), fall back to the
                # tvh DVR entry's disp_title for fresh recordings
                # whose detect hasn't completed yet (.txt missing).
                show = (_show_title_for_rec(d)
                        or _rec_dvr_title(u)
                        or "(unknown)")
                slug = _rec_channel_slug(u) or ""
                date_s = _rec_date_from_filename(d)
                by_show.setdefault(show, []).append((u, slug, date_s))
            _section(f"Test-Set ({len(ts_uuids)} Aufnahmen)")
            out.append(
                "<p class='muted'>Diese Aufnahmen werden vom Per-Show-IoU-"
                "Snapshot ausgewertet und sind die einzigen die nach jedem "
                "Modell-Deploy automatisch re-detected werden (V1: "
                "Test-Set-only Bulk-Invalidate). Alle anderen Recordings "
                "behalten ihre Cutlists vom alten Modell bis du sie öffnest "
                "(=lazy regenerieren via /recording/&lt;uuid&gt;/ads).</p>")
            out.append("<table style='width:100%'><tr>"
                       "<th style='text-align:left'>Show</th>"
                       "<th>n</th><th>Aufnahmen</th></tr>")
            for show in sorted(by_show, key=lambda s: -len(by_show[s])):
                recs = by_show[show]
                links = " · ".join(
                    f"<a href='{HOST_URL}/recording/{u}' "
                    f"title='{slug}'>{date_s or u[:8]}</a>"
                    for u, slug, date_s in recs)
                out.append(f"<tr><td><b>{show}</b></td>"
                           f"<td>{len(recs)}</td>"
                           f"<td style='font-size:.9em'>{links}</td></tr>")
            out.append("</table>")

    _t0 = time.time()
    fingerprints = _compute_show_fingerprints_cached()
    _t_fp = time.time() - _t0
    if _t_fp > 0.05:
        print(f"[learning-page-prof] fingerprints={_t_fp*1000:.0f}ms",
              flush=True)
    # Drop singletons (= movies, one-off specials). A "fingerprint"
    # for a film aired twice is meaningless — same movie, same ad
    # break pattern, no transferable signal. Auto-confirm via
    # fingerprint-scan also wouldn't fire on movie titles since
    # min_recs=2 + count_consensus rarely passes for film duplicates.
    if fingerprints:
        show_n = _show_review_counts()
        autorec_t = _autorec_titles()
        ug_t = _user_grouped_titles()
        fingerprints = {s: fp for s, fp in fingerprints.items()
                        if not _is_singleton_title(s, show_n, autorec_t, ug_t)}
    if fingerprints:
        _section(f"Show-Fingerprints ({len(fingerprints)})")
        out.append("<p class='muted'>Shows mit ≥3 user-bestätigten Episoden + "
                   "konsistenter Block-Anzahl. Auto-Confirm prüft neue Aufnahmen "
                   "gegen den Fingerprint und übernimmt Treffer als bestätigt — "
                   "spart manuelles Klicken bei wiederkehrenden Sendungen.</p>")
        out.append("<table><tr><th>Show</th><th>Episoden</th>"
                   "<th>Blocks</th><th>Layout (rel. zur Show)</th></tr>")
        for show in sorted(fingerprints):
            fp = fingerprints[show]
            layout = " · ".join(
                f"{int(b['start_s']//60)}:{int(b['start_s']%60):02d}–"
                f"{int(b['end_s']//60)}:{int(b['end_s']%60):02d}"
                for b in fp["blocks"])
            out.append(f"<tr><td>{show}</td><td>{fp['n_recs']}</td>"
                       f"<td>{fp['block_count']}</td>"
                       f"<td style='font-size:.85em'>{layout}</td></tr>")
        out.append("</table>")
        out.append("<p><button id='fp-scan-btn' "
                   "style='padding:6px 14px;border-radius:4px;border:1px solid #27ae60;"
                   "background:#27ae60;color:#fff;font-size:.95em;cursor:pointer;"
                   "margin-right:8px'>"
                   "🔍 Auto-Confirm via Fingerprint</button>"
                   "<button id='fp-val-btn' "
                   "style='padding:6px 14px;border-radius:4px;border:1px solid #7f8c8d;"
                   "background:#7f8c8d;color:#fff;font-size:.95em;cursor:pointer'>"
                   "📊 Validieren (Leave-One-Out)</button></p>")
        out.append("""<script>
document.getElementById('fp-scan-btn').addEventListener('click', async (ev) => {
  const btn = ev.target;
  if (!confirm('Alle unreviewten Aufnahmen gegen die Show-Fingerprints prüfen?\\n\\nTreffer werden als bestätigt markiert (= identisch zu manuellem ✓ Geprüft).')) return;
  btn.disabled = true; btn.textContent = '…läuft';
  try {
    const r = await fetch('/api/learning/fingerprint-scan', {method:'POST'});
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || 'unknown');
    let msg = `Geprüft: ${d.scanned} Aufnahmen · Auto-bestätigt: ${d.matched}`;
    if (d.matched_list && d.matched_list.length) {
      msg += '\\n\\nNeu bestätigt:\\n' + d.matched_list.map(m =>
        `  ${m.show} (${m.blocks} Blocks)`).join('\\n');
    }
    if (d.skipped && d.skipped.length) {
      msg += `\\n\\nÜbersprungen (${d.skipped.length}, Fingerprint mismatch):\\n`
        + d.skipped.slice(0, 10).map(s => `  ${s.show}: ${s.reason}`).join('\\n');
      if (d.skipped.length > 10) msg += `\\n  ... +${d.skipped.length - 10} weitere`;
    }
    alert(msg);
    if (d.matched > 0) location.reload();
    else { btn.disabled = false; btn.textContent = '🔍 Auto-Confirm via Fingerprint'; }
  } catch (e) {
    alert('Fehler: ' + e.message);
    btn.disabled = false; btn.textContent = '🔍 Auto-Confirm via Fingerprint';
  }
});
document.getElementById('fp-val-btn').addEventListener('click', async (ev) => {
  const btn = ev.target;
  btn.disabled = true; btn.textContent = '…läuft';
  try {
    const r = await fetch('/api/learning/fingerprint-validate', {method:'POST'});
    const d = await r.json();
    if (!d.ok) throw new Error(d.error || 'unknown');
    const s = d.summary;
    let msg = `Leave-One-Out Validierung über ${s.total} reviewte Aufnahmen:\\n\\n`;
    msg += `  ✓ matched (Fingerprint hätte korrekt auto-bestätigt): ${s.matched}\\n`;
    msg += `  ✗ mismatch (Fingerprint zu eng / Episode Ausreißer):  ${s.mismatch}\\n`;
    msg += `  – no_fp (nicht genug Geschwister-Episoden):           ${s.no_fp}\\n`;
    if (s.matched + s.mismatch > 0) {
      const acc = (100 * s.matched / (s.matched + s.mismatch)).toFixed(0);
      msg += `\\nMatch-Rate (testbar): ${acc}%\\n`;
    }
    const mismatches = d.results.filter(r => r.decision === 'mismatch');
    if (mismatches.length) {
      msg += `\\nMismatches im Detail:\\n` +
        mismatches.slice(0, 10).map(r => `  ${r.show}: ${r.reason}`).join('\\n');
      if (mismatches.length > 10) msg += `\\n  ... +${mismatches.length - 10} weitere`;
    }
    alert(msg);
  } catch (e) {
    alert('Fehler: ' + e.message);
  }
  btn.disabled = false; btn.textContent = '📊 Validieren (Leave-One-Out)';
});
</script>""")

    if uncertain:
        skip_note = (f" · {reviewed_skipped} weitere ausgeblendet "
                     f"(geprüfte Aufnahmen)" if reviewed_skipped else "")
        # Group by show title so the user can pick a show to work on
        # without scrolling through dominant shows (SpongeBob etc).
        # Sort within-show by the existing (src, |p-0.5|) order so the
        # most actionable frame surfaces first; shows themselves sorted
        # by uncertain-frame-count DESC so the highest-leverage shows
        # are at the top.
        from collections import defaultdict
        by_show = defaultdict(list)
        for u in uncertain:
            by_show[u["title"]].append(u)
        shows_sorted = sorted(by_show.items(), key=lambda kv: -len(kv[1]))

        _section(f"Active-Learning Targets "
                 f"({len(uncertain)} offen · {len(by_show)} Sendungen"
                 f"{skip_note})")
        out.append("<p class='muted'>Frames mit hohem Trainings-Wert. "
                   "🎯 = Modell unsicher (p≈0.5). "
                   "⚠ = Modell sicher, aber Wall-Clock-Prior widerspricht "
                   "(z. B. 99% Werbung gesagt, aber dieser Sender hat zur Minute "
                   "fast nie Werbung). Click → Player. "
                   "Sendungen aufklappen für die Frames.</p>")
        PER_SHOW_CAP = 30
        for title, frames in shows_sorted:
            n = len(frames)
            # Aggregate uuids in this show (= number of distinct recordings)
            n_recs = len({f["uuid"] for f in frames})
            out.append(
                f"<details style='margin:6px 0;border:1px solid var(--border);"
                f"border-radius:6px;padding:6px 10px'>"
                f"<summary style='cursor:pointer;font-weight:600'>"
                f"{title} <span class='muted' style='font-weight:400'>"
                f"({n} frames · {n_recs} Aufnahme{'n' if n_recs != 1 else ''})"
                f"</span></summary>"
            )
            out.append("<table style='margin-top:6px'>"
                       "<tr><th></th><th>Zeit</th><th>p</th><th></th></tr>")
            for u in frames[:PER_SHOW_CAP]:
                mm = int(u["t"] // 60); ss = int(u["t"] % 60)
                link = f"{HOST_URL}/recording/{u['uuid']}?t={int(u['t'])}"
                src = u.get("src", "unc")
                icon = ("⚠" if src == "div" else
                        "🎯⚠" if src == "both" else "🎯")
                out.append(f"<tr><td title='{src}'>{icon}</td>"
                           f"<td>{mm}:{ss:02d}</td>"
                           f"<td>{u['p']:.3f}</td>"
                           f"<td><a href='{link}'>öffnen</a></td></tr>")
            if n > PER_SHOW_CAP:
                out.append(f"<tr><td colspan=4 class='muted' "
                           f"style='font-style:italic'>"
                           f"+{n - PER_SHOW_CAP} weitere frames (gekürzt)"
                           f"</td></tr>")
            out.append("</table></details>")

    # ── Per-Show EPG-Drift (Phase-4 manual show-start marks) ────
    _section("Per-Show EPG-Drift")
    out.append(
        "<small style='color:#888;font-weight:400' id='psd-meta'></small>"
        "<p style='color:#666;font-size:.85em;margin:8px 0;line-height:1.45'>"
        "Sender starten Sendungen oft 3-10 min vor dem EPG-Termin "
        "(„Pre-Roll"
        " mit Werbung + Sponsoring"
        ", dann Show"
        ")."
        " Im Player den Mark-Mode 4× tappen → 🎬 Show-Start, dann auf "
        "den ersten Show-Frame springen + tappen. System lernt pro "
        "Sendung wie viel Vorlauf nötig ist."
        "</p>"
        "<div id='psd-list' style='font-size:.92em'>Lade…</div>"
        "<script>"
        "function fmtMin(s){const m=Math.abs(s)/60;"
        "return (s<0?'-':'+')+m.toFixed(1)+' min';}"
        "fetch('/api/internal/per-show-drift').then(r=>r.json()).then(d=>{"
        "const c=document.getElementById('psd-list');"
        "const m=document.getElementById('psd-meta');"
        "const titles=Object.keys(d.shows||{});"
        "if(titles.length===0){"
        "c.innerHTML='<p style=\"color:#888\">Noch keine Show-Start-Marks. "
        "Im Player jede Sendung einmal markieren — System lernt automatisch.</p>';return;}"
        "m.textContent='('+titles.length+' Sendung'+(titles.length>1?'en':'')+')';"
        "const rows=titles.sort((a,b)=>d.shows[b].n-d.shows[a].n).map(t=>{"
        "const s=d.shows[t];"
        "const safeT=t.replace(/\"/g,'&quot;');"
        "return '<tr data-title=\"'+safeT+'\">'"
        "+'<td>'+t+'</td>'"
        "+'<td style=\"text-align:center\">'+s.n+'</td>'"
        "+'<td>'+s.channelname+'</td>'"
        "+'<td>'+fmtMin(s.mean_drift_s)+'</td>'"
        "+'<td>'+fmtMin(s.min_drift_s)+'</td>'"
        "+'<td><b>'+s.suggested_start_extra_min+' min</b>'"
        "+(s.n>=2?'':' <small style=\"color:#888\">(n=1: noch unsicher)</small>')"
        "+'</td>'"
        "+'<td><button class=\"psd-apply\" data-extra=\"'"
        "+s.suggested_start_extra_min+'\" '"
        "+'style=\"padding:3px 10px;font-size:.85em;cursor:pointer;'"
        "+'border:1px solid var(--border);border-radius:4px;'"
        "+'background:var(--stripe);color:var(--fg)\">Anwenden</button></td>'"
        "+'</tr>';}).join('');"
        "c.innerHTML='<table style=\"width:100%;border-collapse:collapse\">"
        "<tr style=\"text-align:left;border-bottom:1px solid #ddd\">"
        "<th>Sendung</th><th style=\"text-align:center\">n</th>"
        "<th>Sender</th><th>Mean drift</th>"
        "<th>Worst (frühster)</th>"
        "<th>Vorschlag start_extra</th><th></th></tr>'+rows+'</table>'"
        "+'<p style=\"color:#888;font-size:.8em;margin-top:8px\">"
        "Anwenden: aktualisiert die tvh autorec-Regel UND alle bereits "
        "geplanten künftigen Aufnahmen mit diesem Titel. Erhöht nur, "
        "reduziert nie (= sicher).</p>';"
        "/* Apply button handler — confirms, POSTs, shows result toast. */"
        "c.querySelectorAll('.psd-apply').forEach(btn=>{"
        "btn.addEventListener('click',async ev=>{"
        "const tr=ev.target.closest('tr'); const tt=tr.dataset.title;"
        "const ex=parseInt(ev.target.dataset.extra,10);"
        "if(!confirm('start_extra='+ex+' min für '+tt+' setzen?')) return;"
        "ev.target.disabled=true; ev.target.textContent='…';"
        "try{const r=await fetch('/api/internal/per-show-drift/apply',"
        "{method:'POST',headers:{'Content-Type':'application/json'},"
        "body:JSON.stringify({title:tt,start_extra:ex})});"
        "const j=await r.json();"
        "if(!j.ok){alert('Fehler: '+(j.error||'?'));ev.target.disabled=false;"
        "ev.target.textContent='Anwenden';return;}"
        "let m='OK · autorec: '+j.autorec_updated+'/'+j.autorec_matched;"
        "if(j.dvr_via_cascade) m+=' · '+j.dvr_via_cascade+' DVR-Entries auto-übernommen via autorec';"
        "else m+=' · DVR-Entries: '+j.dvr_updated+'/'+j.dvr_matched;"
        "if(j.skipped_already_higher) m+=' · skipped (schon höher): '+j.skipped_already_higher;"
        "if(j.no_targets) m='Keine autorec-Regel + keine geplante Aufnahme — wirkt erst beim nächsten manuellen Schedule';"
        "if(j.errors&&j.errors.length) m+=' · Fehler: '+j.errors.join(',');"
        "ev.target.textContent='✓ '+ex+' min'; alert(m);"
        "}catch(e){alert('Netzwerk-Fehler: '+e);ev.target.disabled=false;"
        "ev.target.textContent='Anwenden';}});});"
        "}).catch(e=>document.getElementById('psd-list').textContent="
        "'Fehler: '+e);"
        "</script>")

    if _section_open[0]:
        out.append("</details>")
    # Persist <details> open/closed across page reloads via
    # localStorage. Read state on DOMContentLoaded; intercept each
    # toggle event to save. Defaults from data-default-open survive
    # only if no entry exists for that id yet (= first visit).
    out.append("""<script>
      (function() {
        const KEY = 'learning-section-state';
        let saved = {};
        try { saved = JSON.parse(localStorage.getItem(KEY) || '{}'); } catch(_) {}
        document.querySelectorAll('details.section').forEach(d => {
          const id = d.id;
          if (id in saved) d.open = !!saved[id];
          d.addEventListener('toggle', () => {
            saved[id] = d.open;
            try { localStorage.setItem(KEY, JSON.stringify(saved)); } catch(_) {}
          });
        });
      })();
    </script>""")
    out.append("</body></html>")
    return Response("\n".join(out), mimetype="text/html")


@app.route("/root.crt")
def root_cert():
    """Serve Caddy's local CA root for device trust install."""
    path = HLS_DIR / "caddy-root.crt"
    if not path.exists():
        abort(404, "CA cert not found — copy it to /mnt/tv/caddy-root.crt first")
    resp = send_from_directory(HLS_DIR, "caddy-root.crt",
                                mimetype="application/x-x509-ca-cert",
                                as_attachment=False)
    resp.headers["Content-Disposition"] = 'inline; filename="caddy-root.crt"'
    return resp


# ---------------------------------------------------------------------
# Shared player scaffold. Both the live `/watch/<slug>` player and the
# recording `/recording/<uuid>` player inject these. Each mode still
# defines its own seek(d), refresh(), scrub-bar HTML, plus mode-only
# features (chapters, Mediathek fallback, progress loader, etc.).
#
# Requirements the caller must satisfy BEFORE the base JS runs:
#   - An HTML #v, #chrome, #topbar, #hint in the DOM
#   - const PLAYER_HOME = '...';        // fallback URL on close
#   - function seek(d) {...}            // consumed by double-tap
# ---------------------------------------------------------------------
PLAYER_BASE_CSS = """\
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:#000;color:#eee;
 font-family:-apple-system,BlinkMacSystemFont,sans-serif;
 overflow:hidden;-webkit-user-select:none;user-select:none}
#v{width:100%;height:100%;background:#000;object-fit:contain;display:block}
#chrome{position:fixed;bottom:0;left:0;right:0;
 padding:14px max(16px,env(safe-area-inset-right))
         max(20px,env(safe-area-inset-bottom))
         max(16px,env(safe-area-inset-left));
 background:linear-gradient(transparent,#000d);z-index:10;
 transition:opacity .3s}
#topbar{position:fixed;top:0;left:0;right:0;z-index:10;
 padding:max(12px,env(safe-area-inset-top)) max(16px,env(safe-area-inset-right))
         14px max(16px,env(safe-area-inset-left));
 display:flex;justify-content:space-between;align-items:center;
 background:linear-gradient(#000d,transparent);transition:opacity .3s}
.row{display:flex;align-items:center;gap:6px;margin-top:8px;
 flex-wrap:wrap;row-gap:8px}
/* Minimal-controls mode: hide every button in the row except
   #skipad, keep the #cur timer + scrubbar + title visible. Toggle
   via #ctrlMin in the topbar; state persisted in
   localStorage('player-ctrl-min'). Live-edge is reachable by
   dragging the scrub thumb to the rightmost 2 % (seekTo snaps to
   goLive() in that range). #skipad stays because skipping a 4-min
   ad block by hand on the scrubbar is annoying — but in min mode
   we tone it down so it's not a screaming red CTA. */
body.ctrl-min .row > button,
body.ctrl-min .row > #volume-wrap{display:none}
body.ctrl-min #skipad{background:#0006;color:#ddd;padding:4px 9px;
 font-size:.72em;font-weight:500;border:1px solid #fff3}
.spacer{flex:1 1 auto}
.iconbtn{background:#fff2;color:#fff;border:0;width:34px;height:34px;
 border-radius:17px;font-size:1em;cursor:pointer;display:flex;
 align-items:center;justify-content:center;text-decoration:none;
 flex:0 0 auto;line-height:1;font-variant-emoji:text;
 transition:box-shadow .2s,background .2s;
 /* Mobile Safari: rapid taps on the same button (e.g. user mashing
  ⏩ to scrub forward) get interpreted as double-tap → zoom in.
  touch-action:manipulation disables the 300ms double-tap delay
  AND the zoom gesture for taps inside the button, while still
  permitting pinch-to-zoom elsewhere. */
 touch-action:manipulation;
 -webkit-touch-callout:none}
@media (hover:hover){
 .iconbtn:hover{background:#fff4;
  box-shadow:0 0 10px #7bdcff99,0 0 18px #7bdcff55}
}
.iconbtn:active{opacity:.6}
.iconbtn:disabled{background:#555;color:#bbb;cursor:default}
#volume-wrap{display:inline-flex;align-items:center;gap:0;
 flex:0 0 auto;position:relative}
#volume-wrap #vol-slider{width:0;height:4px;cursor:pointer;
 background:#fff4;border-radius:2px;appearance:none;
 -webkit-appearance:none;outline:none;transition:width .2s,
 margin-left .2s,opacity .2s;opacity:0;margin-left:0;
 accent-color:#fff}
@media (hover:hover){
 #volume-wrap:hover #vol-slider{width:80px;margin-left:6px;opacity:1}
}
#volume-wrap.open #vol-slider{width:80px;margin-left:6px;opacity:1}
#vol-slider::-webkit-slider-thumb{appearance:none;-webkit-appearance:none;
 width:12px;height:12px;border-radius:50%;background:#fff;cursor:pointer}
#vol-slider::-moz-range-thumb{width:12px;height:12px;border-radius:50%;
 background:#fff;border:0;cursor:pointer}
.pill{background:#fff2;color:#fff;border:0;padding:7px 12px;
 border-radius:16px;font-weight:600;font-size:.85em;cursor:pointer;
 display:inline-flex;align-items:center;gap:5px;flex:0 0 auto;line-height:1;
 /* Same Mobile-Safari guard as .iconbtn — rapid taps on the bumper
  ±1s feinjustage buttons get interpreted as double-tap → zoom-in
  without this. Manipulation also kills the 300ms tap-delay so the
  buttons feel snappier. */
 touch-action:manipulation;-webkit-touch-callout:none}
.pill:active{opacity:.7}
.pill:disabled{background:#555;color:#bbb;cursor:default}
/* Skipad keeps its slot in flow at all times so the marker buttons
   to its right (Mark-Mode, ⏵Start, ⏹Ende, ✓Geprüft) don't shift
   left/right whenever the playhead enters/leaves an ad block. The
   off-state is fully invisible (opacity 0 + pointer-events none) so
   it can't be accidentally clicked, but takes up the same space as
   the on-state. */
#skipad{display:inline-flex;opacity:0;pointer-events:none;
 transition:opacity .15s}
#skipad.on{opacity:1;pointer-events:auto}
.time{font-variant-numeric:tabular-nums;font-size:.85em;color:#ddd;
 min-width:44px;text-align:center;flex:0 0 auto}
#scrub{position:relative;width:100%;height:22px;
 cursor:pointer;display:flex;align-items:center}
#track{position:absolute;left:0;right:0;top:50%;transform:translateY(-50%);
 height:3px;background:#fff1;border-radius:2px}
#played{position:absolute;top:0;bottom:0;background:#f44;
 border-radius:2px;width:0;left:0}
#thumb{position:absolute;top:50%;transform:translate(-50%,-50%);
 width:13px;height:13px;background:#fff;border-radius:50%;
 pointer-events:none;box-shadow:0 0 2px #0008;left:0;z-index:2}
.ad-block{position:absolute;top:50%;transform:translateY(-50%);
 height:7px;border-radius:2px;pointer-events:none;
 background:repeating-linear-gradient(45deg,
 #ff8a65,#ff8a65 3px,#4d1c0f 3px,#4d1c0f 6px);
 box-shadow:0 0 0 1px #000a;z-index:1}
.ad-block.editable{pointer-events:auto;cursor:pointer}
.ad-block.editable:hover{height:11px}
.uncertain-mark{position:absolute;top:50%;transform:translate(-50%,-50%);
 width:6px;height:14px;border-radius:1px;background:#f39c12;
 border:1px solid #000;cursor:pointer;pointer-events:auto;
 z-index:3;box-shadow:0 0 3px #f39c12cc}
.uncertain-mark:hover{height:18px;width:8px}
.ad-edit-modal{position:fixed;inset:0;background:#000c;z-index:2000;
 display:flex;align-items:center;justify-content:center;padding:20px}
.ad-edit-card{background:#222;color:#eee;border-radius:10px;
 padding:18px 20px;min-width:260px;max-width:340px;
 box-shadow:0 10px 30px #000c;font-family:-apple-system,sans-serif}
.ad-edit-head{font-weight:700;font-size:1.05em;margin-bottom:14px}
.ad-edit-card label{display:flex;flex-direction:column;font-size:.85em;
 color:#aaa;margin-bottom:10px}
.ad-edit-card input{margin-top:4px;padding:8px 10px;border-radius:6px;
 border:1px solid #444;background:#111;color:#fff;font-size:1em;
 font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.ad-edit-row{display:flex;align-items:center;gap:10px;margin-bottom:10px;
 padding:8px 10px;background:#1a1a1a;border-radius:6px}
.ad-edit-label{color:#888;font-size:.85em;width:42px;flex-shrink:0}
.ad-edit-val{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;
 font-size:1.05em;color:#fff;flex:1;text-align:center}
.ad-edit-grab{padding:8px 12px;border-radius:6px;border:1px solid #2980b9;
 background:#2980b9;color:#fff;font-size:.85em;font-weight:600;
 cursor:pointer;white-space:nowrap}
.ad-edit-grab:active{background:#1f6391}
.ad-edit-hint{font-size:.75em;color:#888;margin-bottom:14px}
/* Live staging bar shown while a block is being marked via START
   (no ENDE yet). Translucent gray so it visually separates from
   confirmed orange ad-blocks. */
.ad-staging{position:absolute;top:50%;transform:translateY(-50%);
 height:9px;border-radius:2px;pointer-events:none;
 background:#7f7f7f88;border:1px dashed #fff8;z-index:2}
.ad-edit-btns{display:flex;gap:8px;align-items:center}
.ad-edit-btns .spacer{flex:1}
.ad-edit-btns button{padding:8px 14px;border-radius:6px;
 border:1px solid #444;font-weight:600;cursor:pointer;
 font-size:.95em;background:#2a2a2a;color:#fff}
.ad-edit-del{background:#c0392b !important;border-color:#c0392b !important}
.ad-edit-cancel{background:transparent !important;font-weight:400 !important}
.ad-edit-save{background:#2980b9 !important;border-color:#2980b9 !important}
.hidden{opacity:0;pointer-events:none}
#ttlrow{margin-top:8px;font-size:.85em;padding-left:4px;
 white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#hint{position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);
 font-size:1.6em;background:#000c;padding:14px 22px;border-radius:12px;
 opacity:0;transition:opacity .2s;pointer-events:none;z-index:20}
#hint.show{opacity:1}
"""

PLAYER_BASE_JS = """\
const v=document.getElementById('v');
const chromeBar=document.getElementById('chrome');
const topbar=document.getElementById('topbar');
const hint=document.getElementById('hint');
const pp=document.getElementById('pp');
const scrub=document.getElementById('scrub');
const played=document.getElementById('played');
const thumb=document.getElementById('thumb');
v.addEventListener('play',()=>pp.textContent='\u23F8');
v.addEventListener('pause',()=>pp.textContent='\u25B6');
let _chromeT=null;
const CHROME_PIN_KEY='player-chrome-pin';
let _chromePinned=false;
try{_chromePinned=localStorage.getItem(CHROME_PIN_KEY)==='1';}catch(e){}
function show(){
  chromeBar.classList.remove('hidden');
  topbar.classList.remove('hidden');
  clearTimeout(_chromeT);
  /* When pinned, leave the chrome visible indefinitely — user
     opted out of the 3.5s auto-hide via the 📌 button. */
  if(_chromePinned)return;
  _chromeT=setTimeout(()=>{
    if(!v.paused){chromeBar.classList.add('hidden');topbar.classList.add('hidden');}
  },3500);
}
function applyChromePin(pinned){
  _chromePinned=!!pinned;
  const btn=document.getElementById('chromePin');
  if(btn){
    btn.textContent=_chromePinned?'📌':'📍';
    btn.setAttribute('aria-label',
      _chromePinned?'Auto-Verbergen aktivieren':'Steuerleiste anpinnen');
    btn.classList.toggle('on',_chromePinned);
  }
  if(_chromePinned){clearTimeout(_chromeT);show();}
}
function toggleChromePin(){
  const next=!_chromePinned;
  try{localStorage.setItem(CHROME_PIN_KEY,next?'1':'0');}catch(e){}
  applyChromePin(next);
}
applyChromePin(_chromePinned);
show();
function toggleFs(){
  const fsEl=document.fullscreenElement||document.webkitFullscreenElement;
  if(fsEl){
    (document.exitFullscreen||document.webkitExitFullscreen).call(document);
    return;
  }
  const root=document.documentElement;
  if(root.requestFullscreen){root.requestFullscreen();return;}
  if(root.webkitRequestFullscreen){root.webkitRequestFullscreen();return;}
  if(v.webkitEnterFullscreen){v.webkitEnterFullscreen();}
}
const CTRL_MIN_KEY='player-ctrl-min';
function applyCtrlMin(min){
  document.body.classList.toggle('ctrl-min',!!min);
  const btn=document.getElementById('ctrlMin');
  if(btn){
    btn.textContent=min?'⊞':'⊟';
    btn.setAttribute('aria-label',
      min?'Steuerleiste einblenden':'Steuerleiste verbergen');
  }
}
function toggleCtrlMin(){
  const min=!document.body.classList.contains('ctrl-min');
  try{localStorage.setItem(CTRL_MIN_KEY,min?'1':'0');}catch(e){}
  applyCtrlMin(min);
}
try{applyCtrlMin(localStorage.getItem(CTRL_MIN_KEY)==='1');}catch(e){}
function closePlayer(ev){
  if(ev)ev.preventDefault();
  try{
    const ref=document.referrer?new URL(document.referrer):null;
    if(ref&&ref.origin===location.origin&&history.length>1){
      history.back();return false;
    }
  }catch(e){}
  location.href=PLAYER_HOME;
  return false;
}
// Scrub-bar drag (mouse + touch). Visual-only during drag — actual
// seek happens once on release. Setting v.currentTime per mousemove
// (~30+/s) drowns hls.js in segment-fetch requests and makes drag
// feel laggy/jumpy. With visual-only drag the thumb tracks the
// pointer at full DOM speed and a single seekTo() commits at the end.
// Each per-mode refresh() guards thumb.style.left + played.style.*
// behind `if(!_dragging)` so the playback ticker doesn't fight the
// drag position mid-gesture.
let _dragging=false;
function _dragVisual(ev){
  const r=scrub.getBoundingClientRect();
  const cx=(ev.touches?ev.touches[0]:ev).clientX;
  const x=Math.max(0,Math.min(r.width,cx-r.left));
  const pct=(x/r.width)*100;
  thumb.style.left=pct+'%';
  const playL=parseFloat(played.style.left)||0;
  played.style.width=Math.max(0,pct-playL)+'%';
  const[ws,we]=scrubWindow();
  const w=ws+(pct/100)*(we-ws);
  if(isFinite(w)&&w>0)cur.textContent=new Date(w*1000).toLocaleTimeString(
    'de-DE',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
scrub.addEventListener('mousedown',e=>{_dragging=true;_dragVisual(e);show();});
window.addEventListener('mousemove',e=>{if(_dragging){_dragVisual(e);show();}});
window.addEventListener('mouseup',e=>{if(_dragging){_dragging=false;seekTo(e);}});
scrub.addEventListener('touchstart',e=>{_dragging=true;_dragVisual(e);show();},{passive:true});
scrub.addEventListener('touchmove',e=>{if(_dragging){_dragVisual(e);show();}},{passive:true});
scrub.addEventListener('touchend',e=>{
  if(!_dragging)return;
  _dragging=false;
  /* touchend's .touches is empty — synthesize from changedTouches so
     seekTo() can pull a clientX out of it. */
  const t=e.changedTouches&&e.changedTouches[0];
  if(t)seekTo({touches:[t],clientX:t.clientX});
},{passive:true});
let _lastTapT=0,_lastTapX=0,_singleTapT=null,_lastTouchT=0;
v.addEventListener('touchend',e=>{
  if(!e.changedTouches[0])return;
  _lastTouchT=Date.now();
  /* Autoplay-muted recovery: first tap only unmutes, doesn't also
     pause via togglePlay. Once audible, subsequent taps work
     normally. */
  if(v.muted&&!v.paused){
    v.muted=false;show();return;
  }
  const now=_lastTouchT;
  const x=e.changedTouches[0].clientX;
  if(now-_lastTapT<300&&Math.abs(x-_lastTapX)<60){
    /* Double-tap: seek 10s. Cancel pending single-tap toggle. */
    if(_singleTapT){clearTimeout(_singleTapT);_singleTapT=null;}
    const delta=x<window.innerWidth/2?-10:10;
    seek(delta);
    hint.textContent=(delta<0?'\u23EA ':'\u23E9 ')+Math.abs(delta)+' s';
    hint.classList.add('show');
    setTimeout(()=>hint.classList.remove('show'),600);
    _lastTapT=0;
  }else{
    _lastTapT=now;_lastTapX=x;
    /* Single tap: defer togglePlay until the double-tap window
       closes so a quick second tap can still seek instead. */
    _singleTapT=setTimeout(()=>{
      _singleTapT=null;
      togglePlay();show();
    },280);
  }
},{passive:true});
/* Desktop: click toggles playback. Skip when a touch just fired so
   we don't double-trigger via the synthetic click that follows
   touchend on mobile. */
v.addEventListener('click',()=>{
  if(Date.now()-_lastTouchT<500)return;
  if(v.muted&&!v.paused){v.muted=false;show();return;}
  togglePlay();show();
});
/* --- Volume + mute control (persisted to localStorage) --------- */
const volWrap=document.getElementById('volume-wrap');
if(volWrap){
  const volIcon=document.getElementById('vol-icon');
  const volSlider=document.getElementById('vol-slider');
  const VOL_KEY='playerVolume';
  let storedVol=1,storedMuted=false;
  try{
    const raw=JSON.parse(localStorage.getItem(VOL_KEY)||'null');
    if(raw){
      if(typeof raw.volume==='number')storedVol=Math.max(0,Math.min(1,raw.volume));
      storedMuted=!!raw.muted;
    }
  }catch(e){}
  function updateVolIcon(){
    if(v.muted||v.volume===0)volIcon.textContent='\U0001F507';
    else if(v.volume<0.5)volIcon.textContent='\U0001F509';
    else volIcon.textContent='\U0001F50A';
    volSlider.value=Math.round((v.muted?0:v.volume)*100);
  }
  function saveVol(){
    try{localStorage.setItem(VOL_KEY,
      JSON.stringify({volume:v.volume,muted:v.muted}));}catch(e){}
  }
  v.volume=storedVol;
  /* Only restore an explicit muted=true preference. If storedMuted is
     false we leave v.muted whatever the HTML attribute set (typically
     true for autoplay) so the browser actually starts playback —
     restoring muted=false on a freshly-loaded page would override the
     autoplay-muted attribute and trigger the autoplay-policy block. */
  if(storedMuted)v.muted=true;
  volIcon.addEventListener('click',e=>{
    e.stopPropagation();
    v.muted=!v.muted;
    if(!v.muted&&v.volume===0)v.volume=0.5;
    updateVolIcon();saveVol();show();
  });
  volSlider.addEventListener('input',e=>{
    const pct=parseInt(volSlider.value,10)/100;
    v.volume=pct;
    v.muted=(pct===0);
    updateVolIcon();saveVol();show();
  });
  v.addEventListener('volumechange',updateVolIcon);
  updateVolIcon();
}
"""


PLAYER_HEAD_META = (
    "<meta name='viewport' content='width=device-width,"
    "initial-scale=1,user-scalable=no,viewport-fit=cover'>"
    "<meta name='color-scheme' content='dark'>"
)


@app.route("/watch/<slug>")
def watch_player(slug):
    """Fullscreen HTML player with swipe-to-switch-channel gestures."""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        abort(404, "unknown channel")
    # Mediathek-live channels (das-erste-hd, etc) get the same VOD-style
    # tap policy as the recordings player — only center-tap pauses, taps
    # elsewhere just reveal/hide chrome. True tvheadend live channels keep
    # the legacy tap-anywhere-pauses (occasionally useful to freeze a
    # broadcast frame for a moment).
    is_mediathek_live = "true" if slug in MEDIATHEK_LIVE else "false"
    return f"""<!doctype html>
<html><head>{PLAYER_HEAD_META}<title>{info['name']}</title>
<script src="https://cdn.jsdelivr.net/npm/hls.js@1/dist/hls.min.js"></script>
<style>
{PLAYER_BASE_CSS}
body{{touch-action:pan-y}}
#avail{{position:absolute;top:0;bottom:0;background:#fff4;
  border-radius:2px;width:0;left:0}}
.chapter{{position:absolute;top:50%;transform:translate(-50%,-50%);
  width:6px;height:16px;background:#fff;border-radius:2px;
  opacity:.85;box-shadow:0 0 2px #000a;z-index:1;
  pointer-events:none}}
.chapter.current{{background:#ffd84d;height:18px}}
/* Same anti-jitter pattern as the recordings player: keep the
   slot in flow, just hide via opacity when not active so the live
   chrome doesn't reflow each time the player crosses an ad
   boundary. */
#skipad{{background:#e74c3c;color:#fff;padding:7px 12px;border:0;
  border-radius:16px;font-weight:600;font-size:.85em;cursor:pointer;
  display:inline-flex;opacity:0;pointer-events:none;flex:0 0 auto;
  transition:opacity .15s}}
#skipad.on{{opacity:1;pointer-events:auto}}
.chname{{font-weight:600;color:#fff;margin-right:8px}}
.epg{{color:#bbb;margin-left:8px}}
#srcbadge{{display:inline-block;font-size:.75em;font-weight:600;
  padding:2px 8px;border-radius:10px;background:#7a7a7a;color:#fff;
  vertical-align:1px}}
#srcbadge.mediathek{{background:#2980b9}}
#srcbadge.rec{{background:#c0392b}}
#srcbadge.at-live{{background:#27ae60}}
#unmute{{position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);
  z-index:30;background:#fff;color:#000;padding:14px 22px;
  border-radius:30px;font-weight:600;font-size:1em;border:0;cursor:pointer;
  display:none}}
</style></head><body>
<video id='v' autoplay muted playsinline webkit-playsinline
 disablepictureinpicture></video>
<button id='unmute' onclick='doUnmute()'>🔊 Ton an</button>
<div id='topbar'>
 <button class='iconbtn' onclick='toggleFs()' aria-label='Vollbild'>⛶</button>
 <button id='ctrlMin' class='iconbtn' onclick='toggleCtrlMin()'
   aria-label='Steuerleiste verbergen'>⊟</button>
 <button id='chromePin' class='iconbtn' onclick='toggleChromePin()'
   aria-label='Steuerleiste anpinnen'>📍</button>
 <a class='iconbtn' href='{HOST_URL}/' aria-label='Schließen' onclick='return closePlayer(event)'>✕</a>
</div>
<div id='chrome'>
 <div id='scrub'>
  <div id='track'>
   <div id='avail'></div>
   <div id='played'></div>
  </div>
  <div id='thumb'></div>
 </div>
 <div class='row'>
  <button id='restartBtn' class='iconbtn' onclick='goShowStart()'
   aria-label='Sendung von Anfang'>⏮</button>
  <button class='iconbtn' onclick='seek(-10)' aria-label='-10 s'>⏪</button>
  <button id='pp' class='iconbtn' onclick='togglePlay()'
   aria-label='Play/Pause'>▶</button>
  <button class='iconbtn' onclick='seek(10)' aria-label='+10 s'>⏩</button>
  <button id='liveBtn' class='iconbtn' onclick='goShowNext()'
   aria-label='Zur nächsten Sendung'>⏭</button>
  <button id='skipad' onclick='skipCurrentAd()'>Werbung ⏭</button>
  <button class='iconbtn' onclick='prevCh()' aria-label='voriger Kanal'>◀︎</button>
  <button class='iconbtn' onclick='nextCh()' aria-label='nächster Kanal'>▶︎</button>
  <span id='volume-wrap'>
   <button id='vol-icon' class='iconbtn' aria-label='Lautstärke'>🔊</button>
   <input type='range' id='vol-slider' min='0' max='100' value='100'>
  </span>
  <span id='cur' class='time'>0:00</span>
 </div>
 <div id='ttlrow'>
  <span class='chname' id='chname'>{info['name']}</span>
  <span id='srcbadge'>Live</span>
  <span class='epg' id='epg'>…</span>
 </div>
</div>
<div id='hint'></div>

<script>
const HOST='{HOST_URL}';
const PLAYER_HOME=HOST+'/';
let channels=[];let idx=0;let current='{slug}';

// Shared scaffold: element refs, show(), toggleFs, closePlayer,
// double-tap seek handler. Mode-specific `function seek(d)` is defined
// further down and reached at call-time via function hoisting.
{PLAYER_BASE_JS}
/* VOD-style tap policy override (mediathek-live channels only) — same
   center-circle gate as the recordings player. PLAYER_BASE_JS toggles
   play/pause on any tap, which is fine for live tvheadend channels but
   wrong for mediathek-live where you typically tap to reveal/hide
   chrome rather than pause. Only the central ~18 % radius zone (= iOS
   native player's play-button area) toggles play/pause; outside taps
   just toggle the chrome bar. */
if({is_mediathek_live}){{
  function _isCenterTap(clientX,clientY){{
    const r=v.getBoundingClientRect();
    const cx=r.left+r.width/2, cy=r.top+r.height/2;
    const dx=clientX-cx, dy=clientY-cy;
    const radius=Math.min(r.width,r.height)*0.18;
    return dx*dx+dy*dy <= radius*radius;
  }}
  v.addEventListener('click',ev=>{{
    ev.stopImmediatePropagation();
    ev.preventDefault();
    if(Date.now()-_lastTouchT<500)return;
    if(v.muted&&!v.paused){{v.muted=false;show();return;}}
    if(_isCenterTap(ev.clientX,ev.clientY)) togglePlay();
    if(chromeBar.classList.contains('hidden'))show();
    else {{chromeBar.classList.add('hidden');
      topbar.classList.add('hidden');}}
  }},true);
  v.style.touchAction='manipulation';
  v.addEventListener('dblclick',ev=>ev.preventDefault());
  v.addEventListener('touchend',ev=>{{
    if(!ev.changedTouches[0])return;
    const cx=ev.changedTouches[0].clientX;
    const cy=ev.changedTouches[0].clientY;
    setTimeout(()=>{{
      if(!_singleTapT)return;
      if(_isCenterTap(cx,cy))return;
      clearTimeout(_singleTapT);_singleTapT=null;
      if(chromeBar.classList.contains('hidden'))show();
      else {{chromeBar.classList.add('hidden');
        topbar.classList.add('hidden');}}
    }},0);
  }});
}}
const chname=document.getElementById('chname');
const epgEl=document.getElementById('epg');
const unmuteBtn=document.getElementById('unmute');
const liveBtn=document.getElementById('liveBtn');
const srcBadge=document.getElementById('srcbadge');
function setSource(which){{
  srcBadge.classList.remove('mediathek','at-live','rec');
  if(which==='mediathek'){{
    const ch=channels[idx];
    const slug=(ch&&ch.slug)||current||'';
    let label='ARD Mediathek';
    if(slug.startsWith('zdf')||slug==='3sat-hd')label='ZDF Mediathek';
    else if(slug==='arte-hd')label='arte.tv';
    else if(slug==='kika-hd')label='KiKA';
    srcBadge.textContent=label;
    srcBadge.classList.add('mediathek');
  }} else if(which==='rec'){{
    srcBadge.textContent='● Aufnahme';
    srcBadge.classList.add('rec');
  }} else {{
    srcBadge.textContent='Live';
  }}
}}
function updateLiveBadge(){{
  if(srcBadge.classList.contains('mediathek'))return;
  if(isAtLive())srcBadge.classList.add('at-live');
  else srcBadge.classList.remove('at-live');
}}
const cur=document.getElementById('cur');
const avail=document.getElementById('avail');
const WINDOW=7200;  /* fixed 2h scrub coordinate system */

function fmt(s){{
  if(!isFinite(s)||s<0)s=0;
  const m=Math.floor(s/60),ss=Math.floor(s%60);
  return m+':'+String(ss).padStart(2,'0');
}}
function seekableRange(){{
  if(v.seekable&&v.seekable.length){{
    return [v.seekable.start(0),v.seekable.end(0)];
  }}
  return [0,0];
}}
function isAtLive(){{
  const[s,e]=seekableRange();
  return (e-s)<2||(e-(v.currentTime||0))<8;
}}
/* Scrub bar coordinate system: fixed 2h window ending at "now".
   Both seekable content and chapter markers are mapped into it. */
let recChain=null;       /* array of chain entries (uuid/start/stop/hls_url) */
let recCurrentIdx=-1;    /* index into recChain of the currently loaded VOD */
let recWindow=null;      /* [chain_start_ts, chain_stop_or_now_ts] */
function scrubWindow(){{
  if(onRecording&&recWindow){{
    return [recWindow[0],recWindow[1]];
  }}
  const now=Date.now()/1000;
  /* Live view but we know there's a recording chain: extend scrub
     window leftward to the chain's earliest recording so the user
     sees every chain boundary without having to jump in first. */
  if(recChain&&recChain.length){{
    const earliest=recChain[0].start;
    return [Math.min(now-WINDOW,earliest),now];
  }}
  return [now-WINDOW,now];
}}
let recChainAds=[];   /* array of [wallStart,wallStop] from chain's comskip */
function loadRecordingChain(slug){{
  return fetch(HOST+'/api/recording-window/'+slug).then(r=>r.json()).then(d=>{{
    if(slug!==current)return;
    const ch=(d&&d.chain)||[];
    recChain=ch.length?ch:null;
    recChainAds=[];
    if(recChain){{
      for(const rec of recChain){{
        fetch(HOST+'/recording/'+rec.uuid+'/ads').then(r=>r.json()).then(a=>{{
          if(slug!==current)return;
          for(const[s,e]of(a.ads||[])){{
            recChainAds.push([rec.start+s,rec.start+e]);
          }}
          renderLiveAds();
        }}).catch(()=>{{}});
      }}
    }}
    renderChapters();renderLiveAds();
  }}).catch(()=>{{}});
}}
function scrubPct(wallTs){{
  const[ws,we]=scrubWindow();
  return Math.max(0,Math.min(100,((wallTs-ws)/(we-ws))*100));
}}
function refresh(){{
  const[s,e]=seekableRange();
  const curWall=wallAt(v.currentTime||0);
  let seekStartW,seekEndW;
  if(onRecording&&recChain&&recChain.length){{
    seekStartW=recChain[0].start;
    const last=recChain[recChain.length-1];
    seekEndW=last.running?Date.now()/1000:last.stop;
  }} else {{
    seekStartW=e>s?wallAt(s):curWall;
    seekEndW  =e>s?wallAt(e):curWall;
  }}
  const availL=scrubPct(seekStartW);
  const availR=scrubPct(seekEndW);
  avail.style.left=availL+'%';
  avail.style.width=(availR-availL)+'%';
  const playL=scrubPct(seekStartW);
  const playR=scrubPct(curWall);
  played.style.left=playL+'%';
  if(!_dragging){{
    played.style.width=Math.max(0,playR-playL)+'%';
    thumb.style.left=scrubPct(curWall)+'%';
  }}
  /* Show wall-clock time of current playback position, not offset to
     live edge — more intuitive and doesn't suffer from segment-jitter. */
  if(!pauseState){{
    if(isAtLive()){{
      cur.textContent='live';
    }} else {{
      const w=curWall;
      if(isFinite(w)&&w>0){{
        cur.textContent=new Date(w*1000).toLocaleTimeString('de-DE',
          {{hour:'2-digit',minute:'2-digit',second:'2-digit'}});
      }}
    }}
  }}
}}
v.addEventListener('timeupdate',()=>{{refresh();updateLiveBadge();refreshSkipAdBtn();}});
v.addEventListener('progress',()=>{{refresh();renderChapters();renderLiveAds();}});
v.addEventListener('loadeddata',()=>{{refresh();renderChapters();renderLiveAds();}});
v.addEventListener('canplay',()=>{{refresh();renderChapters();renderLiveAds();}});
v.addEventListener('loadedmetadata',()=>{{
  if(!onMediathek&&!onRecording)goLive();
  refresh();renderChapters();renderLiveAds();
}});
setInterval(()=>{{if(!v.paused){{renderChapters();renderLiveAds();}}}},5000);
/* Live-ads delivery: SSE stream that pushes a new payload whenever
   the Mac scanner saves a fresh .live_ads.json. Replaces the prior
   30 s polling — skip button now appears with the detection latency
   instead of +0-30 s extra. EventSource auto-reconnects on transient
   network errors, so no manual fallback timer is needed. */
let _liveAdsES=null;
let _liveAdsESSlug=null;
function subscribeLiveAds(slug){{
  if(_liveAdsESSlug===slug&&_liveAdsES)return;
  unsubscribeLiveAds();
  _liveAdsESSlug=slug;
  loadLiveAds(slug);  /* paint immediately, don't wait for first push */
  try{{
    const es=new EventSource(HOST+'/api/live-ads-stream/'+slug);
    es.onmessage=(e)=>{{
      if(slug!==current)return;
      try{{
        const d=JSON.parse(e.data);
        liveAds=d.ads||[];
        renderLiveAds();refreshSkipAdBtn();
      }}catch(err){{}}
    }};
    _liveAdsES=es;
  }}catch(err){{}}
}}
function unsubscribeLiveAds(){{
  if(_liveAdsES){{try{{_liveAdsES.close();}}catch(e){{}}_liveAdsES=null;}}
  _liveAdsESSlug=null;
}}
function goLive(){{
  /* Mediathek-live channel: stay inside the Mediathek HLS and just
     seek to its live edge — avoids spawning a DVB-C tuner. hls.js
     needs stopLoad + startLoad to realign with fresh segments,
     otherwise quick consecutive seeks can freeze on a stale frame. */
  if(onMediathek){{
    const[s,e]=seekableRange();
    if(e>s){{
      const target=Math.max(s+0.5,e-4);
      if(hlsInst){{
        try{{hlsInst.stopLoad();hlsInst.startLoad(target);}}catch(err){{}}
      }}
      v.currentTime=target;
      v.play().catch(()=>{{}});
      /* Safety retry: if playback isn't progressing after 800 ms,
         nudge currentTime to the latest edge and replay. */
      setTimeout(()=>{{
        if(!onMediathek||v.paused)return;
        const[,ee]=seekableRange();
        if(ee>target+3&&Math.abs(v.currentTime-target)<0.3){{
          v.currentTime=Math.max(s+0.5,ee-3);
          v.play().catch(()=>{{}});
        }}
      }},800);
      show();return;
    }}
  }}
  if(onMediathek||onRecording){{
    onMediathek=false;onRecording=false;
    recWindow=null;recChain=null;recCurrentIdx=-1;
    loadDvbcSrc(current);
    v.addEventListener('loadedmetadata',()=>{{
      const[,e]=seekableRange();
      if(e>0)v.currentTime=e-2;
      v.play().catch(()=>{{}});
    }},{{once:true}});
    return;
  }}
  const[,e]=seekableRange();
  if(e>0)v.currentTime=e-2;
}}
/* ⏭ acts as "next show": advance to the start of the next show
   (chain / Mediathek / live currently-airing). At the end of the
   chain or when caught up, falls back to live edge. */
function goShowNext(){{
  /* Recording-chain merged-targets walk forward — symmetric. */
  if(onRecording&&recChain&&recCurrentIdx>=0){{
    const curWall=recChain[recCurrentIdx].start+(v.currentTime||0);
    const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
    const cutoffWall=recentJump?_lastJumpedWall:curWall;
    const lastEntry=recChain[recChain.length-1];
    const chainEnd=lastEntry.running?Date.now()/1000:lastEntry.stop;
    const targets=[];
    for(const a of recChainAds){{
      if(a[1]>cutoffWall+0.5&&a[1]<=chainEnd)targets.push({{wall:a[1],label:'ad-end'}});
    }}
    for(const epg of epgEvents){{
      if(epg.start>cutoffWall+0.5&&epg.start<=chainEnd)targets.push({{wall:epg.start,label:'epg-start',ev:epg}});
    }}
    targets.sort((x,y)=>x.wall-y.wall);
    if(targets.length){{
      const pick=targets[0];
      _lastJumpedEv=pick.ev||_lastJumpedEv;_lastJumpedTs=Date.now();_lastJumpedKind='walk';_lastJumpedWall=pick.wall;
      const idx=recChain.findIndex(c=>c.start<=pick.wall&&pick.wall<c.stop);
      if(idx===recCurrentIdx){{
        v.currentTime=pick.wall-recChain[recCurrentIdx].start;show();
      }} else if(idx>=0){{
        switchToRecording(recChain[idx],pick.wall,idx);
      }} else {{
        goLive();return;
      }}
      const hints={{'epg-start':'Sprung an nächsten Sendungsanfang','ad-end':'Sprung an Werbeblock-Ende'}};
      showHintMsg(hints[pick.label]||'Sprung vorwärts',2500);
      return;
    }}
    /* No more targets in chain — go live if last entry is still running */
    if(lastEntry.running)goLive();
    else showHintMsg('Ende der Aufnahme');
    return;
  }}
  /* Mediathek: jump forward to the next reachable EPG event. */
  if(onMediathek){{
    let cur=currentEvent();
    const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
    if(recentJump)cur=_lastJumpedEv;
    if(cur){{
      const next=epgEvents.find(e=>e.start>=cur.stop);
      if(next&&isReachable(next)){{jumpToEvent(next);return;}}
    }}
    goLive();return;
  }}
  /* Live-buffer step-forward, unified merged-targets walk —
     symmetric to goShowStart. Same target set, same _lastJumpedWall
     cutoff. Picks earliest target after current. Falls through to
     goLive() when nothing's left forward. */
  const recentJumpFwd=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
  fetch(HOST+'/api/live-ads/'+current).then(r=>r.json()).then(adResp=>{{
    const ads=adResp.ads||[];
    const[s,e]=seekableRange();
    if(e<=s+1){{goLive();return;}}
    const nowS=Date.now()/1000;
    const wallS=nowS-(e-s),wallE=nowS;
    const cutoffWall=recentJumpFwd?_lastJumpedWall:wallForCurrent();
    const targets=[];
    for(const a of ads){{
      if(a[1]>cutoffWall+0.5&&a[1]<=wallE)targets.push({{wall:a[1],label:'ad-end'}});
    }}
    for(const epg of epgEvents){{
      if(epg.start>cutoffWall+0.5&&epg.start<=wallE)targets.push({{wall:epg.start,label:'epg-start',ev:epg}});
    }}
    targets.sort((x,y)=>x.wall-y.wall);
    if(targets.length){{
      const pick=targets[0];
      _lastJumpedEv=pick.ev||_lastJumpedEv;
      _lastJumpedTs=Date.now();_lastJumpedKind='walk';_lastJumpedWall=pick.wall;
      v.currentTime=Math.max(s+0.5,Math.min(e-1,pick.wall-wallS));
      show();
      const hints={{'epg-start':'Sprung an nächsten Sendungsanfang','ad-end':'Sprung an Werbeblock-Ende'}};
      showHintMsg(hints[pick.label]||'Sprung vorwärts',2500);
      return;
    }}
    goLive();
  }}).catch(()=>goLive());
}}
let onRecording=false;
/* When the recording-VOD ends (player reached its final segment),
   switch back to the live stream. Guard against spurious events
   during source swaps by requiring currentTime near duration. */
v.addEventListener('ended',()=>{{
  if(!onRecording)return;
  const dur=v.duration;
  if(!isFinite(dur)||dur<=0)return;
  if(Math.abs(v.currentTime-dur)>2)return;
  /* Advance to the next recording in the chain, if any; else go live. */
  if(recChain&&recCurrentIdx>=0&&recCurrentIdx<recChain.length-1){{
    const nextIdx=recCurrentIdx+1;
    switchToRecording(recChain[nextIdx],recChain[nextIdx].start,nextIdx);
    return;
  }}
  goLive();
}});
/* Safari HLS exposes getStartDate() = wall-clock time of currentTime=0
   (from #EXT-X-PROGRAM-DATE-TIME). That gives reliable mapping between
   video time and real-world time. Fallback: Date.now() - (end-ct). */
function wallAt(t){{
  if(onRecording&&recChain&&recCurrentIdx>=0){{
    /* Active VOD is recChain[recCurrentIdx]; its t=0 maps to that
       entry's start wall-clock time. */
    return recChain[recCurrentIdx].start+(t||0);
  }}
  if(typeof v.getStartDate==='function'){{
    const d=v.getStartDate();
    /* Safari returns epoch-0 before the playlist is loaded — filter
       out anything before 2020 as "not yet available". */
    if(d&&d.getTime()>1577836800000)return d.getTime()/1000+t;
  }}
  const[,e]=seekableRange();
  return Date.now()/1000-(e-t);
}}
function posForWall(ts){{
  const[ws,we]=scrubWindow();
  if(ts<ws||ts>we)return null;
  return ((ts-ws)/(we-ws))*100;
}}
function wallForCurrent(){{
  /* iOS Safari on a live HLS stream initially reports currentTime as
     Number.MAX_VALUE until the player actually settles to a position
     — clamp to the seekable live edge so wallAt doesn't overflow. */
  let t=v.currentTime;
  if(!isFinite(t)||t>1e10){{
    const[,e]=seekableRange();
    t=e>0?e-1:0;
  }}
  return wallAt(t||0);
}}
let epgEvents=[];
let mediathekAvailable=false;
let mediathekWindow=0;
function isReachable(ev){{
  /* Recording-chain: any event overlapping the full chain window. */
  if(onRecording&&recChain&&recChain.length){{
    const first=recChain[0];
    const last=recChain[recChain.length-1];
    const chainEnd=last.running?Date.now()/1000:last.stop;
    if(ev.start>=first.start-2&&ev.start<chainEnd)return true;
  }}
  /* Local: event start must be inside currently-seekable range.  */
  const[ss,ee]=seekableRange();
  if(ee>ss){{
    const wallStart=wallAt(ss);
    if(ev.start>=wallStart-2)return true;
  }}
  /* Mediathek: channel supported + event within the CDN DVR window
     (arte 30 min, ARD 2 h, ZDF 3 h — value per channel).  */
  if(mediathekAvailable&&ev.start>=Date.now()/1000-mediathekWindow)return true;
  return false;
}}
/* Gate chapter rendering until both Mediathek-availability and
   the recording-chain fetches have resolved — otherwise the ticks
   appear but tapping them would race the unfinished prerequisites. */
let _chaptersReady=false;
function renderChapters(){{
  document.querySelectorAll('.chapter').forEach(el=>el.remove());
  if(!_chaptersReady)return;
  const nowWall=Date.now()/1000;
  for(const ev of epgEvents){{
    if(!isReachable(ev))continue;
    const pct=posForWall(ev.start);
    if(pct===null)continue;
    const el=document.createElement('div');
    el.className='chapter';
    el.dataset.start=ev.start;
    if(ev.start<=nowWall&&nowWall<ev.stop)el.classList.add('current');
    el.style.left=pct+'%';
    const ts=new Date(ev.start*1000).toLocaleTimeString('de-DE',{{hour:'2-digit',minute:'2-digit'}});
    el.title=ev.title+' ('+ts+')';
    scrub.appendChild(el);
  }}
}}
/* Tap-routing: chapter ticks are visual-only (pointer-events:none).
   On a tap, find the nearest chapter within CHAPTER_TAP_PX of the
   pointer; if one's close enough, jump to that EPG event instead of
   raw scrub seek. Solves the overlap problem on narrow mobile bars
   where adjacent ticks would otherwise compete for the same hit
   area — closest-to-pointer always wins, no DOM-order ambiguity. */
const CHAPTER_TAP_PX=22;
function nearestChapter(clientX){{
  let best=null,bestDist=CHAPTER_TAP_PX;
  document.querySelectorAll('.chapter').forEach(el=>{{
    const r=el.getBoundingClientRect();
    const cx=r.left+r.width/2;
    const d=Math.abs(cx-clientX);
    if(d<bestDist){{bestDist=d;best=el;}}
  }});
  return best;
}}
function loadMediathekAvail(slug){{
  return fetch(HOST+'/api/mediathek-live/'+slug).then(r=>r.json()).then(d=>{{
    if(slug!==current)return;
    mediathekAvailable=!!d.url;
    mediathekWindow=d.window||0;
    renderChapters();
  }}).catch(()=>{{mediathekAvailable=false;mediathekWindow=0;}});
}}
/* EPG start times are when the broadcast SLOT begins, but actual
   content (after station ID / trailers) often starts 10-30 s later.
   Seek a bit before the EPG start so we never miss the opener. ZDF
   tends to have longer slot-padding than ARD, so give it more room.
   KiKa has unusually long branding/trailer pre-rolls (~1:50) before
   each show — use a negative lead-in to skip past them. */
function eventLeadIn(slug){{
  if(slug==='kika-hd')return 0;  /* land exactly at the EPG slot — user can scrub through KiKa's branding pre-roll */
  return slug.startsWith('zdf')||slug==='3sat-hd'?5:15;
}}
/* Per-channel offset added to the authoritative broadcast start. ZDFs
   API reports the EPG slot as "effective start" even when the show
   itself begins ~10 s later after a station ID — so we nudge forward. */
function postRollAdjust(slug){{
  if(slug==='zdf-hd'||slug==='zdfinfo-hd'||slug==='zdfneo-hd'
     ||slug==='3sat-hd'||slug==='arte-hd'||slug==='kika-hd'
     ||slug==='phoenix-hd')return 10;
  return 0;
}}
function doJump(actualStart,fromAuthoritative){{
  const leadIn=fromAuthoritative?0:eventLeadIn(current);
  const postRoll=fromAuthoritative?postRollAdjust(current):0;
  const targetWall=actualStart-leadIn+postRoll;
  const[s,e]=seekableRange();
  const wallS=e>s?wallAt(s):null;
  if(!onRecording&&wallS!==null&&targetWall>=wallS){{
    v.currentTime=Math.max(s+0.5,Math.min(e-1,targetWall-wallS));
    show();return;
  }}
  /* Not in live buffer — if the target falls inside a known chain
     recording, load that VOD chain. */
  if(recChain&&recChain.length){{
    const idx=recChain.findIndex(c=>c.start<=targetWall&&targetWall<c.stop);
    if(idx>=0){{
      activateRecordingChain(recChain,idx,targetWall);
      return;
    }}
  }}
  fetch(HOST+'/api/mediathek-live/'+current)
   .then(r=>r.json()).then(d=>{{
     if(!d.url){{
       hint.textContent='Sendung nicht im Puffer';
       hint.classList.add('show');
       setTimeout(()=>hint.classList.remove('show'),2000);
       return;
     }}
     /* Chapter-tap: no safety offset — the Now-Next / EPG time is
        already the intended start point. */
     switchToMediathek(d.url,targetWall,0);
   }}).catch(()=>{{}});
}}
let _lastJumpedEv=null,_lastJumpedTs=0,_lastJumpedKind=null,_lastJumpedWall=0;
function jumpToEvent(ev){{
  _lastJumpedEv=ev;_lastJumpedTs=Date.now();
  fetch(HOST+'/api/show-actual-start/'+current+'?ts='+ev.start)
   .then(r=>r.json()).then(d=>{{
     const auth=(d.source==='ard-nownext'||d.source==='zdf-getepg');
     doJump(d.actual||ev.start,auth);
   }})
   .catch(()=>doJump(ev.start,false));
}}
function currentEvent(){{
  const w=wallForCurrent();
  for(const ev of epgEvents){{
    if(ev.start<=w&&w<ev.stop)return ev;
  }}
  return null;
}}
let onMediathek=false;
function localSrc(){{return HOST+'/hls/'+current+'/dvr.m3u8';}}
function showHintMsg(msg,ms,kind){{
  /* Default kind='info' → no-op. Only kind='error' actually shows the
     toast. The "Sprung an Werbeblock-Ende" / "Sendungsanfang" /
     "Anfang der Aufnahme" etc. cues felt noisy in the player; the
     visual scrub jump itself is enough confirmation that the action
     worked. Errors stay visible because they're actionable. */
  if(kind!=='error')return;
  hint.textContent=msg;hint.classList.add('show');
  setTimeout(()=>hint.classList.remove('show'),ms||2500);
}}
async function goShowStart(){{
  /* If we're already in recording-chain mode and near the start of
     the current chain member, step back to the previous one. */
  if(onRecording&&recChain&&recCurrentIdx>=0){{
    /* Recording-chain merged-targets walk — same logic as live-buffer,
       but using recChainAds (already in wall-time) and switching
       between chain entries via switchToRecording when a target lies
       in a different recording. */
    const curWall=recChain[recCurrentIdx].start+(v.currentTime||0);
    const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
    const cutoffWall=recentJump?_lastJumpedWall:curWall;
    const chainStart=recChain[0].start;
    const targets=[];
    for(const a of recChainAds){{
      if(a[1]>=chainStart&&a[1]<=cutoffWall-0.5)targets.push({{wall:a[1],label:'ad-end'}});
    }}
    for(const epg of epgEvents){{
      if(epg.start>=chainStart&&epg.start<=cutoffWall-0.5)targets.push({{wall:epg.start,label:'epg-start',ev:epg}});
    }}
    targets.sort((x,y)=>y.wall-x.wall);
    if(targets.length){{
      const pick=targets[0];
      _lastJumpedEv=pick.ev||_lastJumpedEv;_lastJumpedTs=Date.now();_lastJumpedKind='walk';_lastJumpedWall=pick.wall;
      const idx=recChain.findIndex(c=>c.start<=pick.wall&&pick.wall<c.stop);
      if(idx===recCurrentIdx){{
        v.currentTime=pick.wall-recChain[recCurrentIdx].start;show();
      }} else if(idx>=0){{
        switchToRecording(recChain[idx],pick.wall,idx);
      }} else {{
        showHintMsg('Sprungziel nicht in Aufnahme',null,'error');return;
      }}
      const hints={{'epg-start':'Sprung an vorherigen Sendungsanfang','ad-end':'Sprung an Werbeblock-Ende'}};
      showHintMsg(hints[pick.label]||'Sprung zurück',2500);
      return;
    }}
    showHintMsg('Anfang der Aufnahme');
    return;
  }}
  /* Mediathek mode: same step-back idea. If we're near the current
     event's start, jump to the previous (reachable) EPG event. Uses
     _lastJumpedEv as the "current" anchor during the ~2 s window
     right after a jump (before Mediathek playback has settled),
     otherwise a quick double-tap would re-jump to the same event. */
  if(onMediathek){{
    let cur=currentEvent();
    const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
    if(recentJump)cur=_lastJumpedEv;
    const curWall=wallForCurrent();
    const nearStart=cur&&(recentJump||Math.abs(curWall-cur.start)<=10);
    if(cur&&nearStart){{
      const prev=[...epgEvents].reverse().find(e=>e.stop<=cur.start);
      if(prev&&isReachable(prev)){{jumpToEvent(prev);return;}}
    }}
    if(cur){{jumpToEvent(cur);return;}}
  }}
  let ev=currentEvent();
  if(!ev){{
    /* EPG not loaded yet — fetch synchronously and retry. */
    showHintMsg('EPG wird geladen…',4000);
    try{{
      const r=await fetch(HOST+'/api/events/'+current+'?back=7200&fwd=1800');
      const d=await r.json();
      epgEvents=d.events||[];renderChapters();
    }}catch(e){{}}
    ev=currentEvent();
    if(!ev){{showHintMsg('Kein EPG-Event gefunden',null,'error');return;}}
  }}
  /* Live-buffer step-back, unified merged-targets walk.

     Every tap evaluates the same target set:
       - end of every detected ad block in the buffer (= "show
         resumed after this break")
       - start of every EPG event in the buffer (= "this show began")
     Cutoff for "before current position":
       - first tap of a sequence: live edge — picks the latest
         target overall (often the most recent ad-end, sometimes
         the current show start if no ad has aired since)
       - subsequent taps within the 10 s anchor window: the wall-time
         we last jumped to (NOT v.currentTime, which can lag a
         second or two on iOS Safari right after a seek and cause
         "every other tap re-picks the same target")

     If no target lies earlier in the buffer, fall through to the
     classical extend-backward cascade (recording-window → mediathek
     → buffer-start). */
  const recentJump=(Date.now()-_lastJumpedTs<10000)&&_lastJumpedEv;
  fetch(HOST+'/api/live-ads/'+current)
   .then(r=>r.json()).then(adResp=>{{
     const ads=adResp.ads||[];
     const[s,e]=seekableRange();
     if(e<=s+1){{showHintMsg('Buffer leer',null,'error');return;}}
     const nowS=Date.now()/1000;
     const wallS=nowS-(e-s),wallE=nowS;
     const cutoffWall=recentJump?_lastJumpedWall:wallE;
     const targets=[];
     for(const a of ads){{
       if(a[1]>=wallS&&a[1]<=cutoffWall-0.5)targets.push({{wall:a[1],label:'ad-end'}});
     }}
     for(const epg of epgEvents){{
       if(epg.start>=wallS&&epg.start<=cutoffWall-0.5)targets.push({{wall:epg.start,label:'epg-start',ev:epg}});
     }}
     targets.sort((x,y)=>y.wall-x.wall);
     if(targets.length){{
       const pick=targets[0];
       _lastJumpedEv=pick.ev||_lastJumpedEv||ev;
       _lastJumpedTs=Date.now();_lastJumpedKind='walk';_lastJumpedWall=pick.wall;
       v.currentTime=Math.max(s+0.5,Math.min(e-1,pick.wall-wallS));
       show();
       const hints={{'epg-start':'Sprung an vorherigen Sendungsanfang','ad-end':'Sprung an Werbeblock-Ende'}};
       showHintMsg(hints[pick.label]||'Sprung zurück',2500);
       return;
     }}
     /* No targets earlier in buffer — try chain → mediathek → buffer-start */
     showHintMsg('Suche Aufnahme…',4000);
     fetch(HOST+'/api/recording-window/'+current)
      .then(r=>r.json()).then(resp=>{{
        const chain=(resp&&resp.chain)||[];
        const targetIdx=chain.findIndex(c=>c.start<=ev.start&&ev.start<c.stop);
        if(targetIdx<0||chain.length===0){{
          fetch(HOST+'/api/mediathek-live/'+current)
           .then(r=>r.json()).then(d=>{{
             if(!d.url){{
               _lastJumpedEv=ev;_lastJumpedTs=Date.now();_lastJumpedKind='buffer';_lastJumpedWall=wallS+0.5;
               v.currentTime=s+0.5;show();
               showHintMsg('Sendungsanfang außerhalb Puffer — Sprung an Buffer-Anfang',3500);
               return;
             }}
             switchToMediathek(d.url,ev.start);
           }}).catch(()=>showHintMsg('Mediathek-Fehler',null,'error'));
          return;
        }}
        activateRecordingChain(chain,targetIdx,ev.start);
      }}).catch(err=>showHintMsg('Recording-Fehler: '+err,null,'error'));
   }}).catch(()=>showHintMsg('Live-Ads-Fehler',null,'error'));
}}
function activateRecordingChain(chain,targetIdx,wallStartTs){{
  recChain=chain;
  const first=chain[0], last=chain[chain.length-1];
  /* The chain window covers from the earliest chain member's start
     to either the last member's stop OR now (if last is running). */
  const nowS=Date.now()/1000;
  const windowEnd=last.running?nowS:last.stop;
  recWindow=[first.start,windowEnd];
  switchToRecording(chain[targetIdx],wallStartTs,targetIdx);
  /* Re-render chapter ticks with the new (full-chain) reachable
     window so all event boundaries are visible at once. */
  setTimeout(renderChapters,100);
}}
function switchToRecording(rec,wallStartTs,idx){{
  /* Load the tvheadend recording as the active HLS source.
     Poll /progress first — the playlist is 202-pending until
     ffmpeg has remuxed ~10 segments. Seek offset = wall-time of
     target minus recording-start wall time. */
  destroyHls();
  onMediathek=false;
  onRecording=true;
  recCurrentIdx=(typeof idx==='number')?idx:0;
  setSource('rec');
  const offset=Math.max(0,wallStartTs-rec.start);
  hint.textContent='Aufnahme wird vorbereitet…';
  hint.classList.add('show');
  const progUrl=HOST+'/recording/'+rec.uuid+'/progress';
  let tries=0;
  const waitReady=()=>{{
    if(current!==rec.slug_at_switch&&tries>0){{
      /* user swiped away, abort */
      hint.classList.remove('show');return;
    }}
    fetch(progUrl).then(r=>r.json()).then(d=>{{
      if(d.done||d.segments>=10){{
        hint.classList.remove('show');
        doRecordingLoad(rec.hls_url,offset);
        return;
      }}
      if(tries++>120){{
        hint.textContent='Aufnahme-Remux dauert zu lange';
        setTimeout(()=>hint.classList.remove('show'),2500);
        return;
      }}
      setTimeout(waitReady,1500);
    }}).catch(()=>setTimeout(waitReady,2500));
  }};
  rec.slug_at_switch=current;
  waitReady();
  show();
}}
function doRecordingLoad(url,offset){{
  const isIos=/iPad|iPhone|iPod/.test(navigator.userAgent);
  if(!isIos&&typeof Hls!=='undefined'&&Hls.isSupported()){{
    const hls=new Hls({{maxBufferLength:30,backBufferLength:7200,
      renderTextTracksNatively:false,subtitleDisplay:false}});
    hlsInst=hls;hlsCurrentUrl=url;
    hls.loadSource(url);
    hls.attachMedia(v);
    hls.once(Hls.Events.LEVEL_LOADED,()=>{{
      v.currentTime=offset;v.play().catch(()=>{{}});
    }});
  }} else {{
    v.src=url;v.load();
    const start=()=>{{
      v.currentTime=offset;v.play().catch(()=>{{}});
    }};
    v.addEventListener('loadedmetadata',start,{{once:true}});
    v.addEventListener('canplay',start,{{once:true}});
  }}
}}
let hlsInst=null;
let hlsCurrentUrl=null;
function destroyHls(){{
  if(hlsInst){{try{{hlsInst.destroy();}}catch(e){{}}hlsInst=null;}}
  hlsCurrentUrl=null;
}}
function switchToMediathek(rawUrl,showStartTs,safetyOffset){{
  onMediathek=true;
  setSource('mediathek');
  destroyHls();
  hlsCurrentUrl=rawUrl;
  if(typeof safetyOffset!=='number')safetyOffset=5;
  if(typeof Hls==='undefined'||!Hls.isSupported()){{
    hint.textContent='Mediathek nicht verfügbar';
    hint.classList.add('show');
    setTimeout(()=>hint.classList.remove('show'),2000);
    return;
  }}
  const hls=new Hls({{
    forceMseHlsOnAppleDevices:true,
    maxBufferLength:30,
    backBufferLength:7200,
    renderTextTracksNatively:false,
    subtitleDisplay:false,
  }});
  hlsInst=hls;
  /* Also force-disable any text tracks the browser added itself. */
  const disableCaptions=()=>{{
    for(const t of v.textTracks)t.mode='disabled';
  }};
  v.textTracks&&v.textTracks.addEventListener('addtrack',disableCaptions);
  setTimeout(disableCaptions,200);
  setTimeout(disableCaptions,1500);
  let seeked=false;
  hls.on(Hls.Events.LEVEL_LOADED,(_,data)=>{{
    if(seeked)return;
    const frags=(data.details&&data.details.fragments)||[];
    /* First try: fragment that contains showStartTs. If none (target
       is older than earliest fragment), fall back to the earliest.  */
    let matchedFrag=null,matchedPdt=0;
    for(const f of frags){{
      if(!f.programDateTime)continue;
      const pdt=f.programDateTime/1000;
      if(pdt<=showStartTs&&showStartTs<pdt+f.duration){{
        matchedFrag=f;matchedPdt=pdt;break;
      }}
    }}
    if(!matchedFrag){{
      for(const f of frags){{
        if(!f.programDateTime)continue;
        const pdt=f.programDateTime/1000;
        if(pdt>=showStartTs){{
          matchedFrag=f;matchedPdt=pdt;break;
        }}
      }}
    }}
    if(matchedFrag){{
      const f=matchedFrag,pdt=matchedPdt;
      const off=Math.max(0,showStartTs-pdt);
      const targetT=f.start+off;
      try{{hls.stopLoad();}}catch(e){{}}
      hls.startLoad(targetT);
      let tries=0;
      const safeTarget=Math.max(0,targetT-safetyOffset);
      const seekWhenReady=()=>{{
        const[ss,ee]=seekableRange();
        if(ss<=safeTarget&&ee>=safeTarget+2){{
          v.currentTime=safeTarget;
          v.play().catch(()=>{{}});
          return;
        }}
        if(tries++>60)return;
        setTimeout(seekWhenReady,250);
      }};
      seekWhenReady();
      seeked=true;
      show();
    }}
  }});
  hls.on(Hls.Events.ERROR,(_,data)=>{{
    if(data.fatal){{
      hint.textContent='Mediathek-Fehler';
      hint.classList.add('show');
      setTimeout(()=>hint.classList.remove('show'),2500);
    }}
  }});
  hls.attachMedia(v);
  hls.loadSource(rawUrl);
  show();
}}
function loadEvents(slug){{
  fetch(HOST+'/api/events/'+slug+'?back=7200&fwd=1800')
   .then(r=>r.json()).then(d=>{{
     if(slug!==current)return;
     epgEvents=d.events||[];renderChapters();
   }}).catch(()=>{{}});
}}
let liveAds=[];
const skipBtn=document.getElementById('skipad');
function allAdsWall(){{
  /* Union of live-buffer ads + chain recording ads (both already
     wall-clock), de-duped/sorted; chain ads survive even when the
     live buffer rolled past their segments. */
  const all=[...liveAds,...recChainAds];
  all.sort((a,b)=>a[0]-b[0]);
  return all;
}}
function renderLiveAds(){{
  document.querySelectorAll('.ad-block').forEach(el=>el.remove());
  const[ws,we]=scrubWindow();
  if(we<=ws)return;
  /* In live-buffer playback, hide markers for ad blocks outside the
     seekable range — those are orphan entries from a previous buffer
     state (e.g. before an SSD remount truncated the buffer) and you
     can't jump to them anyway. In recording-chain mode the markers
     for other chain members ARE reachable via switchToRecording, so
     keep them visible there. */
  let seekS=null,seekE=null;
  const inChain=onRecording&&recChain;
  if(!inChain){{
    /* HAVE_CURRENT_DATA (readyState>=2) — anything below that and
       v.seekable can still report the previous channel's range or
       NaN bounds during the ~1 s between v.src= and the new
       playlist parsing. Render NO markers in that window rather
       than flash stale orphan markers. The video event listeners
       (loadeddata/canplay/loadedmetadata) re-render once it's ready. */
    if(v.readyState<2)return;
    const[s,e]=seekableRange();
    if(!isFinite(s)||!isFinite(e)||e<=s+1)return;
    /* Derive wall-time of the seekable bounds from "now - duration"
       instead of wallAt(s)/wallAt(e). Safari's getStartDate() can
       return the original playlist start (drifting hours-old) for
       sliding HLS windows, making wallAt(s) too early. The live
       edge is by definition ~now, so live edge minus buffer duration
       gives the actual seekable start. */
    const nowS=Date.now()/1000;
    seekE=nowS;
    seekS=nowS-(e-s);
  }}
  for(const[wStart,wStop]of allAdsWall()){{
    if(wStop<=ws||wStart>=we)continue;
    if(seekS!==null&&(wStop<=seekS||wStart>=seekE))continue;
    const left=posForWall(Math.max(wStart,ws));
    const right=posForWall(Math.min(wStop,we));
    if(left===null||right===null||right<=left)continue;
    const el=document.createElement('div');
    el.className='ad-block';
    el.style.left=left+'%';el.style.width=(right-left)+'%';
    scrub.appendChild(el);
  }}
}}
function currentLiveAd(){{
  const w=wallForCurrent();
  for(const[a,b]of allAdsWall()){{if(w>=a&&w<b)return[a,b];}}
  return null;
}}
function skipCurrentAd(){{
  const a=currentLiveAd();if(!a)return;
  const[s,e]=seekableRange();
  const wallS=e>s?wallAt(s):null;
  if(wallS===null)return;
  v.currentTime=Math.max(s+0.5,Math.min(e-1,a[1]+0.5-wallS));
  show();
}}
function refreshSkipAdBtn(){{
  skipBtn.classList.toggle('on',currentLiveAd()!==null);
}}
function loadLiveAds(slug){{
  fetch(HOST+'/api/live-ads/'+slug).then(r=>r.json()).then(d=>{{
    if(slug!==current)return;
    liveAds=d.ads||[];renderLiveAds();refreshSkipAdBtn();
  }}).catch(()=>{{}});
}}
function seek(d){{
  /* Recording-chain: allow ±N-second seeks to cross chain borders
     and advance to previous/next VOD (or to live at the end). */
  if(onRecording&&recChain&&recCurrentIdx>=0){{
    const curWall=recChain[recCurrentIdx].start+(v.currentTime||0);
    const target=curWall+d;
    const last=recChain[recChain.length-1];
    const nowS=Date.now()/1000;
    if(d>0&&last.running&&target>=last.stop&&target>=nowS-3){{
      goLive();return;
    }}
    const idx=recChain.findIndex(c=>c.start<=target&&target<c.stop);
    if(idx>=0&&idx!==recCurrentIdx){{
      switchToRecording(recChain[idx],target,idx);
      return;
    }}
    /* Same VOD: clamp to 0..duration. */
    const dur=v.duration||0;
    const newT=Math.max(0,Math.min(dur-0.5,(v.currentTime||0)+d));
    v.currentTime=newT;show();return;
  }}
  const[s,e]=seekableRange();
  v.currentTime=Math.max(s,Math.min(e-1,(v.currentTime||0)+d));
  show();
}}
function seekTo(ev){{
  const r=scrub.getBoundingClientRect();
  const clientX=(ev.touches?ev.touches[0]:ev).clientX;
  /* Chapter-tap routing: if the pointer is within CHAPTER_TAP_PX of
     a tick, treat the seek as a chapter jump instead of raw seek.
     Picks the nearest chapter — eliminates DOM-order ambiguity when
     two adjacent ticks would overlap on a narrow scrub bar. */
  const ch=nearestChapter(clientX);
  if(ch){{
    const startTs=parseFloat(ch.dataset.start);
    const evt=epgEvents.find(e=>Math.abs(e.start-startTs)<0.5);
    if(evt){{
      const ts=new Date(evt.start*1000).toLocaleTimeString('de-DE',{{hour:'2-digit',minute:'2-digit'}});
      hint.textContent=ts+' · '+evt.title;
      hint.classList.add('show');
      setTimeout(()=>hint.classList.remove('show'),1200);
      jumpToEvent(evt);
      return;
    }}
  }}
  const x=clientX-r.left;
  const p=Math.max(0,Math.min(1,x/r.width));
  const[ws,we]=scrubWindow();
  const targetWall=ws+p*(we-ws);
  /* Recording-chain mode: target wall-time might fall in a different
     chain member than the currently-loaded one — switch VOD if so. */
  if(onRecording&&recChain){{
    const nowS=Date.now()/1000;
    const lastIdx=recChain.length-1;
    const last=recChain[lastIdx];
    /* Scrubbed past last recording's stop → go live. */
    if(last.running&&targetWall>=last.stop&&targetWall>=nowS-3){{
      goLive();return;
    }}
    const idx=recChain.findIndex(c=>c.start<=targetWall&&targetWall<c.stop);
    const targetIdx=idx>=0?idx:(targetWall<recChain[0].start?0:lastIdx);
    if(targetIdx!==recCurrentIdx){{
      switchToRecording(recChain[targetIdx],targetWall,targetIdx);
      return;
    }}
    /* Same VOD — seek within. */
    v.currentTime=Math.max(0,targetWall-recChain[targetIdx].start);
    return;
  }}
  const[s,e]=seekableRange();
  if(e<=s)return;
  /* Snap to live edge if the drag landed in the rightmost 2 % of the
     scrubbar — otherwise the cursor never quite reaches "live" because
     we clamp 1 s short and the player keeps a tiny gap visible. */
  if(p>=0.98){{goLive();return;}}
  const wallS=wallAt(s),wallE=wallAt(e);
  const clamped=Math.max(wallS,Math.min(wallE-1,targetWall));
  v.currentTime=s+(clamped-wallS);
}}
let pauseState=null;
/* Native HLS pause workaround: iOS Safari ignores v.pause() on a
   live HLS stream, so togglePlay strips v.src to actually halt
   playback — but stripping src blanks the <video>. Capture the
   current frame to a canvas and overlay it for the duration of the
   pause so the user still sees the still image. */
let _pauseCanvas=null;
function showPauseFreeze(){{
  try{{
    if(!_pauseCanvas){{
      _pauseCanvas=document.createElement('canvas');
      _pauseCanvas.id='pause-freeze';
      _pauseCanvas.style.cssText=
        'position:absolute;left:0;top:0;width:100%;height:100%;'
        +'object-fit:contain;z-index:5;pointer-events:none;'
        +'background:#000';
      v.parentNode.insertBefore(_pauseCanvas,v.nextSibling);
    }}
    const w=v.videoWidth||v.clientWidth;
    const h=v.videoHeight||v.clientHeight;
    if(w<=0||h<=0){{_pauseCanvas.style.display='none';return;}}
    _pauseCanvas.width=w;_pauseCanvas.height=h;
    const ctx=_pauseCanvas.getContext('2d');
    ctx.drawImage(v,0,0,w,h);
    _pauseCanvas.style.display='block';
  }}catch(e){{}}
}}
function hidePauseFreeze(){{
  if(_pauseCanvas)_pauseCanvas.style.display='none';
}}
/* While paused, refresh the freeze overlay any time the underlying
   <video> seeks to a new position — so scrubbing during pause shows
   the new still frame instead of the original captured one. iOS
   Safari decodes the frame at the seek target even while paused, so
   drawImage() gets the fresh content. */
v.addEventListener('seeked',()=>{{
  if(pauseState&&!pauseState.pausedHlsJs)showPauseFreeze();
}});
function togglePlay(){{
  if(pauseState){{
    const ps=pauseState;pauseState=null;
    if(ps.pausedHlsJs){{
      /* hls.js (Mediathek or live DVB-C via hls.js) — restart loading
         and resume; the existing buffer + last frame stay visible. */
      try{{if(hlsInst)hlsInst.startLoad();}}catch(e){{}}
      v.play().catch(()=>{{}});
    }} else {{
      /* Native HLS: hide the freeze overlay and resume. The canvas
         covered the video while Safari was paused; v.play() picks
         up from currentTime which Safari kept inside the DVR
         seekable range. */
      hidePauseFreeze();
      v.play().catch(()=>{{}});
    }}
    pp.textContent='\u23F8';
    return;
  }}
  if(v.paused||v.ended){{
    v.play().catch(()=>{{}});
    pp.textContent='\u23F8';
    return;
  }}
  /* Two pause flavours:
     - hls.js (Mediathek + DVB-C via hls.js): v.pause() + stopLoad
       to halt new segments. Keeps the last decoded frame on screen.
       Destroying hls.js or stripping v.src here used to blank the
       <video> element on Safari + Chrome.
     - Native HLS (iOS Safari live): pause() is ignored on live HLS,
       so strip v.src and seek back via wall-time on resume. */
  const usingHlsJs=!!hlsInst;
  pauseState={{
    pausedSrc:usingHlsJs?null:v.src,
    pausedHlsJs:usingHlsJs,
    pausedWall:wallAt(v.currentTime),
    pausedCurrent:v.currentTime
  }};
  if(usingHlsJs){{
    v.pause();
    try{{hlsInst.stopLoad();}}catch(e){{}}
  }} else {{
    /* Native HLS live (iOS Safari): capture frame to canvas overlay
       BEFORE pause, then v.pause(). Don't strip src — Safari keeps
       the seekable DVR window intact and resumes from currentTime
       (no jump to live edge on play). */
    showPauseFreeze();
    v.pause();
  }}
  pp.textContent='\u25B6';
  setTimeout(()=>{{if(pauseState)pp.textContent='\u25B6';}},80);
  setTimeout(()=>{{if(pauseState)pp.textContent='\u25B6';}},500);
}}

function doUnmute(){{
  v.muted=false;v.play().catch(()=>{{}});unmuteBtn.style.display='none';
}}
function tryPlay(){{
  if(!v.paused&&!v.ended)return;
  const p=v.play();
  if(p&&typeof p.then==='function'){{
    p.then(()=>{{if(v.muted)unmuteBtn.style.display='block';}})
     .catch(()=>{{
       if(!v.muted&&!unmutedByUser){{
         v.muted=true;
         v.play().then(()=>{{
           if(v.muted&&!unmutedByUser)unmuteBtn.style.display='block';
         }}).catch(()=>{{}});
       }}
     }});
  }}
}}
let unmutedByUser=false;
function onInteract(){{
  if(!unmutedByUser){{
    v.muted=false;unmutedByUser=true;unmuteBtn.style.display='none';
  }}
  /* Don't auto-resume if the user just hit pause. */
  if(pauseState)return;
  tryPlay();
}}
document.addEventListener('click',onInteract);
document.addEventListener('touchend',()=>setTimeout(onInteract,30),
                          {{passive:true}});

function loadDvbcSrc(slug){{
  onMediathek=false;
  setSource('live');
  destroyHls();
  const url=HOST+'/hls/'+slug+'/dvr.m3u8';
  v.muted=false;
  const isIos=/iPad|iPhone|iPod/.test(navigator.userAgent);
  if(!isIos && typeof Hls!=='undefined' && Hls.isSupported()){{
    const hls=new Hls({{
      maxBufferLength:30,
      backBufferLength:7200,
      renderTextTracksNatively:false,
      subtitleDisplay:false,
    }});
    hlsInst=hls;
    hlsCurrentUrl=url;
    hls.loadSource(url);
    hls.attachMedia(v);
  }} else {{
    v.src=url;v.load();
  }}
  v.addEventListener('loadedmetadata',()=>{{
    tryPlay();restoreLastPos(slug);
  }},{{once:true}});
}}
function tryMediathekLive(url){{
  return new Promise(resolve=>{{
    if(typeof Hls==='undefined'||!Hls.isSupported()){{
      resolve(false);return;
    }}
    destroyHls();
    const hls=new Hls({{forceMseHlsOnAppleDevices:true,
      maxBufferLength:30,backBufferLength:10800,
      renderTextTracksNatively:false,subtitleDisplay:false}});
    hlsInst=hls;
    hlsCurrentUrl=url;
    /* Kill any caption/subtitle tracks the player auto-selected. */
    const disableCaptions=()=>{{
      for(const t of v.textTracks)t.mode='disabled';
    }};
    v.textTracks&&v.textTracks.addEventListener('addtrack',disableCaptions);
    setTimeout(disableCaptions,200);
    setTimeout(disableCaptions,1500);
    let settled=false;
    const finish=ok=>{{
      if(settled)return;settled=true;
      clearTimeout(to);
      if(!ok){{
        try{{hls.destroy();}}catch(e){{}}
        hlsInst=null;hlsCurrentUrl=null;
      }}
      resolve(ok);
    }};
    const to=setTimeout(()=>{{
      console.warn('mediathek live timeout');finish(false);
    }},8000);
    hls.on(Hls.Events.ERROR,(_,data)=>{{
      if(data.fatal){{console.warn('mediathek live error',data);finish(false);}}
    }});
    const onReady=()=>{{
      v.removeEventListener('canplay',onReady);
      /* Muted autoplay to satisfy iOS gesture rules; unmute-button
         overlay appears and the user taps it to hear audio — same
         pattern as the DVB-C path. */
      v.muted=true;
      v.play().then(()=>{{
        if(unmuteBtn){{
          v.muted=true;
          unmuteBtn.style.display='inline-flex';
        }}
      }}).catch(()=>{{}});
      finish(true);
    }};
    v.addEventListener('canplay',onReady);
    v.muted=true;
    hls.loadSource(url);hls.attachMedia(v);
  }});
}}
/* Last-position per channel — persisted so "re-open live stream"
   resumes where the user left off, not at the live edge.
   Stored as wall-clock seconds + timestamp; ignored if older than 4 h
   or outside the current seekable range. */
const LASTPOS_TTL_MS=4*3600*1000;
function saveLastPos(slug){{
  if(!slug||onMediathek)return;
  const t=v.currentTime||0;
  if(t<=0)return;
  const w=wallAt(t);
  if(!w||!isFinite(w))return;
  try{{
    localStorage.setItem('lastpos_'+slug,
      JSON.stringify({{wall:w,ts:Date.now()}}));
  }}catch(e){{}}
}}
function restoreLastPos(slug){{
  let entry=null;
  try{{
    entry=JSON.parse(localStorage.getItem('lastpos_'+slug)||'null');
  }}catch(e){{}}
  if(!entry||Date.now()-entry.ts>LASTPOS_TTL_MS)return;
  let tries=20;
  const tick=()=>{{
    if(tries--<=0||current!==slug||onMediathek)return;
    const[s,e]=seekableRange();
    if(e<=s+2){{setTimeout(tick,300);return;}}
    const wallS=wallAt(s);
    if(!wallS){{setTimeout(tick,300);return;}}
    const target=s+(entry.wall-wallS);
    if(target<s+0.5||target>e-1)return;   /* out of buffer */
    v.currentTime=target;
  }};
  setTimeout(tick,900);
}}
setInterval(()=>{{if(current&&!v.paused)saveLastPos(current);}},20000);
window.addEventListener('beforeunload',()=>saveLastPos(current));
document.addEventListener('visibilitychange',()=>{{
  if(document.visibilityState==='hidden')saveLastPos(current);
}});
async function loadSrc(slug){{
  saveLastPos(current);   /* preserve previous channel position */
  current=slug;onMediathek=false;
  const ch=channels[idx];
  if(ch)chname.textContent=ch.name;
  history.replaceState(null,'','/watch/'+slug);
  loadEpg(slug);
  loadEvents(slug);
  _chaptersReady=false;
  const mtP=loadMediathekAvail(slug);
  subscribeLiveAds(slug);
  recChain=null;recCurrentIdx=-1;
  const chP=loadRecordingChain(slug);
  Promise.all([mtP,chP]).then(()=>{{
    if(slug!==current)return;
    _chaptersReady=true;renderChapters();
  }});
  showHint(slug);
  /* Public-broadcaster channels have a Mediathek live stream — try
     that first to save a DVB-C tuner + the Pi CPU. Only fall back to
     our local ffmpeg pipeline if Mediathek doesn't answer in 8 s or
     errors out fatally. Private channels never had Mediathek so they
     skip the probe entirely. */
  try{{
    const mt=await fetch(HOST+'/api/mediathek-live/'+slug)
      .then(r=>r.json()).catch(()=>null);
    if(mt&&mt.url&&current===slug){{
      if(await tryMediathekLive(mt.url)){{
        if(current!==slug)return; /* user swiped away meanwhile */
        onMediathek=true;setSource('mediathek');
        return;
      }}
    }}
  }}catch(e){{}}
  if(current!==slug)return;
  loadDvbcSrc(slug);
}}

function showHint(slug){{
  const ch=channels[idx];
  hint.textContent=ch?ch.name:slug;
  hint.classList.add('show');
  setTimeout(()=>hint.classList.remove('show'),700);
}}
function nextCh(){{if(!channels.length)return;
  idx=(idx+1)%channels.length;loadSrc(channels[idx].slug);show();}}
function prevCh(){{if(!channels.length)return;
  idx=(idx-1+channels.length)%channels.length;loadSrc(channels[idx].slug);show();}}

window.addEventListener('pagehide',()=>navigator.sendBeacon(HOST+'/stop-all'));
window.addEventListener('beforeunload',()=>navigator.sendBeacon(HOST+'/stop-all'));

function loadEpg(slug){{
  fetch(HOST+'/api/now/'+slug).then(r=>r.json()).then(d=>{{
    epgEl.textContent=d.title?(d.time+' · '+d.title):'';
  }}).catch(()=>{{epgEl.textContent='';}});
}}

// Swipe horizontally on the video to switch channels.
let startX=0,startY=0,startT=0;
document.addEventListener('touchstart',e=>{{
  if(!e.touches[0])return;
  startX=e.touches[0].clientX;startY=e.touches[0].clientY;
  startT=Date.now();
}},{{passive:true,capture:true}});
document.addEventListener('touchend',e=>{{
  if(!e.changedTouches[0])return;
  const dx=e.changedTouches[0].clientX-startX;
  const dy=e.changedTouches[0].clientY-startY;
  const dt=Date.now()-startT;
  if(Math.abs(dx)>60&&Math.abs(dx)>Math.abs(dy)*1.5&&dt<800){{
    if(dx<0)nextCh();else prevCh();
  }}
}},{{passive:true,capture:true}});
document.addEventListener('keydown',e=>{{
  if(e.key==='Escape')closePlayer();
  else if(e.key==='ArrowRight')nextCh();
  else if(e.key==='ArrowLeft')prevCh();
  else if(e.key===' '){{e.preventDefault();togglePlay();show();}}
}});
v.addEventListener('click',()=>{{
  if(chromeBar.classList.contains('hidden'))show();
  else {{chromeBar.classList.add('hidden');topbar.classList.add('hidden');}}
}});
document.addEventListener('mousemove',show);

loadSrc(current);
fetch(HOST+'/api/channels').then(r=>r.json()).then(d=>{{
  channels=d.channels;
  idx=channels.findIndex(c=>c.slug===current);
  if(idx<0)idx=0;
}});
</script></body></html>""", 200, {"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache"}


MEDIATHEK_LIVE = {
    # slug -> (url, dvr_window_seconds)
    "das-erste-hd":    ("https://daserste-live.ard-mcdn.de/daserste/live/hls/de/master.m3u8",    7200),
    "tagesschau24-hd": ("https://tagesschau-live.ard-mcdn.de/tagesschau/live/hls/de/master.m3u8", 7200),
    "zdf-hd":          ("https://zdf-hls-15.akamaized.net/hls/live/2016498/de/veryhigh/master.m3u8", 10800),
    "3sat-hd":         ("https://zdf-hls-18.akamaized.net/hls/live/2016501/dach/veryhigh/master.m3u8", 10800),
    "arte-hd":         ("https://artesimulcast.akamaized.net/hls/live/2030993/artelive_de/index.m3u8", 1800),
    "kika-hd":         ("https://kikageohls.akamaized.net/hls/live/2022693/livetvkika_de/master.m3u8", 7200),
}

# ARD now-next channel CRIDs for precise show-start lookup. Value is
# the base64-encoded channel id that programm-api.ard.de expects.
ARD_NOWNEXT_CRID = {
    "das-erste-hd":    "Y3JpZDovL2Rhc2Vyc3RlLmRlL2xpdmUvY2xpcC9hYmNhMDdhMy0zNDc2LTQ4NTEtYjE2Mi1mZGU4ZjY0NmQ0YzQ",
    "tagesschau24-hd": "Y3JpZDovL2Rhc2Vyc3RlLmRlL3RhZ2Vzc2NoYXUvbGl2ZXN0cmVhbQ",
    "3sat-hd":         "Y3JpZDovLzNzYXQuZGUvTGl2ZXN0cmVhbS0zc2F0",
}

# ZDF broadcaster IDs (used with their getEpg GraphQL persisted query).
# ZDFs API covers more than just ZDF — their EPG service delivers
# precise now/next times for all public-broadcast partners they host.
ZDF_BROADCASTER = {
    "zdf-hd":      "ZDF",
    "zdfinfo-hd":  "ZDFinfo",
    "zdfneo-hd":   "ZDFneo",
    "3sat-hd":     "3sat",
    "kika-hd":     "KI.KA",
    "phoenix-hd":  "PHOENIX",
    "arte-hd":     "arte",
}
ZDF_API_TOKEN = "ahBaeMeekaiy5ohsai4bee4ki6Oopoi5quailieb"
ZDF_GETEPG_HASH = "e36a71fb3206e75a82a5438737113b221e43daf0363d85f3eeceda288d158821"


def _lookup_ard_actual_start(slug, ts):
    crid = ARD_NOWNEXT_CRID.get(slug)
    if not crid:
        return None
    try:
        url = (f"https://programm-api.ard.de/nownext/api/channel"
               f"?channel={crid}&pastHours=6&futureEvents=3")
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        data = json.loads(urllib.request.urlopen(req, timeout=5).read())
    except Exception:
        return None
    from datetime import datetime
    best = None
    best_diff = 360
    for ev in data.get("events", []):
        scheduled = ev.get("startDate")
        current = ev.get("currentStartDate") or scheduled
        if not scheduled:
            continue
        try:
            sched_ts = datetime.fromisoformat(scheduled).timestamp()
        except Exception:
            continue
        diff = abs(sched_ts - ts)
        if diff < best_diff:
            try:
                best = datetime.fromisoformat(current).timestamp()
                best_diff = diff
            except Exception:
                pass
    return int(best) if best is not None else None


def _lookup_zdf_actual_start(slug, ts):
    broadcaster_id = ZDF_BROADCASTER.get(slug)
    if not broadcaster_id:
        return None
    from datetime import datetime, timezone
    try:
        base = datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None
    frm = base.replace(hour=0, minute=0, second=0, microsecond=0)
    to  = frm.replace(hour=23, minute=59, second=59)
    variables = urllib.parse.quote(json.dumps({
        "filter": {
            "broadcasterIds": [broadcaster_id],
            "from": frm.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to":  to.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
    }, separators=(",", ":")))
    extensions = urllib.parse.quote(json.dumps({
        "persistedQuery": {"version": 1, "sha256Hash": ZDF_GETEPG_HASH},
    }, separators=(",", ":")))
    url = (f"https://api.zdf.de/graphql?operationName=getEpg"
           f"&variables={variables}&extensions={extensions}")
    try:
        req = urllib.request.Request(url, headers={
            "api-auth":                  f"Bearer {ZDF_API_TOKEN}",
            "zdf-app-id":                "ngplayer_2_4",
            "x-apollo-operation-name":   "getEpg",
            "apollo-require-preflight":  "true",
            "accept":                    "application/json",
            "referer":                   "https://www.zdf.de/",
        })
        data = json.loads(urllib.request.urlopen(req, timeout=5).read())
    except Exception:
        return None
    best = None
    best_diff = 360
    for entry in data.get("data", {}).get("epg", []):
        bc = entry.get("broadcaster", {})
        for slot in (bc.get("now"), bc.get("next")):
            if not slot:
                continue
            scheduled = slot.get("airtimeBegin")
            effective = slot.get("effectiveAirtimeBegin") or scheduled
            if not scheduled:
                continue
            try:
                sched_ts = datetime.fromisoformat(scheduled).timestamp()
                eff_ts = datetime.fromisoformat(effective).timestamp()
            except Exception:
                continue
            diff = abs(sched_ts - ts)
            if diff < best_diff:
                best = eff_ts
                best_diff = diff
    return int(best) if best is not None else None


@app.route("/api/show-actual-start/<slug>")
def api_show_actual_start(slug):
    """Look up the ACTUAL broadcast start of the show whose EPG slot
    starts at ?ts=<epoch>. Uses ARDs programm-api or ZDFs getEpg
    persisted-query API, depending on the channel. Falls back to EPG
    if no match within ±6 min."""
    try:
        ts = int(request.args.get("ts", "0"))
    except ValueError:
        ts = 0
    if ts <= 0:
        return _cors(Response(json.dumps({"actual": ts, "source": "epg"}),
                               mimetype="application/json"))
    actual = _lookup_ard_actual_start(slug, ts)
    if actual is not None:
        return _cors(Response(json.dumps({"actual": actual, "source": "ard-nownext"}),
                               mimetype="application/json"))
    actual = _lookup_zdf_actual_start(slug, ts)
    if actual is not None:
        return _cors(Response(json.dumps({"actual": actual, "source": "zdf-getepg"}),
                               mimetype="application/json"))
    return _cors(Response(json.dumps({"actual": ts, "source": "epg"}),
                           mimetype="application/json"))


@app.route("/api/mediathek-live/<slug>")
def api_mediathek_live(slug):
    """Public HLS fallback URL (ARD Mediathek etc.) for restart beyond
    the local DVR buffer. Returns { url, window } or { url: null }."""
    info = MEDIATHEK_LIVE.get(slug)
    if info:
        url, window = info
        body = {"url": url, "window": window}
    else:
        body = {"url": None, "window": 0}
    return _cors(Response(json.dumps(body),
                           mimetype="application/json"))


@app.route("/mediathek-passthru/<slug>/master.m3u8")
def mediathek_passthru_master(slug):
    """Serve a single-variant master pointing to the highest BANDWIDTH
    rendition from upstream. Rewrites the variant URI (and its audio
    companion) through our /pl.m3u8 proxy to keep the playlist origin
    on us — sidesteps iOS cross-origin quirks. Ignoring lower variants
    means the player can't ABR-downgrade on a momentary bandwidth dip;
    that's the point — Mediathek streams top out at ~5 Mbps which any
    home connection handles, and the user has explicitly asked for max.

    Upstream URL comes from MEDIATHEK_LIVE[slug] (live-channels path)
    or, when '?u=<encoded url>' is given, that URL directly (for the
    on-demand play / recording paths where the upstream master URL
    isn't pre-known and is resolved per request)."""
    upstream_override = request.args.get("u", "")
    if upstream_override and upstream_override.startswith("http"):
        base_url = upstream_override
    else:
        info = MEDIATHEK_LIVE.get(slug)
        if not info:
            abort(404)
        base_url = info[0]
    try:
        body = _mediathek_manifest_cache.get_or_fetch_master(
            base_url,
            lambda u: _mediathek_upstream_pool.urlopen(u, timeout=8))
        txt = body.decode()
    except Exception as e:
        abort(502, f"upstream: {e}")
    picked = _parse_master(base_url, txt)
    if not picked:
        # Couldn't parse — serve the raw master (legacy passthru). Better
        # than 502'ing because at least the player gets *something*.
        return Response(txt, mimetype="application/vnd.apple.mpegurl")
    video_url, audio_url, codecs = picked
    proxy = lambda u: (f"{HOST_URL}/mediathek-passthru/{slug}/pl.m3u8?"
                       f"u={urllib.parse.quote(u, safe='')}")
    strip_audio = request.args.get("no_audio") == "1"
    out = ["#EXTM3U", "#EXT-X-VERSION:3"]
    audio_attr = ""
    if audio_url and not strip_audio:
        out.append(
            f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="audio",'
            f'DEFAULT=YES,AUTOSELECT=YES,URI="{proxy(audio_url)}"')
        audio_attr = ',AUDIO="aud"'
    codecs_attr = f',CODECS="{codecs}"' if codecs else ""
    # BANDWIDTH is required by the spec but doesn't matter when there's
    # only one variant — set it high so any future ABR-aware caller
    # treats this as the top tier.
    out.append(f"#EXT-X-STREAM-INF:BANDWIDTH=99999999"
               f"{codecs_attr}{audio_attr}")
    out.append(proxy(video_url))
    return Response("\n".join(out) + "\n",
                     mimetype="application/vnd.apple.mpegurl")


@app.route("/mediathek-passthru/<slug>/pl.m3u8")
def mediathek_passthru_variant(slug):
    """Proxy a sub-playlist (video or audio) from ARD, rewriting
    segment URIs to absolute upstream URLs."""
    upstream = request.args.get("u", "")
    if not upstream or not upstream.startswith("http"):
        abort(400)
    try:
        body = _mediathek_manifest_cache.get_or_fetch_variant(
            upstream,
            lambda u: _mediathek_upstream_pool.urlopen(u, timeout=8))
        txt = body.decode()
    except Exception as e:
        abort(502, f"upstream: {e}")
    base = upstream.rsplit("/", 1)[0] + "/"
    out_lines = []
    for line in txt.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and not stripped.startswith("http"):
            out_lines.append(base + stripped)
        else:
            out_lines.append(line)
    out = "\n".join(out_lines) + "\n"
    # Inject EXT-X-PLAYLIST-TYPE:EVENT if upstream omits it. Akamai-served
    # mediathek manifests have a DVR window of segments but no PLAYLIST-TYPE
    # tag, so AVPlayer/mpv treat them as pure-live and snap any seek back to
    # the live edge — breaking the app's scrub-back gesture (2026-05-23).
    # Same mechanism as hls_playlist_dvr above for tuner channels.
    if "#EXT-X-PLAYLIST-TYPE" not in out:
        out = re.sub(r"(#EXT-X-VERSION:[0-9]+\s*\n)",
                     r"\1#EXT-X-PLAYLIST-TYPE:EVENT\n",
                     out, count=1)
    return Response(out, mimetype="application/vnd.apple.mpegurl")


def _parse_master(master_url, master_text):
    """Pick best-bandwidth video variant and its audio companion from a
    master playlist. Returns (video_url, audio_url_or_none, codecs)."""
    base = master_url.rsplit("/", 1)[0] + "/"
    audio_map = {}   # group-id -> list of {uri, default}
    for line in master_text.splitlines():
        if line.startswith("#EXT-X-MEDIA") and 'TYPE=AUDIO' in line:
            gid = re.search(r'GROUP-ID="([^"]+)"', line)
            uri = re.search(r'URI="([^"]+)"', line)
            if not gid or not uri:
                continue
            u = uri.group(1)
            if not u.startswith("http"):
                u = base + u
            is_default = 'DEFAULT=YES' in line
            audio_map.setdefault(gid.group(1), []).append(
                {"uri": u, "default": is_default})
    # Per group, prefer DEFAULT=YES entry, else first one
    audio_pick = {}
    for gid, entries in audio_map.items():
        dflt = next((e for e in entries if e["default"]), entries[0])
        audio_pick[gid] = dflt["uri"]
    audio_map = audio_pick
    best = None
    best_bw = -1
    lines = master_text.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF"):
            bw = int(re.search(r"BANDWIDTH=(\d+)", line).group(1)) \
                if re.search(r"BANDWIDTH=(\d+)", line) else 0
            ag = re.search(r'AUDIO="([^"]+)"', line)
            codecs = re.search(r'CODECS="([^"]+)"', line)
            if i + 1 < len(lines):
                url = lines[i + 1].strip()
                if url and not url.startswith("#") and bw > best_bw:
                    if not url.startswith("http"):
                        url = base + url
                    best_bw = bw
                    best = (url, audio_map.get(ag.group(1)) if ag else None,
                            codecs.group(1) if codecs else "")
    return best


def _rewrite_pl_as_vod(pl_url, start_ts):
    """Fetch a media playlist, slice it to segments with PDT >= start_ts,
    and return a self-contained VOD-style playlist text."""
    pl = urllib.request.urlopen(pl_url, timeout=8).read().decode()
    base = pl_url.rsplit("/", 1)[0] + "/"
    out = ["#EXTM3U", "#EXT-X-VERSION:4",
           "#EXT-X-INDEPENDENT-SEGMENTS",
           "#EXT-X-PLAYLIST-TYPE:VOD",
           "#EXT-X-TARGETDURATION:3",
           "#EXT-X-MEDIA-SEQUENCE:0"]
    pending_extinf = None
    cur_pdt = None
    started = False
    for line in pl.splitlines():
        if line.startswith("#EXT-X-PROGRAM-DATE-TIME:"):
            try:
                from datetime import datetime
                cur_pdt = datetime.fromisoformat(
                    line.split(":", 1)[1].strip()).timestamp()
            except Exception:
                cur_pdt = None
            continue
        if line.startswith("#EXTINF"):
            pending_extinf = line
            continue
        if line.startswith("#") or not line.strip():
            continue
        seg_url = line.strip()
        if not seg_url.startswith("http"):
            seg_url = base + seg_url
        if cur_pdt is not None and cur_pdt + 2.5 >= start_ts:
            if not started:
                tz = time.strftime('%z', time.localtime(cur_pdt))
                tz_iso = tz[:3] + ':' + tz[3:]  # "+0200" → "+02:00"
                out.append(f"#EXT-X-PROGRAM-DATE-TIME:"
                           f"{time.strftime('%Y-%m-%dT%H:%M:%S', time.localtime(cur_pdt))}"
                           f"{tz_iso}")
                started = True
            if pending_extinf:
                out.append(pending_extinf)
            out.append(seg_url)
        pending_extinf = None
        if cur_pdt is not None:
            cur_pdt += 2
    out.append("#EXT-X-ENDLIST")
    return "\n".join(out) + "\n"


@app.route("/mediathek-clip/<slug>/master.m3u8")
def mediathek_clip_master(slug):
    """Build a VOD master playlist with video+audio variants whose URIs
    point back to our /video.m3u8 and /audio.m3u8 (which rewrite from
    the corresponding ARD playlists)."""
    info = MEDIATHEK_LIVE.get(slug)
    if not info:
        abort(404)
    base_url = info[0]
    try:
        start_ts = int(request.args.get("start", "0"))
    except ValueError:
        abort(400)
    if start_ts <= 0:
        abort(400)
    try:
        master = urllib.request.urlopen(base_url, timeout=8).read().decode()
    except Exception as e:
        abort(502, f"upstream: {e}")
    picked = _parse_master(base_url, master)
    if not picked:
        abort(502, "no variant")
    video_url, audio_url, codecs = picked
    # Store the picked upstream URLs so the client-facing video/audio
    # endpoints know what to rewrite. Keyed by slug for simplicity.
    _clip_state[slug] = {"video": video_url, "audio": audio_url}
    vurl = f"{HOST_URL}/mediathek-clip/{slug}/video.m3u8?start={start_ts}"
    aurl = f"{HOST_URL}/mediathek-clip/{slug}/audio.m3u8?start={start_ts}"
    codec_attr = f'CODECS="{codecs}"' if codecs else ''
    lines = ["#EXTM3U", "#EXT-X-VERSION:4",
             "#EXT-X-INDEPENDENT-SEGMENTS"]
    if audio_url:
        lines += [f'#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID="aud",NAME="Deutsch",'
                  f'DEFAULT=YES,AUTOSELECT=YES,LANGUAGE="de",'
                  f'CHANNELS="2",URI="{aurl}"']
        lines += [f'#EXT-X-STREAM-INF:BANDWIDTH=5640800,'
                  f'AVERAGE-BANDWIDTH=5000000,{codec_attr},'
                  f'RESOLUTION=1920x1080,FRAME-RATE=50.000,'
                  f'AUDIO="aud"',
                  vurl]
    else:
        lines += [f'#EXT-X-STREAM-INF:BANDWIDTH=5640800,{codec_attr}',
                  vurl]
    return Response("\n".join(lines) + "\n",
                     mimetype="application/vnd.apple.mpegurl")


_clip_state = {}


@app.route("/mediathek-clip/<slug>/video.m3u8")
def mediathek_clip_video(slug):
    state = _clip_state.get(slug) or {}
    url = state.get("video")
    if not url:
        abort(404)
    try:
        start_ts = int(request.args.get("start", "0"))
    except ValueError:
        abort(400)
    try:
        return Response(_rewrite_pl_as_vod(url, start_ts),
                        mimetype="application/vnd.apple.mpegurl")
    except Exception as e:
        abort(502, f"upstream: {e}")


@app.route("/mediathek-clip/<slug>/audio.m3u8")
def mediathek_clip_audio(slug):
    state = _clip_state.get(slug) or {}
    url = state.get("audio")
    if not url:
        abort(404)
    try:
        start_ts = int(request.args.get("start", "0"))
    except ValueError:
        abort(400)
    try:
        return Response(_rewrite_pl_as_vod(url, start_ts),
                        mimetype="application/vnd.apple.mpegurl")
    except Exception as e:
        abort(502, f"upstream: {e}")


# === Mediathek poster + episode-thumbnail API ===========================
# Two endpoints: /api/poster/show?topic=... (= wraps the existing show-meta
# cascade _fetch_show_meta = fernsehserien.de → TMDB → TVmaze) and
# /api/poster/episode?url=... (= og:image scrape of the episode landing
# page, universal fallback that works for ARD/ZDF/3sat/arte). Shared
# SQLite cache; 30d TTL for hits, 1d for 404 negatives so dead lookups
# don't re-fetch on every app tap. Built for the iOS app's Mediathek-Tab
# (2026-05-25, app-dev spec) — keeps poster-enrichment server-side so all
# clients see the same images and no client embeds TMDB tokens.

POSTER_CACHE_PATH = HLS_DIR / ".poster-cache.sqlite"
POSTER_HIT_TTL_S  = 30 * 24 * 3600
POSTER_MISS_TTL_S = 24 * 3600
_poster_db_lock = threading.Lock()


def _poster_db():
    conn = sqlite3.connect(str(POSTER_CACHE_PATH), timeout=5)
    conn.execute("""CREATE TABLE IF NOT EXISTS poster (
        key TEXT PRIMARY KEY,
        url TEXT,
        expires_at INTEGER NOT NULL)""")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _poster_lookup(key):
    """Returns (url_or_none, hit). hit=True means cached entry is fresh
    (url may be None = cached 404). hit=False = miss, caller must fetch."""
    now = int(time.time())
    with _poster_db_lock:
        conn = _poster_db()
        try:
            row = conn.execute(
                "SELECT url, expires_at FROM poster WHERE key=?",
                (key,)).fetchone()
        finally:
            conn.close()
    if not row or row[1] < now:
        return (None, False)
    return (row[0] or None, True)


def _poster_store(key, url):
    """url=None caches a 404 negative (shorter TTL)."""
    ttl = POSTER_MISS_TTL_S if url is None else POSTER_HIT_TTL_S
    expires = int(time.time()) + ttl
    with _poster_db_lock:
        conn = _poster_db()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO poster (key, url, expires_at) "
                "VALUES (?, ?, ?)", (key, url, expires))
            conn.commit()
        finally:
            conn.close()


# Two regexes because og:image can be either property=... content=... or
# content=... property=... — both orderings are valid HTML and seen in
# the wild (ARD: property first, some ZDF templates: content first).
_OG_IMAGE_RE_FWD = re.compile(
    r'<meta[^>]+(?:property|name)=["\']og:image(?::secure_url)?["\']'
    r'[^>]+content=["\']([^"\']+)["\']', re.IGNORECASE)
_OG_IMAGE_RE_REV = re.compile(
    r'<meta[^>]+content=["\']([^"\']+)["\']'
    r'[^>]+(?:property|name)=["\']og:image(?::secure_url)?["\']',
    re.IGNORECASE)

# Browser-like UA — bot-blockers (Akamai, Cloudflare on some sender
# pages) reject empty / Python-default UA strings outright.
_POSTER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) "
              "AppleWebKit/605.1.15 (KHTML, like Gecko) "
              "Version/17.0 Safari/605.1.15")


def _scrape_og_image(page_url, timeout=8):
    """Fetch HTML, regex out og:image, return absolute URL or None."""
    try:
        req = urllib.request.Request(page_url,
                                      headers={"User-Agent": _POSTER_UA})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            # og:image lives in <head>; 256 KB always reaches it even on
            # bloated pages, keeps Pi RAM bounded and parse fast.
            html = resp.read(256 * 1024).decode("utf-8", errors="replace")
    except Exception:
        return None
    for pat in (_OG_IMAGE_RE_FWD, _OG_IMAGE_RE_REV):
        m = pat.search(html)
        if m:
            img = m.group(1).strip()
            if not img:
                continue
            # Resolve protocol-relative + root-relative URLs against page.
            if img.startswith("//"):
                img = "https:" + img
            elif img.startswith("/"):
                p = urllib.parse.urlparse(page_url)
                img = f"{p.scheme}://{p.netloc}{img}"
            return img
    return None


def _apply_width(url, w):
    """Rewrite URL to request width=w if the image-CDN supports sizing.
    Unknown URL patterns pass through unchanged."""
    if not w or w <= 0 or not url:
        return url
    # TMDB: https://image.tmdb.org/t/p/wXXX/path.jpg — discrete widths only
    m = re.match(r"(https://image\.tmdb\.org/t/p/)w\d+(/.+)", url)
    if m:
        valid = [92, 154, 185, 342, 500, 780]
        chosen = min(valid, key=lambda v: abs(v - w))
        return f"{m.group(1)}w{chosen}{m.group(2)}"
    # ARD image service: api.ardmediathek.de/image-service/... — query param
    if "api.ardmediathek.de/image-service/" in url:
        p = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qs(p.query)
        q["w"] = [str(w)]
        return urllib.parse.urlunparse(
            p._replace(query=urllib.parse.urlencode(q, doseq=True)))
    # ZDF CDN: width param
    if "cdn.zdf.de" in url or "zdf-cdn.live" in url:
        p = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qs(p.query)
        q["width"] = [str(w)]
        return urllib.parse.urlunparse(
            p._replace(query=urllib.parse.urlencode(q, doseq=True)))
    return url


@app.route("/api/poster/episode")
def api_poster_episode():
    """Resolve an episode-thumbnail by og:image-scraping the mediathek
    episode landing page. 302 to image URL, 404 if not found.

    Query params:
      url=<https://...>   landing-page URL (required, from mediathekviewweb's
                          url_website field)
      w=<int>             optional image width hint, pass-through to ARD/ZDF/
                          TMDB image services that support it
    """
    page_url = (request.args.get("url") or "").strip()
    if not page_url.startswith("http"):
        abort(400, "url required (must be absolute http/https)")
    try:
        w = int(request.args.get("w", "0"))
    except ValueError:
        w = 0
    cache_key = f"ep|{page_url}"
    cached_url, hit = _poster_lookup(cache_key)
    if hit:
        if cached_url is None:
            abort(404)
        return redirect(_apply_width(cached_url, w), code=302)
    img = _scrape_og_image(page_url)
    _poster_store(cache_key, img)   # stores None too = 404 negative
    if img is None:
        abort(404)
    return redirect(_apply_width(img, w), code=302)


@app.route("/api/poster/show")
def api_poster_show():
    """Resolve a show-poster by topic via the existing show-meta cascade
    (_fetch_show_meta = fernsehserien.de → TMDB → TVmaze, with German-
    source-first preference to dodge TMDB's English homonym confusion).
    302 to image URL, 404 if no cascade source has a hit.

    Query params:
      topic=<name>        show name (required, from mediathekviewweb's
                          topic field, e.g. "Tatort")
      channel=<slug>      currently informational (cascade is title-driven);
                          reserved for future per-channel disambiguation
      w=<int>             optional image width hint, pass-through to TMDB
    """
    topic = (request.args.get("topic") or "").strip()
    if not topic:
        abort(400, "topic required")
    try:
        w = int(request.args.get("w", "0"))
    except ValueError:
        w = 0
    cache_key = f"sh|{topic}"
    cached_url, hit = _poster_lookup(cache_key)
    if hit:
        if cached_url is None:
            abort(404)
        return redirect(_apply_width(cached_url, w), code=302)
    meta = _fetch_show_meta(topic) or {}
    # tmdb_poster is the portrait (best for thumbnail-card display);
    # poster (fernsehserien) is a landscape banner — fall back to it
    # when TMDB has no hit.
    img = meta.get("tmdb_poster") or meta.get("poster")
    _poster_store(cache_key, img)
    if not img:
        abort(404)
    return redirect(_apply_width(img, w), code=302)


@app.route("/api/events/<slug>")
def api_events(slug):
    """EPG events for a channel within a time window (default: last 2h +
    next 1h). Used by the watch player to render chapter markers on the
    scrub bar."""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        return _cors(Response(json.dumps({"events": []}),
                               mimetype="application/json"))
    try:
        back = max(60, min(24 * 3600, int(request.args.get("back", 7200))))
        fwd  = max(0,  min(24 * 3600, int(request.args.get("fwd",  3600))))
    except ValueError:
        back, fwd = 7200, 3600
    data = fetch_epg(window_before=back, window_after=fwd)
    out = []
    for e in data["events"].get(slug, []):
        out.append({"start": e["start"], "stop": e["stop"],
                    "title": e.get("title", "")})
    return _cors(Response(json.dumps({"events": out, "now": data["now"]}),
                           mimetype="application/json"))


@app.route("/api/now/<slug>")
def api_now(slug):
    """Current EPG show for this channel + show-poster. Backward-compat
    keeps `time` (HH:MM-HH:MM) for the existing Watch-player UI; adds
    `subtitle`, `start`, `stop`, `poster_url` for the iOS app's live-
    tile refresh."""
    with cmap_lock:
        info = channel_map.get(slug)
    empty = {"title": None, "subtitle": "", "start": 0, "stop": 0,
             "time": "", "poster_url": ""}
    if not info:
        return _cors(Response(json.dumps(empty),
                              mimetype="application/json"))
    now_ts = int(time.time())
    try:
        data = fetch_epg(window_before=0, window_after=300)
        for e in data["events"].get(slug, []):
            if e["start"] <= now_ts < e["stop"]:
                title = (e.get("title") or "").strip()
                tm = (time.strftime("%H:%M", time.localtime(e["start"])) +
                      "–" +
                      time.strftime("%H:%M", time.localtime(e["stop"])))
                return _cors(Response(json.dumps({
                    "title": title,
                    "subtitle": (e.get("subtitle") or "").strip(),
                    "start": e["start"],
                    "stop": e["stop"],
                    "time": tm,
                    "poster_url": _show_poster_url("", title),
                }), mimetype="application/json"))
    except Exception:
        pass
    return _cors(Response(json.dumps(empty),
                          mimetype="application/json"))


@app.route("/api/recording-window/<slug>")
def api_recording_window(slug):
    """Find chained DVR entries on this channel that form a continuous
    back-buffer. Starts from the currently-recording entry (if any)
    and walks backwards through contiguous completed recordings
    (gap ≤ 180 s counts as "contiguous"). Returns the chain sorted
    oldest → newest. Empty if nothing relevant is on disk."""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        return _cors(Response(json.dumps({"chain": []}),
                               mimetype="application/json"))
    ch_uuid = info.get("uuid")
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=200&sort=start&dir=DESC",
            timeout=5).read())
    except Exception:
        return _cors(Response(json.dumps({"chain": []}),
                               mimetype="application/json"))
    now_ts = int(time.time())
    CHAIN_GAP = 180   # seconds — two recordings within 3 min are "chained"
    MAX_BACK  = 6 * 3600  # how far back we'll walk (safety)
    # Collect relevant DVR entries: currently-recording OR completed
    # within the last 6 h on this channel.
    candidates = []
    for e in data.get("entries", []):
        if e.get("channel") != ch_uuid:
            continue
        start = e.get("start", 0); stop = e.get("stop", 0)
        sched = e.get("sched_status", "")
        is_running = (start <= now_ts < stop and sched == "recording")
        is_recent  = (sched == "completed"
                      and stop > now_ts - MAX_BACK
                      and stop <= now_ts)
        if not (is_running or is_recent):
            continue
        if not e.get("filename") and not is_running:
            continue   # nothing on disk yet
        candidates.append({
            "uuid": e.get("uuid"),
            "title": e.get("disp_title") or "",
            "start": start,
            "stop": stop,
            "running": is_running,
        })
    candidates.sort(key=lambda c: c["start"])
    # Walk backwards from the newest relevant entry, pulling in the
    # previous one if it's within CHAIN_GAP of the current chain-start.
    if not candidates:
        return _cors(Response(json.dumps({"chain": []}),
                               mimetype="application/json"))
    chain = [candidates[-1]]
    for cand in reversed(candidates[:-1]):
        if chain[0]["start"] - cand["stop"] > CHAIN_GAP:
            break
        chain.insert(0, cand)
    for c in chain:
        c["hls_url"] = f"{HOST_URL}/recording/{c['uuid']}/index.m3u8"
    return _cors(Response(json.dumps({"chain": chain}),
                           mimetype="application/json"))


@app.route("/api/internal/mediathek-cache-stats")
def api_mediathek_cache_stats():
    """Debug: hit/miss + speculative-prefetch counters for Step-1 verification."""
    return _cors(Response(json.dumps(_mediathek_manifest_cache.stats(),
                                       ensure_ascii=False, sort_keys=True),
                          mimetype="application/json"))


@app.route("/api/internal/mediathek-pool-stats")
def api_mediathek_pool_stats():
    """Debug: hit/miss counts + per-host pool sizes for Step-7 verification."""
    return _cors(Response(json.dumps(_mediathek_upstream_pool.stats(),
                                       ensure_ascii=False, sort_keys=True),
                          mimetype="application/json"))


@app.route("/api/tuners-in-use")
def api_tuners_in_use():
    """Per-mux snapshot of which channels currently hold a DVB-C tuner slot.

    Used by the app-side Mux-Verify mode (Step-0 of gateway-speedup
    initiative): request channel A, request channels A+B in parallel,
    inspect this endpoint's by_mux delta. If delta-keys is 1 → A and B
    share a mux/tuner. If 2 → different tuners. Pure metadata-free
    verification — doesn't trust tvh's slug→mux mapping, watches actual
    subscription state.

    subscribers vs warm_only: a slug counts as "active" if a viewer has
    pulled a segment within the last 5s. warm_only=true means the slot
    is reserved (= ALWAYS_WARM or prewarm-pool) but no active viewer.
    Both flavors reserve a tuner against TUNER_TOTAL — for the verify
    test, the distinction is informational only.

    Mediathek/IPTV passthru channels have no mux_uuid and are excluded.
    """
    now = time.time()
    ACTIVE_WINDOW_S = 5.0

    with active_lock:
        active_snapshot = list(channels.items())
    with cmap_lock:
        cmap_snapshot = dict(channel_map)

    by_mux = {}
    for slug, info in active_snapshot:
        if not info or info.get("process") is None:
            continue
        cm = cmap_snapshot.get(slug, {})
        mux_uuid = cm.get("mux_uuid", "")
        if not mux_uuid:
            continue  # IPTV/passthru, no DVB tuner held
        last_seen = info.get("last_seen", 0)
        is_active = (now - last_seen) < ACTIVE_WINDOW_S
        entry = by_mux.setdefault(mux_uuid, {
            "name": cm.get("mux_name", ""),
            "channels": [],
            "subscribers": 0,
            "warm_only": True,
        })
        entry["channels"].append(slug)
        entry["subscribers"] += 1
        if is_active:
            entry["warm_only"] = False

    return _cors(Response(
        json.dumps({
            "total": TUNER_TOTAL,
            "in_use": len(by_mux),
            "free": max(0, TUNER_TOTAL - len(by_mux)),
            "by_mux": by_mux,
        }, ensure_ascii=False, sort_keys=True),
        mimetype="application/json"))


@app.route("/api/pi-context")
def api_pi_context():
    """Aggregate Pi-side state for baseline-capture manifests. One
    round-trip = one source-of-truth, replaces 4 separate API calls
    in the dev's capture script. All sub-fetches best-effort: any
    individual failure is silently degraded to a safe default
    (= empty list / None) so a transient tvh hiccup doesn't tank
    the whole capture run.
    """
    recs = []
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=500",
            timeout=5).read())
        recs = [e["uuid"] for e in data.get("entries", [])
                if e.get("sched_status") == "recording"]
    except Exception:
        pass
    with active_lock:
        warm = sorted(s for s, i in channels.items()
                      if i and i.get("process") is not None)
    load_1m = None
    try:
        load_1m = float(Path("/proc/loadavg").read_text().split()[0])
    except Exception:
        pass
    # Count active subscriptions. Try tv-receiver first (= the slots that
    # are actually serving live consumers); fall back to tvh if unreachable.
    n_subs = 0
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TV_RECEIVER_BASE}/api/status/subscriptions", timeout=3).read())
        n_subs = len(data.get("entries", []))
    except Exception:
        try:
            data = json.loads(urllib.request.urlopen(
                f"{dvr_base()}/api/status/subscriptions", timeout=5).read())
            n_subs = len(data.get("entries", []))
        except Exception:
            pass
    return _cors(Response(json.dumps({
        "active_recordings": recs,
        "active_subscriptions": n_subs,
        "load_1m": load_1m,
        "warm_streams": warm,
        "ts": int(time.time()),
    }, ensure_ascii=False, sort_keys=True), mimetype="application/json"))


@app.route("/api/internal/force-evict-others", methods=["POST"])
def api_force_evict_others():
    """Capture-mode helper: stop all warm channels EXCEPT the ones in
    ?keep=slug1,slug2. Lets baseline-capture scripts guarantee that
    only the channel-under-test is warm during a measurement, so the
    Pi5 load isn't polluted by stale tuners from earlier taps.

    ALWAYS_WARM-pinned slugs are exempt (= same guard as the normal
    LRU eviction).

    NOT for production use — bypasses the normal WARM_TTL policy. Lives
    under /api/internal/ to make that obvious.
    """
    keep = set((request.args.get("keep") or "").split(","))
    keep.discard("")
    with active_lock:
        targets = [s for s, i in channels.items()
                   if i and i.get("process") is not None
                   and s not in keep
                   and s not in ALWAYS_WARM]
    for s in targets:
        print(f"[force-evict] stopping {s}", flush=True)
        stop_channel(s)
    return _cors(Response(json.dumps({
        "evicted": targets,
        "kept": sorted(keep),
    }), mimetype="application/json"))


@app.route("/api/ready")
def api_ready():
    """Readiness probe. Returns ready=true once the initial channel-map
    load has completed (= /play requests can resolve a slug). Used by
    capture scripts to poll after `docker compose restart` instead of
    a blind sleep — typical readiness is ~10-15s but cold first-load
    can take 30s+ if tvh is also booting.
    """
    with cmap_lock:
        n_channels = len(channel_map)
    ready = n_channels > 0
    return _cors(Response(json.dumps({
        "ready": ready,
        "channel_map_size": n_channels,
    }), mimetype="application/json"))


@app.route("/api/channels")
def api_channels():
    """Live-TV channel listing for external apps + the Watch-player
    swipe navigation. Sorted by recent watch time (most-used first).
    Each channel includes the curated PNG icon (iOS UIImage-friendly),
    current EPG show with show-poster, live + DVR HLS URLs, and the
    is_warm flag (= ffmpeg producing segments right now, instant
    playback ≤ 1s; cold channels need ~5-10s spin-up).

    Response: {channels: [...], n: N, warm_used: K, warm_max: M,
               server_time: <unix_ts>} — CORS-enabled."""
    now_ts = int(time.time())
    with stats_lock:
        st_snap = {s: dict(v) for s, v in stats.items()}
    with active_lock:
        warm_slugs = set(channels.keys())
        for s, i in channels.items():
            st_snap.setdefault(s, {"watch_seconds": 0, "starts": 0})
            st_snap[s]["watch_seconds"] = st_snap[s].get("watch_seconds", 0) \
                                           + (now_ts - i.get("started_at", now_ts))
    with cmap_lock:
        items = list(channel_map.items())
    items.sort(key=lambda kv: (
        -st_snap.get(kv[0], {}).get("watch_seconds", 0),
        -st_snap.get(kv[0], {}).get("starts", 0),
        kv[1]["name"].lower(),
    ))

    # One EPG lookup for all channels — saves ~24× tvh roundtrips.
    try:
        epg = fetch_epg(window_before=0, window_after=300)
        epg_events = epg.get("events", {}) if isinstance(epg, dict) else {}
    except Exception:
        epg_events = {}

    host = HOST_URL.rstrip("/")
    out = []
    for slug, info in items:
        icon = _channel_logo_url(slug, info.get("icon", ""),
                                 ext_priority=("png", "svg", "jpg"))
        current = None
        for e in epg_events.get(slug, []):
            if e["start"] <= now_ts < e["stop"]:
                title = (e.get("title") or "").strip()
                current = {
                    "title": title,
                    "subtitle": (e.get("subtitle") or "").strip(),
                    "start": e["start"],
                    "stop": e["stop"],
                    "poster_url": _show_poster_url("", title),
                }
                break
        # Mediathek-passthru wins for the 6 public broadcasters — same
        # app_live_url, but internally the gateway routes to the CDN
        # instead of spinning up a tuner. Surfaces `via` so the app can
        # render a "via Mediathek" badge if desired, and
        # `mediathek_window` (= upstream restart depth in seconds) so
        # the app can advertise longer scrub-back than the local 2 h.
        mt = MEDIATHEK_LIVE.get(slug)
        out.append({
            "slug": slug,
            "name": info["name"],
            "icon": icon,
            "is_warm": (slug in warm_slugs) or bool(mt),
            "live_url": f"{host}/hls/{slug}/index.m3u8",
            "dvr_url": f"{host}/hls/{slug}/dvr.m3u8",
            # App-scoped endpoints — single-tuner cap + tighter idle
            # eviction so DVR isn't blocked by lingering app sessions.
            "app_live_url": f"{host}/api/app/live/{slug}/index.m3u8",
            "app_dvr_url": f"{host}/api/app/live/{slug}/dvr.m3u8",
            "ads_stream_url": f"{host}/api/live-ads-stream/{slug}",
            "via": "mediathek" if mt else "tuner",
            "mediathek_window": (mt[1] if mt else 0),
            "current": current,
        })

    return _cors(Response(
        json.dumps({"channels": out, "n": len(out),
                    "warm_used": len(warm_slugs),
                    "warm_max": MAX_WARM_STREAMS,
                    "server_time": now_ts}),
        mimetype="application/json"))


@app.route("/api/internal/scheduled-events")
def api_internal_scheduled_events():
    """Lightweight list of DVR entries (scheduled OR currently
    recording) keyed by EPG event id. Used by /epg's
    syncScheduledFromUpcoming to refresh the green-dot indicator
    after schedule actions without a full page reload.

    Two endpoints needed because tvh splits them: grid_upcoming
    contains future-scheduled entries, grid_recording is the
    in-progress ones. Without the latter, an actively-recording
    show loses its green dot the moment it starts."""
    out = []
    seen = set()
    for ep in ("/api/dvr/entry/grid_upcoming",
               "/api/dvr/entry/grid_recording"):
        try:
            data = json.loads(urllib.request.urlopen(
                f"{dvr_base()}{ep}?limit=500", timeout=5).read())
            for e in data.get("entries", []):
                eid = e.get("broadcast")
                if eid is None:
                    continue
                u = e.get("uuid")
                if u in seen:
                    continue
                seen.add(u)
                out.append({
                    "eid": eid,
                    "uuid": u,
                    "channel": e.get("channel"),
                    "start": e.get("start"),
                })
        except Exception:
            pass
    return _cors(Response(json.dumps({"entries": out}),
                            mimetype="application/json"))


@app.route("/record-event/<event_id>")
def record_event(event_id):
    """Schedule a DVR entry for a specific EPG event (whole programme).
    Adds 5 min pre / 10 min post padding so we don't lose the start
    if the broadcaster runs early or the end if they run long.
    Witnessed: 'Davina & Shania - We Love Monaco' on RTLZWEI scheduled
    via this endpoint without padding, broadcast ran ~13 min past EPG
    end, recording cut off at end. tvh's global default pre/post is
    0/0 — autorec rules carry their own padding but manual schedules
    from this endpoint don't inherit anything."""
    body = urllib.parse.urlencode({
        "event_id": event_id, "config_uuid": "",
        "start_extra": 5, "stop_extra": 10,
    }).encode()
    req = urllib.request.Request(f"{dvr_base()}/api/dvr/entry/create_by_event",
                                  data=body, method="POST")
    try:
        res = urllib.request.urlopen(req, timeout=10).read().decode()
        data = json.loads(res)
        uuid = (data.get("uuid") or [None])[0] \
            if isinstance(data.get("uuid"), list) else data.get("uuid")
        return _cors(Response(json.dumps({"ok": True, "uuid": uuid}),
                               mimetype="application/json"))
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")


# DVB channels where the ARD Mediathek is likely to have coverage.
# We don't filter further by Mediathek publicationService because
# ARD-aired shows are produced by various regional broadcasters
# (WDR, NDR, BR, …) and appear in search under the producing
# broadcaster's name, not under "Das Erste".
ARD_SEARCH_CHANNELS = {
    "das-erste-hd", "tagesschau24-hd", "rbb-berlin-hd",
    "arte-hd", "kika-hd", "3sat-hd",
}
ZDF_SEARCH_CHANNELS = {"zdf-hd"}


def _mediathek_match(title, ch_slug, start_ts):
    """Core matcher used by both the lookup endpoint and the autorec
    follow-up loop. Returns the best match dict (same shape as the
    endpoint) or None. Title + channel slug + broadcast timestamp in,
    match-with-HLS-capable-id out."""
    if not title or not start_ts:
        return None
    if ch_slug in ARD_SEARCH_CHANNELS:
        source = "ard"
    elif ch_slug in ZDF_SEARCH_CHANNELS:
        source = "zdf"
    else:
        return None
    from datetime import datetime
    MAX_DELTA = 48 * 3600
    title_l = title.lower()
    best = None
    if source == "ard":
        qs = urllib.parse.urlencode({
            "searchString": title,
            "searchResultsPageSize": "24",
        })
        search_url = (f"https://api.ardmediathek.de/page-gateway/pages/ard/"
                      f"search?{qs}")
        data = None
        for attempt in range(2):
            try:
                data = json.loads(urllib.request.urlopen(
                    search_url, timeout=15).read())
                break
            except Exception as e:
                print(f"mediathek search (try {attempt+1}): {e}", flush=True)
        if data is None:
            return None
        for v in data.get("vodResults", []):
            svc = (v.get("publicationService") or {}).get("name", "")
            vt = (v.get("longTitle") or v.get("mediumTitle") or "").strip()
            vt_l = vt.lower()
            if title_l not in vt_l and vt_l not in title_l:
                continue
            bt = 0
            if v.get("broadcastedOn"):
                try:
                    bt = int(datetime.fromisoformat(
                        v["broadcastedOn"].replace("Z", "+00:00")).timestamp())
                except Exception:
                    pass
            if not bt or abs(bt - start_ts) > MAX_DELTA:
                continue
            delta = abs(bt - start_ts)
            if best is None or delta < best["_delta"]:
                avail_to = 0
                if v.get("availableTo"):
                    try:
                        avail_to = int(datetime.fromisoformat(
                            v["availableTo"].replace("Z", "+00:00")).timestamp())
                    except Exception:
                        pass
                best = {
                    "_delta": delta, "source": "ard",
                    "title": vt, "channel": svc,
                    "broadcast": bt, "available_to": avail_to,
                    "duration": v.get("duration", 0),
                    "id": v.get("id"),
                    "player_url": f"https://www.ardmediathek.de/video/{v.get('id')}",
                }
    else:  # zdf
        for r in _zdf_search(title):
            rt = (r.get("title") or "").lower()
            if title_l not in rt and rt not in title_l:
                continue
            if not r["broadcast_ts"]:
                continue
            delta = abs(r["broadcast_ts"] - start_ts)
            if delta > MAX_DELTA:
                continue
            if best is None or delta < best["_delta"]:
                best = {
                    "_delta": delta, "source": "zdf",
                    "title": r["title"], "channel": "ZDF",
                    "broadcast": r["broadcast_ts"], "available_to": 0,
                    "duration": r["duration"],
                    "id": r["id"],
                    "player_url": f"https://www.zdf.de/play/{r['id']}",
                }
    if not best:
        return None
    if best["source"] == "zdf" and not best["available_to"]:
        _, vis_to = _zdf_resolve_hls(best["id"])
        if vis_to:
            best["available_to"] = vis_to
    best.pop("_delta", None)
    return best


@app.route("/api/mediathek-lookup/<event_id>")
def api_mediathek_lookup(event_id):
    """For an EPG event, try to find the same show in the ARD
    Mediathek and return its availability window + player URL."""
    title, ch_name, start_ts = "", "", 0
    # Synthetic archive key: arc_<slug>_<start> for past events that
    # were archived before we started persisting the tvheadend eid.
    if event_id.startswith("arc_"):
        try:
            _, slug_, start_s = event_id.split("_", 2)
            start_ts = int(start_s)
            with _epg_archive_lock:
                rec = _epg_archive.get((slug_, start_ts)) or {}
            title = rec.get("title", "")
            with cmap_lock:
                ch_name = channel_map.get(slug_, {}).get("name", slug_)
        except Exception:
            pass
    else:
        try:
            ev = json.loads(urllib.request.urlopen(
                f"{dvr_base()}/api/epg/events/load?eventId={event_id}"
                + (f"&slug={urllib.parse.quote(request.args.get('slug',''))}"
                   if request.args.get('slug') else ""),
                timeout=6).read())
            entry = (ev.get("entries") or [{}])[0]
            title = (entry.get("title") or "").strip()
            ch_name = entry.get("channelName") or ""
            start_ts = entry.get("start", 0)
        except Exception:
            pass
        if not title:
            try:
                with _epg_archive_lock:
                    for (slug_, start), rec in _epg_archive.items():
                        if str(rec.get("event_id")) == str(event_id):
                            title = rec.get("title", "")
                            start_ts = start
                            with cmap_lock:
                                ch_name = (channel_map.get(slug_, {})
                                            .get("name", slug_))
                            break
            except Exception:
                pass
    if not title:
        return _cors(Response(json.dumps({"match": None}),
                               mimetype="application/json"))
    slug = slugify(ch_name)
    if slug not in ARD_SEARCH_CHANNELS and slug not in ZDF_SEARCH_CHANNELS:
        return _cors(Response(json.dumps({
            "match": None, "reason": "channel not covered"}),
            mimetype="application/json"))
    match = _mediathek_match(title, slug, start_ts)
    return _cors(Response(json.dumps({"match": match}),
                           mimetype="application/json"))




@app.route("/api/mediathek-schedule/<event_id>", methods=["POST"])
def api_mediathek_schedule(event_id):
    """Store a virtual recording that streams from ARD Mediathek on
    playback. Uses the lookup we already have, resolves the item's
    HLS master URL, and saves a local stub so /recordings + the
    recording player pick it up."""
    ev_title, ch_name, ev_start, ev_stop = "", "", 0, 0
    if event_id.startswith("arc_"):
        try:
            _, slug_, start_s = event_id.split("_", 2)
            ev_start = int(start_s)
            with _epg_archive_lock:
                rec = _epg_archive.get((slug_, ev_start)) or {}
            ev_title = rec.get("title", "")
            ev_stop = rec.get("stop", 0)
            with cmap_lock:
                ch_name = channel_map.get(slug_, {}).get("name", slug_)
        except Exception:
            pass
    else:
        try:
            ev = json.loads(urllib.request.urlopen(
                f"{dvr_base()}/api/epg/events/load?eventId={event_id}"
                + (f"&slug={urllib.parse.quote(request.args.get('slug',''))}"
                   if request.args.get('slug') else ""),
                timeout=6).read())
            entry = (ev.get("entries") or [{}])[0]
            ev_title = (entry.get("title") or "").strip()
            ch_name = entry.get("channelName") or ""
            ev_start = entry.get("start", 0)
            ev_stop = entry.get("stop", 0)
        except Exception:
            pass
        if not ev_title:
            with _epg_archive_lock:
                for (slug_, start), rec in _epg_archive.items():
                    if str(rec.get("event_id")) == str(event_id):
                        ev_title = rec.get("title", "")
                        ev_start = start
                        ev_stop = rec.get("stop", 0)
                        with cmap_lock:
                            ch_name = (channel_map.get(slug_, {})
                                        .get("name", slug_))
                        break
    if not ev_title:
        return Response(json.dumps({"ok": False,
                                     "error": "event not found"}),
                        status=404, mimetype="application/json")
    # Re-run the lookup. Loopback HTTP is simpler than refactoring
    # the matcher into a shared helper for one more caller.
    try:
        res = urllib.request.urlopen(
            f"http://localhost:8080/api/mediathek-lookup/{event_id}",
            timeout=8).read()
        match = json.loads(res).get("match")
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": f"match: {e}"}),
                        status=500, mimetype="application/json")
    if not match or not match.get("id"):
        return Response(json.dumps({"ok": False,
                                     "error": "no mediathek match"}),
                        status=404, mimetype="application/json")
    hls_url = _resolve_mediathek_hls(match["id"],
                                       source=match.get("source", "ard"))
    if not hls_url:
        return Response(json.dumps({"ok": False,
                                     "error": "no hls url"}),
                        status=500, mimetype="application/json")
    import uuid as uuid_mod
    vuuid = "mt_" + uuid_mod.uuid4().hex[:16]
    with _mediathek_rec_lock:
        _mediathek_rec[vuuid] = {
            "title": match.get("title") or ev_title,
            "channel": ch_name,
            "start": ev_start,
            "stop": ev_stop,
            "hls_url": hls_url,
            "available_to": match.get("available_to", 0),
            "eid": event_id,
            "created_at": int(time.time()),
        }
    save_mediathek_rec()
    return _cors(Response(json.dumps({
        "ok": True, "uuid": vuuid,
        "title": _mediathek_rec[vuuid]["title"],
        "available_to": _mediathek_rec[vuuid]["available_to"],
    }), mimetype="application/json"))


@app.route("/record-series/<event_id>")
def record_series(event_id):
    """Create a tvheadend autorec rule to capture every future airing of
    this programme on the same channel. Title-regex based because
    German DVB-C doesn't ship series-link CRIDs."""
    # Resolve event → title + channel
    try:
        ev = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/epg/events/load?eventId={event_id}"
            + (f"&slug={urllib.parse.quote(request.args.get('slug',''))}"
               if request.args.get('slug') else ""),
            timeout=6).read())
        entry = (ev.get("entries") or [{}])[0]
        title = entry.get("title")
        ch_uuid = entry.get("channelUuid")
        ch_name = entry.get("channelName") or ""
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": f"lookup: {e}"}),
                        status=500, mimetype="application/json")
    if not title or not ch_uuid:
        return Response(json.dumps({"ok": False,
                                     "error": "event not found"}),
                        status=404, mimetype="application/json")
    # Anchor the regex so "Tagesschau" doesn't grab "Tagesschau um 5".
    title_regex = "^" + re.escape(title) + "$"
    conf = {
        "enabled": True,
        "name": f"{title} ({ch_name})",
        "title": title_regex,
        "fulltext": False,
        "channel": ch_uuid,
        "comment": f"auto via /record-series for eid={event_id}",
        # Padding (minutes): broadcasters routinely start 1-3 min early
        # and run 5-10 min long past the EPG-scheduled stop. Tvh's
        # global pre/post-extra-time isn't applied to autorec-spawned
        # entries (they freeze 0/0 unless overridden here). 5 min pre /
        # 10 min post matches what we patched onto the existing rules.
        "start_extra": 5,
        "stop_extra": 10,
        # Cap parallel scheduled entries per series. Without this tvh
        # schedules every matching airing in the EPG window — Comedy
        # Central runs South Park 5-10x/day → 54 entries queued from a
        # single click. 10 keeps a Daily covered for ~1-2 weeks; tvh
        # auto-schedules the next one as each completes. Override in
        # the tvh autorec UI if you actually want unlimited (rare).
        "maxsched": 10,
    }
    # Lock to the seed event's time-of-day so a midday rerun on the
    # same channel doesn't get picked up alongside the prime-time
    # original. tvheadend autorec uses HH:MM strings and treats
    # start_window as the upper bound of the acceptable start time
    # (not a duration). Bracket the seed by -5/+15 min — drift seen
    # on kabel eins is typically forward (slot fills with promos) but
    # can be backward by 1-2 min if a preceding programme finishes
    # early. 20-min total window stays well clear of any rerun slot.
    #
    # Exception: when the EPG already shows MULTIPLE same-title same-
    # channel events today (= classic morning kid-block pattern with
    # SpongeBob 06:25 + 06:50 + 07:15, or daytime Tröödeltrupp marathons),
    # skip the time window entirely — the user wants every episode of
    # the day, not just the one that happened to be the seed slot.
    # The midnight-rerun concern was for prime-time singletons; not
    # relevant once tvh sees siblings.
    seed_start = entry.get("start")
    if seed_start:
        siblings = 0
        try:
            day_end = seed_start - (seed_start % 86400) + 86400  # end of day
            params = urllib.parse.urlencode({
                "limit": 100, "channel": ch_uuid, "title": title})
            grid = json.loads(urllib.request.urlopen(
                f"{dvr_base()}/api/epg/events/grid?{params}",
                timeout=5).read())
            for ev in grid.get("entries", []):
                s = ev.get("start", 0)
                if (s != seed_start and s < day_end
                        and ev.get("channelUuid") == ch_uuid
                        and ev.get("title") == title):
                    siblings += 1
        except Exception:
            pass
        if siblings == 0:
            lt = time.localtime(seed_start)
            seed_min = lt.tm_hour * 60 + lt.tm_min
            start_min = (seed_min - 5) % (24 * 60)
            end_min = (seed_min + 15) % (24 * 60)
            conf["start"] = f"{start_min // 60:02d}:{start_min % 60:02d}"
            conf["start_window"] = f"{end_min // 60:02d}:{end_min % 60:02d}"
        else:
            print(f"[record-series] {title}: {siblings} sibling episode(s) "
                  f"on {ch_name} today — omitting start_window so all get "
                  f"scheduled", flush=True)
    # Idempotency check: tvheadend doesn't dedup autorec rules by
    # (title, channel) — calling /record-series twice on the same show
    # produces two identical rules, doubling future scheduled entries
    # (root cause of the SpongeBob 153-instead-of-75 incident
    # 2026-05-01). Look up existing rules with same regex+channel and
    # return early if already present.
    try:
        existing_grid = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/autorec/grid?limit=500",
            timeout=5).read())
        for er in existing_grid.get("entries", []):
            if (er.get("title") == title_regex
                    and er.get("channel") == ch_uuid
                    and er.get("enabled")):
                return _cors(Response(json.dumps({
                    "ok": True, "uuid": er.get("uuid"),
                    "title": title, "channel": ch_name,
                    "already_exists": True,
                    "scheduled": 0,
                    "tuner_conflicts": [],
                    "tuner_total": TUNER_TOTAL}),
                    mimetype="application/json"))
    except Exception:
        # Best-effort dedup; on failure we still create — duplicate is
        # better than failing the user's recording request.
        pass
    body = urllib.parse.urlencode({"conf": json.dumps(conf)}).encode()
    try:
        req = urllib.request.Request(
            f"{dvr_base()}/api/dvr/autorec/create",
            data=body, method="POST")
        res = urllib.request.urlopen(req, timeout=10).read().decode()
        data = json.loads(res) if res else {}
        autorec_uuid = data.get("uuid")
        # tvheadend schedules matching EPG events asynchronously. Give
        # it ~2 s then count how many upcoming DVR entries the rule has
        # spawned so the client can show "N Folgen geplant".
        scheduled = 0
        spawned_uuids = set()
        try:
            time.sleep(2)
            up = json.loads(urllib.request.urlopen(
                f"{dvr_base()}/api/dvr/entry/grid_upcoming?limit=500",
                timeout=10).read())
            for e in up.get("entries", []):
                if e.get("autorec") == autorec_uuid:
                    scheduled += 1
                    spawned_uuids.add(e.get("uuid"))
        except Exception:
            pass
        # Tuner-conflict report: of the entries the autorec just spawned,
        # how many overlap with > TUNER_TOTAL unique muxes? Helps the
        # caller surface a warning ("3 of 5 planned episodes will silently
        # fail at recording time — same-time conflict with X").
        conflicts = []
        try:
            cmap = _compute_tuner_conflicts(int(time.time()))
            conflicts = sorted(
                u for u in spawned_uuids
                if cmap.get(u, 0) > TUNER_TOTAL)
        except Exception:
            pass
        return _cors(Response(json.dumps({"ok": True,
                                           "uuid": autorec_uuid,
                                           "title": title,
                                           "channel": ch_name,
                                           "scheduled": scheduled,
                                           "tuner_conflicts": conflicts,
                                           "tuner_total": TUNER_TOTAL}),
                               mimetype="application/json"))
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")


@app.route("/cancel-series/<autorec_uuid>")
def cancel_series(autorec_uuid):
    """Delete an autorec rule and cancel every upcoming DVR entry it
    has spawned. Already-recorded episodes on disk are left alone —
    the user is explicitly only tearing down the "record future
    episodes" automation, not their archive."""
    cancelled = 0
    try:
        up = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid_upcoming?limit=500",
            timeout=10).read())
        for e in up.get("entries", []):
            if e.get("autorec") != autorec_uuid:
                continue
            ep_uuid = e.get("uuid")
            if not ep_uuid:
                continue
            body = urllib.parse.urlencode({"uuid": ep_uuid}).encode()
            for ep in ("/api/dvr/entry/cancel",
                       "/api/dvr/entry/remove"):
                try:
                    urllib.request.urlopen(
                        urllib.request.Request(
                            f"{dvr_base()}{ep}",
                            data=body, method="POST"),
                        timeout=5).read()
                    cancelled += 1
                    break
                except Exception:
                    continue
    except Exception:
        pass
    try:
        body = urllib.parse.urlencode({"uuid": autorec_uuid}).encode()
        urllib.request.urlopen(
            urllib.request.Request(f"{dvr_base()}/api/idnode/delete",
                                    data=body, method="POST"),
            timeout=10).read()
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")
    return _cors(Response(json.dumps({"ok": True,
                                        "cancelled": cancelled}),
                           mimetype="application/json"))


# --- EPG metadata enrichment via TVmaze (free, no API key) -----
_epg_meta = {}
_epg_meta_lock = threading.Lock()


def _load_epg_meta():
    global _epg_meta
    if not EPG_META_FILE.exists():
        return
    try:
        _epg_meta = json.loads(EPG_META_FILE.read_text())
        print(f"Loaded EPG metadata for {len(_epg_meta)} titles", flush=True)
    except Exception as e:
        print(f"load epg meta: {e}", flush=True)


def _save_epg_meta():
    try:
        tmp = EPG_META_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(_epg_meta))
        tmp.replace(EPG_META_FILE)
    except Exception as e:
        print(f"save epg meta: {e}", flush=True)


def _normalize_title(t):
    """Strip episode-suffix variants so 'Show - Episode Title' looks up
    the parent show. TVmaze indexes shows, not episodes."""
    if not t:
        return ""
    # Common DE-EPG pattern: "Show - Episode Title" or "Show: Subtitle"
    for sep in (" - ", ": "):
        if sep in t:
            return t.split(sep, 1)[0].strip()
    return t.strip()


def _fetch_tmdb(title):
    """Query TMDB. Accepts both v4 bearer tokens (JWT, starts with
    "eyJ") and v3 api_key values (32-char hex). Tries /search/tv first
    (= most German EPG content is series); falls back to /search/movie
    for one-off films like Rocky/Asterix/Jungle Cruise. Stores `kind`
    so downstream code (singleton heuristic, etc.) can distinguish
    series from movies authoritatively. TMDB reports vote_average=0.0
    when no user ratings — treat as "no rating" rather than literal 0."""
    def _query(endpoint, name_field):
        try:
            params = {"query": title, "language": "de-DE"}
            headers = {"Accept": "application/json"}
            if TMDB_API_KEY.startswith("eyJ"):
                headers["Authorization"] = f"Bearer {TMDB_API_KEY}"
            else:
                params["api_key"] = TMDB_API_KEY
            url = (f"https://api.themoviedb.org/3/search/{endpoint}?"
                   + urllib.parse.urlencode(params))
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=8) as r:
                return (json.loads(r.read()).get("results") or [])
        except Exception as e:
            print(f"[epg-enrich tmdb {endpoint}] {title}: {e}", flush=True)
            return []
    def _pick(results, name_field):
        if not results:
            return None
        lc = title.lower()
        return (next((r for r in results
                      if (r.get(name_field) or "").lower() == lc), None)
                or next((r for r in results
                         if (r.get(name_field) or "").lower().startswith(lc)),
                        results[0]))
    # Query BOTH endpoints, pick the result with higher vote_count.
    # Older "TV first, movie fallback" picked bogus 1-vote TV shows
    # for genuine movies (= "Minions" matched a 10/10 single-vote
    # TV entry instead of the actual film with 17000+ votes). vote_count
    # is the strongest signal of "this is the canonical match" — high-
    # votes results are real well-known content; low-vote results are
    # often homonyms or fan-uploaded mismatches.
    tv_pick = _pick(_query("tv", "name"), "name")
    mv_pick = _pick(_query("movie", "title"), "title")
    tv_votes = (tv_pick or {}).get("vote_count") or 0
    mv_votes = (mv_pick or {}).get("vote_count") or 0
    if tv_pick and mv_pick:
        # Both have results. Tie-break by vote_count, then prefer movie
        # on equal counts (= cheaper false-positive for our corpus).
        if mv_votes >= tv_votes:
            best, kind, name_field = mv_pick, "movie", "title"
        else:
            best, kind, name_field = tv_pick, "tv", "name"
    elif tv_pick:
        best, kind, name_field = tv_pick, "tv", "name"
    elif mv_pick:
        best, kind, name_field = mv_pick, "movie", "title"
    else:
        return None
    poster = best.get("poster_path")
    rating = best.get("vote_average") or 0.0
    # Don't trust ratings from low-vote-count entries (= 10/10 with 1
    # vote is meaningless). Threshold loosely at >=20 votes.
    if (best.get("vote_count") or 0) < 20:
        rating = 0
    # Larger poster (w342) for hero/film-tile use cases — looks crisp
    # on retina at 2-col grid widths. The list-tile (w185) version is
    # still served as `poster` for backwards-compat of the cached field.
    poster_url   = (f"https://image.tmdb.org/t/p/w185{poster}"
                    if poster else None)
    poster_large = (f"https://image.tmdb.org/t/p/w342{poster}"
                    if poster else None)
    return {
        "poster": poster_url,
        "tmdb_poster": poster_large,
        "rating": round(rating, 1) if rating > 0 else None,
        "tmdb_id": best.get("id"),
        "kind": kind,
    }


# Manual slug overrides for ambiguous titles where fernsehserien's
# default redirect lands on the wrong-era show. Add to this dict
# when a tile shows visibly outdated art (e.g. "NOTRUF" defaults
# to the 1992 Hans-Meiser show; the user's actual recording is
# the SAT.1 2024 reboot).
_FS_SLUG_ALIASES = {
    "notruf": "notruf-2024",
    # "lenssen-hilft" redirects to "cafe-puls" (= unrelated show, same
    # production company). The actual SAT.1 reboot lives at
    # "lenssen-hilft-2024".
    "lenssen-hilft": "lenssen-hilft-2024",
}

# Manual poster URL pins for one-off shows (= TV-events, single-episode
# documentaries) that don't exist in fernsehserien/TMDB/TVmaze. Key is
# the recording's full title (case-sensitive, exact match against tvh's
# disp_title). Value is the direct image URL.
_BIBLIOTHEK_POSTER_PINS = {
    "Die Mark Forster-Story": (
        "https://pictures.tvinfo.net/pictures/d6/80/a0/75/2c/61/87/1f"
        "/0f/fb/64/f7/03/b7/ee/43/large_vox_260505_2215_461c403f"
        "_die_mark_forster-story.jpg"),
}


def _fetch_fernsehserien(title):
    """Last-resort fallback for German daily shows TMDB+TVmaze miss
    (Lenßen, Wetzel, Hundeprofi, hundkatzemaus, Abenteuer Leben etc).
    Direct slug GET → parse og:image. Returns None when:
      - HTTP error / network fail
      - Page redirected to a different slug (= wrong-show fuzzy match,
        e.g. /lenssen-hilft → /cafe-puls)
      - og:image is the site-wide placeholder ("og-image-1200.png")"""
    slug = title.lower()
    slug = (slug.replace("ä", "ae").replace("ö", "oe")
                .replace("ü", "ue").replace("ß", "ss"))
    # Apostrophes (= "Let's Dance") DROP rather than become hyphens —
    # fernsehserien indexes "lets-dance" not "let-s-dance". Same for
    # other intra-word punctuation (dots in "Dr." → drop, not split).
    slug = slug.replace("'", "").replace("’", "").replace(".", "")
    slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-")
    if not slug:
        return None
    # Apply manual override (= ambiguous titles where fernsehserien
    # redirects to the wrong-era show, e.g. "notruf" → 1992 Hans-Meiser
    # show but user's recording is the SAT.1 2024 reboot).
    slug = _FS_SLUG_ALIASES.get(slug, slug)
    url = f"https://www.fernsehserien.de/{slug}"
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            html = r.read().decode("utf-8", errors="replace")
            final_url = r.geturl()
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"[epg-enrich fs {title}] {e}", flush=True)
        return None
    except Exception as e:
        print(f"[epg-enrich fs {title}] {e}", flush=True)
        return None
    # Redirect-detection: accept only when final URL's slug equals our
    # request slug OR is our slug + "-<suffix>" (= year/qualifier like
    # "notruf" → "notruf-1992", same show, formalized name). Reject
    # arbitrary fuzzy-matches ("lenssen-hilft" → "cafe-puls").
    final_slug = final_url.rstrip("/").rsplit("/", 1)[-1]
    if not (final_slug == slug or final_slug.startswith(slug + "-")):
        return None
    # Re-derive slug from final URL so the slug-match in the HTML
    # parser below uses the canonical form (= "notruf-1992" not "notruf").
    slug = final_slug
    # fernsehserien serves 4 image variants per show:
    #   gfx/bv/<slug>_<id>.jpg     1940×462 banner (= og:image)
    #   sendung/hr2/<slug>_<id>.png 600×320 (1.875:1 wide, our pick)
    #   sendung/hr/<slug>_<id>.png  300×160
    #   sendung/<slug>_<id>.png     150×80
    # Prefer the hr2 variant whose URL slug MATCHES our request slug
    # (= page contains hr2 references in the sidebar for "related
    # shows" too — first-match without slug-check returned wrong-show
    # posters for "taff" page → navy-cis_xxx.png). Falls back to og:image
    # then to any hr2 only if scoping fails.
    poster = None
    # 1) hr2 with slug match in URL — strongest signal. Image host
    # can be either bilder.fernsehserien.de OR bilder.wunschliste.de
    # (= same group, fernsehserien redirects some shows). Extension
    # can be .png OR .jpg.
    # Slug-match: hr2 filename slug must equal OR be prefix of the
    # URL slug. Many shows have URL slug "notruf-1992" but their
    # hr2 image is "notruf_164168.png" — same show, different naming
    # convention. Prefix-match catches these without admitting random
    # cross-show sidebar links.
    for m in re.finditer(
            r'(https://bilder\.(?:fernsehserien|wunschliste)\.de'
            r'/sendung/hr2/([^/"]+?)_\d+\.(?:png|jpg))',
            html):
        url, fname_slug = m.group(1), m.group(2)
        if fname_slug == slug or slug.startswith(fname_slug + "-"):
            poster = url
            break
    # 2) og:image (= 1940×462 banner, less ideal aspect but show-correct)
    if not poster:
        og = re.search(
            r'<meta\s+property="og:image"\s+content="([^"]+)"', html)
        if og:
            og_url = og.group(1)
            og_m = re.search(r'/gfx/bv/([^/"]+?)_\d+\.', og_url)
            if og_m and og_m.group(1) == slug:
                poster = og_url
            elif og_m and og_m.group(1).rstrip("0123456789-") == slug:
                poster = og_url
            elif "/gfx/bv/" + slug + "." in og_url:
                # bare-name og:image form ("taff.jpg" with no _id suffix)
                poster = og_url
    if not poster:
        return None
    # Site-wide placeholder filter
    if "og-image" in poster or "/fs-2021/img/" in poster:
        return None
    return {"poster": poster, "fernsehserien_slug": slug}


def _fetch_tvmaze(title):
    """Fallback enricher when TMDB isn't configured or misses. TVmaze
    skews US/UK so most DE reality/cooking shows miss."""
    try:
        url = ("https://api.tvmaze.com/singlesearch/shows?q="
               + urllib.parse.quote(title))
        with urllib.request.urlopen(url, timeout=8) as r:
            d = json.loads(r.read())
        img = d.get("image") or {}
        return {
            "poster": img.get("medium") or img.get("original"),
            "rating": (d.get("rating") or {}).get("average"),
            "tvmaze_id": d.get("id"),
        }
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"[epg-enrich tvmaze] {title}: {e}", flush=True)
        return None
    except Exception as e:
        print(f"[epg-enrich tvmaze] {title}: {e}", flush=True)
        return None


def _fetch_show_meta(title, full_title=None):
    """Order of preference: fernsehserien.de → TMDB → TVmaze. German
    source first because TMDB's English-skewed search picks homonym
    movies for German TV titles ("taff" 2011 film, "Let's Dance"
    unrelated movie, "Galileo Stories" wrong show, "Das perfekte
    Dinner" wrong movie). fernsehserien's strict slug-matcher rejects
    such confusion via redirect-detection; if it has a hit, it's the
    right show.

    title       — normalized search term (= "Charmed", "Lenßen hilft").
                  Used for TMDB/TVmaze where shorter is friendlier.
    full_title  — original disp_title (= "Charmed - Zauberhafte Hexen",
                  "Ulrich Wetzel - Das Strafgericht"). Preferred for
                  fernsehserien.de which slug-matches strictly. Falls
                  back to `title` when not given.

    Returns dict stamped with fetched_at so negative results cache too
    and don't retry hourly."""
    if full_title is None:
        full_title = title
    out = {"fetched_at": int(time.time())}
    # Manual override wins over all sources (= one-off TV-events that
    # don't exist on fernsehserien/TMDB/TVmaze).
    for key in (full_title, title):
        pinned = _BIBLIOTHEK_POSTER_PINS.get(key)
        if pinned:
            out["poster"] = pinned
            return out
    # EPG-fallback titles like "NOTRUF / oder SAT.1 Regional-Magazine"
    # are tvh's marker for "EPG had no proper event title" — content is
    # whatever happened to air at that timeslot, NOT the show literally
    # named "NOTRUF". Pattern: contains "/oder" or "/ oder" (= the
    # disambiguating slash is the smoking gun; bare " oder " also
    # appears in legit show names like "Hot oder Schrott").
    if "/oder" in full_title.lower() or "/ oder" in full_title.lower():
        return out
    # fernsehserien first — try multiple title variants in order of
    # specificity. Many German titles contain separators that don't
    # belong in fernsehserien's slug ("Ulrich Wetzel - Das Strafgericht"
    # → full slug works; "Charmed - Zauberhafte Hexen" → normalized
    # "Charmed" wins).
    candidates = []
    for c in (full_title, title):
        if c and c not in candidates:
            candidates.append(c)
    # Split on common separator patterns; take the head as a candidate.
    for sep in (" - ", " – ", ":"):
        for c in [full_title, title]:
            if not c or sep not in c: continue
            head = c.split(sep, 1)[0].strip()
            if head and len(head) >= 2 and head not in candidates:
                candidates.append(head)
    fs_poster = None
    for try_title in candidates:
        meta = _fetch_fernsehserien(try_title)
        if meta and meta.get("poster"):
            fs_poster = meta["poster"]
            out.update(meta)
            break
    # Always probe TMDB too — even when fernsehserien wins for the
    # series-tile poster. TMDB gives us `kind` (= movie/tv classification)
    # and `tmdb_poster` (= portrait artwork) which the bibliothek-tile
    # render uses for movies. Without this, films stuck in user-groups
    # (Rocky, Minions) inherit fernsehserien's small landscape banner
    # instead of TMDB's proper portrait poster.
    if TMDB_API_KEY:
        meta = _fetch_tmdb(title)
        if meta:
            for k, v in meta.items():
                # Don't let TMDB clobber fernsehserien's poster — that
                # was the whole point of preferring fernsehserien for
                # German series. tmdb_poster is the parallel field.
                if k == "poster" and fs_poster:
                    continue
                out[k] = v
            if fs_poster or meta.get("poster") or meta.get("rating") is not None:
                return out
    if not out.get("poster"):
        meta = _fetch_tvmaze(title)
        if meta:
            out.update(meta)
    return out


def _enrich_recordings_loop():
    """Hourly: scan recordings list, fetch TVmaze metadata for any
    new (or stale) title. Rate-limited to ~2 lookups/sec to be polite
    to TVmaze's free public API."""
    time.sleep(60)
    while True:
        try:
            data = json.loads(urllib.request.urlopen(
                f"{dvr_base()}/api/dvr/entry/grid?limit=500",
                timeout=10).read())
            now = time.time()
            seen = set()
            for e in data.get("entries", []):
                key = _normalize_title(e.get("disp_title", ""))
                if not key or key in seen:
                    continue
                seen.add(key)
                with _epg_meta_lock:
                    cached = _epg_meta.get(key)
                if cached and now - cached.get("fetched_at", 0) < EPG_META_TTL_S:
                    continue
                # Pass BOTH normalized key (for TMDB/TVmaze, which
                # do better with short titles like "Charmed") AND
                # the full disp_title (= "Ulrich Wetzel - Das
                # Strafgericht") for fernsehserien.de's strict
                # slug-matcher.
                full_title = (e.get("disp_title") or "").strip() or key
                meta = _fetch_show_meta(key, full_title=full_title)
                with _epg_meta_lock:
                    _epg_meta[key] = meta
                _save_epg_meta()
                time.sleep(0.5)  # gentle on upstream APIs
        except Exception as e:
            print(f"[epg-enrich] loop: {e}", flush=True)
        time.sleep(3600)


@app.route("/api/recording/<uuid>/watched", methods=["POST"])
def api_recording_watched(uuid):
    """Toggle the watched flag on a recording. tvheadend's `watched`
    field is read-only/computed — set the user-settable `playcount`
    instead. The auto-cleanup loop uses playcount>0 to decide what's
    eligible for deletion after WATCHED_AUTO_DELETE_DAYS days."""
    try:
        body = request.get_json(silent=True) or {}
        watched = bool(body.get("watched", True))
    except Exception:
        watched = True
    payload = urllib.parse.urlencode({
        "node": json.dumps({"uuid": uuid,
                              "playcount": 1 if watched else 0})
    }).encode()
    try:
        req = urllib.request.Request(f"{dvr_base()}/api/idnode/save",
                                      data=payload, method="POST")
        urllib.request.urlopen(req, timeout=5).read()
        return _cors(Response(json.dumps({"ok": True, "watched": watched}),
                               mimetype="application/json"))
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")


def _cleanup_watched_loop():
    """Delete recordings that have been marked watched (playcount>0)
    for longer than WATCHED_AUTO_DELETE_DAYS days. Runs every 6 h.
    Only touches `Completed` entries — never deletes something still
    being recorded or remuxed."""
    time.sleep(120)  # let other startup work settle
    while True:
        try:
            cutoff = time.time() - WATCHED_AUTO_DELETE_DAYS * 86400
            d = json.loads(urllib.request.urlopen(
                f"{dvr_base()}/api/dvr/entry/grid?limit=1000",
                timeout=20).read())
            deleted = 0
            for e in d.get("entries", []):
                if (e.get("playcount", 0) > 0
                        and "Completed" in (e.get("status") or "")
                        and (e.get("stop") or 0) < cutoff):
                    try:
                        body = urllib.parse.urlencode({"uuid": e["uuid"]}).encode()
                        urllib.request.urlopen(urllib.request.Request(
                            f"{dvr_base()}/api/dvr/entry/remove",
                            data=body, method="POST"),
                            timeout=10).read()
                        deleted += 1
                        print(f"[watched-cleanup] removed "
                              f"{e.get('disp_title','?')[:40]} "
                              f"(stopped {time.strftime('%Y-%m-%d', time.localtime(e.get('stop', 0)))})",
                              flush=True)
                    except Exception as ex:
                        print(f"[watched-cleanup] remove {e['uuid']}: {ex}",
                              flush=True)
            if deleted:
                print(f"[watched-cleanup] {deleted} recording(s) deleted",
                      flush=True)
        except Exception as ex:
            print(f"[watched-cleanup] loop: {ex}", flush=True)
        time.sleep(6 * 3600)


@app.route("/cancel-recording/<uuid>")
def cancel_recording(uuid):
    """Cancel a scheduled (or running) DVR entry, JSON response."""
    body = urllib.parse.urlencode({"uuid": uuid}).encode()
    last_err = None
    for ep in ("/api/dvr/entry/cancel", "/api/dvr/entry/remove"):
        try:
            req = urllib.request.Request(f"{dvr_base()}{ep}",
                                          data=body, method="POST")
            urllib.request.urlopen(req, timeout=5).read()
            return _cors(Response(json.dumps({"ok": True}),
                                   mimetype="application/json"))
        except Exception as e:
            last_err = str(e)
    return Response(json.dumps({"ok": False, "error": last_err}),
                    status=500, mimetype="application/json")


@app.route("/api/is-recording/<slug>")
def api_is_recording(slug):
    """Does this channel currently have an active or upcoming recording?"""
    with cmap_lock:
        info = channel_map.get(slug)
    if not info:
        return _cors(Response(json.dumps({"active": False}),
                               mimetype="application/json"))
    ch_uuid = info["uuid"]
    now_ts = int(time.time())
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid_upcoming?limit=200",
            timeout=6).read())
    except Exception:
        return _cors(Response(json.dumps({"active": False}),
                               mimetype="application/json"))
    active = False
    title = None
    stop = 0
    for e in data.get("entries", []):
        if e.get("channel") != ch_uuid:
            continue
        s = e.get("start", 0); st = e.get("stop", 0)
        if s <= now_ts < st:
            active = True
            title = e.get("disp_title", "")
            stop = st
            break
    return _cors(Response(json.dumps({"active": active, "title": title,
                                        "stop": stop}),
                           mimetype="application/json"))


@app.route("/recordings")
def recordings_page():
    """List of recordings (ongoing + finished) with links. Entries
    spawned by the same autorec rule are collapsed into a series
    group with an episode count."""
    try:
        # limit=600 covers ~131 completed + up to ~470 scheduled
        # entries (today's library has 387 total; series-autorec
        # rules can balloon scheduled fast — SpongeBob alone adds
        # 7 future days × 4 episodes = 28). 200 was too tight: with
        # sort=start DESC, future-scheduled entries crowded the 200
        # slots and completed ones got dropped from the page entirely.
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=600&sort=start&dir=DESC",
            timeout=10).read())
    except Exception as e:
        abort(502, f"tvheadend: {e}")

    # Filter out user-rejected entries so they don't clutter the
    # recordings list. The /recording/<uuid>/delete endpoint marks
    # scheduled entries as enabled=False (NOT removed) so the parent
    # autorec rule sees an existing entry for the broadcast and
    # skips re-creation. Running entries get /cancel'd (status
    # "Aborted by user"). Completed entries get fully purged.
    _DROP_SCHED = {"missed", "cancelled", "removed", "completedRebuild"}
    if data.get("entries"):
        data["entries"] = [
            e for e in data["entries"]
            if e.get("enabled", True)
            and e.get("sched_status", "") not in _DROP_SCHED
            and "Aborted by user" not in (e.get("status", "") or "")
        ]

    now_ts = int(time.time())
    # Bucket entries into series groups. Four passes (priority order):
    #  0) User-defined groups (= manual franchise grouping like Rocky
    #     1-5 or Asterix-films, where each entry has a unique title so
    #     autorec can't catch it). UUIDs in user groups are pre-claimed
    #     and skipped by passes 1-3.
    #  1) Real autorec entries → by_autorec[ar]
    #  2) Build a (title, channel) → ar_uuid lookup from pass 1 so
    #     orphans can rejoin their old group (= autorec rule deleted
    #     after some episodes already aired, OR rule replaced and the
    #     old episodes lost their back-reference).
    #  3) Orphans → join existing autorec group if (title, channel)
    #     matches; else cluster orphans-with-orphans into a synthetic
    #     "orphan:title|channel" group when ≥2 share a title; else
    #     solo. Singletons stay in the "" solo bucket so a one-off
    #     recording doesn't render as a 1-episode series.
    user_groups = _load_user_groups()
    uuid_to_user_group = {}
    for g_name, g_uuids in user_groups.items():
        for u in g_uuids:
            uuid_to_user_group[u] = g_name
    by_autorec = {}
    # Pre-create user group buckets in deterministic order so they
    # render at a stable position (alphabetical by name).
    for g_name in sorted(user_groups.keys()):
        by_autorec[f"usergroup:{g_name}"] = []
    title_ch_to_ar = {}
    for e in data.get("entries", []):
        u = e.get("uuid", "")
        # Pass 0 — user-group has top priority over autorec/orphan.
        if u in uuid_to_user_group:
            by_autorec[f"usergroup:{uuid_to_user_group[u]}"].append(e)
            continue
        ar = e.get("autorec") or ""
        if ar:
            by_autorec.setdefault(ar, []).append(e)
            title = (e.get("disp_title") or "").strip()
            ch = (e.get("channelname") or "").strip()
            if title:
                title_ch_to_ar.setdefault((title, ch), ar)
    # Drop any user-group bucket that ended up empty (= configured
    # UUIDs no longer exist on the grid; recording was deleted from
    # tvh).
    by_autorec = {k: v for k, v in by_autorec.items()
                  if not k.startswith("usergroup:") or v}
    orphan_buckets = {}
    for e in data.get("entries", []):
        u = e.get("uuid", "")
        if u in uuid_to_user_group:
            continue
        ar = e.get("autorec") or ""
        if ar:
            continue
        title = (e.get("disp_title") or "").strip()
        ch = (e.get("channelname") or "").strip()
        if not title:
            by_autorec.setdefault("", []).append(e)
            continue
        existing_ar = title_ch_to_ar.get((title, ch))
        if existing_ar:
            by_autorec[existing_ar].append(e)
        else:
            orphan_buckets.setdefault((title, ch), []).append(e)
    for (title, ch), eps in orphan_buckets.items():
        if len(eps) >= 2:
            by_autorec[f"orphan:{title}|{ch}"] = eps
        else:
            by_autorec.setdefault("", []).extend(eps)
    # The 3 next-to-start scheduled recordings get a "⏰ als nächstes"
    # badge so the user can see at a glance what's coming up next
    # without scanning through all the future entries (sorted
    # newest-first puts the FURTHEST-future stuff at the top, so the
    # imminent ones can sit anywhere depending on schedule density).
    upcoming = sorted(
        (e for e in data.get("entries", [])
         if e.get("sched_status") in ("scheduled", "recording")
         and (e.get("start") or 0) > now_ts),
        key=lambda e: e.get("start") or 0)[:3]
    next_up_uuids = {e["uuid"]: i for i, e in enumerate(upcoming)}
    # Tuner-conflict map: uuid → peak # unique muxes overlapping that
    # entry's window. Compared against TUNER_TOTAL to mark over-booked
    # rows in red — tvh would otherwise silently flip the lower-priority
    # one to "Time missed" at recording time.
    tuner_conflicts = _compute_tuner_conflicts(now_ts)

    # Track the first is_now-or-earlier row so JS can scroll to it on
    # load (newest-first sort puts FUTURE scheduled stuff at the top;
    # the user's actual focus is the first row that's either currently
    # recording or already done — that's "today's section").
    now_anchor_set = {"hit": False}
    # Per-status row counter — fills in during _render_row, used by
    # the filter UI to show "(N)" next to each checkbox label.
    status_counts = {"live": 0, "warming": 0, "playable": 0,
                     "pending": 0, "failed": 0, "scheduled": 0,
                     "unedited": 0}

    def _render_row(e, in_series=False, show_title_in_series=False):
        uuid = e.get("uuid", "")
        title = e.get("disp_title", "?")
        start = e.get("start", 0)
        stop  = e.get("stop", 0)
        status = e.get("status", "")
        sched = e.get("sched_status", "")
        size = e.get("filesize", 0) or 0
        is_live = start <= now_ts < stop and sched == "recording"
        is_done = now_ts >= stop or "Completed" in status
        when = time.strftime("%d.%m %H:%M", time.localtime(start))
        dur_min = max(0, (stop - start) // 60)
        play_url = f"{HOST_URL}/recording/{uuid}"
        ch_icon = e.get("channel_icon") or ""
        ch_name = (e.get("channelname") or "").replace('"', "&quot;")
        ch_slug = slugify(e.get("channelname") or "")
        ch_logo_src = _channel_logo_url(ch_slug, ch_icon)
        ch_logo_html = (
            f'<img class="ch-logo" src="{ch_logo_src}" '
            f'alt="{ch_name}" title="{ch_name}" loading="lazy">'
            if ch_logo_src else ''
        )
        if is_live or is_done:
            title_cell = f'<a href="{play_url}">{title}</a>'
        else:
            title_cell = f'<span>{title}</span>'
        if is_done:
            out_dir = HLS_DIR / f"_rec_{uuid}"
            playlist = out_dir / "index.m3u8"
            scanning_lock = out_dir / ".scanning"
            # When the Mac handler is doing work, _rec_hls_procs and
            # _rec_cskip_procs are empty. Infer Mac activity from the
            # cooperative .scanning lock-file on the SMB share — fresh
            # lock + no Pi-local proc means the Mac is mid-work on
            # this recording.
            try:
                mac_active = (scanning_lock.exists() and
                              time.time() - scanning_lock.stat().st_mtime < 900)
            except Exception:
                mac_active = False
            try:
                has_endlist = (playlist.exists() and
                               "#EXT-X-ENDLIST" in playlist.read_text(errors="ignore"))
            except Exception:
                has_endlist = False
            has_txt = any(f.stat().st_size > 0
                          for f in out_dir.glob("*.txt")
                          if f.is_file())

            proc_info = _rec_hls_procs.get(uuid)
            pi_remux_running = (proc_info is not None and
                                proc_info["proc"].poll() is None)
            cskip_info = _rec_cskip_procs.get(uuid)
            pi_cskip_running = (cskip_info and
                                cskip_info["proc"].poll() is None)
            mac_remuxing = (mac_active and not pi_remux_running
                            and not has_endlist)
            # Mac-detect-running is signalled directly by the daemon via
            # /api/internal/detect-started — the old .scanning lockfile
            # heuristic doesn't fire because tv-thumbs-daemon doesn't
            # write that file.
            mac_scanning = (_detect_is_running(uuid)
                            and not pi_cskip_running and not has_txt)

            # HLS-VOD takes precedence over tvh's sched_status: if the
            # playlist is on disk, the recording IS playable regardless
            # of what tvh thinks. Specifically handles the "Pi original
            # .ts dedup'd against T7 cache" case from 2026-05-11 — tvh
            # flips to completedError "File missing" when it notices
            # the .ts is gone, but HLS-VOD (= what the player actually
            # uses) is intact + served by Caddy from /mnt/tv/hls/.
            if playlist.exists() and not pi_remux_running and not mac_remuxing:
                # Ready to stream → play button.
                status_cell = (f'<a class="badge play-btn" href="{play_url}" '
                               f'title="Abspielen">▶ abspielen</a>')
                row_status = "playable"
            elif sched == "completedError":
                # tvh says recording failed AND we have no HLS-VOD to
                # fall back on — typically "Time missed" (tuner didn't
                # tune in time, EPG drift, signal glitch) or genuine
                # "File missing" without prior remux.
                tvh_msg = status or "fehlgeschlagen"
                status_cell = (f'<span class="badge failed" '
                               f'title="tvheadend: {tvh_msg}">'
                               f'⚠ Aufnahme fehlgeschlagen</span>')
                row_status = "failed"
            elif pi_remux_running:
                total = (proc_info or {}).get("total_segs", 0)
                segs = 0
                if playlist.exists():
                    try: segs = playlist.read_text().count(".ts")
                    except Exception: pass
                pct = (int(segs * 100 / total) if total > 0 else 0)
                status_cell = (f'<span class="badge warming" '
                               f'title="Remux läuft">⏳ {pct}%</span>')
                row_status = "warming"
            elif mac_remuxing:
                status_cell = ('<span class="badge warming" '
                               'title="Mac remuxt">⏳ Mac</span>')
                row_status = "warming"
            else:
                status_cell = ('<span class="badge pending" '
                               'title="noch nicht remuxt">◌ ausstehend</span>')
                row_status = "pending"
            if pi_cskip_running:
                status_cell += (' <span class="badge scanning" '
                                'title="comskip analysiert Werbeblöcke">🔍</span>')
            elif mac_scanning:
                status_cell += (' <span class="badge scanning" '
                                'title="Mac comskip analysiert Werbeblöcke">🔍</span>')
            user_p_path = out_dir / "ads_user.json"
            user_reviewed = False
            user_auto_confirmed = False
            user_auto_score = None
            if user_p_path.exists():
                try:
                    cur_user = json.loads(user_p_path.read_text())
                    user_reviewed = bool(cur_user.get("reviewed_at"))
                    user_auto_confirmed = bool(cur_user.get("auto_confirmed_at"))
                    user_auto_score = cur_user.get("auto_confirm_score")
                except Exception:
                    pass
                if user_auto_confirmed:
                    score_pct = (f" ({int(user_auto_score*100)}%)"
                                 if user_auto_score else "")
                    status_cell += (
                        f' <span class="badge auto-confirmed-applied" '
                        f'title="Automatisch bestätigt durch Multi-Signal '
                        f'Auto-Confirm{score_pct} — kein manueller Review '
                        f'nötig. Player → Undo wenn nicht passt.">'
                        f'✓ auto{score_pct}</span>')
                else:
                    status_cell += (' <span class="badge ads-edited" '
                                    'title="Werbeblöcke manuell angepasst">'
                                    '✏️</span>')
            # Detect-done indicator: cutlist .txt with the comskip
            # FILE PROCESSING COMPLETE marker means the daemon has
            # actually run and written its result. Combined with
            # !user_reviewed = "ready for your review". Surfaces a
            # cheap "go look at this" badge so the user doesn't open
            # recordings whose detect is still pending (= empty
            # marker auto-redirects with no blocks shown).
            if not user_reviewed and not pi_cskip_running and not mac_scanning:
                detect_done = False
                try:
                    for t in out_dir.glob("*.txt"):
                        if any(t.name.endswith(s) for s in
                               (".logo.txt", ".cskp.txt", ".tvd.txt",
                                ".trained.logo.txt")):
                            continue
                        if t.stat().st_size > 50:
                            head = t.read_text(errors="ignore")[:200]
                            if "FILE PROCESSING COMPLETE" in head:
                                detect_done = True
                                break
                except Exception:
                    pass
                if detect_done:
                    status_cell += (
                        f' <a class="badge ads-ready" '
                        f'href="{HOST_URL}/recording/{uuid}" '
                        f'title="Detect ist durch — bitte Werbeblöcke '
                        f'prüfen + ✓ Geprüft klicken (jede Review '
                        f'verbessert das Modell)">'
                        f'📋 prüfbar</a>')
            unc_n = _uncertain_count(uuid)
            if unc_n > 0:
                status_cell += (
                    f' <a class="badge ads-uncertain" '
                    f'href="{HOST_URL}/recording/{uuid}" '
                    f'title="{unc_n} hochwertige Label-Targets — '
                    f'NN-Modell ist hier unsicher, manuelle Prüfung '
                    f'verbessert das Training am meisten">'
                    f'🎯 {unc_n}</a>')
        elif is_live:
            status_cell = (f'<a class="badge live" href="{play_url}" '
                           f'title="Live ansehen">● live</a>')
            row_status = "live"
        else:
            status_cell = ('<span class="badge scheduled" '
                           'title="Sendung wird zur geplanten Zeit aufgenommen">⏱ geplant</span>')
            row_status = "scheduled"
        # Mark the next 3 imminent scheduled recordings with a "next-up"
        # badge + countdown. JS at the bottom of the page refreshes the
        # countdown text every minute so the page stays informative
        # between auto-reloads.
        # Adaptive-padding badge: shown when this recording was
        # auto-extended because an ad block was still running at its
        # scheduled stop time. Applies to both live (still recording)
        # and completed entries.
        with _adaptive_padding_lock:
            ap_st = _adaptive_padding_state.get(uuid)
        if ap_st and ap_st.get("count", 0) > 0:
            ap_min = ap_st["count"] * ADAPTIVE_PADDING_STEP_MIN
            status_cell += (f' <span class="badge auto-extended" '
                            f'title="Aufnahme wurde automatisch um '
                            f'{ap_min} Min verlängert weil bei geplantem '
                            f'Ende noch Werbung lief">'
                            f'+{ap_min}m auto</span>')
        # Tuner-conflict warning (rendered alongside the status badge).
        # peak = how many unique muxes overlap THIS entry's window.
        # If > TUNER_TOTAL the recording WILL fail at start time.
        peak = tuner_conflicts.get(uuid, 0)
        if peak > TUNER_TOTAL:
            status_cell += (f' <span class="badge tuner-overbook" '
                            f'title="{peak} überlappende Mux(e), nur '
                            f'{TUNER_TOTAL} Tuner — niedrigere Priorität '
                            f'wird tvh-seitig auf Time missed gesetzt">'
                            f'⚠ {peak}/{TUNER_TOTAL} Tuner</span>')
        if uuid in next_up_uuids:
            secs = max(0, start - now_ts)
            mins = secs // 60
            if secs < 60:
                rel = "<1 min"
            elif mins < 60:
                rel = f"{mins} min"
            else:
                rel = f"{mins // 60}h {mins % 60:02d}m"
            status_cell += (f' <span class="badge next-up" '
                            f'data-start="{start}" '
                            f'title="Startet als nächstes">'
                            f'⏰ in {rel}</span>')
        # Watched-button removed 2026-05-05 — paired with the
        # auto-mark + cleanup-loop disable. Manual lifetime control
        # via the trash icon is the user's preferred workflow.
        watch_cell = ''
        # TVmaze enrichment: poster + rating attached to the series-
        # head row (rendered separately below). Skip on individual
        # episode rows inside a series — duplicating the same poster
        # 5× per series turns the table into visual mush. Solo
        # recordings (no autorec) keep them inline.
        if not in_series:
            with _epg_meta_lock:
                meta = _epg_meta.get(_normalize_title(title)) or {}
            poster_html = (f'<img class="rec-poster" src="{meta["poster"]}" '
                           f'alt="" loading="lazy">'
                           if meta.get("poster") else '')
            rating_html = (f' <span class="rec-rating" title="TVmaze rating">'
                           f'★ {meta["rating"]}</span>'
                           if meta.get("rating") else '')
            title_cell = ch_logo_html + poster_html + title_cell + rating_html
        del_btn = (f'<a class="del-btn" href="{HOST_URL}/recording/{uuid}/delete" '
                   f'data-title="{title.replace(chr(34),"&quot;")}" '
                   f'data-when="{when}">🗑</a>')
        tools_cell = f'<td class="row-tools">{watch_cell}{del_btn}</td>'
        # First is_done/is_live row across the whole list is the
        # "now anchor" — JS scrolls here on initial load.
        anchor_attr = ""
        if (is_done or is_live) and not now_anchor_set["hit"]:
            anchor_attr = ' id="now-anchor"'
            now_anchor_set["hit"] = True
        # "Edited" only makes sense for playable rows — scheduled
        # recordings haven't aired yet, live ones are mid-air. So we
        # only tag is_edited on playable rows; the "nur unbearbeitete"
        # filter then targets the actual review backlog (= ready to
        # play but not yet user-confirmed).
        edited_attr = ''
        if row_status == "playable":
            is_edited = (HLS_DIR / f"_rec_{uuid}" / "ads_user.json").exists()
            edited_attr = ' data-edited="1"' if is_edited else ' data-edited="0"'
            if not is_edited:
                status_counts["unedited"] += 1
        status_counts[row_status] = status_counts.get(row_status, 0) + 1
        if in_series:
            # In an autorec series all episodes share the same title
            # → showing it per-row is redundant. User-groups (Rocky/
            # Asterix) bundle DIFFERENT titles → show the title so
            # the user can tell Rocky 1 from Rocky 2.
            title_html = (f'<td>{title_cell}</td>'
                          if show_title_in_series else '')
            return (f'<tr{anchor_attr} data-status="{row_status}"'
                    f' data-uuid="{uuid}"{edited_attr}>'
                    f'<td>{status_cell}</td>'
                    f'{title_html}'
                    f'<td>{when}</td>'
                    f'<td>{dur_min} min</td>'
                    f'{tools_cell}</tr>')
        # Edit-mode checkbox prepended to status cell. Hidden via CSS
        # outside of edit-mode. Only top-level (solo) rows are
        # selectable — series-children and series headers stay non-
        # selectable (= can't pull an episode out of an autorec series
        # into a manual group; dissolve-group is the way back).
        sel_box = (f'<input type="checkbox" class="rec-select" '
                   f'data-uuid="{uuid}" data-title="{title}" '
                   f'aria-label="Auswählen">')
        # Sort attrs on top-level rows — JS reorders by these on
        # dropdown change. Lowercased title/channel for stable
        # locale-insensitive compare.
        sort_attrs = (f' data-sort-start="{start}"'
                      f' data-sort-title="{title.lower().replace(chr(34),"&quot;")}"'
                      f' data-sort-channel="{ch_name.lower()}"')
        return (f'<tr{anchor_attr} data-status="{row_status}"'
                f' data-uuid="{uuid}"{edited_attr}'
                f'{sort_attrs}>'
                f'<td>{sel_box}{status_cell}</td>'
                f'<td>{title_cell}</td>'
                f'<td>{when}</td>'
                f'<td>{dur_min} min</td>'
                f'{tools_cell}</tr>')

    rows = []
    # Solo recordings (no autorec) — render flat.
    for e in by_autorec.get("", []):
        rows.append(_render_row(e))
    # Series groups — render as <details> with a summary row and the
    # episodes inside. Sorted by the most recent episode in the group.
    series_groups = [(ar, eps) for ar, eps in by_autorec.items() if ar]
    series_groups.sort(
        key=lambda kv: max((e.get("start", 0) for e in kv[1]), default=0),
        reverse=True)
    for ar_uuid, eps in series_groups:
        # User-group: synthetic key carries the user-chosen name.
        # Display that as the group title rather than the first
        # episode's disp_title (which would be one specific film).
        if ar_uuid.startswith("usergroup:"):
            group_title = ar_uuid[len("usergroup:"):]
        else:
            group_title = eps[0].get("disp_title", "?")
        live = sum(1 for e in eps
                   if e.get("start", 0) <= now_ts < e.get("stop", 0)
                   and e.get("sched_status") == "recording")
        upcoming = sum(1 for e in eps
                       if e.get("start", 0) > now_ts and "Completed" not in e.get("status", ""))
        done = sum(1 for e in eps if now_ts >= e.get("stop", 0) or "Completed" in e.get("status", ""))
        parts = [f"{done} aufgen."]
        if live:
            parts.append(f'<span class="live-count">● {live} läuft</span>')
        parts.append(f"{upcoming} geplant")
        badge = (f'<span class="badge series">📺 Serie · '
                 f'{" · ".join(parts)}</span>')
        # Per-group-type buttons:
        #   real autorec → "🗑" cancels the autorec rule (= future
        #     scheduled entries gone, completed kept)
        #   orphan       → no button (rule already gone, nothing
        #     to cancel)
        #   usergroup    → "🗑" dissolves the manual grouping (= just
        #     removes the user-group entry, recordings stay)
        if ar_uuid.startswith("orphan:"):
            kill_btn = ""
        elif ar_uuid.startswith("usergroup:"):
            g = ar_uuid[len("usergroup:"):].replace("'", "\\'")
            kill_btn = (f'<button class="series-kill" '
                        f'onclick="dissolveUserGroup(event,\'{g}\')" '
                        f'title="Manuelle Gruppe auflösen (Aufnahmen bleiben)">'
                        f'🗑</button>')
        else:
            kill_btn = (f'<button class="series-kill" '
                        f'onclick="cancelSeries(event,\'{ar_uuid}\',\'{group_title}\',{upcoming})" '
                        f'title="Serien-Aufzeichnungs-Regel löschen">'
                        f'🗑</button>')
        # Keep group expanded if any episode needs attention (recording
        # right now, being remuxed, or comskip is running on it).
        any_active = bool(live) or any(
            e.get("uuid") in _rec_hls_procs or e.get("uuid") in _rec_cskip_procs
            for e in eps)
        open_attr = " open" if any_active else ""
        with _epg_meta_lock:
            meta = _epg_meta.get(_normalize_title(group_title)) or {}
        poster_html = (f'<img class="rec-poster" src="{meta["poster"]}" '
                       f'alt="" loading="lazy">'
                       if meta.get("poster") else '')
        rating_html = (f' <span class="rec-rating" title="TVmaze rating">'
                       f'★ {meta["rating"]}</span>'
                       if meta.get("rating") else '')
        # Channel logo from the first episode (autorec is channel-locked
        # so all episodes share it).
        first_ep = eps[0] if eps else {}
        ep_ch_icon = first_ep.get("channel_icon") or ""
        ep_ch_name = (first_ep.get("channelname") or "").replace('"', "&quot;")
        ep_ch_slug = slugify(first_ep.get("channelname") or "")
        ep_ch_logo_src = _channel_logo_url(ep_ch_slug, ep_ch_icon)
        ch_logo_html = (
            f'<img class="ch-logo" src="{ep_ch_logo_src}" '
            f'alt="{ep_ch_name}" title="{ep_ch_name}" loading="lazy">'
            if ep_ch_logo_src else ''
        )
        # Next-up hint shown collapsed in the summary so the user
        # doesn't have to expand a series to see when the next
        # episode airs. Picks the earliest still-future episode.
        next_ep = min(
            (e for e in eps if (e.get("start") or 0) > now_ts),
            key=lambda e: e["start"], default=None)
        next_html = ""
        if next_ep:
            t = next_ep["start"]
            sd = time.strftime("%d.%m %H:%M", time.localtime(t))
            mins = (t - now_ts) // 60
            if mins < 60:
                rel = f"in {mins} min"
            elif mins < 24*60:
                rel = f"in {mins // 60}h {mins % 60:02d}m"
            else:
                rel = f"in {mins // (24*60)}T"
            next_html = (f' <small class="series-next" '
                         f'title="nächste Folge: {sd}">'
                         f'· nächste {sd} ({rel})</small>')
        # Sort attrs on the series-head row. Two values:
        #   data-sort-start      = newest completed-or-recording episode
        #                          (= "what content do I have to watch")
        #   data-sort-start-all  = "next activity" — soonest UPCOMING
        #                          (= what the badge shows as "nächste")
        #                          if any, else newest completed.
        # Using MIN of upcoming (not max) matches what users see in the
        # "nächste 04.05" badge. Earlier max-of-all version put a series
        # with planned 11.05 above one with planned 04.05 — confusing
        # because the visible badge said the opposite.
        completed = [e.get("start", 0) for e in eps
                     if (e.get("sched_status") in ("completed", "recording")
                         or now_ts >= e.get("stop", 0))]
        upcoming = [e.get("start", 0) for e in eps
                    if e.get("start", 0) > now_ts]
        max_start = (max(completed) if completed
                     else max((e.get("start", 0) for e in eps), default=0))
        max_start_all = (min(upcoming) if upcoming
                         else max_start)
        first_ch = (eps[0].get("channelname") or "").lower()
        head_sort = (f' data-sort-start="{max_start}"'
                     f' data-sort-start-all="{max_start_all}"'
                     f' data-sort-title="{group_title.lower().replace(chr(34),"&quot;")}"'
                     f' data-sort-channel="{first_ch}"')
        summary = (f'<tr class="series-head"{head_sort}><td colspan="5">'
                   f'<details data-ar="{ar_uuid}"{open_attr}>'
                   f'<summary>{ch_logo_html}{poster_html}{badge}'
                   f'<span class="series-title">{group_title}'
                   f' <small>({len(eps)})</small>{next_html}</span>'
                   f'{rating_html}{kill_btn}</summary>'
                   f'<table class="series-sub"><tbody>'
                   + "".join(_render_row(
                       e, in_series=True,
                       show_title_in_series=ar_uuid.startswith("usergroup:"))
                     for e in sorted(eps, key=lambda x: x.get("start", 0)))
                   + '</tbody></table></details></td></tr>')
        rows.append(summary)

    # Virtual Mediathek recordings — stream via hls.js on play. Sorted
    # newest-first, with an expiry reminder.
    with _mediathek_rec_lock:
        mt_entries = [(k, v) for k, v in _mediathek_rec.items()]
    mt_entries.sort(key=lambda kv: kv[1].get("created_at", 0), reverse=True)
    from datetime import datetime as _dt
    for vuuid, m in mt_entries:
        mt_title = m.get("title", "?").replace("<", "&lt;")
        when = time.strftime("%d.%m %H:%M",
                              time.localtime(m.get("start", 0)))
        dur_min = max(0, (m.get("stop", 0) - m.get("start", 0)) // 60)
        avail_to = m.get("available_to", 0)
        avail_str = ""
        if avail_to:
            try:
                avail_str = _dt.fromtimestamp(avail_to).strftime("%d.%m.%Y")
            except Exception:
                pass
        expired = avail_to and now_ts > avail_to
        # Ripped MP4 takes over once V3's background worker finishes —
        # playable forever after that, no remote HLS dependency.
        ripped_path = m.get("ripped_path", "")
        has_local = (ripped_path and
                     (HLS_DIR / f"_{vuuid}" / "file.mp4").exists())
        if has_local:
            mt_badge = '<span class="badge mediathek local">💾 Mediathek lokal</span>'
        elif expired:
            mt_badge = '<span class="badge expired">✗ abgelaufen</span>'
        else:
            mt_badge = '<span class="badge mediathek">📡 Mediathek</span>'
        if has_local:
            size_mb = (m.get("ripped_bytes", 0) or 0) / (1024 * 1024)
            avail_cell = f'<small>lokal ({size_mb:.0f} MB)</small>'
        elif avail_str and not expired:
            avail_cell = (f'<small style="color:var(--muted)">bis '
                          f'{avail_str}</small>')
        elif expired:
            avail_cell = '<small>abgelaufen</small>'
        else:
            avail_cell = ''
        if expired and not has_local:
            title_cell = f'<span>{mt_title}</span>'
        else:
            title_cell = f'<a href="{HOST_URL}/recording/{vuuid}">{mt_title}</a>'
        if avail_cell:
            title_cell += f'<br>{avail_cell}'
        rows.append(
            f'<tr><td>{mt_badge}</td>'
            f'<td>{title_cell}</td>'
            f'<td>{when}</td>'
            f'<td>{dur_min} min</td>'
            f'<td class="row-tools">'
            f'<a class="del-btn" href="{HOST_URL}/mediathek-rec/{vuuid}/delete" '
            f'data-title="{mt_title.replace(chr(34),"&quot;")}" '
            f'data-mt="1">🗑</a></td></tr>')

    body = (f"<html><head><meta name='viewport' "
            f"content='width=device-width,initial-scale=1'>"
            f"<meta name='color-scheme' content='light dark'>"
            f"<style>{BASE_CSS}"
            f"body{{max-width:none;margin:0;padding:0}}"
            f".rec-header{{display:flex;align-items:center;gap:8px 14px;"
            f"padding:6px 10px;border-bottom:1px solid var(--border);"
            f"background:var(--bg);font-size:.9em}}"
            f".rec-header h1{{margin:0;font-size:1.1em;font-weight:600}}"
            # Bigger tap target on touch — same pattern as EPG header.
            f".home-link{{display:inline-flex;align-items:center;gap:8px;"
            f"padding:8px 12px;margin:-6px -8px;border-radius:6px;"
            f"text-decoration:none;color:inherit;-webkit-tap-highlight-color:transparent}}"
            f".home-link .arrow{{font-size:1.4em;line-height:1}}"
            f"@media (hover:hover){{.home-link:hover{{background:var(--stripe)}}}}"
            f".rec-body{{padding:0 10px}}"
            f".badge{{font-size:.75em;padding:2px 6px;border-radius:3px;"
            f"font-weight:600;display:inline-block}}"
            f".badge.live{{background:#e74c3c;color:#fff}}"
            f".badge.done{{background:#27ae60;color:#fff}}"
            f".badge.scheduled{{background:var(--stripe);color:var(--muted)}}"
            f".badge.ready{{background:#2980b9;color:#fff}}"
            f".badge.warming{{background:#f39c12;color:#fff}}"
            f".badge.pending{{background:var(--stripe);color:var(--muted)}}"
            f".badge.failed{{background:#7f1d1d;color:#fecaca}}"
            f".badge.next-up{{background:#1e3a8a;color:#dbeafe}}"
            f"tr:has(.badge.next-up){{outline:1px solid #2563eb;"
            f"outline-offset:-1px}}"
            f".badge.tuner-overbook{{background:#92400e;color:#fed7aa;"
            f"animation:livepulse 2s ease-in-out infinite}}"
            f"tr:has(.badge.tuner-overbook){{outline:1px solid #f59e0b;"
            f"outline-offset:-1px}}"
            f".badge.scanning{{background:#8e44ad;color:#fff;"
            f"animation:scanpulse 1.2s ease-in-out infinite}}"
            f".badge.ads-edited{{background:#16a085;color:#fff}}"
            f".badge.ads-uncertain{{background:#d35400;color:#fff;"
            f"text-decoration:none}}"
            f".badge.auto-extended{{background:#0f766e;color:#ccfbf1}}"
            f".badge.auto-confirm-ok{{background:#16a34a;color:#fff}}"
            f".badge.auto-confirm-review{{background:#ca8a04;color:#fff}}"
            f".badge.auto-confirmed-applied{{background:#16a34a;color:#fff}}"
            f".badge.dupe{{background:#7c3aed;color:#fff;text-decoration:none}}"
            f"@keyframes scanpulse{{0%,100%{{opacity:.5}}50%{{opacity:1}}}}"
            f".badge.live{{animation:livepulse 1.5s ease-in-out infinite}}"
            f"@keyframes livepulse{{0%,100%{{opacity:1}}50%{{opacity:.6}}}}"
            f".live-count{{color:#ffeb3b;"
            f"animation:livepulse 1.5s ease-in-out infinite}}"
            f".del-modal{{position:fixed;inset:0;background:#000a;"
            f"display:flex;align-items:center;justify-content:center;z-index:100}}"
            f".del-dialog{{background:var(--bg);color:var(--fg);"
            f"padding:20px 22px;border-radius:12px;max-width:420px;"
            f"width:calc(100% - 40px);border:1px solid var(--border);"
            f"box-shadow:0 6px 20px #0008}}"
            f".del-head{{font-weight:700;font-size:1.05em;margin-bottom:10px}}"
            f".del-title{{font-weight:600;margin-bottom:6px;word-break:break-word}}"
            f".del-sub{{font-size:.85em;color:var(--muted);margin-bottom:16px}}"
            f".del-btns{{display:flex;gap:10px;justify-content:flex-end}}"
            f".del-btns button{{padding:8px 14px;border-radius:6px;"
            f"border:1px solid var(--border);font-weight:600;cursor:pointer;"
            f"font-size:.95em;background:var(--stripe);color:var(--fg)}}"
            f".del-confirm{{background:#c0392b !important;color:#fff !important;"
            f"border-color:#c0392b !important}}"
            f".del-cancel{{background:transparent !important;font-weight:400 !important}}"
            f".watch-btn{{display:inline-block;width:24px;height:24px;line-height:22px;"
            f"text-align:center;border-radius:50%;border:1px solid var(--border);"
            f"color:var(--muted);text-decoration:none;font-size:.95em;cursor:pointer}}"
            f".watch-btn.on{{background:#27ae60;color:#fff;border-color:#27ae60}}"
            f".rec-poster{{width:24px;height:34px;object-fit:cover;border-radius:3px;"
            f"vertical-align:middle;margin-right:6px;background:var(--stripe);"
            f"flex-shrink:0}}"
            f".ch-logo{{width:44px;height:32px;object-fit:contain;border-radius:3px;"
            f"vertical-align:middle;margin-right:8px;flex-shrink:0;"
            f"background:#fff;padding:2px;box-sizing:border-box}}"
            f".rec-rating{{display:inline-block;background:#f39c12;color:#000;"
            f"padding:1px 6px;border-radius:3px;font-size:.8em;font-weight:600;"
            f"margin-left:6px;vertical-align:middle;flex-shrink:0}}"
            f".row-tools{{text-align:right;white-space:nowrap;width:1%}}"
            f".row-tools .watch-btn{{margin-right:6px}}"
            f".badge.series{{background:#8e44ad;color:#fff;flex-shrink:0}}"
            f"a.badge.play-btn{{background:#2980b9;color:#fff;text-decoration:none;"
            f"font-weight:600;cursor:pointer}}"
            f"a.badge.live{{background:#e74c3c;color:#fff;text-decoration:none;"
            f"font-weight:600;cursor:pointer}}"
            f".badge.mediathek{{background:#2980b9;color:#fff}}"
            f".badge.mediathek.local{{background:#27ae60}}"
            f".badge.expired{{background:#7f8c8d;color:#fff}}"
            f"tr.series-head td{{padding:0}}"
            f"tr.series-head summary{{cursor:pointer;padding:6px 8px;"
            f"background:var(--stripe);display:flex;align-items:center;"
            f"gap:8px}}"
            f"tr.series-head summary .series-title{{flex:1;min-width:0;"
            f"overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}"
            f"tr.series-head .series-kill{{margin-left:auto;background:none;"
            f"border:0;color:var(--muted);cursor:pointer;font-size:.9em;"
            f"padding:2px 0 2px 6px}}"
            f"tr.series-head .series-kill:hover{{color:#e74c3c}}"
            f"table.series-sub{{width:100%;border-collapse:collapse}}"
            f"table.series-sub td{{padding:4px 8px}}"
            f"<style>"
            f".rec-filter{{display:flex;flex-wrap:wrap;gap:6px;margin:8px 12px;"
            f"font-size:.85em}}"
            f".rec-filter label{{display:inline-flex;align-items:center;"
            f"gap:4px;padding:3px 8px;border:1px solid var(--border);"
            f"border-radius:12px;cursor:pointer;background:var(--stripe);"
            f"user-select:none}}"
            f".rec-filter label:has(input:checked){{background:var(--code-bg);"
            f"border-color:var(--link);color:var(--link)}}"
            f".rec-filter input{{margin:0}}"
            f".rec-filter .cnt{{color:var(--muted);font-size:.85em}}"
            f".series-toggle{{padding:3px 10px;border:1px solid var(--border);"
            f"border-radius:12px;background:var(--stripe);color:var(--fg);"
            f"font-size:.85em;cursor:pointer;font-family:inherit}}"
            f".series-toggle:hover{{background:var(--code-bg);"
            f"border-color:var(--link);color:var(--link)}}"
            f"tr.row-hidden{{display:none}}"
            # Edit-mode UI — checkboxes hidden by default, made
            # visible by JS toggling body.edit-mode. Floating toolbar
            # sits below the filter row when active.
            f".rec-select{{display:none;margin-right:6px;"
            f"vertical-align:middle;width:16px;height:16px;cursor:pointer}}"
            f"body.edit-mode .rec-select{{display:inline-block}}"
            f"body.edit-mode .series-kill,body.edit-mode .row-tools{{"
            f"opacity:.4;pointer-events:none}}"
            f".edit-toolbar{{display:flex;gap:10px;align-items:center;"
            f"padding:8px 16px;background:var(--code-bg);"
            f"border-bottom:1px solid var(--border);"
            f"position:sticky;top:0;z-index:10}}"
            f".edit-toolbar .sel-count{{color:var(--muted);font-size:.9em}}"
            f".edit-toolbar button{{padding:5px 12px;"
            f"border:1px solid var(--border);border-radius:6px;"
            f"background:var(--stripe);color:var(--fg);cursor:pointer;"
            f"font-family:inherit;font-size:.9em}}"
            f".edit-toolbar button:hover:not(:disabled){{"
            f"background:var(--code-bg);border-color:var(--link);"
            f"color:var(--link)}}"
            f".edit-toolbar button:disabled{{opacity:.4;cursor:default}}"
            f".edit-toolbar #create-group-btn{{font-weight:600}}"
            f"</style></head><body>"
            f"<div class='rec-header'>"
            f"<a class='home-link' href='{HOST_URL}/'>"
            f"<span class='arrow'>←</span><h1>Aufnahmen</h1></a>"
            f"</div>"
            # Filter labels — only render statuses with ≥1 row, otherwise
            # the "fehlgeschlagen (0)" pill is just visual noise.
            f"<div class='rec-filter' id='rec-filter'>"
            f"{_render_status_filter(status_counts)}"
            f"<button id='series-expand' class='series-toggle' "
            f"style='margin-left:14px' title='Alle Serien aufklappen'>"
            f"➕ alle</button>"
            f"<button id='series-collapse' class='series-toggle' "
            f"title='Alle Serien zuklappen'>➖ alle</button>"
            f"<button id='edit-mode-toggle' class='series-toggle' "
            f"style='margin-left:14px' "
            f"title='Mehrere Aufnahmen für eine manuelle Gruppe auswählen'>"
            f"✏️ Bearbeiten</button>"
            # Sort dropdown — reorders top-level rows (= solo + series
            # heads) by the selected key. Choice persists per browser
            # via localStorage. Sort happens client-side: cheap with
            # ~600 rows, no server round-trip.
            f"<select id='sort-select' class='series-toggle' "
            f"style='margin-left:14px;padding:3px 6px' "
            f"title='Sortierung'>"
            f"<option value='start_desc'>Neueste zuerst</option>"
            f"<option value='start_asc'>Älteste zuerst</option>"
            f"<option value='channel'>Sender (A-Z)</option>"
            f"<option value='title'>Sendung (A-Z)</option>"
            f"</select>"
            f"</div>"
            # Edit-mode toolbar — hidden by default, JS toggles
            # display + manages selection state.
            f"<div id='edit-toolbar' class='edit-toolbar' "
            f"style='display:none'>"
            f"<span class='sel-count'>0 ausgewählt</span>"
            f"<button id='create-group-btn' disabled>"
            f"📺 Neue Gruppe …</button>"
            f"<button id='edit-mode-cancel'>Abbrechen</button>"
            f"</div>"
            f"{_learning_health_banner()}"
            f"<div class='rec-body'>"
            f"<table>"
            f"{''.join(rows) if rows else '<tr><td colspan=5>Keine Aufnahmen</td></tr>'}"
            f"</table>"
            f"</div>"
            f"<script>"
            # 15-s auto-reload while there's active live/scanning/warming
            # content. Pauses (re-arms in 15 s) if edit-mode is on so a
            # multi-select for a manual user-group doesn't get wiped
            # mid-pick.
            f"function _maybeReload(){{"
            f"  if(document.body.classList.contains('edit-mode')){{"
            f"    setTimeout(_maybeReload,15000);return;"
            f"  }}"
            f"  location.reload();"
            f"}}"
            f"if(document.querySelector('.badge.scanning,.badge.warming,.badge.live'))"
            f"  setTimeout(_maybeReload,15000);"
            # Live countdown for the .next-up badges — refreshed every
            # minute so users see "in 4 min" → "in 3 min" without
            # waiting for the full page reload (15s loop only fires
            # when there's active live/scanning/warming content).
            f"(function(){{"
            f"  function refresh(){{"
            f"    const now=Date.now()/1000;"
            f"    document.querySelectorAll('.badge.next-up[data-start]').forEach(b=>{{"
            f"      const secs=Math.max(0,parseInt(b.dataset.start)-now);"
            f"      const m=Math.floor(secs/60);"
            f"      b.textContent=secs<60?'⏰ in <1 min':"
            f"        m<60?'⏰ in '+m+' min':"
            f"        '⏰ in '+Math.floor(m/60)+'h '+("
            f"        String(m%60).padStart(2,'0'))+'m';"
            f"    }});"
            f"  }}"
            f"  setInterval(refresh,60000);refresh();"
            f"}})();"
            # Auto-confirm badge — fetches multi-signal verdict per
            # recording (whisper + cluster-anchored + block structure)
            # and adds a colored badge: green = auto_confirm (no
            # review needed), amber = needs_review (manual check), red
            # = anomaly (possible missed ad). Async + bulk, runs once
            # per page load.
            f"(async function(){{"
            f"  try{{"
            f"    const r=await fetch("
            f"      '{HOST_URL}/api/internal/auto-confirm-bulk');"
            f"    if(!r.ok)return;"
            f"    const d=await r.json();"
            f"    const verdicts=d.verdicts||{{}};"
            f"    document.querySelectorAll('tr[data-uuid]').forEach(tr=>{{"
            f"      const uuid=tr.dataset.uuid;if(!uuid)return;"
            f"      const v=verdicts[uuid];if(!v||!v.verdict)return;"
            # Skip rows already manually reviewed (data-edited='1') —
            # ✏️ already conveys "user-confirmed", a parallel green ✓
            # auto badge only adds noise + suggests it was auto-applied.
            f"      if(tr.dataset.edited==='1')return;"
            f"      const td=tr.querySelector('td:first-child');"
            f"      if(!td)return;"
            f"      let cls,txt,title;"
            f"      if(v.verdict==='auto_confirm'){{"
            f"        cls='auto-confirm-ok';"
            f"        txt='✓ auto';"
            f"        title='Multi-Signal Auto-Confirm: '+("
            f"          v.confidence!=null?(Math.round(v.confidence*100)+'%'):'')+"
            f"          ' confidence — '+v.n_high_conf+'/'+v.n_blocks+' Blocks high-conf, '+"
            f"          v.n_anomalies+' Anomalien';"
            f"      }}else if(v.verdict==='needs_review'){{"
            f"        cls='auto-confirm-review';"
            f"        const c=v.confidence!=null?Math.round(v.confidence*100):0;"
            f"        txt='? '+c+'%';"
            f"        title='Auto-Confirm zu schwach ('+c+'%): '+"
            f"          (v.reason||(v.n_high_conf+'/'+v.n_blocks+' Blocks high-conf, '+"
            f"           v.n_anomalies+' Anomalien'))+' — Review empfohlen';"
            f"      }}else return;"
            f"      const b=document.createElement('span');"
            f"      b.className='badge '+cls;"
            f"      b.textContent=txt;b.title=title;"
            f"      td.appendChild(document.createTextNode(' '));"
            f"      td.appendChild(b);"
            f"    }});"
            f"  }}catch(e){{console.warn('auto-confirm fetch failed',e);}}"
            f"}})();"
            # Duplicate-recording badge — fetches the multi-signal dupe
            # detection result and tags each row whose recording has a
            # duplicate partner. Badge links to the partner so the user
            # can compare and decide to delete one.
            f"(async function(){{"
            f"  try{{"
            f"    const r=await fetch("
            f"      '{HOST_URL}/api/internal/duplicate-recordings');"
            f"    if(!r.ok)return;"
            f"    const d=await r.json();"
            f"    const partners={{}};"
            f"    for(const p of (d.pairs||[])){{"
            f"      const sc=p.score||0;"
            f"      if(!partners[p.uuid_a]||partners[p.uuid_a].score<sc)"
            f"        partners[p.uuid_a]={{uuid:p.uuid_b,score:sc,basis:p.basis}};"
            f"      if(!partners[p.uuid_b]||partners[p.uuid_b].score<sc)"
            f"        partners[p.uuid_b]={{uuid:p.uuid_a,score:sc,basis:p.basis}};"
            f"    }}"
            f"    document.querySelectorAll('tr[data-uuid]').forEach(tr=>{{"
            f"      const uuid=tr.dataset.uuid;if(!uuid)return;"
            f"      const p=partners[uuid];if(!p)return;"
            f"      const td=tr.querySelector('td:first-child');"
            f"      if(!td)return;"
            f"      const a=document.createElement('a');"
            f"      a.className='badge dupe';"
            f"      a.href='{HOST_URL}/recording/'+p.uuid;"
            f"      a.textContent='📋 dup';"
            f"      a.title='Duplikat-Verdacht ('+p.basis+', score='"
            f"        +Math.round(p.score*100)+'%) — Klick öffnet Partner';"
            f"      td.appendChild(document.createTextNode(' '));"
            f"      td.appendChild(a);"
            f"    }});"
            f"  }}catch(e){{console.warn('dupe fetch failed',e);}}"
            f"}})();"
            # Status filter — multi-select checkboxes hide rows whose
            # data-status isn't in the selected set. State persists per
            # tab via localStorage so the filter survives reloads + the
            # 15s auto-reload above.
            f"(function(){{"
            f"  const KEY='rec-status-filter';"
            f"  const box=document.getElementById('rec-filter');"
            f"  if(!box)return;"
            f"  const cbs=box.querySelectorAll('input[type=checkbox]');"
            f"  let saved={{}};"
            f"  try{{saved=JSON.parse(localStorage.getItem(KEY)||'{{}}');}}"
            f"  catch(e){{}}"
            f"  cbs.forEach(cb=>{{"
            f"    if(cb.value in saved)cb.checked=!!saved[cb.value];"
            f"  }});"
            f"  function apply(){{"
            f"    const allowed=new Set();"
            f"    let editedOnly=false;"
            f"    cbs.forEach(cb=>{{"
            f"      if(cb.value==='unedited-only')editedOnly=cb.checked;"
            f"      else if(cb.checked)allowed.add(cb.value);"
            f"    }});"
            f"    document.querySelectorAll('tr[data-status]').forEach(tr=>{{"
            f"      let ok=allowed.has(tr.dataset.status);"
            # 'unedited-only' is a sub-filter on playable rows ONLY —
            # scheduled/live rows naturally have no ads_user.json.
            f"      if(ok&&editedOnly&&tr.dataset.status==='playable')"
            f"        ok=tr.dataset.edited==='0';"
            f"      tr.classList.toggle('row-hidden',!ok);"
            f"    }});"
            # Hide series wrappers whose every episode row is now
            # filtered out — otherwise the user sees an empty Show
            # title with a (5) badge but no episodes inside.
            f"    document.querySelectorAll('tr.series-head').forEach(head=>{{"
            f"      const visible=head.querySelector("
            f"        'tr[data-status]:not(.row-hidden)');"
            f"      head.classList.toggle('row-hidden',!visible);"
            f"    }});"
            f"  }}"
            f"  cbs.forEach(cb=>cb.addEventListener('change',()=>{{"
            f"    saved[cb.value]=cb.checked;"
            f"    try{{localStorage.setItem(KEY,JSON.stringify(saved));}}"
            f"    catch(e){{}}"
            f"    apply();"
            f"  }}));"
            f"  apply();"
            f"}})();"
            # Expand-all / Collapse-all series buttons. Updates the
            # localStorage saved-state map so the choice persists
            # through the auto-reload (otherwise the next reload
            # would snap individual series back to their default).
            f"(function(){{"
            f"  const KEY='rec-series-open';"
            f"  function setAll(open){{"
            f"    let saved={{}};"
            f"    try{{saved=JSON.parse(localStorage.getItem(KEY)||'{{}}');}}"
            f"    catch(e){{}}"
            f"    document.querySelectorAll('details[data-ar]').forEach(d=>{{"
            f"      d.open=open;saved[d.dataset.ar]=open;"
            f"    }});"
            f"    try{{localStorage.setItem(KEY,JSON.stringify(saved));}}"
            f"    catch(e){{}}"
            f"  }}"
            f"  const eb=document.getElementById('series-expand');"
            f"  const cb=document.getElementById('series-collapse');"
            f"  if(eb)eb.addEventListener('click',()=>setAll(true));"
            f"  if(cb)cb.addEventListener('click',()=>setAll(false));"
            f"}})();"
            # Sort dropdown — reorders top-level rows (= solo + series-
            # head) by data-sort-{start,channel,title}. Series-children
            # stay inside their <details> and ride along with the
            # series-head. Choice persists per browser via localStorage.
            f"(function(){{"
            f"  const KEY='rec-sort';"
            f"  const sel=document.getElementById('sort-select');"
            f"  if(!sel)return;"
            f"  let saved='start_desc';"
            f"  try{{saved=localStorage.getItem(KEY)||'start_desc';}}"
            f"  catch(e){{}}"
            f"  sel.value=saved;"
            f"  function apply(){{"
            # Most-defensive way to find the outer table's tbody:
            # walk via .tBodies[0] which is always populated (browser
            # auto-creates a tbody when source HTML omits it). Selector
            # `.rec-body > table > tbody` SHOULD work but had at least
            # one user report of silent failure → use the JS API for
            # certainty.
            f"    const outerTable=document.querySelector("
            f"      '.rec-body > table');"
            f"    if(!outerTable)return;"
            f"    const tbody=outerTable.tBodies[0];"
            f"    if(!tbody)return;"
            f"    const rows=Array.from(tbody.children).filter("
            f"      r=>r.tagName==='TR'&&r.dataset.sortStart!==undefined);"
            f"    const mode=sel.value;"
            # Always prefer data-sort-start-all when present (= max of
            # ALL episodes including future-scheduled). Series-head rows
            # have no data-status attribute → they're always visible
            # regardless of the status filter, so a series with an
            # episode planned tomorrow visually IS "current activity"
            # for that group. Solo rows have only data-sort-start;
            # fall back to that.
            f"    function startOf(r){{"
            f"      return parseInt(r.dataset.sortStartAll"
            f"        ||r.dataset.sortStart||0);"
            f"    }}"
            f"    rows.sort((a,b)=>{{"
            f"      if(mode==='channel'){{"
            f"        return (a.dataset.sortChannel||'').localeCompare("
            f"               b.dataset.sortChannel||'')"
            f"          ||(startOf(b)-startOf(a));"
            f"      }}"
            f"      if(mode==='title'){{"
            f"        return (a.dataset.sortTitle||'').localeCompare("
            f"               b.dataset.sortTitle||'')"
            f"          ||(startOf(b)-startOf(a));"
            f"      }}"
            f"      const da=startOf(a);"
            f"      const db=startOf(b);"
            f"      return mode==='start_asc'?(da-db):(db-da);"
            f"    }});"
            f"    rows.forEach(r=>tbody.appendChild(r));"
            f"  }}"
            f"  sel.addEventListener('change',()=>{{"
            f"    try{{localStorage.setItem(KEY,sel.value);}}catch(e){{}}"
            f"    apply();"
            f"  }});"
            f"  apply();"
            f"}})();"
            # Edit-mode for manual user-groups (= Rocky/Asterix
            # franchise grouping). Toggle adds body.edit-mode →
            # checkboxes appear via CSS. "Neue Gruppe" merges the
            # current /api/internal/user-groups with the new selection,
            # POSTs back, reloads.
            f"(function(){{"
            f"  const tog=document.getElementById('edit-mode-toggle');"
            f"  const cancel=document.getElementById('edit-mode-cancel');"
            f"  const tb=document.getElementById('edit-toolbar');"
            f"  const cnt=tb&&tb.querySelector('.sel-count');"
            f"  const create=document.getElementById('create-group-btn');"
            f"  if(!tog||!tb||!create)return;"
            f"  function refresh(){{"
            f"    const n=document.querySelectorAll('.rec-select:checked').length;"
            f"    cnt.textContent=n+' ausgewählt';"
            f"    create.disabled=(n<2);"
            f"  }}"
            f"  function setEdit(on){{"
            f"    document.body.classList.toggle('edit-mode',on);"
            f"    tb.style.display=on?'flex':'none';"
            f"    if(!on)document.querySelectorAll('.rec-select:checked')"
            f"      .forEach(cb=>cb.checked=false);"
            f"    refresh();"
            f"  }}"
            f"  tog.addEventListener('click',()=>setEdit(true));"
            f"  cancel.addEventListener('click',()=>setEdit(false));"
            f"  document.addEventListener('change',ev=>{{"
            f"    if(ev.target.classList.contains('rec-select'))refresh();"
            f"  }});"
            f"  create.addEventListener('click',async()=>{{"
            f"    const sel=Array.from(document.querySelectorAll('.rec-select:checked'));"
            f"    const name=prompt('Name der neuen Gruppe:'+"
            f"' (z.B. Rocky, Asterix, Star Wars)');"
            f"    if(!name||!name.trim())return;"
            f"    const cleanName=name.trim();"
            f"    try{{"
            f"      const cur=await fetch('{HOST_URL}/api/internal/user-groups')"
            f"        .then(r=>r.json());"
            f"      const merged=Object.assign({{}},cur);"
            f"      const existing=new Set(merged[cleanName]||[]);"
            f"      sel.forEach(cb=>existing.add(cb.dataset.uuid));"
            f"      merged[cleanName]=Array.from(existing);"
            f"      const r=await fetch('{HOST_URL}/api/internal/user-groups',{{"
            f"        method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"        body:JSON.stringify(merged)}}).then(r=>r.json());"
            f"      if(r.ok)location.reload();"
            f"      else alert('Fehler: '+(r.error||'unknown'));"
            f"    }}catch(e){{alert('Netzwerkfehler: '+e.message);}}"
            f"  }});"
            f"}})();"
            # Dissolve manual user-group: GET current map, drop the
            # named entry, POST back. Recordings themselves untouched
            # — they fall back to whatever the autorec/orphan/solo
            # pass would have done for them.
            f"window.dissolveUserGroup=async function(ev,name){{"
            f"  ev&&ev.stopPropagation();"
            f"  if(!confirm('Gruppe \"'+name+'\" auflösen?\\n'+"
            f"'(Aufnahmen bleiben unverändert)'))return;"
            f"  try{{"
            f"    const cur=await fetch('{HOST_URL}/api/internal/user-groups')"
            f"      .then(r=>r.json());"
            f"    delete cur[name];"
            f"    const r=await fetch('{HOST_URL}/api/internal/user-groups',{{"
            f"      method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"      body:JSON.stringify(cur)}}).then(r=>r.json());"
            f"    if(r.ok)location.reload();"
            f"    else alert('Fehler: '+(r.error||'unknown'));"
            f"  }}catch(e){{alert('Netzwerkfehler: '+e.message);}}"
            f"}};"
            # Persist <details> open/closed state per series across reloads.
            # Key by autorec uuid (data-ar). User toggles win over the
            # server-default 'open if any episode active'.
            f"(function(){{"
            f"  const KEY='rec-series-open';"
            f"  let saved={{}};"
            f"  try{{saved=JSON.parse(localStorage.getItem(KEY)||'{{}}');}}"
            f"  catch(e){{}}"
            f"  for(const d of document.querySelectorAll('details[data-ar]')){{"
            f"    const ar=d.dataset.ar;"
            f"    if(ar in saved)d.open=!!saved[ar];"
            f"    d.addEventListener('toggle',()=>{{"
            f"      saved[ar]=d.open;"
            f"      try{{localStorage.setItem(KEY,JSON.stringify(saved));}}"
            f"      catch(e){{}}"
            f"    }});"
            f"  }}"
            f"}})();"
            f"function showDeleteModal(title,subtitle,onConfirm){{"
            f"  const m=document.createElement('div');"
            f"  m.className='del-modal';"
            f"  m.innerHTML='<div class=\"del-dialog\">'+"
            f"    '<div class=\"del-head\">Aufnahme löschen?</div>'+"
            f"    '<div class=\"del-title\">'+title+'</div>'+"
            f"    (subtitle?'<div class=\"del-sub\">'+subtitle+'</div>':'')+"
            f"    '<div class=\"del-btns\">'+"
            f"    '<button class=\"del-cancel\">Abbrechen</button>'+"
            f"    '<button class=\"del-confirm\">🗑 Löschen</button>'+"
            f"    '</div></div>';"
            f"  document.body.appendChild(m);"
            f"  m.addEventListener('click',ev=>{{"
            f"    if(ev.target===m||ev.target.closest('.del-cancel')){{m.remove();return;}}"
            f"    if(ev.target.closest('.del-confirm')){{m.remove();onConfirm();}}"
            f"  }});"
            f"}}"
            f"document.addEventListener('click',ev=>{{"
            f"  const a=ev.target.closest('.del-btn');if(!a)return;"
            f"  ev.preventDefault();"
            f"  const title=a.dataset.title||'?';"
            f"  const sub=a.dataset.mt?'Aus Liste entfernen (falls lokale Datei existiert, wird sie gelöscht).':"
            f"    (a.dataset.when?'Geplant/aufgenommen am '+a.dataset.when:'');"
            # DELETE via fetch (= /recording/<uuid>/delete and
            # /mediathek-rec/<uuid>/delete are DELETE-only since
            # 2026-05-03; bare GET on the URL returns 405). On
            # success → row-fade then reload. On fail → toast.
            f"  showDeleteModal(title,sub,async()=>{{"
            f"    try{{"
            f"      const r=await fetch(a.href,{{method:'DELETE'}});"
            f"      if(!r.ok)throw new Error('HTTP '+r.status);"
            f"      const tr=a.closest('tr,.fm-rec');"
            f"      if(tr){{tr.style.opacity='.3';tr.style.transition='opacity .2s';}}"
            f"      setTimeout(()=>location.reload(),250);"
            f"    }}catch(e){{"
            f"      alert('Löschen fehlgeschlagen: '+e.message);"
            f"    }}"
            f"  }});"
            f"}});"
            f"document.addEventListener('click',ev=>{{"
            f"  const a=ev.target.closest('.watch-btn');if(!a)return;"
            f"  ev.preventDefault();"
            f"  const next=a.dataset.watched==='1'?0:1;"
            f"  fetch('{HOST_URL}/api/recording/'+a.dataset.uuid+'/watched',"
            f"    {{method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"     body:JSON.stringify({{watched:next===1}})}})"
            f"    .then(r=>r.json()).then(d=>{{"
            f"      if(!d.ok)return;"
            f"      a.dataset.watched=String(next);"
            f"      a.classList.toggle('on',next===1);"
            f"      a.textContent=next===1?'\\u2713':'\\u25CB';"
            f"      a.title='Als '+(next===1?'un':'')+'gesehen markieren';"
            f"    }}).catch(()=>{{}});"
            f"}});"
            f"function cancelSeries(ev,uuid,title,upcoming){{"
            f"  ev.preventDefault();ev.stopPropagation();"
            f"  const msg='Serie abbrechen?\\n\\n'+title+"
            f"'\\n\\nRegel wird gelöscht und '+upcoming+"
            f"' geplante Folge(n) verworfen. Bereits aufgenommene "
            f"Episoden bleiben erhalten.';"
            f"  if(!confirm(msg))return;"
            f"  fetch('{HOST_URL}/cancel-series/'+uuid)"
            f"    .then(r=>r.json()).then(d=>{{"
            f"      if(d.ok)location.reload();"
            f"      else alert('Fehler: '+(d.error||'?'));"
            f"    }}).catch(e=>alert('Fehler: '+e));"
            f"}}"
            f"/* Restore the user's last scroll position so a delete or"
            f"   cancel doesn't kick them back to the top of a 200-row"
            f"   list. Saved on every scroll (debounced) AND right"
            f"   before any action button fires; restored on load if"
            f"   recent (<30 min). Otherwise stay at top, which is"
            f"   today/now since the list is sorted DESC. */"
            f"const RECPOS_KEY='recordings-scroll';"
            f"const RECPOS_TTL_MS=30*60*1000;"
            f"let _recScrollT=null;"
            f"function saveRecScroll(){{"
            f"  try{{localStorage.setItem(RECPOS_KEY,"
            f"    JSON.stringify({{y:window.scrollY,ts:Date.now()}}));}}"
            f"  catch(e){{}}"
            f"}}"
            f"window.addEventListener('scroll',()=>{{"
            f"  clearTimeout(_recScrollT);"
            f"  _recScrollT=setTimeout(saveRecScroll,200);"
            f"}},{{passive:true}});"
            f"/* Wrap fetch so any action (delete, cancel, mark-watched)"
            f"   on this page snapshots scroll first — otherwise the"
            f"   subsequent location.reload() would race the debounce. */"
            f"const _origFetch=window.fetch;"
            f"window.fetch=function(){{saveRecScroll();return _origFetch.apply(this,arguments);}};"
            f"window.addEventListener('load',()=>{{"
            f"  let saved=null;"
            f"  try{{saved=JSON.parse(localStorage.getItem(RECPOS_KEY)||'null');}}"
            f"  catch(e){{}}"
            f"  if(saved&&Date.now()-saved.ts<RECPOS_TTL_MS&&saved.y>0){{"
            f"    window.scrollTo({{top:saved.y,behavior:'instant'}});"
            f"    return;"
            f"  }}"
            f"  /* No recent saved scroll → land on the first not-future"
            f"     row (= today's recordings, since list is sorted DESC"
            f"     and future-scheduled items pile up at the very top). */"
            f"  const a=document.getElementById('now-anchor');"
            f"  if(a){{"
            f"    /* If anchor is inside a collapsed <details>, open it"
            f"       so scrollIntoView lands on a visible element. */"
            f"    const det=a.closest('details');"
            f"    if(det&&!det.open)det.open=true;"
            f"    a.scrollIntoView({{block:'start',behavior:'instant'}});"
            f"  }}"
            f"}});"
            f"</script>"
            f"</body></html>")
    return body


_rec_hls_lock = threading.Lock()
_rec_hls_procs = {}  # uuid -> {"proc": Popen, "started": ts, "total_segs": int}

# Concurrency cap for Pi-local HLS-remux ffmpegs. Without this, multiple
# Mac-fallback timeouts + Kuckuck-app catchup-polls fire >8 ffmpegs in
# parallel on 4 cores → load avg 80+, live recordings stutter, and the
# whole Pi becomes unresponsive. With cap=2, throughput per ffmpeg is
# ~50% of running solo, but no other services starve.
_REMUX_MAX_PARALLEL = int(os.environ.get("REMUX_MAX_PARALLEL", "2"))
_rec_hls_sema = threading.Semaphore(_REMUX_MAX_PARALLEL)
_rec_hls_queued = set()  # uuids waiting on semaphore or running
_rec_cskip_lock = threading.Lock()
_rec_cskip_procs = {}  # uuid -> {"proc": Popen, "started": ts}

# Mac-side detect-running tracker. Mac daemon POSTs to
# /api/internal/detect-started/<uuid> when it picks a job out of the
# pending pool, and the cutlist-uploaded endpoint clears the entry on
# completion. Recordings page reads this to render the 🔍 badge so the
# user sees that detection is mid-flight rather than just "no ads yet".
_detect_running_lock = threading.Lock()
_detect_running = {}  # uuid -> started_at ts
DETECT_RUNNING_STALE_S = 600  # daemon crash → entry expires after 10 min


def _detect_is_running(uuid):
    """True if the Mac daemon has signalled it picked up this detect job
    and hasn't reported completion yet. Stale entries (>10 min) don't count.
    The detect queue now lives in tv-recorder, which persists the running
    set to .detect-running.json; we read that (falling back to the legacy
    in-memory dict for any still-Flask writer)."""
    with _detect_running_lock:
        ts = _detect_running.get(uuid)
    if ts is None:
        try:
            ts = json.loads(
                (HLS_DIR / ".detect-running.json").read_text()).get(uuid)
        except Exception:
            ts = None
    if ts is None:
        return False
    return time.time() - ts <= DETECT_RUNNING_STALE_S


def _rec_source_or_recover(uuid):
    """Like _rec_source_path but auto-generates `.source-recovered.ts`
    from the local HLS-VOD bundle if no other source is available.

    Use from endpoints that NEED source bytes (bumper-capture, trim,
    detect-on-demand). Pre-2026-05-14 these returned 404 for every
    dedup'd recording; now they transparently recover from HLS-VOD
    (one-time ffmpeg + segment-concat per uuid, then cached).

    Returns a path string or None if HLS-VOD is also missing."""
    p = _rec_source_path(uuid)
    if p and Path(p).is_file():
        return p
    out_dir = HLS_DIR / f"_rec_{uuid}"
    pl = out_dir / "index.m3u8"
    if not pl.is_file():
        return None
    recovered = out_dir / ".source-recovered.ts"
    if recovered.is_file():
        return str(recovered)
    # Build a sibling temp playlist with bare basenames (the live
    # index.m3u8 stores URL-paths /hls/_rec_<uuid>/seg_*.ts that the
    # HLS demuxer can't resolve as local files). Same pattern as the
    # trim endpoint's recovery branch.
    try:
        hdr, segs, ftr = _parse_hls_segments(pl)
        if not segs:
            return None
        tmp_pl = out_dir / ".source-recovered.local.m3u8"
        with tmp_pl.open("w") as f:
            f.write("\n".join(hdr) + "\n")
            for extinf, uri, _d, _ts, _te in segs:
                f.write(extinf + "\n")
                f.write(Path(uri).name + "\n")
            if ftr:
                f.write("\n".join(ftr) + "\n")
        tmp_out = out_dir / ".source-recovered.tmp.ts"
        cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
               "-allowed_extensions", "ALL",
               "-i", str(tmp_pl),
               "-c", "copy", "-f", "mpegts",
               str(tmp_out)]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if r.returncode != 0 or not tmp_out.is_file() or tmp_out.stat().st_size < 1024:
            try: tmp_out.unlink(missing_ok=True)
            except Exception: pass
            print(f"[source-recover] {uuid[:8]} ffmpeg rc={r.returncode}: "
                  f"{(r.stderr or '')[-200:]}", flush=True)
            return None
        tmp_out.replace(recovered)
        print(f"[source-recover] {uuid[:8]} HLS-VOD → "
              f"{recovered.stat().st_size/1e6:.0f} MB .source-recovered.ts",
              flush=True)
        return str(recovered)
    except Exception as e:
        print(f"[source-recover] {uuid[:8]} err: {e}", flush=True)
        return None


def _host_to_container(p):
    """Translate `/mnt/tv/...` (host path used by tv-receiver, which
    runs on the Pi outside the container) to `/recordings/...` (the
    bind-mount path inside this container). Pass other paths through
    untouched. Returns str.

    Background: when tv-receiver replaced tvh as the DVR backend, it
    started writing schedules with full host-side filenames (e.g.
    `/mnt/tv/Call Me Kat/...ts`). This container only sees `/mnt/tv`
    via the `/recordings` mount, so Path("/mnt/tv/...").is_file()
    returns False here and recording_source/source delivery 404s.
    """
    if p and p.startswith("/mnt/tv/"):
        return "/recordings/" + p[len("/mnt/tv/"):]
    return p


def _rec_source_path(uuid):
    """Return the file path of the DVR recording inside this container
    (via the /recordings read-only mount). None if not found.
    tvheadend's API stores filenames mojibake-encoded on this host
    (UTF-8 'ü' got round-tripped through Latin-9 → 'ÃŒ' →
    \\xc3\\x83\\xc5\\x92). The actual file on disk is plain UTF-8.
    Try the literal path first; if that misses, scan the parent dir
    and find the file whose mojibake-reversed basename matches.
    Final fallback: a `.source-recovered.ts` inside the HLS dir,
    written by trim/recovery flows when Pi-original is missing."""
    recovered = HLS_DIR / f"_rec_{uuid}" / ".source-recovered.ts"
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000",
            timeout=6).read())
        for e in data.get("entries", []):
            if e.get("uuid") == uuid:
                fn = _host_to_container(e.get("filename") or "")
                if not fn:
                    return str(recovered) if recovered.is_file() else None
                if Path(fn).is_file():
                    return fn
                parent = Path(fn).parent
                if not parent.is_dir():
                    return str(recovered) if recovered.is_file() else fn
                base = Path(fn).name
                try:
                    fixed = base.encode("iso-8859-15").decode("utf-8")
                except Exception:
                    return str(recovered) if recovered.is_file() else fn
                for entry in parent.iterdir():
                    if entry.name == fixed:
                        return str(entry)
                return str(recovered) if recovered.is_file() else fn
    except Exception:
        pass
    return str(recovered) if recovered.is_file() else None


_rec_slug_cache = {"ts": 0, "slug_map": {}, "title_map": {}}
_REC_SLUG_CACHE_TTL_S = 30


def _refresh_rec_dvr_cache():
    """Pull tvh's DVR grid once per TTL and populate uuid→slug AND
    uuid→title maps in one HTTP roundtrip. Called by the per-uuid
    accessors below; both share the same cache because the source
    of truth is the same endpoint."""
    now = time.time()
    if now - _rec_slug_cache["ts"] < _REC_SLUG_CACHE_TTL_S:
        return
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000",
            timeout=6).read())
        slugs, titles = {}, {}
        for e in data.get("entries", []):
            u = e.get("uuid")
            if not u:
                continue
            slugs[u] = slugify(e.get("channelname") or "")
            titles[u] = e.get("disp_title") or ""
        _rec_slug_cache["slug_map"] = slugs
        _rec_slug_cache["title_map"] = titles
        _rec_slug_cache["ts"] = now
    except Exception:
        pass


def _rec_channel_slug(uuid):
    """Look up which channel a DVR entry was recorded from, as a slug.
    The full uuid→slug mapping is cached for 30 s — without this every
    caller (failure-mode aggregator, recommendations engine, etc.)
    hits the tvh /api/dvr/entry/grid endpoint once per uuid, which
    on the /learning page added up to hundreds of redundant calls
    per render and dominated total page-load time."""
    _refresh_rec_dvr_cache()
    return _rec_slug_cache["slug_map"].get(uuid, "")


def _rec_dvr_title(uuid):
    """Look up the tvh DVR entry's `disp_title` for a uuid. Used as a
    fallback when _show_title_for_rec returns empty (= recording's
    cutlist .txt hasn't been written yet by detect, but the user
    might have already reviewed it via fingerprint auto-confirm).
    Same cache as _rec_channel_slug."""
    _refresh_rec_dvr_cache()
    return _rec_slug_cache["title_map"].get(uuid, "")


_TVD_LOGO_DIR = HLS_DIR / ".tvd-logos"
_TVD_UNCERTAIN_FILE = HLS_DIR / ".tvd-models" / "head.uncertain.txt"


# Lazy mtime-cached parse of train-head.py's active-learning output.
# Key: uuid → list of {time_s, prob}. Reloads whenever the file's
# mtime changes, so a fresh head retrain (which rewrites the file)
# is picked up without restarting the gateway.
_uncertain_cache = {"mtime": 0, "data": {}, "total": 0}
_uncertain_lock = threading.Lock()


def _uncertain_for_recording(uuid):
    """Return list of {"t": time_s, "p": probability} for uuid, or
    [] if no entries / file missing. Entries are the timestamps where
    the trained NN head is least confident — surfaced for the user
    to manually verify (active learning).

    Returns [] when the recording has been marked reviewed (matches
    the recordings-list 🎯 badge filter and the /learning queue
    filter — once user clicks Geprüft the orange scrub-bar marks
    must also disappear, both immediately via JS and on page reload
    via this server-side filter)."""
    try:
        mtime = int(_TVD_UNCERTAIN_FILE.stat().st_mtime)
    except Exception:
        return []
    with _uncertain_lock:
        if mtime != _uncertain_cache["mtime"]:
            data, total = {}, 0
            try:
                for ln in _TVD_UNCERTAIN_FILE.read_text().splitlines():
                    if not ln or ln.startswith("#"):
                        continue
                    parts = ln.split("\t")
                    if len(parts) < 3:
                        continue
                    u = parts[0].strip()
                    try:
                        t = float(parts[1]); p = float(parts[2])
                    except ValueError:
                        continue
                    data.setdefault(u, []).append({"t": t, "p": p})
                    total += 1
            except Exception:
                data, total = {}, 0
            for u in data:
                data[u].sort(key=lambda e: e["t"])
            _uncertain_cache.update({"mtime": mtime, "data": data,
                                     "total": total})
        items = list(_uncertain_cache["data"].get(uuid, []))
    # Reviewed-aware filter (mirrors _uncertain_count)
    user_cache = HLS_DIR / f"_rec_{uuid}" / "ads_user.json"
    if items and user_cache.exists():
        try:
            data = json.loads(user_cache.read_text())
            if isinstance(data, dict) and data.get("reviewed_at"):
                return []
        except Exception:
            pass
    return items


def _uncertain_count(uuid):
    """Cheap count for the recordings list. Returns 0 if the user has
    ever marked this recording reviewed — matches the /learning page
    filter so badge and queue stay consistent. Once geprüft, off
    forever, even if a later retrain finds new uncertain frames in
    the same recording (those are usually intra-block confusion or
    already inside a user ad-block)."""
    _uncertain_for_recording(uuid)  # ensure cache fresh
    with _uncertain_lock:
        n = len(_uncertain_cache["data"].get(uuid, []))
    if n == 0:
        return 0
    user_cache = HLS_DIR / f"_rec_{uuid}" / "ads_user.json"
    if user_cache.exists():
        try:
            data = json.loads(user_cache.read_text())
            if isinstance(data, dict) and data.get("reviewed_at"):
                return 0
        except Exception:
            pass
    return n

def _rec_cskip_spawn(uuid):
    """Run tv-detect on the original TS so we can mark commercial blocks
    on the scrub bar. Fire-and-forget; result is polled via files.
    Function name kept for grep history; the underlying tool is
    tv-detect now (formerly comskip — see PHASE6.md in tv-detect repo).

    Logo resolution mirrors the Mac-side tv-comskip.sh:
      1. per-channel cached at /data/hls/.tvd-logos/<slug>.logo.txt
      2. fallback --auto-train 5 (samples first 5 min of THIS recording,
         caches as <basename>.trained.logo.txt next to the source)
    Empty cutlist marker on training failure (typical for ad-free
    public broadcasters).
    """
    with _rec_cskip_lock:
        info = _rec_cskip_procs.get(uuid)
        if info and info["proc"].poll() is None:
            return
        out_dir = HLS_DIR / f"_rec_{uuid}"
        out_dir.mkdir(parents=True, exist_ok=True)
        src = _rec_source_path(uuid)
        if not src or not Path(src).exists():
            return
        slug = _rec_channel_slug(uuid)
        # Output filename matches what comskip would have written —
        # existing parsers (_rec_parse_comskip below) consume it as-is.
        base = Path(src).stem
        out_txt = out_dir / f"{base}.txt"
        # Clear any prior NON-archived txt files so we can detect new ones.
        for old in out_dir.glob("*.txt"):
            n = old.name
            if n.endswith(".logo.txt") or n.endswith(".cskp.txt") \
                    or n.endswith(".tvd.txt") or n.endswith(".trained.logo.txt"):
                continue
            try: old.unlink()
            except Exception: pass

        cached_logo = _TVD_LOGO_DIR / f"{slug}.logo.txt" if slug else None
        # Mac offload via .detect-requested marker. Daemon downloads
        # .ts via HTTP, runs tv-detect with NN flags (Pi-local path
        # below omits these), POSTs cutlist back. Idempotent: if a
        # fresh marker already exists, don't spawn a new fallback
        # (avoids stacked timers when prewarm re-calls every cycle).
        if DETECT_OFFLOAD == "mac":
            marker = out_dir / ".detect-requested"
            if not marker.exists():
                marker.write_text(json.dumps({"ts": time.time()}))
                print(f"[rec-cskip {uuid[:8]}] marker for Mac", flush=True)
            else:
                # Refresh mtime so the daemon's pending-list still sees
                # it as fresh during a long Mac queue
                try: marker.touch()
                except Exception: pass
            # NO automatic Pi-local fallback. With bulk markers (after
            # head-bin invalidation triggers re-detect on every
            # recording), the old 300 s fallback fired for many uuids
            # in parallel — 3+ tv-detect processes on the Pi at once,
            # CPU 90 °C, load 25+. If the Mac daemon is genuinely
            # broken, the user will see no cutlists arriving and can
            # intervene manually (restart daemon, check log) rather
            # than the Pi auto-overloading itself.
            return
        if cached_logo and cached_logo.is_file() and cached_logo.stat().st_size > 0:
            cmd = ["tv-detect", "--quiet", "--workers", "4",
                   "--logo", str(cached_logo),
                   "--output", "cutlist", src]
            mode = f"cached/{slug}"
        else:
            cmd = ["tv-detect", "--quiet", "--workers", "4",
                   "--auto-train", "5", "--output", "cutlist", src]
            mode = f"auto-train{'/'+slug if slug else ''}"

        # Cooperative lock-file so the Mac-side tv-comskip.sh skips
        # this recording while we're working on it (and vice versa —
        # tv-comskip.sh writes the same file before spawning). Stale
        # locks are ignored after 15 min via mtime check on the reader.
        scanning = out_dir / ".scanning"
        try:
            scanning.write_text(f"pi {os.getpid()} {int(time.time())}")
        except Exception:
            pass
        # Redirect tv-detect's cutlist stdout into the .txt file.
        try:
            out_fh = open(out_txt, "wb")
        except Exception as ex:
            print(f"[rec-cskip {uuid[:8]}] open out_txt: {ex}", flush=True)
            return
        proc = subprocess.Popen(cmd,
                                 stdout=out_fh,
                                 stderr=subprocess.DEVNULL)
        _rec_cskip_procs[uuid] = {"proc": proc, "started": time.time()}
        print(f"[rec-cskip {uuid[:8]}] tv-detect spawned ({mode})", flush=True)
        def _cleanup(p, lock, fh):
            try: p.wait()
            except Exception: pass
            try: fh.close()
            except Exception: pass
            try: lock.unlink(missing_ok=True)
            except Exception: pass
        threading.Thread(target=_cleanup, args=(proc, scanning, out_fh),
                         daemon=True).start()


THUMB_INTERVAL = 30   # seconds between thumbnails
THUMBS_OFFLOAD = os.environ.get("THUMBS_OFFLOAD", "")  # "mac" → marker file
HLS_OFFLOAD    = os.environ.get("HLS_OFFLOAD", "")     # "mac" → marker file
HLS_FALLBACK_S = int(os.environ.get("HLS_FALLBACK_S", "60"))  # Pi takes over if Mac silent for this long
DETECT_OFFLOAD = os.environ.get("DETECT_OFFLOAD", "")  # "mac" → marker file
DETECT_FALLBACK_S = int(os.environ.get("DETECT_FALLBACK_S", "120"))
_rec_thumbs_lock = threading.Lock()
_rec_thumbs_running = set()   # uuids currently being processed

def _rec_thumbs_spawn(uuid):
    """Generate scrub-bar thumbnails (1 per THUMB_INTERVAL s, scaled to
    160 px wide) once per recording. Writes to _rec_<uuid>/thumbs/
    and a sentinel `.done` file when finished. Idempotent — safe to
    call on every /progress poll without spawning duplicates.

    When THUMBS_OFFLOAD=mac, drop a `.requested` marker file
    containing the source .ts path and let the Mac-side daemon
    (~/bin/tv-thumbs-daemon.py) do the ffmpeg work. The daemon
    writes JPGs back to the SMB-shared directory and removes the
    marker. If the daemon is offline, no thumbs appear — degraded
    UX but not broken (player handles missing thumbs)."""
    out_dir = HLS_DIR / f"_rec_{uuid}" / "thumbs"
    if (out_dir / ".done").exists():
        return
    with _rec_thumbs_lock:
        if uuid in _rec_thumbs_running:
            return
        _rec_thumbs_running.add(uuid)
    src = _rec_source_path(uuid)
    if not src or not Path(src).exists():
        with _rec_thumbs_lock:
            _rec_thumbs_running.discard(uuid)
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    if THUMBS_OFFLOAD == "mac":
        try:
            (out_dir / ".requested").write_text(src)
            print(f"[rec-thumbs {uuid[:8]}] marker for Mac → {src}",
                  flush=True)
        finally:
            with _rec_thumbs_lock:
                _rec_thumbs_running.discard(uuid)
        return
    def run():
        try:
            # stderr → DEVNULL: -loglevel error suppresses ffmpeg's
            # regular log output but DVB sequence-header issues emit
            # [mpeg2video] codec warnings that bypass that filter →
            # docker logs noise. Thumb job already has no error-
            # reporting path (no .done file = visible missing thumbs
            # on /recordings) so swallowing stderr loses nothing.
            subprocess.run(
                ["nice", "-n", "18", "ionice", "-c", "3",
                 "ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
                 "-i", src,
                 "-vf", f"fps=1/{THUMB_INTERVAL},scale=160:-2",
                 "-q:v", "6",
                 str(out_dir / "t%05d.jpg")],
                timeout=900, check=False, stderr=subprocess.DEVNULL)
            (out_dir / ".done").write_text("")
            print(f"[rec-thumbs {uuid[:8]}] done", flush=True)
        except Exception as e:
            print(f"[rec-thumbs {uuid[:8]}] {e}", flush=True)
        finally:
            with _rec_thumbs_lock:
                _rec_thumbs_running.discard(uuid)
    threading.Thread(target=run, daemon=True).start()
    print(f"[rec-thumbs {uuid[:8]}] spawned", flush=True)


_cohort_cache = {"ts": 0, "suspect": set(), "uuid_to_cohort": {}}
_COHORT_CACHE_TTL_S = 300


def _cohort_has_user_ads(uuid_str):
    """Returns True iff some OTHER recording in the same (title,
    channel) cohort has at least one user-confirmed ad block.

    Used by the auto-confirm pipeline as a sanity gate against
    under-bumper-coverage false positives. Single tvh fetch builds
    both the suspect-cohort set and a uuid→cohort map; cached 5min."""
    now = time.time()
    if now - _cohort_cache["ts"] > _COHORT_CACHE_TTL_S:
        try:
            data = json.loads(urllib.request.urlopen(
                f"{dvr_base()}/api/dvr/entry/grid?limit=2000",
                timeout=8).read())
        except Exception:
            return False
        suspect = set()
        u2c = {}
        for e in data.get("entries", []):
            u = e.get("uuid")
            if not u: continue
            title = (e.get("disp_title") or "").strip()
            ch = (e.get("channelname") or "").strip()
            if not title or not ch: continue
            cohort = (title, ch)
            u2c[u] = cohort
            user_p = HLS_DIR / f"_rec_{u}" / "ads_user.json"
            if not user_p.is_file(): continue
            try:
                d = json.loads(user_p.read_text())
                if not isinstance(d, dict): continue
                if d.get("ads") or []:
                    suspect.add(cohort)
            except Exception:
                continue
        _cohort_cache.update(ts=now, suspect=suspect, uuid_to_cohort=u2c)
    cohort = _cohort_cache["uuid_to_cohort"].get(uuid_str)
    return cohort is not None and cohort in _cohort_cache["suspect"]


def _rec_playlist_duration(out_dir):
    """Sum the HLS playlist's EXTINF lines → playable duration in
    seconds (0.0 if no playlist). This is the length the client's
    <video> element actually exposes, so it's the right denominator
    for clamping ad-blocks + the comskip fps sanity-check."""
    pl = out_dir / "index.m3u8"
    if not pl.exists():
        return 0.0
    total = 0.0
    try:
        for ln in pl.read_text().splitlines():
            if ln.startswith("#EXTINF:"):
                try:
                    total += float(ln.split(":", 1)[1].rstrip(","))
                except Exception:
                    pass
    except Exception:
        return 0.0
    return total


def _rec_parse_comskip(out_dir, true_duration_s=0.0):
    """Parse comskip's default .txt output → list of [start_s, stop_s].
    Adjacent blocks separated by a short sponsor card (≤25 s of "show")
    are merged — typical pattern on VOX/RTL/Pro7: Werbung · 15 s
    Präsentation-Einblendung · Werbung · Sendungsstart.

    Interlaced fps correction: comskip reports the frame rate (e.g.
    2500 = 25.00 fps) but on interlaced SD (720x576 50i: ProSieben,
    RTL, …) it indexes FIELDS, so the frame numbers run at ~50/s while
    "FRAMES AT" still says 2500. Converting field-indices with 25 fps
    doubles every timestamp → blocks land past the recording end
    (witnessed 2026-05-28: block at 2960-3452 s on a 2410 s recording,
    the real ad was at ~1480-1726 s). When true_duration_s is known and
    the comskip total-frame-count implies a materially higher rate than
    the reported fps, trust the derived rate (total_frames /
    true_duration_s) instead."""
    # Exclude sidecar .txt files. .logo.txt is the trained edge mask,
    # .trained.logo.txt is its tv-detect-side equivalent, .cskp.txt is
    # archived comskip output kept as historical reference for diffs,
    # .tvd.txt is a leftover shadow-mode artifact from the comskip-→-
    # tv-detect transition. Filesystem ordering can put any of those
    # ahead of the actual cutlist if we don't filter them out.
    SIDECAR = (".logo.txt", ".trained.logo.txt", ".cskp.txt", ".tvd.txt")
    txts = [p for p in out_dir.glob("*.txt")
            if not any(p.name.endswith(s) for s in SIDECAR)]
    if not txts:
        return []
    try:
        lines = txts[0].read_text().splitlines()
    except Exception:
        return []
    fps = 25.0
    total_frames = 0
    ads = []
    # Comskip's .txt occasionally writes a long run of NUL bytes
    # before the first frame-range line (witnessed on the Mac-side
    # patched build, suspect ftell/fwrite ordering on the merged-ts
    # input stream). After str.strip() those NULs stay in the line —
    # find the LAST whitespace-separated frame pair via regex so the
    # leading garbage doesn't break parsing.
    line_re = re.compile(r"(\d+)\s+(\d+)\s*$")
    # Header: "FILE PROCESSING COMPLETE  110437 FRAMES AT  2500"
    hdr_re = re.compile(r"(\d+)\s+FRAMES AT\s+(\d+)")
    for line in lines:
        line = line.strip().replace("\x00", "")
        if not line or line.startswith("-"):
            continue
        if "FRAMES AT" in line:
            try:
                fps = float(line.split()[-1]) / 100.0
                if fps <= 0:
                    fps = 25.0
            except Exception:
                pass
            m = hdr_re.search(line)
            if m:
                try:
                    total_frames = int(m.group(1))
                except Exception:
                    total_frames = 0
            # Interlaced field-rate correction: if comskip's total frame
            # count over the real duration implies a rate ≥1.5× the
            # reported fps, it was counting fields — use the derived
            # rate so field-indices convert to wall-clock correctly.
            if true_duration_s and true_duration_s > 0 and total_frames > 0:
                derived = total_frames / true_duration_s
                if derived >= fps * 1.5:
                    print(f"[comskip-parse] interlaced field-rate detected: "
                          f"reported {fps:.2f} fps, derived {derived:.2f} "
                          f"(={total_frames}f/{true_duration_s:.0f}s) — "
                          f"using derived", flush=True)
                    fps = derived
            continue
        m = line_re.search(line)
        if not m:
            continue
        try:
            a = float(m.group(1)) / fps
            b = float(m.group(2)) / fps
            if b - a >= 60:   # ignore stub blocks shorter than 60 s
                              # — matches comskip.ini's
                              # min_commercialbreak=60, also serves as
                              # safety net when comskip emits sub-60 s
                              # detections from edge cases (older
                              # ini, partial-window scans, etc.)
                ads.append([round(a, 2), round(b, 2)])
        except Exception:
            continue
    # Comskip's .txt sometimes contains duplicate frame pairs — dedup
    # and sort before we touch them.
    seen = set()
    dedup = []
    for a, b in ads:
        key = (round(a, 1), round(b, 1))
        if key not in seen:
            seen.add(key)
            dedup.append([a, b])
    dedup.sort(key=lambda x: x[0])
    # Merge blocks with short gaps (sponsor card / Praesentation-Einblendung).
    # 45 s catches German private-TV sponsor cards including the
    # "Werbung · Sponsor · Werbung" pattern with mid-block teasers.
    MERGE_GAP = 45.0
    merged = []
    for a, b in dedup:
        if merged and a - merged[-1][1] <= MERGE_GAP:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return merged


def _blackframe_extend_ads(video_path, ads,
                            channel_slug=None,
                            sponsor_duration=None,
                            max_extend=None):
    """Look for blackframes after each comskip ad-end. Three cases:
       (1) two blacks found after end → pattern is
           [ad] · (black) · [sponsor] · (black) · [show] — extend to
           the latest black (sponsor end).
       (2) one black close to end → pattern is [ad] · (black) ·
           [sponsor] · hardcut → extend by sponsor_duration from the
           black (the sponsor's end isn't marked by a black).
       (3) no blackframe → no extension.
       Per-channel sponsor_duration via SPONSOR_DURATION_BY_CHANNEL."""
    if sponsor_duration is None:
        sponsor_duration = SPONSOR_DURATION_BY_CHANNEL.get(
            channel_slug or "", SPONSOR_DURATION_DEFAULT)
    if max_extend is None:
        max_extend = sponsor_duration + 10.0
    if not ads or not video_path or sponsor_duration <= 0:
        return ads
    scan_window = max(sponsor_duration + 8.0, 20.0)
    # Backward window for ad-START. Two blackframes typically precede
    # comskip's logo-loss-based detection on DE private TV:
    #   show-end (BF1) → ~25 s "Programmhinweis" sponsor card
    #   (logo still visible, looks like content) → (BF2) → real ad
    # comskip detects somewhere AFTER BF2 by 5-15 s. We snap back to
    # the EARLIEST blackframe in the window, so a 75 s window catches
    # both: BF2 (~30-40 s back) when no promo, BF1 (~50-65 s back)
    # when there is one. Safely smaller than min_show_segment_length
    # =120 s so we won't pull a blackframe from inside the show.
    START_SCAN = 75.0
    START_MAX_EXTEND = 75.0
    out = []
    for start, end in ads:
        # --- forward extension (ad-end → sponsor-end) ---
        ss = max(0, end - 1.0)
        new_end = end
        blacks_after = []
        try:
            proc = subprocess.run(
                ["ffmpeg", "-hide_banner", "-nostats",
                 "-ss", str(ss), "-i", str(video_path),
                 "-t", str(scan_window + 2),
                 "-vf", "blackdetect=d=0.04:pix_th=0.30:pic_th=0.80",
                 "-an", "-f", "null", "-"],
                capture_output=True, text=True, timeout=25)
            for line in proc.stderr.splitlines():
                if "blackdetect" in line and "black_end:" in line:
                    try:
                        rel = float(line.split("black_end:")[1].split()[0])
                        abs_t = ss + rel
                        if end < abs_t <= end + scan_window:
                            blacks_after.append(abs_t)
                    except Exception:
                        pass
        except Exception:
            pass
        extend_to = end
        if len(blacks_after) >= 2 and (blacks_after[-1] - blacks_after[0]) >= 5.0:
            extend_to = blacks_after[-1] + 0.5
        elif blacks_after and blacks_after[0] <= end + 5.0:
            extend_to = blacks_after[0] + sponsor_duration
        elif blacks_after:
            extend_to = blacks_after[-1] + 0.5
        new_end = max(new_end, min(end + max_extend, extend_to))
        # --- backward extension (ad-start → earlier blackframe) ---
        # ffmpeg fast-seek (-ss BEFORE -i) drops blackdetect output
        # for the first ~6 s after the seek-point — filter warm-up.
        # Pre-roll 12 s before our window so any blackframe at the
        # very start of the actual window survives. Blackframes still
        # filtered to [start - START_SCAN, start - 1].
        WARMUP = 12.0
        ss2 = max(0, start - START_SCAN - WARMUP)
        new_start = start
        blacks_before = []
        try:
            proc = subprocess.run(
                ["ffmpeg", "-hide_banner", "-nostats",
                 "-ss", str(ss2), "-i", str(video_path),
                 "-t", str(START_SCAN + WARMUP + 2),
                 "-vf", "blackdetect=d=0.04:pix_th=0.30:pic_th=0.80",
                 "-an", "-f", "null", "-"],
                capture_output=True, text=True, timeout=25)
            for line in proc.stderr.splitlines():
                if "blackdetect" in line and "black_start:" in line:
                    try:
                        rel = float(line.split("black_start:")[1].split()[0])
                        abs_t = ss2 + rel
                        if start - START_SCAN <= abs_t < start - 1.0:
                            blacks_before.append(abs_t)
                    except Exception:
                        pass
        except Exception:
            pass
        if blacks_before:
            # Earliest black within the scan window = true ad start.
            earliest = min(blacks_before)
            if start - earliest <= START_MAX_EXTEND:
                new_start = earliest
        # Fallback: if backward-extend found no earlier blackframe and
        # the channel is known to have a soft logo fade (comskip lags
        # by N s), shift back by at least that much.
        min_back = START_LAG_FALLBACK.get(channel_slug or "", 0)
        if min_back > 0 and new_start >= start - 1.0:
            new_start = max(0.0, start - min_back)
        out.append([round(new_start, 2), round(new_end, 2)])
    merged = []
    for a, b in out:
        if merged and a - merged[-1][1] <= 1.0:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return merged


def _rec_probe_total_segments(src_url):
    """Approximate total HLS segment count (6 s each) from the source
    duration. Used for progress UI; minor rounding is fine."""
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "format=duration",
             "-of", "default=nokey=1:noprint_wrappers=1", src_url],
            capture_output=True, text=True, timeout=10)
        dur = float((probe.stdout or "0").strip() or 0)
        return max(1, int(dur / 6) + 1)
    except Exception:
        return 0


def _is_recording_in_progress(uuid):
    """Return True if tvheadend's DVR entry for uuid currently has
    sched_status='recording' (= file is still being written). Used as
    a guard so HLS-remux + ad-detect don't run against a partial .ts.
    Best-effort: returns False on tvh API error so we don't block all
    work if the API hiccups.
    """
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=500", timeout=5).read())
        for e in data.get("entries", []):
            if e.get("uuid") == uuid:
                return e.get("sched_status") == "recording"
    except Exception:
        pass
    return False


def _rec_hls_spawn(uuid):
    """Trigger HLS remux. With HLS_OFFLOAD=mac, drops a `.hls-requested`
    marker for the Mac daemon to pick up over HTTP and POST back the
    HLS bundle as a tar (~10s on M5 Pro vs ~30-60s on Pi 5 + 326% Pi
    CPU saved). Falls back to local ffmpeg after HLS_FALLBACK_S if the
    Mac doesn't deliver, so a Mac-down outage doesn't strand viewers.

    Without HLS_OFFLOAD, this is the same in-process ffmpeg spawn as
    before — the actual local-spawn implementation is in
    `_rec_hls_spawn_local`, separated only for the offload-path
    fallback re-call.

    Guards against in-progress recordings: if the DVR entry is still
    being written (sched_status='recording'), returns immediately
    without dropping the marker. The caller (player progress poll,
    prewarm cycle, recording_hls VOD endpoint) can re-call later when
    the recording actually completed; until then the playlist file
    just doesn't exist and the player keeps polling. Was the cause of
    the 'recording finalised short of real length' bug — Mac would
    HTTP-fetch /source while tvh was still flushing the trailing GOP
    and remux only the bytes that had landed at request time.
    """
    if _is_recording_in_progress(uuid):
        return HLS_DIR / f"_rec_{uuid}" / "index.m3u8"
    out_dir = HLS_DIR / f"_rec_{uuid}"
    playlist = out_dir / "index.m3u8"
    with _rec_hls_lock:
        existing = _rec_hls_procs.get(uuid)
        if existing and existing["proc"].poll() is None:
            return playlist
        out_dir.mkdir(parents=True, exist_ok=True)
        try: playlist.unlink()
        except FileNotFoundError: pass

    if HLS_OFFLOAD == "mac":
        marker = out_dir / ".hls-requested"
        marker.write_text(json.dumps({
            "src": _rec_source_path(uuid) or "",
            "ts": time.time()}))
        print(f"[rec-hls {uuid[:8]}] marker for Mac", flush=True)
        # Fallback timer: if Mac doesn't deliver a playlist within
        # HLS_FALLBACK_S, take over locally so the user isn't stuck
        # waiting on a silent Mac.
        #
        # HLS_FALLBACK_S=0 DISABLES the Pi-local fallback entirely: the
        # Mac is the only ffmpeg host, by design (= Pi 5 has no HW
        # encoder, so libx264 on the Pi spikes load to 80+ and starves
        # live recordings). The marker stays until the Mac delivers; the
        # player keeps polling on the missing playlist. Trade-off: a
        # genuinely-offline Mac means VOD playback of un-remuxed
        # recordings never becomes available — acceptable since the Mac
        # is always-on and the live + DVR paths don't depend on it.
        if HLS_FALLBACK_S <= 0:
            return playlist

        def _fallback():
            time.sleep(HLS_FALLBACK_S)
            if not playlist.exists() and marker.exists():
                print(f"[rec-hls {uuid[:8]}] Mac timeout {HLS_FALLBACK_S}s "
                      f"— falling back to Pi-local remux", flush=True)
                try: marker.unlink()
                except Exception: pass
                _rec_hls_spawn_local(uuid)
        threading.Thread(target=_fallback, daemon=True).start()
        return playlist

    return _rec_hls_spawn_local(uuid)


def _rec_hls_spawn_local(uuid):
    """Pi-local ffmpeg HLS-remux. Original `_rec_hls_spawn` body — same
    behaviour, separated only so the offload path can fall back to it.

    Semaphore-gated: caller returns the playlist path immediately; the
    actual ffmpeg-spawn runs in a worker that blocks on _rec_hls_sema
    so we never have more than _REMUX_MAX_PARALLEL ffmpegs alive at
    once. While queued, the playlist file simply doesn't exist yet —
    callers poll for it the same way they already do for the in-progress
    recording case.
    """
    out_dir = HLS_DIR / f"_rec_{uuid}"
    playlist = out_dir / "index.m3u8"
    with _rec_hls_lock:
        existing = _rec_hls_procs.get(uuid)
        if existing and existing["proc"].poll() is None:
            return playlist
        if uuid in _rec_hls_queued:
            return playlist
        _rec_hls_queued.add(uuid)
        out_dir.mkdir(parents=True, exist_ok=True)
        try: playlist.unlink()
        except FileNotFoundError: pass

    def _gated_spawn():
        with _rec_hls_sema:
            try:
                _rec_hls_spawn_local_inner(uuid, out_dir, playlist)
            finally:
                with _rec_hls_lock:
                    _rec_hls_queued.discard(uuid)
    threading.Thread(target=_gated_spawn, daemon=True).start()
    return playlist


def _rec_hls_spawn_local_inner(uuid, out_dir, playlist):
    """Actual ffmpeg-spawn body — runs inside the semaphore-gated worker.
    Blocks until ffmpeg exits so the semaphore is held for the entire
    ffmpeg lifetime (= true concurrency cap)."""
    # Prefer the on-disk file over tvheadend's /dvrfile HTTP endpoint
    # — ffmpeg 8.x's MPEG-2 decoder chokes on the HTTP stream (bails
    # after 25× "Invalid frame dimensions 0x0" with zero output),
    # while the same .ts file read directly decodes fine.
    src = _rec_source_path(uuid) or f"{dvr_base()}/dvrfile/{uuid}"
    # Probe video codec — copy if already H.264, transcode MPEG-2 etc.
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_name", "-of",
             "default=nokey=1:noprint_wrappers=1", src],
            capture_output=True, text=True, timeout=10)
        vcodec = (probe.stdout or "").strip()
    except Exception:
        vcodec = ""
    if vcodec in SAFE_VIDEO:
        v_opts = ["-c:v", "copy"]
    else:
        v_opts = ["-vf",
                  "scale=trunc(iw*sar/2)*2:trunc(ih/2)*2,setsar=1",
                  "-c:v", "libx264", "-preset", "ultrafast",
                  "-profile:v", "main", "-pix_fmt", "yuv420p",
                  "-g", "50"]
    cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y",
           "-i", src,
           "-map", "0:v:0", "-map", "0:a:0",
           *v_opts, "-c:a", "aac", "-b:a", "128k",
           "-f", "hls",
           "-hls_time", "6",
           "-hls_list_size", "0",
           "-hls_playlist_type", "event",
           "-hls_base_url", f"/hls/_rec_{uuid}/",
           "-hls_segment_filename", str(out_dir / "seg_%05d.ts"),
           str(playlist)]
    # Run remux with low CPU priority so live playback wins the
    # scheduler if someone is watching at the same time.
    cmd = ["nice", "-n", "15"] + cmd
    proc = subprocess.Popen(cmd,
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
    total = _rec_probe_total_segments(src)
    with _rec_hls_lock:
        _rec_hls_procs[uuid] = {"proc": proc, "started": time.time(),
                                 "total_segs": total}
    print(f"[rec-hls {uuid[:8]}] ffmpeg spawned, ~{total} segments",
          flush=True)
    # Kick off comskip right away — it reads the same .ts source
    # (read-only) so it can run concurrently with the remux and
    # commercial markers land on the scrub bar ~5 min sooner.
    if not _mac_comskip_alive():
        _rec_cskip_spawn(uuid)
    # Block until ffmpeg exits — keeps the semaphore held.
    proc.wait()
    if proc.returncode != 0:
        # Self-heal on crash: wipe the partial HLS directory so the next
        # player request re-spawns from scratch. Without this we'd serve
        # a truncated playlist + premature ENDLIST = instant-done empty
        # clip.
        print(f"[rec-hls {uuid[:8]}] ffmpeg failed "
              f"(rc={proc.returncode}) — wiping {out_dir.name}",
              flush=True)
        shutil.rmtree(out_dir, ignore_errors=True)
        with _rec_hls_lock:
            _rec_hls_procs.pop(uuid, None)
    return playlist


_live_ads_lock = threading.Lock()
_live_ads = {}   # slug -> {"generated": ts, "ads": [[wall_start, wall_stop], ...]}
_live_ads_proc = {"slug": None, "started": 0}
LIVE_ADS_FILE = HLS_DIR / ".live_ads.json"
MEDIATHEK_REC_FILE = HLS_DIR / ".mediathek_recordings.json"
_mediathek_rec_lock = threading.Lock()
_mediathek_rec = {}  # uuid -> {title, channel, start, stop, hls_url,
                     #         available_to, eid, created_at}


def load_mediathek_rec():
    if not MEDIATHEK_REC_FILE.exists():
        return
    try:
        with _mediathek_rec_lock:
            _mediathek_rec.update(json.loads(MEDIATHEK_REC_FILE.read_text()))
        print(f"Loaded {len(_mediathek_rec)} mediathek recordings", flush=True)
    except Exception as e:
        print(f"load mediathek recordings: {e}", flush=True)


def save_mediathek_rec():
    try:
        with _mediathek_rec_lock:
            snap = dict(_mediathek_rec)
        MEDIATHEK_REC_FILE.write_text(json.dumps(snap))
    except Exception as e:
        print(f"save mediathek recordings: {e}", flush=True)


# V3 — pre-expiry rip of virtual Mediathek recordings to a local MP4.
# Mediathek HLS is already H.264/AAC, so ffmpeg -c copy just swaps the
# container, fast and lossless. Rip when Mediathek expiry approaches
# so the show survives the availability window.
RIP_THRESHOLD_SECONDS = 48 * 3600
RIP_LOAD_CAP = 8.0
RIP_MIN_FREE_GB = 5.0   # don't start a new rip if <5 GB free on /mnt/tv


def _mediathek_rip_file(vuuid):
    """Absolute path for the ripped MP4 of a virtual recording."""
    return HLS_DIR / f"_{vuuid}" / "file.mp4"


def _rip_mediathek(vuuid):
    """Run ffmpeg to fetch the Mediathek HLS stream into a local MP4.
    Copy-mux only (no re-encode). Records success / failure back in
    the state dict."""
    with _mediathek_rec_lock:
        entry = dict(_mediathek_rec.get(vuuid) or {})
    hls_url = entry.get("hls_url")
    if not hls_url:
        return False
    out_dir = HLS_DIR / f"_{vuuid}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "file.mp4"
    tmp_file = out_dir / "file.mp4.part"
    print(f"[mt-rip {vuuid[:10]}] starting → {out_file}", flush=True)
    base = ["nice", "-n", "15", "ffmpeg", "-y",
             "-hide_banner", "-loglevel", "warning",
             "-i", hls_url,
             "-bsf:a", "aac_adtstoasc",
             "-movflags", "+faststart",
             "-f", "mp4", str(tmp_file)]
    # First pass: pure -c copy (fastest, lossless). If that rejects the
    # stream (sometimes MP4 muxer complains about ADTS headers or
    # private-stream metadata), retry with audio re-encoded to AAC.
    attempts = [
        ["-c", "copy"],
        ["-c:v", "copy", "-c:a", "aac", "-b:a", "192k"],
    ]
    r = None
    for opts in attempts:
        cmd = base[:8] + opts + base[8:]
        # Drop any leftover .part from previous attempt
        try: tmp_file.unlink()
        except Exception: pass
        try:
            r = subprocess.run(cmd, timeout=3600, check=False,
                               stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE)
        except Exception as e:
            print(f"[mt-rip {vuuid[:10]}] exception: {e}", flush=True)
            return False
        if (r.returncode == 0 and tmp_file.exists()
            and tmp_file.stat().st_size >= 1_000_000):
            break
        err_short = (r.stderr or b"")[-200:].decode("utf-8", "replace").strip()
        print(f"[mt-rip {vuuid[:10]}] copy attempt failed "
              f"(rc={r.returncode}), retrying with audio re-encode: "
              f"{err_short}", flush=True)
    if r.returncode != 0 or not tmp_file.exists() \
       or tmp_file.stat().st_size < 1_000_000:
        err = (r.stderr or b"")[-500:].decode("utf-8", "replace").strip()
        print(f"[mt-rip {vuuid[:10]}] failed rc={r.returncode} {err}",
              flush=True)
        with _mediathek_rec_lock:
            if vuuid in _mediathek_rec:
                _mediathek_rec[vuuid]["rip_error"] = err[:200] or "unknown"
                _mediathek_rec[vuuid]["rip_attempt_at"] = int(time.time())
        save_mediathek_rec()
        try: tmp_file.unlink()
        except Exception: pass
        return False
    tmp_file.rename(out_file)
    size = out_file.stat().st_size
    with _mediathek_rec_lock:
        if vuuid in _mediathek_rec:
            _mediathek_rec[vuuid]["ripped_path"] = str(out_file)
            _mediathek_rec[vuuid]["ripped_at"] = int(time.time())
            _mediathek_rec[vuuid]["ripped_bytes"] = size
            _mediathek_rec[vuuid].pop("rip_error", None)
    save_mediathek_rec()
    print(f"[mt-rip {vuuid[:10]}] done {size/1e6:.1f} MB", flush=True)
    return True


def _mediathek_rip_loop():
    time.sleep(120)
    while True:
        try:
            now = time.time()
            try:
                load = os.getloadavg()[0]
            except Exception:
                load = 0
            if load <= RIP_LOAD_CAP:
                # Disk-full guard — don't start a new multi-GB rip if
                # the SSD is almost full. 5 GB floor so in-flight
                # recordings + tvheadend DVR still have room.
                try:
                    free_gb = shutil.disk_usage(HLS_DIR).free / (1024 ** 3)
                except Exception:
                    free_gb = 0
                if free_gb < RIP_MIN_FREE_GB:
                    print(f"mt-rip: only {free_gb:.1f} GB free, "
                          f"deferring", flush=True)
                    time.sleep(3600)
                    continue
                with _mediathek_rec_lock:
                    todo = [
                        u for u, v in _mediathek_rec.items()
                        if not v.get("ripped_path")
                        and v.get("available_to")
                        and (v["available_to"] - now) < RIP_THRESHOLD_SECONDS
                        and (v["available_to"] - now) > 60  # still playable
                        # don't retry a recent failure within 6 h
                        and (now - v.get("rip_attempt_at", 0)) > 6 * 3600
                    ]
                if todo:
                    # One at a time to keep disk I/O polite.
                    _rip_mediathek(todo[0])
        except Exception as e:
            print(f"mt-rip loop: {e}", flush=True)
        time.sleep(3600)


def _mediathek_autorec_once():
    """One pass: for every autorec-spawned DVR entry on a Mediathek-
    covered channel that we don't yet have a virtual for, attempt the
    match and schedule a virtual recording. The rip loop then picks
    it up before expiry."""
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=500",
            timeout=10).read())
    except Exception as e:
        print(f"mt-autorec fetch: {e}", flush=True)
        return
    with _mediathek_rec_lock:
        existing = {m.get("tvh_entry") for m in _mediathek_rec.values()
                    if m.get("tvh_entry")}
    import uuid as uuid_mod
    created = 0
    for e in data.get("entries", []):
        if not e.get("autorec"):
            continue
        tvh_uuid = e.get("uuid")
        if not tvh_uuid or tvh_uuid in existing:
            continue
        ch_name = e.get("channelname") or ""
        slug = slugify(ch_name)
        if (slug not in ARD_SEARCH_CHANNELS
            and slug not in ZDF_SEARCH_CHANNELS):
            continue
        title = e.get("disp_title") or e.get("title") or ""
        start = e.get("start", 0)
        stop = e.get("stop", 0)
        if not title or not start:
            continue
        match = _mediathek_match(title, slug, start)
        if not match or not match.get("id"):
            continue
        hls_url = _resolve_mediathek_hls(
            match["id"], source=match.get("source", "ard"))
        if not hls_url:
            continue
        vuuid = "mt_" + uuid_mod.uuid4().hex[:16]
        with _mediathek_rec_lock:
            _mediathek_rec[vuuid] = {
                "title": match.get("title") or title,
                "channel": ch_name,
                "start": start,
                "stop": stop,
                "hls_url": hls_url,
                "available_to": match.get("available_to", 0),
                "eid": f"autorec_{tvh_uuid}",
                "tvh_entry": tvh_uuid,
                "autorec": e.get("autorec"),
                "created_at": int(time.time()),
            }
        save_mediathek_rec()
        created += 1
        print(f"[mt-autorec] scheduled virtual for "
              f"'{title}' on {ch_name} → {vuuid}", flush=True)
    if created:
        print(f"[mt-autorec] pass complete: {created} virtual(s) added",
              flush=True)


def _mediathek_autorec_loop():
    time.sleep(300)   # let tvheadend populate its upcoming list
    while True:
        try:
            _mediathek_autorec_once()
        except Exception as e:
            print(f"mt-autorec loop: {e}", flush=True)
        time.sleep(3600)


def _zdf_search(title):
    """Search ZDF Mediathek for episodes matching `title`. Returns a
    list of dicts {id, title, broadcast_ts, available_to, duration}."""
    qs = urllib.parse.urlencode({
        "q": title, "contentTypes": "episode", "limit": "24",
    })
    url = f"https://api.zdf.de/search/documents?{qs}"
    req = urllib.request.Request(url, headers={
        "api-auth": f"Bearer {ZDF_API_TOKEN}",
    })
    from datetime import datetime
    out = []
    for attempt in range(2):
        try:
            data = json.loads(urllib.request.urlopen(req, timeout=15).read())
            for r in data.get("http://zdf.de/rels/search/results", []):
                t = r.get("http://zdf.de/rels/target", {})
                ed = t.get("editorialDate") or ""
                bt = 0
                if ed:
                    try:
                        bt = int(datetime.fromisoformat(
                            ed.replace("Z", "+00:00")).timestamp())
                    except Exception:
                        pass
                out.append({
                    "id": t.get("id"),
                    "title": t.get("teaserHeadline") or t.get("title") or "",
                    "broadcast_ts": bt,
                    "available_to": 0,   # filled in from item details on demand
                    "duration": t.get("duration", 0),
                })
            return out
        except Exception as e:
            print(f"zdf search (try {attempt+1}): {e}", flush=True)
    return out


def _zdf_resolve_hls(canonical_id):
    """Given a ZDF search id (e.g. 'heute-journal-vom-19-april-2026-100'),
    walk content doc → ptmd template → manifest → HLS URL. Also
    returns the `visibleTo` expiry so the caller can use it as
    available_to if the search result didn't have it."""
    req = urllib.request.Request(
        f"https://api.zdf.de/content/documents/{canonical_id}.json",
        headers={"api-auth": f"Bearer {ZDF_API_TOKEN}"})
    try:
        item = json.loads(urllib.request.urlopen(req, timeout=15).read())
    except Exception as e:
        print(f"zdf item: {e}", flush=True)
        return None, 0
    mvc = (item.get("mainVideoContent") or {}).get(
        "http://zdf.de/rels/target") or {}
    tmpl = mvc.get("http://zdf.de/rels/streams/ptmd-template", "")
    vis_to = 0
    from datetime import datetime
    if mvc.get("visibleTo"):
        try:
            vis_to = int(datetime.fromisoformat(
                mvc["visibleTo"].replace("Z", "+00:00")).timestamp())
        except Exception:
            pass
    if not tmpl:
        return None, vis_to
    ptmd_url = ("https://api.zdf.de"
                + tmpl.replace("{playerId}", "ngplayer_2_4"))
    req = urllib.request.Request(ptmd_url, headers={
        "api-auth": f"Bearer {ZDF_API_TOKEN}"})
    try:
        ptmd = json.loads(urllib.request.urlopen(req, timeout=15).read())
    except Exception as e:
        print(f"zdf ptmd: {e}", flush=True)
        return None, vis_to
    # Walk priorityList → formitaeten → qualities → audio/tracks → uri
    import re
    hls_url = None
    for pri in ptmd.get("priorityList", []):
        for f in pri.get("formitaeten", []):
            if f.get("type") != "hls":
                continue
            for q in f.get("qualities", []):
                for a in q.get("audio", {}).get("tracks", []):
                    u = a.get("uri") or ""
                    if ".m3u8" in u:
                        hls_url = u
                        break
                if hls_url: break
            if hls_url: break
        if hls_url: break
    if not hls_url:
        # Fallback: scan the whole response for any m3u8
        m = re.search(r'https?://[^"\s]+\.m3u8[^"\s]*', json.dumps(ptmd))
        if m:
            hls_url = m.group(0)
    return hls_url, vis_to


def _resolve_mediathek_hls(item_id, source="ard"):
    """Resolve an HLS master URL for a Mediathek match. Dispatches on
    `source`: ARD uses the page-gateway item endpoint, ZDF uses the
    PTMD manifest chain. ARD's item endpoint is occasionally slow
    (>6 s); retry once on timeout."""
    if source == "zdf":
        hls, _ = _zdf_resolve_hls(item_id)
        return hls
    url = (f"https://api.ardmediathek.de/page-gateway/pages/ard/"
           f"item/{item_id}?devicetype=pc&embedded=true")
    for attempt in range(2):
        try:
            data = json.loads(
                urllib.request.urlopen(url, timeout=15).read())
            # Scan every stream/media entry for a .m3u8 URL — the order
            # isn't guaranteed across shows.
            for stream in (data.get("widgets", [{}])[0]
                               .get("mediaCollection", {})
                               .get("embedded", {})
                               .get("streams", [])):
                for m in stream.get("media", []):
                    u = m.get("url", "")
                    if ".m3u8" in u:
                        return u
            # Fallback: any forcedLabel "Auto" entry even without .m3u8
            for stream in (data.get("widgets", [{}])[0]
                               .get("mediaCollection", {})
                               .get("embedded", {})
                               .get("streams", [])):
                for m in stream.get("media", []):
                    if m.get("forcedLabel") == "Auto":
                        return m.get("url")
            return None
        except Exception as e:
            print(f"mediathek hls resolve (try {attempt+1}): {e}",
                  flush=True)
    return None


def load_live_ads():
    if not LIVE_ADS_FILE.exists():
        return
    try:
        data = json.loads(LIVE_ADS_FILE.read_text())
        # Drop entries older than 2 h — the ad blocks' wall times would
        # point outside any live buffer window anyway.
        cutoff = time.time() - 2 * 3600
        with _live_ads_lock:
            _live_ads.update({
                s: v for s, v in data.items()
                if v.get("generated", 0) > cutoff
            })
        print(f"Loaded live-ads for {len(_live_ads)} channels", flush=True)
    except Exception as e:
        print(f"load live-ads: {e}", flush=True)


def save_live_ads():
    try:
        with _live_ads_lock:
            snap = {s: dict(v) for s, v in _live_ads.items()}
        LIVE_ADS_FILE.write_text(json.dumps(snap))
    except Exception as e:
        print(f"save live-ads: {e}", flush=True)


# Only scan channels where commercials are plausible — public
# broadcasters have no regular ads primetime.
LIVE_ADSKIP_SLUGS = {
    "prosieben", "sat-1", "kabel-eins", "kabeleins", "prosiebenmaxx",
    "rtl", "vox", "rtlzwei", "nitro", "rtlup", "superrtl",
    "tele-5", "tele5", "sport1", "dmax", "sixx",
}


def _live_ads_payload(slug):
    """Snapshot of {ads, generated} for a channel. Reads the JSON file
    under LIVE_ADS_OFFLOAD (the authoritative writer is another host),
    or the in-memory cache otherwise."""
    if os.environ.get("LIVE_ADS_OFFLOAD") and LIVE_ADS_FILE.exists():
        try:
            data = json.loads(LIVE_ADS_FILE.read_text())
            v = data.get(slug)
        except Exception:
            v = None
    else:
        with _live_ads_lock:
            v = _live_ads.get(slug)
    if not v:
        return {"ads": [], "generated": 0}
    return {"ads": v["ads"], "generated": int(v["generated"])}


@app.route("/api/live-ads/<slug>")
def api_live_ads(slug):
    return _cors(Response(json.dumps(_live_ads_payload(slug)),
                           mimetype="application/json"))


@app.route("/api/live-ads-stream/<slug>")
def api_live_ads_stream(slug):
    """SSE push: emit a fresh ad payload whenever .live_ads.json
    changes on disk (= the Mac scanner just saved a new scan result).
    Replaces the player's 30 s polling loop so the skip button shows
    up the moment a new ad is detected, not 0-30 s later. Heartbeat
    every 20 s keeps long-lived connections alive through Caddy /
    cellular proxies. mtime-poll cadence 1 s — file is a few kB, OS
    caches it, the cost is negligible.
    Each connection holds a waitress worker thread for its lifetime;
    with the default pool of 4 we can hold 4 concurrent player tabs
    before /api/* requests start queuing. Plenty for home use."""
    # Prewarm refresh removed 2026-05-30 (slice 5): tv-receiver now owns
    # adjacency-prewarm, driven by its own /api/app/live switch hook. The
    # gateway no longer posts to /api/prewarm (dual-writer gone). This SSE
    # still streams the live-ads payload, unchanged.
    def gen():
        last_mtime = -1
        last_payload = None
        last_heartbeat = time.time()
        # Send the current state immediately on connect so the client
        # doesn't wait for the first file change to render.
        try:
            if LIVE_ADS_FILE.exists():
                last_mtime = LIVE_ADS_FILE.stat().st_mtime
            payload = _live_ads_payload(slug)
            last_payload = json.dumps(payload)
            yield f"data: {last_payload}\n\n"
        except Exception:
            pass
        while True:
            time.sleep(1)
            try:
                if LIVE_ADS_FILE.exists():
                    mt = LIVE_ADS_FILE.stat().st_mtime
                    if mt != last_mtime:
                        last_mtime = mt
                        payload = _live_ads_payload(slug)
                        msg = json.dumps(payload)
                        if msg != last_payload:
                            last_payload = msg
                            yield f"data: {msg}\n\n"
                            last_heartbeat = time.time()
                            continue
            except Exception:
                pass
            now = time.time()
            if now - last_heartbeat > 20:
                yield ":hb\n\n"
                last_heartbeat = now
    return Response(gen(),
                     mimetype="text/event-stream",
                     headers={"Cache-Control": "no-cache",
                              "X-Accel-Buffering": "no",
                              "Access-Control-Allow-Origin": "*"})


def _rec_prewarm_loop():
    """Background: remux finished DVR recordings to HLS so the player
    doesn't have to wait. Serial, low priority, skipped while live
    streams are active so we never starve live TV of CPU.
    120 s cadence (was 300 s — tightened back up now that the prewarm
    is the actual eager-remux path, not a nice-to-have). The
    previously-suspect tvh poll is fine at this rate; the table
    parser overload was a separate code path. On-demand remux still
    triggers via /recording/<uuid>/ads as a safety net."""
    time.sleep(20)  # give the service a moment to settle on startup
    while True:
        try:
            _rec_prewarm_once()
        except Exception as e:
            print(f"[rec-prewarm] error: {e}", flush=True)
        time.sleep(120)


def _mac_comskip_alive():
    """True if the Mac-side tv-comskip.sh launchd agent has touched
    the heartbeat file in the last 5 min. Default-skip the Pi's
    rec-comskip path while the Mac is alive — both can do the work
    but Mac's M-series CPU is much faster and the Pi has live-stream
    ffmpeg + remuxing to keep up with.

    Always returns False when DETECT_OFFLOAD=mac — that path uses
    the new HTTP daemon (which gets fed via `.detect-requested`
    markers from `_rec_cskip_spawn`) and would deadlock if we
    deferred-and-then-skipped the marker creation."""
    if DETECT_OFFLOAD == "mac":
        return False
    try:
        hb = HLS_DIR / ".mac-comskip-alive"
        return hb.exists() and time.time() - hb.stat().st_mtime < 300
    except Exception:
        return False


def _rec_prewarm_once():
    # Head.bin mtime tracking — when nightly retrain produces a new
    # head, we want every existing recording's ads.json to be
    # re-generated so the new model's predictions propagate. Drop
    # all non-sidecar .txt cutlists + write .detect-requested
    # markers; daemon serially re-detects via DETECT_OFFLOAD path.
    if DETECT_OFFLOAD == "mac":
        head_path = HLS_DIR / ".tvd-models" / "head.bin"
        marker = HLS_DIR / ".tvd-models" / ".last-head-mtime"
        try:
            cur_mt = int(head_path.stat().st_mtime) if head_path.exists() else 0
            last_mt = int(marker.read_text()) if marker.exists() else 0
        except Exception:
            cur_mt = last_mt = 0
        # First-run case: marker missing → just record current mtime,
        # don't invalidate (we don't know if anything actually changed
        # since last container restart). This avoids a full
        # re-detection of every recording every time the gateway
        # restarts. Real head-changes (mtime drift while gateway is
        # running) still trigger the invalidate path.
        if cur_mt and last_mt == 0:
            try: marker.write_text(str(cur_mt))
            except Exception: pass
        elif cur_mt and cur_mt != last_mt:
            # V2 Smart-Invalidate (revised 2026-05-12): only invalidate
            # recordings that ACTUALLY benefit from the new head — test-
            # set (= eval feedback) + recordings <14 days old (= where
            # users still actively browse). Older completed recordings
            # keep their existing cutlist; user can /redetect manually
            # if a particular old one matters. Net: ~40-60 markers per
            # head deploy instead of 300+, drain finishes in ~2-3h
            # instead of 12-14h, no permanent backlog.
            test_uuids = None
            ts_path = HLS_DIR / ".tvd-models" / "head.test-set.json"
            if ts_path.is_file():
                try:
                    test_uuids = set(
                        json.loads(ts_path.read_text()).get("uuids", []))
                except Exception:
                    test_uuids = None
            # Build uuid → start_ts map for the recency check.
            uuid_start = {}
            try:
                dvr = json.loads(urllib.request.urlopen(
                    f"{dvr_base()}/api/dvr/entry/grid?limit=2000",
                    timeout=10).read())
                for e in dvr.get("entries", []):
                    if e.get("uuid"):
                        uuid_start[e["uuid"]] = e.get("start", 0)
            except Exception:
                pass
            recent_cutoff = time.time() - 14 * 86400
            scope = (f"test-set ({len(test_uuids)}) + recordings <14d"
                     if test_uuids is not None
                     else "<14d recordings (no test-set sidecar)")
            print(f"[rec-prewarm] head.bin changed "
                  f"({last_mt} → {cur_mt}) — Smart-V2 invalidating {scope}",
                  flush=True)
            n_high = n_low = n_skipped = n_skipped_reviewed = 0
            for d in HLS_DIR.glob("_rec_*"):
                uuid = d.name[5:]
                is_test = (test_uuids is not None and uuid in test_uuids)
                start_ts = uuid_start.get(uuid, 0)
                is_recent = start_ts > recent_cutoff
                if not is_test and not is_recent and test_uuids is not None:
                    # Smart-V2: skip old non-test recordings entirely
                    n_skipped += 1
                    continue
                # Without test_uuids sidecar, fallback to "all recordings
                # < 14d" rather than dumping everything; legacy V1 of
                # "all recordings high-prio" was the runaway case.
                if test_uuids is None and not is_recent:
                    n_skipped += 1
                    continue
                # Smart-V2 extension (2026-05-12): even within recent +
                # test-set, skip user-reviewed recordings — their
                # ads_user.json is authoritative + smart-merge in the
                # /recording/<uuid>/ads response combines them with auto
                # blocks. Re-running detect on those gives no new info
                # the user actually consumes (= they've already labeled
                # boundaries). Saves ~50% of drain since most reviewed
                # corpus is the bulk of the test-set.
                ads_user_p = d / "ads_user.json"
                if ads_user_p.is_file() and ads_user_p.stat().st_size > 4:
                    try:
                        raw = json.loads(ads_user_p.read_text())
                        ads = raw.get("ads") if isinstance(raw, dict) else raw
                        if ads:
                            n_skipped_reviewed += 1
                            continue
                    except Exception:
                        pass
                # Truncate (don't delete) the cutlist .txt — its
                # FILENAME encodes the recording basename, which the
                # train-head loader uses to find the .ts source. Daemon
                # overwrites the content on next detect cycle anyway,
                # so empty file is fine. Skip sidecar caches.
                for t in d.glob("*.txt"):
                    if any(t.name.endswith(s) for s in
                           (".logo.txt", ".cskp.txt", ".tvd.txt",
                            ".trained.logo.txt")):
                        continue
                    try: t.write_text("")
                    except Exception: pass
                ads_p = d / "ads.json"
                if ads_p.exists():
                    try: ads_p.unlink()
                    except Exception: pass
                # Write the appropriate marker so the Mac daemon picks
                # this up. No-op in Pi-local mode where the prewarm
                # loop spawns tv-detect directly without markers.
                if DETECT_OFFLOAD == "mac":
                    name = ".detect-requested" if is_test else ".detect-requested-low"
                    try: (d / name).write_text(
                        json.dumps({"ts": time.time()}))
                    except Exception: pass
                if is_test: n_high += 1
                else:       n_low  += 1
            try: marker.write_text(str(cur_mt))
            except Exception: pass
            print(f"[rec-prewarm] Smart-V2 invalidated "
                  f"{n_high} high-prio + {n_low} low-prio "
                  f"(skipped {n_skipped} old/non-test, "
                  f"{n_skipped_reviewed} user-reviewed) — daemon picks "
                  f"high first, low only when idle", flush=True)
    # Load-based gate. The dominant cost in this loop is the
    # MPEG-2 -> H.264 transcode for VOD playback (libx264 ultrafast
    # eats 1.5-2.5 cores on its own). nice 15 helps prevent it from
    # preempting live transcodes, but the scheduler still has to
    # service them — once loadavg passes ~3.0 on this 4-core box
    # users notice live-stream segment stalls. Keep the gate well
    # below saturation so prewarm waits for genuine slack.
    try:
        if os.getloadavg()[0] > 3.0:
            return
    except Exception:
        pass
    # Skip while any remux is already in progress
    with _rec_hls_lock:
        for info in _rec_hls_procs.values():
            if info["proc"].poll() is None:
                return
    try:
        # limit=500 because tvh's default sort isn't newest-first;
        # at limit=100 with 222+ finished entries newer recordings
        # could fall off the end (= why Bull/Charmed/Galileo Stories
        # of 04.05 sat as "ausstehend" indefinitely 2026-05-04 — they
        # were positions 130/180/210 of 222 in tvh's default sort,
        # invisible to prewarm, only spawned when the user hit
        # /recording/<uuid>/index.m3u8 manually). 500 covers any
        # realistic single-corpus size; the response is small JSON
        # (~200 KB) so the over-fetch costs nothing.
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid_finished?limit=500",
            timeout=10).read())
    except Exception:
        return

    # Garbage-collect orphaned _rec_<uuid> dirs: tvheadend's native UI
    # deletes the .ts file but doesn't know about our HLS cache, so
    # entries deleted there leave stale dirs sitting on disk forever.
    # Build the set of currently-known DVR uuids (finished + scheduled
    # + recording), then remove any _rec_<uuid> not in it.
    known_uuids = {e["uuid"] for e in data.get("entries", []) if e.get("uuid")}
    try:
        all_data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=500", timeout=10).read())
        for e in all_data.get("entries", []):
            if e.get("uuid"):
                known_uuids.add(e["uuid"])
    except Exception:
        pass
    if known_uuids:  # only purge if we actually got a real list
        for d in HLS_DIR.glob("_rec_*"):
            if not d.is_dir(): continue
            uuid = d.name[len("_rec_"):]
            if uuid in known_uuids: continue
            # Skip if any cleanup might race with our own work
            if uuid in _rec_hls_procs or uuid in _rec_cskip_procs: continue
            # SAFETY: never delete a dir that contains user-labeled
            # training data. Bug 2026-05-27: after the tvh→tv-receiver
            # migration, tvh's hex UUIDs were rewritten to
            # `dvr-<slug>-<epoch>`, but legacy `_rec_<hex-uuid>` dirs
            # on disk still used the old hex UUIDs → known_uuids didn't
            # match → 485 historical recordings (incl. 230 ads_user.json
            # user-labels) got purged in one GC pass. Preserve any dir
            # with manual labels even if its UUID isn't in the schedule
            # store — those represent training data that can't be
            # regenerated.
            if (d / "ads_user.json").exists():
                continue
            print(f"[rec-prewarm] gc orphan {uuid[:8]}", flush=True)
            shutil.rmtree(d, ignore_errors=True)

    # Per-uuid cooldown so a uuid that fails to produce a cutlist
    # doesn't permanently steal the cskip slot every cycle (= the
    # bug where ef6ad633 got cskip-spawned 6× in a row while
    # bd6f080b never got reached because each cycle returned at
    # the first empty-cutlist match). 10 min cooldown gives the
    # Mac daemon enough time to actually write a cutlist back.
    PREWARM_PER_UUID_COOLDOWN_S = 600
    if not hasattr(_rec_prewarm_once, "_last_spawn"):
        _rec_prewarm_once._last_spawn = {}
    last_spawn = _rec_prewarm_once._last_spawn
    now = time.time()
    # Cap on concurrent spawn-issuances per cycle so we don't burst
    # 100+ markers at once after a head-deploy invalidation.
    MAX_HLS_PER_CYCLE = 3
    MAX_CSKIP_PER_CYCLE = 3
    n_hls_spawned = n_cskip_spawned = 0
    for e in data.get("entries", []):
        uuid = e.get("uuid")
        if not uuid or not e.get("filename"):
            continue
        out_dir = HLS_DIR / f"_rec_{uuid}"
        playlist = out_dir / "index.m3u8"
        if not playlist.exists():
            if n_hls_spawned >= MAX_HLS_PER_CYCLE:
                continue
            if (now - last_spawn.get(("hls", uuid), 0)
                    < PREWARM_PER_UUID_COOLDOWN_S):
                continue
            # Source-existence guard: the schedule store keeps "completed"
            # entries even after their .ts is deleted (disk eviction, or a
            # manual delete that didn't prune the schedule). Without this
            # check prewarm queues a remux every cooldown window, the Mac
            # fetches /source, gets 404, fails — an endless loop that
            # clogs the HLS queue (observed 2026-05-28). Skip queueing
            # when the source is gone; the entry just stays HLS-less,
            # harmless. A .source-recovered.ts in the HLS dir still
            # counts as a valid source.
            container_fn = _host_to_container(e.get("filename") or "")
            recovered = out_dir / ".source-recovered.ts"
            if not (container_fn and Path(container_fn).is_file()) \
                    and not recovered.is_file():
                continue
            # Pi-side eager remux — Mac daemon polls the marker
            # via /api/internal/hls-pending and POSTs back the
            # tarball. Each spawn is just a marker-write; the work
            # itself is async on the Mac.
            print(f"[rec-prewarm] remuxing {uuid[:8]} "
                  f"({e.get('disp_title', '?')[:40]})", flush=True)
            _rec_hls_spawn(uuid)
            _rec_thumbs_spawn(uuid)
            last_spawn[("hls", uuid)] = now
            n_hls_spawned += 1
            continue
        # Playlist exists — ensure thumbs got triggered too
        if not (out_dir / "thumbs" / ".done").exists() and \
           not (out_dir / "thumbs" / ".requested").exists():
            _rec_thumbs_spawn(uuid)
        # HLS already present — fill in the ad markers if missing.
        if not list(out_dir.glob("*.txt")):
            if n_cskip_spawned >= MAX_CSKIP_PER_CYCLE:
                continue
            if (now - last_spawn.get(("cskip", uuid), 0)
                    < PREWARM_PER_UUID_COOLDOWN_S):
                continue
            if _mac_comskip_alive():
                continue
            scanning = out_dir / ".scanning"
            try:
                fresh_lock = (scanning.exists()
                              and time.time() - scanning.stat().st_mtime < 900)
            except Exception:
                fresh_lock = False
            if fresh_lock:
                continue
            cskip_info = _rec_cskip_procs.get(uuid)
            if not (cskip_info and cskip_info["proc"].poll() is None):
                print(f"[rec-prewarm] comskip {uuid[:8]}", flush=True)
                _rec_cskip_spawn(uuid)
                last_spawn[("cskip", uuid)] = now
                n_cskip_spawned += 1


def _rec_playlist_as_vod(text, is_running=False):
    """Convert an EVENT-type playlist into VOD with ENDLIST so iOS
    shows a scrub bar and starts from the beginning instead of live-edge.
    When the remux is still running, keep EVENT type and omit ENDLIST
    so iOS treats the playlist as growing and keeps polling."""
    if is_running:
        return text
    text = text.replace("#EXT-X-PLAYLIST-TYPE:EVENT",
                        "#EXT-X-PLAYLIST-TYPE:VOD")
    if "#EXT-X-PLAYLIST-TYPE" not in text:
        text = text.replace("#EXT-X-TARGETDURATION",
                            "#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-TARGETDURATION",
                            1)
    if "#EXT-X-ENDLIST" not in text:
        text = text.rstrip() + "\n#EXT-X-ENDLIST\n"
    return text


def _rec_state(uuid):
    """Internal: ffmpeg state + segment progress for a recording uuid.

    Treats 0-byte index.m3u8 as missing — Mac-side training-snapshot
    creates empty markers and a tar restore can drop those onto the
    Pi (incident 2026-05-27: 230 empty placeholders blocked playback
    for every old recording until they were deleted). `playlist.exists()`
    alone is not enough.
    """
    playlist = HLS_DIR / f"_rec_{uuid}" / "index.m3u8"
    info = _rec_hls_procs.get(uuid)
    running = info is not None and info["proc"].poll() is None
    segs = 0
    real = False
    try:
        if playlist.is_file() and playlist.stat().st_size > 0:
            real = True
            segs = playlist.read_text().count(".ts")
    except Exception:
        pass
    # "done" means: real playlist exists AND ffmpeg is not currently
    # running. If ffmpeg was never started in this process lifetime
    # but a full playlist is on disk from a prior run → also treated
    # as done.
    done = real and not running
    # `playlist` attribute kept for compatibility — callers can still
    # check .exists(), but should prefer the `real` flag for "is this
    # a real playlist or just a 0-byte placeholder".
    total = (info or {}).get("total_segs", 0)
    return {"done": done, "segments": segs, "total": total,
            "running": running, "playlist": playlist, "real": real}


@app.route("/recording/<uuid>/index.m3u8")
def recording_hls(uuid):
    """VOD playlist endpoint. Spawns ffmpeg in the background on first
    call; serves the growing playlist as soon as ~10 segments are
    ready (EVENT type so iOS keeps polling), then switches to VOD
    with ENDLIST once the remux finishes.

    Rewrites segment URIs from absolute `/hls/_rec_<uuid>/seg_*.ts`
    (what ffmpeg writes) to bare filenames so RFC 8216 §4.1 relative-
    URI resolution lands on `/recording/<uuid>/seg_*.ts` — handled by
    recording_segment() below. iOS Safari accepts absolute paths but
    mpv/ffmpeg-based players treat them as filesystem paths and fail."""
    st = _rec_state(uuid)
    if not st["real"] and not st["running"]:
        # Clean up any 0-byte placeholder so ffmpeg can start fresh.
        try:
            if st["playlist"].exists():
                st["playlist"].unlink()
        except Exception:
            pass
        _rec_hls_spawn(uuid)
        st = _rec_state(uuid)
    # Wait for at least 10 segments OR the remux to actually be done.
    # The 10-segment threshold gives iOS some buffer when streaming
    # mid-prepare, but very short recordings (sub-30s sitcom remnants
    # like duplicate tail-recordings) only ever reach 5 segments —
    # we'd 202 forever without the !running fallback.
    playlist_ready = st["playlist"].exists() and (
        st["segments"] >= 10 or not st["running"])
    if not playlist_ready:
        return Response(json.dumps({"done": False,
                                     "segments": st["segments"],
                                     "total": st["total"]}),
                        status=202, mimetype="application/json")
    raw = st["playlist"].read_text()
    raw = raw.replace(f"/hls/_rec_{uuid}/", "")
    return Response(_rec_playlist_as_vod(raw, is_running=st["running"]),
                    mimetype="application/vnd.apple.mpegurl")


@app.route("/recording/<uuid>/<filename>")
def recording_segment(uuid, filename):
    """Serve HLS .ts segments under the same URL prefix as the playlist
    so bare relative URIs in the manifest resolve correctly for any
    RFC 8216 §4.1-compliant player (mpv, ffmpeg, ExoPlayer, AVPlayer).
    The same bytes are also reachable at /hls/_rec_<uuid>/seg_*.ts
    via Caddy's file_server short-circuit — this route is the relative-
    URI-friendly alias. Guards against non-segment filenames so it
    doesn't shadow other /recording/<uuid>/* routes."""
    if not (filename.startswith("seg_") and filename.endswith(".ts")):
        abort(404)
    seg = HLS_DIR / f"_rec_{uuid}" / filename
    if not seg.is_file():
        abort(404)
    return send_file(seg, mimetype="video/mp2t",
                     conditional=True, max_age=3600)


def _read_user_ads(user_cache):
    """Read ads_user.json. Backward-compat: supports both legacy
    list-of-pairs format ([[s,e], ...]) and the dict format
    ({"ads": [...], "deleted": [...]}) used since the smart-merge
    rewrite. Returns (user_ads, deleted_auto)."""
    if not user_cache.exists():
        return [], []
    try:
        raw = json.loads(user_cache.read_text())
    except Exception:
        return [], []
    if isinstance(raw, list):
        return raw, []
    if isinstance(raw, dict):
        return raw.get("ads", []) or [], raw.get("deleted", []) or []
    return [], []


def _smart_merge_ads(auto, user, deleted):
    """Merge auto-detected blocks with user edits.

    Semantics:
      * any auto block overlapping a user block is dropped (user
        version wins — covers boundary refinements)
      * any auto block overlapping an explicit deletion is dropped
        (covers false-positive removal that survives re-scans)
      * remaining auto blocks + all user blocks form the result

    Overlap = the two intervals share any time. Float comparison
    uses no tolerance — adjacent (touching) blocks don't count as
    overlapping; that mirrors the existing `b > a` vs `>=` semantics
    in the parser."""
    def overlaps(a, b):
        return a[0] < b[1] and b[0] < a[1]
    surviving = [a for a in auto
                 if not any(overlaps(a, x) for x in user)
                 and not any(overlaps(a, d) for d in deleted)]
    out = sorted(surviving + list(user), key=lambda b: b[0])
    return out


def _overrun_block(uuid, duration_s, deleted):
    """If the recording extends past its scheduled EPG end (= broadcast
    padding captured the NEXT show), return [scheduled_end_s,
    duration_s] as a synthetic ad-block so the player skips the
    overrun. Returns None when there's no overrun, padding wasn't
    used, or the user has deleted any block whose start falls inside
    the candidate overrun region (= they reviewed it and want to keep
    the next-show content)."""
    if not duration_s or duration_s < 60:
        return None
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=1000", timeout=5).read())
    except Exception:
        return None
    entry = next((e for e in data.get("entries", [])
                  if e.get("uuid") == uuid), None)
    if not entry:
        return None
    start_real = entry.get("start_real")
    sched_stop = entry.get("stop")
    if not start_real or not sched_stop:
        return None
    overrun_start_s = float(sched_stop - start_real)
    # Need ≥30 s of overrun to bother (avoid noise on perfectly-timed
    # recordings that exit 5-10 s past schedule).
    if overrun_start_s >= duration_s - 30 or overrun_start_s < 60:
        return None
    # Respect explicit user deletions: any deleted block whose start
    # falls in the candidate overrun = user kept the next-show content.
    for d in deleted or []:
        try:
            ds = float(d[0]) if isinstance(d, list) else float(d.get("start", 0))
            if overrun_start_s <= ds < duration_s:
                return None
        except Exception:
            pass
    return [round(overrun_start_s, 2), round(duration_s, 2)]


@app.route("/recording/<uuid>/ads")
def recording_ads(uuid):
    """Commercial-block markers for the scrub bar.

    Returns the smart-merged view of auto-detected blocks
    (`ads.json`, regenerated from the comskip/tv-detect cutlist) and
    the user's manual edits (`ads_user.json` — refined boundaries,
    additions, and explicit deletions of false positives). User
    blocks override overlapping auto blocks; user deletions suppress
    overlapping auto blocks across re-scans.

    Response includes both the merged `ads` (what the player should
    render) and the raw `auto` set so the editor UI can tell which
    blocks came from auto-detection vs the user — essential for
    classifying a future delete-action as "remove user-block" vs
    "mark auto-block as false-positive"."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    cskip_info = _rec_cskip_procs.get(uuid)
    running = cskip_info is not None and cskip_info["proc"].poll() is None
    if not out_dir.exists():
        payload = {"ads": [], "auto": [], "user": [], "deleted": [],
                   "edited": False, "running": running}
        resp = _cors(Response(json.dumps(payload),
                              mimetype="application/json"))
        resp.headers["Cache-Control"] = "no-store"
        return resp

    user_cache = out_dir / "ads_user.json"
    user_ads, deleted = _read_user_ads(user_cache)
    edited = user_cache.exists()

    auto = []
    ads_cache = out_dir / "ads.json"
    SIDECAR = (".logo.txt", ".trained.logo.txt", ".cskp.txt", ".tvd.txt")
    txts = [p for p in out_dir.glob("*.txt")
            if not any(p.name.endswith(s) for s in SIDECAR)]
    txt_mtime = max((t.stat().st_mtime for t in txts), default=0)
    # Compute playable duration up-front: the comskip parser needs it to
    # sanity-check its frame→second conversion against interlaced
    # field-rate inflation (see _rec_parse_comskip).
    duration_s = _rec_playlist_duration(out_dir)
    if (not running and txts
            and ads_cache.exists()
            and ads_cache.stat().st_mtime >= txt_mtime):
        try:
            auto = json.loads(ads_cache.read_text())
        except Exception:
            auto = _rec_parse_comskip(out_dir, duration_s)
    else:
        auto = _rec_parse_comskip(out_dir, duration_s)
        if not running:
            # Blackframe-snap refinement spawns ffmpeg (blackdetect) per
            # ad-block. That violates the "all ffmpeg on the Mac" rule:
            # the Pi 5 has no HW encoder, and worse, the /ads endpoint is
            # polled by every client for every recording — uncached calls
            # fan out into an unbounded ffmpeg storm (load 113 on
            # 2026-05-28). tv-detect already applies --start-extend /
            # --end-extend boundary padding on the Mac, so the primary
            # boundaries are intact; the black-frame snap is a
            # second-order refinement we drop on the Pi. Set
            # BLACKFRAME_EXTEND=1 only on a host with a spare encoder.
            if auto and os.environ.get("BLACKFRAME_EXTEND", "0") == "1":
                src = _rec_source_path(uuid)
                if src and Path(src).exists():
                    auto = _blackframe_extend_ads(
                        src, auto, channel_slug=_rec_channel_slug(uuid))
            # Only cache when the .txt has the comskip "FILE
            # PROCESSING COMPLETE" header — that's the marker the
            # daemon writes ONLY after detect finishes. Without this
            # check a parallel /ads poll during a bulk-invalidate
            # window (.txt truncated to empty for a few seconds
            # before daemon re-detects) caches an empty result that
            # then sticks even after the daemon delivers real blocks
            # (since both .txt and cache mtimes update within the
            # same second on the next read). Truncated/empty .txt
            # → skip cache → next /ads call after daemon delivery
            # parses correctly.
            txt_has_marker = False
            try:
                if txts:
                    head = txts[0].read_text(errors="ignore")[:200]
                    txt_has_marker = "FILE PROCESSING COMPLETE" in head
            except Exception:
                pass
            if txt_has_marker:
                try:
                    ads_cache.write_text(json.dumps(auto))
                except Exception:
                    pass

    merged = _smart_merge_ads(auto, user_ads, deleted)
    # duration_s already computed above from the HLS playlist EXTINF sum
    # (needed early for the comskip fps sanity-check). The client uses it
    # to render the scrub bar immediately instead of waiting ~6 s for the
    # <video> loadedmetadata event.
    # Recording-overrun: tvh schedules with padding, so the .ts often
    # contains 30-600 s of NEXT show after the actual broadcast end.
    # The ad-detector can't tell — it sees continuous logo on the same
    # channel and classifies it as show. Synthesize an "overrun" block
    # spanning [scheduled_stop_in_recording, duration_s] and inject it
    # so the player auto-skips it the same way it skips ads.
    # Skipped if the user has explicitly deleted any block whose START
    # falls in the overrun region (= they reviewed and decided to keep
    # the next-show content). Skipped if scheduled_stop is missing or
    # past the recording end (= no padding was used).
    overrun = _overrun_block(uuid, duration_s, deleted)
    if overrun:
        merged = merged + [overrun]
    # Clamp blocks to the playable duration. comskip counts frames, and
    # on interlaced SD (e.g. ProSieben 720x576 50i) the frame→second
    # conversion uses the reported 25 fps while the real field cadence
    # inflates the frame index — so a block can land past the actual
    # recording length (witnessed 2026-05-28: block [2930,3471] on a
    # 2410 s recording). A client's skipAd would then seek past the end
    # of the video. Drop blocks that start at/after duration_s, clamp
    # ends down to it. Only when duration_s is known (>0); a not-yet-
    # remuxed playlist reports 0.0 and we leave blocks untouched.
    if duration_s and duration_s > 0:
        def _clamp(blocks):
            out = []
            for a, b in blocks:
                if a >= duration_s:
                    continue  # entirely past the end
                out.append([a, min(b, duration_s)])
            return out
        merged = _clamp(merged)
        if overrun:
            overrun = ([overrun[0], min(overrun[1], duration_s)]
                       if overrun[0] < duration_s else None)
    payload = {"ads": merged, "auto": auto, "user": user_ads,
               "deleted": deleted, "edited": edited, "running": running,
               "duration_s": round(duration_s, 3),
               "overrun": overrun,
               "uncertain": _uncertain_for_recording(uuid)}
    resp = _cors(Response(json.dumps(payload),
                          mimetype="application/json"))
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/recording/<uuid>/ads/edit", methods=["POST"])
def api_recording_ads_edit(uuid):
    """Persist a user-edited ad-block list. Body: {"ads": [[s,e], …]}.
    Stored as ads_user.json next to the recording's HLS dir; takes
    precedence over the auto-detected ads.json on subsequent reads
    so manual fixes survive any re-scan. POST with body {"ads": []}
    clears the override (auto-detection re-applies)."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    if not out_dir.exists():
        return Response(json.dumps({"ok": False,
                                      "error": "unknown recording"}),
                        status=404, mimetype="application/json")
    try:
        body = request.get_json(silent=True) or {}
        ads = body.get("ads")
    except Exception:
        ads = None
    if not isinstance(ads, list):
        return Response(json.dumps({"ok": False,
                                      "error": "ads array required"}),
                        status=400, mimetype="application/json")
    # Optional explicit-deletion list — auto-blocks the user marked as
    # false positives. Without this, deletion-of-an-auto-block can't
    # survive a re-scan (the auto would re-add the block).
    deleted_in = body.get("deleted") if isinstance(body, dict) else None
    if deleted_in is not None and not isinstance(deleted_in, list):
        return Response(json.dumps({"ok": False,
                                      "error": "deleted must be array"}),
                        status=400, mimetype="application/json")

    def _clean(items):
        out = []
        for blk in items or []:
            try:
                s = float(blk[0]); e = float(blk[1])
                if e > s and e - s >= 1:
                    out.append([round(s, 2), round(e, 2)])
            except Exception:
                continue
        out.sort(key=lambda b: b[0])
        return out
    cleaned = _clean(ads)
    cleaned_deleted = _clean(deleted_in)

    user_cache = out_dir / "ads_user.json"
    # Preserve confirmed_show + reviewed_at from existing ads_user.json
    # (set by /mark-reviewed) — those are user-meta fields, the ad-edit
    # UI doesn't know about them and a full overwrite would wipe them.
    existing_extras = {}
    if user_cache.exists():
        try:
            cur = json.loads(user_cache.read_text())
            if isinstance(cur, dict):
                for k in ("confirmed_show", "reviewed_at"):
                    if k in cur:
                        existing_extras[k] = cur[k]
        except Exception:
            pass
    try:
        if not cleaned and not cleaned_deleted and not existing_extras:
            user_cache.unlink(missing_ok=True)
        else:
            payload = {"ads": cleaned, "deleted": cleaned_deleted, **existing_extras}
            tmp = user_cache.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload))
            tmp.replace(user_cache)
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")
    # Auto-refingerprint signal: drop the existing fingerprints so
    # this uuid surfaces in /api/internal/spot-fp/queue as "missing".
    # Mac-side tv-spot-extract.py picks it up on its next sweep and
    # re-uploads with fresh blocks. Pi no longer extracts.
    if cleaned:
        try:
            with _spot_fp_lock:
                conn = _spot_fp_open()
                try:
                    conn.execute("DELETE FROM fingerprints WHERE uuid=?",
                                 (uuid,))
                    conn.commit()
                finally:
                    conn.close()
        except Exception as e:
            print(f"[spot-fp] invalidate err {uuid[:8]}: {e}",
                  flush=True)
    return _cors(Response(json.dumps({"ok": True, "ads": cleaned,
                                       "deleted": cleaned_deleted}),
                           mimetype="application/json"))
















@app.route("/recording/<uuid>/source")
def recording_source(uuid):
    """Serve the original .ts file for HTTP-based decode by the
    Mac-side thumbs daemon (and potentially future offload paths).
    Range support via send_file conditional=True so ffmpeg can
    seek when needed. Sequential reads typically saturate gigabit
    (~110 MB/s) vs SMB's ~40 MB/s - 2-3x speedup on Mac decode.

    Guards against in-progress recordings: serving partial bytes
    while tvh is still writing the .ts is the root of the back-
    half-NaN-logo bug - Mac caches the truncated copy and the
    Content-Length check on the daemon side has no signal to
    reject it (length matches what was actually on disk). Return
    425 Too Early; the daemon treats that as transient and retries
    after a cooldown."""
    if _is_recording_in_progress(uuid):
        abort(425)
    src = _rec_source_path(uuid)
    if not src or not Path(src).exists():
        abort(404)
    return send_file(src, mimetype="video/mp2t", conditional=True)




@app.route("/api/internal/active-channels")
def api_internal_active_channels():
    """List of channel slugs whose live HLS playlist has been touched
    in the last ACTIVE_MTIME_S=60 s — i.e. someone is currently
    watching them. Used by the Mac live-detect daemon to decide which
    channels need a rolling-window ad scan.

    Replaces the old `channel_is_active(slug)` SMB-stat lookup in
    tv-live-detect.py — Pi reads its own local disk (no SMB on Mac
    side, no TCC trap)."""
    global _live_scanner_last_poll
    _live_scanner_last_poll = time.time()
    active = []
    now = time.time()
    for d in HLS_DIR.iterdir():
        if not d.is_dir() or d.name.startswith("_") or d.name.startswith("."):
            continue
        m3u = d / "index.m3u8"
        if not m3u.is_file():
            continue
        try:
            if now - m3u.stat().st_mtime < 60:
                active.append(d.name)
        except Exception:
            continue
    # Union tv-receiver's active channels: live-TV (web + app) is served
    # there now, so its warm transcodes are the real "being watched" signal.
    # HLS_DIR above only still moves for legacy gateway-served channels.
    # Without this union the Mac live-ad scanner misses every tv-receiver
    # channel — i.e. live ad-skip would silently stop working.
    try:
        with urllib.request.urlopen(
                f"{TV_RECEIVER_BASE}/api/internal/active-channels", timeout=3) as r:
            active = list(set(active) | set(json.load(r).get("active", [])))
    except Exception as e:
        print(f"active-channels: tv-receiver fetch failed ({e})", flush=True)
    return _cors(Response(json.dumps({"active": sorted(active)}),
                            mimetype="application/json"))


@app.route("/api/internal/live-config/<slug>")
def api_internal_live_config(slug):
    """Combined per-channel live-detect config — replaces 4 separate
    SMB reads in tv-live-detect.py (channel_logo_smooth_s,
    effective_start_lag, effective_sponsor_duration, logo template
    path). Mac script uses these to build the tv-detect command line."""
    cfg = {"slug": slug, "logo_smooth_s": 0.0,
           "start_lag_s": 0.0, "sponsor_duration_s": 0.0,
           "cached_logo_url": "",
           "min_block_s": 0.0, "max_block_s": 0.0}
    try:
        ch_cfg = json.loads(
            (HLS_DIR / ".channel-config.json").read_text()
        ).get("channels", {}).get(slug, {})
        cfg["logo_smooth_s"] = float(ch_cfg.get("logo_smooth_s", 0))
    except Exception: pass
    try:
        learned = json.loads(
            (HLS_DIR / ".detection_learning.json").read_text()
        ).get(slug, {})
        cfg["start_lag_s"] = float(learned.get("start_lag", 0))
        cfg["sponsor_duration_s"] = float(learned.get("sponsor_duration", 0))
    except Exception: pass
    if (_TVD_LOGO_DIR / f"{slug}.logo.txt").is_file():
        cfg["cached_logo_url"] = f"/api/internal/detect-logo/{slug}"
    # Block-length prior — same lookup as detect-config/<uuid> but
    # without the per-show fallback (live runs don't know the show).
    try:
        p = json.loads(
            (HLS_DIR / ".block_length_prior.json").read_text()
        ).get(slug, {})
        if "min_block_s" in p and "max_block_s" in p:
            cfg["min_block_s"] = float(p["min_block_s"])
            cfg["max_block_s"] = float(p["max_block_s"])
    except Exception: pass
    return _cors(Response(json.dumps(cfg), mimetype="application/json"))






@app.route("/api/internal/live-ads", methods=["GET", "POST"])
def api_internal_live_ads():
    """GET: return current `.live_ads.json` content (Mac daemon loads
    state at startup). POST: accept updated full state, write atomically.
    Replaces SMB read+write of the same file with HTTP — no Mac-side
    SMB needed for live-detect."""
    if request.method == "GET":
        global _daemon_last_poll
        _daemon_last_poll = time.time()
        try:
            body = LIVE_ADS_FILE.read_text() if LIVE_ADS_FILE.exists() else "{}"
        except Exception:
            body = "{}"
        return _cors(Response(body, mimetype="application/json"))
    # POST — accept full state replacement (Mac is the sole writer
    # when LIVE_ADS_OFFLOAD=mac, atomic-rename via .tmp)
    body = request.get_data()
    if len(body) > 5 << 20:  # 5 MB sanity cap
        abort(413)
    try:
        json.loads(body)  # validate
    except Exception:
        abort(400)
    tmp = LIVE_ADS_FILE.with_suffix(".tmp")
    tmp.write_bytes(body)
    tmp.rename(LIVE_ADS_FILE)
    # Trigger in-memory reload (load_live_ads picks up next access
    # via mtime-check; nothing more to do here).
    return _cors(Response(json.dumps({"ok": True, "bytes": len(body)}),
                            mimetype="application/json"))




def _load_user_groups():
    """Returns {group_name: [uuid, ...]} or {} on any error.
    User-curated franchise groupings (Rocky/Asterix style — same
    show, no autorec link because each title differs)."""
    try:
        return json.loads(USER_GROUPS_FILE.read_text())
    except Exception:
        return {}


def _save_user_groups(groups: dict) -> bool:
    """Atomic-rename write so a concurrent reader never sees a
    half-written file. Returns True on success."""
    try:
        tmp = USER_GROUPS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(groups, indent=1, ensure_ascii=False))
        tmp.replace(USER_GROUPS_FILE)
        return True
    except Exception as e:
        print(f"[user-groups] save err: {e}", flush=True)
        return False




# ============================================================
# Whisper full-text search (FTS5)
# Index of every whisper-classify window across all recordings,
# enabling "wann sagte X über Y" queries with sub-second response.
# Index lives next to the recordings on the same volume that holds
# the .whisper.json files, so reindexing is a local file walk.
# ============================================================
WHISPER_INDEX_PATH = HLS_DIR / ".whisper-search.sqlite"
_whisper_idx_lock = threading.Lock()
_whisper_refresh_state = {"last_full_scan_ts": 0.0}


def _whisper_open():
    conn = sqlite3.connect(str(WHISPER_INDEX_PATH))
    conn.executescript("""
        CREATE VIRTUAL TABLE IF NOT EXISTS whisper_fts USING fts5(
            text,
            uuid UNINDEXED,
            t_start UNINDEXED,
            prob_ad UNINDEXED,
            tokenize='unicode61 remove_diacritics 2'
        );
        CREATE TABLE IF NOT EXISTS whisper_meta(
            uuid TEXT PRIMARY KEY,
            title TEXT,
            channel_slug TEXT,
            recording_start_s REAL,
            json_path TEXT,
            json_mtime REAL
        );
    """)
    return conn


def _rec_start_s_from_base(base):
    """Parse 'Show $YYYY-MM-DD-HHMM' → unix-ts. Returns 0 on parse fail.
    Used as the sort key for newest-first search results."""
    if " $" not in base:
        return 0.0
    stamp = base.split(" $", 1)[1].split("-", 4)
    if len(stamp) < 4 or len(stamp[3]) < 4:
        return 0.0
    try:
        import datetime as _dt
        dt = _dt.datetime(int(stamp[0]), int(stamp[1]), int(stamp[2]),
                          int(stamp[3][:2]), int(stamp[3][2:4]))
        return dt.timestamp()
    except Exception:
        return 0.0


def _whisper_index_one(conn, uuid_str, json_path):
    """(Re-)index a single recording's whisper.json. Idempotent —
    always deletes existing rows for the uuid before inserting."""
    try:
        data = json.loads(Path(json_path).read_text())
    except Exception as e:
        print(f"[whisper-idx] parse fail {json_path}: {e}", flush=True)
        return 0
    rec_dir = HLS_DIR / f"_rec_{uuid_str}"
    title = _show_title_for_rec(rec_dir) or _rec_dvr_title(uuid_str) or ""
    channel_slug = _rec_channel_slug(uuid_str) or ""
    # Recording start derived from the cutlist .txt basename
    # (= "Show $YYYY-MM-DD-HHMM.txt"), since the .whisper.json
    # filename is now hidden + uuid-keyed and carries no date.
    base = ""
    sidecar_endings = (".logo.txt", ".cskp.txt", ".tvd.txt",
                       ".trained.logo.txt")
    for p in rec_dir.glob("*.txt"):
        if any(p.name.endswith(s) for s in sidecar_endings):
            continue
        base = p.stem
        break
    rec_start = _rec_start_s_from_base(base)

    conn.execute("DELETE FROM whisper_fts WHERE uuid = ?", (uuid_str,))
    rows = []
    for w in data.get("windows", []):
        text = (w.get("text") or "").strip()
        if not text:
            continue
        rows.append((text, uuid_str, float(w.get("t", 0)),
                     float(w.get("prob", 0))))
    if rows:
        conn.executemany(
            "INSERT INTO whisper_fts(text, uuid, t_start, prob_ad) "
            "VALUES (?,?,?,?)", rows)
    conn.execute(
        "INSERT OR REPLACE INTO whisper_meta(uuid, title, channel_slug, "
        "recording_start_s, json_path, json_mtime) VALUES (?,?,?,?,?,?)",
        (uuid_str, title, channel_slug, rec_start, str(json_path),
         Path(json_path).stat().st_mtime))
    return len(rows)


def _whisper_lazy_refresh(force=False):
    """Walk all whisper.json files; reindex any whose mtime differs
    from what's in whisper_meta. Also drops index rows for recordings
    whose .whisper.json or rec_dir has been deleted. Throttled to one
    full scan per 30 s unless force=True."""
    now = time.time()
    if not force and (now - _whisper_refresh_state["last_full_scan_ts"]) < 30:
        return
    with _whisper_idx_lock:
        conn = _whisper_open()
        try:
            cur = {row[0]: row[1] for row in
                   conn.execute("SELECT uuid, json_mtime FROM whisper_meta")}
            seen = set()
            n_new = n_changed = 0
            for jpath in HLS_DIR.glob("_rec_*/.whisper.json"):
                try:
                    uuid_str = jpath.parent.name[5:]
                    seen.add(uuid_str)
                    mtime = jpath.stat().st_mtime
                    prev = cur.get(uuid_str)
                    if prev is not None and abs(prev - mtime) < 1.0:
                        continue
                    _whisper_index_one(conn, uuid_str, jpath)
                    if prev is None:
                        n_new += 1
                    else:
                        n_changed += 1
                except Exception as e:
                    print(f"[whisper-idx] err {jpath}: {e}", flush=True)
            stale = set(cur.keys()) - seen
            for uuid_str in stale:
                conn.execute("DELETE FROM whisper_fts WHERE uuid=?",
                             (uuid_str,))
                conn.execute("DELETE FROM whisper_meta WHERE uuid=?",
                             (uuid_str,))
            conn.commit()
            if n_new or n_changed or stale:
                print(f"[whisper-idx] +{n_new} new, ~{n_changed} updated, "
                      f"-{len(stale)} stale", flush=True)
            _whisper_refresh_state["last_full_scan_ts"] = now
        finally:
            conn.close()


def _whisper_search(query, max_results=100, include_ads=False):
    """FTS5 query → ordered list of {uuid, t_start, prob_ad, snippet,
    title, channel_slug, recording_start_s}. Newest recordings first.
    Default excludes windows with prob_ad >= 0.5 so search returns
    show content, not ad transcripts (toggleable via include_ads)."""
    _whisper_lazy_refresh()
    if not query.strip():
        return []
    with _whisper_idx_lock:
        conn = _whisper_open()
        try:
            sql = ("SELECT w.uuid, w.t_start, w.prob_ad, "
                   "snippet(whisper_fts, 0, '<mark>', '</mark>', '…', 16) "
                   "AS snip, m.title, m.channel_slug, m.recording_start_s "
                   "FROM whisper_fts w "
                   "JOIN whisper_meta m ON m.uuid = w.uuid "
                   "WHERE whisper_fts MATCH ?")
            params = [query]
            if not include_ads:
                sql += " AND w.prob_ad < 0.5"
            sql += " ORDER BY m.recording_start_s DESC LIMIT ?"
            params.append(max_results)
            try:
                rows = conn.execute(sql, params).fetchall()
            except sqlite3.OperationalError as e:
                # Malformed FTS query (e.g. lone quote or operator) →
                # return empty rather than 500.
                print(f"[whisper-search] bad query {query!r}: {e}", flush=True)
                return []
        finally:
            conn.close()
    out = []
    for uuid_str, t_start, prob, snip, title, slug, rec_start in rows:
        out.append({"uuid": uuid_str, "t_start": float(t_start),
                    "prob_ad": float(prob), "snippet": snip,
                    "title": title or "", "channel_slug": slug or "",
                    "recording_start_s": float(rec_start)})
    return out


@app.route("/api/search")
def api_search():
    q = (request.args.get("q") or "").strip()
    if not q:
        return _cors(Response(json.dumps({"results": [], "n": 0}),
                              mimetype="application/json"))
    include_ads = request.args.get("include_ads") in ("1", "true")
    try:
        limit = max(1, min(200, int(request.args.get("limit") or 100)))
    except Exception:
        limit = 100
    results = _whisper_search(q, max_results=limit, include_ads=include_ads)
    return _cors(Response(json.dumps({"results": results, "n": len(results),
                                       "q": q, "include_ads": include_ads}),
                          mimetype="application/json"))


def _recording_app_schema(e, now_ts, host_url):
    """Apply Bibliothek-playability filter to a tvh dvr entry and return
    the app-flat schema, or None if the entry isn't playable. Shared by
    /api/recordings list + /api/recordings/<uuid> single endpoints."""
    if not e.get("enabled", True):
        return None
    if "Aborted by user" in (e.get("status", "") or ""):
        return None
    uuid_ = e.get("uuid", "")
    if not uuid_:
        return None
    ss = e.get("sched_status", "") or ""
    start = e.get("start", 0)
    stop = e.get("stop", 0)

    if ss in ("completed", "completedError"):
        size = e.get("filesize") or 0
        if size >= 50 * 1024 * 1024:
            state = "completed"
        else:
            try:
                n_segs = sum(1 for _ in
                             (HLS_DIR / f"_rec_{uuid_}").glob("seg_*.ts"))
            except Exception:
                n_segs = 0
            if n_segs >= 20 and (HLS_DIR / f"_rec_{uuid_}" /
                                 "index.m3u8").is_file():
                state = "completed"
            else:
                return None
    elif ss == "recording" or (start <= now_ts < stop):
        state = "recording"
    else:
        return None

    # Prefer the curated /static/ch-logos/<slug>.png override over the
    # low-res tvheadend imagecache default. PNG-first because iOS
    # UIImage doesn't handle SVG natively; web UI uses default SVG-
    # first via the same helper.
    slug = _rec_channel_slug(uuid_) or ""
    icon = _channel_logo_url(slug, e.get("channel_icon", "") or "",
                             ext_priority=("png", "svg", "jpg"))

    return {
        "uuid": uuid_,
        "title": e.get("disp_title", "") or "",
        "subtitle": e.get("disp_subtitle", "") or "",
        "description": e.get("disp_description", "") or "",
        "channel": e.get("channelname", "") or "",
        "channel_icon": icon,
        "start": start,
        "stop": stop,
        "duration": e.get("duration", 0),
        "state": state,
        "playposition": e.get("playposition", 0),
        "playcount": e.get("playcount", 0),
        "watched": bool(e.get("watched", 0)),
        "autorec": e.get("autorec", "") or "",
        "hls_url": f"/recording/{uuid_}/index.m3u8",
        "ads_url": f"/recording/{uuid_}/ads",
        "thumbs_url": f"/recording/{uuid_}/thumbs.json",
        "poster_url": _show_poster_url(uuid_, e.get("disp_title") or ""),
    }


@app.route("/api/recordings")
def api_recordings():
    """JSON list of playable recordings for external apps. Applies the
    same filtering as the /bibliothek consumer page: only sched_status
    completed/completedError, plus a playability check (tvh .ts >= 50 MB
    OR HLS-VOD bundle with >=20 segments — covers T7-deduped recordings
    where the original .ts is gone but the HLS bundle stays).

    Query params:
      ?include_recording=1   include currently-recording entries
      ?watched=true|false    filter by playcount (>0 means watched)
      ?limit=N               cap output count (post-filter)
      ?since=<unix_ts>       delta-sync: only entries with start>since
                             OR create>since. Caveat: deletions are NOT
                             reported — clients should do a full fetch
                             periodically (e.g. once/day) to reconcile.

    Response: {recordings: [...], n: N, server_time: <unix_ts>} —
    save `server_time` and pass it as `?since=` on the next call.
    """
    include_recording = request.args.get("include_recording") in ("1", "true")
    watched_q = request.args.get("watched")
    try:
        limit = int(request.args.get("limit") or 0)
    except Exception:
        limit = 0
    try:
        since = int(request.args.get("since") or 0)
    except Exception:
        since = 0
    now_ts = int(time.time())
    host_url = request.host_url

    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000&sort=start&dir=DESC",
            timeout=10).read())
    except Exception as e:
        abort(502, f"tvheadend: {e}")

    out = []
    for e in data.get("entries", []):
        item = _recording_app_schema(e, now_ts, host_url)
        if item is None:
            continue
        if item["state"] == "recording" and not include_recording:
            continue
        if watched_q == "true" and item["playcount"] == 0:
            continue
        if watched_q == "false" and item["playcount"] > 0:
            continue
        if since:
            # Newer-than-cursor: include if broadcast start is past
            # `since`, OR the dvr entry was created past `since`
            # (covers re-scheduling, autorec spawning a fresh entry).
            create_ts = e.get("create", 0) or 0
            if item["start"] <= since and create_ts <= since:
                continue
        out.append(item)
        if limit and len(out) >= limit:
            break

    return _cors(Response(
        json.dumps({"recordings": out, "n": len(out),
                    "server_time": now_ts}),
        mimetype="application/json"))


@app.route("/api/recordings/<uuid>")
def api_recording_single(uuid):
    """Single-recording GET — same schema as the list entries. Returns
    404 if uuid doesn't exist or doesn't pass the playability filter.
    Use this on detail-view open to refresh playposition/watched state
    without re-fetching the whole library.

    Implementation note: idnode/load returns verbose params-array
    metadata, not flat entries, so we fetch via the grid (with local
    uuid filter). One tvh roundtrip, ~50ms over loopback."""
    now_ts = int(time.time())
    host_url = request.host_url
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000&sort=start&dir=DESC",
            timeout=10).read())
    except Exception as e:
        abort(502, f"tvheadend: {e}")
    entry = next((e for e in data.get("entries", [])
                  if e.get("uuid") == uuid), None)
    if entry is None:
        return _cors(Response(json.dumps({"error": "not found"}),
                              status=404, mimetype="application/json"))
    item = _recording_app_schema(entry, now_ts, host_url)
    if item is None:
        return _cors(Response(json.dumps({"error": "not playable"}),
                              status=404, mimetype="application/json"))
    return _cors(Response(json.dumps(item), mimetype="application/json"))


@app.route("/api/recording/<uuid>/playposition", methods=["POST"])
def api_recording_playposition(uuid):
    """Set playback position on a recording (in seconds). tvheadend's
    `playposition` is global per recording — cross-device resume works
    but two devices playing simultaneously will overwrite each other.
    App should debounce writes (every ~10s or on pause/stop)."""
    try:
        body = request.get_json(silent=True) or {}
        pos = max(0, int(body.get("position", 0)))
    except Exception:
        return _cors(Response(json.dumps({"ok": False,
                                          "error": "bad position"}),
                              status=400, mimetype="application/json"))
    payload = urllib.parse.urlencode({
        "node": json.dumps({"uuid": uuid, "playposition": pos})
    }).encode()
    try:
        req = urllib.request.Request(f"{dvr_base()}/api/idnode/save",
                                      data=payload, method="POST")
        urllib.request.urlopen(req, timeout=5).read()
        return _cors(Response(json.dumps({"ok": True, "position": pos}),
                              mimetype="application/json"))
    except Exception as e:
        return _cors(Response(json.dumps({"ok": False, "error": str(e)}),
                              status=500, mimetype="application/json"))




# ============================================================
# Cross-channel ad-spot fingerprinting (silence-aligned)
#
# For each user-confirmed ad block:
#   1. ffmpeg silencedetect → list of silence intervals
#   2. spots = audio runs BETWEEN silences (= the actual
#      commercials separated by ~300-1000 ms of silence)
#   3. fingerprint each spot starting at silence-aligned offset
#
# Why silence-aligned: chromaprint emits one hash per ~124 ms
# of audio. Two airings of the same commercial produce IDENTICAL
# hash sequences only if they start at the same audio offset.
# Sliding-window approach failed because two windows shifted by
# 10 s of the same audio still produced fingerprints that bytewise
# diff ratio ≈ 0.46-0.48 — indistinguishable from random. With
# silence-aligned starts, both airings of the same spot share the
# same hash positions, and bit-by-bit Hamming compare works.
#
# Storage: /mnt/tv/hls/.spot-fingerprints.sqlite
#   - fingerprints: one row per detected spot (not sliding window)
#   - family_members: maintained by _spot_fp_rebuild_families()
# ============================================================
SPOT_FP_DB              = HLS_DIR / ".spot-fingerprints.sqlite"
SPOT_SEG_DUR_S          = 6.0    # HLS target duration
SPOT_SILENCE_DB         = -30    # silencedetect threshold (dBFS)
SPOT_SILENCE_MIN_S      = 0.30   # min silence duration to count as boundary
SPOT_MIN_DUR_S          = 12.0   # spots shorter than this = artifact / sponsor card
SPOT_MAX_DUR_S          = 60.0   # spots longer than this = merged blocks / non-spot music
SPOT_TRIM_EDGE_S        = 2.0    # skip this much at spot start AND end before
                                 # fingerprinting — outros/intros are often
                                 # shared across same-advertiser commercials
                                 # (e.g. IKEA "Gemacht fürs Leben" tag) and
                                 # would over-cluster otherwise.
SPOT_VISUAL_FRAMES      = 5      # dHash samples per spot (evenly spaced)
SPOT_VISUAL_BIT_BUDGET  = 12     # max ≤ this many bits diff (out of 64) for
                                 # visual match — picked from per-frame
                                 # consecutive-frame baseline (~11 bits) plus
                                 # tolerance for compression noise across
                                 # different DVB encodes of the same spot.
SPOT_MATCH_THRESHOLD    = 0.20   # silence-aligned Hamming-ratio cutoff
SPOT_FP_QUEUE           = []
_spot_fp_lock           = threading.Lock()
_spot_fp_queue_lock     = threading.Lock()
_spot_fp_queue_cv       = threading.Condition(_spot_fp_queue_lock)


def _spot_fp_open():
    conn = sqlite3.connect(str(SPOT_FP_DB))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fingerprints(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid TEXT NOT NULL,
            channel_slug TEXT,
            block_idx INTEGER,
            block_start_s REAL,
            block_end_s REAL,
            window_start_s REAL,
            recording_start_ts REAL,
            fp BLOB,
            dhashes BLOB
        );
        CREATE INDEX IF NOT EXISTS ix_fp_uuid ON fingerprints(uuid);
        CREATE TABLE IF NOT EXISTS family_members(
            family_id INTEGER NOT NULL,
            fp_id INTEGER NOT NULL UNIQUE
        );
        CREATE INDEX IF NOT EXISTS ix_fm_family
            ON family_members(family_id);
        CREATE TABLE IF NOT EXISTS rebuild_meta(
            key TEXT PRIMARY KEY, val TEXT
        );
    """)
    # Migrate: add dhashes column if it didn't exist already
    cols = {r[1] for r in conn.execute(
        "PRAGMA table_info(fingerprints)").fetchall()}
    if "dhashes" not in cols:
        conn.execute("ALTER TABLE fingerprints ADD COLUMN dhashes BLOB")
        conn.commit()
    return conn


def _extract_dhashes(rec_dir, abs_start_s, dur_s,
                     n_frames=SPOT_VISUAL_FRAMES):
    """Single ffmpeg call: extract n_frames evenly-spaced 9×8 grayscale
    frames over [abs_start_s+1, abs_start_s+dur_s-1], compute dHash for
    each. Returns bytes (n_frames × 8 = 40 bytes packed) or None on
    failure. Skips first/last 1 s to avoid transition flashes."""
    inner_start = abs_start_s + 1.0
    inner_dur = dur_s - 2.0
    if inner_dur < 2.0:
        return None
    seg_paths, seek_s = _spot_block_seg_paths(rec_dir, inner_start,
                                              inner_start + inner_dur)
    if len(seg_paths) < 1:
        return None
    fps = max(0.5, n_frames / inner_dur)
    try:
        r = subprocess.run(
            ["ffmpeg", "-loglevel", "error",
             "-i", "concat:" + "|".join(seg_paths),
             "-ss", f"{seek_s:.3f}", "-t", f"{inner_dur:.2f}",
             "-vf", f"fps={fps:.4f},scale=9:8,format=gray",
             "-f", "rawvideo", "-pix_fmt", "gray", "-"],
            capture_output=True, timeout=30)
        out = r.stdout
    except Exception as e:
        print(f"[spot-fp] dhash extract err: {e}", flush=True)
        return None
    hashes = []
    for i in range(0, len(out), 72):
        chunk = out[i:i + 72]
        if len(chunk) != 72:
            break
        h = 0
        for y in range(8):
            row_off = y * 9
            bit_off = y * 8
            for x in range(8):
                if chunk[row_off + x] > chunk[row_off + x + 1]:
                    h |= 1 << (bit_off + x)
        hashes.append(h)
        if len(hashes) >= n_frames:
            break
    if not hashes:
        return None
    # Pack to bytes: each dHash = 8 bytes big-endian; varying-length
    # blob since some spots produce fewer frames than requested.
    return b"".join(h.to_bytes(8, "big") for h in hashes)


def _dhashes_min_diff(a_blob, b_blob):
    """Best-match Hamming bit-distance between any frame of a and any
    frame of b. Returns int (0..64) or None if either is empty."""
    if not a_blob or not b_blob:
        return None
    a_hs = [int.from_bytes(a_blob[i:i+8], "big")
            for i in range(0, len(a_blob), 8) if len(a_blob[i:i+8]) == 8]
    b_hs = [int.from_bytes(b_blob[i:i+8], "big")
            for i in range(0, len(b_blob), 8) if len(b_blob[i:i+8]) == 8]
    if not a_hs or not b_hs:
        return None
    best = 64
    bit_count = int.bit_count
    for ah in a_hs:
        for bh in b_hs:
            d = bit_count(ah ^ bh)
            if d < best:
                best = d
                if best == 0:
                    return 0
    return best


def _spot_block_seg_paths(rec_dir, block_start_s, block_end_s):
    """Return (segment_paths, seek_s) covering a given block. seek_s
    is the offset INTO the first segment to skip to reach
    block_start_s."""
    first_seg = max(0, int(block_start_s / SPOT_SEG_DUR_S))
    last_seg  = int(block_end_s / SPOT_SEG_DUR_S) + 1
    seg_paths = []
    for i in range(first_seg, last_seg + 1):
        p = rec_dir / f"seg_{i:05d}.ts"
        if p.is_file():
            seg_paths.append(str(p))
    seek_s = max(0.0, block_start_s - first_seg * SPOT_SEG_DUR_S)
    return seg_paths, seek_s


def _spot_silence_intervals(rec_dir, block_start_s, block_end_s):
    """Run ffmpeg silencedetect over [block_start_s, block_end_s].
    Returns list of (silence_start, silence_end) intervals in
    BLOCK-relative seconds (= 0 = block_start_s)."""
    seg_paths, seek_s = _spot_block_seg_paths(rec_dir,
                                              block_start_s,
                                              block_end_s)
    if len(seg_paths) < 1:
        return []
    dur = max(0.5, block_end_s - block_start_s)
    try:
        r = subprocess.run(
            ["ffmpeg", "-i", "concat:" + "|".join(seg_paths),
             "-ss", f"{seek_s:.3f}", "-t", f"{dur:.2f}",
             "-vn",
             "-af", f"silencedetect=n={SPOT_SILENCE_DB}dB:"
                    f"d={SPOT_SILENCE_MIN_S}",
             "-f", "null", "-"],
            capture_output=True, timeout=120)
    except Exception as e:
        print(f"[spot-fp] silencedetect err: {e}", flush=True)
        return []
    intervals = []
    cur_start = None
    for line in r.stderr.decode("utf-8", errors="replace").splitlines():
        if "silence_start:" in line:
            try:
                cur_start = float(line.split(
                    "silence_start:", 1)[1].strip().split()[0])
            except Exception:
                cur_start = None
        elif "silence_end:" in line and cur_start is not None:
            try:
                end_str = line.split("silence_end:", 1)[1]
                end = float(end_str.split("|", 1)[0].strip().split()[0])
                if end > cur_start:
                    intervals.append((cur_start, end))
            except Exception:
                pass
            cur_start = None
    return intervals


def _spot_intervals_from_silences(silences, block_dur_s):
    """Compute spot intervals = audio between silences. Return list
    of (spot_start_s, spot_end_s) in block-relative time, filtered
    to SPOT_MIN_DUR_S ≤ dur ≤ SPOT_MAX_DUR_S."""
    boundaries = [(0.0, 0.0)] + silences + [(block_dur_s, block_dur_s)]
    spots = []
    for i in range(len(boundaries) - 1):
        ss = boundaries[i][1]      # spot starts at end-of-silence
        se = boundaries[i + 1][0]  # spot ends at next silence-start
        d = se - ss
        if SPOT_MIN_DUR_S <= d <= SPOT_MAX_DUR_S:
            spots.append((ss, se))
    return spots


def _spot_fp_extract_one(rec_dir, abs_start_s, dur_s):
    """Extract chromaprint for one spot starting at abs_start_s (=
    recording-relative seconds), for dur_s seconds. Trims
    SPOT_TRIM_EDGE_S off both ends to avoid shared intros/outros
    (= same-brand spots cluster falsely on shared "Qualität von X"
    closing tags otherwise). Returns raw bytes or None."""
    inner_dur = dur_s - 2 * SPOT_TRIM_EDGE_S
    if inner_dur < (SPOT_MIN_DUR_S - 2 * SPOT_TRIM_EDGE_S):
        return None
    inner_start = abs_start_s + SPOT_TRIM_EDGE_S
    seg_paths, seek_s = _spot_block_seg_paths(rec_dir, inner_start,
                                              inner_start + inner_dur)
    if len(seg_paths) < 1:
        return None
    try:
        r = subprocess.run(
            ["ffmpeg", "-loglevel", "error",
             "-i", "concat:" + "|".join(seg_paths),
             "-ss", f"{seek_s:.3f}", "-t", f"{inner_dur:.2f}",
             "-vn", "-ac", "1", "-ar", "22050",
             "-f", "chromaprint", "-fp_format", "raw", "-"],
            capture_output=True, timeout=30)
        if r.returncode != 0:
            return None
        # ~7-8 hashes/sec × 4 bytes; SPOT_MIN_DUR_S=8 → ≥ 224 bytes.
        # Anything significantly smaller = audio dropout / decode fail.
        if len(r.stdout) < 200:
            return None
        return r.stdout
    except Exception as e:
        print(f"[spot-fp] ffmpeg err: {e}", flush=True)
        return None


def _spot_fp_index_recording(uuid_str):
    """(Re-)build fingerprints for one recording's confirmed ad blocks.
    Per block: silence-detect → split into spots → fingerprint each
    spot from its silence-aligned start. Returns # spots inserted."""
    rec_dir = HLS_DIR / f"_rec_{uuid_str}"
    user_p = rec_dir / "ads_user.json"
    if not user_p.is_file():
        return 0
    try:
        data = json.loads(user_p.read_text())
        blocks = data.get("ads") if isinstance(data, dict) else data
        blocks = blocks or []
    except Exception:
        return 0
    channel_slug = _rec_channel_slug(uuid_str) or ""
    base = ""
    sidecar_endings = (".logo.txt", ".cskp.txt", ".tvd.txt",
                       ".trained.logo.txt")
    for p in rec_dir.glob("*.txt"):
        if any(p.name.endswith(s) for s in sidecar_endings):
            continue
        base = p.stem
        break
    rec_start_ts = _rec_start_s_from_base(base)
    n_inserted = 0
    with _spot_fp_lock:
        conn = _spot_fp_open()
        try:
            conn.execute("DELETE FROM fingerprints WHERE uuid = ?",
                         (uuid_str,))
            for bi, blk in enumerate(blocks):
                try:
                    bs, be = float(blk[0]), float(blk[1])
                except Exception:
                    continue
                block_dur = be - bs
                if block_dur < SPOT_MIN_DUR_S:
                    continue
                silences = _spot_silence_intervals(rec_dir, bs, be)
                spots = _spot_intervals_from_silences(silences,
                                                     block_dur)
                if not spots:
                    # No usable silences detected — fall back to a
                    # single fingerprint of the whole block (= often
                    # one continuous spot or a music-bed promo).
                    if SPOT_MIN_DUR_S <= block_dur <= SPOT_MAX_DUR_S:
                        spots = [(0.0, block_dur)]
                for ss_rel, se_rel in spots:
                    spot_dur = se_rel - ss_rel
                    abs_start = bs + ss_rel
                    fp = _spot_fp_extract_one(rec_dir, abs_start,
                                              spot_dur)
                    if fp is None:
                        continue
                    dh = _extract_dhashes(rec_dir, abs_start, spot_dur)
                    conn.execute(
                        "INSERT INTO fingerprints(uuid, channel_slug, "
                        "block_idx, block_start_s, block_end_s, "
                        "window_start_s, recording_start_ts, fp, "
                        "dhashes) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (uuid_str, channel_slug, bi, bs, be, abs_start,
                         rec_start_ts, sqlite3.Binary(fp),
                         sqlite3.Binary(dh) if dh else None))
                    n_inserted += 1
            conn.commit()
        finally:
            conn.close()
    return n_inserted


# Bit-count lookup table — each byte's popcount precomputed once
_POPCNT = bytes(bin(b).count("1") for b in range(256))


def _hamming_ratio(a, b):
    """Bytewise bit-diff ratio between fp blobs, trimmed to shorter.
    Returns float in [0.0, 1.0] or None if either is empty."""
    n = min(len(a), len(b))
    if n == 0:
        return None
    diff = 0
    for x, y in zip(a[:n], b[:n]):
        diff += _POPCNT[x ^ y]
    return diff / (n * 8)


def _spot_fp_rebuild_families(threshold=SPOT_MATCH_THRESHOLD,
                              visual_bits=SPOT_VISUAL_BIT_BUDGET):
    """Pairwise compare every fingerprint via BOTH audio (chromaprint
    bytewise Hamming ratio ≤ threshold) AND visual (best-pair dHash
    bit-diff ≤ visual_bits). Both must pass to merge. Visual is the
    decisive complement that splits IKEA-outro audio clusters into
    real per-spot groupings. Returns family count."""
    with _spot_fp_lock:
        conn = _spot_fp_open()
        try:
            rows = conn.execute(
                "SELECT id, fp, dhashes FROM fingerprints").fetchall()
        finally:
            conn.close()
    n = len(rows)
    if n == 0:
        return 0
    # Pre-convert: audio fp as bignum_int + bit-length, dhashes as
    # tuple of int64 hashes (or empty tuple if missing).
    items = []
    for fp_id, blob, dh_blob in rows:
        b = bytes(blob)
        dh_b = bytes(dh_blob) if dh_blob else b""
        dh_list = tuple(
            int.from_bytes(dh_b[i:i+8], "big")
            for i in range(0, len(dh_b), 8) if len(dh_b[i:i+8]) == 8)
        items.append((fp_id, int.from_bytes(b, "big"),
                      len(b) * 8, dh_list))
    parent = list(range(n))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[rx] = ry
    bit_count = int.bit_count
    for i in range(n):
        _id_i, ai, bi, dh_i = items[i]
        for j in range(i + 1, n):
            _id_j, aj, bj, dh_j = items[j]
            # --- Audio Hamming ratio ---
            if bi == bj:
                diff = bit_count(ai ^ aj)
                total = bi
            elif bi > bj:
                diff = bit_count((ai >> (bi - bj)) ^ aj)
                total = bj
            else:
                diff = bit_count(ai ^ (aj >> (bj - bi)))
                total = bi
            if total <= 0 or (diff / total) > threshold:
                continue
            # --- Visual confirmation (best frame-pair dHash match) ---
            # If either spot has no dHash data (= partial backfill),
            # fall back to audio-only so we don't lose pre-backfill
            # matches entirely.
            if dh_i and dh_j:
                best = 64
                for ah in dh_i:
                    for bh in dh_j:
                        d = bit_count(ah ^ bh)
                        if d < best:
                            best = d
                            if best == 0:
                                break
                    if best == 0:
                        break
                if best > visual_bits:
                    continue
            union(i, j)
    # Group + persist
    groups = {}
    for i in range(n):
        groups.setdefault(find(i), []).append(items[i][0])
    with _spot_fp_lock:
        conn = _spot_fp_open()
        try:
            conn.execute("DELETE FROM family_members")
            for fam_id, members in enumerate(groups.values(), start=1):
                conn.executemany(
                    "INSERT INTO family_members(family_id, fp_id) "
                    "VALUES (?, ?)",
                    [(fam_id, m) for m in members])
            conn.execute(
                "INSERT OR REPLACE INTO rebuild_meta(key, val) "
                "VALUES ('rebuilt_at', ?)", (str(int(time.time())),))
            conn.execute(
                "INSERT OR REPLACE INTO rebuild_meta(key, val) "
                "VALUES ('n_fp', ?)", (str(n),))
            conn.execute(
                "INSERT OR REPLACE INTO rebuild_meta(key, val) "
                "VALUES ('n_families', ?)", (str(len(groups)),))
            conn.commit()
        finally:
            conn.close()
    return len(groups)


def _spot_fp_top_families(min_size=2, limit=50):
    """Return top families by airing count. With silence-aligned
    spots each fingerprint is already one airing, so n_airings =
    raw row count per family."""
    with _spot_fp_lock:
        conn = _spot_fp_open()
        try:
            rows = conn.execute("""
                SELECT fm.family_id, fp.uuid, fp.channel_slug,
                       fp.block_start_s, fp.window_start_s,
                       fp.recording_start_ts
                FROM family_members fm
                JOIN fingerprints fp ON fp.id = fm.fp_id
            """).fetchall()
        finally:
            conn.close()
    by_fam = {}
    for fam_id, uuid, slug, bs, ws, rs in rows:
        e = by_fam.setdefault(fam_id, {
            "id": fam_id, "raw": [],
            "channels": set(), "uuids": set()})
        e["raw"].append({
            "uuid": uuid, "channel_slug": slug,
            "block_start_s": bs, "window_start_s": ws,
            "recording_start_ts": rs})
        e["channels"].add(slug or "?")
        e["uuids"].add(uuid)
    out = []
    for e in by_fam.values():
        n_recs = len(e["uuids"])
        if n_recs < min_size:
            continue
        airings = sorted(e["raw"],
                         key=lambda o: o["recording_start_ts"])
        first = airings[0]
        out.append({
            "id": e["id"],
            "n_airings": len(airings),
            "n_recordings": n_recs,
            "n_channels": len(e["channels"]),
            "channels": sorted(e["channels"]),
            "first_uuid": first["uuid"],
            "first_t_s": first["window_start_s"],
            "first_rec_ts": first["recording_start_ts"],
            "last_rec_ts": airings[-1]["recording_start_ts"],
        })
    out.sort(key=lambda x: (-x["n_airings"], -x["last_rec_ts"]))
    return out[:limit]


def _spot_fp_resume_at_boot():
    """At startup, enqueue any recording with ads_user.json that has
    no fingerprints in the index yet. Lets the worker resume after a
    service.py reload (= queue is in-memory, lost across self-exec)."""
    try:
        with _spot_fp_lock:
            conn = _spot_fp_open()
            try:
                indexed = {r[0] for r in conn.execute(
                    "SELECT DISTINCT uuid FROM fingerprints").fetchall()}
            finally:
                conn.close()
        n_q = 0
        for d in HLS_DIR.glob("_rec_*"):
            if not (d / "ads_user.json").is_file():
                continue
            uuid = d.name[5:]
            if uuid in indexed:
                continue
            _spot_fp_enqueue(uuid)
            n_q += 1
        if n_q:
            print(f"[spot-fp] resume-at-boot: enqueued {n_q} unindexed",
                  flush=True)
    except Exception as e:
        print(f"[spot-fp] resume-at-boot err: {e}", flush=True)


def _spot_fp_worker():
    """Consume SPOT_FP_QUEUE: re-fingerprint queued uuids, then
    rebuild families when the queue drains."""
    while True:
        with _spot_fp_queue_cv:
            while not SPOT_FP_QUEUE:
                _spot_fp_queue_cv.wait()
            uuid_str = SPOT_FP_QUEUE.pop(0)
        try:
            n = _spot_fp_index_recording(uuid_str)
            print(f"[spot-fp] indexed {uuid_str[:8]}: {n} windows",
                  flush=True)
        except Exception as e:
            print(f"[spot-fp] index err {uuid_str[:8]}: {e}", flush=True)
        # Rebuild families only when queue is empty (= avoid thrash
        # during a bulk reindex).
        with _spot_fp_queue_cv:
            empty = (len(SPOT_FP_QUEUE) == 0)
        if empty:
            try:
                t0 = time.time()
                n_fam = _spot_fp_rebuild_families()
                print(f"[spot-fp] families rebuilt: {n_fam} "
                      f"in {time.time()-t0:.1f}s", flush=True)
            except Exception as e:
                print(f"[spot-fp] family rebuild err: {e}", flush=True)


def _spot_fp_enqueue(uuid_str):
    """Add a recording to the fingerprint worker's queue."""
    with _spot_fp_queue_cv:
        if uuid_str not in SPOT_FP_QUEUE:
            SPOT_FP_QUEUE.append(uuid_str)
        _spot_fp_queue_cv.notify()




def _cluster_anchored_for_recording(uuid_str, min_family_size=3,
                                    spot_dur_default=20.0):
    """Return list of {window_start_s, end_s, family_id, family_size}
    for spots in this recording whose family has ≥ min_family_size
    members across the corpus (= confidently a recurring ad).

    Used as a high-confidence ad-anchor signal:
      - reviewed recording: cross-check user labels (= QA)
      - unreviewed recording: pseudo-label as confirmed-ad without
        manual review (= path to fully-automated review)"""
    with _spot_fp_lock:
        conn = _spot_fp_open()
        try:
            # Family size lookup
            sizes = {fid: n for fid, n in conn.execute(
                "SELECT family_id, COUNT(*) FROM family_members "
                "GROUP BY family_id").fetchall()}
            rows = conn.execute(
                "SELECT fp.id, fp.window_start_s, fp.block_end_s, "
                "fm.family_id "
                "FROM fingerprints fp "
                "JOIN family_members fm ON fm.fp_id = fp.id "
                "WHERE fp.uuid = ?", (uuid_str,)).fetchall()
        finally:
            conn.close()
    out = []
    for fp_id, ws, be, fam in rows:
        n = sizes.get(fam, 1)
        if n < min_family_size:
            continue
        # Spot duration not stored; use the silence-aligned windowing
        # default (= ~20s per spot, conservative). Real boundaries are
        # in the source ads.json's block, this is just the detected
        # spot's local extent within it.
        out.append({
            "window_start_s": float(ws),
            "end_s": min(float(be), float(ws) + spot_dur_default),
            "family_id": int(fam),
            "family_size": int(n),
        })
    return out


def _cluster_coverage_pct(uuid_str, blocks):
    """% of total ad-block time covered by cluster-anchored spots
    with family_size ≥ 3. Returns (coverage_pct, n_anchored,
    total_block_s) — coverage_pct ∈ [0, 100] or None if no blocks."""
    if not blocks:
        return (None, 0, 0)
    total_block_s = sum(max(0, e - s) for s, e in blocks)
    if total_block_s <= 0:
        return (None, 0, 0)
    anchors = _cluster_anchored_for_recording(uuid_str)
    if not anchors:
        return (0.0, 0, total_block_s)
    # Build interval-set of anchored spans, intersect with blocks
    anchored = sorted([(a["window_start_s"], a["end_s"]) for a in anchors])
    # Merge overlapping anchors first
    merged = []
    for s, e in anchored:
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    # Intersection length with blocks
    cov_s = 0.0
    for bs, be in blocks:
        for ms, me in merged:
            ov_s = max(bs, ms)
            ov_e = min(be, me)
            if ov_e > ov_s:
                cov_s += ov_e - ov_s
    pct = min(100.0, 100.0 * cov_s / total_block_s)
    return (round(pct, 1), len(anchors), total_block_s)




def _read_whisper_windows(uuid_str):
    """Return list of {t, prob, text} from .whisper.json or empty."""
    p = HLS_DIR / f"_rec_{uuid_str}" / ".whisper.json"
    if not p.is_file():
        return []
    try:
        d = json.loads(p.read_text())
        return d.get("windows") or []
    except Exception:
        return []


def _compute_auto_confirm(uuid_str,
                          whisper_block_pct=0.70,
                          whisper_show_anomaly_p=0.70,
                          whisper_show_anomaly_n=3):
    """Decide whether a recording can be auto-confirmed without
    manual review. Combines Whisper-classifier per-window probs +
    cluster-anchored spot coverage + ad-block structure.

    Returns dict with per-block verdicts, show-region anomalies,
    overall verdict ('auto_confirm' | 'needs_review' | 'no_data'),
    and a confidence score in [0, 1]."""
    rec_dir = HLS_DIR / f"_rec_{uuid_str}"
    if not rec_dir.is_dir():
        return {"verdict": "no_data", "error": "unknown uuid"}
    # Auto-detected blocks come from the cutlist .txt via comskip
    # parser (= same path /recording/<uuid>/ads uses). ads.json is
    # only a transient cache that's regenerated on demand and
    # frequently absent after a head-deploy V2 invalidation.
    blocks = _rec_parse_comskip(rec_dir) or []
    if not blocks:
        ads_p = rec_dir / "ads.json"
        if ads_p.is_file():
            try:
                raw = json.loads(ads_p.read_text())
                blocks = [(float(b[0]), float(b[1]))
                          for b in (raw if isinstance(raw, list)
                                    else raw.get("ads") or [])]
            except Exception:
                blocks = []
    # Whisper windows + cluster anchors. Loaded BEFORE the no-blocks
    # branch so the "trivial auto-confirm" path can still validate
    # against whisper (= an empty cutlist is only safe to confirm if
    # whisper agrees there are no ads; cutlists are routinely empty
    # immediately after a V2 head deploy invalidation, where treating
    # them as ground-truth would lock in a wrong answer).
    windows = _read_whisper_windows(uuid_str)
    anchors = _cluster_anchored_for_recording(uuid_str,
                                              min_family_size=3)
    has_whisper = bool(windows)
    has_anchors = bool(anchors)
    # Whisper window centre (60 s windows: t=start, centre = t+30)
    win_center = lambda w: float(w.get("t", 0)) + 30.0
    win_prob   = lambda w: float(w.get("prob", 0))
    if not blocks:
        # No detected ads — only safe to trivially auto-confirm if
        # whisper exists AND has no contiguous high-prob run that
        # would indicate the detector missed something.
        if not has_whisper:
            return {"verdict": "needs_review", "confidence": 0.0,
                    "reason": "no detected ad blocks but no whisper "
                              "data to validate (likely V2-invalidated "
                              "cutlist awaiting re-detect)",
                    "has_whisper": False, "has_anchors": has_anchors,
                    "blocks": [], "show_anomalies": []}
        # Look for ≥whisper_show_anomaly_n contiguous high-prob windows;
        # if found, detector likely missed ads → defer to review.
        run_n = max_run = 0
        for w in windows:
            if win_prob(w) > whisper_show_anomaly_p:
                run_n += 1
                if run_n > max_run:
                    max_run = run_n
            else:
                run_n = 0
        if max_run >= whisper_show_anomaly_n:
            return {"verdict": "needs_review", "confidence": 0.3,
                    "reason": f"detector found no ads but whisper has "
                              f"{max_run} contiguous windows >"
                              f"{whisper_show_anomaly_p:.2f} — likely "
                              f"missed ads",
                    "has_whisper": True, "has_anchors": has_anchors,
                    "blocks": [], "show_anomalies": []}
        # Cohort-trust gate: if any other recording in the same
        # (title, channel) cohort has user-confirmed ad blocks, then
        # the detector finding 0 ads here is more likely an under-
        # bumper-coverage miss than a true "no ads" episode. Refuse
        # to auto-confirm — let the user manually verify or wait for
        # better bumper templates. Without this gate, Nick SpongeBob
        # auto-confirmed 91/118 recordings as "no ads" because
        # detect couldn't see the ads (pre-bumper-batch coverage was
        # 19+14 templates vs ProSieben 99+100); 11 user-reviewed in
        # the same cohort showed those episodes DO have ad blocks.
        if _cohort_has_user_ads(uuid_str):
            return {"verdict": "needs_review", "confidence": 0.2,
                    "reason": "no detected ad blocks but other "
                              "recordings of this show on this "
                              "channel have user-confirmed ads — "
                              "likely under-bumper-coverage miss",
                    "has_whisper": True, "has_anchors": has_anchors,
                    "blocks": [], "show_anomalies": []}
        return {"verdict": "auto_confirm", "confidence": 1.0,
                "reason": "no detected ad blocks, whisper agrees",
                "has_whisper": True, "has_anchors": has_anchors,
                "n_blocks": 0,
                "blocks": [], "show_anomalies": []}
    if not has_whisper:
        return {"verdict": "needs_review", "confidence": 0.0,
                "reason": "no whisper.json — can't auto-confirm",
                "has_whisper": False, "has_anchors": has_anchors,
                "blocks": [], "show_anomalies": []}
    # Per-block analysis
    block_results = []
    n_high_conf = 0
    for s, e in blocks:
        in_block = [w for w in windows
                    if s <= win_center(w) <= e]
        n_in = len(in_block)
        n_ad = sum(1 for w in in_block if win_prob(w) > 0.5)
        whisper_pct = (n_ad / n_in) if n_in else 0.0
        anchored = [a for a in anchors
                    if a["window_start_s"] >= s - 5
                    and a["end_s"] <= e + 5]
        anchor_count = len(anchored)
        # Verdict: high_conf when BOTH conditions hold
        is_high = (whisper_pct >= whisper_block_pct
                   and anchor_count >= 1)
        if is_high:
            n_high_conf += 1
        block_results.append({
            "start": round(s, 2), "end": round(e, 2),
            "whisper_n_windows": n_in,
            "whisper_pct_ad": round(whisper_pct, 3),
            "anchor_count": anchor_count,
            "verdict": "high_conf" if is_high else "uncertain",
        })
    # Show-region anomalies: streaks of high-prob whisper windows
    # OUTSIDE any ad-block (= candidate for "missed ad" the auto
    # detector failed to flag).
    in_block_set = set()
    for s, e in blocks:
        for i, w in enumerate(windows):
            if s <= win_center(w) <= e:
                in_block_set.add(i)
    show_anomalies = []
    streak_start = None
    streak_n = 0
    for i, w in enumerate(windows):
        if i in in_block_set:
            streak_start = None
            streak_n = 0
            continue
        if win_prob(w) >= whisper_show_anomaly_p:
            if streak_start is None:
                streak_start = w
                streak_n = 1
            else:
                streak_n += 1
        else:
            if streak_n >= whisper_show_anomaly_n and streak_start:
                show_anomalies.append({
                    "t_start": round(float(streak_start.get("t", 0)), 1),
                    "n_windows": streak_n,
                    "verdict": "possible missed ad",
                })
            streak_start = None
            streak_n = 0
    if streak_n >= whisper_show_anomaly_n and streak_start:
        show_anomalies.append({
            "t_start": round(float(streak_start.get("t", 0)), 1),
            "n_windows": streak_n,
            "verdict": "possible missed ad",
        })
    # Overall verdict + confidence
    n_blocks = len(blocks)
    block_pct = n_high_conf / n_blocks
    anomaly_penalty = min(0.5, 0.1 * len(show_anomalies))
    confidence = max(0.0, block_pct * 1.0 - anomaly_penalty)
    verdict = ("auto_confirm" if confidence >= 0.85
               and not show_anomalies
               else "needs_review")
    # Human-readable reason — populates the badge tooltip on
    # /recordings so the user sees WHY a recording is flagged
    # needs_review (= which signal disagreed with which) instead of
    # just a percentage. Pre-fix this dict shipped without `reason`,
    # which left the empty-string fallback in the JS tooltip.
    if verdict == "auto_confirm":
        reason = (f"all {n_blocks} block(s) high-conf via whisper, "
                  f"no show-region anomalies")
    elif n_high_conf == 0:
        reason = (f"detector found {n_blocks} block(s) but whisper "
                  f"agrees on 0 of them (= prob<{whisper_block_pct:.2f} "
                  f"in the overlapping windows) — likely false-positive "
                  f"detections")
    elif show_anomalies:
        reason = (f"{n_high_conf}/{n_blocks} blocks high-conf BUT "
                  f"{len(show_anomalies)} show-region anomaly run(s) — "
                  f"detector likely missed ads outside the marked blocks")
    else:
        reason = (f"only {n_high_conf}/{n_blocks} blocks high-conf "
                  f"via whisper (need ≥{0.85*n_blocks:.1f} for "
                  f"auto-confirm at threshold 0.85)")
    return {
        "verdict": verdict,
        "confidence": round(confidence, 3),
        "reason": reason,
        "n_blocks": n_blocks,
        "n_blocks_high_conf": n_high_conf,
        "n_show_anomalies": len(show_anomalies),
        "blocks": block_results,
        "show_anomalies": show_anomalies,
        "has_whisper": has_whisper,
        "has_anchors": has_anchors,
    }


# Path to the JSONL audit log of every auto-confirm action — one
# line per auto-confirmed recording for transparency + manual undo.
AUTO_CONFIRM_LOG = HLS_DIR / ".tvd-models" / "auto-confirmed.jsonl"
# Path to the on/off marker — when present, auto-confirm loop is
# PAUSED. Same pattern as AUTO_SCHED_PAUSE on /learning.
AUTO_CONFIRM_PAUSE = HLS_DIR / ".tvd-models" / "auto-confirm-paused"
# Per-loop cap so a classifier regression can't auto-confirm 100+
# recordings before user notices.
AUTO_CONFIRM_MAX_PER_LOOP = 5
# Min confidence + extra hard requirements (Whisper + cluster anchors)
# already enforced inside _compute_auto_confirm — this gate adds one
# more sanity check at the apply side.
AUTO_CONFIRM_MIN_CONFIDENCE = 0.85


def _auto_confirm_apply(uuid_str, verdict_dict):
    """Persist an auto-confirm decision: write ads_user.json with the
    auto-detected blocks + reviewed_at + auto_confirmed_at +
    auto_confirm_score. Skips if ads_user.json already exists (= user
    already touched it manually). Returns True on write."""
    rec_dir = HLS_DIR / f"_rec_{uuid_str}"
    user_p = rec_dir / "ads_user.json"
    if user_p.is_file():
        return False
    blocks = _rec_parse_comskip(rec_dir) or []
    if not blocks:
        ads_p = rec_dir / "ads.json"
        if ads_p.is_file():
            try:
                raw = json.loads(ads_p.read_text())
                blocks = [[float(b[0]), float(b[1])]
                          for b in (raw if isinstance(raw, list)
                                    else raw.get("ads") or [])]
            except Exception:
                blocks = []
    # If the active-learning surface flagged any frames in this
    # recording as uncertain, those are the highest-value labelling
    # targets — silently auto-confirming them would lock in the
    # model's GUESS as ground truth + remove the recording from the
    # /learning queue + 🎯 badge (both gate on `reviewed_at`). Apply
    # the auto blocks but OMIT `reviewed_at` so the user still sees
    # the recording as needing optional review of the uncertain
    # frames. /mark-reviewed sets `reviewed_at` later when the user
    # explicitly taps Geprüft, also folding in confirmed_show.
    has_uncertain = bool(_uncertain_for_recording(uuid_str))
    payload = {
        "ads": [[round(s, 2), round(e, 2)] for s, e in blocks],
        "deleted": [],
        "auto_confirmed_at": int(time.time()),
        "auto_confirm_score": verdict_dict.get("confidence"),
        "auto_confirm_n_blocks": verdict_dict.get("n_blocks"),
    }
    if not has_uncertain:
        # Clean recording (= no active-learning targets) → fully
        # close the loop. With reviewed_at set, /recordings drops the
        # 🎯 badge to 0 and /learning excludes the recording entirely.
        payload["reviewed_at"] = int(time.time())
    try:
        tmp = user_p.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload))
        tmp.replace(user_p)
    except Exception as e:
        print(f"[auto-confirm] write err {uuid_str[:8]}: {e}", flush=True)
        return False
    # Audit log
    try:
        AUTO_CONFIRM_LOG.parent.mkdir(parents=True, exist_ok=True)
        with AUTO_CONFIRM_LOG.open("a") as f:
            f.write(json.dumps({
                "ts": int(time.time()),
                "uuid": uuid_str,
                "confidence": verdict_dict.get("confidence"),
                "n_blocks": verdict_dict.get("n_blocks"),
                "n_high_conf": verdict_dict.get("n_blocks_high_conf"),
            }) + "\n")
    except Exception:
        pass
    print(f"[auto-confirm] {uuid_str[:8]}: confidence "
          f"{verdict_dict.get('confidence')} → wrote ads_user.json "
          f"({len(blocks)} blocks)", flush=True)
    return True


def _auto_confirm_loop():
    """Every 5 min: scan recordings without ads_user.json, compute
    multi-signal auto-confirm verdict, apply if SAFE.

    Pause via AUTO_CONFIRM_PAUSE marker file (= toggleable from
    /learning). Cap AUTO_CONFIRM_MAX_PER_LOOP per cycle so a
    classifier regression can't burn through the corpus."""
    time.sleep(60)  # let the gateway settle on startup
    while True:
        try:
            if AUTO_CONFIRM_PAUSE.exists():
                time.sleep(300)
                continue
            n_applied = 0
            for d in HLS_DIR.glob("_rec_*"):
                if n_applied >= AUTO_CONFIRM_MAX_PER_LOOP:
                    break
                if not d.is_dir():
                    continue
                if (d / "ads_user.json").is_file():
                    continue  # already user- or auto-confirmed
                uuid = d.name[5:]
                try:
                    v = _compute_auto_confirm(uuid)
                except Exception as e:
                    print(f"[auto-confirm] compute err {uuid[:8]}: {e}",
                          flush=True)
                    continue
                if v.get("verdict") != "auto_confirm":
                    continue
                conf = v.get("confidence") or 0
                if conf < AUTO_CONFIRM_MIN_CONFIDENCE:
                    continue
                # Need both signals for safety
                if not v.get("has_whisper"):
                    continue
                # has_anchors is FALSE if cluster anchors found 0 spots
                # OR if no fingerprints exist at all. Reject when there
                # are detected blocks — we need anchored spots to cross-
                # check them. Skip the gate for empty cutlists (n_blocks
                # == 0): there is nothing to anchor against, and the
                # detector+whisper agreement validated inside
                # _compute_auto_confirm is already the full safety story.
                if v.get("n_blocks", 0) > 0 and not v.get("has_anchors"):
                    continue
                if _auto_confirm_apply(uuid, v):
                    n_applied += 1
            if n_applied:
                print(f"[auto-confirm] loop applied {n_applied} "
                      f"recordings", flush=True)
        except Exception as e:
            print(f"[auto-confirm] loop err: {e}", flush=True)
        time.sleep(300)






@app.route("/api/recording/<uuid>/auto-confirm-undo", methods=["POST"])
def api_recording_auto_confirm_undo(uuid):
    """Revert an auto-confirm decision. Deletes ads_user.json IF it
    was auto-confirmed (= has auto_confirmed_at field) — won't touch
    a manually-edited file. Re-detects from cutlist on next scan."""
    rec_dir = HLS_DIR / f"_rec_{uuid}"
    user_p = rec_dir / "ads_user.json"
    if not user_p.is_file():
        return Response(json.dumps({"ok": False, "error": "no ads_user.json"}),
                        status=404, mimetype="application/json")
    try:
        cur = json.loads(user_p.read_text())
        if not isinstance(cur, dict) or not cur.get("auto_confirmed_at"):
            return Response(json.dumps({
                "ok": False,
                "error": "not an auto-confirmed entry — manual review present"}),
                status=409, mimetype="application/json")
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")
    user_p.unlink()
    return _cors(Response(json.dumps({"ok": True, "uuid": uuid}),
                          mimetype="application/json"))


def _whisper_text_for(uuid_str):
    """Concatenate all whisper window texts for one recording into
    one document. None if no whisper.json."""
    p = HLS_DIR / f"_rec_{uuid_str}" / ".whisper.json"
    if not p.is_file():
        return None
    try:
        d = json.loads(p.read_text())
        return " ".join((w.get("text") or "") for w in d.get("windows", []))
    except Exception:
        return None


def _whisper_jaccard(text_a, text_b, n=4):
    """Char-n-gram Jaccard similarity (0..1). 0 if either empty."""
    if not text_a or not text_b:
        return 0.0
    sa = {text_a[i:i+n] for i in range(len(text_a) - n + 1)}
    sb = {text_b[i:i+n] for i in range(len(text_b) - n + 1)}
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def _dhashes_for(uuid_str):
    """All dHash hashes from all spots of this recording. Returns
    list of int64 hashes (possibly empty)."""
    out = []
    try:
        with _spot_fp_lock:
            conn = _spot_fp_open()
            try:
                rows = conn.execute(
                    "SELECT dhashes FROM fingerprints "
                    "WHERE uuid = ? AND dhashes IS NOT NULL",
                    (uuid_str,)).fetchall()
            finally:
                conn.close()
    except Exception:
        return []
    for (blob,) in rows:
        b = bytes(blob)
        for i in range(0, len(b), 8):
            chunk = b[i:i+8]
            if len(chunk) == 8:
                out.append(int.from_bytes(chunk, "big"))
    return out


def _dhash_overlap(hashes_a, hashes_b, max_bit_diff=12):
    """Fraction of dHashes in the SHORTER list that have any match
    within `max_bit_diff` bits in the OTHER list. 0..1."""
    if not hashes_a or not hashes_b:
        return 0.0
    shorter, longer = (hashes_a, hashes_b) if len(hashes_a) <= len(hashes_b) \
                      else (hashes_b, hashes_a)
    bit_count = int.bit_count
    matches = 0
    for h in shorter:
        for k in longer:
            if bit_count(h ^ k) <= max_bit_diff:
                matches += 1
                break
    return matches / len(shorter)


def _find_duplicate_recordings(min_cluster_overlap=0.75,
                               min_anchors=5):
    """Find pairs of recordings that are SAME CONTENT (not just same
    show — the SAME broadcast).

    Three signals, weighted:
      1. Same disp_title AND broadcast within ±5 min (= autorec hit
         the same EPG event twice). Different episodes of the same
         series excluded by the tight time-window.
      2. Cluster-anchored spot overlap ≥min_cluster_overlap
         (= shared known commercial spots).
      3. Whisper-text Jaccard similarity ≥0.5 (= same dialogue =
         same broadcast even with different titles).
      4. dHash visual overlap ≥0.4 (= same keyframes appear).

    Pair survives if signal 1 fires OR (signal 2 + signal 3 + signal 4
    weighted score ≥ 1.5; e.g. cluster 0.7 + whisper 0.5 + dhash 0.4).
    All 3 sub-scores reported for UI decision-making."""
    by_title = {}
    rec_meta = {}
    # Pull broadcast start times from tvh — needed to filter
    # "different episodes of same series" out of signal 1.
    tvh_start = {}
    try:
        td = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000",
            timeout=8).read())
        for e in td.get("entries", []):
            if e.get("uuid"):
                tvh_start[e["uuid"]] = e.get("start") or 0
    except Exception:
        pass
    for d in HLS_DIR.glob("_rec_*"):
        if not d.is_dir():
            continue
        uuid = d.name[5:]
        title = _show_title_for_rec(d) or _rec_dvr_title(uuid) or ""
        if not title:
            continue
        dur = _rec_duration_s(d)
        if dur < 60:
            continue
        rec_meta[uuid] = {
            "title": title,
            "duration_s": dur,
            "channel_slug": _rec_channel_slug(uuid) or "",
            "rec_dir": d,
            "start_ts": tvh_start.get(uuid, 0),
        }
        by_title.setdefault(title, []).append(uuid)

    pairs = []

    # --- Signal 1: same title + same broadcast (start within ±5min) ---
    # Tight window because SpongeBob etc. air the same show multiple
    # times per day = different episodes. ±5 min only catches the
    # "autorec hit the same EPG event twice on different muxes" case.
    SAME_BROADCAST_WIN = 5 * 60
    for title, uuids in by_title.items():
        if len(uuids) < 2:
            continue
        for i in range(len(uuids)):
            for j in range(i + 1, len(uuids)):
                a, b = uuids[i], uuids[j]
                ta = rec_meta[a]["start_ts"]
                tb = rec_meta[b]["start_ts"]
                if not (ta and tb):
                    continue  # missing start — can't tell
                if abs(ta - tb) > SAME_BROADCAST_WIN:
                    continue  # different broadcast → different episode
                da = rec_meta[a]["duration_s"]
                db = rec_meta[b]["duration_s"]
                ratio = min(da, db) / max(da, db) if max(da, db) > 0 else 0
                if ratio < 0.95:
                    continue
                pairs.append({
                    "uuid_a": a, "uuid_b": b,
                    "title": title,
                    "channel_a": rec_meta[a]["channel_slug"],
                    "channel_b": rec_meta[b]["channel_slug"],
                    "duration_a": round(da, 1),
                    "duration_b": round(db, 1),
                    "start_diff_s": int(abs(ta - tb)),
                    "basis": "same_broadcast",
                    "score": round(ratio, 3),
                })

    # --- Pre-build per-uuid signals ONCE (= O(n) per signal) ---
    sig1_pairs = {(p["uuid_a"], p["uuid_b"]) for p in pairs}
    try:
        with _spot_fp_lock:
            conn = _spot_fp_open()
            try:
                rows = conn.execute(
                    "SELECT fp.uuid, fm.family_id "
                    "FROM family_members fm "
                    "JOIN fingerprints fp ON fp.id = fm.fp_id "
                    "WHERE fm.family_id IN ("
                    "  SELECT family_id FROM family_members "
                    "  GROUP BY family_id HAVING COUNT(*) >= 3"
                    ")").fetchall()
            finally:
                conn.close()
    except Exception:
        rows = []
    by_uuid_anchors = {}
    for uuid, fam_id in rows:
        by_uuid_anchors.setdefault(uuid, set()).add(fam_id)
    # Cache whisper text + dhash list per uuid (lazy — only when first
    # asked, since not every uuid will be paired)
    _wcache = {}
    _dcache = {}
    def _get_whisper(u):
        if u not in _wcache:
            _wcache[u] = _whisper_text_for(u)
        return _wcache[u]
    def _get_dhashes(u):
        if u not in _dcache:
            _dcache[u] = _dhashes_for(u)
        return _dcache[u]

    # --- Signal 2/3/4 combined: candidate pairs from anchors ---
    uuids_with_anchors = sorted(by_uuid_anchors.keys())
    for i in range(len(uuids_with_anchors)):
        for j in range(i + 1, len(uuids_with_anchors)):
            a, b = uuids_with_anchors[i], uuids_with_anchors[j]
            if (a, b) in sig1_pairs or (b, a) in sig1_pairs:
                continue
            if a not in rec_meta or b not in rec_meta:
                continue
            sa = by_uuid_anchors[a]
            sb = by_uuid_anchors[b]
            # Cheap pre-filter: need at least min_anchors on each side
            # and SOME shared. Anything below that = not a candidate
            # for further signals.
            if min(len(sa), len(sb)) < min_anchors:
                continue
            shared = len(sa & sb)
            cluster_ovl = shared / min(len(sa), len(sb))
            # Skip non-candidates early — anything below 0.4 cluster
            # overlap won't survive even with strong whisper+dhash.
            if cluster_ovl < 0.4:
                continue
            # Whisper Jaccard
            wa = _get_whisper(a)
            wb = _get_whisper(b)
            whisper_ovl = _whisper_jaccard(wa, wb) if (wa and wb) else 0.0
            # dHash overlap
            dha = _get_dhashes(a)
            dhb = _get_dhashes(b)
            dhash_ovl = _dhash_overlap(dha, dhb) if (dha and dhb) else 0.0
            # Combined score: weighted sum, max 1.0.
            #   cluster:  weight 0.4 — broad audio-pattern match
            #   whisper:  weight 0.4 — strong dialogue match (= same broadcast)
            #   dhash:    weight 0.2 — visual confirmation
            combined = (0.4 * cluster_ovl
                        + 0.4 * whisper_ovl
                        + 0.2 * dhash_ovl)
            # Accept rules — REQUIRE whisper ≥ 0.3 (= same dialogue is
            # the strongest "same broadcast" signal; cluster + dHash
            # alone match channels-with-shared-ad-slates without
            # distinguishing different episodes). Whisper transcripts
            # are noisy so 0.3 is the practical lower bound for "same
            # dialogue, just different ASR runs".
            # German ASR has a natural ~0.3 n-gram floor from common
            # function words (und, ich, sie, das); 0.4 is the actual
            # "same dialogue" threshold. Same-set shows (Lenßen hilft
            # courtroom, GZSZ apartment) trip cluster + dhash but the
            # different DIALOGUE keeps whisper below 0.4 → correctly
            # excluded.
            if whisper_ovl < 0.4:
                continue
            if combined < 0.5:
                continue
            pairs.append({
                "uuid_a": a, "uuid_b": b,
                "title_a": rec_meta[a]["title"],
                "title_b": rec_meta[b]["title"],
                "channel_a": rec_meta[a]["channel_slug"],
                "channel_b": rec_meta[b]["channel_slug"],
                "shared_anchors": shared,
                "n_anchors_a": len(sa),
                "n_anchors_b": len(sb),
                "cluster_overlap": round(cluster_ovl, 3),
                "whisper_jaccard": round(whisper_ovl, 3),
                "dhash_overlap": round(dhash_ovl, 3),
                "basis": "multi_signal",
                "score": round(combined, 3),
            })

    pairs.sort(key=lambda p: -p["score"])
    return pairs


@app.route("/api/internal/duplicate-recordings")
def api_internal_duplicate_recordings():
    """List of suspected duplicate recording pairs across the corpus.
    Used by /recordings to flag redundancies + by user to manually
    decide which copy to keep."""
    try:
        min_overlap = max(0.3,
                          min(1.0, float(request.args.get("min_overlap")
                                         or 0.75)))
    except Exception:
        min_overlap = 0.75
    pairs = _find_duplicate_recordings(min_cluster_overlap=min_overlap)
    return _cors(Response(json.dumps({
        "pairs": pairs, "n": len(pairs)}),
        mimetype="application/json"))












def _spot_fp_backfill_dhashes():
    """Walk fingerprints with NULL dhashes, extract from source HLS
    segments, write back. Parallelised — each ffmpeg call is mostly
    I/O bound, 4-way pool empirically saturates the Pi without
    starving other gateway work."""
    with _spot_fp_lock:
        conn = _spot_fp_open()
        try:
            rows = conn.execute(
                "SELECT id, uuid, window_start_s "
                "FROM fingerprints WHERE dhashes IS NULL").fetchall()
        finally:
            conn.close()
    print(f"[spot-fp] backfill: {len(rows)} fingerprints to dHash",
          flush=True)
    n_filled = n_failed = 0
    n_done_lock = threading.Lock()

    def _job(row):
        nonlocal n_filled, n_failed
        fp_id, uuid_str, ws = row
        rec_dir = HLS_DIR / f"_rec_{uuid_str}"
        if not rec_dir.is_dir():
            with n_done_lock:
                n_failed += 1
            return None
        # 25 s default spot length — most cluster 20-30 s and dHash
        # is forgiving on overshoot (we just sample more frames from
        # the next ad segment, still within the same spot family).
        dh = _extract_dhashes(rec_dir, ws, 25.0)
        if dh is None:
            with n_done_lock:
                n_failed += 1
            return None
        return (fp_id, dh)

    BATCH = 64
    with ThreadPoolExecutor(max_workers=4) as pool:
        for batch_start in range(0, len(rows), BATCH):
            batch = rows[batch_start:batch_start + BATCH]
            results = list(pool.map(_job, batch))
            updates = [r for r in results if r is not None]
            if updates:
                with _spot_fp_lock:
                    conn = _spot_fp_open()
                    try:
                        conn.executemany(
                            "UPDATE fingerprints SET dhashes = ? "
                            "WHERE id = ?",
                            [(sqlite3.Binary(dh), fp_id)
                             for fp_id, dh in updates])
                        conn.commit()
                    finally:
                        conn.close()
                n_filled += len(updates)
            done = n_filled + n_failed
            if done % 256 < BATCH:
                print(f"[spot-fp] backfill: {done}/{len(rows)} "
                      f"({n_filled} ok, {n_failed} fail)", flush=True)
    return (n_filled, n_failed)






@app.route("/api/internal/recording-uuids")
def api_internal_recording_uuids():
    """All currently-valid recording uuids (= those with an actual
    rec_dir on disk). Mac daemon uses this for orphan-GC of its
    local .ts source cache: any cached uuid not in this list is
    a deleted recording, safe to evict."""
    out = []
    for d in HLS_DIR.glob("_rec_*"):
        if not d.is_dir():
            continue
        out.append(d.name[5:])
    return _cors(Response(json.dumps({"uuids": sorted(out)}),
                            mimetype="application/json"))


@app.route("/api/internal/drop-pi-source/<uuid>", methods=["POST"])
def api_internal_drop_pi_source(uuid):
    """Delete the tvh-original .ts on Pi for a recording the Mac daemon
    has just cached on its T7 SSD. Idempotent + safe-guarded.

    Daemon flow (driven from tv-thumbs-daemon's prefetch loop): after a
    `cached <uuid> (X MB in Ys)` line, the daemon POSTs here. Gateway
    verifies safety and deletes; daemon's cache becomes authoritative.

    Without this drop step, Pi-disk fills with ~40 GB/week of fresh
    DVR-output that duplicates the Mac's T7 cache. With it, Pi only
    keeps the HLS-VOD remux (= ~1/2 the size of original .ts, served
    directly by Caddy for playback).

    Safety gates (any failure → return {ok: False, reason: ...}):
      1. tvh DVR entry exists for uuid + sched_status == "completed"
         (= not currently recording, not failed)
      2. HLS-VOD playlist exists at /mnt/tv/hls/_rec_<uuid>/index.m3u8
         (= playback works without the original)
      3. tvh's filename field points to an existing .ts file
      4. Recording is > 3 days old (= conservative buffer for human
         review / re-trigger; can be relaxed via ?min_age_h=N query)

    Returns: {ok, deleted_bytes, title} on success or
    {ok: False, reason} on any guard failure."""
    try:
        d = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000", timeout=10).read())
    except Exception as e:
        return _cors(Response(json.dumps({"ok": False,
            "reason": f"tvh unreachable: {e}"}),
            mimetype="application/json", status=502))
    entry = next((e for e in d.get("entries", []) if e.get("uuid") == uuid),
                  None)
    if not entry:
        return _cors(Response(json.dumps({"ok": False,
            "reason": "unknown uuid"}), mimetype="application/json",
            status=404))
    if entry.get("sched_status") != "completed":
        return _cors(Response(json.dumps({"ok": False,
            "reason": f"not completed: sched_status={entry.get('sched_status')}"}),
            mimetype="application/json", status=409))
    # Age guard — default 3 days, overrideable for emergency dedup runs
    min_age_h = int(request.args.get("min_age_h", "72"))
    age_h = (time.time() - (entry.get("start") or 0)) / 3600
    if age_h < min_age_h:
        return _cors(Response(json.dumps({"ok": False,
            "reason": f"too fresh: age={age_h:.1f}h < {min_age_h}h"}),
            mimetype="application/json", status=409))
    # HLS-VOD playable check
    hls_index = HLS_DIR / f"_rec_{uuid}" / "index.m3u8"
    if not hls_index.exists():
        return _cors(Response(json.dumps({"ok": False,
            "reason": "no HLS-VOD on disk — playback would break"}),
            mimetype="application/json", status=409))
    # Map the recording's absolute path to THIS container's view. tv-receiver
    # stores host paths like /mnt/tv/<title>/...ts; the gateway mounts host
    # /mnt/tv at /recordings (see docker-compose), so /mnt/tv/ → /recordings/
    # for the stat + unlink. (Legacy tvh entries already used /recordings/ —
    # the replace is then a no-op.) Before 2026-05-29 this translated the
    # WRONG way (/recordings/→/mnt/tv/) and checked a path the container can't
    # see → every drop returned "file already gone" → 330 GB never reclaimed.
    fname = entry.get("filename") or ""
    if not fname:
        return _cors(Response(json.dumps({"ok": False,
            "reason": "no filename in DVR entry (= already dedup'd?)"}),
            mimetype="application/json"))
    cpath = Path(fname.replace("/mnt/tv/", "/recordings/", 1))
    if not cpath.is_file():
        return _cors(Response(json.dumps({"ok": False,
            "reason": f"file already gone: {cpath}"}),
            mimetype="application/json"))
    try:
        sz = cpath.stat().st_size
        cpath.unlink()
    except Exception as e:
        return _cors(Response(json.dumps({"ok": False,
            "reason": f"delete failed: {e}"}),
            mimetype="application/json", status=500))
    print(f"[drop-pi-source] {uuid[:8]} {entry.get('disp_title','?')[:30]} "
          f"freed {sz/1024**2:.0f} MB", flush=True)
    return _cors(Response(json.dumps({"ok": True,
        "deleted_bytes": sz,
        "title": entry.get("disp_title", ""),
        "age_h": round(age_h, 1)}),
        mimetype="application/json"))


@app.route("/api/internal/cleanup-orphans", methods=["POST"])
def api_internal_cleanup_orphans():
    """Delete _rec_<uuid>/ HLS-bundle dirs whose recording no longer
    exists in tvh DVR. Triggered when tvh deletes a DVR entry but
    the bundle stays behind — usually a race against an in-flight
    HLS-write that briefly held the dir open. Result: thumbs/.requested
    marker keeps showing in the queue forever, daemon retries every
    cycle with a 404 source. Daemon polls this on its hourly orphan-GC."""
    import shutil
    tvh_uuids = set()
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000", timeout=10).read())
        for e in data.get("entries", []):
            if e.get("uuid"):
                tvh_uuids.add(e["uuid"])
    except Exception as e:
        return _cors(Response(json.dumps({"ok": False, "err": str(e)}),
                            mimetype="application/json", status=500))
    if not tvh_uuids:
        # Empty tvh response → bail (= unsafe to wipe everything)
        return _cors(Response(json.dumps(
            {"ok": False, "err": "tvh returned 0 uuids"}),
            mimetype="application/json", status=503))
    now = time.time()
    removed = []
    for d in HLS_DIR.glob("_rec_*"):
        if not d.is_dir():
            continue
        uuid = d.name[5:]
        if uuid in tvh_uuids:
            continue
        # Safety: only delete if dir mtime > 1h old (= avoid race
        # against a recording that's currently mid-finalisation).
        try:
            if now - d.stat().st_mtime < 3600:
                continue
        except Exception:
            continue
        # Same protection as the in-line GC at line ~12832: never delete
        # dirs with user-labeled training data, even on UUID-mismatch.
        if (d / "ads_user.json").exists():
            continue
        try:
            shutil.rmtree(d)
            removed.append(uuid)
        except Exception as e:
            print(f"  cleanup-orphans: failed {uuid}: {e}", flush=True)
    return _cors(Response(json.dumps(
        {"ok": True, "removed": removed, "n_removed": len(removed)}),
        mimetype="application/json"))


def _detect_pending_scan(marker_name):
    """Shared scan for .detect-requested / .detect-requested-low.
    Filters out in-progress recordings + ones whose cutlist already
    exists (cleans up stale markers in passing)."""
    in_progress = set()
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=500", timeout=5).read())
        for e in data.get("entries", []):
            if e.get("sched_status") in ("recording", "scheduled"):
                if e.get("uuid"):
                    in_progress.add(e["uuid"])
    except Exception:
        pass
    out = []
    for marker in sorted(HLS_DIR.glob(f"_rec_*/{marker_name}")):
        rec_dir = marker.parent
        uuid = rec_dir.name[5:]
        if uuid in in_progress:
            continue
        # Non-empty non-sidecar .txt = real cutlist already produced.
        # An empty file is the head-invalidation truncation marker
        # (filename kept so train-head can derive .ts basename, content
        # cleared so we re-detect on the new model).
        existing = [t for t in rec_dir.glob("*.txt")
                    if not any(t.name.endswith(s) for s in
                                (".logo.txt", ".cskp.txt", ".tvd.txt",
                                 ".trained.logo.txt"))
                       and not t.name.startswith(".")
                       and t.stat().st_size > 0]
        if existing:
            try: marker.unlink()
            except Exception: pass
            continue
        out.append({"uuid": uuid})
    return out










_TVD_LOGO_CNN_DIR = HLS_DIR / ".tvd-logo-cnn"






_TVD_BUMPER_DIR = HLS_DIR / ".tvd-bumpers"


@app.route("/api/recording/<uuid>/bumper-capture", methods=["POST"])
def api_recording_bumper_capture(uuid):
    """User marks a bumper window in the player. We extract one frame
    per second across [start_s, end_s] from the source .ts and write
    them as PNGs into /mnt/tv/hls/.tvd-bumpers/<channel_slug>/<kind>/.

    Body: {"start_s": float, "end_s": float, "kind": "start"|"end"}.
    `kind` defaults to "end" for backward compatibility — the player's
    capture button historically only produced end-bumpers (sixx
    "WIE SIXX IST DAS DENN?"), but start-bumpers (sixx "WERBUNG"-card)
    work too now. Each kind feeds an independent per-frame conf stream
    in tv-detect and only snaps its own boundary.
    Returns: {ok, slug, kind, count, files}."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    if not out_dir.exists():
        return Response(json.dumps({"ok": False, "error": "unknown recording"}),
                        status=404, mimetype="application/json")
    body = request.get_json(silent=True) or {}
    try:
        s = float(body.get("start_s"))
        e = float(body.get("end_s"))
    except (TypeError, ValueError):
        return Response(json.dumps({"ok": False, "error": "start_s/end_s required"}),
                        status=400, mimetype="application/json")
    kind = (body.get("kind") or "end").lower()
    if kind not in ("start", "end"):
        return Response(json.dumps({"ok": False,
            "error": "kind must be 'start' or 'end'"}),
                        status=400, mimetype="application/json")
    if e <= s or e - s > 30:
        return Response(json.dumps({"ok": False,
            "error": "window must be 0 < (end-start) <= 30s"}),
                        status=400, mimetype="application/json")
    slug = _rec_channel_slug(uuid) or ""
    if not slug:
        return Response(json.dumps({"ok": False, "error": "no channel slug"}),
                        status=400, mimetype="application/json")
    src = _rec_source_or_recover(uuid)
    if not src or not Path(src).exists():
        return Response(json.dumps({"ok": False,
            "error": "source .ts missing and no HLS-VOD fallback"}),
            status=404, mimetype="application/json")
    bdir = _TVD_BUMPER_DIR / slug / kind
    bdir.mkdir(parents=True, exist_ok=True)
    base = f"bumper-{uuid[:8]}-{int(s)}"
    pattern = str(bdir / f"{base}-%02d.png")
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
           "-ss", f"{s:.2f}", "-t", f"{e-s:.2f}",
           "-i", src,
           "-vf", "fps=1,scale=720:576",
           pattern]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except (subprocess.TimeoutExpired, OSError) as exc:
        return Response(json.dumps({"ok": False,
            "error": f"ffmpeg failed: {exc}"}),
                        status=500, mimetype="application/json")
    if r.returncode != 0:
        return Response(json.dumps({"ok": False,
            "error": f"ffmpeg rc={r.returncode}: {r.stderr[-200:]}"}),
                        status=500, mimetype="application/json")
    # No auto-filter: brightness-based heuristics don't separate
    # bumpers from show frames cleanly across channels. Pro7's pink
    # "We love…" card and a typical HIMYM interior shot have nearly
    # identical YAVG. Instead the user prunes via the /learning page
    # which shows a thumbnail + delete button per template.
    files = sorted(p.name for p in bdir.glob(f"{base}-*.png"))
    skipped = []
    note = ""
    if skipped:
        note = f", skipped {len(skipped)} oversized (likely show)"

    # Auto-align: if the user has an ad-block whose matching boundary
    # sits within ±30 s of the captured bumper window, snap it to the
    # bumper position. The bumper is the precise transition frame the
    # user just identified — leaving the rougher block boundary in
    # place would waste that signal. For kind=start, snap the block
    # START to the FIRST captured frame (= first ad frame for a
    # WERBUNG-card); for kind=end, snap the block END to one frame
    # after the LAST captured frame (= first show frame after the
    # bumper window). Returns the alignment in the response so the
    # player can show a confirmation toast.
    aligned = None
    user_cache = out_dir / "ads_user.json"
    if user_cache.exists():
        try:
            raw = json.loads(user_cache.read_text())
        except Exception:
            raw = None
        if isinstance(raw, dict) and isinstance(raw.get("ads"), list):
            blocks = raw["ads"]
            best = None  # (block_idx, dist, old_val, new_val)
            for i, blk in enumerate(blocks):
                if not (isinstance(blk, list) and len(blk) >= 2):
                    continue
                bs, be = float(blk[0]), float(blk[1])
                if kind == "start":
                    new_val = float(s)
                    d = abs(bs - new_val)
                    old_val = bs
                else:
                    new_val = float(e)
                    d = abs(be - new_val)
                    old_val = be
                if d > 30:
                    continue
                if best is None or d < best[1]:
                    best = (i, d, old_val, new_val)
            if best:
                i, d, old_val, new_val = best
                # Refuse if the new boundary would invert or zero the
                # block (start >= end). Better to bail than to corrupt
                # the user's manual block.
                bs, be = float(blocks[i][0]), float(blocks[i][1])
                if kind == "start":
                    if new_val < be:
                        blocks[i][0] = new_val
                        aligned = {"block": i, "boundary": "start",
                                   "old": old_val, "new": new_val}
                else:
                    if new_val > bs:
                        blocks[i][1] = new_val
                        aligned = {"block": i, "boundary": "end",
                                   "old": old_val, "new": new_val}
                if aligned:
                    raw["ads"] = blocks
                    user_cache.write_text(json.dumps(raw))

    n_invalidated = _invalidate_detect_for_channel(slug, exclude_uuid=uuid)
    align_log = (f" — aligned block {aligned['block']} "
                 f"{aligned['boundary']} {aligned['old']:.1f}→"
                 f"{aligned['new']:.1f}s") if aligned else ""
    print(f"[bumper-capture {uuid[:8]}] {slug}/{kind}: {len(files)} "
          f"frames in [{s:.1f}, {e:.1f}]s{note}{align_log} — invalidated "
          f"{n_invalidated} recording(s) for re-detect", flush=True)
    return _cors(Response(json.dumps({
        "ok": True, "slug": slug, "kind": kind, "count": len(files),
        "files": files, "skipped_oversized": skipped,
        "aligned": aligned,
        "invalidated": n_invalidated}),
        mimetype="application/json"))


def _mark_recording_for_redetect(rec_dir):
    """Drop a `.detect-requested` marker AND truncate the cutlist .txt
    + delete ads.json. Without truncating the .txt the daemon's
    detect-pending scan silently removes the marker on next poll
    because a non-empty cutlist is treated as "already done"
    (_detect_pending_scan above). Mirrors the head-invalidate pattern
    around line 11157. Sidecar .txt files (.logo.txt etc) are
    preserved because their filenames don't track the cutlist."""
    SIDECAR = (".logo.txt", ".cskp.txt", ".tvd.txt", ".trained.logo.txt")
    for t in rec_dir.glob("*.txt"):
        if any(t.name.endswith(s) for s in SIDECAR):
            continue
        try: t.write_text("")
        except Exception: pass
    ads_p = rec_dir / "ads.json"
    if ads_p.exists():
        try: ads_p.unlink()
        except Exception: pass
    try:
        (rec_dir / ".detect-requested").write_text(
            json.dumps({"ts": time.time()}))
    except Exception:
        return False
    return True


def _invalidate_detect_for_channel(slug, exclude_uuid=None):
    """Mark every recording of the given channel as detect-pending so
    the daemon re-runs detection with the new bumper template set.
    Called after a bumper-template add/delete — old auto-cutlists
    stale relative to the new templates would persist forever
    otherwise. Returns count of invalidated recordings.

    `exclude_uuid` skips one specific recording — used when the
    invalidation source IS a user-action on that recording (= they're
    actively reviewing it, manual ads_user.json is the ground truth,
    re-detecting WHILE they're in the player would wipe their just-
    finished cutlist and trigger the prewarm/cskip loop that strips
    the .txt mid-edit).
    """
    if not slug or not HLS_DIR.exists():
        return 0
    n = 0
    for d in HLS_DIR.glob("_rec_*"):
        uuid = d.name[5:]
        if exclude_uuid and uuid == exclude_uuid:
            continue
        try:
            if _rec_channel_slug(uuid) != slug:
                continue
            if _mark_recording_for_redetect(d):
                n += 1
        except Exception:
            continue
    return n






























@app.route("/api/recording/<uuid>/skip-event", methods=["POST"])
def api_recording_skip_event(uuid):
    """User pressed the Werbung-Skip button while an auto-detected
    block was active. Implicit confirmation that the block IS a real
    ad break (= positive class with bonus weight at training time).
    Cleaner signal than scrub-through or completion-percent — those
    have too many alternative interpretations.

    Stored as `confirmed_ad_skips: [t1, t2, ...]` in ads_user.json,
    deduped within ±5 s so repeated taps don't pile up. train-head.py
    treats each timestamp as a forced label=1 with sample_weight ×1.5
    (between auto's 1.0 and explicit-edit's 2.0)."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    if not out_dir.exists():
        return Response(json.dumps({"ok": False, "error": "unknown recording"}),
                        status=404, mimetype="application/json")
    try:
        body = request.get_json(silent=True) or {}
        t = float(body.get("t", -1))
    except Exception:
        t = -1
    if t < 0:
        return Response(json.dumps({"ok": False, "error": "t required"}),
                        status=400, mimetype="application/json")
    user_cache = out_dir / "ads_user.json"
    user_ads, deleted = _read_user_ads(user_cache)
    cur = {}
    if user_cache.exists():
        try:
            raw = json.loads(user_cache.read_text())
            if isinstance(raw, dict):
                cur = raw
        except Exception:
            pass
    skips = [float(x) for x in cur.get("confirmed_ad_skips", []) or []]
    # Dedupe within ±5 s — repeated skip-taps in same block are noise
    if any(abs(t - s) <= 5.0 for s in skips):
        return _cors(Response(json.dumps({"ok": True, "added": False,
                                            "total": len(skips)}),
                               mimetype="application/json"))
    skips.append(round(t, 1))
    skips.sort()
    payload = dict(cur)
    payload.update({"ads": user_ads, "deleted": deleted,
                    "confirmed_ad_skips": skips})
    try:
        tmp = user_cache.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload))
        tmp.replace(user_cache)
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")
    return _cors(Response(json.dumps({"ok": True, "added": True,
                                       "total": len(skips)}),
                           mimetype="application/json"))


@app.route("/api/recording/<uuid>/mark-reviewed", methods=["POST"])
def api_recording_mark_reviewed(uuid):
    """User explicitly tapped 'Geprüft' in the player. Two effects:

      1. UI: hides the 🎯 active-learning badge on the recordings list
         (gateway compares uncertain count vs reviewed_at + confirmed_show).
      2. TRAINING: each currently-uncertain frame in this recording that
         the user did NOT cover with an ad block becomes a confirmed-
         show negative-sample at training time. The next nightly retrain
         picks them up via train-head.py's confirmed_show handling.

    Persisted as `confirmed_show: [t1, t2, ...]` and `reviewed_at: <ts>`
    in ads_user.json alongside the existing ads/deleted fields.
    Idempotent — re-tapping just refreshes the timestamp."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    if not out_dir.exists():
        return Response(json.dumps({"ok": False, "error": "unknown recording"}),
                        status=404, mimetype="application/json")
    user_cache = out_dir / "ads_user.json"
    user_ads, deleted = _read_user_ads(user_cache)
    # Read existing confirmed_show so re-tapping doesn't drop frames
    # that were confirmed-show in a previous review cycle.
    confirmed = []
    if user_cache.exists():
        try:
            cur = json.loads(user_cache.read_text())
            if isinstance(cur, dict):
                confirmed = [float(x) for x in cur.get("confirmed_show", [])]
        except Exception:
            pass
    confirmed_set = set(round(x, 1) for x in confirmed)
    # Preserve auto-confirm provenance fields if this recording was
    # previously auto-confirmed (= ads_user.json written by the
    # auto-confirm loop without `reviewed_at` because uncertain
    # frames existed). The user is now closing that loop manually
    # via Geprüft; the auto-confirm history shouldn't disappear.
    auto_meta = {}
    if user_cache.exists():
        try:
            cur = json.loads(user_cache.read_text())
            if isinstance(cur, dict):
                for k in ("auto_confirmed_at", "auto_confirm_score",
                          "auto_confirm_n_blocks"):
                    if k in cur:
                        auto_meta[k] = cur[k]
        except Exception:
            pass
    # Pull this recording's currently-uncertain timestamps
    new_confirmed = 0
    for u in _uncertain_for_recording(uuid):
        t = float(u.get("t", 0))
        # Skip if inside any user-marked ad block (= it IS an ad,
        # the model just was uncertain within the block)
        if any(s <= t <= e for s, e in user_ads):
            continue
        key = round(t, 1)
        if key in confirmed_set:
            continue
        confirmed.append(round(t, 1))
        confirmed_set.add(key)
        new_confirmed += 1
    payload = {"ads": user_ads, "deleted": deleted,
               "confirmed_show": sorted(confirmed),
               "reviewed_at": int(time.time()),
               **auto_meta}
    try:
        tmp = user_cache.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload))
        tmp.replace(user_cache)
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")
    return _cors(Response(json.dumps({
        "ok": True, "added": new_confirmed,
        "total_confirmed": len(confirmed)}),
        mimetype="application/json"))


def _dvr_entry_for_uuid(uuid_str):
    """Fetch one tvh DVR entry by uuid. Returns dict or None."""
    try:
        d = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000",
            timeout=8).read())
        for e in d.get("entries", []):
            if e.get("uuid") == uuid_str:
                return e
    except Exception:
        pass
    return None


PER_SHOW_DRIFT_PATH = HLS_DIR / ".tvd-models" / "per-show-drift.json"
_per_show_drift_lock = threading.Lock()


def _aggregate_per_show_drift():
    """Walk all reviewed recordings (= ads_user.json with show_start_s),
    join with tvh DVR entries to derive EPG-vs-broadcast drift, group by
    disp_title, persist to PER_SHOW_DRIFT_PATH.

    drift_s = (start_real + show_start_s) - epg_start
              = how many seconds the broadcaster ran ahead of EPG
              (negative = early, positive = late).

    Suggested start_extra = ceil(max(|drift|) / 60) + 2  (=2 min buffer)."""
    try:
        d = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=3000", timeout=10).read())
    except Exception as e:
        print(f"[show-drift] tvh fetch err: {e}", flush=True)
        return {}
    by_uuid = {e.get("uuid"): e for e in d.get("entries", []) if e.get("uuid")}
    samples = {}  # title → [{drift_s, uuid, start_real, show_start_s}, ...]
    for rec_dir in HLS_DIR.glob("_rec_*"):
        user_p = rec_dir / "ads_user.json"
        if not user_p.is_file():
            continue
        try:
            data = json.loads(user_p.read_text())
            if not isinstance(data, dict):
                continue
            ss = data.get("show_start_s")
            if ss is None:
                continue
        except Exception:
            continue
        uuid_str = rec_dir.name[5:]
        ent = by_uuid.get(uuid_str)
        if not ent:
            continue
        epg_start = ent.get("start") or 0
        start_real = ent.get("start_real") or 0
        if not (epg_start and start_real):
            continue
        title = (ent.get("disp_title") or "").strip()
        if not title:
            continue
        drift = (start_real + float(ss)) - epg_start
        samples.setdefault(title, []).append({
            "uuid": uuid_str, "drift_s": round(drift, 1),
            "show_start_s": float(ss),
            "start_real": int(start_real), "epg_start": int(epg_start),
            "channelname": ent.get("channelname") or ""})
    out = {}
    for title, items in samples.items():
        drifts = [s["drift_s"] for s in items]
        n = len(drifts)
        # Negative drift = broadcaster runs early. The amount of pre-roll
        # we need to capture = MAX early-drift + a safety buffer.
        min_drift = min(drifts)  # most negative (= earliest broadcaster)
        suggested_extra_min = max(
            5, int(-min(0, min_drift) / 60) + 3) if n >= 1 else 5
        out[title] = {
            "n": n,
            "drifts_s": drifts,
            "mean_drift_s": round(sum(drifts) / n, 1),
            "min_drift_s": round(min_drift, 1),
            "suggested_start_extra_min": suggested_extra_min,
            "channelname": items[0]["channelname"],
            "samples": items,
            "computed_at": int(time.time()),
        }
    with _per_show_drift_lock:
        try:
            PER_SHOW_DRIFT_PATH.parent.mkdir(parents=True, exist_ok=True)
            PER_SHOW_DRIFT_PATH.write_text(json.dumps(out, indent=1))
        except Exception as e:
            print(f"[show-drift] save err: {e}", flush=True)
    return out


@app.route("/api/recording/<uuid>/show-start", methods=["POST"])
def api_recording_show_start(uuid):
    """Mark the actual show-start time within the recording. Body
    {"t": <seconds>}. Stored as show_start_s in ads_user.json. Returns
    drift_s = (start_real + t) - epg_start so the player can confirm
    "broadcaster ran X min early" inline. Triggers per-show drift
    aggregation in background."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    if not out_dir.exists():
        return Response(json.dumps({"ok": False, "error": "unknown recording"}),
                        status=404, mimetype="application/json")
    try:
        body = request.get_json(silent=True) or {}
        t_s = float(body.get("t"))
        if not (t_s >= 0):
            raise ValueError("t must be ≥ 0")
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=400, mimetype="application/json")
    user_cache = out_dir / "ads_user.json"
    cur = {}
    if user_cache.exists():
        try:
            cur = json.loads(user_cache.read_text())
            if not isinstance(cur, dict):
                cur = {}
        except Exception:
            cur = {}
    cur["show_start_s"] = round(t_s, 2)
    cur.setdefault("ads", [])
    cur.setdefault("deleted", [])
    try:
        tmp = user_cache.with_suffix(".tmp")
        tmp.write_text(json.dumps(cur))
        tmp.replace(user_cache)
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")
    drift = None
    ent = _dvr_entry_for_uuid(uuid)
    if ent:
        epg = ent.get("start") or 0
        sr = ent.get("start_real") or 0
        if epg and sr:
            drift = (sr + t_s) - epg
    threading.Thread(target=_aggregate_per_show_drift, daemon=True).start()
    return _cors(Response(json.dumps({
        "ok": True, "show_start_s": round(t_s, 2),
        "drift_s": round(drift, 1) if drift is not None else None}),
        mimetype="application/json"))


def _idnode_save(uuid_str, **fields):
    """tvh idnode/save helper: POSTs {"uuid": ..., **fields} as the
    'node' form value. Raises on HTTP failure."""
    body = urllib.parse.urlencode({
        "node": json.dumps({"uuid": uuid_str, **fields})
    }).encode()
    req = urllib.request.Request(
        f"{dvr_base()}/api/idnode/save",
        data=body, method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"})
    return urllib.request.urlopen(req, timeout=10).read()


def _autorec_rules_matching_title(title):
    """Find autorec rules whose anchored regex title would match the
    given disp_title. The rules use ^EscapedTitle$ patterns so we
    just compare the plain title to the unescaped version."""
    matches = []
    try:
        d = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/autorec/grid?limit=500", timeout=8).read())
    except Exception as e:
        print(f"[show-drift] autorec fetch err: {e}", flush=True)
        return matches
    for r in d.get("entries", []):
        pat = (r.get("title") or "").strip()
        if not pat.startswith("^") or not pat.endswith("$"):
            continue
        # Strip leading ^ and trailing $, then unescape backslashes
        # before regex meta-chars (= the form tvh writes them in:
        # \\space, \\-, \\!, \\&, etc.).
        body = pat[1:-1]
        try:
            unesc = re.sub(r"\\(.)", r"\1", body)
        except Exception:
            continue
        if unesc == title:
            matches.append(r)
    return matches


def _scheduled_dvr_for_title(title):
    """Find DVR entries (= future scheduled or currently recording)
    whose disp_title equals the given title."""
    out = []
    try:
        d = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000", timeout=8).read())
    except Exception as e:
        print(f"[show-drift] dvr fetch err: {e}", flush=True)
        return out
    for e in d.get("entries", []):
        if (e.get("sched_status") in ("scheduled", "recording")
                and (e.get("disp_title") or "").strip() == title):
            out.append(e)
    return out






@app.route("/api/recording/<uuid>/redetect", methods=["POST"])
def api_recording_redetect(uuid):
    """Force a fresh tv-detect pass on this recording.

    Truncates the cutlist .txt (filename preserved — train-head loader
    needs it to find the .ts source) and writes the .detect-requested
    marker. Daemon picks it up on its next poll cycle (≤5s).

    Filename-preserving truncate mirrors the head-invalidation path
    in _rec_prewarm so we don't introduce a second invalidation
    convention. Idempotent — repeated calls just refresh the marker."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    if not out_dir.exists():
        return Response(json.dumps({"ok": False, "error": "unknown recording"}),
                        status=404, mimetype="application/json")
    n_truncated = 0
    for t in out_dir.glob("*.txt"):
        if any(t.name.endswith(s) for s in
               (".logo.txt", ".cskp.txt", ".tvd.txt",
                ".trained.logo.txt")):
            continue
        try: t.write_text(""); n_truncated += 1
        except Exception: pass
    marker = out_dir / ".detect-requested"
    try: marker.write_text(json.dumps({"ts": time.time()}))
    except Exception as e:
        return Response(json.dumps({"ok": False, "error": str(e)}),
                        status=500, mimetype="application/json")
    return _cors(Response(json.dumps({
        "ok": True, "truncated": n_truncated}),
        mimetype="application/json"))


def _parse_hls_segments(playlist_path):
    """Parse an HLS playlist into [(extinf_line, uri, duration, t_start,
    t_end), ...]. Header lines (everything before the first EXTINF) and
    footer (#EXT-X-ENDLIST etc) are returned separately so we can
    reassemble a new manifest with a different segment subset.
    Returns (header_lines, segments, footer_lines) or (None, None, None)
    on any parse failure."""
    try:
        lines = playlist_path.read_text().splitlines()
    except Exception:
        return None, None, None
    header, segments, footer = [], [], []
    pending_extinf = None
    t = 0.0
    seen_segment = False
    for ln in lines:
        if ln.startswith("#EXTINF:"):
            try:
                dur = float(ln.split(":", 1)[1].rstrip(","))
            except Exception:
                continue
            pending_extinf = (ln, dur)
            seen_segment = True
        elif ln and not ln.startswith("#") and pending_extinf is not None:
            uri = ln.strip()
            extinf_line, dur = pending_extinf
            segments.append((extinf_line, uri, dur,
                             round(t, 3), round(t + dur, 3)))
            t += dur
            pending_extinf = None
        elif not seen_segment:
            header.append(ln)
        else:
            # After last segment URI: anything else is footer
            # (typically just #EXT-X-ENDLIST). pending_extinf without
            # following URI is junk — skip.
            if pending_extinf is None:
                footer.append(ln)
    return header, segments, footer


def _hls_trim_segments(out_dir, start_s, end_s):
    """Reuse existing HLS segments instead of full re-encode. Keeps
    every segment fully inside [start_s, end_s] in the OLD timeline,
    deletes the rest, renumbers survivors to seg_00000..seg_NNNNN,
    rewrites index.m3u8.

    Returns (snapped_start_s, snapped_end_s, n_kept, new_duration_s)
    on success — the trim caller uses snapped_start_s as the actual
    ffmpeg-trim start so source .ts and HLS segments stay in sync.
    Returns None if no HLS bundle exists, parsing fails, or no
    segments fall fully inside the requested range — caller falls
    back to full rebuild via .hls-requested marker.
    """
    pl = out_dir / "index.m3u8"
    if not pl.exists():
        return None
    header, segments, footer = _parse_hls_segments(pl)
    if not segments:
        return None
    keep = [s for s in segments
            if s[3] >= start_s - 0.001 and s[4] <= end_s + 0.001]
    if not keep:
        return None
    snapped_start = keep[0][3]
    snapped_end = keep[-1][4]
    # Playlist URIs are absolute URL paths like
    # "/hls/_rec_<uuid>/seg_NNNNN.ts" — strip to basename for fs ops,
    # preserve the URL prefix for manifest regen.
    def _basename(uri):
        return Path(uri).name
    url_prefix = ""
    sample_uri = keep[0][1]
    if "/" in sample_uri:
        url_prefix = sample_uri.rsplit("/", 1)[0] + "/"
    keep_basenames = {_basename(s[1]) for s in keep}
    # Delete segments not in keep set
    for s in segments:
        bn = _basename(s[1])
        if bn not in keep_basenames:
            try: (out_dir / bn).unlink()
            except Exception: pass
    # Two-phase rename to avoid colliding with surviving filenames
    # (e.g. keep[5..15] → renumber 0..10 collides on seg_00010 unless
    # we move via temp names first).
    for i, (extinf, uri, dur, ts, te) in enumerate(keep):
        old_p = out_dir / _basename(uri)
        if not old_p.exists():
            continue
        try: old_p.rename(out_dir / f".trim-rename.{i:05d}")
        except Exception: pass
    new_segments = []
    for i, (extinf, uri, dur, ts, te) in enumerate(keep):
        tmp_p = out_dir / f".trim-rename.{i:05d}"
        if not tmp_p.exists():
            continue
        new_basename = f"seg_{i:05d}.ts"
        new_uri = f"{url_prefix}{new_basename}"
        try: tmp_p.rename(out_dir / new_basename)
        except Exception:
            continue
        new_segments.append((extinf, new_uri, dur))
    # Rebuild manifest. Header lines containing MEDIA-SEQUENCE need to
    # reset to 0 since we renumbered.
    new_lines = []
    for h in header:
        if h.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            new_lines.append("#EXT-X-MEDIA-SEQUENCE:0")
        else:
            new_lines.append(h)
    for extinf, uri, dur in new_segments:
        new_lines.append(extinf)
        new_lines.append(uri)
    for f in footer:
        new_lines.append(f)
    if "#EXT-X-ENDLIST" not in new_lines:
        new_lines.append("#EXT-X-ENDLIST")
    pl.write_text("\n".join(new_lines) + "\n")
    new_dur = sum(d for _, _, d in new_segments)
    return (snapped_start, snapped_end, len(new_segments), round(new_dur, 3))


@app.route("/api/recording/<uuid>/trim", methods=["POST"])
def api_recording_trim(uuid):
    """Lossless ffmpeg trim of the source .ts to [start_s, end_s].
    Body: {"start_s": float (default 0), "end_s": float (default
    duration)}. Uses `-c copy` (no re-encode) — cuts at the nearest
    keyframe so actual boundary may be ±2 s off. Atomic-replaces the
    original .ts on the Pi (no backup — user accepted the trade-off
    for storage savings). After replace, invalidates: ads.json,
    whisper.json + postprocess.json, cutlist .txt, and the entire HLS
    bundle (index.m3u8 + seg_*.ts). When start_s > 0, all
    ads_user.json block timestamps are shifted by -start_s. Re-detect
    + HLS-rebuild markers are dropped so the daemon regenerates
    everything in the next 5-10 min."""
    out_dir = HLS_DIR / f"_rec_{uuid}"
    if not out_dir.exists():
        return Response(json.dumps({"ok": False, "error": "unknown recording"}),
                        status=404, mimetype="application/json")
    src = _rec_source_path(uuid)
    src_from_hls = False
    if not src or not Path(src).is_file():
        # Fallback: source is dedup'd off Pi but HLS-VOD bundle still
        # exists. We can ffmpeg-trim directly from the playlist, write
        # the result as .source-recovered.ts inside the HLS dir, and
        # let _rec_source_path pick it up for subsequent detect/remux.
        pl = out_dir / "index.m3u8"
        if pl.is_file():
            src = str(pl)
            src_from_hls = True
        else:
            return Response(json.dumps({"ok": False,
                "error": "source .ts missing and no HLS-VOD fallback"}),
                status=404, mimetype="application/json")
    body = request.get_json(silent=True) or {}
    try:
        start_s = float(body.get("start_s", 0))
        end_s = float(body.get("end_s"))
    except (TypeError, ValueError):
        return Response(json.dumps({"ok": False, "error": "start_s/end_s required as numbers"}),
                        status=400, mimetype="application/json")
    if start_s < 0 or end_s <= start_s:
        return Response(json.dumps({"ok": False, "error": "need 0 <= start_s < end_s"}),
                        status=400, mimetype="application/json")
    # Snap [start_s, end_s] to HLS segment boundaries BEFORE the ffmpeg
    # cut, so we can reuse existing segments instead of regenerating
    # the entire HLS bundle (= save 30s-2min Pi CPU per trim). The
    # snapping rounds start DOWN and end UP to the nearest segment
    # edge, so the user gets at-or-more content than requested. If no
    # HLS bundle exists, fall through to old behaviour (full rebuild).
    pl = out_dir / "index.m3u8"
    snapped_start = start_s
    snapped_end = end_s
    if pl.exists():
        _h, _segs, _f = _parse_hls_segments(pl)
        if _segs:
            # round start DOWN to segment start, end UP to segment end
            for s in _segs:
                if s[3] <= start_s < s[4]:
                    snapped_start = s[3]
                    break
            for s in reversed(_segs):
                if s[3] < end_s <= s[4]:
                    snapped_end = s[4]
                    break
            # Sanity: if snapping moved boundary outside original
            # request beyond the segment grain (e.g. user picked
            # exactly a segment boundary, no change), keep as-is.
    src_p = Path(src)
    ffmpeg_input = str(src_p)
    # Tmp filename ends in .ts so ffmpeg's auto-format-detection works,
    # AND we pass -f mpegts explicitly belt-and-braces. Without that,
    # ffmpeg fails with "Unable to choose an output format for ...trim.tmp".
    if src_from_hls:
        # HLS-VOD recovery: emit a fresh .source-recovered.ts in the
        # HLS dir; no atomic-replace against Pi-original (which is gone).
        recovered_p = out_dir / ".source-recovered.ts"
        tmp_p = out_dir / ".source-recovered.trim-tmp.ts"
        # The on-disk index.m3u8 stores segment URIs as URL-paths like
        # `/hls/_rec_<uuid>/seg_00000.ts` (the player serves them via
        # HTTP). ffmpeg's hls demuxer treats those as local-file
        # references and can't find them. Rewrite a sibling temp
        # playlist with bare basenames so ffmpeg resolves segments
        # relative to its own dir.
        try:
            hdr, segs, ftr = _parse_hls_segments(src_p)
            if segs:
                tmp_pl = out_dir / ".source-recovered.local.m3u8"
                with tmp_pl.open("w") as f:
                    f.write("\n".join(hdr) + "\n")
                    for extinf, uri, _d, _ts, _te in segs:
                        f.write(extinf + "\n")
                        f.write(Path(uri).name + "\n")
                    if ftr:
                        f.write("\n".join(ftr) + "\n")
                ffmpeg_input = str(tmp_pl)
        except Exception as e:
            return Response(json.dumps({"ok": False,
                "error": f"hls-recovery playlist rewrite failed: {e}"}),
                status=500, mimetype="application/json")
    else:
        recovered_p = None
        tmp_p = src_p.parent / f".{src_p.stem}.trim-tmp.ts"
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
           "-ss", f"{snapped_start:.3f}",
           "-to", f"{snapped_end:.3f}"]
    # -allowed_extensions only exists in the HLS demuxer; the mpegts
    # demuxer rejects it with rc=8 ("Option not found"). Add only when
    # input is an m3u8.
    if ffmpeg_input.endswith(".m3u8"):
        cmd += ["-allowed_extensions", "ALL"]
    cmd += ["-i", ffmpeg_input,
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            "-f", "mpegts",
            str(tmp_p)]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        try: tmp_p.unlink(missing_ok=True)
        except Exception: pass
        return Response(json.dumps({"ok": False, "error": "ffmpeg timeout (>10min)"}),
                        status=500, mimetype="application/json")
    if r.returncode != 0 or not tmp_p.exists() or tmp_p.stat().st_size < 1024:
        try: tmp_p.unlink(missing_ok=True)
        except Exception: pass
        return Response(json.dumps({"ok": False,
            "error": f"ffmpeg failed rc={r.returncode}: {r.stderr[-300:]}"}),
                        status=500, mimetype="application/json")
    new_size = tmp_p.stat().st_size
    if src_from_hls:
        # "Old size" in recovery mode is the total HLS-VOD payload (the
        # segments we trimmed from), not the playlist file size.
        try:
            old_size = sum(p.stat().st_size for p in out_dir.glob("seg_*.ts"))
        except Exception:
            old_size = new_size
        try:
            tmp_p.replace(recovered_p)
        except Exception as e:
            try: tmp_p.unlink(missing_ok=True)
            except Exception: pass
            return Response(json.dumps({"ok": False,
                "error": f"recovered-replace failed: {e}"}),
                status=500, mimetype="application/json")
    else:
        old_size = src_p.stat().st_size
        try:
            tmp_p.replace(src_p)  # atomic on same filesystem
        except Exception as e:
            try: tmp_p.unlink(missing_ok=True)
            except Exception: pass
            return Response(json.dumps({"ok": False, "error": f"replace failed: {e}"}),
                            status=500, mimetype="application/json")
    # Shift user-ads using SNAPPED start (= what's actually kept in the
    # source .ts and HLS bundle). Drop blocks fully outside the new
    # range.
    user_p = out_dir / "ads_user.json"
    n_shifted = 0
    if user_p.exists():
        try:
            cur = json.loads(user_p.read_text())
            ads = cur.get("ads", []) if isinstance(cur, dict) else cur
            new_ads = []
            new_dur = snapped_end - snapped_start
            for blk in ads or []:
                try:
                    s = float(blk[0]) - snapped_start
                    e = float(blk[1]) - snapped_start
                    if e <= 0 or s >= new_dur:
                        continue
                    s = max(0, s); e = min(new_dur, e)
                    if e - s >= 1:
                        new_ads.append([round(s, 2), round(e, 2)])
                        n_shifted += 1
                except Exception:
                    pass
            if isinstance(cur, dict):
                cur["ads"] = new_ads
                user_p.write_text(json.dumps(cur))
            else:
                user_p.write_text(json.dumps(new_ads))
        except Exception as e:
            print(f"[trim {uuid[:8]}] ads_user shift err: {e}", flush=True)
    # Wipe ads.json cache — references OLD timeline. Will regenerate
    # from the (truncated then re-detected) cutlist .txt on next /ads
    # endpoint hit.
    for p in (out_dir / "ads.json",):
        if p.exists():
            try: p.unlink()
            except Exception: pass
    # HLS bundle: try to reuse existing segments by keeping those
    # fully inside [snapped_start, snapped_end] and renumbering them.
    # Falls back to full rebuild via .hls-requested if reuse fails
    # (no playlist, parse error, no segments in range).
    hls_kept = 0
    hls_reuse = _hls_trim_segments(out_dir, snapped_start, snapped_end)
    if hls_reuse is not None:
        _ss, _se, hls_kept, _new_dur = hls_reuse
    else:
        for p in out_dir.glob("seg_*.ts"):
            try: p.unlink()
            except Exception: pass
        for fname in ("index.m3u8",):
            p = out_dir / fname
            if p.exists():
                try: p.unlink()
                except Exception: pass
        try: (out_dir / ".hls-requested").write_text(
            json.dumps({"ts": time.time()}))
        except Exception: pass
    # Whisper cache (windows reference old timestamps).
    # WHISPER_CACHE is on the daemon box, not the gateway — best-effort
    # POST notify so the daemon can drop its local copy. Daemon will
    # also regenerate on next detect.
    # Truncate cutlist .txt + drop detect marker for re-detect.
    SIDECAR = (".logo.txt", ".cskp.txt", ".tvd.txt", ".trained.logo.txt")
    for t in out_dir.glob("*.txt"):
        if any(t.name.endswith(s) for s in SIDECAR):
            continue
        try: t.write_text("")
        except Exception: pass
    try: (out_dir / ".detect-requested").write_text(
        json.dumps({"ts": time.time()}))
    except Exception: pass
    hls_msg = (f"reused {hls_kept} HLS seg(s)" if hls_reuse
               else "queued full HLS rebuild")
    print(f"[trim {uuid[:8]}] {start_s:.1f}..{end_s:.1f}s "
          f"(snapped {snapped_start:.1f}..{snapped_end:.1f}s), "
          f"{old_size/1e6:.0f}MB → {new_size/1e6:.0f}MB, "
          f"shifted {n_shifted} user-ad block(s), {hls_msg}",
          flush=True)
    return _cors(Response(json.dumps({
        "ok": True,
        "old_size_mb": round(old_size/1e6, 1),
        "new_size_mb": round(new_size/1e6, 1),
        "saved_mb": round((old_size-new_size)/1e6, 1),
        "new_duration_s": round(snapped_end - snapped_start, 2),
        "snapped_start_s": round(snapped_start, 2),
        "snapped_end_s": round(snapped_end, 2),
        "shifted_blocks": n_shifted,
        "hls_segments_reused": hls_kept,
        "hls_full_rebuild": hls_reuse is None}),
        mimetype="application/json"))


@app.route("/recording/<uuid>/thumbs.json")
def recording_thumbs_manifest(uuid):
    """How many thumbs are ready and what interval they cover."""
    thumbs_dir = HLS_DIR / f"_rec_{uuid}" / "thumbs"
    done = (thumbs_dir / ".done").exists()
    count = len(list(thumbs_dir.glob("t*.jpg"))) if thumbs_dir.exists() else 0
    return _cors(Response(json.dumps({
        "interval": THUMB_INTERVAL,
        "count": count,
        "done": done,
    }), mimetype="application/json"))


@app.route("/recording/<uuid>/thumbs/<fname>")
def recording_thumb(uuid, fname):
    if not re.fullmatch(r"t\d{5}\.jpg", fname):
        abort(404)
    fp = HLS_DIR / f"_rec_{uuid}" / "thumbs" / fname
    if not fp.exists():
        abort(404)
    resp = Response(fp.read_bytes(), mimetype="image/jpeg")
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return _cors(resp)


def _show_poster_url(uuid, title):
    """Show poster URL using the same logic as the /bibliothek tiles:
    prefer TMDB/fernsehserien show artwork from the _epg_meta cache,
    fall back to a representative thumb-frame at t00010.jpg (= ~5 min
    in, past intros) when no metadata match exists. Returns "" if
    neither source has anything usable."""
    if not title:
        return ""
    with _epg_meta_lock:
        meta = _epg_meta.get(_normalize_title(title)) or {}
    poster = meta.get("tmdb_poster") or meta.get("poster") or ""
    if poster:
        if poster.startswith(("http://", "https://")):
            return poster
        return f"{HOST_URL.rstrip('/')}/{poster.lstrip('/')}"
    if uuid:
        thumb_path = HLS_DIR / f"_rec_{uuid}" / "thumbs" / "t00010.jpg"
        if thumb_path.is_file():
            return (f"{HOST_URL.rstrip('/')}"
                    f"/recording/{uuid}/thumbs/t00010.jpg")
    return ""


@app.route("/recording/<uuid>/poster.jpg")
def recording_poster(uuid):
    """Show-poster redirect. 302s to the same TMDB/fernsehserien poster
    URL that the /bibliothek tiles use for this show, falling back to
    a representative thumb-frame at t00010.jpg when no metadata match
    exists. 404 if neither source has anything usable."""
    title = _rec_dvr_title(uuid) or ""
    url = _show_poster_url(uuid, title)
    if not url:
        abort(404)
    return redirect(url, code=302)


@app.route("/recording/<uuid>/progress")
def recording_hls_progress(uuid):
    """Lightweight progress poll for the player loader. Also kicks off
    ffmpeg if it hasn't started yet (so the player can just poll)."""
    st = _rec_state(uuid)
    if not st["real"] and not st["running"]:
        # Clean up any 0-byte placeholder so ffmpeg can start fresh.
        try:
            if st["playlist"].exists():
                st["playlist"].unlink()
        except Exception:
            pass
        _rec_hls_spawn(uuid)
        st = _rec_state(uuid)
    # Make sure comskip is on its way — idempotent, safe on every
    # poll. Covers pre-fix recordings that never got a scan, and
    # cleans up 0-byte .txt leftovers from an aborted prior run that
    # would otherwise make us think comskip already finished.
    out_dir = HLS_DIR / f"_rec_{uuid}"
    if out_dir.exists():
        usable = [t for t in out_dir.glob("*.txt")
                  if t.stat().st_size > 0]
        if not usable and not _mac_comskip_alive():
            _rec_cskip_spawn(uuid)
        if not (out_dir / "thumbs" / ".done").exists():
            _rec_thumbs_spawn(uuid)
    # Expose the playlist's mtime so the player can invalidate a
    # stored localStorage seek-position from before the recording was
    # remuxed (otherwise an old saved offset against a partial old
    # playlist makes the player land in the middle on every reload).
    pl_mtime = 0
    try:
        pl_mtime = int(st["playlist"].stat().st_mtime * 1000)
    except Exception:
        pass
    return _cors(Response(json.dumps({"done": st["done"],
                                        "segments": st["segments"],
                                        "total": st["total"],
                                        "playlist_mtime_ms": pl_mtime}),
                           mimetype="application/json"))


def _render_mediathek_player(uuid, entry):
    """HTML for a virtual Mediathek recording. If we've already ripped
    the show to a local MP4 (V3), play that directly via <video src>.
    Otherwise stream the remote HLS via hls.js."""
    title_safe = entry["title"].replace("<", "&lt;")
    upstream_hls = entry["hls_url"]
    # Route through our passthru so the player gets a single-variant
    # master pinned to the upstream's highest BANDWIDTH rendition,
    # bypassing client-side ABR downgrade. Same endpoint that serves
    # live-mediathek channels — the slug here is just routing for the
    # /pl.m3u8 sub-playlist proxy and doesn't have to match anything.
    if upstream_hls.startswith("http"):
        wrapped = (f"{HOST_URL}/mediathek-passthru/_max/master.m3u8?"
                   f"u={urllib.parse.quote(upstream_hls, safe='')}")
        hls_url = wrapped.replace("'", "%27")
    else:
        hls_url = upstream_hls.replace("'", "%27")
    ripped_path = entry.get("ripped_path", "")
    has_local = False
    if ripped_path and uuid != "preview":
        try:
            has_local = Path(ripped_path).exists()
        except Exception:
            has_local = False
    local_url = f"{HOST_URL}/mediathek-rec/{uuid}/file.mp4"
    avail_to = entry.get("available_to", 0)
    from datetime import datetime
    avail_str = ""
    if avail_to:
        try:
            avail_str = datetime.fromtimestamp(avail_to).strftime("%d.%m.%Y")
        except Exception:
            pass
    badge_label = ("Mediathek · lokal" if has_local else "ARD Mediathek")
    badge_extra = (" · bis " + avail_str if avail_str and not has_local
                   else "")
    src_badge = (f"<span class='src-badge'>{badge_label}{badge_extra}</span>")
    return (f"<!doctype html><html><head>"
            f"{PLAYER_HEAD_META}"
            f"<title>{title_safe}</title>"
            f"<script src='https://cdn.jsdelivr.net/npm/hls.js@1/"
            f"dist/hls.min.js'></script>"
            f"<style>{PLAYER_BASE_CSS}"
            f".src-badge{{display:inline-block;background:#2980b9;"
            f"color:#fff;padding:2px 8px;border-radius:10px;"
            f"font-size:.75em;margin-left:8px;vertical-align:1px}}"
            f"#unmute{{position:fixed;left:50%;top:50%;"
            f"transform:translate(-50%,-50%);z-index:30;background:#fff;"
            f"color:#000;padding:14px 22px;border-radius:30px;"
            f"font-weight:600;font-size:1em;border:0;cursor:pointer;"
            f"display:none}}"
            f"</style></head><body>"
            f"<video id='v' autoplay muted playsinline webkit-playsinline></video>"
            f"<button id='unmute'>🔊 Ton an</button>"
            f"<div id='topbar'>"
            f"<button class='iconbtn' onclick='toggleFs()' aria-label='Vollbild'>⛶</button>"
            f"<a class='iconbtn' href='{HOST_URL}/recordings' "
            f"onclick='return closePlayer(event)' aria-label='Schließen'>✕</a>"
            f"</div>"
            f"<div id='hint'></div>"
            f"<div id='chrome'>"
            f"<div id='scrub'><div id='track'><div id='played'></div></div>"
            f"<div id='thumb'></div></div>"
            f"<div class='row'>"
            f"<button class='iconbtn' onclick='seek(-10)'>⏪</button>"
            f"<button id='pp' class='iconbtn' onclick='togglePlay()'>▶</button>"
            f"<button class='iconbtn' onclick='seek(10)'>⏩</button>"
            f"<span id='volume-wrap'>"
            f"<button id='vol-icon' class='iconbtn' aria-label='Lautstärke'>🔊</button>"
            f"<input type='range' id='vol-slider' min='0' max='100' value='100'>"
            f"</span>"
            f"<span id='cur' class='time'>0:00</span>"
            f"<span class='spacer'></span>"
            f"<span id='dur' class='time'>0:00</span>"
            f"</div>"
            f"<div id='ttlrow'>{title_safe}{src_badge}</div>"
            f"</div>"
            f"<script>"
            f"const PLAYER_HOME='{HOST_URL}/recordings';"
            f"{PLAYER_BASE_JS}"
            f"/* Tap policy override — same center-circle gate as the"
            f"   recordings player. PLAYER_BASE_JS toggles play/pause on"
            f"   any tap (correct for live channels), but for VOD-style"
            f"   playback (Mediathek) we want to be able to tap"
            f"   anywhere to reveal/hide chrome WITHOUT pausing. Only"
            f"   the central ~18 % radius zone (matching the iOS"
            f"   native player's play-button area) toggles play/pause. */"
            f"function _isCenterTap(clientX,clientY){{"
            f"  const r=v.getBoundingClientRect();"
            f"  const cx=r.left+r.width/2, cy=r.top+r.height/2;"
            f"  const dx=clientX-cx, dy=clientY-cy;"
            f"  const radius=Math.min(r.width,r.height)*0.18;"
            f"  return dx*dx+dy*dy <= radius*radius;"
            f"}}"
            f"v.addEventListener('click',ev=>{{"
            f"  ev.stopImmediatePropagation();"
            f"  ev.preventDefault();"
            f"  if(Date.now()-_lastTouchT<500)return;"
            f"  if(v.muted&&!v.paused){{v.muted=false;show();return;}}"
            f"  if(_isCenterTap(ev.clientX,ev.clientY)) togglePlay();"
            f"  if(chromeBar.classList.contains('hidden'))show();"
            f"  else {{chromeBar.classList.add('hidden');"
            f"    topbar.classList.add('hidden');}}"
            f"}},true);"
            f"v.style.touchAction='manipulation';"
            f"v.addEventListener('dblclick',ev=>ev.preventDefault());"
            f"v.addEventListener('touchend',ev=>{{"
            f"  if(!ev.changedTouches[0])return;"
            f"  const cx=ev.changedTouches[0].clientX;"
            f"  const cy=ev.changedTouches[0].clientY;"
            f"  setTimeout(()=>{{"
            f"    if(!_singleTapT)return;"
            f"    if(_isCenterTap(cx,cy))return;"
            f"    clearTimeout(_singleTapT);_singleTapT=null;"
            f"    if(chromeBar.classList.contains('hidden'))show();"
            f"    else {{chromeBar.classList.add('hidden');"
            f"      topbar.classList.add('hidden');}}"
            f"  }},0);"
            f"}});"
            f"const cur=document.getElementById('cur');"
            f"const dur=document.getElementById('dur');"
            f"function fmt(s){{"
            f"  if(!isFinite(s)||s<0)s=0;"
            f"  const m=Math.floor(s/60),ss=Math.floor(s%60);"
            f"  const h=Math.floor(m/60);"
            f"  return h>0?h+':'+String(m%60).padStart(2,'0')+':'+String(ss).padStart(2,'0')"
            f"           :m+':'+String(ss).padStart(2,'0');"
            f"}}"
            f"function togglePlay(){{"
            f"  if(v.paused||v.ended){{v.play().catch(()=>{{}});}}else{{v.pause();}}"
            f"}}"
            f"function seek(d){{"
            f"  const D=isFinite(v.duration)?v.duration:Infinity;"
            f"  v.currentTime=Math.max(0,Math.min(D-0.5,(v.currentTime||0)+d));"
            f"  show();"
            f"}}"
            f"function refresh(){{"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  const T=v.currentTime||0;"
            f"  cur.textContent=fmt(T);dur.textContent=fmt(D);"
            f"  const pct=D>0?(T/D)*100:0;"
            f"  if(!_dragging){{played.style.width=pct+'%';thumb.style.left=pct+'%';}}"
            f"}}"
            f"v.addEventListener('timeupdate',refresh);"
            f"v.addEventListener('loadedmetadata',refresh);"
            f"function seekTo(ev){{"
            f"  const r=scrub.getBoundingClientRect();"
            f"  const x=(ev.touches?ev.touches[0]:ev).clientX-r.left;"
            f"  const p=Math.max(0,Math.min(1,x/r.width));"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  if(D>0)v.currentTime=p*D;"
            f"}}"
            f"document.addEventListener('keydown',e=>{{"
            f"  if(e.key==='Escape')closePlayer();"
            f"  else if(e.key==='ArrowRight')seek(10);"
            f"  else if(e.key==='ArrowLeft')seek(-10);"
            f"  else if(e.key===' '){{e.preventDefault();togglePlay();show();}}"
            f"}});"
            f"v.addEventListener('click',()=>{{"
            f"  if(chromeBar.classList.contains('hidden'))show();"
            f"  else {{chromeBar.classList.add('hidden');topbar.classList.add('hidden');}}"
            f"}});"
            f"document.addEventListener('mousemove',show);"
            f"const rawUrl='{hls_url}';"
            f"const localUrl='{local_url}';"
            f"const hasLocal={'true' if has_local else 'false'};"
            f"const unmuteBtn=document.getElementById('unmute');"
            f"const disableCaptions=()=>{{"
            f"  for(const t of v.textTracks)t.mode='disabled';"
            f"}};"
            f"v.textTracks&&v.textTracks.addEventListener('addtrack',disableCaptions);"
            f"setTimeout(disableCaptions,200);"
            f"setTimeout(disableCaptions,1500);"
            f"function doUnmute(){{"
            f"  v.muted=false;unmuteBtn.style.display='none';"
            f"  v.play().catch(()=>{{}});"
            f"}}"
            f"unmuteBtn.onclick=doUnmute;"
            f"v.addEventListener('click',()=>{{if(v.muted)doUnmute();}});"
            f"function startPlayback(){{"
            f"  v.muted=true;"
            f"  v.play().then(()=>{{unmuteBtn.style.display='inline-flex';}})"
            f"    .catch(()=>{{unmuteBtn.style.display='inline-flex';}});"
            f"}}"
            # If we already ripped the show to MP4, play the local file
            # directly — iOS handles MP4 natively, no hls.js needed.
            f"if(hasLocal){{"
            f"  v.src=localUrl;"
            f"  v.addEventListener('loadedmetadata',startPlayback,{{once:true}});"
            f"}} else if(window.Hls&&Hls.isSupported()){{"
            f"  const hls=new Hls({{forceMseHlsOnAppleDevices:true,"
            f"    renderTextTracksNatively:false,subtitleDisplay:false}});"
            f"  hls.loadSource(rawUrl);hls.attachMedia(v);"
            f"  v.addEventListener('canplay',startPlayback,{{once:true}});"
            f"}} else if(v.canPlayType('application/vnd.apple.mpegurl')){{"
            f"  v.src=rawUrl;"
            f"  v.addEventListener('loadedmetadata',startPlayback,{{once:true}});"
            f"}} else {{"
            f"  document.body.innerHTML='<p style=color:#fff;padding:20px>"
            f"HLS wird von diesem Browser nicht unterstützt.</p>';"
            f"}}"
            f"</script></body></html>")


@app.route("/mediathek-play/<event_id>")
def mediathek_play(event_id):
    """Play a Mediathek match of an EPG event directly, without
    persisting a virtual recording. Used by the long-press modal's
    "Jetzt abspielen" shortcut for past shows you just want to watch."""
    try:
        res = urllib.request.urlopen(
            f"http://localhost:8080/api/mediathek-lookup/{event_id}",
            timeout=35).read()
        match = json.loads(res).get("match")
    except Exception as e:
        abort(502, f"lookup: {e}")
    if not match or not match.get("id"):
        abort(404, "no mediathek match")
    hls_url = _resolve_mediathek_hls(match["id"],
                                       source=match.get("source", "ard"))
    if not hls_url:
        abort(502, "no hls url")
    entry = {
        "title": match.get("title", "Mediathek"),
        "hls_url": hls_url,
        "available_to": match.get("available_to", 0),
    }
    return _render_mediathek_player("preview", entry)


@app.route("/recording/<uuid>")
def play_recording(uuid):
    """Player page for a DVR recording — wraps tvheadend's dvrfile in
    a styled <video> so iOS Safari doesn't force native fullscreen.
    Virtual mt_* UUIDs stream directly from ARD Mediathek via hls.js."""
    if uuid.startswith("mt_"):
        with _mediathek_rec_lock:
            entry = _mediathek_rec.get(uuid)
        if not entry:
            abort(404, "unknown mediathek recording")
        return _render_mediathek_player(uuid, entry)
    title = "Aufnahme"
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000",
            timeout=6).read())
        for e in data.get("entries", []):
            if e.get("uuid") == uuid:
                title = e.get("disp_title") or title
                break
    except Exception:
        pass
    title_safe = title.replace("<", "&lt;")
    src = f"{HOST_URL}/recording/{uuid}/index.m3u8"
    # Auto-confirm metadata — surface a banner if this recording was
    # auto-reviewed by the Phase-6A loop so the user can undo if the
    # verdict is wrong.
    auto_confirm_banner = ""
    user_p = HLS_DIR / f"_rec_{uuid}" / "ads_user.json"
    if user_p.is_file():
        try:
            cur = json.loads(user_p.read_text())
            if isinstance(cur, dict) and cur.get("auto_confirmed_at"):
                score = cur.get("auto_confirm_score")
                pct = (f" ({int(score*100)}%)" if score else "")
                ts = cur["auto_confirmed_at"]
                age_h = max(0, int((time.time() - ts) / 3600))
                auto_confirm_banner = (
                    f"<div id='ac-banner' style='position:fixed;top:0;"
                    f"left:0;right:0;background:#16a34a;color:#fff;"
                    f"padding:8px 14px;font-size:.9em;z-index:25;"
                    f"display:flex;align-items:center;gap:12px'>"
                    f"<span>✓ Auto-Confirmed{pct} vor {age_h} h — "
                    f"prüfe und tippe Undo wenn die Werbeblöcke falsch "
                    f"sind</span>"
                    f"<button onclick='undoAutoConfirm()' "
                    f"style='margin-left:auto;padding:4px 12px;"
                    f"border-radius:4px;border:1px solid #fff;"
                    f"background:transparent;color:#fff;cursor:pointer'>"
                    f"Undo</button>"
                    f"<button onclick=\"document.getElementById('ac-banner')"
                    f".style.display='none'\" "
                    f"style='padding:4px 8px;border-radius:4px;"
                    f"border:1px solid #fff6;background:transparent;"
                    f"color:#fff;cursor:pointer'>×</button>"
                    f"</div>")
        except Exception:
            pass
    html = (f"<!doctype html><html><head>"
            f"{PLAYER_HEAD_META}"
            f"<title>{title_safe}</title>"
            f"<style>{PLAYER_BASE_CSS}"
            f".pill.rec{{background:#e74c3c}}"
            f"#thumb-preview{{position:fixed;width:160px;height:90px;"
            f"border:2px solid #fff;border-radius:4px;overflow:hidden;"
            f"box-shadow:0 2px 8px #000a;z-index:20;background:#000;"
            f"opacity:0;pointer-events:none;transition:opacity .1s}}"
            f"#thumb-preview img{{width:100%;height:100%;object-fit:cover;"
            f"display:block}}"
            f"#thumb-preview.visible{{opacity:1}}"
            # Loader z-index BELOW #chrome (= 10) so the bottom-bar
            # scrub + already-rendered ad-markers stay visible during
            # the ~6 s "Aufnahme wird vorbereitet" buffer-warmup. The
            # /ads fetch fires immediately on page load and populates
            # ads + adsDurationFallback within ~250 ms, so the markers
            # are in the DOM long before the video itself is playable
            # — they were just hidden under the full-screen overlay.
            f"#loader{{position:fixed;inset:0;display:flex;flex-direction:column;"
            f"align-items:center;justify-content:center;background:#000c;"
            f"z-index:5;font-size:1.05em;gap:8px;transition:opacity .3s}}"
            f"#loader.hidden{{opacity:0;pointer-events:none}}"
            f".spinner{{width:40px;height:40px;border:4px solid #fff3;"
            f"border-top-color:#fff;border-radius:50%;"
            f"animation:spin 1s linear infinite}}"
            f"@keyframes spin{{to{{transform:rotate(360deg)}}}}"
            f"</style></head><body>"
            f"{auto_confirm_banner}"
            f"<video id='v' autoplay muted playsinline "
            f"webkit-playsinline disablepictureinpicture></video>"
            f"<div id='loader'>"
            f"<div class='spinner'></div>"
            f"<div id='lmsg'>Aufnahme wird vorbereitet…</div>"
            f"</div>"
            f"<div id='topbar'>"
            f"<button class='iconbtn' onclick='toggleFs()' aria-label='Vollbild'>⛶</button>"
            f"<button id='ctrlMin' class='iconbtn' onclick='toggleCtrlMin()' "
            f"aria-label='Steuerleiste verbergen'>⊟</button>"
            f"<button id='chromePin' class='iconbtn' onclick='toggleChromePin()' "
            f"aria-label='Steuerleiste anpinnen'>📍</button>"
            f"<a class='iconbtn' href='{HOST_URL}/recordings' aria-label='Schließen' "
            f"onclick='return closePlayer(event)'>✕</a>"
            f"</div>"
            f"<div id='hint'></div>"
            f"<div id='chrome'>"
            f"<div id='scrub'>"
            f"<div id='track'><div id='played'></div></div>"
            f"<div id='thumb'></div>"
            f"</div>"
            f"<div class='row'>"
            f"<button class='iconbtn' onclick='seek(-10)' aria-label='-10 s'>⏪</button>"
            f"<button id='pp' class='iconbtn' onclick='togglePlay()' "
            f"aria-label='Play/Pause'>▶</button>"
            f"<button class='iconbtn' onclick='seek(10)' aria-label='+10 s'>⏩</button>"
            f"<button id='speedbtn' class='iconbtn' "
            f"onclick='cycleSpeed()' aria-label='Geschwindigkeit' "
            f"style='font-size:.75em;font-weight:600'>1×</button>"
            f"<span id='volume-wrap'>"
            f"<button id='vol-icon' class='iconbtn' aria-label='Lautstärke'>🔊</button>"
            f"<input type='range' id='vol-slider' min='0' max='100' value='100'>"
            f"</span>"
            f"<span id='cur' class='time'>0:00</span>"
            f"<span class='spacer'></span>"
            f"<button id='skipad' class='pill rec' onclick='skipAd()'"
            f">Werbung ⏭</button>"
            f"<button id='mark-mode' class='pill' onclick='toggleMarkMode()'"
            f" title='Markier-Modus umschalten: Werbung 🎯 ↔ Bumper 🎬 "
            f"(Bumper = sender-spezifische Animation am Werbeblock-Ende, "
            f"wird als Frame-Template für die automatische Detection gespeichert)'>"
            f"🎯 Werbung</button>"
            f"<button id='step-back' class='pill bumper-only' "
            f"style='display:none' onclick='stepFrame(-1)' "
            f"title='1 Sekunde zurück (Bumper-Feinjustage)'>"
            f"⏴ −1s</button>"
            f"<button id='step-fwd' class='pill bumper-only' "
            f"style='display:none' onclick='stepFrame(1)' "
            f"title='1 Sekunde vor (Bumper-Feinjustage)'>"
            f"+1s ⏵</button>"
            f"<button id='ad-start' class='pill' onclick='markStart()'"
            f" title='Aktuelle Stelle als Start markieren'>"
            f"⏵ Start</button>"
            f"<button id='ad-end' class='pill' onclick='markEnd()'"
            f" title='Aktuelle Stelle als Ende übernehmen'>"
            f"⏹ Ende</button>"
            f"<button id='ad-reviewed' class='pill' onclick='markReviewed()'"
            f" title='Aufnahme als geprüft markieren — "
            f"verbleibende unsichere Frames werden als Show bestätigt"
            f" (höhere Trainings-Wirkung als kein-Klick)'>"
            f"✓ Geprüft</button>"
            f"<button id='ad-redetect' class='pill' onclick='redetectNow()'"
            f" title='Detect neu starten — z.B. nach neuen Bumper-"
            f"Templates, damit Snap auf den frisch markierten Frame"
            f" landet (~3 min Hintergrund-Job)'>"
            f"🔄 Re-Detect</button>"
            f"<button id='ad-trim' class='pill' onclick='trimNow()'"
            f" title='Aufnahme verlustfrei beschneiden — alles vor"
            f" und nach dem gewählten Bereich wird gelöscht. Speicher"
            f" wird sofort frei. Re-Detect + HLS-Rebuild laufen danach"
            f" automatisch (~5 min).'>"
            f"✂️ Trim</button>"
            f"<span id='dur' class='time'>0:00</span>"
            f"</div>"
            f"<div id='ttlrow'>{title_safe}</div>"
            f"</div>"
            f"<script>"
            f"const PLAYER_HOME='{HOST_URL}/recordings';"
            f"{PLAYER_BASE_JS}"
            f"const cur=document.getElementById('cur');"
            f"const dur=document.getElementById('dur');"
            f"const loader=document.getElementById('loader');"
            f"const lmsg=document.getElementById('lmsg');"
            f"const skipBtn=document.getElementById('skipad');"
            f"const speedBtn=document.getElementById('speedbtn');"
            f"const SPEEDS=[1,1.25,1.5,2,0.75];"
            f"let speedIdx=0;"
            f"function cycleSpeed(){{"
            f"  speedIdx=(speedIdx+1)%SPEEDS.length;"
            f"  v.playbackRate=SPEEDS[speedIdx];"
            f"  speedBtn.textContent=SPEEDS[speedIdx]+'×';"
            f"  show();"
            f"}}"
            f"let ads=[];"
            f"function fmt(s){{"
            f"  if(!isFinite(s)||s<0)s=0;"
            f"  const m=Math.floor(s/60),ss=Math.floor(s%60);"
            f"  const h=Math.floor(m/60);"
            f"  return h>0?h+':'+String(m%60).padStart(2,'0')+':'+String(ss).padStart(2,'0')"
            f"           :m+':'+String(ss).padStart(2,'0');"
            f"}}"
            f"function togglePlay(){{"
            f"  if(v.paused||v.ended){{v.play().catch(()=>{{}});}}else{{v.pause();}}"
            f"}}"
            f"function seek(d){{"
            f"  const D=isFinite(v.duration)?v.duration:Infinity;"
            f"  v.currentTime=Math.max(0,Math.min(D-0.5,(v.currentTime||0)+d));"
            f"  show();"
            f"}}"
            f"function currentAd(){{"
            f"  const t=v.currentTime||0;"
            f"  for(const a of ads){{if(t>=a[0]&&t<a[1])return a;}}"
            f"  return null;"
            f"}}"
            f"function skipAd(){{"
            f"  const a=currentAd();"
            f"  if(!a)return;"
            f"  /* Implicit confirmation: pressing Skip while inside a"
            f"     block confirms it WAS a real ad — soft positive label"
            f"     for training (deduped server-side within ±5 s). */"
            f"  const t0=v.currentTime||a[0];"
            f"  fetch('{HOST_URL}/api/recording/{uuid}/skip-event',"
            f"    {{method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"     body:JSON.stringify({{t:t0}})}}).catch(()=>{{}});"
            f"  v.currentTime=a[1]+0.1;"
            f"}}"
            f"function renderAds(){{"
            f"  /* Prefer the video element's duration (authoritative once"
            f"     metadata is loaded). Fall back to the server-supplied"
            f"     playlist duration so blocks render immediately on first"
            f"     /ads response, instead of waiting ~6 s for"
            f"     loadedmetadata to fire. The loadedmetadata handler"
            f"     re-renders to refine positions if they shift. */"
            f"  const vd=isFinite(v.duration)?v.duration:0;"
            f"  const D=vd>0?vd:adsDurationFallback;"
            f"  document.querySelectorAll('.ad-block').forEach(e=>e.remove());"
            f"  if(D<=0)return;"
            f"  ads.forEach((blk,idx)=>{{"
            f"    const[s,e]=blk;"
            f"    const left=(s/D)*100,width=Math.max(0.3,((e-s)/D)*100);"
            f"    const el=document.createElement('div');"
            f"    el.className='ad-block editable';"
            f"    el.style.left=left+'%';el.style.width=width+'%';"
            f"    el.dataset.idx=String(idx);"
            f"    el.title='Werbeblock bearbeiten ('+fmt(s)+' – '+fmt(e)+')';"
            f"    el.addEventListener('click',ev=>{{"
            f"      ev.stopPropagation();openAdEditor(idx);"
            f"    }});"
            f"    scrub.appendChild(el);"
            f"  }});"
            f"}}"
            f"/* A 'user' ad is one that doesn't overlap any auto-detected"
            f"   block — boundary refinements (which DO overlap an auto"
            f"   block) override the auto via the smart-merge and don't"
            f"   need to be re-listed separately. Sending only the"
            f"   diverging blocks keeps ads_user.json minimal and lets"
            f"   re-scans pick up newly detected blocks automatically. */"
            f"function blocksOverlap(a,b){{return a[0]<b[1]&&b[0]<a[1];}}"
            f"function saveAdsEdit(){{"
            f"  const userOnly=ads.filter(u=>"
            f"    !autoAds.some(a=>blocksOverlap(a,u))"
            f"  );"
            f"  /* User-edits that DO overlap an auto-block override it"
            f"     — include them in the user list too so smart-merge"
            f"     drops the (now superseded) auto version. */"
            f"  const overrides=ads.filter(u=>"
            f"    autoAds.some(a=>blocksOverlap(a,u)"
            f"      && (Math.abs(a[0]-u[0])>0.5 || Math.abs(a[1]-u[1])>0.5))"
            f"  );"
            f"  const userList=[...userOnly,...overrides]"
            f"    .sort((a,b)=>a[0]-b[0]);"
            f"  fetch('{HOST_URL}/api/recording/{uuid}/ads/edit',"
            f"    {{method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"     body:JSON.stringify({{ads:userList,deleted:userDeleted}})"
            f"    }}).catch(()=>{{}});"
            f"}}"
            f"/* Parse 'mm:ss' / 'hh:mm:ss' / 'ss' → seconds. */"
            f"function parseTimeToSec(s){{"
            f"  s=String(s||'').trim();if(!s)return null;"
            f"  const parts=s.split(':').map(p=>parseFloat(p));"
            f"  if(parts.some(isNaN))return null;"
            f"  let t=0;for(const p of parts)t=t*60+p;"
            f"  return t;"
            f"}}"
            f"/* Two-button mark-and-save flow for touchscreens —"
            f"   no time inputs, no modal. While the user watches:"
            f"   tap START at the moment the ad begins → the position"
            f"   is captured, the START button hides and ENDE appears."
            f"   At the end of the ad, tap ENDE → block is sealed and"
            f"   POSTed. Visual feedback: a translucent gray bar grows"
            f"   on the scrub bar from the start position to the live"
            f"   playhead while the block is staged. */"
            f"let _adStaging=null;  /* {{startTime}} or null */"
            f"const adStartBtn=document.getElementById('ad-start');"
            f"const adEndBtn  =document.getElementById('ad-end');"
            f"function _adShowToast(msg){{"
            f"  let t=document.getElementById('ad-toast');"
            f"  if(!t){{"
            f"    t=document.createElement('div');t.id='ad-toast';"
            f"    Object.assign(t.style,{{position:'fixed',left:'50%',"
            f"      bottom:'90px',transform:'translateX(-50%)',"
            f"      background:'#16a085',color:'#fff',padding:'8px 14px',"
            f"      borderRadius:'4px',fontSize:'.9em',zIndex:'2500',"
            f"      pointerEvents:'none',transition:'opacity .3s'}});"
            f"    document.body.appendChild(t);"
            f"  }}"
            f"  t.textContent=msg;t.style.opacity='1';"
            f"  clearTimeout(t._h);"
            f"  t._h=setTimeout(()=>{{t.style.opacity='0';}},2000);"
            f"}}"
            f"function _renderStagingBar(){{"
            f"  const old=document.querySelector('.ad-staging');"
            f"  if(old)old.remove();"
            f"  if(!_adStaging)return;"
            f"  const D=isFinite(v.duration)?v.duration:adsDurationFallback;"
            f"  if(D<=0)return;"
            f"  const s=_adStaging.startTime;"
            f"  const e=Math.max(s,v.currentTime||s);"
            f"  const el=document.createElement('div');"
            f"  el.className='ad-staging';"
            f"  el.style.left=((s/D)*100)+'%';"
            f"  el.style.width=Math.max(0.3,((e-s)/D)*100)+'%';"
            f"  scrub.appendChild(el);"
            f"}}"
            f"v.addEventListener('timeupdate',_renderStagingBar);"
            f"/* Find the block that the user is implicitly addressing"
            f"   for an in-place boundary edit. Priority: 1) playhead"
            f"   inside a block, 2) playhead within ±60 s of a block"
            f"   edge (extending mode). Returns the index or -1. */"
            f"const ADJUST_TOLERANCE_S=60;"
            f"function _adFindNearbyBlock(t){{"
            f"  for(let i=0;i<ads.length;i++){{"
            f"    if(t>=ads[i][0] && t<=ads[i][1]) return i;"
            f"  }}"
            f"  let best=-1, bestDist=ADJUST_TOLERANCE_S+1;"
            f"  for(let i=0;i<ads.length;i++){{"
            f"    const d=Math.min(Math.abs(t-ads[i][0]),Math.abs(t-ads[i][1]));"
            f"    if(d<bestDist){{best=i;bestDist=d;}}"
            f"  }}"
            f"  return best;"
            f"}}"
            f"function adMarkStart(){{"
            f"  const t=Math.max(0,v.currentTime||0);"
            f"  /* In-place edit: find nearby block and adjust its"
            f"     start to current playhead. Beats a 4-tap modal. */"
            f"  const idx=_adFindNearbyBlock(t);"
            f"  if(idx>=0){{"
            f"    const newStart=Math.min(t,ads[idx][1]-1);"
            f"    ads[idx]=[Math.max(0,newStart),ads[idx][1]];"
            f"    ads.sort((a,b)=>a[0]-b[0]);"
            f"    renderAds();saveAdsEdit();"
            f"    _adShowToast('Start korrigiert @ '+fmt(newStart));"
            f"    return;"
            f"  }}"
            f"  /* No block in range → stage a new one. */"
            f"  _adStaging={{startTime:t}};"
            f"  _adShowToast('Werbe-Start markiert @ '+fmt(t)+' — jetzt ⏹ Ende');"
            f"  _renderStagingBar();"
            f"}}"
            f"/* Mark this recording as reviewed: server promotes any"
            f"   currently-uncertain frame that isn't covered by a user"
            f"   ad-block to a confirmed-show negative for training,"
            f"   then the 🎯 badge disappears from the recordings list. */"
            f"function markReviewed(){{"
            f"  fetch('{HOST_URL}/api/recording/{uuid}/mark-reviewed',"
            f"    {{method:'POST'}}).then(r=>r.json()).then(d=>{{"
            f"      if(d && d.ok){{"
            f"        _adShowToast('Geprüft — '+d.added+' frame(s) als Show bestätigt');"
            f"        document.querySelectorAll('.uncertain-mark').forEach(e=>e.remove());"
            f"        uncertainPts=[];"
            f"      }} else {{"
            f"        _adShowToast('Fehler beim Speichern');"
            f"      }}"
            f"  }}).catch(()=>_adShowToast('Netzwerk-Fehler'));"
            f"}}"
            f"function redetectNow(){{"
            f"  if(!confirm('Detect neu starten? Auto-Cutlist wird "
            f"gelöscht + neu berechnet (~3 min). Manuelle Edits in "
            f"ads_user.json bleiben erhalten.')) return;"
            f"  fetch('{HOST_URL}/api/recording/{uuid}/redetect',"
            f"    {{method:'POST'}}).then(r=>r.json()).then(d=>{{"
            f"      if(d && d.ok){{"
            f"        _adShowToast('Re-Detect angefragt — neue Boundaries in ~3 min sichtbar');"
            f"      }} else {{"
            f"        _adShowToast('Fehler: '+(d && d.error || 'unbekannt'));"
            f"      }}"
            f"  }}).catch(()=>_adShowToast('Netzwerk-Fehler'));"
            f"}}"
            # Default trim range: 0 → overrun start (or recording end).
            # Was "current playback position → overrun" but that silently
            # nuked review-data when user clicked Trim mid-playback.
            # Safer default = "remove only the overrun tail"; user can
            # still set start via the "Aktuelle Stelle" button.
            f"function trimNow(){{"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  if(D<=0){{_adShowToast('Dauer noch nicht geladen');return;}}"
            f"  let defStart=0;"
            f"  let defEnd=Math.floor(D);"
            f"  fetch('{HOST_URL}/recording/{uuid}/ads').then(r=>r.json()).then(a=>{{"
            f"    if(a&&a.overrun&&Array.isArray(a.overrun)){{"
            f"      defEnd=Math.floor(a.overrun[0]);"
            f"    }}"
            f"    _showTrimModal(defStart,defEnd,D);"
            f"  }}).catch(()=>_showTrimModal(defStart,defEnd,D));"
            f"}}"
            f"function _showTrimModal(defStart,defEnd,D){{"
            f"  const m=document.createElement('div');"
            f"  m.className='ad-edit-modal';"
            f"  const f=t=>{{const h=Math.floor(t/3600),mm=Math.floor((t%3600)/60),s=Math.floor(t%60);"
            f"    return h>0?`${{h}}:${{String(mm).padStart(2,'0')}}:${{String(s).padStart(2,'0')}}`:`${{mm}}:${{String(s).padStart(2,'0')}}`;}};"
            f"  m.innerHTML='<div class=\"ad-edit-card\">'"
            f"   +'<div class=\"ad-edit-head\">✂️ Aufnahme beschneiden (verlustfrei)</div>'"
            f"   +'<div style=\"margin:4px 0 10px 0;color:#aaa;font-size:13px\">'"
            f"   +'Wähle den Bereich der <b>BEHALTEN</b> wird. Alles davor und danach wird gelöscht.'"
            f"   +'</div>'"
            f"   +'<div class=\"ad-edit-row\"><span class=\"ad-edit-label\">Behalten ab</span>'"
            f"   +'<span class=\"ad-edit-val\" id=\"trimValS\">'+f(defStart)+'</span>'"
            f"   +'<button class=\"ad-edit-grab\" id=\"trimGrabS\">Aktuelle Stelle</button></div>'"
            f"   +'<div class=\"ad-edit-row\"><span class=\"ad-edit-label\">Behalten bis</span>'"
            f"   +'<span class=\"ad-edit-val\" id=\"trimValE\">'+f(defEnd)+'</span>'"
            f"   +'<button class=\"ad-edit-grab\" id=\"trimGrabE\">Aktuelle Stelle</button></div>'"
            f"   +'<div style=\"margin:8px 0;color:#aaa;font-size:13px\">'"
            f"   +'Gesamt: '+f(D)+' → <span style=\"color:#8f8\">behalten: <b id=\"trimKeep\">'+f(defEnd-defStart)+'</b></span><br>'"
            f"   +'<span style=\"color:#f88\">gelöscht: <b>'+f(D-(defEnd-defStart))+'</b></span><br>'"
            f"   +'<small>Cut am nächsten Keyframe (±2s). Original wird ÜBERSCHRIEBEN. '"
            f"   +'ads_user-Blöcke im behaltenen Bereich werden geshifted, andere fallen weg.</small>'"
            f"   +'</div>'"
            f"   +'<div class=\"ad-edit-actions\">'"
            f"   +'<button class=\"ad-edit-cancel\" id=\"trimCancel\">Abbrechen</button>'"
            f"   +'<button class=\"ad-edit-save\" id=\"trimGo\" style=\"background:#c33\">Beschneiden</button>'"
            f"   +'</div></div>';"
            f"  document.body.appendChild(m);"
            f"  let curS=defStart,curE=defEnd;"
            f"  const elS=m.querySelector('#trimValS'),elE=m.querySelector('#trimValE'),elK=m.querySelector('#trimKeep');"
            f"  const upd=()=>{{elS.textContent=f(curS);elE.textContent=f(curE);elK.textContent=f(curE-curS);}};"
            f"  m.querySelector('#trimGrabS').onclick=()=>{{curS=Math.max(0,Math.floor(v.currentTime||0));upd();}};"
            f"  m.querySelector('#trimGrabE').onclick=()=>{{curE=Math.min(D,Math.floor(v.currentTime||0));upd();}};"
            f"  m.querySelector('#trimCancel').onclick=()=>m.remove();"
            f"  m.querySelector('#trimGo').onclick=()=>{{"
            f"    if(curE<=curS){{_adShowToast('Ende muss nach Start liegen');return;}}"
            f"    if(!confirm('Wirklich beschneiden? '+f(curE-curS)+' werden behalten, '+f(D-(curE-curS))+' GELÖSCHT (irreversibel).'))return;"
            f"    m.querySelector('#trimGo').textContent='Beschneide…';"
            f"    m.querySelector('#trimGo').disabled=true;"
            f"    fetch('{HOST_URL}/api/recording/{uuid}/trim',"
            f"      {{method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"       body:JSON.stringify({{start_s:curS,end_s:curE}})}})"
            f"     .then(r=>r.json()).then(d=>{{"
            f"        m.remove();"
            f"        if(d&&d.ok){{"
            f"          _adShowToast('Trim ok — '+d.saved_mb+'MB frei. Re-Detect+HLS-Rebuild laufen ~5min.');"
            f"          setTimeout(()=>location.reload(),3000);"
            f"        }}else{{_adShowToast('Fehler: '+(d&&d.error||'unbekannt'));}}"
            f"     }}).catch(()=>_adShowToast('Netzwerk-Fehler'));"
            f"  }};"
            f"}}"
            f"/* Undo auto-confirm: deletes ads_user.json IFF auto-"
            f"   confirmed (server enforces — manual reviews are safe). */"
            f"function undoAutoConfirm(){{"
            f"  if(!confirm('Auto-Confirm rückgängig? Werbeblöcke werden "
            f"beim nächsten Detect frisch berechnet.')) return;"
            f"  fetch('{HOST_URL}/api/recording/{uuid}/auto-confirm-undo',"
            f"    {{method:'POST'}}).then(r=>r.json()).then(d=>{{"
            f"      if(d && d.ok){{"
            f"        _adShowToast('Auto-Confirm rückgängig — Seite lädt neu');"
            f"        setTimeout(()=>location.reload(), 800);"
            f"      }} else {{"
            f"        _adShowToast('Fehler: '+(d && d.error || 'unbekannt'));"
            f"      }}"
            f"  }}).catch(()=>_adShowToast('Netzwerk-Fehler'));"
            f"}}"
            f"function adMarkEnd(){{"
            f"  const t=Math.max(0,v.currentTime||0);"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  /* Staging in progress takes priority: commits the"
            f"     half-open block the user opened with START. */"
            f"  if(_adStaging){{"
            f"    const s=_adStaging.startTime;"
            f"    const e=Math.max(s+1,t||s+1);"
            f"    const eClamp=D>0?Math.min(D,e):e;"
            f"    ads.push([s,eClamp]);"
            f"    ads.sort((a,b)=>a[0]-b[0]);"
            f"    _adStaging=null;"
            f"    document.querySelectorAll('.ad-staging').forEach(x=>x.remove());"
            f"    renderAds();saveAdsEdit();"
            f"    _adShowToast('Block gespeichert: '+fmt(s)+' – '+fmt(eClamp));"
            f"    return;"
            f"  }}"
            f"  /* Otherwise: in-place end-edit on nearby block. */"
            f"  const idx=_adFindNearbyBlock(t);"
            f"  if(idx<0){{_adShowToast('Kein Block in der Nähe');return;}}"
            f"  const newEnd=Math.max(ads[idx][0]+1,t);"
            f"  const eClamp=D>0?Math.min(D,newEnd):newEnd;"
            f"  ads[idx]=[ads[idx][0],eClamp];"
            f"  ads.sort((a,b)=>a[0]-b[0]);"
            f"  renderAds();saveAdsEdit();"
            f"  _adShowToast('Ende korrigiert @ '+fmt(eClamp));"
            f"}}"
            f"/* Mark-mode wrapper: same buttons drive ad-block marking"
            f"   OR bumper-template capture, depending on the mode toggle."
            f"   Three states cycle on tap:"
            f"     'ad'           — Start/End mark a Werbung-block"
            f"     'bumper-end'   — Start/End mark END-of-ad-break bumper"
            f"                       (ad→show transition; e.g. sixx 'WIE SIXX"
            f"                       IST DAS DENN?'). Snaps block.endS."
            f"     'bumper-start' — Start/End mark START-of-ad-break bumper"
            f"                       (show→ad transition; e.g. sixx 'WERBUNG'-"
            f"                       announcer card). Snaps block.startS."
            f"   The capture POST sends `kind` so the server writes to the"
            f"   correct .tvd-bumpers/<slug>/<kind>/ subdir. Mode persists"
            f"   in localStorage. */"
            f"const MARK_MODE_KEY='player-mark-mode';"
            f"const MARK_MODES=['ad','bumper-end','bumper-start','show-start'];"
            f"let _markMode='ad';"
            f"try{{const m=localStorage.getItem(MARK_MODE_KEY);"
            f"     if(MARK_MODES.indexOf(m)>=0)_markMode=m;}}catch(e){{}}"
            f"let _bumperStaging=null;"
            f"function _isBumperMode(){{return _markMode==='bumper-start'||_markMode==='bumper-end';}}"
            f"function _isShowStartMode(){{return _markMode==='show-start';}}"
            f"function _bumperKind(){{return _markMode==='bumper-start'?'start':'end';}}"
            f"function _renderMarkBtn(){{"
            f"  const b=document.getElementById('mark-mode');if(!b)return;"
            f"  if(_markMode==='bumper-end'){{b.textContent='🎬 Bumper End';b.classList.add('rec');}}"
            f"  else if(_markMode==='bumper-start'){{b.textContent='🎬 Bumper Start';b.classList.add('rec');}}"
            f"  else if(_markMode==='show-start'){{b.textContent='🎬 Show-Start';b.classList.add('rec');}}"
            f"  else{{b.textContent='🎯 Werbung';b.classList.remove('rec');}}"
            f"  /* Show step-buttons only in bumper modes — they're noisy"
            f"     during normal ad-block marking and useless in playback. */"
            f"  document.querySelectorAll('.bumper-only').forEach(el=>{{"
            f"    el.style.display=_isBumperMode()?'':'none';"
            f"  }});"
            f"}}"
            f"/* Pause-on-click + delta-jump. Pausing first ensures the"
            f"   visible frame matches the bumper-mark we will capture; the"
            f"   currentTime setter is robust against tiny floating-point"
            f"   drift across calls. */"
            f"function stepFrame(deltaS){{"
            f"  if(!v.paused)v.pause();"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  const t=Math.max(0,Math.min(D||1e9,(v.currentTime||0)+deltaS));"
            f"  v.currentTime=t;"
            f"}}"
            f"_renderMarkBtn();"
            f"function toggleMarkMode(){{"
            f"  const i=MARK_MODES.indexOf(_markMode);"
            f"  _markMode=MARK_MODES[(i+1)%MARK_MODES.length];"
            f"  try{{localStorage.setItem(MARK_MODE_KEY,_markMode);}}catch(e){{}}"
            f"  _renderMarkBtn();"
            f"  const msg=_markMode==='bumper-end'?"
            f"    'Bumper-End-Modus: Werbung→Show Übergang markieren':"
            f"    _markMode==='bumper-start'?"
            f"      'Bumper-Start-Modus: Show→Werbung Übergang markieren':"
            f"      _markMode==='show-start'?"
            f"        'Show-Start-Modus: Tap markiert wo die Sendung TATSÄCHLICH beginnt'"
            f"        +' (lernt EPG-Drift pro Show)':"
            f"        'Werbe-Modus: Start/Ende markieren einen Werbeblock';"
            f"  _adShowToast(msg);"
            f"}}"
            f"function markStart(){{"
            f"  if(_isShowStartMode()){{showStartMark();return;}}"
            f"  if(_isBumperMode()){{bumperMarkStart();}}else{{adMarkStart();}}"
            f"}}"
            f"function markEnd(){{"
            f"  if(_isShowStartMode()){{showStartMark();return;}}"
            f"  if(_isBumperMode()){{bumperMarkEnd();}}else{{adMarkEnd();}}"
            f"}}"
            f"/* One-tap: capture playhead as the actual show-start time."
            f"   Persisted as show_start_s in ads_user.json. Per-show drift"
            f"   aggregator on the gateway then suggests start_extra. */"
            f"function showStartMark(){{"
            f"  const t=Math.max(0,v.currentTime||0);"
            f"  fetch('{HOST_URL}/api/recording/{uuid}/show-start',"
            f"    {{method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"     body:JSON.stringify({{t:t}})}}"
            f"   ).then(r=>r.json()).then(d=>{{"
            f"     if(d&&d.ok){{"
            f"       let m='Show-Start @ '+fmt(t)+' gespeichert';"
            f"       if(d.drift_s!==undefined&&d.drift_s!==null){{"
            f"         const sign=d.drift_s<0?'früher':'später';"
            f"         m+=' (Sender '+Math.abs(Math.round(d.drift_s/60*10)/10)+' min '+sign+' als EPG)';"
            f"       }}"
            f"       _adShowToast(m);"
            f"     }}else{{_adShowToast('Fehler: '+(d&&d.error||'?'));}}"
            f"   }}).catch(()=>_adShowToast('Netzwerk-Fehler'));"
            f"}}"
            f"function bumperMarkStart(){{"
            f"  const t=Math.max(0,v.currentTime||0);"
            f"  _bumperStaging={{startTime:t}};"
            f"  _adShowToast('Bumper-Start @ '+fmt(t)+' — jetzt ⏹ Ende beim letzten Frame');"
            f"}}"
            f"function bumperMarkEnd(){{"
            f"  if(!_bumperStaging){{_adShowToast('Erst Bumper-Start klicken');return;}}"
            f"  const s=_bumperStaging.startTime;"
            f"  const e=Math.max(s+0.5,v.currentTime||s+0.5);"
            f"  if(e-s>30){{_adShowToast('Bumper-Fenster zu lang (max 30 s)');return;}}"
            f"  _bumperStaging=null;"
            f"  _adShowToast('Speichere Bumper-Frames…');"
            f"  fetch('{HOST_URL}/api/recording/{uuid}/bumper-capture',"
            f"    {{method:'POST',headers:{{'Content-Type':'application/json'}},"
            f"     body:JSON.stringify({{start_s:s,end_s:e,kind:_bumperKind()}})}})"
            f"   .then(r=>r.json()).then(d=>{{"
            f"      if(d&&d.ok){{"
            f"        let msg=d.count+' Frame(s) als Bumper-'+(d.kind||'end')+' für '+d.slug+' gespeichert';"
            f"        if(d.aligned){{"
            f"          msg+=' • Block '+d.aligned.boundary+' '+d.aligned.old.toFixed(1)+'s→'+d.aligned.new.toFixed(1)+'s justiert';"
            f"          /* Reload merged ads view so the scrub-bar marker"
            f"             jumps to the new boundary immediately. */"
            f"          fetch('{HOST_URL}/recording/{uuid}/ads').then(r=>r.json())"
            f"            .then(a=>{{ads=a.ads||[];renderAds();}}).catch(()=>{{}});"
            f"        }}"
            f"        if(d.skipped_oversized&&d.skipped_oversized.length){{"
            f"          msg+=' ('+d.skipped_oversized.length+' Show-Frame'+"
            f"               (d.skipped_oversized.length===1?'':'s')+' verworfen)';"
            f"        }}"
            f"        _adShowToast(msg);"
            f"      }}else{{"
            f"        _adShowToast('Fehler: '+(d&&d.error||'unbekannt'));"
            f"      }}"
            f"   }}).catch(e=>_adShowToast('Netzwerk-Fehler'));"
            f"}}"
            f"function openAdEditor(idx,isNew){{"
            f"  const blk=ads[idx];if(!blk)return;"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  const m=document.createElement('div');"
            f"  m.className='ad-edit-modal';"
            f"  /* Touch-first design — no time inputs. The user scrubs"
            f"     to the right position then taps a 'Übernehmen' button."
            f"     Block boundaries are stored in modal-local state and"
            f"     committed on Speichern. */"
            f"  m.innerHTML="
            f"    '<div class=\"ad-edit-card\">'"
            f"   +'<div class=\"ad-edit-head\">Werbeblock bearbeiten</div>'"
            f"   +'<div class=\"ad-edit-row\">'"
            f"   +'<span class=\"ad-edit-label\">Start</span>'"
            f"   +'<span class=\"ad-edit-val\" id=\"adValS\"></span>'"
            f"   +'<button class=\"ad-edit-grab\" id=\"adGrabS\">'"
            f"   +    'aktuelle Stelle'"
            f"   +'</button></div>'"
            f"   +'<div class=\"ad-edit-row\">'"
            f"   +'<span class=\"ad-edit-label\">Ende</span>'"
            f"   +'<span class=\"ad-edit-val\" id=\"adValE\"></span>'"
            f"   +'<button class=\"ad-edit-grab\" id=\"adGrabE\">'"
            f"   +    'aktuelle Stelle'"
            f"   +'</button></div>'"
            f"   +'<div class=\"ad-edit-hint\">'"
            f"   +    'Player-Position scrubben → Button drücken'"
            f"   +'</div>'"
            f"   +'<div class=\"ad-edit-btns\">'"
            f"   +'<button class=\"ad-edit-del\">Löschen</button>'"
            f"   +'<span class=\"spacer\"></span>'"
            f"   +'<button class=\"ad-edit-cancel\">Abbrechen</button>'"
            f"   +'<button class=\"ad-edit-save\">Speichern</button>'"
            f"   +'</div></div>';"
            f"  document.body.appendChild(m);"
            f"  /* Modal-local state: start/end times, updated by 'grab'"
            f"     buttons, displayed in the .ad-edit-val spans. */"
            f"  let modalS=blk[0], modalE=blk[1];"
            f"  const valS=m.querySelector('#adValS');"
            f"  const valE=m.querySelector('#adValE');"
            f"  const refreshVals=()=>{{"
            f"    valS.textContent=fmt(modalS);valE.textContent=fmt(modalE);"
            f"  }};"
            f"  refreshVals();"
            f"  m.querySelector('#adGrabS').addEventListener('click',()=>{{"
            f"    modalS=Math.max(0,v.currentTime||0);refreshVals();"
            f"  }});"
            f"  m.querySelector('#adGrabE').addEventListener('click',()=>{{"
            f"    modalE=Math.max(modalS+1,v.currentTime||(modalS+1));"
            f"    if(D>0)modalE=Math.min(D,modalE);"
            f"    refreshVals();"
            f"  }});"
            f"  /* In 'create' mode the placeholder was already pushed"
            f"     into ads[]; on Cancel we have to roll it back so the"
            f"     scrub bar doesn't keep an unconfirmed block. Save"
            f"     paths null isNew so this branch is a no-op there. */"
            f"  const close=()=>{{"
            f"    if(isNew){{"
            f"      const i=ads.findIndex(a=>a===blk);"
            f"      if(i>=0){{ads.splice(i,1);renderAds();}}"
            f"    }}"
            f"    m.remove();"
            f"  }};"
            f"  m.addEventListener('click',ev=>{{if(ev.target===m)close();}});"
            f"  m.querySelector('.ad-edit-cancel').addEventListener('click',close);"
            f"  m.querySelector('.ad-edit-del').addEventListener('click',()=>{{"
            f"    /* In create-mode (placeholder block, never saved):"
            f"       Delete means 'don't bother', same as Cancel. Do"
            f"       NOT record a userDeleted entry — the block isn't"
            f"       real yet, and any incidentally-overlapping auto"
            f"       block should stay untouched. */"
            f"    if(isNew){{close();return;}}"
            f"    /* For a real existing block: if it covers an auto-"
            f"       detected one, record the deletion explicitly so"
            f"       the next re-scan doesn't resurrect it. User-only"
            f"       blocks just disappear from the user list. */"
            f"    const target=ads[idx];"
            f"    const auto=autoAds.find(a=>blocksOverlap(a,target));"
            f"    if(auto && !userDeleted.some(d=>blocksOverlap(d,auto))){{"
            f"      userDeleted=[...userDeleted,auto].sort((a,b)=>a[0]-b[0]);"
            f"    }}"
            f"    ads.splice(idx,1);renderAds();saveAdsEdit();close();"
            f"  }});"
            f"  m.querySelector('.ad-edit-save').addEventListener('click',()=>{{"
            f"    if(modalE<=modalS+0.5){{"
            f"      valE.style.color='#e74c3c';return;"
            f"    }}"
            f"    ads[idx]=[Math.max(0,modalS),D>0?Math.min(D,modalE):modalE];"
            f"    ads.sort((a,b)=>a[0]-b[0]);"
            f"    /* Confirmed save — clear isNew so close() doesn't"
            f"       roll back the (now-real) block. */"
            f"    isNew=false;"
            f"    renderAds();saveAdsEdit();close();"
            f"  }});"
            f"}}"
            f"function refresh(){{"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  const T=v.currentTime||0;"
            f"  cur.textContent=fmt(T);dur.textContent=fmt(D);"
            f"  const pct=D>0?(T/D)*100:0;"
            f"  if(!_dragging){{played.style.width=pct+'%';thumb.style.left=pct+'%';}}"
            f"  skipBtn.classList.toggle('on',currentAd()!==null);"
            f"}}"
            f"v.addEventListener('timeupdate',refresh);"
            f"v.addEventListener('loadedmetadata',()=>{{refresh();renderAds();}});"
            f"v.addEventListener('durationchange',renderAds);"
            f"function seekTo(ev){{"
            f"  const r=scrub.getBoundingClientRect();"
            f"  const x=(ev.touches?ev.touches[0]:ev).clientX-r.left;"
            f"  const p=Math.max(0,Math.min(1,x/r.width));"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  if(D>0)v.currentTime=p*D;"
            f"}}"
            f"/* Tap policy on the recording player:"
            f"   - tap inside a CIRCULAR center zone (like the native"
            f"     iOS player's central play button) → play/pause"
            f"   - tap outside → show/hide chrome only, no pause"
            f"   - registered in CAPTURE phase + stopImmediatePropagation"
            f"     so it OVERRIDES the PLAYER_BASE_JS click handler"
            f"     which toggles play/pause on every click (correct for"
            f"     live channels but wrong for the recording player). */"
            f"function _isCenterTap(clientX,clientY){{"
            f"  const r=v.getBoundingClientRect();"
            f"  const cx=r.left+r.width/2, cy=r.top+r.height/2;"
            f"  const dx=clientX-cx, dy=clientY-cy;"
            f"  const radius=Math.min(r.width,r.height)*0.18;"
            f"  return dx*dx+dy*dy <= radius*radius;"
            f"}}"
            f"v.addEventListener('click',ev=>{{"
            f"  ev.stopImmediatePropagation();"
            f"  ev.preventDefault();"
            f"  /* Skip if a touch just fired — mobile sends a synthetic"
            f"     click after touchend; without this guard we'd toggle"
            f"     twice on iOS taps. */"
            f"  if(Date.now()-_lastTouchT<500)return;"
            f"  if(_isCenterTap(ev.clientX,ev.clientY)) togglePlay();"
            f"  if(chromeBar.classList.contains('hidden'))show();"
            f"  else {{chromeBar.classList.add('hidden');"
            f"    topbar.classList.add('hidden');}}"
            f"}},true);"
            f"/* CSS touch-action: manipulation tells the browser the"
            f"   player handles its own gestures — disables the iOS/"
            f"   Android double-tap-to-zoom delay without breaking"
            f"   single-tap. Belt-and-suspenders preventDefault on"
            f"   dblclick stops mouse-driven double-clicks too. */"
            f"v.style.touchAction='manipulation';"
            f"v.addEventListener('dblclick',ev=>ev.preventDefault());"
            f"/* Mobile single-tap goes through PLAYER_BASE_JS's"
            f"   touchend → 280 ms-deferred togglePlay() pipeline."
            f"   We can't intercept it the same way as a click; instead"
            f"   we wait one tick (so PLAYER_BASE has set _singleTapT)"
            f"   and CANCEL the pending toggle when the tap was outside"
            f"   the middle 60 % of the video — same gate as the click"
            f"   handler above. Chrome-visibility toggle still happens"
            f"   either way so the user can find the controls. */"
            f"v.addEventListener('touchend',ev=>{{"
            f"  if(!ev.changedTouches[0])return;"
            f"  const cx=ev.changedTouches[0].clientX;"
            f"  const cy=ev.changedTouches[0].clientY;"
            f"  setTimeout(()=>{{"
            f"    if(!_singleTapT)return;"
            f"    if(_isCenterTap(cx,cy))return;"
            f"    clearTimeout(_singleTapT);_singleTapT=null;"
            f"    if(chromeBar.classList.contains('hidden'))show();"
            f"    else {{chromeBar.classList.add('hidden');"
            f"      topbar.classList.add('hidden');}}"
            f"  }},0);"
            f"}});"
            f"document.addEventListener('mousemove',show);"
            f"const hideLoader=()=>loader.classList.add('hidden');"
            f"v.addEventListener('playing',hideLoader);"
            f"v.addEventListener('loadedmetadata',hideLoader);"
            f"v.addEventListener('canplay',hideLoader);"
            f"const LASTPOS_KEY='recpos_{uuid}';"
            f"const LASTPOS_TTL_MS=30*24*3600*1000;"
            f"function saveRecPos(){{"
            f"  const t=v.currentTime||0;"
            f"  if(!isFinite(v.duration)||v.duration<=0||t<=0)return;"
            f"  if(t>v.duration-10){{"
            f"    try{{localStorage.removeItem(LASTPOS_KEY);}}catch(e){{}}"
            f"    return;"
            f"  }}"
            f"  try{{localStorage.setItem(LASTPOS_KEY,"
            f"    JSON.stringify({{t:t,ts:Date.now()}}));}}catch(e){{}}"
            f"}}"
            f"function restoreRecPos(){{"
            f"  let entry=null;"
            f"  try{{entry=JSON.parse(localStorage.getItem(LASTPOS_KEY)||'null');}}"
            f"  catch(e){{}}"
            f"  if(!entry||Date.now()-entry.ts>LASTPOS_TTL_MS)return;"
            # Discard the stored seek-position if the playlist on disk
            # was rebuilt after we saved it — otherwise a recording
            # remuxed in the middle of being-watched lands the player
            # at an offset that no longer corresponds to the same
            # timeline (e.g. after a Mac re-remux that filled in
            # missing early segments).
            f"  if(window._recPlaylistMtime"
            f"     && entry.ts < window._recPlaylistMtime){{"
            f"    try{{localStorage.removeItem(LASTPOS_KEY);}}catch(e){{}}"
            f"    return;"
            f"  }}"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  if(entry.t>0&&entry.t<D-2)v.currentTime=entry.t;"
            f"}}"
            f"setInterval(()=>{{if(!v.paused)saveRecPos();}},15000);"
            f"window.addEventListener('beforeunload',saveRecPos);"
            f"document.addEventListener('visibilitychange',()=>{{"
            f"  if(document.visibilityState==='hidden')saveRecPos();"
            f"}});"
            f"v.addEventListener('pause',saveRecPos);"
            # Race-safe: defer restoreRecPos until the first progress
            # tick has populated _recPlaylistMtime (otherwise we miss
            # the freshness check and restore a stale offset). Cap at
            # 5 s so we don't sit forever if the endpoint is broken.
            # Deep-link from /search results: ?t=<seconds> jumps the
            # player straight to that position and overrides the saved
            # localStorage offset. The freshness wait is skipped — the
            # caller asked for an explicit time, not a "resume where
            # you left off" restore.
            f"const _deepT=(()=>{{"
            f"  try{{const u=new URL(window.location.href);"
            f"      const v=u.searchParams.get('t');"
            f"      if(v===null)return null;"
            f"      const f=parseFloat(v);return isFinite(f)&&f>=0?f:null;}}"
            f"  catch(e){{return null;}}"
            f"}})();"
            f"v.addEventListener('loadedmetadata',()=>{{"
            f"  if(_deepT!==null){{"
            f"    const D=isFinite(v.duration)?v.duration:0;"
            f"    if(D>0)v.currentTime=Math.max(0,Math.min(D-1,_deepT));"
            f"    else v.currentTime=_deepT;"
            f"    v.play().catch(()=>{{}});return;"
            f"  }}"
            f"  let waited=0;"
            f"  const tryRestore=()=>{{"
            f"    if(window._recPlaylistMtime||waited>=5000){{"
            f"      restoreRecPos();return;"
            f"    }}"
            f"    waited+=200;setTimeout(tryRestore,200);"
            f"  }};"
            f"  setTimeout(tryRestore,400);"
            f"}},{{once:true}});"
            f"let srcSet=false;"
            f"function tick(){{"
            f"  fetch('{HOST_URL}/recording/{uuid}/progress').then(r=>r.json())"
            f"   .then(d=>{{"
            # Pick up the playlist mtime so restoreRecPos can compare
            # it against the saved position's timestamp and discard
            # stale offsets after a re-remux.
            f"     if(d.playlist_mtime_ms)"
            f"       window._recPlaylistMtime=d.playlist_mtime_ms;"
            # Start playback once ~10 segments (60 s at hls_time=6) are
            # on disk — iOS can stream a growing EVENT playlist and will
            # keep polling for new segments as we remux.
            f"     const ready=d.done||d.segments>=10;"
            f"     if(ready&&!srcSet){{srcSet=true;v.src='{src}';v.load();"
            f"       v.play().catch(()=>{{}});}}"
            f"     if(d.done){{"
            f"       lmsg.textContent='Fertig — wird geladen…';"
            f"     }} else {{"
            f"       const pct=d.total>0?Math.min(99,Math.round(d.segments*100/d.total)):0;"
            f"       lmsg.textContent='Aufnahme wird vorbereitet… '+"
            f"         (d.total>0?pct+' %':(d.segments+' Segmente'));"
            f"       setTimeout(tick,1500);"
            f"     }}"
            f"   }}).catch(()=>setTimeout(tick,2500));"
            f"}}"
            f"tick();"
            f"let autoAds=[],userDeleted=[],adsDurationFallback=0,"
            f"    uncertainPts=[];"
            f"function fetchAds(){{"
            f"  fetch('{HOST_URL}/recording/{uuid}/ads').then(r=>r.json())"
            f"   .then(d=>{{"
            f"     ads=d.ads||[];autoAds=d.auto||[];userDeleted=d.deleted||[];"
            f"     adsDurationFallback=d.duration_s||0;"
            f"     uncertainPts=d.uncertain||[];"
            f"     renderAds();renderUncertain();refresh();"
            f"     if(d.running)setTimeout(fetchAds,10000);"
            f"   }}).catch(()=>{{}});"
            f"}}"
            f"/* Active-learning markers: small chevrons on the scrub"
            f"   bar at frames where the trained NN is least sure"
            f"   (~p=0.5). Click jumps the player there so the user"
            f"   can verify ad-vs-show and refine the cutlist. */"
            f"function renderUncertain(){{"
            f"  document.querySelectorAll('.uncertain-mark').forEach("
            f"    e=>e.remove());"
            f"  const vd=isFinite(v.duration)?v.duration:0;"
            f"  const D=vd>0?vd:adsDurationFallback;"
            f"  if(D<=0||!uncertainPts.length)return;"
            f"  uncertainPts.forEach(pt=>{{"
            f"    const el=document.createElement('div');"
            f"    el.className='uncertain-mark';"
            f"    el.style.left=((pt.t/D)*100)+'%';"
            f"    el.title='NN unsicher (p='+pt.p.toFixed(2)+') — Werbung?';"
            f"    el.addEventListener('click',ev=>{{"
            f"      ev.stopPropagation();"
            f"      v.currentTime=Math.max(0,pt.t-2);"
            f"      v.play().catch(()=>{{}});"
            f"    }});"
            f"    scrub.appendChild(el);"
            f"  }});"
            f"}}"
            f"v.addEventListener('loadedmetadata',()=>renderUncertain());"
            f"v.addEventListener('durationchange',renderUncertain);"
            f"fetchAds();"
            f"let thumbMeta={{count:0,interval:30,done:false}};"
            f"const thumbPrev=document.createElement('div');"
            f"thumbPrev.id='thumb-preview';"
            f"const thumbImg=document.createElement('img');"
            f"thumbImg.alt='';thumbImg.decoding='async';"
            f"thumbPrev.appendChild(thumbImg);"
            f"document.body.appendChild(thumbPrev);"
            f"function fetchThumbs(){{"
            f"  fetch('{HOST_URL}/recording/{uuid}/thumbs.json').then(r=>r.json())"
            f"   .then(d=>{{thumbMeta=d;if(!d.done)setTimeout(fetchThumbs,8000);}})"
            f"   .catch(()=>setTimeout(fetchThumbs,8000));"
            f"}}"
            f"fetchThumbs();"
            f"let lastThumbIdx=0;"
            f"function showThumb(ev){{"
            f"  if(!thumbMeta.count)return;"
            f"  const r=scrub.getBoundingClientRect();"
            f"  const x=(ev.touches?ev.touches[0]:ev).clientX-r.left;"
            f"  const p=Math.max(0,Math.min(1,x/r.width));"
            f"  const D=isFinite(v.duration)?v.duration:0;"
            f"  if(D<=0)return;"
            f"  const t=p*D;"
            f"  const idx=Math.min(thumbMeta.count,"
            f"    Math.max(1,Math.floor(t/thumbMeta.interval)+1));"
            f"  if(idx!==lastThumbIdx){{"
            f"    lastThumbIdx=idx;"
            f"    thumbImg.src='{HOST_URL}/recording/{uuid}/thumbs/t'+"
            f"      String(idx).padStart(5,'0')+'.jpg';"
            f"  }}"
            f"  const thumbW=160;"
            f"  let left=ev.clientX-thumbW/2;"
            f"  left=Math.max(8,Math.min(window.innerWidth-thumbW-8,left));"
            f"  thumbPrev.style.left=left+'px';"
            f"  thumbPrev.style.bottom=(window.innerHeight-r.top+10)+'px';"
            f"  thumbPrev.classList.add('visible');"
            f"}}"
            f"function hideThumb(){{thumbPrev.classList.remove('visible');}}"
            f"scrub.addEventListener('mousemove',showThumb);"
            f"scrub.addEventListener('touchmove',showThumb,{{passive:true}});"
            f"scrub.addEventListener('mouseleave',hideThumb);"
            f"scrub.addEventListener('touchend',hideThumb);"
            f"document.addEventListener('keydown',e=>{{"
            f"  if(e.key==='Escape')closePlayer();"
            f"  else if(e.key==='ArrowRight')seek(10);"
            f"  else if(e.key==='ArrowLeft')seek(-10);"
            f"  else if(e.key===' '){{e.preventDefault();togglePlay();show();}}"
            f"}});"
            # Auto-mark watched + watched-cleanup-loop both DISABLED
            # 2026-05-05: user wants explicit control over recording
            # lifetime. Manual ✓ via the recordings list still works
            # (= toggle the circle), but playback no longer auto-marks
            # AND the 6h cleanup loop no longer auto-deletes
            # (see _cleanup_watched_loop start commented out below).
            f"</script></body></html>")
    return html


@app.route("/mediathek-rec/<uuid>/file.mp4")
def mediathek_rec_file(uuid):
    """Serve a ripped Mediathek recording. send_from_directory handles
    HTTP range requests correctly, which iOS needs for MP4 seeking."""
    out_dir = HLS_DIR / f"_{uuid}"
    if not (out_dir / "file.mp4").exists():
        abort(404)
    return send_from_directory(out_dir, "file.mp4",
                                  mimetype="video/mp4",
                                  conditional=True)


@app.route("/mediathek-rec/<uuid>/delete", methods=["DELETE"])
def delete_mediathek_rec(uuid):
    """Drop a virtual Mediathek recording from the local list. Also
    cleans up the ripped MP4 if one exists — the whole point of
    "delete" is freeing the disk.

    DELETE-only since 2026-05-03: prevents the smoke-route hitter
    + accidental browser-prefetch from triggering deletion via a
    bare GET. UI uses fetch(method:'DELETE') from the global
    .del-btn click handler in the recordings page."""
    with _mediathek_rec_lock:
        _mediathek_rec.pop(uuid, None)
    save_mediathek_rec()
    shutil.rmtree(HLS_DIR / f"_{uuid}", ignore_errors=True)
    return _cors(Response(json.dumps({"ok": True}),
                          mimetype="application/json"))


@app.route("/recording/<uuid>/delete", methods=["DELETE"])
def delete_recording(uuid):
    """Cancel/delete a DVR entry.

    DELETE-only since 2026-05-03: prevents the smoke-route hitter
    + accidental browser-prefetch from triggering deletion via a
    bare GET. UI uses fetch(method:'DELETE') from the global
    .del-btn click handler in the recordings page."""
    # Kill any running ffmpeg remux and comskip, remove cached HLS output
    with _rec_hls_lock:
        p = _rec_hls_procs.pop(uuid, None)
    if p and p["proc"].poll() is None:
        try: p["proc"].kill()
        except Exception: pass
    with _rec_cskip_lock:
        c = _rec_cskip_procs.pop(uuid, None)
    if c and c["proc"].poll() is None:
        try: c["proc"].kill()
        except Exception: pass

    # Determine entry state so we can pick the right tvh call.
    # Three states, three behaviours:
    #   scheduled → set enabled=False via /api/idnode/save. The entry
    #               stays in tvh's DB but won't tune; autorec sees an
    #               existing entry for this broadcast and skips
    #               re-creation. /cancel and /remove both drop the
    #               entry entirely, after which autorec re-schedules
    #               within seconds (= "delete" appears as no-op).
    #   recording → /cancel stops the tuner; tvh marks the entry
    #               status="Aborted by user" + sched_status=completedError.
    #               The UI filter ignores "Aborted by user" entries.
    #   completed → /cancel + /remove purges entry + file. Broadcast
    #               is in the past, autorec won't re-create.
    state = ""
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000", timeout=5).read())
        for e in data.get("entries", []):
            if e.get("uuid") == uuid:
                state = e.get("sched_status", "")
                break
    except Exception:
        pass
    body_uuid = urllib.parse.urlencode({"uuid": uuid}).encode()
    if state == "scheduled":
        node = json.dumps({"uuid": uuid, "enabled": False})
        body_save = urllib.parse.urlencode({"node": node}).encode()
        try:
            req = urllib.request.Request(f"{dvr_base()}/api/idnode/save",
                                          data=body_save, method="POST")
            urllib.request.urlopen(req, timeout=5).read()
        except Exception:
            pass
    elif state == "recording":
        # Set enabled=False ONLY (no /cancel). tvh automatically stops
        # the tuner when an entry becomes disabled mid-recording — the
        # entry transitions to status="Aborted by user",
        # sched_status=completedError, enabled=False. Without /cancel,
        # tvh treats this as "user disabled this entry" (durable marker)
        # rather than "recording failed" (cleaned up after timeout) —
        # autorec then leaves the broadcast alone instead of retrying
        # the moment its internal cleanup loop wipes the failed-state
        # entry.
        node = json.dumps({"uuid": uuid, "enabled": False})
        body_save = urllib.parse.urlencode({"node": node}).encode()
        try:
            req = urllib.request.Request(f"{dvr_base()}/api/idnode/save",
                                          data=body_save, method="POST")
            urllib.request.urlopen(req, timeout=5).read()
        except Exception:
            pass
    elif state == "completedError":
        # Failed recordings ("Time missed", "File missing", "Aborted
        # by user", etc.) — use enabled=False, NOT /remove. Autorec
        # otherwise re-creates entries for the same EPG broadcast
        # within seconds, leaving the user's "delete" looking like
        # a no-op (= 14.05: GZSZ 12.05 + 13.05 deleted via UI, both
        # back with new uuids minutes later because autorec re-tried
        # the failed broadcasts). enabled=False persists as a "user
        # explicitly rejected this" marker that survives the autorec
        # loop's broadcast-rescan.
        node = json.dumps({"uuid": uuid, "enabled": False})
        body_save = urllib.parse.urlencode({"node": node}).encode()
        try:
            req = urllib.request.Request(f"{dvr_base()}/api/idnode/save",
                                          data=body_save, method="POST")
            urllib.request.urlopen(req, timeout=5).read()
        except Exception:
            pass
    else:
        for ep in ("/api/dvr/entry/cancel", "/api/dvr/entry/remove"):
            try:
                req = urllib.request.Request(f"{dvr_base()}{ep}",
                                              data=body_uuid, method="POST")
                urllib.request.urlopen(req, timeout=5).read()
            except Exception:
                pass
    shutil.rmtree(HLS_DIR / f"_rec_{uuid}", ignore_errors=True)
    return _cors(Response(json.dumps({"ok": True}),
                          mimetype="application/json"))


@app.route("/reload")
def reload_channels():
    load_favorites()
    return f"{len(channel_map)} Kanäle geladen\n"


@app.route("/prewarm/<slug>")
def prewarm_channel(slug):
    """Start ffmpeg for channel without waiting for segments — returns
    immediately. Use for background prewarming from the Watch player."""
    with cmap_lock:
        if slug not in channel_map:
            abort(404, "unknown channel")
    ensure_running(slug)
    return _cors(Response(json.dumps({"ok": True, "slug": slug}),
                           mimetype="application/json"))


@app.route("/stop/<slug>")
def stop_channel_endpoint(slug):
    """Immediately kill the ffmpeg for this channel, freeing the tuner."""
    with active_lock:
        running = slug in channels
    if running:
        stop_channel(slug)
        return _cors(Response(json.dumps({"ok": True, "stopped": slug}),
                               mimetype="application/json"))
    return _cors(Response(json.dumps({"ok": True, "stopped": None}),
                           mimetype="application/json"))


@app.route("/stop-all")
def stop_all_endpoint():
    """Free all tuners (stop every running channel)."""
    with active_lock:
        slugs = list(channels.keys())
    for s in slugs:
        stop_channel(s)
    return _cors(Response(json.dumps({"ok": True, "stopped": slugs}),
                           mimetype="application/json"))


@app.route("/static/ch-logos/<fname>")
def static_ch_logo(fname):
    """Serve a bundled / user-supplied channel logo from the repo's
    static/ch-logos/ directory. Falls back to 404 if missing — the
    recordings template probes for file existence before building the
    URL so a 404 here implies drift, not a normal miss."""
    if "/" in fname or ".." in fname:
        abort(400)
    return send_from_directory(CH_LOGO_DIR, fname,
                                 max_age=86400)


def _channel_logo_url(slug, fallback, ext_priority=("svg", "png", "jpg")):
    """Return the best channel-logo URL for `slug`. Prefers a local
    override from static/ch-logos/<slug>.(svg|png|jpg) over the
    low-res tvheadend imagecache default. `fallback` may be a full
    URL (already-prefixed icon_public_url) or a relative path like
    "imagecache/108". `ext_priority` lets callers prefer PNG over SVG
    when the consumer can't render SVG (iOS UIImage)."""
    if slug:
        for ext in ext_priority:
            p = CH_LOGO_DIR / f"{slug}.{ext}"
            if p.is_file():
                return f"{HOST_URL}/static/ch-logos/{slug}.{ext}"
    if not fallback:
        return ""
    # tvh's imagecache (:9981) was decommissioned 2026-05-27 and tv-receiver
    # has no equivalent, so any imagecache/<id> fallback is a dead URL. Return
    # "" (-> client shows a placeholder) instead of a 404/502. The curated
    # /static/ch-logos/<slug> override above still wins for channels that have
    # one; only logo-less niche channels lose their (already-broken) icon.
    if "imagecache" in fallback:
        return ""
    if fallback.startswith("http"):
        return fallback
    return f"{HOST_URL}/{fallback.lstrip('/')}"


@app.route("/status")
def status():
    now = time.time()
    with active_lock:
        active = [{"slug": s,
                    "name": channel_map.get(s, {}).get("name", "?"),
                    "idle_seconds": int(now - i["last_seen"]),
                    "buffer_seconds": min(int(now - i.get("started_at", now)),
                                           WINDOW_SECONDS),
                    "always_warm": s in ALWAYS_WARM,
                    "codecs": codec_cache.get(s)}
                   for s, i in channels.items()]
    with codec_lock:
        probed = {s: c for s, c in codec_cache.items()}
    return {"total_channels": len(channel_map),
            "max_warm": MAX_WARM_STREAMS,
            "always_warm": sorted(ALWAYS_WARM),
            "active": active,
            "probed_codecs": probed}


_dvr_upcoming_cache = {"count": 0, "expires": 0}
_tuner_cache = {"used": None, "total": None, "expires": 0}


def tuner_status():
    """Return (used, total, epggrab) for FRITZ!Box SAT>IP tuners.
    `used` counts tuners tuned to any mux, `epggrab` is the subset
    that tvheadend is currently using for EIT collection. Cached 5 s."""
    now = time.time()
    if now < _tuner_cache["expires"]:
        return (_tuner_cache["used"], _tuner_cache["total"],
                _tuner_cache.get("epggrab", 0))
    used, total, epg = None, TUNER_TOTAL, 0
    # tv-receiver /healthz reports per-slot consumer counts; an active slot
    # (= consumers>0) maps 1:1 to "tuner in use".
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TV_RECEIVER_BASE}/healthz", timeout=2).read())
        slots = data.get("slots", [])
        if slots is not None:
            # tv-receiver dials slots LAZILY and keeps them warm-forever,
            # each holding one FritzBox tuner allocation. So a dialed slot =
            # a tuner in use; total capacity stays TUNER_TOTAL (the hw tuner
            # count), NOT len(slots) — that's only the currently-dialed
            # subset and would render "1/1" when a single mux is active.
            used = len(slots)
    except Exception:
        pass
    try:
        subs = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/status/subscriptions", timeout=2).read())
        epg = sum(1 for e in subs.get("entries", [])
                  if e.get("title", "").lower() == "epggrab")
    except Exception:
        pass
    _tuner_cache["used"] = used
    _tuner_cache["total"] = total
    _tuner_cache["epggrab"] = epg
    _tuner_cache["expires"] = now + 5
    return used, total, epg


def _compute_tuner_conflicts(now_ts):
    """Walk all upcoming/running DVR entries, group by overlapping
    time windows, count UNIQUE muxes per cluster. Channels on the
    same mux share one tuner. Returns dict {uuid: peak_mux_count}
    where peak is the max count across all overlap-clusters that
    entry belongs to. uuid not in dict = no conflict info available.
    Conflict exists if peak > TUNER_TOTAL.
    """
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=500", timeout=5).read())
    except Exception:
        return {}
    # Channel-name → mux_uuid map (falls back to channel name itself
    # so unmapped channels still count individually).
    with cmap_lock:
        ch_to_mux = {}
        for slug, info in channel_map.items():
            name = info.get("name") or ""
            mux = info.get("mux_uuid") or name
            if name:
                ch_to_mux[name] = mux
    relevant = []
    for e in data.get("entries", []):
        if e.get("sched_status") not in ("scheduled", "recording"):
            continue
        # User-disabled entries (= cancelled via /recording/<uuid>/delete
        # but kept in DB so autorec doesn't re-create) won't actually
        # tune, so they don't count toward the 5/4 tuner overbook
        # warning. Without this filter, cancelled-but-disabled entries
        # would still inflate the per-cluster mux count.
        if not e.get("enabled", True):
            continue
        s = e.get("start") or 0
        p = e.get("stop") or 0
        if not s or not p or p < now_ts:
            continue
        ch = e.get("channelname") or ""
        mu = ch_to_mux.get(ch, ch)
        relevant.append((s, p, mu, e.get("uuid", "")))
    out = {}
    for s, p, mu, u in relevant:
        muxes = set()
        for s2, p2, mu2, u2 in relevant:
            if s2 < p and p2 > s:
                muxes.add(mu2)
        out[u] = len(muxes)
    return out


_pinned_mux_cache = {"muxes": set(), "running": 0, "expires": 0}


def _mux_from_svc(svc):
    # tvheadend subscription 'service' looks like
    # "SAT>IP DVB-C Tuner #1 (...)/FritzBox DVB-C/546MHz/VOX"
    # The second-to-last slash-separated component is the mux id.
    parts = (svc or "").split("/")
    return parts[-2] if len(parts) >= 2 else None


def pinned_mux_info():
    """Return (distinct_mux_count, running_pin_count) for pinned channels
    with an active tvheadend subscription. Lets us give a mux-sharing
    bonus to the pin limit — two pins on the same transponder cost one
    tuner, not two."""
    now = time.time()
    if now < _pinned_mux_cache["expires"]:
        return len(_pinned_mux_cache["muxes"]), _pinned_mux_cache["running"]
    muxes, running = set(), 0
    with cmap_lock:
        name_to_slug = {info["name"]: s for s, info in channel_map.items()}
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/status/subscriptions", timeout=2).read())
        for e in data.get("entries", []):
            slug = name_to_slug.get(e.get("channel", ""))
            if slug and slug in ALWAYS_WARM:
                running += 1
                m = _mux_from_svc(e.get("service"))
                if m:
                    muxes.add(m)
    except Exception:
        pass
    _pinned_mux_cache["muxes"] = muxes
    _pinned_mux_cache["running"] = running
    _pinned_mux_cache["expires"] = now + 5
    return len(muxes), running


def compute_pin_limit():
    """Tuner-based pin cap. Base = total tuners minus 1 reserved for
    ad-hoc viewing minus active DVR jobs. Bonus = pins that share muxes
    with other pins (they cost a fractional tuner each). Falls back to
    the static PIN_HARD_MAX if tvheadend status is unavailable."""
    used, total, _ = tuner_status()
    total = total or TUNER_TOTAL
    dvr = active_dvr_count()
    base = max(0, total - 1 - dvr)
    distinct_muxes, running_pins = pinned_mux_info()
    mux_bonus = max(0, running_pins - distinct_muxes)
    limit = base + mux_bonus
    # Never retroactively shrink below what's already pinned.
    return max(limit, len(ALWAYS_WARM))


def active_dvr_count():
    """Count of tvheadend DVR entries that are either airing right now
    or scheduled within the next 10 min — these will hold a tuner. We
    cache for 30 s to keep the /warm-status poll lightweight."""
    now = time.time()
    if now < _dvr_upcoming_cache["expires"]:
        return _dvr_upcoming_cache["count"]
    count = 0
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid_upcoming?limit=50",
            timeout=4).read())
        horizon = int(now) + 600
        for e in data.get("entries", []):
            start = e.get("start", 0)
            stop = e.get("stop", 0)
            if stop > now and start <= horizon:
                count += 1
    except Exception:
        pass
    _dvr_upcoming_cache["count"] = count
    _dvr_upcoming_cache["expires"] = now + 30
    return count


@app.route("/api/always-warm/<slug>", methods=["POST"])
def api_always_warm(slug):
    """Toggle permanent-warm on a channel. Body: {"on": true|false}.
    Refuses new pins that would exceed the tuner-derived dynamic cap."""
    with cmap_lock:
        if slug not in channel_map:
            abort(404)
    try:
        body = json.loads(request.get_data() or b"{}")
    except Exception:
        body = {}
    want = bool(body.get("on", True))
    if want and slug not in ALWAYS_WARM:
        pin_limit = compute_pin_limit()
        if len(ALWAYS_WARM) >= pin_limit:
            return Response(json.dumps({
                "slug": slug, "on": False, "changed": False,
                "error": "Pin-Limit erreicht (Tuner-Reserve für DVR)",
            }), status=409, mimetype="application/json")
    changed = set_always_warm(slug, want)
    return _cors(Response(json.dumps({"slug": slug, "on": want,
                                        "changed": changed}),
                           mimetype="application/json"))


@app.route("/api/warm-status")
def api_warm_status():
    """Compact per-channel warm-state for the client (channel grid)."""
    now = time.time()
    with active_lock:
        out = {s: {
            "running": True,
            "idle_seconds": int(now - i["last_seen"]),
            "buffer_seconds": min(int(now - i.get("started_at", now)),
                                    WINDOW_SECONDS),
            "always_warm": s in ALWAYS_WARM,
        } for s, i in channels.items()}
    # Add a single now-playing string per channel — ONE EPG fetch for
    # the whole grid is much cheaper than N /api/now calls from the JS.
    try:
        now_ts = int(now)
        epg = fetch_epg(window_before=0, window_after=0)
        with cmap_lock:
            slugs = list(channel_map.keys())
        for slug in slugs:
            title = None
            for ev in epg["events"].get(slug, []):
                if ev["start"] <= now_ts < ev["stop"]:
                    title = ev.get("title")
                    break
            out.setdefault(slug, {"running": False})["now"] = title
    except Exception:
        pass
    with _dormant_pins_lock:
        dormant_snapshot = set(_dormant_pins)
    for s in ALWAYS_WARM:
        out.setdefault(s, {"running": False, "always_warm": True,
                            "idle_seconds": 0, "buffer_seconds": 0})
        # setdefault only inserts the always_warm flag when the key is
        # NEW. If the channel was already in `out` (because it had a
        # 'now' title or an active session), the flag never got set —
        # the frontend then thought it wasn't pinned, hid the pin
        # icon, and wouldn't let the user toggle it. Set it explicitly.
        out[s]["always_warm"] = True
        out[s]["dormant"] = s in dormant_snapshot
    dvr_busy = active_dvr_count()
    pins_used = len(ALWAYS_WARM)
    pin_limit = compute_pin_limit()
    pin_budget = max(0, pin_limit - pins_used)
    tuners_used, tuners_total, tuners_epggrab = tuner_status()
    ffmpeg_count = 0
    try:
        for p in Path("/proc").iterdir():
            if not p.name.isdigit():
                continue
            try:
                comm = (p / "comm").read_text().strip()
                status = (p / "status").read_text()
            except Exception:
                continue
            if comm != "ffmpeg":
                continue
            if "\nState:\tZ" in status:   # ignore zombies
                continue
            ffmpeg_count += 1
    except Exception:
        pass
    # Host metrics — /proc/loadavg, /proc/meminfo and the thermal zone
    # reflect the Pi 5 host even inside the container.
    load1 = None
    try:
        load1 = float(Path("/proc/loadavg").read_text().split()[0])
    except Exception:
        pass
    cpu_count = os.cpu_count() or 4
    mem_total_mb = mem_avail_mb = None
    try:
        meminfo = Path("/proc/meminfo").read_text()
        for line in meminfo.splitlines():
            if line.startswith("MemTotal:"):
                mem_total_mb = int(line.split()[1]) // 1024
            elif line.startswith("MemAvailable:"):
                mem_avail_mb = int(line.split()[1]) // 1024
    except Exception:
        pass
    cpu_temp_c = None
    try:
        cpu_temp_c = round(int(Path(
            "/sys/class/thermal/thermal_zone0/temp").read_text()) / 1000, 1)
    except Exception:
        pass
    disk_free_gb = disk_total_gb = None
    try:
        import shutil as _sh
        du = _sh.disk_usage(HLS_DIR)
        disk_free_gb = round(du.free / (1024**3), 1)
        disk_total_gb = round(du.total / (1024**3), 1)
    except Exception:
        pass
    return _cors(Response(json.dumps({
        "channels": out,
        "max_warm": MAX_WARM_STREAMS,
        "window_seconds": WINDOW_SECONDS,
        "pin_hard_max": pin_limit,
        "pin_dvr_reserve": dvr_busy,
        "pin_limit": pin_limit,
        "pin_budget": pin_budget,
        "tuners_used": tuners_used,
        "tuners_total": tuners_total,
        "tuners_epggrab": tuners_epggrab,
        "adskip_slug": _live_ads_proc.get("slug"),
        "ffmpeg_count": ffmpeg_count,
        "load1": load1,
        "cpu_count": cpu_count,
        "mem_total_mb": mem_total_mb,
        "mem_avail_mb": mem_avail_mb,
        "cpu_temp_c": cpu_temp_c,
        "disk_free_gb": disk_free_gb,
        "disk_total_gb": disk_total_gb,
    }), mimetype="application/json"))


# Mac-daemon heartbeats — synthesised from /api/internal/* endpoint
# hits. Replaces the old SMB-written .mac-*-alive files which break
# under macOS TCC for launchd Aqua agents accessing network mounts.
_daemon_last_poll = 0.0          # rec-side daemon (thumbs/hls/detect)
_live_scanner_last_poll = 0.0    # live-detect script (active-channels)


def _hb_age(filename):
    """Seconds since the named heartbeat file was last touched, or None.
    For .mac-{comskip,live-comskip}-alive we synthesise the value from
    the in-memory daemon-poll timestamps instead of reading a file —
    daemons pull jobs over HTTP, don't write to the SMB share."""
    if filename == ".mac-comskip-alive":
        # tv-recorder owns the detect/hls/thumbs pollers now and bumps
        # .daemon-last-poll's mtime; fall back to the in-memory global for
        # any still-Flask poller. Whichever is fresher wins.
        ts = _daemon_last_poll
        try:
            ts = max(ts, (HLS_DIR / ".daemon-last-poll").stat().st_mtime)
        except Exception:
            pass
        if ts == 0:
            return None
        return int(time.time() - ts)
    if filename == ".mac-live-comskip-alive":
        if _live_scanner_last_poll == 0:
            return None
        return int(time.time() - _live_scanner_last_poll)
    try:
        return int(time.time() - (HLS_DIR / filename).stat().st_mtime)
    except Exception:
        return None


def _compute_feedback_stats():
    """Walk every _rec_<uuid>/ads_user.json, diff against ads.json,
    aggregate per channel-slug. Returns {slug: {n, mean_dstart,
    mean_dend, added, deleted, sample_uuids[:3]}}.

    Δstart = user_start - auto_start (negative → user pulled start
    earlier, suggesting comskip detects the slot too late).
    Δend   = user_end - auto_end (positive → user extended ad past
    comskip's call, suggesting sponsor-card padding).

    Match: each user block paired with the auto block it overlaps
    most. Unpaired user blocks count as 'added' (comskip missed),
    unpaired auto blocks as 'deleted' (comskip false positive)."""
    stats = {}
    if not HLS_DIR.exists():
        return stats
    for d in HLS_DIR.glob("_rec_*"):
        uuid = d.name[5:]
        user_p = d / "ads_user.json"
        auto_p = d / "ads.json"
        if not user_p.is_file():
            continue
        try:
            user_raw = json.loads(user_p.read_text())
            auto_ads = json.loads(auto_p.read_text()) if auto_p.is_file() else []
        except Exception:
            continue
        # Backward-compat: ads_user.json may be either the legacy
        # list-of-pairs or the {"ads":[…], "deleted":[…]} dict the
        # smart-merge UI writes. Stats only care about the user's
        # positive list (added/refined blocks), not deletions.
        if isinstance(user_raw, list):
            user_ads = user_raw
        elif isinstance(user_raw, dict):
            user_ads = user_raw.get("ads") or []
        else:
            user_ads = []
        slug = _rec_channel_slug(uuid) or "?"
        s = stats.setdefault(slug, {
            "n": 0, "dstart_sum": 0.0, "dend_sum": 0.0, "matched": 0,
            "added": 0, "deleted": 0, "sample_uuids": [],
        })
        s["n"] += 1
        if len(s["sample_uuids"]) < 3:
            s["sample_uuids"].append(uuid)
        # Pair user blocks with their best-overlapping auto block.
        used = set()
        for us, ue in user_ads:
            best = None; best_overlap = 0.0
            for i, (as_, ae) in enumerate(auto_ads):
                if i in used:
                    continue
                ov = max(0.0, min(ue, ae) - max(us, as_))
                if ov > best_overlap:
                    best_overlap = ov; best = i
            if best is not None and best_overlap > 5.0:
                used.add(best)
                as_, ae = auto_ads[best]
                s["dstart_sum"] += us - as_
                s["dend_sum"] += ue - ae
                s["matched"] += 1
            else:
                s["added"] += 1
        for i in range(len(auto_ads)):
            if i not in used:
                s["deleted"] += 1
    # Finalise: compute means + suggestions.
    out = {}
    for slug, s in stats.items():
        if s["matched"] > 0:
            mean_ds = s["dstart_sum"] / s["matched"]
            mean_de = s["dend_sum"] / s["matched"]
        else:
            mean_ds = mean_de = 0.0
        suggestions = []
        if s["matched"] >= 3 and mean_ds <= -8:
            suggestions.append(
                f"START_LAG_FALLBACK[{slug!r}] = {abs(mean_ds):.0f}")
        if s["matched"] >= 3 and mean_de >= 8:
            suggestions.append(
                f"SPONSOR_DURATION_BY_CHANNEL[{slug!r}] += {mean_de:.0f}")
        if s["n"] >= 3:
            # Wording reflects the current tv-detect pipeline (we
            # haven't used comskip since 2026-04-25). Knobs that
            # actually exist: bumper_threshold per channel in
            # .channel-config.json, logo template quality, more
            # user reviews driving the next nightly head training.
            if s["added"] > s["deleted"] * 2:
                suggestions.append(
                    f"auto under-detects ({s['added']} added vs "
                    f"{s['deleted']} deleted) — try lowering "
                    f"bumper_threshold for {slug} in .channel-config.json, "
                    f"or surface more {slug} recordings for review so "
                    f"the next head-training learns better")
            elif s["deleted"] > s["added"] * 2:
                suggestions.append(
                    f"auto over-detects ({s['deleted']} deleted vs "
                    f"{s['added']} added) — likely logo template issue "
                    f"(washout, wrong bbox), or raise bumper_threshold "
                    f"for {slug} to reject ambiguous matches")
        out[slug] = {
            "n": s["n"],
            "matched": s["matched"],
            "mean_dstart_s": round(mean_ds, 1),
            "mean_dend_s": round(mean_de, 1),
            "added": s["added"],
            "deleted": s["deleted"],
            "suggestions": suggestions,
            "sample_uuids": s["sample_uuids"],
        }
    return out


_feedback_cache = {"data": None, "computed_at": 0}
_feedback_lock = threading.Lock()
DETECTION_LEARNING_FILE = HLS_DIR / ".detection_learning.json"
DETECTION_LEARNING_BY_SHOW_FILE = HLS_DIR / ".detection_learning_by_show.json"


def _compute_feedback_stats_by_show():
    """Same shape as _compute_feedback_stats but keyed by show_title
    instead of channel slug. Per-show drift can differ a lot from the
    channel mean — e.g. RTL Spielfilm has +60 s sponsor-tail while RTL
    GZSZ has 0 s; averaging both as 'rtl' over-corrects GZSZ. Returns
    {show: {n, matched, mean_dstart_s, mean_dend_s, sample_uuids}}.
    """
    stats = {}
    if not HLS_DIR.exists():
        return stats
    for d in HLS_DIR.glob("_rec_*"):
        uuid = d.name[5:]
        user_p = d / "ads_user.json"
        auto_p = d / "ads.json"
        if not user_p.is_file():
            continue
        try:
            user_raw = json.loads(user_p.read_text())
            auto_ads = json.loads(auto_p.read_text()) if auto_p.is_file() else []
        except Exception:
            continue
        if isinstance(user_raw, list):
            user_ads = user_raw
        elif isinstance(user_raw, dict):
            user_ads = user_raw.get("ads") or []
        else:
            user_ads = []
        show = _show_title_for_rec(d)
        if not show:
            continue
        s = stats.setdefault(show, {
            "n": 0, "dstart_sum": 0.0, "dend_sum": 0.0, "matched": 0,
            "sample_uuids": [],
        })
        s["n"] += 1
        if len(s["sample_uuids"]) < 3:
            s["sample_uuids"].append(uuid)
        used = set()
        for us, ue in user_ads:
            best = None; best_overlap = 0.0
            for i, (as_, ae) in enumerate(auto_ads):
                if i in used:
                    continue
                ov = max(0.0, min(ue, ae) - max(us, as_))
                if ov > best_overlap:
                    best_overlap = ov; best = i
            if best is not None and best_overlap > 5.0:
                used.add(best)
                as_, ae = auto_ads[best]
                s["dstart_sum"] += us - as_
                s["dend_sum"] += ue - ae
                s["matched"] += 1
    out = {}
    for show, s in stats.items():
        if s["matched"] == 0:
            continue
        out[show] = {
            "n": s["n"], "matched": s["matched"],
            "mean_dstart_s": round(s["dstart_sum"] / s["matched"], 1),
            "mean_dend_s":   round(s["dend_sum"]   / s["matched"], 1),
            "sample_uuids":  s["sample_uuids"],
        }
    return out


def _persist_detection_learning_by_show(stats):
    """Same gating as _persist_detection_learning but per-show. Higher
    sample threshold (5 vs 3) since per-show data is sparser — avoid
    flip-flopping a show's correction on noisy 3-episode samples.
    Writes {show: {start_lag, sponsor_duration, sample_n, computed_at}}.
    """
    learned = {}
    for show, s in stats.items():
        if s.get("matched", 0) < LEARNING_MIN_SAMPLES:
            continue
        entry = {}
        ds = s["mean_dstart_s"]
        de = s["mean_dend_s"]
        # Same symmetric handling as the per-channel persist (see
        # _persist_detection_learning docstring).
        if ds <= -8:
            entry["start_lag"] = round(abs(ds), 1)
        elif ds >= 8:
            entry["start_shrink"] = round(ds, 1)
        if de >= 8:
            entry["sponsor_duration"] = round(de, 1)
        elif de <= -8:
            entry["end_shrink"] = round(abs(de), 1)
        if entry:
            entry["sample_n"] = s["matched"]
            entry["computed_at"] = int(time.time())
            learned[show] = entry
    try:
        tmp = DETECTION_LEARNING_BY_SHOW_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(learned, indent=2))
        tmp.replace(DETECTION_LEARNING_BY_SHOW_FILE)
    except Exception as e:
        print(f"[learning persist by-show]: {e}", flush=True)
    return learned
BLOCK_LENGTH_PRIOR_FILE = HLS_DIR / ".block_length_prior.json"
BLOCK_LENGTH_PRIOR_BY_SHOW_FILE = HLS_DIR / ".block_length_prior_by_show.json"


def _show_title_for_rec(rec_dir):
    """Derive a stable show key from the recording's .txt basename
    (e.g. 'Galileo $2026-04-25-1905' → 'Galileo'). Used for per-show
    prior aggregation — finer-grained than per-channel for shows
    with distinct ad patterns (Galileo magazin vs. Simpsons clean
    cuts, both ProSieben)."""
    for p in rec_dir.glob("*.txt"):
        if any(p.name.endswith(s) for s in
               (".logo.txt", ".cskp.txt", ".tvd.txt", ".trained.logo.txt")):
            continue
        title = p.stem
        if " $" in title:
            title = title.split(" $", 1)[0]
        return title.strip()
    return ""


def _compute_block_length_priors_by_show():
    """Same shape as _compute_block_length_priors but grouped by
    show title instead of channel slug. Need ≥5 user-confirmed blocks
    AND σ≥30s. Channels host multiple shows with different ad
    patterns (Galileo vs Die Simpsons on ProSieben), so per-show is
    tighter where data permits, falls back to per-channel otherwise."""
    import statistics
    by_show = {}
    if not HLS_DIR.exists():
        return {}
    for d in HLS_DIR.glob("_rec_*"):
        user = d / "ads_user.json"
        if not user.is_file():
            continue
        try:
            raw = json.loads(user.read_text())
        except Exception:
            continue
        blocks = raw if isinstance(raw, list) else raw.get("ads", []) or []
        show = _show_title_for_rec(d)
        if not show:
            continue
        for s, e in blocks:
            try:
                dur = float(e) - float(s)
            except Exception:
                continue
            if dur > 0:
                by_show.setdefault(show, []).append(dur)
    out = {}
    for show, durs in by_show.items():
        n = len(durs)
        if n < 5:
            continue
        mean = statistics.mean(durs)
        sd = statistics.stdev(durs) if n > 1 else 0.0
        if sd < 30.0:
            continue
        out[show] = {"min_block_s": 60.0,
                     "max_block_s": round(mean + 3.0 * sd, 1),
                     "mean_s": round(mean, 1),
                     "std_s": round(sd, 1),
                     "sample_n": n,
                     "computed_at": int(time.time())}
    return out


def _rec_duration_s(rec_dir):
    """Sum #EXTINF entries from index.m3u8 for total HLS duration.
    Returns 0.0 if the playlist is missing or unparseable. Used by
    fingerprint matching to normalise block positions across episodes
    with different lengths (e.g. an episode running 3 min over)."""
    pl = rec_dir / "index.m3u8"
    if not pl.is_file():
        return 0.0
    dur = 0.0
    try:
        for ln in pl.read_text().splitlines():
            if ln.startswith("#EXTINF:"):
                try: dur += float(ln.split(":", 1)[1].rstrip(","))
                except Exception: pass
    except Exception:
        return 0.0
    return dur


def _block_iou(a, b):
    """Block-IoU between two cutlists (lists of [start,end] pairs).
    Empty/empty = 1.0 (perfect agreement that there are no ads).
    Empty/non-empty = 0.0. Used by per-show IoU snapshot.
    """
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    ts = sorted({t for blk in (list(a) + list(b)) for t in blk})
    inter = unio = 0.0
    for i in range(len(ts) - 1):
        s, e = ts[i], ts[i + 1]
        ina = any(blk[0] <= s < blk[1] for blk in a)
        inb = any(blk[0] <= s < blk[1] for blk in b)
        d = e - s
        if ina and inb: inter += d
        if ina or inb:  unio += d
    return (inter / unio) if unio else 1.0


def _compute_per_show_iou():
    """Walk every _rec_*/ads_user.json + ads.json and compute per-show
    aggregate Block-IoU between user truth and current auto-detect.
    Used by the per-show IoU trend chart on /learning.

    Returns dict { show_title: {"mean_iou": float, "n": int, "ious": [...]} }.

    Auto-source priority:
      1. ads.json if its mtime ≥ head.bin mtime (= post-deploy fresh cache)
      2. .txt cutlist parsed via _rec_parse_comskip when present + non-empty
         (= daemon delivered fresh detect via cutlist-uploaded but no
         /ads call has warmed the cache yet — the bulk-drain case under
         the V1 test-set-only invalidation strategy where most recordings
         never get a /ads call automatically)
      3. Skip the recording entirely (snapshot won't include it)

    Without the .txt fallback, the snapshot would only see ~10 % of
    reviewed recordings after a head deploy under V1 — most have a
    fresh .txt but a stale ads.json and would be silently dropped.
    """
    by_show = {}
    if not HLS_DIR.exists():
        return {}
    head_p = HLS_DIR / ".tvd-models" / "head.bin"
    head_mtime = head_p.stat().st_mtime if head_p.is_file() else 0
    SIDECAR = (".logo.txt", ".trained.logo.txt", ".cskp.txt", ".tvd.txt")
    for d in HLS_DIR.glob("_rec_*"):
        user_p = d / "ads_user.json"
        if not user_p.is_file():
            continue
        auto_p = d / "ads.json"
        auto = None
        if auto_p.is_file() and auto_p.stat().st_mtime >= head_mtime:
            try:
                auto_raw = json.loads(auto_p.read_text())
                auto = (auto_raw if isinstance(auto_raw, list)
                        else (auto_raw.get("ads", []) or []))
            except Exception:
                auto = None
        if auto is None:
            # Fall back to the cutlist .txt — reflects the daemon's
            # latest detect even when /ads hasn't warmed the cache.
            txts = [t for t in d.glob("*.txt")
                    if not any(t.name.endswith(s) for s in SIDECAR)
                    and t.stat().st_size > 50
                    and "FILE PROCESSING COMPLETE" in t.read_text(errors="ignore")[:200]]
            if not txts:
                continue
            auto = _rec_parse_comskip(d) or []
        try:
            user_raw = json.loads(user_p.read_text())
            user = user_raw if isinstance(user_raw, list) else (user_raw.get("ads", []) or [])
        except Exception:
            continue
        show = _show_title_for_rec(d)
        if not show:
            continue
        iou = _block_iou(user, auto)
        agg = by_show.setdefault(show, {"ious": [], "n": 0})
        agg["ious"].append(iou)
        agg["n"] += 1
    for show, agg in by_show.items():
        agg["mean_iou"] = sum(agg["ious"]) / len(agg["ious"])
    return by_show


def _aggregate_episodes_by_show():
    """Walk every _rec_*/ads_user.json (excluding fingerprint-auto-
    confirmed ones to avoid circular reinforcement), return a dict
    {show: [(rec_dir_name, merged_blocks), ...]}. Each episode's
    blocks are the smart-merge of user-edits + non-deleted auto.
    Empty (0-block) episodes are excluded — they're valid 'no ads'
    reviews but contribute no structural signal.

    Factored out of _compute_show_fingerprints so leave-one-out
    validation can rebuild fingerprints excluding individual episodes."""
    by_show = {}
    if not HLS_DIR.exists():
        return {}
    for d in HLS_DIR.glob("_rec_*"):
        user = d / "ads_user.json"
        if not user.is_file():
            continue
        try:
            raw = json.loads(user.read_text())
        except Exception:
            continue
        if isinstance(raw, dict) and raw.get("auto_confirmed_via_fingerprint"):
            continue
        blocks = raw if isinstance(raw, list) else (raw.get("ads", []) or [])
        deleted = [] if isinstance(raw, list) else (raw.get("deleted") or [])
        try:
            auto_p = d / "ads.json"
            auto_blocks = (json.loads(auto_p.read_text())
                           if auto_p.is_file() else [])
        except Exception:
            auto_blocks = []
        def _ov(a, b): return a[0] < b[1] and b[0] < a[1]
        kept_auto = [a for a in auto_blocks
                     if not any(_ov(a, x) for x in blocks)
                     and not any(_ov(a, dd) for dd in deleted)]
        merged = sorted(blocks + kept_auto, key=lambda b: b[0])
        if not merged:
            continue
        show = _show_title_for_rec(d)
        if not show:
            continue
        by_show.setdefault(show, []).append((d.name, merged))
    return by_show


def _build_fingerprint(episode_blocks_list, min_recs=2,
                        count_consensus=0.66, pos_tolerance_s=90.0):
    """Build a single fingerprint dict from a list of [(start,end),...]
    block lists. Returns None if not enough recs or no count consensus."""
    import statistics
    from collections import Counter
    if len(episode_blocks_list) < min_recs:
        return None
    counts = Counter(len(eb) for eb in episode_blocks_list)
    consensus_count, consensus_n = counts.most_common(1)[0]
    if consensus_count == 0:
        return None
    if consensus_n / len(episode_blocks_list) < count_consensus:
        return None
    consensus_eps = [eb for eb in episode_blocks_list
                     if len(eb) == consensus_count]
    block_stats = []
    for i in range(consensus_count):
        starts = sorted(float(eb[i][0]) for eb in consensus_eps)
        ends = sorted(float(eb[i][1]) for eb in consensus_eps)
        n = len(starts)
        def pct(arr, p):
            idx = max(0, min(n - 1, int(p / 100.0 * (n - 1))))
            return arr[idx]
        block_stats.append({
            "start_s": round(statistics.median(starts), 1),
            "end_s": round(statistics.median(ends), 1),
            "start_p10": round(pct(starts, 10), 1),
            "start_p90": round(pct(starts, 90), 1),
            "end_p10": round(pct(ends, 10), 1),
            "end_p90": round(pct(ends, 90), 1),
        })
    return {
        "n_recs": len(consensus_eps),
        "block_count": consensus_count,
        "blocks": block_stats,
        "tolerance_s": pos_tolerance_s,
        "computed_at": int(time.time()),
    }


def _compute_show_fingerprints(min_recs=2, count_consensus=0.66,
                                pos_tolerance_s=90.0):
    """Build a per-show structural fingerprint of ad-block layout from
    user-confirmed episodes. Recurring shows (Unter uns, Galileo, GZSZ)
    have very stable structure week-to-week — same number of blocks,
    similar positions relative to show start. From n≥min_recs episodes
    where ≥count_consensus fraction agree on block count, we extract
    median block start/end times.

    Output: {show_title: {n_recs, block_count, blocks: [{start_s,
    end_s, start_p10, start_p90, end_p10, end_p90}, ...]}}.

    Used by the auto-confirm pass (api_learning_fingerprint_scan):
    a new auto-detected ads.json that matches its show's fingerprint
    within tolerance can be promoted to user-quality without manual
    review — same effect as the user clicking ✓ Geprüft."""
    by_show = _aggregate_episodes_by_show()  # {show: [(rec_dir, blocks)]}
    out = {}
    for show, eps in by_show.items():
        fp = _build_fingerprint([b for _, b in eps],
                                  min_recs=min_recs,
                                  count_consensus=count_consensus,
                                  pos_tolerance_s=pos_tolerance_s)
        if fp is None:
            continue
        out[show] = {
            "n_recs": fp["n_recs"],
            "block_count": fp["block_count"],
            "blocks": fp["blocks"],
            "tolerance_s": fp["tolerance_s"],
            "computed_at": fp["computed_at"],
        }
    return out


def _check_fingerprint_match(fingerprint, auto_blocks, tolerance_s=None):
    """Test whether an auto-detected ads list matches a show's
    fingerprint within tolerance. Returns (matched: bool, reason: str).

    Match criteria:
      1. Same block count as the fingerprint consensus
      2. Each block's start AND end within ±tolerance_s of the
         fingerprint's median for that block index
    Both checks must pass — partial matches are NOT auto-confirmed
    (better to flag for review than silently mislabel)."""
    if not fingerprint or not auto_blocks:
        return False, "no fingerprint or no auto blocks"
    if tolerance_s is None:
        tolerance_s = fingerprint.get("tolerance_s", 60.0)
    if len(auto_blocks) != fingerprint["block_count"]:
        return False, (f"block count {len(auto_blocks)} != "
                       f"fingerprint {fingerprint['block_count']}")
    sorted_auto = sorted(auto_blocks, key=lambda b: float(b[0]))
    for i, (s, e) in enumerate(sorted_auto):
        fb = fingerprint["blocks"][i]
        ds = abs(float(s) - fb["start_s"])
        de = abs(float(e) - fb["end_s"])
        if ds > tolerance_s or de > tolerance_s:
            return False, (f"block {i+1}: Δstart={ds:.0f}s "
                           f"Δend={de:.0f}s > tol {tolerance_s:.0f}s")
    return True, "ok"


def _compute_block_length_priors():
    """Walk every _rec_<uuid>/ads_user.json, group block durations by
    channel slug, fit a per-channel min/max range that tv-detect
    consumes via --min-block-sec / --max-block-sec. Channels with
    too few samples or a degenerate (zero) std fall back to the
    library defaults (60-900 s) — the prior only kicks in when we
    have enough data to trust it.

    Range formula: [60, mean + 3σ]. We deliberately DON'T tighten
    the lower bound from the prior — channels mix long-format shows
    (RTL Wetzel: 6-10 min ads) with short-format (RTL Unter uns:
    3-5 min ads) under the same slug. A per-channel mean-2σ floor
    would reject the short-format block as spurious. The library's
    MinBlockS=60 already filters single-frame noise, so the prior
    only needs to clip suprious-LONG blocks.

    Quality gate: n_samples ≥ 5 AND σ ≥ 30 s. Below either, the
    sample is too thin to fit confidently."""
    import statistics
    by_slug = {}
    if not HLS_DIR.exists():
        return {}
    for d in HLS_DIR.glob("_rec_*"):
        uuid = d.name[5:]
        user = d / "ads_user.json"
        if not user.is_file():
            continue
        try:
            raw = json.loads(user.read_text())
        except Exception:
            continue
        blocks = raw if isinstance(raw, list) else raw.get("ads", []) or []
        slug = _rec_channel_slug(uuid) or ""
        if not slug:
            continue
        for s, e in blocks:
            try:
                dur = float(e) - float(s)
            except Exception:
                continue
            if dur > 0:
                by_slug.setdefault(slug, []).append(dur)
    out = {}
    for slug, durs in by_slug.items():
        n = len(durs)
        if n < 5:
            continue
        mean = statistics.mean(durs)
        sd = statistics.stdev(durs) if n > 1 else 0.0
        if sd < 30.0:
            # Too tight — refuse to fit. Coincidence rather than signal.
            continue
        # MIN_BLOCK_S stays at library default 60 — see docstring.
        lo = 60.0
        hi = mean + 3.0 * sd
        out[slug] = {
            "min_block_s": round(lo, 1),
            "max_block_s": round(hi, 1),
            "mean_s": round(mean, 1),
            "std_s": round(sd, 1),
            "sample_n": n,
            "computed_at": int(time.time()),
        }
    return out
LEARNING_MIN_SAMPLES = 5  # auto-apply only with ≥N edited recordings


def _persist_detection_learning(stats):
    """Write the auto-applied subset of feedback to a JSON the live
    scanner reads on every scan. Only entries with enough samples and
    a meaningful drift get persisted — conservative threshold to
    avoid flip-flopping the live constants on early data.

    Symmetric drift handling (signed):
      mean_dstart ≤ -8 → start_lag    = abs(ds)  (auto too LATE  → pull START earlier)
      mean_dstart ≥ +8 → start_shrink = ds       (auto too EARLY → push START later)
      mean_dend   ≥ +8 → sponsor_duration = de   (auto too EARLY → push END later)
      mean_dend   ≤ -8 → end_shrink   = abs(de)  (auto too LATE  → pull END earlier)
    detect-config combines them: start_extend_s = start_lag - start_shrink
    """
    learned = {}
    for slug, s in stats.items():
        if s.get("matched", 0) < LEARNING_MIN_SAMPLES:
            continue
        entry = {}
        ds = s["mean_dstart_s"]
        de = s["mean_dend_s"]
        if ds <= -8:
            entry["start_lag"] = round(abs(ds), 1)
        elif ds >= 8:
            entry["start_shrink"] = round(ds, 1)
        if de >= 8:
            entry["sponsor_duration"] = round(de, 1)
        elif de <= -8:
            entry["end_shrink"] = round(abs(de), 1)
        if entry:
            entry["sample_n"] = s["matched"]
            entry["computed_at"] = int(time.time())
            learned[slug] = entry
    try:
        tmp = DETECTION_LEARNING_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(learned, indent=2))
        tmp.replace(DETECTION_LEARNING_FILE)
    except Exception as e:
        print(f"[learning persist]: {e}", flush=True)
    return learned


def _get_feedback_stats(max_age_s=300):
    """Cached wrapper — recompute at most every 5 min. Side effect:
    refreshes the persisted learning file so the live scanner can
    pick up the latest tuning per channel."""
    with _feedback_lock:
        if _feedback_cache["data"] is None \
                or time.time() - _feedback_cache["computed_at"] > max_age_s:
            try:
                stats = _compute_feedback_stats()
                _feedback_cache["data"] = stats
                _feedback_cache["computed_at"] = time.time()
                applied = _persist_detection_learning(stats)
                # Per-show drift learning runs in parallel — same shape,
                # different keying. Detect-config endpoint prefers the
                # per-show entry over the per-channel one when both
                # exist (analog to block-length priors).
                try:
                    show_stats = _compute_feedback_stats_by_show()
                    _persist_detection_learning_by_show(show_stats)
                except Exception as e:
                    print(f"[learning by-show] {e}", flush=True)
                # Block-length priors live in their own file so the
                # consumer side (tv-live-detect.py + daemon-spawned
                # tv-detect runs) can read just the channels-with-
                # enough-samples and not misinterpret detection_learning
                # entries.
                try:
                    by_show = _compute_block_length_priors_by_show()
                    tmp = BLOCK_LENGTH_PRIOR_BY_SHOW_FILE.with_suffix(".tmp")
                    tmp.write_text(json.dumps(by_show, indent=2))
                    tmp.replace(BLOCK_LENGTH_PRIOR_BY_SHOW_FILE)
                    priors = _compute_block_length_priors()
                    tmp = BLOCK_LENGTH_PRIOR_FILE.with_suffix(".tmp")
                    tmp.write_text(json.dumps(priors, indent=2))
                    tmp.replace(BLOCK_LENGTH_PRIOR_FILE)
                except Exception as e:
                    print(f"[block-prior write] {e}", flush=True)
                # Tag each stats entry with whether it was applied
                for slug, s in stats.items():
                    s["applied"] = slug in applied
                    if slug in applied:
                        s["applied_values"] = {
                            k: v for k, v in applied[slug].items()
                            if k in ("start_lag", "sponsor_duration")
                        }
            except Exception as e:
                print(f"[feedback-stats] {e}", flush=True)
                if _feedback_cache["data"] is None:
                    _feedback_cache["data"] = {}
        return _feedback_cache["data"]


def _satip_stream_health():
    """SAT>IP / FritzBox stream health — detects the case where
    DVB-C signal/SNR are fine but the actual TS stream rate is far
    below normal (= classic FritzBox SAT>IP RTSP stuck-session bug
    where signal looks good but no video data flows).

    Uses running-average (total_in / runtime) rather than the
    instantaneous bps tvh exposes — instantaneous fluctuates wildly
    on SD streams (= still-frames during ad breaks drop to <0.5 Mbps
    momentarily even when the recording is healthy at 2-3 Mbps avg).
    Running-avg only crosses the threshold when the stream is genuinely
    starved.

    First 30 s of a sub are noisy (= early packets, mux re-tune); we
    skip the threshold check and report status="ok" until then.

    Returns dict with:
      - active_subs: list of {title, channel, bps_avg, status}
      - tuner_bps_max: max bps across all tuners
      - status: "ok" | "degraded" | "broken" | "idle"

    "broken" = active DVR sub avg < 100 Kbps over runtime (~1% normal).
    "degraded" = avg 100-500 Kbps (partial stream).
    "ok" = avg > 500 Kbps, OR sub younger than 30 s, OR no active subs.
    """
    BROKEN_BPS = 100_000
    DEGRADED_BPS = 500_000
    SETTLE_S = 30
    now = time.time()
    out = {"active_subs": [], "tuner_bps_max": 0, "status": "idle"}
    try:
        subs = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/status/subscriptions", timeout=2).read())
        worst = "ok"
        for e in subs.get("entries", []):
            if not e.get("title", "").startswith("DVR:"):
                continue
            total_in = int(e.get("total_in", 0) or 0)
            start_ts = int(e.get("start", 0) or 0)
            runtime_s = max(1, now - start_ts) if start_ts else 1
            bps_avg = int(total_in * 8 / runtime_s)
            # Skip threshold check during sub settle window
            if runtime_s < SETTLE_S:
                status = "ok"
            elif bps_avg < BROKEN_BPS:
                status = "broken"
            elif bps_avg < DEGRADED_BPS:
                status = "degraded"
            else:
                status = "ok"
            out["active_subs"].append({
                "title": e.get("title", "?"),
                "channel": e.get("channel", "?"),
                "bps_avg": bps_avg,
                "status": status,
            })
            if status == "broken" or (status == "degraded" and worst != "broken"):
                worst = status
        if out["active_subs"]:
            out["status"] = worst
    except Exception:
        pass
    # Tuner bandwidth: pull from tv-receiver /healthz (= total_bytes / age
    # gives a rough running average per slot). Used downstream by the
    # health UI to show "is RTL streaming at expected rate".
    try:
        hz = json.loads(urllib.request.urlopen(
            f"{TV_RECEIVER_BASE}/healthz", timeout=2).read())
        bps_per_slot = []
        for s in hz.get("slots", []):
            age = s.get("age_seconds", 0)
            if age > 1 and s.get("total_bytes", 0) > 0:
                bps_per_slot.append(int(s["total_bytes"] * 8 / age))
        out["tuner_bps_max"] = max(bps_per_slot, default=0)
    except Exception:
        pass
    return out


@app.route("/healthz")
def healthz():
    """Lightweight liveness/readiness probe — mirrors tv-receiver's
    `/healthz` shape (`{"ok": bool, ...}`, 200 or 503). Deliberately fast:
    no SMB reads (unlike `/api/health`, which builds the dashboard). The
    only IO is a 2 s probe of the tv-receiver backend so the result doubles
    as a readiness signal. 200 + ok:true when this gateway serves AND the
    backend is reachable; 503 + ok:false if the backend can't be reached."""
    backend_ok = False
    slots = active = None
    try:
        data = json.loads(urllib.request.urlopen(
            f"{TV_RECEIVER_BASE}/healthz", timeout=2).read())
        backend_ok = bool(data.get("ok", True))
        s = data.get("slots", []) or []
        slots = len(s)
        active = sum(1 for x in s if x.get("consumers", 0) > 0)
    except Exception:
        backend_ok = False
    resp = {
        "ok": backend_ok,
        "service": "hls-gateway",
        "backend": "tv-receiver",
        "backend_ok": backend_ok,
        "tuner_slots": slots,
        "active_slots": active,
    }
    return Response(json.dumps(resp), status=200 if backend_ok else 503,
                    mimetype="application/json")


@app.route("/api/health")
def api_health():
    """Aggregate health snapshot for the dashboard. Reads heartbeats
    from the SMB share for the Mac scanners and probes per-channel
    state from in-memory caches + the live-ads JSON."""
    now = time.time()
    # Mac-side scanners drop heartbeat files on the SMB share. None
    # = file missing entirely, age in seconds = how long since last
    # touch. Stale > 120 s ≈ "scanner died".
    mac_live_age = _hb_age(".mac-live-comskip-alive")
    mac_rec_age = _hb_age(".mac-comskip-alive")
    # Per-channel scan freshness from .live_ads.json.
    chans = []
    try:
        if LIVE_ADS_FILE.exists():
            d = json.loads(LIVE_ADS_FILE.read_text())
            for slug, v in d.items():
                gen = v.get("generated", 0)
                q = v.get("quality") or {}
                chans.append({
                    "slug": slug,
                    "ad_count": len(v.get("ads", [])),
                    "scan_age_s": int(now - gen) if gen else None,
                    "latest_seg": v.get("latest_seg"),
                    "quality_score": q.get("score"),
                    "quality": q,
                })
    except Exception:
        pass
    chans.sort(key=lambda x: x["slug"])
    # System metrics (subset of what api_warm_status returns).
    cpu_temp_c = None
    try:
        cpu_temp_c = round(int(Path(
            "/sys/class/thermal/thermal_zone0/temp").read_text()) / 1000, 1)
    except Exception:
        pass
    mem_avail_mb = mem_total_mb = None
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                mem_total_mb = int(line.split()[1]) // 1024
            elif line.startswith("MemAvailable:"):
                mem_avail_mb = int(line.split()[1]) // 1024
    except Exception:
        pass
    disk_free_gb = disk_total_gb = None
    try:
        import shutil as _sh
        du = _sh.disk_usage(HLS_DIR)
        disk_free_gb = round(du.free / (1024**3), 1)
        disk_total_gb = round(du.total / (1024**3), 1)
    except Exception:
        pass
    load1 = None
    try:
        load1 = round(os.getloadavg()[0], 2)
    except Exception:
        pass
    # Active warm streams.
    with active_lock:
        warm = sorted([s for s in channels.keys()])
    # Recording counts.
    recs_completed = recs_scheduled = recs_watched = 0
    try:
        d = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=500", timeout=5).read())
        for e in d.get("entries", []):
            if "Completed" in (e.get("status") or ""):
                recs_completed += 1
                if (e.get("playcount") or 0) > 0:
                    recs_watched += 1
            elif "Scheduled" in (e.get("status") or "") \
                    or e.get("sched_status") in ("recording",
                                                   "scheduled"):
                recs_scheduled += 1
    except Exception:
        pass
    feedback = _get_feedback_stats()
    for c in chans:
        if c["slug"] in feedback:
            c["feedback"] = feedback[c["slug"]]
    return _cors(Response(json.dumps({
        "now": int(now),
        "learning_min_samples": LEARNING_MIN_SAMPLES,
        "mac_live_scanner_age_s": mac_live_age,
        "mac_rec_scanner_age_s": mac_rec_age,
        "channels": chans,
        "warm": warm,
        "always_warm": sorted(ALWAYS_WARM),
        "cpu_temp_c": cpu_temp_c,
        "mem_avail_mb": mem_avail_mb,
        "mem_total_mb": mem_total_mb,
        "disk_free_gb": disk_free_gb,
        "disk_total_gb": disk_total_gb,
        "load1": load1,
        "recs_completed": recs_completed,
        "recs_scheduled": recs_scheduled,
        "recs_watched": recs_watched,
        "satip_stream": _satip_stream_health(),
        "stale_channels": _stale_channels(),
        "dvr_health": _dvr_health(),
    }), mimetype="application/json"))


def _dvr_health():
    """Phantom + error stats for finished DVR entries in the last 7 days.

    "Phantom" = >5 min scheduled but final filesize < 5 MB/min (= the
    sub allocated but the stream never actually delivered TS packets,
    typically a stuck IPTV path on RTL/COMEDY/etc.). "Errors" = tvh's
    `errors` counter > 0 on the entry (= TS continuity glitches; not
    fatal but indicates packet-loss).

    Used by the /health "DVR Health" tile so problems stay visible
    instead of needing to manually scan grid_finished after each
    night."""
    try:
        d = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid_finished?limit=300",
            timeout=5).read())
    except Exception:
        return {"err": "tvh unreachable"}
    cutoff = time.time() - 7 * 86400
    total = with_errors = phantoms = 0
    phantom_titles = []
    for e in d.get("entries", []):
        if e.get("start", 0) < cutoff:
            continue
        total += 1
        dur_min = (e.get("stop", 0) - e.get("start", 0)) / 60
        size_mb = e.get("filesize", 0) / 1024 / 1024
        rate = size_mb / max(0.1, dur_min)
        errs = e.get("errors", 0)
        # "with errors" = ones we actually want to look at: either
        # many continuity glitches (= systematic packet-loss) or any
        # errors combined with a below-half-normal bitrate (= the
        # error correlates with real corruption). A single error in
        # an otherwise full-bitrate recording is just a brief
        # signal blip — visible 0.04 s glitch, not worth flagging.
        if errs > 5 or (errs > 0 and rate < 10):
            with_errors += 1
        if dur_min > 5 and rate < 5:
            phantoms += 1
            phantom_titles.append(e.get("disp_title", "?"))
    return {
        "total_7d": total,
        "with_errors_7d": with_errors,
        "phantoms_7d": phantoms,
        "phantom_titles": phantom_titles[:5],
    }


def _stale_channels():
    """Channels whose attached services are all disabled (= Vodafone
    line-up shifted the channel to a different mux, tvh marked the
    old service auto-disabled, but no replacement was wired up). These
    record as 5-10 MB phantoms because the channel still tunes the
    old mux but the new SDT no longer carries the sid.

    The weekly /home/simon/dvbc_rescan_remap.py cron self-heals these,
    but the tile lets us see the gap between Vodafone's shift and
    the next Sunday.

    Post-tvh removal (2026-05-27): this check relied on tvh's
    auto-disabled-service detection. tv-receiver's channels.json is
    hand-curated from the FritzBox m3u; Vodafone-shift detection now
    happens at m3u-parse time (a removed channel just disappears).
    We return an empty result so the dashboard tile still renders."""
    return {"stale": [], "n": 0}


HEALTH_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>Health</title>
<style>
/* Default = light theme (tagsüber); dark override via prefers-
   color-scheme matches the rest of /learning, /recordings, /epg. */
:root{
  --bg:#fafafa; --fg:#222; --muted:#666;
  --card:#ffffff; --border:#e1e1e1; --th-bg:#f0f0f0;
  --code-bg:#0001; --link:#0366d6;
  --ok:#27ae60; --warn:#f39c12; --err:#e74c3c;
}
@media (prefers-color-scheme: dark){
  :root{
    --bg:#1a1a1a; --fg:#eee; --muted:#888;
    --card:#252525; --border:#333; --th-bg:#2c2c2c;
    --code-bg:#0006; --link:#5dade2;
  }
}
body{margin:0;padding:16px;background:var(--bg);color:var(--fg);font-family:-apple-system,sans-serif;max-width:900px;margin:0 auto}
h1{margin:0 0 16px;font-size:1.4em}
h2{margin:20px 0 10px;font-size:1.1em;color:var(--muted);text-transform:uppercase;letter-spacing:.05em}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}
.card{background:var(--card);border:1px solid var(--border);border-radius:8px;padding:12px}
.lbl{color:var(--muted);font-size:.8em;text-transform:uppercase}
.val{font-size:1.4em;font-weight:600;margin-top:4px}
.ok{color:var(--ok)} .warn{color:var(--warn)} .err{color:var(--err)}
table{width:100%;border-collapse:collapse;background:var(--card);border-radius:8px;overflow:hidden;margin-top:8px}
th,td{padding:8px 12px;text-align:left;border-bottom:1px solid var(--border);font-size:.95em}
th{background:var(--th-bg);color:var(--muted);font-weight:500;text-transform:uppercase;font-size:.75em}
tr:last-child td{border:0}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:baseline}
.refresh{color:var(--muted);font-size:.8em}
a{color:var(--link)}
</style></head><body>
<h1>System Health <span class="refresh" id="refresh"></span></h1>
<div id="root">Loading…</div>
<script>
function fmtAge(s){if(s==null)return'<span class="err">offline</span>';
  if(s<60)return s+'s';if(s<3600)return Math.floor(s/60)+'m '+(s%60)+'s';
  return Math.floor(s/3600)+'h '+Math.floor((s%3600)/60)+'m';}
function classifyHb(s){if(s==null)return'err';if(s<120)return'ok';if(s<300)return'warn';return'err';}
function pct(part,whole){return whole>0?Math.round(part/whole*100):0;}
async function load(){
  const d=await fetch('/api/health').then(r=>r.json());
  const cells=[];
  cells.push(card('Mac Live-Scanner','<span class="dot '+classifyHb(d.mac_live_scanner_age_s)+'"></span>'+fmtAge(d.mac_live_scanner_age_s),classifyHb(d.mac_live_scanner_age_s)));
  cells.push(card('Mac Rec-Scanner','<span class="dot '+classifyHb(d.mac_rec_scanner_age_s)+'"></span>'+fmtAge(d.mac_rec_scanner_age_s),classifyHb(d.mac_rec_scanner_age_s)));
  cells.push(card('CPU Temp',(d.cpu_temp_c||'?')+'°C',d.cpu_temp_c>=75?'err':d.cpu_temp_c>=65?'warn':'ok'));
  cells.push(card('Load 1m',d.load1||'?',d.load1>=4?'err':d.load1>=2?'warn':'ok'));
  cells.push(card('Memory free',(d.mem_avail_mb||'?')+'M / '+(d.mem_total_mb||'?')+'M',d.mem_avail_mb<256?'err':d.mem_avail_mb<512?'warn':'ok'));
  const dPct=d.disk_free_gb&&d.disk_total_gb?pct(d.disk_free_gb,d.disk_total_gb):0;
  cells.push(card('Disk free',(d.disk_free_gb||'?')+'G / '+(d.disk_total_gb||'?')+'G',dPct<10?'err':dPct<20?'warn':'ok'));
  cells.push(card('Recordings',d.recs_completed+' fertig, '+d.recs_watched+' gesehen, '+d.recs_scheduled+' geplant'));
  cells.push(card('Warm streams',(d.warm.length?d.warm.join(', '):'—')));
  /* SAT>IP stream health: detects FritzBox stuck-RTSP (signal/SNR ok
     but TS data rate ~0). Only meaningful when DVR sub is active. */
  if(d.satip_stream){
    const s=d.satip_stream;
    let txt, cls;
    if(s.status==='idle'){
      txt='kein DVR sub'; cls='';
    } else if(s.status==='broken'){
      txt='⚠ Stream tot'; cls='err';
    } else if(s.status==='degraded'){
      txt='⚠ Stream schwach'; cls='warn';
    } else {
      txt='✓ '+(s.active_subs.length)+' sub'+(s.active_subs.length===1?'':'s'); cls='ok';
    }
    if(s.active_subs && s.active_subs.length){
      const rates=s.active_subs.map(a=>{
        const mbps=(a.bps_avg/1000000).toFixed(1);
        return a.title.replace('DVR: ','')+' '+mbps+'M avg';
      }).join(', ');
      txt+='<br><small style="font-size:.7em;color:var(--muted);font-weight:400">'+rates+'</small>';
    }
    cells.push(card('SAT>IP Stream',txt,cls));
  }
  /* Stale-channels tile: channels whose attached services are all
     disabled (= Vodafone shifted them to a different mux, awaiting
     next Sunday rescan). Phantoms-in-waiting until the cron
     auto-remaps them. */
  if(d.stale_channels){
    const sc=d.stale_channels;
    let txt, cls;
    if(sc.n===0){
      txt='✓ alle aktiv'; cls='ok';
    } else {
      txt='⚠ '+sc.n+' ohne svc';
      cls='warn';
      const list=sc.stale.slice(0,4).join(', ')+(sc.n>4?' …':'');
      txt+='<br><small style="font-size:.7em;color:var(--muted);font-weight:400">'+list+'</small>';
    }
    cells.push(card('Stale Channels',txt,cls));
  }
  /* DVR health: phantoms (= tuner allocated but stream never landed)
     + recordings-with-errors over the last 7d. Catches the case where
     the IPTV-fallback gets a stuck mux-tune and we end up with 0-byte
     recordings — historically only visible via manual filesize-grep. */
  if(d.dvr_health){
    const dh=d.dvr_health;
    let txt, cls;
    if((dh.phantoms_7d||0)===0 && (dh.with_errors_7d||0)===0){
      txt='✓ alle ok'; cls='ok';
    } else if((dh.phantoms_7d||0)>=2){
      txt='⚠ '+dh.phantoms_7d+' phantoms'; cls='err';
    } else if((dh.phantoms_7d||0)===1){
      txt='⚠ 1 phantom'; cls='warn';
    } else {
      txt='⚠ '+dh.with_errors_7d+' mit errors'; cls='warn';
    }
    txt+='<br><small style="font-size:.7em;color:var(--muted);font-weight:400">letzte 7d von '+(dh.total_7d||0)+' Aufnahmen';
    if(dh.phantom_titles && dh.phantom_titles.length){
      txt+='<br>'+dh.phantom_titles.slice(0,3).map(t=>t.length>22?t.slice(0,22)+'…':t).join(', ');
    }
    txt+='</small>';
    cells.push(card('DVR Health',txt,cls));
  }
  let html='<div class="grid">'+cells.join('')+'</div>';
  html+='<h2>Channel Scanner</h2><table><tr><th>Channel</th><th>Letzter Scan</th><th>Ad-Blöcke</th><th>Quality</th><th>Letzter Lauf</th></tr>';
  for(const c of d.channels){
    const age=c.scan_age_s;
    const cls=age==null?'err':age<900?'ok':age<1800?'warn':'err';
    const q=c.quality||{};
    const sc=c.quality_score;
    const qcls=sc==null?'':sc>=80?'ok':sc>=50?'warn':'err';
    const qcell=sc==null?'—':sc+' '
      +'<small style="color:var(--muted);font-size:.8em">('
      +'L'+q.logo_shifts+' B'+q.bf_shifts+' S'+q.silence_shifts
      +(q.tail_extended?' ⏵':'')
      +(q.very_short_blocks>0?' ⚠'+q.very_short_blocks:'')
      +')</small>';
    const det=q.scan_mode?(q.scan_mode+' · '+q.ads_in_scan+' ads · '+q.scan_dur_s+'s'):'—';
    html+='<tr><td>'+c.slug+'</td>'
      +'<td class="'+cls+'">'+fmtAge(age)+'</td>'
      +'<td>'+c.ad_count+'</td>'
      +'<td class="'+qcls+'">'+qcell+'</td>'
      +'<td><small style="color:var(--muted)">'+det+'</small></td></tr>';
  }
  html+='</table>';
  /* Per-channel learning aggregated from user-edited recordings. */
  const fbRows=[];
  for(const c of d.channels){
    const fb=c.feedback;if(!fb||fb.n===0)continue;
    const status=fb.applied
      ?'<span class="ok">✓ aktiv</span>'
      :(fb.matched>=3?'<small style="color:var(--muted)">Vorschlag</small>':'<small style="color:var(--muted)">zu wenig Daten</small>');
    const sug=fb.suggestions.length
      ?fb.suggestions.map(s=>'<code style="background:var(--code-bg);padding:2px 6px;border-radius:3px">'+s+'</code>').join('<br>')
      :'<small style="color:var(--muted)">—</small>';
    fbRows.push('<tr><td>'+c.slug+'</td>'
      +'<td>'+fb.n+' rec, '+fb.matched+' matched</td>'
      +'<td>Δstart '+fb.mean_dstart_s+'s · Δend '+fb.mean_dend_s+'s</td>'
      +'<td>+'+fb.added+' / -'+fb.deleted+'</td>'
      +'<td>'+status+'<br>'+sug+'</td></tr>');
  }
  if(fbRows.length){
    html+='<h2>User-Feedback Learning</h2>'
      +'<p style="color:var(--muted);font-size:.85em;margin:0 0 8px">'
      +'Vorschläge mit ≥'+(d.learning_min_samples||5)+' editierten Aufnahmen werden automatisch live übernommen (Mac live-comskip liest die Werte pro scan).</p>'
      +'<table>'
      +'<tr><th>Channel</th><th>Sample</th><th>Boundary-Drift</th>'
      +'<th>Add/Del</th><th>Status / Vorschläge</th></tr>'
      +fbRows.join('')+'</table>';
  }
  document.getElementById('root').innerHTML=html;
  document.getElementById('refresh').textContent='aktualisiert '+new Date().toLocaleTimeString('de-DE');
}
function card(lbl,val,cls){return '<div class="card"><div class="lbl">'+lbl+'</div><div class="val '+(cls||'')+'">'+val+'</div></div>';}
load();setInterval(load,5000);
</script></body></html>"""


@app.route("/health")
def health_page():
    return Response(HEALTH_HTML, mimetype="text/html")


SEARCH_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>Suche</title>
<style>
:root{
  --bg:#fafafa; --fg:#222; --muted:#666;
  --card:#ffffff; --border:#e1e1e1; --th-bg:#f0f0f0;
  --code-bg:#0001; --link:#0366d6;
  --hl:#fff3a0; --hl-fg:#222;
}
@media (prefers-color-scheme: dark){
  :root{
    --bg:#1a1a1a; --fg:#eee; --muted:#888;
    --card:#252525; --border:#333; --th-bg:#2c2c2c;
    --code-bg:#0006; --link:#5dade2;
    --hl:#5d4d00; --hl-fg:#fff3a0;
  }
}
body{margin:0;padding:16px;background:var(--bg);color:var(--fg);
  font-family:-apple-system,sans-serif;max-width:900px;margin:0 auto}
h1{margin:0 0 14px;font-size:1.4em}
h1 a{color:var(--muted);text-decoration:none;font-size:.7em;margin-left:8px}
.q-row{display:flex;gap:8px;align-items:center;margin-bottom:12px}
#q{flex:1;padding:10px 12px;font-size:1em;border:1px solid var(--border);
  border-radius:6px;background:var(--card);color:var(--fg)}
#q:focus{outline:none;border-color:var(--link)}
.opts{font-size:.85em;color:var(--muted)}
.opts label{cursor:pointer}
#status{color:var(--muted);font-size:.85em;margin:6px 0 12px;min-height:1.2em}
.rec-group{background:var(--card);border:1px solid var(--border);
  border-radius:8px;padding:10px 14px;margin-bottom:10px}
.rec-head{font-weight:600;margin-bottom:6px}
.rec-head .ch{color:var(--muted);font-weight:400;font-size:.85em;
  margin-left:6px}
.rec-head .date{color:var(--muted);font-weight:400;font-size:.8em;
  float:right}
.hit{padding:4px 0;border-top:1px solid var(--border);font-size:.92em;
  display:flex;gap:10px;align-items:baseline}
.hit:first-of-type{border-top:0}
.hit .t{color:var(--link);text-decoration:none;font-variant-numeric:
  tabular-nums;flex-shrink:0;min-width:64px;font-family:monospace}
.hit .t:hover{text-decoration:underline}
.hit .snip{color:var(--fg);line-height:1.4}
.hit mark{background:var(--hl);color:var(--hl-fg);padding:0 2px;
  border-radius:2px}
.hit.is-ad .t{color:var(--muted)}
.hit.is-ad .snip{color:var(--muted)}
.hit .ad-tag{font-size:.7em;color:#e74c3c;font-weight:600;
  text-transform:uppercase;margin-left:auto;flex-shrink:0}
.empty{color:var(--muted);text-align:center;padding:30px;font-size:.9em}
.help{color:var(--muted);font-size:.8em;margin-top:14px;line-height:1.6}
.help code{background:var(--code-bg);padding:1px 5px;border-radius:3px}
</style></head><body>
<h1>Whisper-Suche <a href="/recordings">← Aufnahmen</a></h1>
<div class="q-row">
  <input id="q" type="search" autofocus
    placeholder="Suche nach gesprochenem Wort…"
    autocapitalize="off" autocorrect="off">
  <span class="opts">
    <label><input type="checkbox" id="incl-ads"> auch Werbung</label>
  </span>
</div>
<div id="status"></div>
<div id="results"></div>
<div class="help">
  Tipp: Mehrere Begriffe = AND. <code>"genaue phrase"</code> in
  Anführungszeichen. Suffix-* für Präfix: <code>kanzler*</code>.
</div>
<script>
const qInp=document.getElementById('q');
const inclAds=document.getElementById('incl-ads');
const statusEl=document.getElementById('status');
const resultsEl=document.getElementById('results');
let timer=null, lastQ='';

function fmtTs(s){
  if(!isFinite(s)||s<0)s=0;
  const m=Math.floor(s/60), ss=Math.floor(s%60);
  const h=Math.floor(m/60);
  return h>0?h+':'+String(m%60).padStart(2,'0')+':'+String(ss).padStart(2,'0')
           :m+':'+String(ss).padStart(2,'0');
}
function fmtDate(s){
  if(!s)return '';
  const d=new Date(s*1000);
  return d.toLocaleDateString('de-DE',{day:'2-digit',month:'2-digit',
    year:'2-digit',hour:'2-digit',minute:'2-digit'});
}
function escAttr(s){return String(s||'').replace(/"/g,'&quot;');}

async function run(){
  const q=qInp.value.trim();
  if(!q){statusEl.textContent='';resultsEl.innerHTML='';lastQ='';return;}
  if(q===lastQ)return;
  lastQ=q;
  statusEl.textContent='Suche…';
  const t0=performance.now();
  let d;
  try{
    const u='/api/search?q='+encodeURIComponent(q)
      +(inclAds.checked?'&include_ads=1':'');
    d=await fetch(u).then(r=>r.json());
  }catch(e){
    statusEl.textContent='Netzwerk-Fehler.';return;
  }
  if(q!==qInp.value.trim())return;  // user typed further, ignore
  const ms=Math.round(performance.now()-t0);
  if(!d.results||d.results.length===0){
    statusEl.textContent=ms+' ms — keine Treffer.';
    resultsEl.innerHTML='<div class="empty">Keine Treffer für '
      +'<code>'+escAttr(q)+'</code></div>';
    return;
  }
  statusEl.textContent=d.n+' Treffer in '+ms+' ms';
  /* Group by uuid, preserve order = newest recording first. */
  const groups=[];
  const byUuid=new Map();
  for(const r of d.results){
    let g=byUuid.get(r.uuid);
    if(!g){g={uuid:r.uuid,title:r.title,channel_slug:r.channel_slug,
             rec_start:r.recording_start_s,hits:[]};
      byUuid.set(r.uuid,g);groups.push(g);}
    g.hits.push(r);
  }
  resultsEl.innerHTML=groups.map(g=>{
    const recUrl='/recording/'+g.uuid;
    const head='<div class="rec-head">'
      +'<a href="'+recUrl+'" style="color:var(--fg);'
      +'text-decoration:none">'+escAttr(g.title||'(unbekannt)')+'</a>'
      +'<span class="ch">'+escAttr(g.channel_slug)+'</span>'
      +'<span class="date">'+fmtDate(g.rec_start)+'</span></div>';
    const hits=g.hits.map(h=>{
      const isAd=h.prob_ad>=0.5;
      const cls='hit'+(isAd?' is-ad':'');
      return '<div class="'+cls+'">'
        +'<a class="t" href="'+recUrl+'?t='+Math.floor(h.t_start)+'">'
        +fmtTs(h.t_start)+'</a>'
        +'<span class="snip">'+h.snippet+'</span>'
        +(isAd?'<span class="ad-tag">Werbung</span>':'')+'</div>';
    }).join('');
    return '<div class="rec-group">'+head+hits+'</div>';
  }).join('');
}

qInp.addEventListener('input',()=>{
  clearTimeout(timer);
  timer=setTimeout(run,300);
});
inclAds.addEventListener('change',()=>{lastQ='';run();});
qInp.addEventListener('keydown',e=>{
  if(e.key==='Enter'){e.preventDefault();clearTimeout(timer);
    lastQ='';run();}
});
/* Pre-fill from ?q= for shareable links. */
try{const p=new URL(window.location.href).searchParams.get('q');
    if(p){qInp.value=p;run();}}catch(e){}
</script></body></html>"""


@app.route("/search")
def search_page():
    return Response(SEARCH_HTML, mimetype="text/html")


def _self_exec(reason):
    # Replace this Python process in place. Child ffmpegs (spawned with
    # start_new_session=True and tracked via PID files) remain our
    # children by PID and are re-adopted by the fresh image.
    print(f"self-exec ({reason})", flush=True)
    try: sys.stdout.flush()
    except Exception: pass
    # Close inherited FDs (listening socket on :8080 above all) so the
    # new image can rebind cleanly. Keep stdin/stdout/stderr (0,1,2).
    try: os.closerange(3, 1024)
    except Exception: pass
    os.execv(sys.executable, [sys.executable, "-u", os.path.abspath(__file__)])


def _sighup_reload(signum, frame):
    _self_exec("SIGHUP")


def _file_watcher_loop():
    # Poll service.py mtime; self-exec when the mount picks up a new
    # version. Avoids `docker restart` on hot-reloads so the ffmpeg
    # buffer survives. A broken scp would crash the fresh Python
    # (container dies, cgroup kill, buffer gone) — so compile-check
    # first and skip the reload if the file doesn't parse.
    path = os.path.abspath(__file__)
    try:
        last = os.path.getmtime(path)
    except OSError:
        return
    while True:
        time.sleep(0.5)
        try:
            mt = os.path.getmtime(path)
        except OSError:
            continue
        if mt == last:
            continue
        # Wait a tick for scp to finish writing.
        time.sleep(1)
        try:
            src = open(path, "rb").read()
            compile(src, path, "exec")
        except SyntaxError as e:
            print(f"hot-reload skipped: {path} has SyntaxError "
                  f"at line {e.lineno}: {e.msg}", flush=True)
            last = mt   # don't spam the log every 2 s; wait for next edit
            continue
        except Exception as e:
            print(f"hot-reload skipped: compile failed: {e}", flush=True)
            last = mt
            continue
        _self_exec(f"{path} changed")


# ─── Bibliothek (= viewer-focused page, separate from /recordings) ───
# Persona-split: /recordings is admin-side (= status filters, prüfbar,
# delete, bulk actions). /bibliothek is consumer-side (= "ich will was
# gucken"): tile grid per show, click → episode list → player. No
# admin clutter, no scheduled/error entries — only completed playable.

def _bib_html_escape(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _bib_rel_time(seconds_ago):
    """Compact German relative time: 'vor 5 min', 'vor 3 h', 'gestern',
    'vor 4 Tagen', 'am 12.04.'. Negative = future, treated as 'jetzt'."""
    if seconds_ago < 60:
        return "jetzt"
    m = seconds_ago // 60
    if m < 60:
        return f"vor {m} min"
    h = m // 60
    if h < 24:
        return f"vor {h} h"
    d = h // 24
    if d == 1:
        return "gestern"
    if d < 14:
        return f"vor {d} Tagen"
    # Beyond 2 weeks: absolute date
    import datetime
    return datetime.datetime.fromtimestamp(time.time() - seconds_ago).strftime("am %d.%m.")


def _bib_bucket_completed():
    """Fetch tvh dvr grid, filter to playable (= sched_status==completed
    AND status==Completed OK / not 'File missing'), bucket into per-show
    groups using the same priority as /recordings: user-groups → autorec
    → orphans-by-(title,channel). Returns dict[group_key, list[entry]]."""
    try:
        data = json.loads(urllib.request.urlopen(
            f"{dvr_base()}/api/dvr/entry/grid?limit=2000&sort=start&dir=DESC",
            timeout=10).read())
    except Exception as e:
        abort(502, f"tvheadend: {e}")
    completed = []
    for e in data.get("entries", []):
        sched = e.get("sched_status", "")
        # Allow both "completed" AND "completedError" — Pi-disk T7
        # dedup removes the original .ts so tvh flips status to
        # "File missing" / completedError, but the HLS-VOD bundle on
        # /mnt/tv/hls/_rec_<uuid>/ stays intact and plays fine via the
        # gateway's recording route. Pre-2026-05-13 filter dropped
        # those, taking the library from ~hundreds to ~21 tiles.
        if sched not in ("completed", "completedError"):
            continue
        # Drop user-aborted (= explicit cancellation, not a phantom).
        if "Aborted by user" in (e.get("status", "") or ""):
            continue
        if not e.get("enabled", True):
            continue
        uuid = e.get("uuid", "")
        has_hls = (HLS_DIR / f"_rec_{uuid}" / "index.m3u8").is_file()
        size = e.get("filesize") or 0
        # Two acceptance paths:
        #   1) tvh's .ts still present and big enough (legacy fast path)
        #   2) tvh says file missing/zero-size BUT HLS-VOD is on disk —
        #      that's a dedup'd-but-playable recording.
        if size >= 50 * 1024 * 1024:
            completed.append(e)
            continue
        if has_hls:
            # Sanity: a phantom remux may have 1-2 segments. Real
            # recordings have many. Require >=20 segments (= ~2min of
            # content) so 5-second phantoms don't sneak back in.
            try:
                n_segs = sum(1 for _ in (HLS_DIR / f"_rec_{uuid}").glob("seg_*.ts"))
            except Exception:
                n_segs = 0
            if n_segs >= 20:
                completed.append(e)

    user_groups = _load_user_groups()
    uuid_to_user_group = {u: name for name, uuids in user_groups.items()
                          for u in uuids}
    by_group = {}
    title_ch_to_ar = {}
    for e in completed:
        u = e.get("uuid", "")
        if u in uuid_to_user_group:
            by_group.setdefault(
                f"usergroup:{uuid_to_user_group[u]}", []).append(e)
            continue
        ar = e.get("autorec") or ""
        if ar:
            by_group.setdefault(ar, []).append(e)
            t = (e.get("disp_title") or "").strip()
            ch = (e.get("channelname") or "").strip()
            if t:
                title_ch_to_ar.setdefault((t, ch), ar)
    orphan = {}
    for e in completed:
        u = e.get("uuid", "")
        if u in uuid_to_user_group: continue
        if e.get("autorec"): continue
        t = (e.get("disp_title") or "").strip()
        ch = (e.get("channelname") or "").strip()
        if not t: continue
        ar = title_ch_to_ar.get((t, ch))
        if ar:
            by_group[ar].append(e)
        else:
            orphan.setdefault((t, ch), []).append(e)
    for (t, ch), eps in orphan.items():
        by_group[f"orphan:{t}|{ch}"] = eps

    # Cross-channel merge: same normalized-title across different
    # channels collapses into ONE bucket. SpongeBob recorded on Nick +
    # COMEDY CENTRAL was 2 tiles before; user wants 1. user-groups
    # opt out (= explicit user-curated bundles, don't conflate). The
    # canonical key is whichever bucket has the most episodes (= most
    # representative); ties go to the lexicographically smallest key
    # so URLs are deterministic across requests.
    by_norm = {}
    for k, eps in by_group.items():
        if k.startswith("usergroup:"):
            continue
        title = _bib_group_title(k, eps)
        norm = _normalize_title(title)
        if not norm: continue
        by_norm.setdefault(norm, []).append(k)
    merged = {k: eps for k, eps in by_group.items()
              if k.startswith("usergroup:")}
    for norm, keys in by_norm.items():
        if len(keys) == 1:
            merged[keys[0]] = by_group[keys[0]]
            continue
        canonical = sorted(keys, key=lambda k: (-len(by_group[k]), k))[0]
        all_eps = []
        for k in keys:
            all_eps.extend(by_group[k])
        merged[canonical] = all_eps
    return merged


def _bib_group_title(key, eps):
    """Display title for a bucket. user-groups encode the name in the
    key; orphans encode title+channel; autorec uses the most recent
    episode's disp_title."""
    if key.startswith("usergroup:"):
        return key[len("usergroup:"):]
    if key.startswith("orphan:"):
        body = key[len("orphan:"):]
        return body.rsplit("|", 1)[0] if "|" in body else body
    latest = max(eps, key=lambda e: e.get("start") or 0)
    return (latest.get("disp_title") or "?").strip()


def _bib_tiles_data():
    """Structured per-show data — same grouping + playability filter +
    movie/series classification as the /bibliothek tile grid, but
    returned as a list of dicts (no HTML). Used by /api/series for
    external apps. /bibliothek-page renderer still owns its own copy
    of the tile-building logic; merging the two is a future cleanup."""
    by_group = _bib_bucket_completed()
    tiles = []
    for key, eps in by_group.items():
        if not eps:
            continue
        latest = max(eps, key=lambda e: e.get("start") or 0)
        title = _bib_group_title(key, eps)
        ch_name = latest.get("channelname") or ""
        ch_slug = slugify(ch_name)
        channel_icon = _channel_logo_url(
            ch_slug, latest.get("channel_icon", ""),
            ext_priority=("png", "svg", "jpg"))
        with _epg_meta_lock:
            meta = _epg_meta.get(_normalize_title(title)) or {}
            if (not meta.get("poster")) and key.startswith("usergroup:"):
                for ep in eps:
                    ep_t = (ep.get("disp_title") or "").strip()
                    if not ep_t:
                        continue
                    ep_meta = _epg_meta.get(_normalize_title(ep_t)) or {}
                    if ep_meta.get("poster") or ep_meta.get("tmdb_poster"):
                        meta = ep_meta
                        break
        rating = meta.get("rating")
        kind_meta = meta.get("kind")
        latest_dur_s = (latest.get("stop") or 0) - (latest.get("start") or 0)
        latest_dur_min = max(0, latest_dur_s // 60)
        is_epg_fallback = bool(re.match(
            r"^[A-ZÄÖÜ][\w\s.&-]+ \(\d{1,2}:\d{2}\)\s*$", title))
        if key.startswith("usergroup:"):
            is_movie = True
        elif latest.get("autorec"):
            is_movie = False
        elif is_epg_fallback:
            is_movie = False
        elif len(eps) > 1 and (" - " in (latest.get("disp_title") or "")
                               or " — " in (latest.get("disp_title") or "")):
            is_movie = False
        elif kind_meta == "movie" and latest_dur_min >= 80:
            is_movie = True
        elif kind_meta == "tv":
            is_movie = False
        elif kind_meta is None and latest_dur_min >= 80:
            is_movie = True
        else:
            is_movie = False
        if is_movie and meta.get("tmdb_poster"):
            poster = meta["tmdb_poster"]
        else:
            poster = meta.get("poster")
        if not poster:
            latest_uuid = latest.get("uuid", "")
            if latest_uuid:
                thumb_path = (HLS_DIR / f"_rec_{latest_uuid}"
                              / "thumbs" / "t00010.jpg")
                if thumb_path.is_file():
                    poster = (f"{HOST_URL.rstrip('/')}"
                              f"/recording/{latest_uuid}/thumbs/t00010.jpg")
        if poster and not poster.startswith(("http://", "https://")):
            poster = f"{HOST_URL.rstrip('/')}/{poster.lstrip('/')}"
        ch_names = sorted({(e.get("channelname") or "").strip()
                           for e in eps if e.get("channelname")})
        ep_sorted = sorted(eps, key=lambda e: -(e.get("start") or 0))
        ep_uuids = [e.get("uuid", "") for e in ep_sorted if e.get("uuid")]
        tiles.append({
            "key": key,
            "title": title,
            "channel": ch_name,
            "channels": ch_names,
            "channel_icon": channel_icon,
            "poster_url": poster or "",
            "rating": rating,
            "kind": "movie" if is_movie else "series",
            "episode_count": len(eps),
            "latest_start": latest.get("start") or 0,
            "episodes": ep_uuids,
        })
    return tiles


@app.route("/api/series")
def api_series():
    """Per-show aggregated listing — same grouping logic as the
    /bibliothek tile grid (user-groups > autorec > orphan-by-
    (title,channel) > cross-channel merge) and same playability filter
    (sched_status completed/completedError + size/HLS-VOD check).

    Each entry: title, cover, episode count + uuid list (newest first),
    channel(s), TMDB rating, kind=movie|series. Episode uuids resolve
    against /api/recordings/<uuid> for full episode data.

    Query params:
      ?kind=movies|series   filter by classification
      ?sort=date|title      sort key, default = date (latest_start DESC)
      ?since=<unix_ts>      delta-sync: only series with latest_start
                            > since (= got a new episode since cursor).
                            Same deletion caveat as /api/recordings.

    Response: {series: [...], n: N, server_time: <unix_ts>} — save
    `server_time` and pass as `?since=` next call.
    """
    kind_q = (request.args.get("kind") or "").lower()
    sort_q = (request.args.get("sort") or "date").lower()
    try:
        since = int(request.args.get("since") or 0)
    except Exception:
        since = 0
    tiles = _bib_tiles_data()
    if kind_q == "movies":
        tiles = [t for t in tiles if t["kind"] == "movie"]
    elif kind_q == "series":
        tiles = [t for t in tiles if t["kind"] == "series"]
    if since:
        tiles = [t for t in tiles if t["latest_start"] > since]
    if sort_q == "title":
        tiles.sort(key=lambda t: t["title"].lower())
    else:
        tiles.sort(key=lambda t: -t["latest_start"])
    return _cors(Response(
        json.dumps({"series": tiles, "n": len(tiles),
                    "server_time": int(time.time())}),
        mimetype="application/json"))


@app.route("/bibliothek")
def bibliothek_page():
    """Watch-focused tile grid: per show, latest episode + count.
    Persona-split from /recordings (= admin/management). Filtered to
    completed playable recordings; click tile → episode list →
    existing /recording/<uuid> player.

    Query args:
      ch=A,B,C    — filter to these channels (channel_name slugs).
                    Empty = all channels.
      sort=date|title — sort key. Default = date (newest first).
                        title = alphabetical."""
    by_group = _bib_bucket_completed()
    now = int(time.time())
    all_tiles = []
    for key, eps in by_group.items():
        if not eps: continue
        latest = max(eps, key=lambda e: e.get("start") or 0)
        title = _bib_group_title(key, eps)
        ch_name = latest.get("channelname") or ""
        ch_slug = slugify(ch_name)
        ch_logo = _channel_logo_url(ch_slug, latest.get("channel_icon", ""))
        with _epg_meta_lock:
            meta = _epg_meta.get(_normalize_title(title)) or {}
            # User-groups bundle individually-named entries under a
            # custom group title (= "Asterix und Obelix" containing
            # "Asterix erobert Rom" / "Asterix und die Wikinger" /
            # ...). The group key has no TMDB hit since TMDB doesn't
            # know that user-defined name; fall back to the first
            # member with a usable poster so the tile isn't a blank
            # dark card.
            if (not meta.get("poster")) and key.startswith("usergroup:"):
                for ep in eps:
                    ep_t = (ep.get("disp_title") or "").strip()
                    if not ep_t: continue
                    ep_meta = _epg_meta.get(_normalize_title(ep_t)) or {}
                    if ep_meta.get("poster") or ep_meta.get("tmdb_poster"):
                        meta = ep_meta
                        break
        rating = meta.get("rating")
        # Movie vs series classification — user-explicit signals
        # outrank TMDB because TMDB confuses German TV shows with
        # similarly-named movies. Duration is a final tie-breaker
        # for unknown cases: movies are 80+ min, TV episodes 25-60.
        #   1) user-groups → movie franchise (Rocky 1-5, Asterix-Filme)
        #   2) autorec rule attached → series (= recurring by definition)
        #   3) TMDB explicit kind="movie" → movie
        #   4) TMDB explicit kind="tv" → series
        #   5) duration ≥ 80 min → likely movie (= news blocks ~30 min,
        #      sitcoms ~25 min, dramas ~45 min, talk ~60 min all
        #      under threshold; only films + sport-events span 80+)
        #   6) anything else → series
        kind = meta.get("kind")
        latest_dur_s = (latest.get("stop") or 0) - (latest.get("start") or 0)
        latest_dur_min = max(0, latest_dur_s // 60)
        # EPG-fallback titles like "VOX (06:48)" / "ZDF (19:00)" — tvh
        # generates these when the EPG event has no proper name. They're
        # always news / regional / continuous-feed blocks, never films.
        is_epg_fallback = bool(re.match(r"^[A-ZÄÖÜ][\w\s.&-]+ \(\d{1,2}:\d{2}\)\s*$",
                                          title))
        if key.startswith("usergroup:"):
            is_movie = True
        elif latest.get("autorec"):
            is_movie = False
        elif is_epg_fallback:
            is_movie = False
        elif len(eps) > 1 and (" - " in (latest.get("disp_title") or "")
                                  or " — " in (latest.get("disp_title") or "")):
            # Multi-recording title with " - <subtitle>" is almost
            # always a TV show variant (= "Staying Alive - Stars singen
            # mit Legenden" → Stefan Raab show, NOT the Travolta movie
            # that TMDB matches after subtitle stripping). Movies
            # rebroadcast 2× without subtitle (= "Jungle Cruise") fall
            # through to TMDB-confirms-movie below and stay tagged as
            # films.
            is_movie = False
        elif kind == "movie" and latest_dur_min >= 80:
            # TMDB says movie AND length looks movie-ish. Both signals
            # required because TMDB confuses German short-form TV
            # ("taff" 25 min, "Galileo Stories" 66 min) with similarly-
            # named films.
            is_movie = True
        elif kind == "tv":
            is_movie = False
        elif kind is None and latest_dur_min >= 80:
            # No TMDB hit AND long broadcast → likely film
            is_movie = True
        else:
            is_movie = False
        # Poster selection: films get TMDB's portrait poster (= proper
        # Filmplakat aspect, looks like a film). Series get whatever
        # source won (= fernsehserien landscape banner for German shows,
        # else TMDB portrait, else TVmaze). Films without a TMDB poster
        # fall through to whatever the meta has.
        if is_movie and meta.get("tmdb_poster"):
            poster = meta["tmdb_poster"]
        else:
            poster = meta.get("poster")
        # Thumb-fallback for German shows TMDB+TVmaze don't know
        # ("Lenßen hilft", "hundkatzemaus", "Abenteuer Leben täglich"
        # etc.). Pick a thumb 5 min into the latest episode (= past
        # intros/title-cards, before credits — usually shows content).
        if not poster:
            latest_uuid = latest.get("uuid", "")
            if latest_uuid:
                thumb_path = (HLS_DIR / f"_rec_{latest_uuid}"
                              / "thumbs" / "t00010.jpg")
                if thumb_path.exists():
                    poster = f"/recording/{latest_uuid}/thumbs/t00010.jpg"
        # ch_names: every channel that contributed an episode (= for
        # cross-channel merged buckets like "SpongeBob on Nick +
        # COMEDY CENTRAL"). Used by the channel-chip filter so a tile
        # stays visible whenever any of its channels is selected.
        ch_names = sorted({(e.get("channelname") or "").strip()
                           for e in eps if e.get("channelname")})
        all_tiles.append({
            "key": key, "title": title, "count": len(eps),
            "latest_ts": latest.get("start") or 0,
            "ch_logo": ch_logo, "poster": poster, "rating": rating,
            "ch_name": ch_name, "ch_names": ch_names,
            "is_movie": is_movie,
        })

    # Channel filter: chips at top let user toggle channel inclusion.
    # All channels in the unfiltered set show as chips so a fully-
    # filtered-out channel can be re-enabled. Empty selection = no
    # filter (= show all), matching default.
    raw_ch = (request.args.get("ch") or "").strip()
    selected_ch = {c.strip() for c in raw_ch.split(",") if c.strip()}
    all_channels = sorted({c for t in all_tiles for c in t["ch_names"]})

    if selected_ch:
        # Match if ANY of the tile's channels is selected (= cross-
        # channel merged tile stays visible when only one of its
        # channels is in the chip filter).
        tiles = [t for t in all_tiles
                 if not selected_ch.isdisjoint(t["ch_names"])]
    else:
        tiles = list(all_tiles)

    # Type filter: serien / filme / alle (default).
    type_filter = (request.args.get("type") or "all").lower()
    if type_filter not in ("all", "series", "movies"):
        type_filter = "all"
    n_movies_in_view = sum(1 for t in tiles if t["is_movie"])
    n_series_in_view = len(tiles) - n_movies_in_view
    if type_filter == "series":
        tiles = [t for t in tiles if not t["is_movie"]]
    elif type_filter == "movies":
        tiles = [t for t in tiles if t["is_movie"]]

    sort_key = request.args.get("sort", "date").lower()
    if sort_key == "title":
        tiles.sort(key=lambda t: t["title"].lower())
    else:
        sort_key = "date"
        tiles.sort(key=lambda t: -t["latest_ts"])

    # URL-helpers: each preserves the OTHER active filters so toggling
    # one control doesn't reset the rest. Defaults are omitted from the
    # querystring for clean shareable URLs.
    def _build_qs(*, ch=None, sort=None, type_=None):
        ch = selected_ch if ch is None else ch
        sort = sort_key if sort is None else sort
        type_ = type_filter if type_ is None else type_
        params = []
        if ch:
            params.append("ch=" + urllib.parse.quote(",".join(sorted(ch))))
        if sort and sort != "date":
            params.append(f"sort={sort}")
        if type_ and type_ != "all":
            params.append(f"type={type_}")
        return ("?" + "&".join(params)) if params else ""

    def _chip_url(ch_name):
        new_set = set(selected_ch)
        if ch_name in new_set:
            new_set.discard(ch_name)
        else:
            new_set.add(ch_name)
        return "/bibliothek" + _build_qs(ch=new_set)

    type_url_all    = "/bibliothek" + _build_qs(type_="all")
    type_url_series = "/bibliothek" + _build_qs(type_="series")
    type_url_movies = "/bibliothek" + _build_qs(type_="movies")
    sort_url_date   = "/bibliothek" + _build_qs(sort="date")
    sort_url_title  = "/bibliothek" + _build_qs(sort="title")
    chip_url_all    = "/bibliothek" + _build_qs(ch=set())

    chip_html = []
    chip_html.append(
        f'<a class="chip {"" if selected_ch else "chip-active"}" '
        f'href="{chip_url_all}">Alle</a>')
    for ch in all_channels:
        active = "chip-active" if ch in selected_ch else ""
        chip_html.append(
            f'<a class="chip {active}" href="{_chip_url(ch)}">'
            f'{_bib_html_escape(ch)}</a>')

    # Type filter pills (= series / movies / all). Counts are within
    # the channel-filtered set (= "🎬 Filme (3) on this channel"
    # matters more than total across all channels).
    type_html = (
        f'<a class="sort {"sort-active" if type_filter == "all" else ""}" '
        f'href="{type_url_all}">Alle</a>'
        f'<a class="sort {"sort-active" if type_filter == "series" else ""}" '
        f'href="{type_url_series}">📺 Serien ({n_series_in_view})</a>'
        f'<a class="sort {"sort-active" if type_filter == "movies" else ""}" '
        f'href="{type_url_movies}">🎬 Filme ({n_movies_in_view})</a>')

    sort_html = (
        f'<a class="sort {"sort-active" if sort_key == "date" else ""}" '
        f'href="{sort_url_date}">📅 Neueste</a>'
        f'<a class="sort {"sort-active" if sort_key == "title" else ""}" '
        f'href="{sort_url_title}">🔤 A-Z</a>')

    rows = []
    for t in tiles:
        rel = _bib_rel_time(now - t["latest_ts"])
        if t["poster"]:
            # All tiles render at the same 2:3 portrait aspect for a
            # uniform Netflix-style grid. Landscape sources (fernsehserien
            # 1.875:1 banners, tvinfo 1.775:1, wunschliste 4:1) get the
            # AppleTV blur-bg trick: blurred-zoomed copy fills the portrait
            # rectangle, sharp banner sits centered at native aspect.
            # Portraits (TMDB 2:3) just object-fit:cover the tile.
            is_ls = ("bilder.fernsehserien.de" in t["poster"]
                     or "bilder.wunschliste.de" in t["poster"]
                     or "pictures.tvinfo.net" in t["poster"])
            esc = _bib_html_escape(t["poster"])
            if is_ls:
                img_inner = (
                    f'<div class="tile-bg-blur" '
                    f'style="background-image:url({esc})"></div>'
                    f'<img class="tile-bg-sharp" src="{esc}" '
                    f'alt="" loading="lazy">')
            else:
                img_inner = (f'<img class="tile-poster" src="{esc}" '
                             f'alt="" loading="lazy">')
        elif t["ch_logo"]:
            img_inner = (f'<div class="tile-no-poster">'
                         f'<img src="{_bib_html_escape(t["ch_logo"])}" '
                         f'alt="" loading="lazy"></div>')
        else:
            img_inner = '<div class="tile-no-poster"></div>'
        # Movie badge top-left over poster
        if t["is_movie"]:
            img = f'<div class="tile-img-wrap"><span class="tile-badge">🎬</span>{img_inner}</div>'
        else:
            img = f'<div class="tile-img-wrap">{img_inner}</div>'
        rating_html = (f'<span class="tile-rating">★ {t["rating"]}</span>'
                       if t["rating"] else '')
        # Sub-line differs for movies vs series
        if t["is_movie"] and t["count"] == 1:
            sub = f'Film · {rel}'
        else:
            sub = (f'{t["count"]} '
                   f'{"Folge" if t["count"] == 1 else "Folgen"} · {rel}')
        rows.append(
            f'<a class="tile" href="/bibliothek/{urllib.parse.quote(t["key"], safe="")}">'
            f'{img}'
            f'<div class="tile-meta">'
            f'<div class="tile-title">{_bib_html_escape(t["title"])} {rating_html}</div>'
            f'<div class="tile-sub">{sub}</div>'
            f'</div>'
            f'</a>')
    body = ("\n".join(rows) or
            '<p style="padding:1rem;color:var(--bib-muted)">Keine Treffer.</p>')
    html = f"""<!doctype html>
<html lang="de"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bibliothek ({len(tiles)})</title>
<style>
  /* Color tokens follow the system pattern (= /recordings, /epg). */
  :root {{
    --bib-bg:#f5f6f8;
    --bib-fg:#222;
    --bib-muted:#777;
    --bib-card:#ffffff;
    --bib-card-border:rgba(0,0,0,0.08);
    --bib-topbar-bg:#ffffff;
    --bib-topbar-border:#e5e6e8;
    --bib-chip-bg:#f0f0f3;
    --bib-chip-fg:#444;
    --bib-chip-active-bg:#0066cc;
    --bib-chip-active-fg:#ffffff;
    --bib-sort-active-bg:#222;
    --bib-sort-active-fg:#ffffff;
    --bib-link:#0066cc;
    --bib-sep:#ddd;
    --bib-tile-frame-bg:#1a1d24;
    --bib-poster-placeholder:#ddd;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bib-bg:#121417;
      --bib-fg:#eee;
      --bib-muted:#888;
      --bib-card:#1d2025;
      --bib-card-border:rgba(255,255,255,0.06);
      --bib-topbar-bg:#1a1d24;
      --bib-topbar-border:#2a2e36;
      --bib-chip-bg:#2a2e36;
      --bib-chip-fg:#ccc;
      --bib-chip-active-bg:#3b8eff;
      --bib-chip-active-fg:#fff;
      --bib-sort-active-bg:#eee;
      --bib-sort-active-fg:#111;
      --bib-link:#5dade2;
      --bib-sep:#2a2e36;
      --bib-tile-frame-bg:#0d0f12;
      --bib-poster-placeholder:#2a2e36;
    }}
  }}
  body {{ background:var(--bib-bg); margin:0;
    font:16px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    color:var(--bib-fg); }}
  .topbar {{ background:var(--bib-topbar-bg);
    border-bottom:1px solid var(--bib-topbar-border);
    position:sticky; top:0; z-index:10; }}
  .topbar-row1 {{ padding:0.9rem 1rem 0.5rem; display:flex;
    align-items:baseline; gap:1rem; }}
  .topbar h1 {{ margin:0; font-size:1.4rem; }}
  .topbar .count {{ color:var(--bib-muted); font-size:0.95rem; }}
  .topbar .links {{ margin-left:auto; }}
  .topbar .links a {{ color:var(--bib-link); text-decoration:none;
    font-size:0.9rem; margin-left:1rem; }}
  .topbar-row2 {{ padding:0.4rem 1rem 0.5rem; display:flex;
    gap:0.4rem; align-items:center;
    overflow-x:auto; -webkit-overflow-scrolling:touch;
    white-space:nowrap; }}
  .sort {{ display:inline-block; padding:0.35rem 0.7rem;
    border-radius:1rem; background:var(--bib-chip-bg);
    color:var(--bib-chip-fg);
    text-decoration:none; font-size:0.85rem; flex-shrink:0; }}
  .sort-active {{ background:var(--bib-sort-active-bg);
    color:var(--bib-sort-active-fg); }}
  .sort-sep {{ width:1px; height:1.5rem; background:var(--bib-sep);
    margin:0 0.3rem; flex-shrink:0; }}
  .topbar-row3 {{ padding:0 1rem 0.7rem; display:flex;
    gap:0.4rem; overflow-x:auto; -webkit-overflow-scrolling:touch;
    white-space:nowrap; }}
  .chip {{ display:inline-block; padding:0.35rem 0.75rem;
    border-radius:1rem; background:var(--bib-chip-bg);
    color:var(--bib-chip-fg);
    text-decoration:none; font-size:0.85rem; flex-shrink:0;
    border:1px solid transparent; }}
  .chip-active {{ background:var(--bib-chip-active-bg);
    color:var(--bib-chip-active-fg); }}
  .grid {{ display:grid; gap:0.75rem; padding:0.9rem;
    grid-template-columns: repeat(2, 1fr); }}
  @media (min-width:600px)  {{ .grid {{ grid-template-columns: repeat(3, 1fr); }} }}
  @media (min-width:900px)  {{ .grid {{ grid-template-columns: repeat(4, 1fr); }} }}
  @media (min-width:1200px) {{ .grid {{ grid-template-columns: repeat(6, 1fr); }} }}
  .tile {{ background:var(--bib-card); border-radius:10px; overflow:hidden;
    text-decoration:none; color:var(--bib-fg);
    box-shadow:0 1px 3px var(--bib-card-border);
    display:flex; flex-direction:column;
    transition:transform 0.15s, box-shadow 0.15s; }}
  .tile:active {{ transform:scale(0.97); }}
  .tile-img-wrap {{ position:relative; aspect-ratio:2/3;
    overflow:hidden; background:var(--bib-tile-frame-bg); }}
  .tile-poster {{ width:100%; height:100%; object-fit:cover;
    background:var(--bib-poster-placeholder); display:block; }}
  /* Landscape sources rendered as portrait tile via blur-bg trick:
     blurred copy of the banner fills the 2:3 frame as background,
     sharp copy sits centered at native aspect. Same approach as the
     hero on the detail page. Visually all tiles share the same
     aspect → uniform grid. */
  .tile-bg-blur {{ position:absolute; inset:-8%;
    background-size:cover; background-position:center;
    filter:blur(20px) brightness(0.55); }}
  .tile-bg-sharp {{ position:absolute; inset:0; margin:auto;
    max-width:100%; max-height:100%; object-fit:contain; }}
  .tile-no-poster {{ width:100%; height:100%;
    background:var(--bib-tile-frame-bg);
    display:flex; align-items:center; justify-content:center;
    padding:1.2rem; box-sizing:border-box; }}
  .tile-no-poster img {{ max-width:100%; max-height:100%; object-fit:contain;
    filter:brightness(1.1); }}
  .tile-badge {{ position:absolute; top:0.4rem; left:0.4rem;
    background:rgba(0,0,0,0.7); color:#fff;
    padding:0.15rem 0.45rem; border-radius:0.4rem;
    font-size:0.85rem; line-height:1; backdrop-filter:blur(4px); }}
  .tile-meta {{ padding:0.55rem 0.65rem 0.7rem; }}
  .tile-title {{ font-weight:600; font-size:0.95rem; line-height:1.2;
    display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical;
    overflow:hidden; }}
  .tile-rating {{ color:#e0a000; font-size:0.85rem; font-weight:500;
    margin-left:0.3rem; }}
  .tile-sub {{ font-size:0.78rem; color:var(--bib-muted);
    margin-top:0.25rem; }}
</style>
</head><body>
<div class="topbar">
  <div class="topbar-row1">
    <h1>📚 Bibliothek</h1>
    <span class="count">{len(tiles)} / {len(all_tiles)} Serien</span>
    <span class="links"><a href="/recordings">→ Verwalten</a></span>
  </div>
  <div class="topbar-row2">
    {type_html}
    <span class="sort-sep"></span>
    {sort_html}
  </div>
  <div class="topbar-row3">
    {''.join(chip_html)}
  </div>
</div>
<div class="grid">
{body}
</div>
</body></html>"""
    return Response(html, mimetype="text/html")


@app.route("/bibliothek/<path:group_key>")
def bibliothek_show(group_key):
    """Episode list for one show. Click episode → existing player."""
    by_group = _bib_bucket_completed()
    eps = by_group.get(group_key)
    if not eps:
        abort(404)
    title = _bib_group_title(group_key, eps)
    eps_sorted = sorted(eps, key=lambda e: -(e.get("start") or 0))
    now = int(time.time())

    with _epg_meta_lock:
        meta = _epg_meta.get(_normalize_title(title)) or {}
        # User-group fallback: pick first member with a poster.
        if (not meta.get("poster")) and group_key.startswith("usergroup:"):
            for ep in eps:
                ep_t = (ep.get("disp_title") or "").strip()
                if not ep_t: continue
                ep_meta = _epg_meta.get(_normalize_title(ep_t)) or {}
                if ep_meta.get("poster"):
                    meta = ep_meta; break
    poster = meta.get("poster")
    rating = meta.get("rating")
    rating_html = (f'<span class="hero-rating">★ {rating}</span>'
                   if rating else '')

    # Hero (= AppleTV-style backdrop). Blurred-zoomed copy of the
    # poster fills the full hero rectangle as background, then the
    # sharp poster sits centered at its native aspect ratio. Avoids
    # the brutal-crop you get when forcing 4:1 fernsehserien banners
    # or 2:3 TMDB posters into one fixed-aspect frame.
    if poster:
        hero_inner = (
            f'<div class="hero-bg-blur" '
            f'style="background-image:url({_bib_html_escape(poster)})"></div>'
            f'<img class="hero-bg-sharp" src="{_bib_html_escape(poster)}" alt="">'
            f'<div class="hero-fade"></div>'
            f'<div class="hero-text">'
            f'  <h1 class="hero-title">{_bib_html_escape(title)}</h1>'
            f'  <div class="hero-sub">'
            f'{_bib_html_escape(eps_sorted[0].get("channelname") or "")} '
            f'· {len(eps_sorted)} Folge{"n" if len(eps_sorted) != 1 else ""}'
            f'{(" " + rating_html) if rating else ""}'
            f'  </div>'
            f'</div>')
    else:
        hero_inner = (
            f'<div class="hero-fade"></div>'
            f'<div class="hero-text">'
            f'  <h1 class="hero-title">{_bib_html_escape(title)}</h1>'
            f'  <div class="hero-sub">'
            f'{_bib_html_escape(eps_sorted[0].get("channelname") or "")} '
            f'· {len(eps_sorted)} Folge{"n" if len(eps_sorted) != 1 else ""}'
            f'  </div>'
            f'</div>')

    rows = []
    for e in eps_sorted:
        u = e.get("uuid", "")
        start = e.get("start") or 0
        stop = e.get("stop") or start
        dur_min = max(1, (stop - start) // 60)
        ch = e.get("channelname") or ""
        rel = _bib_rel_time(now - start) if start else ""
        import datetime
        when = datetime.datetime.fromtimestamp(start).strftime("%a %d.%m. %H:%M")
        rows.append(
            f'<a class="ep" href="/recording/{u}">'
            f'<div class="ep-when">{_bib_html_escape(when)}</div>'
            f'<div class="ep-meta">{_bib_html_escape(ch)} · {dur_min} min · {rel}</div>'
            f'<div class="ep-play">▶</div>'
            f'</a>')
    body = "\n".join(rows)
    html = f"""<!doctype html>
<html lang="de"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{_bib_html_escape(title)}</title>
<style>
  /* Color tokens — same names as bibliothek_page so dark-mode behaves
     identically across the two views. */
  :root {{
    --bib-bg:#f5f6f8;
    --bib-fg:#222;
    --bib-muted:#777;
    --bib-card:#ffffff;
    --bib-card-border:rgba(0,0,0,0.05);
    --bib-link:#0066cc;
    --bib-fab-bg:rgba(255,255,255,0.92);
    --bib-fab-fg:#0066cc;
    --bib-hero-bg:#1a1d24;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --bib-bg:#121417;
      --bib-fg:#eee;
      --bib-muted:#888;
      --bib-card:#1d2025;
      --bib-card-border:rgba(255,255,255,0.06);
      --bib-link:#5dade2;
      --bib-fab-bg:rgba(30,32,38,0.92);
      --bib-fab-fg:#5dade2;
      --bib-hero-bg:#0d0f12;
    }}
  }}
  body {{ background:var(--bib-bg); margin:0;
    font:16px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
    color:var(--bib-fg); }}
  /* Floating back-button on top-left of the hero (= no separate
     topbar bar). Stays legible on dark hero in both color schemes. */
  .back-fab {{ position:absolute; top:0.75rem; left:0.75rem; z-index:5;
    background:var(--bib-fab-bg); color:var(--bib-fab-fg);
    width:2.2rem; height:2.2rem; border-radius:50%;
    display:flex; align-items:center; justify-content:center;
    text-decoration:none; font-size:1.3rem; line-height:1;
    box-shadow:0 2px 6px rgba(0,0,0,0.25);
    backdrop-filter:blur(6px); -webkit-backdrop-filter:blur(6px); }}
  /* Hero (= AppleTV-style backdrop). Width-100%, fixed aspect
     ratio so layout doesn't shift while image loads. */
  .hero {{ position:relative; width:100%; aspect-ratio:16/9;
    background:var(--bib-hero-bg); overflow:hidden; }}
  @media (min-width:700px) {{ .hero {{ aspect-ratio:21/9; }} }}
  /* Blurred backdrop (= poster scaled to cover, blurred to fill
     the gaps when sharp foreground is letterboxed). Slight scale
     prevents blur-edge halos. */
  .hero-bg-blur {{ position:absolute; inset:-8%;
    background-size:cover; background-position:center;
    filter:blur(28px) brightness(0.55); }}
  /* Sharp foreground (= poster at its native aspect, no crop).
     For a 4:1 fernsehserien banner: appears as wide horizontal
     band centered, blur fills top+bottom. For a 2:3 TMDB portrait:
     appears as tall narrow center column, blur fills left+right. */
  .hero-bg-sharp {{ position:absolute; inset:0; margin:auto;
    max-width:100%; max-height:100%;
    object-fit:contain; }}
  .hero-fade {{ position:absolute; inset:0;
    background:linear-gradient(to top,
      rgba(0,0,0,0.85) 0%,
      rgba(0,0,0,0.55) 35%,
      rgba(0,0,0,0.15) 70%,
      rgba(0,0,0,0.05) 100%); }}
  .hero-text {{ position:absolute; left:1rem; right:1rem; bottom:0.9rem;
    color:#fff;
    text-shadow:0 1px 3px rgba(0,0,0,0.5); }}
  .hero-title {{ margin:0; font-size:1.7rem; font-weight:700;
    line-height:1.15; }}
  @media (min-width:700px) {{ .hero-title {{ font-size:2.4rem; }} }}
  .hero-sub {{ margin-top:0.35rem; font-size:0.95rem; opacity:0.95; }}
  .hero-rating {{ color:#ffd54a; margin-left:0.4rem; font-weight:500; }}
  .ep-list {{ padding:1rem 0.75rem; }}
  .ep {{ background:var(--bib-card); border-radius:8px;
    padding:0.7rem 0.9rem;
    margin-bottom:0.5rem; display:grid;
    grid-template-columns: 1fr auto; grid-template-areas:
      "when play" "meta play";
    align-items:center; gap:0.1rem 1rem;
    text-decoration:none; color:var(--bib-fg);
    box-shadow:0 1px 2px var(--bib-card-border);
    transition:transform 0.15s; }}
  .ep:active {{ transform:scale(0.98); }}
  .ep-when {{ grid-area:when; font-weight:500; font-size:0.95rem; }}
  .ep-meta {{ grid-area:meta; color:var(--bib-muted); font-size:0.8rem;
    margin-top:0.1rem; }}
  .ep-play {{ grid-area:play; color:var(--bib-link); font-size:1.6rem; }}
</style>
</head><body>
<a class="back-fab" href="/bibliothek" title="Zurück">←</a>
<div class="hero">
{hero_inner}
</div>
<div class="ep-list">
{body}
</div>
</body></html>"""
    return Response(html, mimetype="text/html")


def _fix_latin1_filenames():
    """Rename any file/dir under HLS_DIR whose raw bytes are Latin-1
    (ä/ö/ü/ß as single bytes) to proper UTF-8. After a Mac→Pi tar/rsync
    restore the original tvh-era filenames land as Latin-1 bytes; this
    container's UTF-8 fs-encoding then surfaces them as surrogateescape
    strings (\\udcXX), and Flask crashes with
    'utf-8 codec can't encode character \\udc..' whenever such a name
    appears in a response (/learning, training-snapshot, etc.).

    Operates entirely on bytes-paths, so it never has to decode a name
    for the OS and works regardless of locale. Walks bottom-irrelevant
    (renames as it goes, descends via the post-rename path). Runs once
    at startup — cheap (a few hundred dir entries), and self-healing for
    future restores so we don't have to remember the manual fix-script."""
    root = str(HLS_DIR).encode("utf-8")

    def is_utf8(b):
        try:
            b.decode("utf-8"); return True
        except UnicodeDecodeError:
            return False

    fixed = [0]

    def walk(dir_bytes):
        try:
            entries = os.listdir(dir_bytes)
        except OSError:
            return
        for name in entries:
            full = dir_bytes + b"/" + name
            if not is_utf8(name):
                try:
                    new_name = name.decode("latin-1").encode("utf-8")
                    new_full = dir_bytes + b"/" + new_name
                    os.rename(full, new_full)
                    fixed[0] += 1
                    full = new_full
                except OSError:
                    pass
            try:
                if os.path.isdir(full):
                    walk(full)
            except OSError:
                pass

    try:
        walk(root)
    except Exception as e:
        print(f"[startup] latin1-filename check failed: {e}", flush=True)
        return
    if fixed[0]:
        print(f"[startup] renamed {fixed[0]} Latin-1 filename(s) → UTF-8",
              flush=True)


if __name__ == "__main__":
    HLS_DIR.mkdir(parents=True, exist_ok=True)
    _fix_latin1_filenames()
    # tv-detect (which fully replaced comskip 2026-04-25) reads its
    # tuning from CLI flags, not from .ini files, so the legacy
    # COMSKIP_INI / COMSKIP_INI_PER_CHANNEL maps are no longer
    # written to disk. The constants stay defined for backward
    # compat with downstream code that still imports them.
    load_codec_cache()
    load_stats()
    load_epg_archive()
    load_favorites()
    load_always_warm()
    load_live_ads()
    load_mediathek_rec()
    _adaptive_padding_load()
    adopt_surviving_ffmpegs()
    signal.signal(signal.SIGHUP, _sighup_reload)
    threading.Thread(target=_file_watcher_loop, daemon=True).start()
    threading.Thread(target=idle_killer_loop, daemon=True).start()
    # _app_idle_loop removed 2026-05-30 (slice 5): tv-receiver evicts app sessions.
    threading.Thread(target=prewarm_codecs, daemon=True).start()
    threading.Thread(target=epg_snapshot_loop, daemon=True).start()
    threading.Thread(target=_rec_prewarm_loop, daemon=True).start()
    # Default-pause the auto-scheduler on first install (= log file
    # absent → never ran here before). User opts-in via the toggle on
    # /learning to avoid surprise auto-scheduled recordings on a
    # freshly-deployed instance. Same Henne-Ei-Pattern as auto-confirm:
    # gate on a persistent .auto-schedule-init-done marker so a
    # container restart before the first successful auto-schedule
    # apply doesn't silently re-pause a user who already opted in.
    sched_init_marker = HLS_DIR / ".tvd-models" / ".auto-schedule-init-done"
    if (not sched_init_marker.exists()
            and not AUTO_SCHED_LOG.is_file()
            and not AUTO_SCHED_PAUSE.exists()):
        try:
            AUTO_SCHED_PAUSE.parent.mkdir(parents=True, exist_ok=True)
            AUTO_SCHED_PAUSE.write_text(str(int(time.time())))
            print("[auto-sched] default-paused on first install — "
                  "user opt-in via /learning toggle", flush=True)
        except Exception as e:
            print(f"[auto-sched] init err: {e}", flush=True)
    try:
        sched_init_marker.parent.mkdir(parents=True, exist_ok=True)
        sched_init_marker.touch()
    except Exception:
        pass
    # Auto-scheduler daily loop migrated to tv-recorder (slice 6h). Set
    # AUTO_SCHED_LOOP_OWNER=recorder to disable Flask's loop and avoid both
    # services double-scheduling. Default "flask" keeps legacy behaviour.
    if os.environ.get("AUTO_SCHED_LOOP_OWNER", "flask") == "flask":
        threading.Thread(target=_auto_schedule_loop, daemon=True).start()
    threading.Thread(target=_adaptive_padding_loop, daemon=True).start()
    # Default-pause auto-confirm on first install — user opts-in
    # explicitly via /learning toggle once they've seen a few sample
    # verdicts on the /recordings badge to gain confidence.
    #
    # The "first install" guard is gated by AUTO_CONFIRM_INIT_DONE
    # (= a marker file written ONCE on the first start of any install).
    # Without this marker, every container restart would re-pause an
    # already opted-in user as long as no auto-confirm had successfully
    # applied yet — which happens often in practice (V2 head deploys
    # routinely empty the cutlists for a few minutes, AUTO_CONFIRM_LOG
    # stays absent, restart re-pauses the user's choice). The marker
    # decouples "is this the very first start ever" from "has the
    # apply loop ever produced output".
    init_done_marker = HLS_DIR / ".tvd-models" / ".auto-confirm-init-done"
    if (not init_done_marker.exists()
            and not AUTO_CONFIRM_LOG.is_file()
            and not AUTO_CONFIRM_PAUSE.exists()):
        try:
            AUTO_CONFIRM_PAUSE.parent.mkdir(parents=True, exist_ok=True)
            AUTO_CONFIRM_PAUSE.write_text(str(int(time.time())))
            print("[auto-confirm] default-paused on first install — "
                  "user opt-in via /learning toggle", flush=True)
        except Exception as e:
            print(f"[auto-confirm] init err: {e}", flush=True)
    # Always lay down the marker after the (possibly no-op) init pass
    # so future restarts respect the user's explicit toggle state.
    try:
        init_done_marker.parent.mkdir(parents=True, exist_ok=True)
        init_done_marker.touch()
    except Exception:
        pass
    # Auto-confirm apply-loop migrated to tv-recorder (slice 4b). Set
    # AUTO_CONFIRM_LOOP_OWNER=recorder to disable Flask's loop; both running
    # would double-apply auto-confirmations.
    if os.environ.get("AUTO_CONFIRM_LOOP_OWNER", "flask") == "flask":
        threading.Thread(target=_auto_confirm_loop, daemon=True).start()
    # spot-fp extraction moved to Mac (tv-spot-extract.py) — Pi only
    # stores + indexes via /api/internal/spot-fp/upload. Worker thread
    # + resume-at-boot are no-ops now (kept callable for one-off
    # diagnostics if needed; just don't start them automatically).
    threading.Thread(target=always_warm_loop, daemon=True).start()
    threading.Thread(target=_mediathek_rip_loop, daemon=True).start()
    threading.Thread(target=_mediathek_autorec_loop, daemon=True).start()
    threading.Thread(target=ffmpeg_watchdog_loop, daemon=True).start()
    threading.Thread(target=disk_cleanup_loop, daemon=True).start()
    threading.Thread(target=state_backup_loop, daemon=True).start()
    # _cleanup_watched_loop disabled 2026-05-05 — user wants explicit
    # control over recording lifetime, no auto-delete after watched+N-days.
    # threading.Thread(target=_cleanup_watched_loop, daemon=True).start()
    _load_epg_meta()
    threading.Thread(target=_enrich_recordings_loop, daemon=True).start()
    # waitress in production; fall back to Flask's built-in only if
    # waitress somehow isn't importable (e.g. an older image).
    try:
        from waitress import serve
        # threads=64 — was 16, but the Mac-offload daemon's HLS PUT
        # uploads + .ts downloads + simultaneous page-loads from
        # iOS browser overran the pool, causing /recordings + /learning
        # to take 5-10 s through Caddy while the in-process handler
        # itself returned in <1.5 s. With 64 threads, page handlers
        # always have a free slot even when several daemon transfers
        # are in flight.
        # removed so server banner doesn't leak the version.
        # Port is env-driven for the Go strangler-fig cutover: the Go gateway
        # takes :8080 (Caddy unchanged) and proxies unmigrated routes here on
        # GATEWAY_PORT=8081. Default 8080 = pre-cutover behaviour (no change).
        gw_port = int(os.environ.get("GATEWAY_PORT", "8080"))
        print(f"serving via waitress on 0.0.0.0:{gw_port}", flush=True)
        serve(app, host="0.0.0.0", port=gw_port, threads=64, ident=None)
    except ImportError:
        print("waitress not installed, falling back to flask dev server",
              flush=True)
        app.run(host="0.0.0.0", port=int(os.environ.get("GATEWAY_PORT", "8080")), threaded=True)
