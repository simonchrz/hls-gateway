# Mac-side offload handlers

A Mac on the same LAN runs two launchd-managed handlers that take
CPU-heavy comskip + remux work off the Pi (Pi5's 4 cores are
otherwise saturated by 3-4 live ffmpeg transcodes). Both write their
output back onto the SSD via SMB and coordinate with the Pi via
cooperative lock-files + a heartbeat.

| Agent | Script | Cadence | Job |
|---|---|---|---|
| `com.user.tv-comskip` | `~/bin/tv-comskip.sh` | every 60 s | Finished recordings: remux `.ts` → HLS-VOD + run comskip cutlist |
| `com.user.tv-live-comskip` | `~/bin/tv-live-comskip.py` | every 30 s | Live-buffer ad detection: 25-min sliding windows on warm-channel HLS, writes `.live_ads.json` |

Both scripts also live in `mac/` of this repo for backup/visibility.

## Heartbeat → Pi defers when Mac is alive

The Mac touches `/mnt/tv/hls/.mac-comskip-alive` every iteration.
Pi-side `_mac_comskip_alive()` (in `service.py`, 5-min staleness
window) makes both `_rec_prewarm_once` and `_rec_hls_spawn` defer
**remux + comskip** to the Mac when the heartbeat is fresh. Pi-side
lazy fallback in `recording_hls()` (a user actually opened the
recording) is unaffected — that path always spawns so playback isn't
held up by an asleep Mac.

The live-buffer offload toggles via `LIVE_ADS_OFFLOAD=mac` env var on
hls-gateway; when set, `_live_adskip_loop` early-returns and
`/api/live-ads/<slug>` re-reads `.live_ads.json` each call.

## Cooperative `.scanning` lock-file

Both Mac and Pi write `<rec-dir>/.scanning` before spawning
comskip/remux on a recording, and read it before starting their own
— prevents the brief race window where heartbeats overlap with
in-flight work. Stale locks (>15 min, e.g. crashed scanner) are
ignored.

## macOS TCC requirement (non-obvious)

On macOS 15+ (this host runs 26.5), launchd-spawned processes do
**not** inherit a Terminal session's access to SMB/network-volume
mounts. Until **every binary in the exec chain** has Full Disk
Access, the launchd path silently fails on the SMB mount even though
the same script run from a terminal works. The chain for
`tv-comskip.sh` today is `bash → CommandLineTools-python3 → bash -c
→ ffmpeg/ffprobe/comskip` — and **all four** need FDA grants:

- `/bin/bash`
- `/Library/Developer/CommandLineTools/usr/bin/python3`
- `/opt/homebrew/bin/ffmpeg`
- `/opt/homebrew/bin/ffprobe`

(The Comskip binary at `~/src/Comskip/comskip` doesn't seem to need
it explicitly — at least so far it works because the launching python
already has the grant. If it ever fails with "Operation not
permitted", grant it too.)

TCC uses the *responsible process*, not just the final binary making
the syscall, so missing **any** ancestor in the chain breaks access.
Two failure modes are worth recognising:

- **Total**: `PermissionError: [Errno 1] Operation not permitted` on
  the SMB path. Usually means `/bin/bash` or python lost their grant.
- **Per-binary**: ffprobe returns "unknown" codec instead of
  `mpeg2video`, ffmpeg hangs in `open()` with no progress for minutes,
  eventually surfacing `Error opening input: Interrupted system call`
  (EINTR). This means ffprobe/ffmpeg specifically lacks FDA — the
  python parent has it (which is why the script gets that far), but
  the children don't.

**Re-signing kills grants.** OS updates that re-sign `/bin/bash`,
Xcode updates that swap out `/usr/bin/python3` (it's a stub that
delegates via `xcode-select`), or `brew upgrade ffmpeg` that lays
down a new binary on a new path can all silently drop the cached
grant. Symptom: the same launchd log entries that worked yesterday
are now full of `Operation not permitted` or open()-hangs. Re-grant
the affected binary.

**`/usr/bin/python3` is a moving target** — it's a stub that
delegates via `xcode-select` to whichever Python the active
developer-tools install ships, so after an Xcode install/update its
TCC identity flips and the FDA grant silently stops applying. Pin to
`/Library/Developer/CommandLineTools/usr/bin/python3` instead
(Apple-signed, stable across Xcode updates) — both in launchd plists'
`ProgramArguments` and in script shebangs. Grant FDA on that exact
path.

## Python-`subprocess.run` is ~30x slower than `bash -c` for ffmpeg on macOS

Discovered while building the rec-handler remux path: invoking ffmpeg
via `subprocess.run(["ffmpeg", ...])` (Python 3.9 from
CommandLineTools, posix_spawn-based exec) makes ffmpeg run **30x
slower** than the same args invoked from bash with fork+exec. Measured
on the M5 Pro: 5 segments / 10 s via subprocess.run vs 165 segments
/ 10 s via `bash -c`. Same binary, same args, same input file.
Reproducible from terminal, not specific to launchd context.

Workaround in `tv-comskip.sh`: shell-escape the argv with
`shlex.quote` and run via `subprocess.run(["/bin/bash", "-c",
cmd_str])`. Comskip itself doesn't seem affected (still invoked
directly), only ffmpeg.

Suspected cause is `close_fds=True` (Python 3.x default) interacting
with libdispatch / libx264's thread pool init under posix_spawn — but
not confirmed. Bash sidesteps the entire question.

## Local `/tmp` cache + bulk SMB copy beats writing HLS segments straight to SMB

The naive approach (write each `seg_NNNNN.ts` directly to the
SMB-mounted output dir during the transcode) caps throughput at ~1
MB/s — not bandwidth-limited (raw SMB sustains 90+ MB/s for one big
file), but **per-file sync overhead** as each 2 MB segment gets
flushed individually. At 30 segs/s ffmpeg encode rate this means
HLS-write becomes the bottleneck and total time stays close to
realtime.

`tv-comskip.sh` instead stages segments + playlist locally under
`/tmp/tv-rec-handler/<uuid>/` during transcode (~10x faster IO),
then bulk-streams the whole tree to the SMB target dir at the end
(~23 MB/s sustained). Net: a 70 min Galileo MPEG-2 source remuxes in
~71 s + ~78 s SMB-copy = ~2.5 min on the M5 Pro, vs 15-20 min on the
Pi5 doing the same work directly to the SSD.

## History — how this got here

**Quick Win Teil 1 (finished-recording comskip, Mac):** done since
mid-April. Pi's `_rec_prewarm_once` skips when a non-empty `.txt`
already exists; Mac at 60 s interval reliably writes first on the M5
Pro.

**Quick Win Teil 2 (live-buffer comskip, Mac):** active since
2026-04-22. Mac script reads `.m3u8` via SMB, segments via Caddy
HTTPS at `:8443` (file_server fast-path bypasses Flask), with a local
segment cache at `/tmp/tv-live-scan/<slug>/segs/` that incrementally
fetches only new segments per scan. Each scan: ~50-1300 MB depending
on window size, ~3-25 s comskip runtime.

The first attempt 2026-04-21 was reverted ~1 h later when parallel
Mac-side SMB reads (~900 MB per scan) + Pi-side DVB writes saturated
the USB-2 link, drove the SSD into EXT4 shutdown. Root cause was the
**USB-2 cable, not the architecture** — replaced 2026-04-22 with a
SuperSpeed cable, measured 332 MB/s sustained read on the SSD (was
~40 MB/s on USB-2). Pi5 → Mac WiFi link delivers ~824 Mbps via Caddy
HTTPS, not the bottleneck either.

To revert again: `launchctl bootout
gui/$UID/com.user.tv-live-comskip` on the Mac, comment
`LIVE_ADS_OFFLOAD=mac` in `hls-gateway/docker-compose.yml`, `docker
compose up -d`.

Two non-obvious gotchas from the live-comskip port:

1. *Python 3.9 ISO-8601 PDT parsing*: `datetime.fromisoformat()` on
   macOS's system python rejects `+0200` (no colon); 3.11+ handles
   natively. `tv-live-comskip.py` normalizes with regex before parsing.
2. *Xcode-installed `/usr/bin/python3` breaks TCC FDA grant* —
   covered under the TCC section above.

## Pi CPU load profile (measured)

Idle / no live streams: loadavg ~0.4 of 4 cores, comskip at ~0.5%.
The Pi is 89% idle, comskip is not the bottleneck on its own.

Under live TV load (3 concurrent channels transcoding): 3× ffmpeg
pulling ~190%+43%+34% CPU plus comskip at ~40% → **loadavg 3.5-18**
measured. When ffmpeg drops to 1-2 streams but loadavg stays >10,
it's I/O-bound on the SSD (SMB traffic + comskip reads + ffmpeg
segment writes all thrashing the same ext4). The Mac offload removed
two contenders (rec-comskip + live-buffer comskip), leaving only the
live-transcode ffmpegs and the rec-prewarm libx264 transcode as
Pi-local CPU work.
