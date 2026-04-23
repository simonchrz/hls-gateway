# Operations

## iOS Safari gotchas worth remembering

- Native HLS does **not** allow pausing live streams or seeking back
  in live playlists. Workaround: hls.js + MSE (iOS 17.1+), or `src`-
  strip for "pause" (screen goes black, resume re-seeks via stored
  wall-time).
- iOS clamps `v.currentTime` to already-buffered segments. To seek
  into fresh content reliably, `hls.stopLoad() + hls.startLoad(ts)`
  first, then poll `v.seekable` until it covers the target.
- iOS reports `duration = Infinity` for live-type HLS even when a
  finite DVR window exists → use `v.seekable.end(0) - start(0)` for
  scrub-bar length, not `duration`.
- `.ts` extension confuses Caddy's MIME sniffer (Qt-linguist). Force
  `Content-Type: video/mp2t` via `header` directive.
- Self-signed CA cert must be installed on every iOS device with the
  "Full Trust" step under Settings → General → Profiles.

## Deploying code changes: SIGHUP, NOT `compose restart`

`docker compose restart hls-gateway` tears down the entire container PID
namespace, which kills every per-channel ffmpeg child along with
python — losing **2 hours of warm-buffer cache** per channel and the
live-buffer ad markers that reference those segments. Only the source
`.live_ads.json` survives (Mac-side data); on-disk it's still valid
but the player can't render markers for time ranges no longer in the
buffer.

Instead, after `scp`-ing the new `service.py`:

```sh
ssh raspberrypi5lan 'docker kill -s HUP hls-gateway'
```

There's a `_sighup_reload` handler in `service.py` that calls
`_self_exec` → `os.execv(sys.executable, [...])`, replacing the python
process *in place* (same PID 1, new code). Children spawned with
`start_new_session=True` (every per-channel ffmpeg, every comskip,
every rec-prewarm ffmpeg) survive the parent's image swap because the
parent didn't actually die. On startup, `adopt_surviving_ffmpegs()`
walks the per-channel `<slug>/.ffmpeg.pid` files, reattaches the live
processes via the `AdoptedProcess` Popen-stub, and the always-warm
loop continues like nothing happened.

When `compose restart` is unavoidable (image rebuild, env-var changes,
dependency upgrades): warn the user that ~2 h of live cache will reset,
then confirm.

```sh
# Rebuild image (e.g. Dockerfile change) — buffer will reset.
ssh raspberrypi5lan 'cd ~/hls-gateway && docker compose build && docker compose up -d'
```

## SSD swap / device re-enumeration

The recordings SSD is USB-connected. When swapping cables or moving
the drive to another port, follow this order:

```sh
# Mac (if SMB mount + offload agent are active): stop the agent + unmount first
launchctl bootout gui/$UID/com.user.tv-comskip
diskutil unmount ~/mnt/pi-tv

# Pi: stop anything touching /mnt/tv
# 1. POST /api/dvr/entry/stop for any active DVR recording (else lsof shows open write handles)
# 2. take services down — `docker compose down`, NOT stop (see gotcha below)
for d in tvheadend hls-gateway caddy; do (cd /home/simon/$d && docker compose down); done
sudo systemctl stop smbd nmbd
sudo sync && sudo umount /mnt/tv
echo 1 | sudo tee /sys/block/sda/device/delete   # flush write cache + detach USB device

# --- physically swap cable / port ---

# Pi: bring back up in dependency order
sudo mount /mnt/tv           # fstab uses UUID so /dev/sdb vs /dev/sda doesn't matter
sudo systemctl start smbd nmbd
for d in tvheadend hls-gateway caddy; do (cd /home/simon/$d && docker compose up -d); done

# Mac: remount + reload agent
/sbin/mount_smbfs "//simon:***@raspberrypi5lan/tv" ~/mnt/pi-tv
launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.user.tv-comskip.plist
```

**Docker bind-mount staleness after device re-enumeration** — use
`docker compose down && up`, **not** `docker stop && start`. When the
underlying device disappears and reappears (SSD unplug → replug),
containers started with `docker start` keep a stale mount-namespace
reference to the old device node; every access inside the container
returns `OSError: [Errno 5] Input/output error` even though `ls
/mnt/tv/hls` on the host works fine. `compose down && up` recreates
the container, rebuilding the mount namespace fresh. Symptom in the
wild: `/recordings` returns HTTP 500 with `[Errno 5]` in the
hls-gateway traceback while every `.ts` file is visibly present from
the host shell. dmesg hint: EXT4 warnings referencing the *previous*
device node (e.g. `device sda1` after the drive re-enumerated as
`sdb1`).

## USB cable + port identification for the T5

The Samsung T5 negotiates USB 3.2 Gen 2 (10 Gbps) only with a proper
SuperSpeed A-to-C cable. Labels like "Anker 3.1A" refer to *charging
amperage*, not the USB spec — those are USB 2.0 cables. Diagnose via
`lsusb -t`: the T5 must attach to Bus 002 or Bus 004 (5000M root
hubs) for SuperSpeed; attaching to Bus 001/003 (480M) means USB 2.0
negotiation, either wrong port or a cable without SuperSpeed pins
(only 4 contacts in the USB-A plug vs. 9 for USB 3). `dmesg`
distinguishes: `high-speed` = USB 2.0, `SuperSpeed` = USB 3.0.
Orthogonal issue: the kernel also applies a UAS-disable quirk for the
T5 (`UAS is ignored for this device, using usb-storage instead`) which
costs another 30-40% throughput even after USB 3 negotiates;
overrideable with `usb-storage.quirks=04e8:61f5:` on the kernel
cmdline.

## Pi 5 USB current cap — `usb_max_current_enable=1`

Pi 5 caps the four USB-A ports to **600 mA combined** by default,
even with the official 5A power supply. The Samsung T5 SSD pulls
~900 mA peak, so under any sustained load (live TV write + recording
+ casual reads) the bus drops the device → USB disconnect → ext4
shutdown. Symptom: dmesg shows `usb X-Y: USB disconnect, device number
Z` immediately followed by `Buffer I/O error`, `failed Synchronize
Cache(10)`, journal abort. Diagnosed 2026-04-22 by isolating the SSD
on a Mac (4-hour sustained 20 GB read with zero USB events) and
finding `usb_max_current_enable` not set in `/boot/firmware/config.txt`.
Fix:

```sh
echo "usb_max_current_enable=1" | sudo tee -a /boot/firmware/config.txt
sudo reboot
# verify after reboot:
vcgencmd get_config usb_max_current_enable   # should print =1
```

## EXT4 shutdown under USB-2 I/O saturation

If the SSD is on a USB 2.0 link (480M, ~40 MB/s effective), the bus
becomes the bottleneck once parallel workloads stack up. Witnessed
2026-04-21: 3 concurrent DVB recordings + HLS segment writes + Mac-
side SMB reads (live-comskip pulling ~900 MB per scan) → USB reset →
`ext4_end_bio: I/O error 16` → journal abort → ext4 remounts read-only
with `shutdown` in the mount options. (Note: after the
`usb_max_current_enable=1` fix above, the disconnects under load
mostly stopped; what remained was occasional bus-saturation under
truly extreme parallel I/O, addressable by reducing read pressure.)
Recovery path (does not require physical access — the umount itself
triggered a USB reset):

```sh
# 1. Stop everything writing
for d in tvheadend hls-gateway caddy; do (cd /home/simon/$d && docker compose down); done
sudo systemctl stop smbd nmbd
# 2. umount (works because writes are already blocked)
sudo umount /mnt/tv
# 3. fsck — may report device not found if the kernel re-enumerated during umount; that's fine
sudo fsck.ext4 -y /dev/sdb1   # or /dev/sda1 after re-enumeration
# 4. Mount + restart
sudo mount /mnt/tv
sudo systemctl start smbd nmbd
for d in tvheadend hls-gateway caddy; do (cd /home/simon/$d && docker compose up -d); done
```

Prevention: don't stack read-heavy external consumers (Mac SMB scans)
on top of the live DVB writes while the SSD is on USB 2. The
hls-gateway's internal `_live_adskip_loop` is load-gated (`loadavg >
10 → skip`) and runs from the same ext4 — those reads don't traverse
SMB and don't compete with a network consumer. External scanners via
SMB do; keep `LIVE_ADS_OFFLOAD` unset until the SSD is on SuperSpeed.
