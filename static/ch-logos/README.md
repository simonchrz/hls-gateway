# Channel Logo Overrides

Drop `<slug>.svg`, `<slug>.png`, or `<slug>.jpg` files here to override
tvheadend's imagecache logo for that channel in the recordings page +
scrub-bar UI.

The `slug` is the tvheadend channel slug (lowercase, hyphens for
spaces/dots) — find it in `/api/channels` or in `hls-gateway`'s
`channel_map`.

Examples of common DE-private slugs: `vox`, `rtl`, `rtlzwei`,
`prosieben`, `kabel-eins`, `sixx`, `sat-1`, `nitro`, `prosiebenmaxx`,
`super-rtl`, `ntv`, `dmax`, `sport1`, `tele-5`.

SVG is preferred (scales cleanly to any display size). The
`service.py` logo resolver picks the first extension it finds, so
dropping a higher-resolution PNG alongside an existing SVG won't
take effect — delete the SVG first.
