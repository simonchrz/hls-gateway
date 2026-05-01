# DESIGN.md — hls-gateway UI

Tokens + components + patterns extracted from the live `service.py`. Use this when generating or editing any HTML/CSS rendered by the gateway so the look stays coherent. Pragmatic / functional / dense — closer to a tvheadend admin UI than to a polished SaaS, deliberately.

## 1. Theme & color tokens

Default = **light** (tagsüber). Dark override via `prefers-color-scheme: dark`. Both schemes MUST be supported on every page; the user opens this UI from morning to night and the OS toggles around 18:00.

```css
:root {
    --bg: #fafafa; --fg: #222; --muted: #777;
    --border: #ddd; --stripe: #f0f0f0;
    --link: #0366d6; --code-bg: #f3f3f3;
    --logo-bg: #1f1f1f;     /* dark plaque works for bright + transparent picons */
}
@media (prefers-color-scheme: dark) {
    :root {
        --bg: #1a1a1a; --fg: #e4e4e4; --muted: #999;
        --border: #333; --stripe: #242424;
        --link: #79b8ff; --code-bg: #2a2a2a;
        --logo-bg: #1f1f1f;
    }
}
```

Always include in `<head>`:
```html
<meta name="color-scheme" content="light dark">
```
…so the browser themes form elements, scrollbars, and date pickers correctly.

**Status / semantic colors** (do not vary by theme — they read on both):
- `--ok` / success: `#27ae60`
- `--warn`: `#f39c12`
- `--err`: `#e74c3c`
- `live` / recording-active: `#e74c3c` (with `livepulse` animation)
- `scheduled` / pending: uses `var(--stripe)` bg + `var(--muted)` fg
- `done` / completed: `#27ae60`
- `ready` / fresh detect waiting for review: `#2980b9`
- `next-up` (next 3 imminent recordings): `#1e3a8a` bg / `#dbeafe` fg
- `tuner-overbook`: `#92400e` bg / `#fed7aa` fg, with `livepulse`
- `scanning` (active detect): `#8e44ad` bg / `#fff` fg, with `scanpulse`
- `ads-edited`: `#16a085` (Türkis = "user touched")
- `ads-uncertain`: `#d35400` (Orange = labelling target)
- `failed`: `#7f1d1d` bg / `#fecaca` fg

Animations:
```css
@keyframes scanpulse { 0%,100% {opacity:.5} 50% {opacity:1} }
@keyframes livepulse { 0%,100% {opacity:1} 50% {opacity:.6} }
```

## 2. Typography

- Stack: `-apple-system, BlinkMacSystemFont, sans-serif` — match host OS
- Body: `line-height: 1.5`
- Tabular numerals on time values: `font-variant-numeric: tabular-nums`
- Sizes are RELATIVE (em / .75em / .9em) — never `px` for text
- Heading scale: `h1` 1.4em, `h2` 1.1em, `h3` 1em (smaller than typical sites — this is a dashboard)

## 3. Layout

- Body: `max-width: 720px` (admin pages) or `900px` (data-heavy like /health), `margin: 0 auto`, `padding: 1.2em` (or `16px` on health)
- `* { box-sizing: border-box; }` global
- Mobile-first: every wide table wrapped in `<div style='overflow-x:auto;-webkit-overflow-scrolling:touch'>` so horizontal scroll happens INSIDE the table container, not on the page body
- Tables: `width:100%`, with `min-width:520-560px` on the actual `<table>` to enforce a readable layout that scrolls horizontally on phones rather than collapsing into illegible micro-columns

## 4. Components

### `.pill` — primary action chip

```css
.pill {
    background: #fff2; color: #fff; border: 0; padding: 7px 12px;
    border-radius: 16px; font-weight: 600; font-size: .85em;
    cursor: pointer; display: inline-flex; align-items: center;
    gap: 5px; flex: 0 0 auto; line-height: 1;
    /* Mobile-Safari: rapid taps interpret as double-tap → zoom-in.
       touch-action:manipulation kills the 300ms tap-delay AND the
       zoom gesture inside the button. */
    touch-action: manipulation; -webkit-touch-callout: none;
}
.pill:active { opacity: .7 }
.pill:disabled { background: #555; color: #bbb; cursor: default }
```

Used for player Start/Ende/Geprüft/Bumper-Mode buttons, mode toggles, action buttons in tables. The `#fff2` background works on the player's dark-overlay context. For light-bg context, override per-instance via inline `background:`.

### `.iconbtn` — circular icon button

```css
.iconbtn {
    background: #fff2; color: #fff; border: 0;
    width: 34px; height: 34px; border-radius: 17px;
    font-size: 1em; cursor: pointer;
    display: flex; align-items: center; justify-content: center;
    text-decoration: none; flex: 0 0 auto; line-height: 1;
    font-variant-emoji: text;
    transition: box-shadow .2s, background .2s;
    touch-action: manipulation; -webkit-touch-callout: none;
}
@media (hover:hover) {
    .iconbtn:hover {
        background: #fff4;
        box-shadow: 0 0 10px #7bdcff99, 0 0 18px #7bdcff55;
    }
}
.iconbtn:active { opacity: .6 }
.iconbtn:disabled { background: #555; color: #bbb; cursor: default }
```

For Vollbild ⛶, ✕ Schließen, ⏪ Zurück, ⏵ Play, ⏩ Vor, etc.

### `.badge` — inline status indicator

```css
.badge {
    font-size: .75em; padding: 2px 6px; border-radius: 3px;
    font-weight: 600; display: inline-block;
}
```

Then variant classes for state (see §1 colors). Badges are SQUARE-ish (3px radius), pills are PILL-SHAPED (16px radius). Don't mix.

### Tables

```css
table { width: 100%; border-collapse: collapse; }
th, td { padding: 8px 12px; text-align: left;
         border-bottom: 1px solid var(--border); font-size: .95em }
th { background: var(--th-bg, var(--stripe));
     color: var(--muted); font-weight: 500;
     text-transform: uppercase; font-size: .75em }
tr:last-child td { border: 0 }
```

For dashboards: also wrap in a `<div>` with rounded corners + card background:
```css
.card-table table { background: var(--card, var(--bg));
                    border-radius: 8px; overflow: hidden; }
```

### Cards (used on /health and /learning)

```css
.card { background: var(--card-bg, var(--code-bg));
        border: 1px solid var(--border); border-radius: 8px;
        padding: 12px; }
.card .lbl { color: var(--muted); font-size: .8em;
             text-transform: uppercase }
.card .val { font-size: 1.4em; font-weight: 600; margin-top: 4px }
```

Wrap in a responsive grid:
```css
.grid { display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 10px; }
```

### Collapsible sections (`<details>`/`<summary>`)

```css
details.section {
    margin: 1.2em 0; border: 1px solid var(--border);
    border-radius: 6px; padding: 0 12px;
}
details.section[open] { padding: 0 12px 12px; }
details.section > summary {
    cursor: pointer; padding: 10px 0;
    list-style-position: inside;
}
details.section > summary > h2 {
    display: inline; margin: 0; font-size: 1.1em;
}
details.section > summary:hover { color: var(--link); }
```

Pattern: every long /learning section is a `<details>`. State persists across reloads via `localStorage` keyed by `sec-<slug>`. Dashboards default-closed except for "Verlauf" and current-data sections.

## 5. Mobile patterns (non-negotiable)

The user mostly browses on iPhone/iPad. Every UI must hold up:

1. **Touch hit-targets ≥ 34×34 px** — see `.iconbtn` width/height.
2. **`touch-action: manipulation`** on every interactive element to kill the 300ms tap delay AND prevent rapid-tap → zoom-in. Already in `.pill` and `.iconbtn`.
3. **`<meta name="viewport" content="width=device-width,initial-scale=1">`** in every page `<head>`.
4. **Horizontal-scroll wrappers** for wide tables (see §3 Layout).
5. **`white-space: nowrap`** on action-button cells in tables so buttons don't wrap to two lines.
6. **No hover-only affordances** — wrap in `@media (hover:hover)` so they don't trigger phantom-active states on touch.

## 6. Status semantics — what colors mean

Critical: stay consistent across `/recordings`, `/epg`, `/learning`, `/health`. The user reads these as a system, not as separate pages.

| Concept | Color | Animation | Notes |
|---|---|---|---|
| Currently recording / live | `#e74c3c` red | `livepulse` | Same red everywhere |
| Active background work (detect, scan) | `#8e44ad` purple | `scanpulse` | Distinguishes from recording-live |
| Successfully done / playable | `#27ae60` green | none | Plain green dot for "complete" |
| Scheduled / pending | `var(--stripe)` neutral | none | Quiet — no urgency |
| User edited (touched) | `#16a085` teal | none | "I have done something here" |
| User-review opportunity | `#d35400` orange | none | "Look at me, give me labels" |
| Warning / regression | `#f39c12` amber | none | Possible problem, not blocking |
| Failure / error | `#7f1d1d` dark red / `#e74c3c` | none | Things broke |
| Tuner over-booking | `#92400e` brown | `livepulse` | Specific to scheduling-conflict warning |
| Next-up imminent | `#1e3a8a` deep blue / `#dbeafe` | none | "Starting soon" highlight |

Status emojis used inline next to badges (de-DE labels):
- `▶` abspielen / playable
- `⏳` remux / warming
- `◌` ausstehend / pending
- `⚠` fehlgeschlagen / failed
- `⏱` geplant / scheduled
- `🔍` scannt / scanning
- `✏️` edited
- `🎯` uncertain (= label-target)
- `📋 prüfbar` ready-for-review
- `●` live
- `🔄` training läuft

## 7. Banners

Status / info banners across the top of `/recordings` and `/learning`:

```html
<div style='background: {color}22; border-left: 4px solid {color};
            padding: 10px 14px; margin: 0 0 14px;
            border-radius: 4px; font-size: .95em'>
  {emoji} <b>{headline}</b> {body}
</div>
```

The `{color}22` notation = base color + `22` hex = ~13% alpha = tinted background. Border-left in the full color. Used for:
- 🔄 blue (`#3498db`) = info / training läuft
- ⚠ amber (`#f39c12`) = warning
- ⚠ red (`#e74c3c`) = error / urgent

## 8. Inline-style philosophy

This codebase is intentionally **inline-style heavy** for short HTML snippets generated server-side. Reasons:
- Single-file deploy (no CSS asset pipeline)
- Hot-reload via mtime watch — one file = one restart
- Easier to grep ("where is this color used?")
- No naming overhead for one-off styling

Rules:
- **Variables** (`var(--*)`) in inline-style are OK and encouraged — keeps theme consistency
- **Hex literals** in inline-style only for status semantics from §6
- **Typography sizes** as `em` / `.85em` etc, not `px`
- **Don't** introduce a global `<style>` for one-off use; just inline it
- **Do** factor into a class when used 3+ times

## 9. Component composition example

Good (`/learning` Show-Lücken row):
```html
<tr>
  <td><b>Charmed - Zauberhafte Hexen</b></td>
  <td>sixx</td>
  <td>15</td>
  <td><b style='color:#f39c12'>1</b></td>
  <td title='hint' style='white-space:nowrap'>🔓 drift_unlock</td>
  <td style='white-space:nowrap'>
    <a href='...' class='pill' style='background:#f39c12;color:#fff;
        text-decoration:none;padding:3px 8px;font-size:.85em;
        white-space:nowrap'>➜ prüfen</a>
  </td>
</tr>
```

Notice: `.pill` class for shape + accessibility, inline override for variant color matching the row's semantic state, `nowrap` to prevent button text wrapping, smaller padding (`3px 8px`) than default for inline-table use.

## 10. Forbidden patterns

Don't do these — they've all been removed at some point:

- ❌ Hard-coded `background:#1a1a1a` or `color:#fff` without dark/light awareness → use `var(--bg)` / `var(--fg)`
- ❌ `display:none` for buttons that toggle on/off based on state → use `opacity:0; pointer-events:none` so layout doesn't shift
- ❌ JavaScript-driven theme switcher / "dark mode toggle" → respect `prefers-color-scheme` only, OS decides
- ❌ Pixel-precise mobile layouts → mobile gets horizontal-scroll wrappers, not separate breakpoints
- ❌ External CSS frameworks (Bootstrap / Tailwind / etc) → keep self-contained
- ❌ Loading spinners that block interaction → all loads should fade/replace inline
