// hls-gateway-go — strangler-fig front for the ~22.7k-line Flask service.py.
//
// It listens where Flask used to (Caddy points here unchanged), serves the
// routes that have been migrated to Go NATIVELY, and reverse-proxies every
// other path straight to Flask. Migrating a route = add a native handler
// here; until then nothing changes for that route. No big-bang: this stays
// transparent for unmigrated paths.
//
//	Caddy(:8443) → hls-gateway-go(:8080) → Flask(:8081, unmigrated routes)
//	                                     → tv-receiver(:9983) for live/EPG/DVR
//
// Run side-by-side first (e.g. -addr :8090 -flask :8080) to verify
// transparency before the cutover (move Flask to :8081, take :8080).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

type server struct {
	flask      *url.URL
	tvReceiver string
	proxy      *httputil.ReverseProxy
	client     *http.Client
}

func main() {
	addr := flag.String("addr", ":8080", "listen address (where Caddy points)")
	flaskURL := flag.String("flask", "http://127.0.0.1:8081",
		"Flask backend for unmigrated routes")
	tvReceiver := flag.String("tv-receiver", "http://127.0.0.1:9983",
		"tv-receiver base (live TV / EPG / DVR / channels)")
	tvRecorder := flag.String("tv-recorder", "http://127.0.0.1:9984",
		"tv-recorder base (recordings backend / detect orchestration)")
	flag.Parse()

	flask, err := url.Parse(*flaskURL)
	if err != nil {
		log.Fatalf("bad -flask url: %v", err)
	}
	recorder, err := url.Parse(*tvRecorder)
	if err != nil {
		log.Fatalf("bad -tv-recorder url: %v", err)
	}

	s := &server{
		flask:      flask,
		tvReceiver: *tvReceiver,
		proxy:      httputil.NewSingleHostReverseProxy(flask),
		client:     &http.Client{Timeout: 5 * time.Second},
	}
	recProxy := httputil.NewSingleHostReverseProxy(recorder)

	mux := http.NewServeMux()
	// --- Routes migrated to Go (served natively) ---
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	// --- Routes migrated to tv-recorder (slice 1: stateless detect-asset
	//     file-serving + recording-uuids). Byte-faithful to Flask, verified
	//     side-by-side. Rollback = delete these lines + redeploy. ---
	mux.Handle("GET /api/internal/detect-models/", recProxy)
	mux.Handle("GET /api/internal/detect-logo/", recProxy)
	mux.Handle("GET /api/internal/detect-logo-cnn/", recProxy)
	mux.Handle("GET /api/internal/detect-bumpers/", recProxy)
	mux.Handle("GET /api/internal/detect-bumper/", recProxy)
	mux.Handle("GET /api/internal/recording-uuids", recProxy)
	// detect-config — the Mac tv-detect daemon's per-job config fetch.
	mux.Handle("GET /api/internal/detect-config/{uuid}", recProxy)
	// slice 2 — HLS-remux job queue (Mac offload). hls-segment is PUT,
	// hls-done is POST, so route the whole prefix (any method) to tv-recorder.
	mux.Handle("GET /api/internal/hls-pending", recProxy)
	mux.Handle("/api/internal/hls-segment/", recProxy)
	mux.Handle("/api/internal/hls-done/", recProxy)
	// slice 2b — thumbnail extraction offload (thumbs-uploaded is POST)
	mux.Handle("GET /api/internal/thumbs-pending", recProxy)
	mux.Handle("/api/internal/thumbs-uploaded/", recProxy)
	// slice 2c — detect job queue (the last poller; tv-recorder now owns the
	// _daemon_last_poll + _detect_running badge state via files Flask reads)
	mux.Handle("GET /api/internal/detect-pending", recProxy)
	mux.Handle("GET /api/internal/detect-pending-low", recProxy)
	mux.Handle("/api/internal/detect-started/", recProxy)
	mux.Handle("/api/internal/detect-give-up/", recProxy)
	mux.Handle("/api/internal/cutlist-uploaded/", recProxy)
	// housekeeping — disk reclaim the Mac daemon triggers on its GC/prefetch loop
	mux.Handle("/api/internal/drop-pi-source/", recProxy)
	mux.Handle("/api/internal/cleanup-orphans", recProxy)
	// slice 6a — training orchestration (Mac trainer in/outputs)
	mux.Handle("GET /api/internal/training-snapshot", recProxy)
	mux.Handle("/api/internal/training-active", recProxy)
	mux.Handle("POST /api/internal/training-duration", recProxy)
	mux.Handle("POST /api/internal/head-bundle", recProxy)
	mux.Handle("POST /api/internal/snapshot-per-show-iou", recProxy)
	// slice 3a — VOD playlist (recording/<uuid>/index.m3u8). Segments are
	// Caddy-static (@rec_ts); /recording/<uuid>/{ads,source,...} stay Flask.
	mux.Handle("GET /recording/{uuid}/index.m3u8", recProxy)
	// slice 3b — original .ts source (Mac fetch for remux/detect/thumbs)
	mux.Handle("GET /recording/{uuid}/source", recProxy)
	// slice 3d — player-loader progress poll (kicks off remux/detect/thumbs)
	mux.Handle("GET /recording/{uuid}/progress", recProxy)
	// slice 4 — ad-block markers (GET)
	mux.Handle("GET /recording/{uuid}/ads", recProxy)
	// slice 4b — ads edit (writes ads_user.json + drops spot fingerprints,
	// both now owned by tv-recorder)
	mux.Handle("POST /api/recording/{uuid}/ads/edit", recProxy)
	// slice 5 — whisper full-text search (FTS5, pure-Go sqlite in tv-recorder)
	mux.Handle("GET /api/search", recProxy)
	mux.Handle("POST /api/internal/whisper-reindex", recProxy)
	// slice 5b — spot-fingerprint endpoints (clustering + extraction). The
	// extraction worker is NOT enabled (matches Flask, whose worker is dead
	// code); these serve/cluster the shared .spot-fingerprints.sqlite. Proven
	// byte-identical extraction + identical clustering partition.
	mux.Handle("GET /api/internal/spot-fp/cluster-anchored/", recProxy)
	mux.Handle("GET /api/internal/spot-fp/queue", recProxy)
	mux.Handle("GET /api/internal/spot-fingerprints/families", recProxy)
	mux.Handle("POST /api/internal/spot-fp/upload", recProxy)
	mux.Handle("POST /api/internal/spot-fingerprints/rebuild", recProxy)
	mux.Handle("POST /api/internal/spot-fingerprints/backfill-dhashes", recProxy)
	// slice 6c — read-only learning-dashboard metrics. The scheduler-mutating
	// /api/learning/* routes (auto-schedule-*, plan, fingerprint-scan/validate)
	// stay on Flask via the catch-all below — a later (6d) slice.
	mux.Handle("GET /api/learning/summary", recProxy)
	mux.Handle("GET /api/learning/deletion-candidates", recProxy)
	// slice 6d — show-fingerprint auto-confirm (scan + leave-one-out validate).
	// EPG/DVR-mutating learning routes (plan, auto-schedule-*) stay on Flask.
	mux.Handle("POST /api/learning/fingerprint-scan", recProxy)
	mux.Handle("POST /api/learning/fingerprint-validate", recProxy)
	// slice 6e — file-based auto-scheduler state (log read + pause toggle).
	// auto-schedule-trigger + plan stay on Flask (EPG/DVR-mutating + the
	// recommendations engine) via the catch-all below.
	mux.Handle("GET /api/learning/auto-schedule-log", recProxy)
	mux.Handle("POST /api/learning/auto-schedule-pause", recProxy)
	// slice 4a — auto-confirm verdict (read-only) + status/pause. The
	// background apply-loop stays on Flask until slice 4b; the pause marker
	// tv-recorder writes here is the file bridge Flask's loop reads.
	mux.Handle("GET /api/internal/auto-confirm/status", recProxy)
	mux.Handle("POST /api/internal/auto-confirm/pause", recProxy)
	mux.Handle("GET /api/internal/auto-confirm-bulk", recProxy)
	mux.Handle("GET /api/internal/auto-confirm/{uuid}", recProxy)
	// slice 6f — external-app recordings library (list + single). Playability
	// filter + flat app schema. /api/recording/<uuid>/* (singular) stays Flask.
	mux.Handle("GET /api/recordings", recProxy)
	mux.Handle("GET /api/recordings/{uuid}", recProxy)
	// slice 6g — playback-state writers (resume position + watched flag).
	mux.Handle("POST /api/recording/{uuid}/playposition", recProxy)
	mux.Handle("POST /api/recording/{uuid}/watched", recProxy)
	// slice 6h — recommendation scheduler (plan) + auto-scheduler trigger.
	// The daily auto-schedule loop also moved to tv-recorder (gateway env
	// AUTO_SCHED_LOOP_OWNER=recorder disables Flask's loop).
	mux.Handle("POST /api/learning/plan", recProxy)
	mux.Handle("POST /api/learning/auto-schedule-trigger", recProxy)
	// slice 3a/3b — recording-management: thumb/poster serves + ads_user.json
	// review writers (mark-reviewed/skip-event/auto-confirm-undo).
	mux.Handle("GET /recording/{uuid}/thumbs.json", recProxy)
	mux.Handle("GET /recording/{uuid}/thumbs/{fname}", recProxy)
	mux.Handle("GET /recording/{uuid}/poster.jpg", recProxy)
	mux.Handle("POST /api/recording/{uuid}/mark-reviewed", recProxy)
	mux.Handle("POST /api/recording/{uuid}/skip-event", recProxy)
	mux.Handle("POST /api/recording/{uuid}/auto-confirm-undo", recProxy)
	// slice 3c — redetect (re-queue detect) + show-start (drift marker).
	mux.Handle("POST /api/recording/{uuid}/redetect", recProxy)
	mux.Handle("POST /api/recording/{uuid}/show-start", recProxy)
	// slice 3e — bumper-capture (ffmpeg frame extract + channel re-detect)
	mux.Handle("POST /api/recording/{uuid}/bumper-capture", recProxy)
	// slice 3f — trim (lossless ffmpeg cut, replaces source) — DESTRUCTIVE
	mux.Handle("POST /api/recording/{uuid}/trim", recProxy)
	// --- Everything else still belongs to Flask (incl. /api/channels,
	//     which applies the favourites filter — a later slice) ---
	mux.HandleFunc("/", s.proxy.ServeHTTP)

	log.Printf("hls-gateway-go: listening on %s", *addr)
	log.Printf("  native: GET /healthz")
	log.Printf("  tv-recorder: detect-models/logo(-cnn)/bumper(s) + recording-uuids → %s", recorder)
	log.Printf("  proxy : everything else → %s", flask)
	log.Printf("  tv-receiver: %s", *tvReceiver)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("listen: %v", err)
	}
}

// healthzResp is the exact shape Flask's /healthz returns (field order +
// null on backend failure), so this is a byte-faithful drop-in. service stays
// "hls-gateway" (not "-go") on purpose — clients shouldn't notice the swap.
type healthzResp struct {
	Ok          bool   `json:"ok"`
	Service     string `json:"service"`
	Backend     string `json:"backend"`
	BackendOk   bool   `json:"backend_ok"`
	TunerSlots  *int   `json:"tuner_slots"`
	ActiveSlots *int   `json:"active_slots"`
}

// handleHealthz mirrors Flask's /healthz: a fast readiness probe gated on
// tv-receiver. 2s probe of the backend's /healthz; reports slot counts.
// 200 + ok:true when the backend answers, else 503 + ok:false + null counts.
func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	out := healthzResp{Service: "hls-gateway", Backend: "tv-receiver"}
	req, _ := http.NewRequestWithContext(ctx, "GET", s.tvReceiver+"/healthz", nil)
	if resp, err := s.client.Do(req); err == nil {
		var data struct {
			Ok    *bool `json:"ok"`
			Slots []struct {
				Consumers int `json:"consumers"`
			} `json:"slots"`
		}
		if json.NewDecoder(resp.Body).Decode(&data) == nil {
			out.BackendOk = data.Ok == nil || *data.Ok // default true if absent
			n := len(data.Slots)
			a := 0
			for _, sl := range data.Slots {
				if sl.Consumers > 0 {
					a++
				}
			}
			out.TunerSlots, out.ActiveSlots = &n, &a
		}
		resp.Body.Close()
	}
	out.Ok = out.BackendOk

	w.Header().Set("Content-Type", "application/json")
	if !out.BackendOk {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(out)
}
