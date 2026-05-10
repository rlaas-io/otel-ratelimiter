// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

// adminRegistration records per-processor state accessible from the admin API.
type adminRegistration struct {
	signal      string
	engine      *engine
	getReceived func() int64
	getDropped  func() int64
}

// globalAdmin is the package-level shared admin server.
// One HTTP server is started per process regardless of how many processor
// instances are configured; all instances share it.
var globalAdmin = &adminState{} //nolint:gochecknoglobals

type adminState struct {
	mu         sync.Mutex
	server     *http.Server
	startedAt  time.Time
	processors map[string]*adminRegistration // keyed by signal type
}

// registerWithAdmin starts the admin HTTP server (once per address) and
// registers this processor's engine so it appears in /stats and /reload.
func registerWithAdmin(
	cfg *Config,
	eng *engine,
	signal string,
	getReceived, getDropped func() int64,
	logger *zap.Logger,
) {
	if cfg.AdminAddr == "" {
		return
	}

	globalAdmin.mu.Lock()
	defer globalAdmin.mu.Unlock()

	if globalAdmin.processors == nil {
		globalAdmin.processors = make(map[string]*adminRegistration)
	}
	globalAdmin.processors[signal] = &adminRegistration{
		signal:      signal,
		engine:      eng,
		getReceived: getReceived,
		getDropped:  getDropped,
	}

	if globalAdmin.server != nil {
		return // already running
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", adminHealth)
	mux.HandleFunc("GET /stats", adminStats)
	mux.HandleFunc("GET /config", adminConfig(cfg))
	mux.HandleFunc("POST /reload", adminReload)
	mux.HandleFunc("GET /metrics", adminMetrics)

	srv := &http.Server{
		Addr:         cfg.AdminAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	ln, err := net.Listen("tcp", cfg.AdminAddr)
	if err != nil {
		logger.Error("Admin server failed to bind; admin API disabled",
			zap.String("addr", cfg.AdminAddr), zap.Error(err))
		return
	}

	globalAdmin.server = srv
	globalAdmin.startedAt = time.Now()

	go func() {
		logger.Info("RLAAS admin HTTP server started",
			zap.String("addr", cfg.AdminAddr),
			zap.String("endpoints", "GET /health  GET /stats  GET /config  POST /reload  GET /metrics"),
		)
		if serveErr := srv.Serve(ln); serveErr != nil && serveErr != http.ErrServerClosed {
			logger.Error("Admin server error", zap.Error(serveErr))
		}
	}()
}

// deregisterFromAdmin removes a processor's engine from the admin registry.
func deregisterFromAdmin(signal string) {
	globalAdmin.mu.Lock()
	defer globalAdmin.mu.Unlock()
	if globalAdmin.processors != nil {
		delete(globalAdmin.processors, signal)
	}
}

// writeJSON serialises v as JSON and writes it with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// GET /health — liveness probe.
func adminHealth(w http.ResponseWriter, _ *http.Request) {
	globalAdmin.mu.Lock()
	uptime := time.Since(globalAdmin.startedAt).Seconds()
	globalAdmin.mu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "ok",
		"uptime_seconds": int(uptime),
	})
}

// GET /stats — per-signal counters for all registered processors.
func adminStats(w http.ResponseWriter, _ *http.Request) {
	globalAdmin.mu.Lock()
	regs := snapshotRegs()
	globalAdmin.mu.Unlock()

	type signalStats struct {
		Received int64 `json:"received"`
		Dropped  int64 `json:"dropped"`
		Allowed  int64 `json:"allowed"`
		Denied   int64 `json:"denied"`
		Shadow   int64 `json:"shadow"`
		Errors   int64 `json:"errors"`
		Reloads  int64 `json:"reloads"`
	}

	result := map[string]any{}
	for signal, reg := range regs {
		allowed, denied, shadow, errors, reloads := reg.engine.Stats()
		result[signal] = signalStats{
			Received: reg.getReceived(),
			Dropped:  reg.getDropped(),
			Allowed:  allowed,
			Denied:   denied,
			Shadow:   shadow,
			Errors:   errors,
			Reloads:  reloads,
		}
	}
	writeJSON(w, http.StatusOK, result)
}

// GET /config — current processor configuration (sanitised; no secrets).
func adminConfig(cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		type safeConfig struct {
			PolicyFile     string `json:"policy_file"`
			PoliciesInline bool   `json:"policies_inline"` // true/false, never the full content
			FailOpen       bool   `json:"fail_open"`
			CacheTTL       string `json:"cache_ttl"`
			WatchPolicies  bool   `json:"watch_policies"`
			WatchInterval  string `json:"watch_interval"`
			KeyPrefix      string `json:"key_prefix"`
			OrgID          string `json:"org_id"`
			TenantID       string `json:"tenant_id"`
			Application    string `json:"application"`
			Environment    string `json:"environment"`
			MaxBatchSize   int    `json:"max_batch_size"`
			AdminAddr      string `json:"admin_addr"`
		}
		writeJSON(w, http.StatusOK, safeConfig{
			PolicyFile:     cfg.PolicyFile,
			PoliciesInline: cfg.PoliciesInline != "",
			FailOpen:       cfg.FailOpen,
			CacheTTL:       cfg.CacheTTL.String(),
			WatchPolicies:  cfg.WatchPolicies,
			WatchInterval:  cfg.WatchInterval.String(),
			KeyPrefix:      cfg.KeyPrefix,
			OrgID:          cfg.OrgID,
			TenantID:       cfg.TenantID,
			Application:    cfg.Application,
			Environment:    cfg.Environment,
			MaxBatchSize:   cfg.MaxBatchSize,
			AdminAddr:      cfg.AdminAddr,
		})
	}
}

// POST /reload — force an immediate policy reload on all registered engines.
func adminReload(w http.ResponseWriter, _ *http.Request) {
	globalAdmin.mu.Lock()
	regs := snapshotRegs()
	globalAdmin.mu.Unlock()

	for _, reg := range regs {
		reg.engine.Reload()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "reloaded",
		"engines": len(regs),
	})
}

// GET /metrics — Prometheus text format (OpenMetrics-compatible).
// Integrate with Prometheus by pointing a scrape job at this endpoint.
func adminMetrics(w http.ResponseWriter, _ *http.Request) {
	globalAdmin.mu.Lock()
	regs := snapshotRegs()
	globalAdmin.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	type metricDef struct{ help, typ, name string }
	defs := []metricDef{
		{"Total records received by the rate limiter", "counter", "ratelimiter_records_received_total"},
		{"Total records dropped (rate-limited, error, batch limit)", "counter", "ratelimiter_records_dropped_total"},
		{"Total records allowed through", "counter", "ratelimiter_records_allowed_total"},
		{"Total records denied by a rate limit policy", "counter", "ratelimiter_records_denied_total"},
		{"Total records observed in shadow mode", "counter", "ratelimiter_records_shadow_total"},
		{"Total RLAAS evaluation errors", "counter", "ratelimiter_evaluate_errors_total"},
		{"Total policy reload events", "counter", "ratelimiter_policy_reloads_total"},
	}

	for i, def := range defs {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", def.name, def.help, def.name, def.typ)
		for signal, reg := range regs {
			allowed, denied, shadow, errors, reloads := reg.engine.Stats()
			vals := []int64{reg.getReceived(), reg.getDropped(), allowed, denied, shadow, errors, reloads}
			fmt.Fprintf(w, "%s{signal=%q} %d\n", def.name, signal, vals[i])
		}
	}
}

// snapshotRegs returns a copy of the processors map. Must be called with mu held.
func snapshotRegs() map[string]*adminRegistration {
	cp := make(map[string]*adminRegistration, len(globalAdmin.processors))
	for k, v := range globalAdmin.processors {
		cp[k] = v
	}
	return cp
}
