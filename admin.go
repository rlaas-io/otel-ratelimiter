// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
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
	mux.HandleFunc("GET /config/policies", adminPolicyConfig(cfg))
	mux.HandleFunc("POST /reload", adminReload)
	mux.HandleFunc("GET /metrics", adminMetrics)
	// /ui/ serves the embedded static admin console — no token auth required.
	// The UI itself contains no secrets; the user enters their token interactively.
	// All API calls made from the UI still require the token (enforced by the
	// endpoints above).
	mux.Handle("GET /ui/", http.StripPrefix("/ui", adminUIHandler()))
	mux.HandleFunc("GET /ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusMovedPermanently)
	})
	handler := adminCORSMiddleware(cfg, adminAuthMiddleware(cfg, mux))

	srv := &http.Server{
		Addr:         cfg.AdminAddr,
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  30 * time.Second,
	}
	if tlsCfg, err := buildAdminTLSConfig(cfg); err != nil {
		logger.Error("Admin TLS configuration error; admin API disabled", zap.Error(err))
		return
	} else if tlsCfg != nil {
		srv.TLSConfig = tlsCfg
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
		scheme := "http"
		if cfg.AdminTLSCertFile != "" {
			scheme = "https"
		}
		authEnabled := cfg.AdminAuthToken != ""
		logger.Info("RLAAS admin HTTP server started",
			zap.String("scheme", scheme),
			zap.String("addr", cfg.AdminAddr),
			zap.Bool("auth_enabled", authEnabled),
			zap.String("ui", scheme+"://"+cfg.AdminAddr+"/ui/"),
			zap.String("endpoints", "GET /ui/  GET /health  GET /stats  GET /config  GET /config/policies  POST /reload  GET /metrics"),
		)
		var serveErr error
		if cfg.AdminTLSCertFile != "" {
			serveErr = srv.ServeTLS(ln, cfg.AdminTLSCertFile, cfg.AdminTLSKeyFile)
		} else {
			serveErr = srv.Serve(ln)
		}
		if serveErr != nil && serveErr != http.ErrServerClosed {
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

func adminAuthMiddleware(cfg *Config, next http.Handler) http.Handler {
	token := strings.TrimSpace(cfg.AdminAuthToken)
	if token == "" {
		return next
	}

	headerName := strings.TrimSpace(cfg.AdminTokenHeader)
	if headerName == "" {
		headerName = "Authorization"
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The admin UI static files are always public — they contain no secrets
		// and the user enters their token interactively inside the UI. All API
		// calls made from the UI still go through auth above.
		if r.URL.Path == "/ui" || strings.HasPrefix(r.URL.Path, "/ui/") {
			next.ServeHTTP(w, r)
			return
		}
		raw := strings.TrimSpace(r.Header.Get(headerName))
		if strings.EqualFold(headerName, "Authorization") && strings.HasPrefix(strings.ToLower(raw), "bearer ") {
			raw = strings.TrimSpace(raw[7:])
		}
		if raw != token {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func adminCORSMiddleware(cfg *Config, next http.Handler) http.Handler {
	allowed := make([]string, 0, len(cfg.AdminCORSAllowedOrigins))
	for _, v := range cfg.AdminCORSAllowedOrigins {
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			allowed = append(allowed, trimmed)
		}
	}
	if len(allowed) == 0 {
		return next
	}

	allows := func(origin string) bool {
		for _, candidate := range allowed {
			if candidate == "*" || candidate == origin {
				return true
			}
		}
		return false
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin != "" && allows(origin) {
			if containsWildcard(allowed) {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Add("Vary", "Origin")
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type,X-Admin-Token")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func containsWildcard(values []string) bool {
	for _, v := range values {
		if v == "*" {
			return true
		}
	}
	return false
}

func buildAdminTLSConfig(cfg *Config) (*tls.Config, error) {
	if cfg.AdminTLSCertFile == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if strings.TrimSpace(cfg.AdminTLSClientCAFile) == "" {
		return tlsCfg, nil
	}

	caPEM, err := os.ReadFile(cfg.AdminTLSClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("read admin_tls_client_ca_file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse admin_tls_client_ca_file: no valid CA certs found")
	}
	tlsCfg.ClientCAs = pool
	tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	return tlsCfg, nil
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
			PolicyFile              string   `json:"policy_file"`
			PoliciesInline          bool     `json:"policies_inline"` // true/false, never the full content
			FailOpen                bool     `json:"fail_open"`
			CacheTTL                string   `json:"cache_ttl"`
			WatchPolicies           bool     `json:"watch_policies"`
			WatchInterval           string   `json:"watch_interval"`
			KeyPrefix               string   `json:"key_prefix"`
			OrgID                   string   `json:"org_id"`
			TenantID                string   `json:"tenant_id"`
			Application             string   `json:"application"`
			Environment             string   `json:"environment"`
			ServiceExpr             string   `json:"service_expr"`
			OrgIDExpr               string   `json:"org_id_expr"`
			TenantIDExpr            string   `json:"tenant_id_expr"`
			ApplicationExpr         string   `json:"application_expr"`
			EnvironmentExpr         string   `json:"environment_expr"`
			MaxBatchSize            int      `json:"max_batch_size"`
			AdminAddr               string   `json:"admin_addr"`
			AdminAuthEnabled        bool     `json:"admin_auth_enabled"`
			AdminTokenHeader        string   `json:"admin_token_header"`
			AdminTLSEnabled         bool     `json:"admin_tls_enabled"`
			AdminTLSClientCA        bool     `json:"admin_tls_client_ca"`
			AdminCORSAllowedOrigins []string `json:"admin_cors_allowed_origins"`
		}
		tokenHeader := cfg.AdminTokenHeader
		if tokenHeader == "" {
			tokenHeader = "Authorization"
		}
		writeJSON(w, http.StatusOK, safeConfig{
			PolicyFile:              cfg.PolicyFile,
			PoliciesInline:          cfg.PoliciesInline != "",
			FailOpen:                cfg.FailOpen,
			CacheTTL:                cfg.CacheTTL.String(),
			WatchPolicies:           cfg.WatchPolicies,
			WatchInterval:           cfg.WatchInterval.String(),
			KeyPrefix:               cfg.KeyPrefix,
			OrgID:                   cfg.OrgID,
			TenantID:                cfg.TenantID,
			Application:             cfg.Application,
			Environment:             cfg.Environment,
			ServiceExpr:             cfg.ServiceExpr,
			OrgIDExpr:               cfg.OrgIDExpr,
			TenantIDExpr:            cfg.TenantIDExpr,
			ApplicationExpr:         cfg.ApplicationExpr,
			EnvironmentExpr:         cfg.EnvironmentExpr,
			MaxBatchSize:            cfg.MaxBatchSize,
			AdminAddr:               cfg.AdminAddr,
			AdminAuthEnabled:        cfg.AdminAuthToken != "",
			AdminTokenHeader:        tokenHeader,
			AdminTLSEnabled:         cfg.AdminTLSCertFile != "",
			AdminTLSClientCA:        cfg.AdminTLSClientCAFile != "",
			AdminCORSAllowedOrigins: cfg.AdminCORSAllowedOrigins,
		})
	}
}

// GET /config/policies — active policy configuration and metadata.
func adminPolicyConfig(cfg *Config) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		payload := []byte(cfg.PoliciesInline)
		source := "inline"
		lastModified := ""

		if cfg.PolicyFile != "" {
			data, err := os.ReadFile(cfg.PolicyFile)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
				return
			}
			payload = data
			source = "file"
			if info, err := os.Stat(cfg.PolicyFile); err == nil {
				lastModified = info.ModTime().UTC().Format(time.RFC3339)
			}
		}

		type response struct {
			Source        string `json:"source"`
			PolicyFile    string `json:"policy_file,omitempty"`
			PolicyCount   int    `json:"policy_count"`
			ContentSHA256 string `json:"content_sha256"`
			LastModified  string `json:"last_modified,omitempty"`
			Policies      any    `json:"policies"`
		}

		hash := sha256.Sum256(payload)
		count := 0
		var parsed any
		if err := json.Unmarshal(payload, &parsed); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "policy payload is not valid JSON", "details": err.Error()})
			return
		}
		if arr, ok := parsed.([]any); ok {
			count = len(arr)
		}

		writeJSON(w, http.StatusOK, response{
			Source:        source,
			PolicyFile:    cfg.PolicyFile,
			PolicyCount:   count,
			ContentSHA256: hex.EncodeToString(hash[:]),
			LastModified:  lastModified,
			Policies:      parsed,
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
