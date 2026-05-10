// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/component"
)

var _ component.Config = (*Config)(nil)

// Config defines the configuration for the RLAAS Rate Limiter processor.
//
// # Quick-start
//
//	processors:
//	  ratelimiter:
//	    policy_file: /etc/otel/policies.json
//	    fail_open: true
//	    watch_policies: true   # hot-reload on file change
//	    admin_addr: ":8888"    # optional admin HTTP API
//
// See https://rlaas-io.github.io/otel-ratelimiter/configuration for the full
// configuration reference.
type Config struct {
	// PolicyFile is the path to a RLAAS policy JSON file on disk.
	// Mutually exclusive with PoliciesInline.
	PolicyFile string `mapstructure:"policy_file"`

	// PoliciesInline embeds the policy JSON directly in the collector YAML.
	// Ideal for Kubernetes ConfigMap / Helm values deployments.
	// Must be a valid JSON array of RLAAS policy objects.
	// Mutually exclusive with PolicyFile.
	//
	// Example:
	//   policies_inline: |
	//     [{"policy_id":"limit-logs","algorithm":{"type":"token_bucket",
	//       "limit":1000,"window":"1s","burst":1500},"scope":{"signal_type":"log"},
	//       "action":"drop","enforcement_mode":"enforce","enabled":true}]
	PoliciesInline string `mapstructure:"policies_inline"`

	// FailOpen controls behaviour when the RLAAS engine returns an error.
	//   true  (default) — allow the record through (safe for production).
	//   false           — drop the record (strict / compliance mode).
	FailOpen bool `mapstructure:"fail_open"`

	// CacheTTL controls how long policies are cached inside the RLAAS engine
	// before being re-read from disk on the next request. Default: 30s.
	// Set to a lower value for more responsive (but costlier) policy refresh.
	CacheTTL time.Duration `mapstructure:"cache_ttl"`

	// WatchPolicies enables real-time file system watching of the policy file.
	// When true, policy changes are detected and applied immediately (within
	// milliseconds for local file systems; within WatchInterval for network mounts).
	// Only applies to PolicyFile; inline policies require a collector restart.
	// Default: false.
	WatchPolicies bool `mapstructure:"watch_policies"`

	// WatchInterval is the polling fallback period used when native fsnotify
	// events are unreliable (NFS, CIFS, Docker bind-mounts, etc.).
	// Default: 15s. Only effective when WatchPolicies is true.
	WatchInterval time.Duration `mapstructure:"watch_interval"`

	// KeyPrefix namespaces all rate-limit counter keys inside the RLAAS engine.
	// Use separate prefixes when running multiple ratelimiter processor instances
	// in the same collector so their counters do not collide.
	// Default: "otel".
	KeyPrefix string `mapstructure:"key_prefix"`

	// OrgID is the default Organization ID injected into every RLAAS request
	// context when the telemetry record does not supply one.
	OrgID string `mapstructure:"org_id"`

	// TenantID is the default Tenant ID injected into every RLAAS request context.
	TenantID string `mapstructure:"tenant_id"`

	// Application is the default application name used for policy scoping.
	Application string `mapstructure:"application"`

	// Environment is the deployment environment (e.g. "production", "staging").
	Environment string `mapstructure:"environment"`

	// ServiceExpr extracts service from telemetry attributes using a fallback
	// expression. Use "||" to try multiple candidates.
	//
	// Supported tokens:
	//   resource.attributes.<key>
	//   attributes.<key>
	//   <key> (resolved from merged attributes)
	//
	// Example:
	//   service_expr: "resource.attributes.service.name || attributes.service || k8s.deployment.name"
	ServiceExpr string `mapstructure:"service_expr"`

	// OrgIDExpr extracts org ID from telemetry attributes before falling back to
	// org_id default.
	OrgIDExpr string `mapstructure:"org_id_expr"`

	// TenantIDExpr extracts tenant ID from telemetry attributes before falling
	// back to tenant_id default.
	TenantIDExpr string `mapstructure:"tenant_id_expr"`

	// ApplicationExpr extracts application from telemetry attributes before
	// falling back to application default.
	ApplicationExpr string `mapstructure:"application_expr"`

	// EnvironmentExpr extracts environment from telemetry attributes before
	// falling back to environment default.
	EnvironmentExpr string `mapstructure:"environment_expr"`

	// MaxBatchSize caps how many records are evaluated per batch invocation.
	// Records beyond this limit are dropped before reaching RLAAS (reason:
	// "batch_size_exceeded"). Use this as a safety valve against burst floods.
	// 0 = unlimited (default).
	MaxBatchSize int `mapstructure:"max_batch_size"`

	// AdminAddr is the TCP address for the built-in admin HTTP server.
	// Examples: ":8888"  "127.0.0.1:9090"
	//
	// Endpoints:
	//   GET  /health  — liveness probe
	//   GET  /stats   — per-signal allowed/denied/shadow/error counters (JSON)
	//   GET  /config  — active configuration (sanitised, no secrets)
	//   GET  /config/policies — active policy payload + metadata
	//   POST /reload  — force immediate policy reload on all engines
	//   GET  /metrics — Prometheus/OpenMetrics text format
	//
	// Security can be enabled with admin_auth_token and admin_tls_* fields.
	//
	// Leave empty to disable (default).
	AdminAddr string `mapstructure:"admin_addr"`

	// AdminAuthToken requires callers to provide a token for all admin endpoints.
	// If empty, admin endpoints are unauthenticated.
	//
	// For Authorization header, accepted values are either:
	//   Authorization: Bearer <token>
	//   Authorization: <token>
	AdminAuthToken string `mapstructure:"admin_auth_token"`

	// AdminTokenHeader is the HTTP header used for admin token auth.
	// Default: "Authorization".
	AdminTokenHeader string `mapstructure:"admin_token_header"`

	// AdminCORSAllowedOrigins lists allowed Origins for browser access to admin
	// endpoints. Example: ["https://rlaas-io.github.io"].
	// Use ["*"] to allow any origin.
	AdminCORSAllowedOrigins []string `mapstructure:"admin_cors_allowed_origins"`

	// AdminTLSCertFile/AdminTLSKeyFile enable HTTPS for the admin server.
	// Both must be provided together.
	AdminTLSCertFile string `mapstructure:"admin_tls_cert_file"`
	AdminTLSKeyFile  string `mapstructure:"admin_tls_key_file"`

	// AdminTLSClientCAFile enables mTLS when set; clients must present a cert
	// signed by this CA.
	AdminTLSClientCAFile string `mapstructure:"admin_tls_client_ca_file"`
}

// Validate checks all configuration fields for consistency errors.
func (cfg *Config) Validate() error {
	if cfg.PolicyFile == "" && cfg.PoliciesInline == "" {
		return errors.New("either policy_file or policies_inline is required")
	}
	if cfg.PolicyFile != "" && cfg.PoliciesInline != "" {
		return errors.New("policy_file and policies_inline are mutually exclusive; use one or the other")
	}
	if cfg.CacheTTL < 0 {
		return errors.New("cache_ttl must not be negative")
	}
	if cfg.WatchInterval < 0 {
		return errors.New("watch_interval must not be negative")
	}
	if cfg.MaxBatchSize < 0 {
		return errors.New("max_batch_size must not be negative")
	}
	if (cfg.AdminTLSCertFile == "") != (cfg.AdminTLSKeyFile == "") {
		return errors.New("admin_tls_cert_file and admin_tls_key_file must be set together")
	}
	return nil
}
