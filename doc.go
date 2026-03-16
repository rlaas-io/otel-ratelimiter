// Package ratelimiterprocessor provides an OpenTelemetry Collector processor
// that applies configurable rate limiting to logs, traces, and metrics pipelines.
//
// It delegates all rate limiting decisions to the RLAAS (Rate Limiting As A Service)
// engine (https://github.com/suresh-p26/RLAAS), which serves as the core decision-making
// heart of this processor. RLAAS provides:
//
//   - 7 rate limiting algorithms: token_bucket, sliding_window_log, sliding_window_counter,
//     fixed_window, leaky_bucket, concurrency, and quota
//   - 8 decision actions: allow, deny, delay, sample, drop, downgrade, drop_low_priority,
//     and shadow_only
//   - Policy-based matching with priority, scope, rollout, and enforcement modes
//   - Shadow mode for dry-run evaluation without dropping records
//
// Each telemetry record (log, span, or metric) is converted to a RLAAS RequestContext
// and evaluated against the configured policies. Policies are defined in a standard
// RLAAS policy JSON file, keeping configuration centralized and reusable.
//
// Usage:
//
//	processors:
//	  ratelimiter:
//	    policy_file: /etc/otel/policies.json
//	    fail_open: true
//	    org_id: "my-org"
package ratelimiterprocessor
