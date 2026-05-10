// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rlaas-io/rlaas/pkg/model"
	"github.com/rlaas-io/rlaas/pkg/rlaas"
	"go.uber.org/zap"
)

// engine wraps a RLAAS client with fail-open/fail-closed semantics, thread-safe
// hot reload, and optional inline-policy temp-file management.
type engine struct {
	mu        sync.RWMutex // protects client during Reload()
	client    rlaas.Evaluator
	logger    *zap.Logger
	failOpen  bool
	defaults  requestDefaults
	selectors fieldSelectors

	// Retained for hot reload.
	policyFile string
	keyPrefix  string
	cacheTTL   time.Duration

	// tempFile is non-empty when PoliciesInline is used; cleaned up by close().
	tempFile string

	// Observability counters (also exposed via admin /stats and /metrics).
	allowed atomic.Int64
	denied  atomic.Int64
	shadow  atomic.Int64
	errors  atomic.Int64
	reloads atomic.Int64
}

// requestDefaults holds values injected into each RequestContext when the
// telemetry record does not supply them.
type requestDefaults struct {
	orgID       string
	tenantID    string
	application string
	environment string
}

// newEngine creates a RLAAS-backed engine from cfg.
// When cfg.PoliciesInline is set the JSON is written to a temp file so RLAAS
// can read it; the file is removed in close().
func newEngine(cfg *Config, logger *zap.Logger) (*engine, error) {
	cacheTTL := cfg.CacheTTL
	if cacheTTL <= 0 {
		cacheTTL = 30 * time.Second
	}
	keyPrefix := cfg.KeyPrefix
	if keyPrefix == "" {
		keyPrefix = "otel"
	}

	policyFile := cfg.PolicyFile
	var tempFile string

	if cfg.PoliciesInline != "" {
		f, err := os.CreateTemp("", "rlaas-policies-*.json")
		if err != nil {
			return nil, fmt.Errorf("ratelimiter: create temp policy file: %w", err)
		}
		if _, err := f.WriteString(cfg.PoliciesInline); err != nil {
			f.Close()
			os.Remove(f.Name())
			return nil, fmt.Errorf("ratelimiter: write inline policies to temp file: %w", err)
		}
		f.Close()
		policyFile = f.Name()
		tempFile = f.Name()
		logger.Info("RLAAS using inline policies via temp file", zap.String("path", policyFile))
	}

	client := rlaas.NewWithConfig(policyFile, keyPrefix, cacheTTL)

	return &engine{
		client:     client,
		logger:     logger,
		failOpen:   cfg.FailOpen,
		policyFile: policyFile,
		keyPrefix:  keyPrefix,
		cacheTTL:   cacheTTL,
		tempFile:   tempFile,
		defaults: requestDefaults{
			orgID:       cfg.OrgID,
			tenantID:    cfg.TenantID,
			application: cfg.Application,
			environment: cfg.Environment,
		},
		selectors: selectorsFromConfig(cfg),
	}, nil
}

// Reload atomically replaces the RLAAS client so the next request picks up
// any changes in the policy file immediately.  Safe to call concurrently.
func (e *engine) Reload() {
	newClient := rlaas.NewWithConfig(e.policyFile, e.keyPrefix, e.cacheTTL)
	e.mu.Lock()
	e.client = newClient
	e.mu.Unlock()
	e.reloads.Add(1)
	e.logger.Info("RLAAS policy reloaded", zap.String("policy_file", e.policyFile))
}

// close releases resources held by the engine.
// For inline-policy engines it removes the temporary policy file.
func (e *engine) close() {
	if e.tempFile == "" {
		return
	}
	if err := os.Remove(e.tempFile); err != nil {
		e.logger.Warn("Failed to remove temp policy file",
			zap.String("path", e.tempFile), zap.Error(err))
	}
}

// evaluate runs a single telemetry record through the RLAAS engine.
// It fills default fields and acquires only a read lock during the remote call.
func (e *engine) evaluate(ctx context.Context, req model.RequestContext) (model.Decision, error) {
	if req.OrgID == "" {
		req.OrgID = e.defaults.orgID
	}
	if req.TenantID == "" {
		req.TenantID = e.defaults.tenantID
	}
	if req.Application == "" {
		req.Application = e.defaults.application
	}
	if req.Environment == "" {
		req.Environment = e.defaults.environment
	}
	if req.Quantity <= 0 {
		req.Quantity = 1
	}

	e.logger.Debug("RLAAS evaluate request",
		zap.String("service", req.Service),
		zap.String("signal_type", req.SignalType),
		zap.String("severity", req.Severity),
		zap.String("span_name", req.SpanName),
		zap.String("resource", req.Resource),
		zap.String("operation", req.Operation),
		zap.String("org_id", req.OrgID),
		zap.String("tenant_id", req.TenantID),
		zap.String("environment", req.Environment),
		zap.Int64("quantity", req.Quantity),
	)

	e.mu.RLock()
	dec, err := e.client.Evaluate(ctx, req)
	e.mu.RUnlock()

	if err != nil {
		e.errors.Add(1)
		if e.failOpen {
			e.logger.Debug("RLAAS evaluation error, fail-open allows record",
				zap.Error(err),
				zap.String("service", req.Service),
				zap.String("signal", req.SignalType),
			)
			return model.Decision{Allowed: true, Action: model.ActionAllow, Reason: "fail_open"}, nil
		}
		return model.Decision{Allowed: false, Action: model.ActionDeny, Reason: "fail_closed"}, err
	}

	e.logger.Debug("RLAAS evaluate response",
		zap.String("service", req.Service),
		zap.String("signal_type", req.SignalType),
		zap.Bool("allowed", dec.Allowed),
		zap.String("action", string(dec.Action)),
		zap.String("matched_policy", dec.MatchedPolicyID),
		zap.Bool("shadow_mode", dec.ShadowMode),
		zap.String("reason", dec.Reason),
		zap.Int64("remaining", dec.Remaining),
	)

	if dec.ShadowMode {
		e.shadow.Add(1)
	} else if dec.Allowed {
		e.allowed.Add(1)
	} else {
		e.denied.Add(1)
	}

	return dec, nil
}

// shouldKeep translates a Decision to a keep/drop boolean.
// Shadow-mode records are always kept (enforcement is disabled).
func shouldKeep(d model.Decision) bool {
	return d.ShadowMode || d.Allowed
}

// Stats returns accumulated counters for the admin API and shutdown logging.
func (e *engine) Stats() (allowed, denied, shadow, errors, reloads int64) {
	return e.allowed.Load(), e.denied.Load(), e.shadow.Load(), e.errors.Load(), e.reloads.Load()
}
