package ratelimiterprocessor

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/suresh-p26/RLAAS/pkg/model"
	"github.com/suresh-p26/RLAAS/pkg/rlaas"
	"go.uber.org/zap"
)

// engine wraps a RLAAS client and applies fail-open/fail-closed semantics.
// It also fills default request context fields from the processor config.
type engine struct {
	client   rlaas.Evaluator
	logger   *zap.Logger
	failOpen bool
	defaults requestDefaults

	// Observability counters.
	allowed atomic.Int64
	denied  atomic.Int64
	shadow  atomic.Int64
	errors  atomic.Int64
}

// requestDefaults holds default values that are populated on every RequestContext
// if the telemetry record does not already provide them.
type requestDefaults struct {
	orgID       string
	tenantID    string
	application string
	environment string
}

// newEngine creates a new RLAAS-backed engine from the given config.
func newEngine(cfg *Config, logger *zap.Logger) *engine {
	cacheTTL := cfg.CacheTTL
	if cacheTTL <= 0 {
		cacheTTL = 30 * time.Second
	}
	keyPrefix := cfg.KeyPrefix
	if keyPrefix == "" {
		keyPrefix = "otel"
	}

	client := rlaas.NewWithConfig(cfg.PolicyFile, keyPrefix, cacheTTL)

	return &engine{
		client:   client,
		logger:   logger,
		failOpen: cfg.FailOpen,
		defaults: requestDefaults{
			orgID:       cfg.OrgID,
			tenantID:    cfg.TenantID,
			application: cfg.Application,
			environment: cfg.Environment,
		},
	}
}

// evaluate runs a single telemetry record through the RLAAS engine.
// It populates defaults on the RequestContext before evaluation.
func (e *engine) evaluate(ctx context.Context, req model.RequestContext) (model.Decision, error) {
	// Fill default fields.
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

	dec, err := e.client.Evaluate(ctx, req)
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

	// Track counters.
	if dec.Allowed {
		if dec.ShadowMode {
			e.shadow.Add(1)
		} else {
			e.allowed.Add(1)
		}
	} else {
		if dec.ShadowMode {
			e.shadow.Add(1)
		} else {
			e.denied.Add(1)
		}
	}

	return dec, nil
}

// shouldKeep translates a RLAAS Decision into a keep/drop boolean for the
// OTel collector pipeline. Records are kept when:
//   - Decision.Allowed is true, OR
//   - Decision.ShadowMode is true (shadow mode evaluates but passes all through)
func shouldKeep(d model.Decision) bool {
	if d.ShadowMode {
		return true
	}
	return d.Allowed
}

// Stats returns the current engine statistics.
func (e *engine) Stats() (allowed, denied, shadow, errors int64) {
	return e.allowed.Load(), e.denied.Load(), e.shadow.Load(), e.errors.Load()
}
