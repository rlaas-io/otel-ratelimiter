// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// tracesProcessor applies RLAAS rate limiting to spans.
// It is used with processorhelper.NewTraces — the helper handles
// Capabilities, Start/Shutdown delegation, and nextConsumer forwarding.
type tracesProcessor struct {
	cfg         *Config
	logger      *zap.Logger
	engine      *engine
	tel         *processorTelemetry
	watchCancel context.CancelFunc

	received atomic.Int64
	dropped  atomic.Int64
}

// start is called by processorhelper on pipeline start.
func (tp *tracesProcessor) start(_ context.Context, _ component.Host) error {
	watchCtx, cancel := context.WithCancel(context.Background())
	tp.watchCancel = cancel

	if tp.cfg.WatchPolicies && tp.cfg.PolicyFile != "" {
		watcher, err := newPolicyWatcher(tp.cfg.PolicyFile, tp.cfg.WatchInterval, func() {
			tp.engine.Reload()
			tp.tel.recordReload()
		}, tp.logger)
		if err != nil {
			tp.logger.Warn("Policy file watcher unavailable; using cache TTL only",
				zap.Error(err), zap.Duration("cache_ttl", tp.cfg.CacheTTL))
		} else {
			go watcher.run(watchCtx)
		}
	}

	registerWithAdmin(tp.cfg, tp.engine, "traces",
		func() int64 { return tp.received.Load() },
		func() int64 { return tp.dropped.Load() },
		tp.logger,
	)

	tp.logger.Info("RLAAS rate limiter traces processor started",
		zap.String("policy_file", tp.cfg.PolicyFile),
		zap.Bool("fail_open", tp.cfg.FailOpen),
		zap.Bool("watch_policies", tp.cfg.WatchPolicies),
		zap.String("admin_addr", tp.cfg.AdminAddr),
	)
	return nil
}

// shutdown is called by processorhelper on pipeline stop.
func (tp *tracesProcessor) shutdown(_ context.Context) error {
	if tp.watchCancel != nil {
		tp.watchCancel()
	}
	deregisterFromAdmin("traces")
	tp.engine.close()

	allowed, denied, shadow, errors, reloads := tp.engine.Stats()
	tp.logger.Info("RLAAS rate limiter traces processor stopped",
		zap.Int64("total_received", tp.received.Load()),
		zap.Int64("total_dropped", tp.dropped.Load()),
		zap.Int64("allowed", allowed),
		zap.Int64("denied", denied),
		zap.Int64("shadow", shadow),
		zap.Int64("errors", errors),
		zap.Int64("reloads", reloads),
	)
	return nil
}

// processTraces is the core processing function passed to processorhelper.NewTraces.
// processTraces evaluates each span through RLAAS and removes rate-limited ones.
func (tp *tracesProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if td.ResourceSpans().Len() == 0 {
		return td, nil
	}

	budget := tp.cfg.MaxBatchSize

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resource := rs.Resource()

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)

			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				tp.received.Add(1)
				tp.tel.recordReceived(ctx)

				if tp.cfg.MaxBatchSize > 0 {
					if budget <= 0 {
						tp.dropped.Add(1)
						tp.tel.recordDropped(ctx, "batch_size_exceeded")
						return true
					}
					budget--
				}

				reqCtx := buildTracesContextWithSelectors(resource, span, tp.engine.selectors)
				decision, err := tp.engine.evaluate(ctx, reqCtx)
				if err != nil {
					tp.dropped.Add(1)
					tp.tel.recordError(ctx)
					tp.tel.recordDropped(ctx, "error")
					tp.logger.Debug("span dropped due to RLAAS error",
						zap.Error(err), zap.String("span_name", span.Name()))
					return true
				}

				if !shouldKeep(decision) {
					tp.dropped.Add(1)
					tp.tel.recordDropped(ctx, "rate_limited")
					tp.logger.Debug("span rate-limited",
						zap.String("span_name", span.Name()),
						zap.String("policy", decision.MatchedPolicyID),
						zap.String("action", string(decision.Action)),
					)
					return true
				}
				if decision.ShadowMode {
					tp.tel.recordShadow(ctx)
				} else {
					tp.tel.recordAllowed(ctx)
				}
				return false
			})
		}
	}

	// Remove empty containers left after span filtering.
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})

	return td, nil
}
