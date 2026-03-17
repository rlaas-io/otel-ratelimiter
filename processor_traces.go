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
	cfg    *Config
	logger *zap.Logger
	engine *engine

	received atomic.Int64
	dropped  atomic.Int64
}

// start is called by processorhelper on pipeline start.
func (tp *tracesProcessor) start(_ context.Context, _ component.Host) error {
	tp.logger.Info("RLAAS rate limiter traces processor started",
		zap.String("policy_file", tp.cfg.PolicyFile),
		zap.Bool("fail_open", tp.cfg.FailOpen),
	)
	return nil
}

// shutdown is called by processorhelper on pipeline stop.
func (tp *tracesProcessor) shutdown(_ context.Context) error {
	allowed, denied, shadow, errors := tp.engine.Stats()
	tp.logger.Info("RLAAS rate limiter traces processor stopped",
		zap.Int64("total_received", tp.received.Load()),
		zap.Int64("total_dropped", tp.dropped.Load()),
		zap.Int64("allowed", allowed),
		zap.Int64("denied", denied),
		zap.Int64("shadow", shadow),
		zap.Int64("errors", errors),
	)
	return nil
}

// processTraces is the core processing function passed to processorhelper.NewTraces.
// Each span is converted to a RLAAS RequestContext and evaluated by the
// RLAAS engine. Spans whose Decision is not "keep" are removed from the batch.
func (tp *tracesProcessor) processTraces(ctx context.Context, td ptrace.Traces) (ptrace.Traces, error) {
	if td.ResourceSpans().Len() == 0 {
		return td, nil
	}

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resource := rs.Resource()

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)

			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				tp.received.Add(1)

				// Build RLAAS request context from OTel span.
				reqCtx := buildTracesContext(resource, span)

				// Evaluate using the RLAAS engine.
				decision, err := tp.engine.evaluate(ctx, reqCtx)
				if err != nil {
					tp.dropped.Add(1)
					tp.logger.Debug("span dropped due to RLAAS error",
						zap.Error(err),
						zap.String("span_name", span.Name()),
					)
					return true // remove
				}

				if !shouldKeep(decision) {
					tp.dropped.Add(1)
					tp.logger.Debug("span dropped by RLAAS",
						zap.String("span_name", span.Name()),
						zap.String("action", string(decision.Action)),
						zap.String("policy", decision.MatchedPolicyID),
					)
					return true // remove
				}
				return false // keep
			})
		}
	}

	// Remove empty containers.
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})

	return td, nil
}
