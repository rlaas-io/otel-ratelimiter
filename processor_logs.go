// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// logsProcessor applies RLAAS rate limiting to log records.
// It is used with processorhelper.NewLogs — the helper handles
// Capabilities, Start/Shutdown delegation, and nextConsumer forwarding.
type logsProcessor struct {
	cfg    *Config
	logger *zap.Logger
	engine *engine

	// Per-processor counters.
	received atomic.Int64
	dropped  atomic.Int64
}

// start is called by processorhelper on pipeline start.
func (lp *logsProcessor) start(_ context.Context, _ component.Host) error {
	lp.logger.Info("RLAAS rate limiter logs processor started",
		zap.String("policy_file", lp.cfg.PolicyFile),
		zap.Bool("fail_open", lp.cfg.FailOpen),
	)
	return nil
}

// shutdown is called by processorhelper on pipeline stop.
func (lp *logsProcessor) shutdown(_ context.Context) error {
	allowed, denied, shadow, errors := lp.engine.Stats()
	lp.logger.Info("RLAAS rate limiter logs processor stopped",
		zap.Int64("total_received", lp.received.Load()),
		zap.Int64("total_dropped", lp.dropped.Load()),
		zap.Int64("allowed", allowed),
		zap.Int64("denied", denied),
		zap.Int64("shadow", shadow),
		zap.Int64("errors", errors),
	)
	return nil
}

// processLogs is the core processing function passed to processorhelper.NewLogs.
// Each log record is converted to a RLAAS RequestContext and evaluated by the
// RLAAS engine. Records whose Decision is not "keep" are removed from the batch.
func (lp *logsProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	if ld.ResourceLogs().Len() == 0 {
		return ld, nil
	}

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resource := rl.Resource()

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)

			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				lp.received.Add(1)

				// Build RLAAS request context from OTel record.
				reqCtx := buildLogsContext(resource, lr)

				// Evaluate using the RLAAS engine.
				decision, err := lp.engine.evaluate(ctx, reqCtx)
				if err != nil {
					lp.dropped.Add(1)
					lp.logger.Debug("log record dropped due to RLAAS error",
						zap.Error(err),
						zap.String("service", reqCtx.Service),
					)
					return true // remove
				}

				if !shouldKeep(decision) {
					lp.dropped.Add(1)
					lp.logger.Debug("log record dropped by RLAAS",
						zap.String("service", reqCtx.Service),
						zap.String("action", string(decision.Action)),
						zap.String("policy", decision.MatchedPolicyID),
					)
					return true // remove
				}
				return false // keep
			})
		}
	}

	// Remove empty scope/resource containers.
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})

	return ld, nil
}
