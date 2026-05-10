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
	cfg         *Config
	logger      *zap.Logger
	engine      *engine
	tel         *processorTelemetry
	watchCancel context.CancelFunc

	received atomic.Int64
	dropped  atomic.Int64
}

// start is called by processorhelper on pipeline start.
func (lp *logsProcessor) start(_ context.Context, _ component.Host) error {
	watchCtx, cancel := context.WithCancel(context.Background())
	lp.watchCancel = cancel

	if lp.cfg.WatchPolicies && lp.cfg.PolicyFile != "" {
		watcher, err := newPolicyWatcher(lp.cfg.PolicyFile, lp.cfg.WatchInterval, func() {
			lp.engine.Reload()
			lp.tel.recordReload()
		}, lp.logger)
		if err != nil {
			lp.logger.Warn("Policy file watcher unavailable; using cache TTL only",
				zap.Error(err), zap.Duration("cache_ttl", lp.cfg.CacheTTL))
		} else {
			go watcher.run(watchCtx)
		}
	}

	registerWithAdmin(lp.cfg, lp.engine, "logs",
		func() int64 { return lp.received.Load() },
		func() int64 { return lp.dropped.Load() },
		lp.logger,
	)

	lp.logger.Info("RLAAS rate limiter logs processor started",
		zap.String("policy_file", lp.cfg.PolicyFile),
		zap.Bool("fail_open", lp.cfg.FailOpen),
		zap.Bool("watch_policies", lp.cfg.WatchPolicies),
		zap.String("admin_addr", lp.cfg.AdminAddr),
	)
	return nil
}

// shutdown is called by processorhelper on pipeline stop.
func (lp *logsProcessor) shutdown(_ context.Context) error {
	if lp.watchCancel != nil {
		lp.watchCancel()
	}
	deregisterFromAdmin("logs")
	lp.engine.close()

	allowed, denied, shadow, errors, reloads := lp.engine.Stats()
	lp.logger.Info("RLAAS rate limiter logs processor stopped",
		zap.Int64("total_received", lp.received.Load()),
		zap.Int64("total_dropped", lp.dropped.Load()),
		zap.Int64("allowed", allowed),
		zap.Int64("denied", denied),
		zap.Int64("shadow", shadow),
		zap.Int64("errors", errors),
		zap.Int64("reloads", reloads),
	)
	return nil
}

// processLogs is the core processing function passed to processorhelper.NewLogs.
// processLogs evaluates each log record through RLAAS and removes rate-limited ones.
func (lp *logsProcessor) processLogs(ctx context.Context, ld plog.Logs) (plog.Logs, error) {
	if ld.ResourceLogs().Len() == 0 {
		return ld, nil
	}

	budget := lp.cfg.MaxBatchSize // 0 = unlimited

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resource := rl.Resource()

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)

			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				lp.received.Add(1)
				lp.tel.recordReceived(ctx)

				// Pre-policy batch size limit.
				if lp.cfg.MaxBatchSize > 0 {
					if budget <= 0 {
						lp.dropped.Add(1)
						lp.tel.recordDropped(ctx, "batch_size_exceeded")
						return true
					}
					budget--
				}

				reqCtx := buildLogsContext(resource, lr)
				decision, err := lp.engine.evaluate(ctx, reqCtx)
				if err != nil {
					lp.dropped.Add(1)
					lp.tel.recordError(ctx)
					lp.tel.recordDropped(ctx, "error")
					lp.logger.Debug("log record dropped due to RLAAS error",
						zap.Error(err), zap.String("service", reqCtx.Service))
					return true
				}

				if !shouldKeep(decision) {
					lp.dropped.Add(1)
					lp.tel.recordDropped(ctx, "rate_limited")
					lp.logger.Debug("log record rate-limited",
						zap.String("service", reqCtx.Service),
						zap.String("policy", decision.MatchedPolicyID),
						zap.String("action", string(decision.Action)),
					)
					return true
				}
				if decision.ShadowMode {
					lp.tel.recordShadow(ctx)
				} else {
					lp.tel.recordAllowed(ctx)
				}
				return false
			})
		}
	}

	// Remove empty scope/resource containers left after record filtering.
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})

	return ld, nil
}
