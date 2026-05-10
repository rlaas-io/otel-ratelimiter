// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// metricsProcessor applies RLAAS rate limiting to metrics.
// It is used with processorhelper.NewMetrics — the helper handles
// Capabilities, Start/Shutdown delegation, and nextConsumer forwarding.
type metricsProcessor struct {
	cfg         *Config
	logger      *zap.Logger
	engine      *engine
	tel         *processorTelemetry
	watchCancel context.CancelFunc

	received atomic.Int64
	dropped  atomic.Int64
}

// start is called by processorhelper on pipeline start.
func (mp *metricsProcessor) start(_ context.Context, _ component.Host) error {
	watchCtx, cancel := context.WithCancel(context.Background())
	mp.watchCancel = cancel

	if mp.cfg.WatchPolicies && mp.cfg.PolicyFile != "" {
		watcher, err := newPolicyWatcher(mp.cfg.PolicyFile, mp.cfg.WatchInterval, func() {
			mp.engine.Reload()
			mp.tel.recordReload()
		}, mp.logger)
		if err != nil {
			mp.logger.Warn("Policy file watcher unavailable; using cache TTL only",
				zap.Error(err), zap.Duration("cache_ttl", mp.cfg.CacheTTL))
		} else {
			go watcher.run(watchCtx)
		}
	}

	registerWithAdmin(mp.cfg, mp.engine, "metrics",
		func() int64 { return mp.received.Load() },
		func() int64 { return mp.dropped.Load() },
		mp.logger,
	)

	mp.logger.Info("RLAAS rate limiter metrics processor started",
		zap.String("policy_file", mp.cfg.PolicyFile),
		zap.Bool("fail_open", mp.cfg.FailOpen),
		zap.Bool("watch_policies", mp.cfg.WatchPolicies),
		zap.String("admin_addr", mp.cfg.AdminAddr),
	)
	return nil
}

// shutdown is called by processorhelper on pipeline stop.
func (mp *metricsProcessor) shutdown(_ context.Context) error {
	if mp.watchCancel != nil {
		mp.watchCancel()
	}
	deregisterFromAdmin("metrics")
	mp.engine.close()

	allowed, denied, shadow, errors, reloads := mp.engine.Stats()
	mp.logger.Info("RLAAS rate limiter metrics processor stopped",
		zap.Int64("total_received", mp.received.Load()),
		zap.Int64("total_dropped", mp.dropped.Load()),
		zap.Int64("allowed", allowed),
		zap.Int64("denied", denied),
		zap.Int64("shadow", shadow),
		zap.Int64("errors", errors),
		zap.Int64("reloads", reloads),
	)
	return nil
}

// processMetrics is the core processing function passed to processorhelper.NewMetrics.
// processMetrics evaluates each metric through RLAAS and removes rate-limited ones.
func (mp *metricsProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.ResourceMetrics().Len() == 0 {
		return md, nil
	}

	budget := mp.cfg.MaxBatchSize

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resource := rm.Resource()

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)

			sm.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
				mp.received.Add(1)
				mp.tel.recordReceived(ctx)

				if mp.cfg.MaxBatchSize > 0 {
					if budget <= 0 {
						mp.dropped.Add(1)
						mp.tel.recordDropped(ctx, "batch_size_exceeded")
						return true
					}
					budget--
				}

				reqCtx := buildMetricsContext(resource, metric)
				decision, err := mp.engine.evaluate(ctx, reqCtx)
				if err != nil {
					mp.dropped.Add(1)
					mp.tel.recordError(ctx)
					mp.tel.recordDropped(ctx, "error")
					mp.logger.Debug("metric dropped due to RLAAS error",
						zap.Error(err), zap.String("metric_name", metric.Name()))
					return true
				}

				if !shouldKeep(decision) {
					mp.dropped.Add(1)
					mp.tel.recordDropped(ctx, "rate_limited")
					mp.logger.Debug("metric rate-limited",
						zap.String("metric_name", metric.Name()),
						zap.String("policy", decision.MatchedPolicyID),
						zap.String("action", string(decision.Action)),
					)
					return true
				}
				if decision.ShadowMode {
					mp.tel.recordShadow(ctx)
				} else {
					mp.tel.recordAllowed(ctx)
				}
				return false
			})
		}
	}

	// Remove empty containers left after metric filtering.
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	return md, nil
}
