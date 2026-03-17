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
	cfg    *Config
	logger *zap.Logger
	engine *engine

	received atomic.Int64
	dropped  atomic.Int64
}

// start is called by processorhelper on pipeline start.
func (mp *metricsProcessor) start(_ context.Context, _ component.Host) error {
	mp.logger.Info("RLAAS rate limiter metrics processor started",
		zap.String("policy_file", mp.cfg.PolicyFile),
		zap.Bool("fail_open", mp.cfg.FailOpen),
	)
	return nil
}

// shutdown is called by processorhelper on pipeline stop.
func (mp *metricsProcessor) shutdown(_ context.Context) error {
	allowed, denied, shadow, errors := mp.engine.Stats()
	mp.logger.Info("RLAAS rate limiter metrics processor stopped",
		zap.Int64("total_received", mp.received.Load()),
		zap.Int64("total_dropped", mp.dropped.Load()),
		zap.Int64("allowed", allowed),
		zap.Int64("denied", denied),
		zap.Int64("shadow", shadow),
		zap.Int64("errors", errors),
	)
	return nil
}

// processMetrics is the core processing function passed to processorhelper.NewMetrics.
// Each metric is converted to a RLAAS RequestContext and evaluated by the
// RLAAS engine. Metrics whose Decision is not "keep" are removed from the batch.
func (mp *metricsProcessor) processMetrics(ctx context.Context, md pmetric.Metrics) (pmetric.Metrics, error) {
	if md.ResourceMetrics().Len() == 0 {
		return md, nil
	}

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resource := rm.Resource()

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)

			sm.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
				mp.received.Add(1)

				// Build RLAAS request context from OTel metric.
				reqCtx := buildMetricsContext(resource, metric)

				// Evaluate using the RLAAS engine.
				decision, err := mp.engine.evaluate(ctx, reqCtx)
				if err != nil {
					mp.dropped.Add(1)
					mp.logger.Debug("metric dropped due to RLAAS error",
						zap.Error(err),
						zap.String("metric_name", metric.Name()),
					)
					return true // remove
				}

				if !shouldKeep(decision) {
					mp.dropped.Add(1)
					mp.logger.Debug("metric dropped by RLAAS",
						zap.String("metric_name", metric.Name()),
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
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})

	return md, nil
}
