package ratelimiterprocessor

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

// metricsProcessor implements processor.Metrics using the RLAAS engine for decisions.
type metricsProcessor struct {
	cfg          *Config
	logger       *zap.Logger
	nextConsumer consumer.Metrics
	engine       *engine

	received atomic.Int64
	dropped  atomic.Int64
}

// Capabilities implements consumer.Metrics.
func (mp *metricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// Start implements component.Component.
func (mp *metricsProcessor) Start(_ context.Context, _ component.Host) error {
	mp.logger.Info("RLAAS rate limiter metrics processor started",
		zap.String("policy_file", mp.cfg.PolicyFile),
		zap.Bool("fail_open", mp.cfg.FailOpen),
	)
	return nil
}

// Shutdown implements component.Component.
func (mp *metricsProcessor) Shutdown(_ context.Context) error {
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

// ConsumeMetrics implements consumer.Metrics.
// Each metric is converted to a RLAAS RequestContext and evaluated by the
// RLAAS engine. Metrics whose Decision is not "keep" are removed from the batch.
func (mp *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if md.ResourceMetrics().Len() == 0 {
		return mp.nextConsumer.ConsumeMetrics(ctx, md)
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

	if md.ResourceMetrics().Len() == 0 {
		return nil
	}

	return mp.nextConsumer.ConsumeMetrics(ctx, md)
}
