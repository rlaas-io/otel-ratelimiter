// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrScope = "github.com/rlaas-io/otel-ratelimiter"

// processorTelemetry holds OTel SDK instruments for one signal type (logs/traces/metrics).
// All counters are labeled with the signal dimension so a single Prometheus/Grafana
// dashboard can visualise all three together.
type processorTelemetry struct {
	signal   string
	received metric.Int64Counter
	dropped  metric.Int64Counter
	allowed  metric.Int64Counter
	shadow   metric.Int64Counter
	evalErr  metric.Int64Counter
	reloaded metric.Int64Counter
}

// newProcessorTelemetry creates OTel SDK counters using the MeterProvider from the
// collector's own telemetry settings.  These counters are automatically exported
// via whatever backends the operator has configured (Prometheus, OTLP, etc.).
func newProcessorTelemetry(mp metric.MeterProvider, signal string) (*processorTelemetry, error) {
	meter := mp.Meter(instrScope)

	received, err := meter.Int64Counter("ratelimiter.records.received",
		metric.WithDescription("Total telemetry records received by the rate limiter processor."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, err
	}

	dropped, err := meter.Int64Counter("ratelimiter.records.dropped",
		metric.WithDescription("Total telemetry records dropped (rate-limited, eval error, or batch limit)."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, err
	}

	allowed, err := meter.Int64Counter("ratelimiter.records.allowed",
		metric.WithDescription("Total telemetry records allowed through by the rate limiter."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, err
	}

	shadow, err := meter.Int64Counter("ratelimiter.records.shadow",
		metric.WithDescription("Total records evaluated in shadow mode (enforcement disabled; all pass through)."),
		metric.WithUnit("{record}"),
	)
	if err != nil {
		return nil, err
	}

	evalErr, err := meter.Int64Counter("ratelimiter.evaluate.errors",
		metric.WithDescription("Total RLAAS engine evaluation errors."),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	reloaded, err := meter.Int64Counter("ratelimiter.policy.reloads",
		metric.WithDescription("Total policy reload events (file watcher or POST /reload admin call)."),
		metric.WithUnit("{reload}"),
	)
	if err != nil {
		return nil, err
	}

	return &processorTelemetry{
		signal:   signal,
		received: received,
		dropped:  dropped,
		allowed:  allowed,
		shadow:   shadow,
		evalErr:  evalErr,
		reloaded: reloaded,
	}, nil
}

func (t *processorTelemetry) attrs(extra ...attribute.KeyValue) metric.MeasurementOption {
	kv := append([]attribute.KeyValue{attribute.String("signal", t.signal)}, extra...)
	return metric.WithAttributes(kv...)
}

func (t *processorTelemetry) recordReceived(ctx context.Context) {
	t.received.Add(ctx, 1, t.attrs())
}

func (t *processorTelemetry) recordDropped(ctx context.Context, reason string) {
	t.dropped.Add(ctx, 1, t.attrs(attribute.String("reason", reason)))
}

func (t *processorTelemetry) recordAllowed(ctx context.Context) {
	t.allowed.Add(ctx, 1, t.attrs())
}

func (t *processorTelemetry) recordShadow(ctx context.Context) {
	t.shadow.Add(ctx, 1, t.attrs())
}

func (t *processorTelemetry) recordError(ctx context.Context) {
	t.evalErr.Add(ctx, 1, t.attrs())
}

func (t *processorTelemetry) recordReload() {
	t.reloaded.Add(context.Background(), 1, t.attrs())
}
