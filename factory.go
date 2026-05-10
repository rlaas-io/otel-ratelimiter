// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/rlaas-io/otel-ratelimiter/internal/metadata"
)

var processorCapabilities = consumer.Capabilities{MutatesData: true}

// NewFactory creates the rate limiter processor factory.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		metadata.Type,
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, metadata.LogsStability),
		processor.WithTraces(createTracesProcessor, metadata.TracesStability),
		processor.WithMetrics(createMetricsProcessor, metadata.MetricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		PolicyFile:    "",
		FailOpen:      true,
		CacheTTL:      30 * time.Second,
		WatchInterval: 15 * time.Second,
		KeyPrefix:     "otel",
	}
}

func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	oCfg := cfg.(*Config)
	eng, err := newEngine(oCfg, set.TelemetrySettings.Logger)
	if err != nil {
		return nil, fmt.Errorf("ratelimiter: create engine: %w", err)
	}
	tel, err := newProcessorTelemetry(set.TelemetrySettings.MeterProvider, "logs")
	if err != nil {
		return nil, fmt.Errorf("ratelimiter: create telemetry instruments: %w", err)
	}
	lp := &logsProcessor{
		cfg:    oCfg,
		logger: set.TelemetrySettings.Logger,
		engine: eng,
		tel:    tel,
	}
	return processorhelper.NewLogs(
		ctx, set, cfg, nextConsumer, lp.processLogs,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(lp.start),
		processorhelper.WithShutdown(lp.shutdown),
	)
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	oCfg := cfg.(*Config)
	eng, err := newEngine(oCfg, set.TelemetrySettings.Logger)
	if err != nil {
		return nil, fmt.Errorf("ratelimiter: create engine: %w", err)
	}
	tel, err := newProcessorTelemetry(set.TelemetrySettings.MeterProvider, "traces")
	if err != nil {
		return nil, fmt.Errorf("ratelimiter: create telemetry instruments: %w", err)
	}
	tp := &tracesProcessor{
		cfg:    oCfg,
		logger: set.TelemetrySettings.Logger,
		engine: eng,
		tel:    tel,
	}
	return processorhelper.NewTraces(
		ctx, set, cfg, nextConsumer, tp.processTraces,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(tp.start),
		processorhelper.WithShutdown(tp.shutdown),
	)
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	oCfg := cfg.(*Config)
	eng, err := newEngine(oCfg, set.TelemetrySettings.Logger)
	if err != nil {
		return nil, fmt.Errorf("ratelimiter: create engine: %w", err)
	}
	tel, err := newProcessorTelemetry(set.TelemetrySettings.MeterProvider, "metrics")
	if err != nil {
		return nil, fmt.Errorf("ratelimiter: create telemetry instruments: %w", err)
	}
	mp := &metricsProcessor{
		cfg:    oCfg,
		logger: set.TelemetrySettings.Logger,
		engine: eng,
		tel:    tel,
	}
	return processorhelper.NewMetrics(
		ctx, set, cfg, nextConsumer, mp.processMetrics,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(mp.start),
		processorhelper.WithShutdown(mp.shutdown),
	)
}
