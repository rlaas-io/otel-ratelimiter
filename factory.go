// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processorhelper"

	"github.com/rlaas-io/otel-ratelimiter/internal/metadata"
)

var processorCapabilities = consumer.Capabilities{MutatesData: true}

// NewFactory creates a new factory for the rate limiter processor.
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
		PolicyFile: "",
		FailOpen:   true,
		CacheTTL:   30 * time.Second,
		KeyPrefix:  "otel",
	}
}

func createLogsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Logs,
) (processor.Logs, error) {
	oCfg := cfg.(*Config)
	eng := newEngine(oCfg, set.TelemetrySettings.Logger)
	lp := &logsProcessor{
		cfg:    oCfg,
		logger: set.TelemetrySettings.Logger,
		engine: eng,
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
	eng := newEngine(oCfg, set.TelemetrySettings.Logger)
	tp := &tracesProcessor{
		cfg:    oCfg,
		logger: set.TelemetrySettings.Logger,
		engine: eng,
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
	eng := newEngine(oCfg, set.TelemetrySettings.Logger)
	mp := &metricsProcessor{
		cfg:    oCfg,
		logger: set.TelemetrySettings.Logger,
		engine: eng,
	}
	return processorhelper.NewMetrics(
		ctx, set, cfg, nextConsumer, mp.processMetrics,
		processorhelper.WithCapabilities(processorCapabilities),
		processorhelper.WithStart(mp.start),
		processorhelper.WithShutdown(mp.shutdown),
	)
}
