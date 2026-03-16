package ratelimiterprocessor

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
)

const (
	typeStr = "ratelimiter"
)

// NewFactory creates a new factory for the rate limiter processor.
func NewFactory() processor.Factory {
	return processor.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		processor.WithLogs(createLogsProcessor, component.StabilityLevelAlpha),
		processor.WithTraces(createTracesProcessor, component.StabilityLevelAlpha),
		processor.WithMetrics(createMetricsProcessor, component.StabilityLevelAlpha),
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
	return &logsProcessor{
		cfg:          oCfg,
		logger:       set.TelemetrySettings.Logger,
		nextConsumer: nextConsumer,
		engine:       eng,
	}, nil
}

func createTracesProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Traces,
) (processor.Traces, error) {
	oCfg := cfg.(*Config)
	eng := newEngine(oCfg, set.TelemetrySettings.Logger)
	return &tracesProcessor{
		cfg:          oCfg,
		logger:       set.TelemetrySettings.Logger,
		nextConsumer: nextConsumer,
		engine:       eng,
	}, nil
}

func createMetricsProcessor(
	ctx context.Context,
	set processor.Settings,
	cfg component.Config,
	nextConsumer consumer.Metrics,
) (processor.Metrics, error) {
	oCfg := cfg.(*Config)
	eng := newEngine(oCfg, set.TelemetrySettings.Logger)
	return &metricsProcessor{
		cfg:          oCfg,
		logger:       set.TelemetrySettings.Logger,
		nextConsumer: nextConsumer,
		engine:       eng,
	}, nil
}
