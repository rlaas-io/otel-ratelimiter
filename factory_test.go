package ratelimiterprocessor

import (
	"context"
	"testing"

	"github.com/rlaas-io/rlaas/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
)

func TestFactory_Type(t *testing.T) {
	factory := NewFactory()
	assert.Equal(t, "ratelimiter", factory.Type().String())
}

func TestFactory_CreateDefaultConfig(t *testing.T) {
	factory := NewFactory()
	cfg := factory.CreateDefaultConfig()
	require.NotNil(t, cfg)

	oCfg := cfg.(*Config)
	assert.Equal(t, "", oCfg.PolicyFile)
	assert.True(t, oCfg.FailOpen)
	assert.Equal(t, "otel", oCfg.KeyPrefix)
}

func TestFactory_CreateLogsProcessor(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "test", "log", 100, 100, model.ActionDrop),
	})

	factory := NewFactory()
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.LogsSink)

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)
}

func TestFactory_CreateTracesProcessor(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "test", "span", 100, 100, model.ActionDrop),
	})

	factory := NewFactory()
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.TracesSink)

	proc, err := factory.CreateTraces(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)
}

func TestFactory_CreateMetricsProcessor(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "test", "metric", 100, 100, model.ActionDrop),
	})

	factory := NewFactory()
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.MetricsSink)

	proc, err := factory.CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)
}

func TestFactory_CreateLogsProcessor_InvalidConfig(t *testing.T) {
	factory := NewFactory()
	// Missing policy file and policies_inline
	cfg := &Config{FailOpen: true}
	sink := new(consumertest.LogsSink)

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	// Should still succeed but engine will use empty/default policies
	require.NoError(t, err)
	require.NotNil(t, proc)
}

func TestFactory_CreateTracesProcessor_InvalidConfig(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{FailOpen: true}
	sink := new(consumertest.TracesSink)

	proc, err := factory.CreateTraces(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)
}

func TestFactory_CreateMetricsProcessor_InvalidConfig(t *testing.T) {
	factory := NewFactory()
	cfg := &Config{FailOpen: true}
	sink := new(consumertest.MetricsSink)

	proc, err := factory.CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NotNil(t, proc)
}
