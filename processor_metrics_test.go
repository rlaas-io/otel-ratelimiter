package ratelimiterprocessor

import (
	"context"
	"fmt"
	"testing"

	"github.com/rlaas-io/rlaas/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestMetricsProcessor_Basic(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "limit-metrics", "metric", 5, 5, model.ActionDrop),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.MetricsSink)
	factory := NewFactory()

	proc, err := factory.CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "payment-svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	for i := 0; i < 10; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("http.request.duration.%d", i))
		m.SetEmptyGauge()
	}

	err = proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	totalMetrics := 0
	for _, mm := range sink.AllMetrics() {
		totalMetrics += mm.MetricCount()
	}
	assert.Equal(t, 5, totalMetrics, "expected 5 metrics to pass through")
}

func TestMetricsProcessor_ShadowMode(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		shadowPolicy("p1", "shadow-metrics", "metric", 2),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.MetricsSink)
	factory := NewFactory()

	proc, err := factory.CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "web-api")
	sm := rm.ScopeMetrics().AppendEmpty()
	for i := 0; i < 5; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName("shadow_metric")
		m.SetEmptyGauge()
	}

	err = proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	totalMetrics := 0
	for _, mm := range sink.AllMetrics() {
		totalMetrics += mm.MetricCount()
	}
	assert.Equal(t, 5, totalMetrics, "shadow mode should pass all metrics")
}

func TestMetricsProcessor_EmptyMetrics(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "test", "metric", 100, 100, model.ActionDrop),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.MetricsSink)
	factory := NewFactory()

	proc, err := factory.CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	md := pmetric.NewMetrics()
	err = proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)
}

func TestMetricsProcessor_WithWatcher(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "watch-metrics", "metric", 5, 5, model.ActionDrop),
	})
	cfg := &Config{
		PolicyFile:    policyFile,
		FailOpen:      true,
		WatchPolicies: true,
	}

	sink := new(consumertest.MetricsSink)
	factory := NewFactory()

	proc, err := factory.CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Create metric to verify processor is working
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "watched-metrics-service")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("test_metric")
	m.SetEmptyGauge()

	err = proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	assert.Len(t, sink.AllMetrics(), 1)
}

