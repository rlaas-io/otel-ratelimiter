package ratelimiterprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/rlaas-io/rlaas/pkg/model"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestTracesProcessor_Basic(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "limit-spans", "span", 5, 5, model.ActionDrop),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.TracesSink)
	factory := NewFactory()

	proc, err := factory.CreateTraces(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Create 10 spans.
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "payment-svc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 10; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName("process-payment")
	}

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	totalSpans := 0
	for _, tr := range sink.AllTraces() {
		totalSpans += tr.SpanCount()
	}
	assert.Equal(t, 5, totalSpans, "expected 5 spans to pass through")
}

func TestTracesProcessor_ShadowMode(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		shadowPolicy("p1", "shadow-spans", "span", 2),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.TracesSink)
	factory := NewFactory()

	proc, err := factory.CreateTraces(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "payment-svc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 5; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName("shadow-span")
	}

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	totalSpans := 0
	for _, tr := range sink.AllTraces() {
		totalSpans += tr.SpanCount()
	}
	assert.Equal(t, 5, totalSpans, "shadow mode should pass all spans")
}

func TestTracesProcessor_EmptyTraces(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "test", "span", 100, 100, model.ActionDrop),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.TracesSink)
	factory := NewFactory()

	proc, err := factory.CreateTraces(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	td := ptrace.NewTraces()
	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)
}
