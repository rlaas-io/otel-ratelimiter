package ratelimiterprocessor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rlaas-io/rlaas/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap/zaptest"
)

type fakeEvaluator struct {
	decision model.Decision
	err      error
}

func (f *fakeEvaluator) Evaluate(_ context.Context, _ model.RequestContext) (model.Decision, error) {
	return f.decision, f.err
}

func (f *fakeEvaluator) StartConcurrencyLease(_ context.Context, _ model.RequestContext) (model.Decision, func() error, error) {
	return f.decision, func() error { return nil }, f.err
}

func newTestTelemetry(t *testing.T, signal string) *processorTelemetry {
	t.Helper()
	tel, err := newProcessorTelemetry(noop.NewMeterProvider(), signal)
	require.NoError(t, err)
	return tel
}

func TestLogsProcessor_ProcessLogs_ErrorAndBatchExceeded(t *testing.T) {
	lp := &logsProcessor{
		cfg:    &Config{MaxBatchSize: 1},
		logger: zaptest.NewLogger(t),
		engine: &engine{client: &fakeEvaluator{err: errors.New("eval-fail")}, logger: zaptest.NewLogger(t), failOpen: false},
		tel:    newTestTelemetry(t, "logs"),
	}

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "svc")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.LogRecords().AppendEmpty().Body().SetStr("first")  // eval error branch
	sl.LogRecords().AppendEmpty().Body().SetStr("second") // batch limit branch

	out, err := lp.processLogs(context.Background(), ld)
	require.NoError(t, err)
	assert.Equal(t, 0, out.LogRecordCount())
	assert.Equal(t, int64(2), lp.received.Load())
	assert.Equal(t, int64(2), lp.dropped.Load())
}

func TestTracesProcessor_ProcessTraces_ErrorAndBatchExceeded(t *testing.T) {
	tp := &tracesProcessor{
		cfg:    &Config{MaxBatchSize: 1},
		logger: zaptest.NewLogger(t),
		engine: &engine{client: &fakeEvaluator{err: errors.New("eval-fail")}, logger: zaptest.NewLogger(t), failOpen: false},
		tel:    newTestTelemetry(t, "traces"),
	}

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "svc")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Spans().AppendEmpty().SetName("first")
	ss.Spans().AppendEmpty().SetName("second")

	out, err := tp.processTraces(context.Background(), td)
	require.NoError(t, err)
	assert.Equal(t, 0, out.SpanCount())
	assert.Equal(t, int64(2), tp.received.Load())
	assert.Equal(t, int64(2), tp.dropped.Load())
}

func TestMetricsProcessor_ProcessMetrics_ErrorAndBatchExceeded(t *testing.T) {
	mp := &metricsProcessor{
		cfg:    &Config{MaxBatchSize: 1},
		logger: zaptest.NewLogger(t),
		engine: &engine{client: &fakeEvaluator{err: errors.New("eval-fail")}, logger: zaptest.NewLogger(t), failOpen: false},
		tel:    newTestTelemetry(t, "metrics"),
	}

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Metrics().AppendEmpty().SetName("first")
	sm.Metrics().AppendEmpty().SetName("second")

	out, err := mp.processMetrics(context.Background(), md)
	require.NoError(t, err)
	assert.Equal(t, 0, out.MetricCount())
	assert.Equal(t, int64(2), mp.received.Load())
	assert.Equal(t, int64(2), mp.dropped.Load())
}

func TestProcessors_StartWithPolicyWatcher(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "watch", "log", 10, 10, model.ActionDrop),
	})

	eng, err := newEngine(&Config{PolicyFile: policyFile, FailOpen: true}, zaptest.NewLogger(t))
	require.NoError(t, err)

	lp := &logsProcessor{cfg: &Config{PolicyFile: policyFile, WatchPolicies: true, WatchInterval: 10 * time.Millisecond}, logger: zaptest.NewLogger(t), engine: eng, tel: newTestTelemetry(t, "logs")}
	tp := &tracesProcessor{cfg: &Config{PolicyFile: policyFile, WatchPolicies: true, WatchInterval: 10 * time.Millisecond}, logger: zaptest.NewLogger(t), engine: eng, tel: newTestTelemetry(t, "traces")}
	mp := &metricsProcessor{cfg: &Config{PolicyFile: policyFile, WatchPolicies: true, WatchInterval: 10 * time.Millisecond}, logger: zaptest.NewLogger(t), engine: eng, tel: newTestTelemetry(t, "metrics")}

	require.NoError(t, lp.start(context.Background(), nil))
	require.NoError(t, tp.start(context.Background(), nil))
	require.NoError(t, mp.start(context.Background(), nil))

	require.NoError(t, lp.shutdown(context.Background()))
	require.NoError(t, tp.shutdown(context.Background()))
	require.NoError(t, mp.shutdown(context.Background()))
}

func TestProcessors_StartWatcherErrorPath(t *testing.T) {
	cfg := &Config{PolicyFile: "definitely-missing-file.json", WatchPolicies: true, WatchInterval: 10 * time.Millisecond}
	eng, err := newEngine(&Config{PoliciesInline: "[]", FailOpen: true}, zaptest.NewLogger(t))
	require.NoError(t, err)
	defer eng.close()

	lp := &logsProcessor{cfg: cfg, logger: zaptest.NewLogger(t), engine: eng, tel: newTestTelemetry(t, "logs")}
	tp := &tracesProcessor{cfg: cfg, logger: zaptest.NewLogger(t), engine: eng, tel: newTestTelemetry(t, "traces")}
	mp := &metricsProcessor{cfg: cfg, logger: zaptest.NewLogger(t), engine: eng, tel: newTestTelemetry(t, "metrics")}

	require.NoError(t, lp.start(context.Background(), nil))
	require.NoError(t, tp.start(context.Background(), nil))
	require.NoError(t, mp.start(context.Background(), nil))

	require.NoError(t, lp.shutdown(context.Background()))
	require.NoError(t, tp.shutdown(context.Background()))
	require.NoError(t, mp.shutdown(context.Background()))
}
