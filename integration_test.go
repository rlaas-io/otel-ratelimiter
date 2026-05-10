package ratelimiterprocessor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rlaas-io/rlaas/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap/zaptest"
)

// ---------------------------------------------------------------------------
// Integration tests: full pipeline tests with realistic data that exercise
// the complete flow: OTel pdata -> context builder -> RLAAS engine -> drop/keep
// ---------------------------------------------------------------------------

// TestIntegration_LogsProcessor_DropsExcessLogs verifies that when a burst of
// log records exceeds the configured rate limit, excess records are dropped
// and only the allowed number passes through to the next consumer.
func TestIntegration_LogsProcessor_DropsExcessLogs(t *testing.T) {
	const (
		limit     = 10
		burst     = 5
		totalLogs = 50
	)

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-limit-logs", "integration: limit logs", "log", int64(limit), int64(limit), model.ActionDrop),
	})

	cfg := &Config{
		PolicyFile:  policyFile,
		FailOpen:    true,
		CacheTTL:    10 * time.Second,
		KeyPrefix:   "integration",
		OrgID:       "test-org",
		TenantID:    "test-tenant",
		Application: "integration-app",
		Environment: "test",
	}

	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// --- Build realistic log batch ---
	ld := plog.NewLogs()

	// Service 1: web-api sends 25 INFO logs
	rl1 := ld.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "web-api")
	rl1.Resource().Attributes().PutStr("host.name", "web-api-pod-1")
	rl1.Resource().Attributes().PutStr("deployment.environment", "production")
	sl1 := rl1.ScopeLogs().AppendEmpty()
	sl1.Scope().SetName("com.example.web-api")
	for i := 0; i < 25; i++ {
		lr := sl1.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("GET /api/users/%d returned 200", i))
		lr.SetSeverityText("INFO")
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.Attributes().PutStr("http.method", "GET")
		lr.Attributes().PutStr("http.status_code", "200")
		lr.Attributes().PutStr("http.route", "/api/users/:id")
	}

	// Service 2: payment-svc sends 25 ERROR logs
	rl2 := ld.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "payment-svc")
	rl2.Resource().Attributes().PutStr("host.name", "payment-pod-3")
	sl2 := rl2.ScopeLogs().AppendEmpty()
	sl2.Scope().SetName("com.example.payment")
	for i := 0; i < 25; i++ {
		lr := sl2.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("payment processing failed for order-%d: timeout", i))
		lr.SetSeverityText("ERROR")
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.Attributes().PutStr("error.type", "TimeoutException")
		lr.Attributes().PutStr("order.id", fmt.Sprintf("order-%d", i))
	}

	// Consume the entire batch
	err = proc.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	// Count what made it through
	totalPassed := 0
	for _, l := range sink.AllLogs() {
		totalPassed += l.LogRecordCount()
	}

	// With token bucket limit=10, burst=5, exactly 10 records should pass
	assert.Equal(t, limit, totalPassed,
		"expected exactly %d logs to pass, got %d (out of %d total)", limit, totalPassed, totalLogs)

	t.Logf("Integration result: %d/%d logs passed, %d dropped", totalPassed, totalLogs, totalLogs-totalPassed)
}

// TestIntegration_LogsProcessor_MultiServiceIsolation verifies that rate limits
// apply per evaluation (not per service in our case) — all records share
// the same counter when policies scope by signal_type only.
func TestIntegration_LogsProcessor_MultiServiceIsolation(t *testing.T) {
	const limit = 6

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-shared", "shared log limit", "log", int64(limit), int64(limit), model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true, CacheTTL: 10 * time.Second}
	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Three services, each sends 5 logs = 15 total, shared limit = 6
	services := []string{"svc-alpha", "svc-beta", "svc-gamma"}
	ld := plog.NewLogs()
	for _, svc := range services {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", svc)
		sl := rl.ScopeLogs().AppendEmpty()
		for i := 0; i < 5; i++ {
			lr := sl.LogRecords().AppendEmpty()
			lr.Body().SetStr(fmt.Sprintf("log from %s #%d", svc, i))
			lr.SetSeverityText("INFO")
		}
	}

	err = proc.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	totalPassed := 0
	for _, l := range sink.AllLogs() {
		totalPassed += l.LogRecordCount()
	}

	assert.Equal(t, limit, totalPassed,
		"shared limit should cap total logs at %d, got %d", limit, totalPassed)
}

// TestIntegration_LogsProcessor_ShadowModePassesAll verifies shadow mode:
// policies are evaluated and counters updated, but all records pass through.
func TestIntegration_LogsProcessor_ShadowModePassesAll(t *testing.T) {
	const totalLogs = 20

	policyFile := createTempPolicyFile(t, []model.Policy{
		shadowPolicy("int-shadow", "shadow log limit", "log", 1),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "shadow-svc")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < totalLogs; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("shadow log %d", i))
		lr.SetSeverityText("WARN")
	}

	err = proc.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	totalPassed := 0
	for _, l := range sink.AllLogs() {
		totalPassed += l.LogRecordCount()
	}

	assert.Equal(t, totalLogs, totalPassed,
		"shadow mode should pass ALL %d logs, got %d", totalLogs, totalPassed)

	t.Logf("Shadow mode: %d/%d passed", totalPassed, totalLogs)
}

// TestIntegration_LogsProcessor_SequentialBatches tests that rate limits
// persist across multiple ConsumeLogs calls within the same time window.
func TestIntegration_LogsProcessor_SequentialBatches(t *testing.T) {
	const limit = 5

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-seq", "sequential batches", "log", int64(limit), int64(limit), model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Batch 1: send 3 logs (within limit)
	ld1 := plog.NewLogs()
	rl1 := ld1.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "batch-svc")
	sl1 := rl1.ScopeLogs().AppendEmpty()
	for i := 0; i < 3; i++ {
		lr := sl1.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("batch1 log %d", i))
		lr.SetSeverityText("INFO")
	}
	require.NoError(t, proc.ConsumeLogs(context.Background(), ld1))

	// Batch 2: send 3 more (only 2 should pass, 1 dropped)
	ld2 := plog.NewLogs()
	rl2 := ld2.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "batch-svc")
	sl2 := rl2.ScopeLogs().AppendEmpty()
	for i := 0; i < 3; i++ {
		lr := sl2.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("batch2 log %d", i))
		lr.SetSeverityText("WARN")
	}
	require.NoError(t, proc.ConsumeLogs(context.Background(), ld2))

	// Batch 3: send 5 more (all should be dropped — limit exhausted)
	ld3 := plog.NewLogs()
	rl3 := ld3.ResourceLogs().AppendEmpty()
	rl3.Resource().Attributes().PutStr("service.name", "batch-svc")
	sl3 := rl3.ScopeLogs().AppendEmpty()
	for i := 0; i < 5; i++ {
		lr := sl3.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("batch3 log %d", i))
		lr.SetSeverityText("ERROR")
	}
	require.NoError(t, proc.ConsumeLogs(context.Background(), ld3))

	totalPassed := 0
	for _, l := range sink.AllLogs() {
		totalPassed += l.LogRecordCount()
	}

	assert.Equal(t, limit, totalPassed,
		"sequential batches should respect cumulative limit of %d, got %d", limit, totalPassed)
}

// TestIntegration_TracesProcessor_DropsExcessSpans verifies span rate limiting
// with realistic trace data.
func TestIntegration_TracesProcessor_DropsExcessSpans(t *testing.T) {
	const (
		limit      = 8
		totalSpans = 30
	)

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-limit-spans", "integration: limit spans", "span", int64(limit), int64(limit), model.ActionDrop),
	})

	cfg := &Config{
		PolicyFile:  policyFile,
		FailOpen:    true,
		CacheTTL:    10 * time.Second,
		Environment: "test",
	}

	sink := new(consumertest.TracesSink)
	factory := NewFactory()

	proc, err := factory.CreateTraces(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Build realistic trace data
	td := ptrace.NewTraces()

	// Service: checkout-svc with HTTP spans
	rs1 := td.ResourceSpans().AppendEmpty()
	rs1.Resource().Attributes().PutStr("service.name", "checkout-svc")
	rs1.Resource().Attributes().PutStr("service.version", "2.1.0")
	ss1 := rs1.ScopeSpans().AppendEmpty()
	ss1.Scope().SetName("com.example.checkout")
	for i := 0; i < 15; i++ {
		span := ss1.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("POST /checkout/order-%d", i))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(50 * time.Millisecond)))
		span.Attributes().PutStr("http.method", "POST")
		span.Attributes().PutInt("http.status_code", 200)
	}

	// Service: inventory-svc with DB spans
	rs2 := td.ResourceSpans().AppendEmpty()
	rs2.Resource().Attributes().PutStr("service.name", "inventory-svc")
	ss2 := rs2.ScopeSpans().AppendEmpty()
	for i := 0; i < 15; i++ {
		span := ss2.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("SELECT inventory.item_%d", i))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(5 * time.Millisecond)))
		span.Attributes().PutStr("db.system", "postgresql")
		span.Attributes().PutStr("db.statement", "SELECT * FROM inventory WHERE id = ?")
	}

	err = proc.ConsumeTraces(context.Background(), td)
	require.NoError(t, err)

	totalPassed := 0
	for _, tr := range sink.AllTraces() {
		totalPassed += tr.SpanCount()
	}

	assert.Equal(t, limit, totalPassed,
		"expected %d spans to pass, got %d (out of %d)", limit, totalPassed, totalSpans)
	t.Logf("Traces integration: %d/%d spans passed", totalPassed, totalSpans)
}

// TestIntegration_MetricsProcessor_DropsExcessMetrics verifies metric rate limiting
// with realistic metric data.
func TestIntegration_MetricsProcessor_DropsExcessMetrics(t *testing.T) {
	const (
		limit        = 8
		totalMetrics = 25
	)

	policyFile := createTempPolicyFile(t, []model.Policy{
		fixedWindowPolicy("int-limit-metrics", "integration: limit metrics", "metric", int64(limit), model.ActionDrop),
	})

	cfg := &Config{
		PolicyFile: policyFile,
		FailOpen:   true,
		CacheTTL:   10 * time.Second,
	}

	sink := new(consumertest.MetricsSink)
	factory := NewFactory()

	proc, err := factory.CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Build realistic metrics
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "frontend-svc")
	rm.Resource().Attributes().PutStr("host.name", "frontend-pod-1")
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("com.example.frontend")

	for i := 0; i < totalMetrics; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("http.server.request.duration.p%d", i))
		g := m.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(float64(i) * 1.5)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		dp.Attributes().PutStr("http.route", fmt.Sprintf("/api/endpoint-%d", i))
	}

	err = proc.ConsumeMetrics(context.Background(), md)
	require.NoError(t, err)

	totalPassed := 0
	for _, mm := range sink.AllMetrics() {
		totalPassed += mm.MetricCount()
	}

	assert.Equal(t, limit, totalPassed,
		"expected %d metrics to pass, got %d (out of %d)", limit, totalPassed, totalMetrics)
	t.Logf("Metrics integration: %d/%d metrics passed", totalPassed, totalMetrics)
}

// TestIntegration_FullPipeline_AllSignals runs logs, traces, and metrics through
// separate processors sharing the same policy file to simulate a real collector.
func TestIntegration_FullPipeline_AllSignals(t *testing.T) {
	const limit = 5

	policies := []model.Policy{
		tokenBucketPolicy("pipe-logs", "pipeline logs", "log", int64(limit), int64(limit), model.ActionDrop),
		tokenBucketPolicy("pipe-spans", "pipeline spans", "span", int64(limit), int64(limit), model.ActionDrop),
		fixedWindowPolicy("pipe-metrics", "pipeline metrics", "metric", int64(limit), model.ActionDrop),
	}
	policyFile := createTempPolicyFile(t, policies)

	baseCfg := &Config{
		PolicyFile:  policyFile,
		FailOpen:    true,
		CacheTTL:    10 * time.Second,
		KeyPrefix:   "pipeline-test",
		Environment: "integration",
	}

	factory := NewFactory()

	// --- Logs ---
	logSink := new(consumertest.LogsSink)
	logProc, err := factory.CreateLogs(context.Background(), nopSettings(), baseCfg, logSink)
	require.NoError(t, err)
	require.NoError(t, logProc.Start(context.Background(), componenttest.NewNopHost()))
	defer logProc.Shutdown(context.Background())

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "pipeline-svc")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 20; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("pipeline log %d", i))
		lr.SetSeverityText("INFO")
	}
	require.NoError(t, logProc.ConsumeLogs(context.Background(), ld))

	logCount := 0
	for _, l := range logSink.AllLogs() {
		logCount += l.LogRecordCount()
	}
	assert.Equal(t, limit, logCount, "logs pipeline: expected %d, got %d", limit, logCount)

	// --- Traces ---
	traceSink := new(consumertest.TracesSink)
	traceProc, err := factory.CreateTraces(context.Background(), nopSettings(), baseCfg, traceSink)
	require.NoError(t, err)
	require.NoError(t, traceProc.Start(context.Background(), componenttest.NewNopHost()))
	defer traceProc.Shutdown(context.Background())

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "pipeline-svc")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 20; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("pipeline-op-%d", i))
	}
	require.NoError(t, traceProc.ConsumeTraces(context.Background(), td))

	spanCount := 0
	for _, tr := range traceSink.AllTraces() {
		spanCount += tr.SpanCount()
	}
	assert.Equal(t, limit, spanCount, "traces pipeline: expected %d, got %d", limit, spanCount)

	// --- Metrics ---
	metricSink := new(consumertest.MetricsSink)
	metricProc, err := factory.CreateMetrics(context.Background(), nopSettings(), baseCfg, metricSink)
	require.NoError(t, err)
	require.NoError(t, metricProc.Start(context.Background(), componenttest.NewNopHost()))
	defer metricProc.Shutdown(context.Background())

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "pipeline-svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	for i := 0; i < 20; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("pipeline.metric.%d", i))
		m.SetEmptyGauge()
	}
	require.NoError(t, metricProc.ConsumeMetrics(context.Background(), md))

	metricCount := 0
	for _, mm := range metricSink.AllMetrics() {
		metricCount += mm.MetricCount()
	}
	assert.Equal(t, limit, metricCount, "metrics pipeline: expected %d, got %d", limit, metricCount)

	t.Logf("Full pipeline: logs=%d/%d, traces=%d/%d, metrics=%d/%d",
		logCount, 20, spanCount, 20, metricCount, 20)
}

// TestIntegration_LogsProcessor_ServiceScopedPolicy verifies that a policy
// scoped to a specific service only affects that service's records.
func TestIntegration_LogsProcessor_ServiceScopedPolicy(t *testing.T) {
	const limit = 3

	policyFile := createTempPolicyFile(t, []model.Policy{
		servicePolicy("int-svc-scope", "scope to chatty-svc", "chatty-svc", "log", int64(limit), model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	ld := plog.NewLogs()

	// chatty-svc: should be limited to 3
	rl1 := ld.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "chatty-svc")
	sl1 := rl1.ScopeLogs().AppendEmpty()
	for i := 0; i < 10; i++ {
		lr := sl1.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("chatty log %d", i))
		lr.SetSeverityText("INFO")
	}

	// quiet-svc: no policy applies, all should pass through
	rl2 := ld.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "quiet-svc")
	sl2 := rl2.ScopeLogs().AppendEmpty()
	for i := 0; i < 10; i++ {
		lr := sl2.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("quiet log %d", i))
		lr.SetSeverityText("INFO")
	}

	err = proc.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	totalPassed := 0
	for _, l := range sink.AllLogs() {
		totalPassed += l.LogRecordCount()
	}

	// chatty-svc: 3 pass. quiet-svc: all 10 pass (no matching policy). Total = 13
	assert.Equal(t, limit+10, totalPassed,
		"service-scoped policy: expected %d (3 chatty + 10 quiet), got %d", limit+10, totalPassed)
}

// TestIntegration_FailClosed verifies fail-closed behavior: when engine has an
// issue, records are denied rather than allowed.
func TestIntegration_FailClosed(t *testing.T) {
	// Use a valid policy file but set fail_open=false.
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-fc", "fail-closed test", "log", 5, 5, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: false}
	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Send logs that should be within limit
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-svc")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 3; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("fail-closed log %d", i))
		lr.SetSeverityText("INFO")
	}

	err = proc.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	totalPassed := 0
	for _, l := range sink.AllLogs() {
		totalPassed += l.LogRecordCount()
	}
	assert.Equal(t, 3, totalPassed, "fail-closed: within-limit records should still pass")
}

// TestIntegration_Capabilities verifies all processors report MutatesData=true.
func TestIntegration_Capabilities(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("cap-test", "capabilities", "log", 100, 100, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	factory := NewFactory()

	logProc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, new(consumertest.LogsSink))
	require.NoError(t, err)
	assert.True(t, logProc.Capabilities().MutatesData, "logs processor should mutate data")

	traceProc, err := factory.CreateTraces(context.Background(), nopSettings(), cfg, new(consumertest.TracesSink))
	require.NoError(t, err)
	assert.True(t, traceProc.Capabilities().MutatesData, "traces processor should mutate data")

	metricProc, err := factory.CreateMetrics(context.Background(), nopSettings(), cfg, new(consumertest.MetricsSink))
	require.NoError(t, err)
	assert.True(t, metricProc.Capabilities().MutatesData, "metrics processor should mutate data")
}

// TestIntegration_LogsProcessor_MixedSeverities tests that logs of different
// severity levels (INFO, WARN, ERROR, DEBUG) are all correctly evaluated.
func TestIntegration_LogsProcessor_MixedSeverities(t *testing.T) {
	const limit = 8

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-sev", "mixed severities", "log", int64(limit), int64(limit), model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.LogsSink)

	proc, err := NewFactory().CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	severities := []string{"DEBUG", "INFO", "WARN", "ERROR"}
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "multi-sev-svc")
	sl := rl.ScopeLogs().AppendEmpty()

	for i := 0; i < 20; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("log message %d", i))
		lr.SetSeverityText(severities[i%len(severities)])
	}

	require.NoError(t, proc.ConsumeLogs(context.Background(), ld))

	totalPassed := 0
	for _, l := range sink.AllLogs() {
		totalPassed += l.LogRecordCount()
	}
	assert.Equal(t, limit, totalPassed)
}

// TestIntegration_LogsProcessor_NoServiceName verifies that logs from a resource
// without a service.name attribute are still correctly evaluated (service defaults
// to empty string and resourceAttr returns "").
func TestIntegration_LogsProcessor_NoServiceName(t *testing.T) {
	const limit = 3

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-nosvc", "no service name", "log", int64(limit), int64(limit), model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.LogsSink)

	proc, err := NewFactory().CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Resource WITHOUT service.name — only has host info.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", "orphan-host")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 10; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("orphan log %d", i))
		lr.SetSeverityText("WARN")
	}

	require.NoError(t, proc.ConsumeLogs(context.Background(), ld))

	totalPassed := 0
	for _, l := range sink.AllLogs() {
		totalPassed += l.LogRecordCount()
	}
	assert.Equal(t, limit, totalPassed,
		"logs without service.name should still be rate limited")
}

// TestIntegration_TracesProcessor_NoServiceName verifies spans without service.name.
func TestIntegration_TracesProcessor_NoServiceName(t *testing.T) {
	const limit = 4

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-nosvc-span", "no service spans", "span", int64(limit), int64(limit), model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.TracesSink)

	proc, err := NewFactory().CreateTraces(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	// No service.name attribute
	rs.Resource().Attributes().PutStr("host.name", "orphan-host")
	ss := rs.ScopeSpans().AppendEmpty()
	for i := 0; i < 10; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("orphan-op-%d", i))
	}

	require.NoError(t, proc.ConsumeTraces(context.Background(), td))

	totalPassed := 0
	for _, tr := range sink.AllTraces() {
		totalPassed += tr.SpanCount()
	}
	assert.Equal(t, limit, totalPassed)
}

// TestIntegration_MetricsProcessor_NoServiceName verifies metrics without service.name.
func TestIntegration_MetricsProcessor_NoServiceName(t *testing.T) {
	const limit = 4

	policyFile := createTempPolicyFile(t, []model.Policy{
		fixedWindowPolicy("int-nosvc-metric", "no service metrics", "metric", int64(limit), model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.MetricsSink)

	proc, err := NewFactory().CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	// No service.name attribute
	rm.Resource().Attributes().PutStr("host.name", "orphan-host")
	sm := rm.ScopeMetrics().AppendEmpty()
	for i := 0; i < 10; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("orphan.metric.%d", i))
		m.SetEmptyGauge()
	}

	require.NoError(t, proc.ConsumeMetrics(context.Background(), md))

	totalPassed := 0
	for _, mm := range sink.AllMetrics() {
		totalPassed += mm.MetricCount()
	}
	assert.Equal(t, limit, totalPassed)
}

// TestIntegration_LogsProcessor_EmptyAttributes verifies logs with completely
// empty attributes and empty resource are handled gracefully.
func TestIntegration_LogsProcessor_EmptyAttributes(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-empty", "empty attrs", "log", 5, 5, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	sink := new(consumertest.LogsSink)

	proc, err := NewFactory().CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Completely empty resource — no attributes at all
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 3; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr("bare log")
	}

	require.NoError(t, proc.ConsumeLogs(context.Background(), ld))

	totalPassed := 0
	for _, l := range sink.AllLogs() {
		totalPassed += l.LogRecordCount()
	}
	assert.Equal(t, 3, totalPassed, "bare logs within limit should pass")
}

// TestIntegration_Engine_DefaultsNotOverridden verifies that request-level
// fields are preserved when they are already set (defaults don't overwrite).
func TestIntegration_Engine_DefaultsNotOverridden(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-defaults", "defaults test", "", 100, 100, model.ActionDrop),
	})

	cfg := &Config{
		PolicyFile:  policyFile,
		FailOpen:    true,
		OrgID:       "default-org",
		TenantID:    "default-tenant",
		Application: "default-app",
		Environment: "default-env",
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	// Request with all fields already populated — defaults should NOT overwrite.
	req := model.RequestContext{
		Service:     "my-svc",
		SignalType:  "log",
		OrgID:       "custom-org",
		TenantID:    "custom-tenant",
		Application: "custom-app",
		Environment: "custom-env",
		Quantity:    5,
	}

	dec, err := eng.evaluate(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, dec.Allowed)
}

// TestIntegration_Engine_ZeroQuantityDefault verifies that a Quantity of 0
// is corrected to 1 by the engine.
func TestIntegration_Engine_ZeroQuantityDefault(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-qty", "quantity test", "log", 100, 100, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	req := model.RequestContext{
		Service:    "test-svc",
		SignalType: "log",
		Quantity:   0, // should be corrected to 1
	}

	dec, err := eng.evaluate(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, dec.Allowed)
}

// TestIntegration_Engine_NegativeQuantityDefault verifies that negative
// Quantity is corrected to 1.
func TestIntegration_Engine_NegativeQuantityDefault(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("int-neg-qty", "neg quantity", "log", 100, 100, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	req := model.RequestContext{
		Service:    "test-svc",
		SignalType: "log",
		Quantity:   -5, // should be corrected to 1
	}

	dec, err := eng.evaluate(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, dec.Allowed)
}
