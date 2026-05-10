package ratelimiterprocessor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rlaas-io/rlaas/pkg/model"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// Benchmarks: measure throughput of the rate limiter processor under load
// ---------------------------------------------------------------------------

// --- Helpers to build pdata batches for benchmarks ---

func buildLogBatch(n int, service, severity string) plog.Logs {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", service)
	rl.Resource().Attributes().PutStr("host.name", "bench-host")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < n; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("benchmark log message %d", i))
		lr.SetSeverityText(severity)
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.Attributes().PutStr("bench.index", fmt.Sprintf("%d", i))
	}
	return ld
}

func buildTraceBatch(n int, service, spanName string) ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", service)
	ss := rs.ScopeSpans().AppendEmpty()
	now := time.Now()
	for i := 0; i < n; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName(fmt.Sprintf("%s-%d", spanName, i))
		span.SetStartTimestamp(pcommon.NewTimestampFromTime(now))
		span.SetEndTimestamp(pcommon.NewTimestampFromTime(now.Add(10 * time.Millisecond)))
		span.Attributes().PutStr("bench.index", fmt.Sprintf("%d", i))
	}
	return td
}

func buildMetricBatch(n int, service string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", service)
	sm := rm.ScopeMetrics().AppendEmpty()
	for i := 0; i < n; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("bench.metric.%d", i))
		g := m.SetEmptyGauge()
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(float64(i))
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	}
	return md
}

// --- Benchmark: Logs Processor ---

func BenchmarkLogsProcessor_AllAllowed(b *testing.B) {
	// High limit so nothing is dropped — measures pure evaluation overhead.
	policyFile := createTempPolicyFile(b, []model.Policy{
		tokenBucketPolicy("bench-allow", "bench allow", "log", 1_000_000, 1_000_000, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true, CacheTTL: 60 * time.Second}
	sink := new(consumertest.LogsSink)
	proc, _ := NewFactory().CreateLogs(context.Background(), nopSettings(), cfg, sink)
	proc.Start(context.Background(), componenttest.NewNopHost())
	defer proc.Shutdown(context.Background())

	batch := buildLogBatch(100, "bench-svc", "INFO")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Clone to avoid mutation artifacts across iterations.
		ld := plog.NewLogs()
		batch.CopyTo(ld)
		proc.ConsumeLogs(context.Background(), ld)
	}
}

func BenchmarkLogsProcessor_HalfDropped(b *testing.B) {
	// Limit = 50, batch = 100 → 50 allowed, 50 dropped per batch.
	policyFile := createTempPolicyFile(b, []model.Policy{
		tokenBucketPolicy("bench-half", "bench half", "log", 1_000_000, 50, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true, CacheTTL: 60 * time.Second}
	sink := new(consumertest.LogsSink)
	proc, _ := NewFactory().CreateLogs(context.Background(), nopSettings(), cfg, sink)
	proc.Start(context.Background(), componenttest.NewNopHost())
	defer proc.Shutdown(context.Background())

	batch := buildLogBatch(100, "bench-svc", "INFO")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ld := plog.NewLogs()
		batch.CopyTo(ld)
		proc.ConsumeLogs(context.Background(), ld)
	}
}

func BenchmarkLogsProcessor_ShadowMode(b *testing.B) {
	// Shadow mode: evaluate but pass everything — measures overhead of evaluation.
	policyFile := createTempPolicyFile(b, []model.Policy{
		shadowPolicy("bench-shadow", "bench shadow", "log", 10),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true, CacheTTL: 60 * time.Second}
	sink := new(consumertest.LogsSink)
	proc, _ := NewFactory().CreateLogs(context.Background(), nopSettings(), cfg, sink)
	proc.Start(context.Background(), componenttest.NewNopHost())
	defer proc.Shutdown(context.Background())

	batch := buildLogBatch(100, "bench-svc", "WARN")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ld := plog.NewLogs()
		batch.CopyTo(ld)
		proc.ConsumeLogs(context.Background(), ld)
	}
}

// --- Benchmark: Traces Processor ---

func BenchmarkTracesProcessor_AllAllowed(b *testing.B) {
	policyFile := createTempPolicyFile(b, []model.Policy{
		tokenBucketPolicy("bench-traces", "bench traces", "span", 1_000_000, 1_000_000, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true, CacheTTL: 60 * time.Second}
	sink := new(consumertest.TracesSink)
	proc, _ := NewFactory().CreateTraces(context.Background(), nopSettings(), cfg, sink)
	proc.Start(context.Background(), componenttest.NewNopHost())
	defer proc.Shutdown(context.Background())

	batch := buildTraceBatch(100, "bench-svc", "GET /api")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		td := ptrace.NewTraces()
		batch.CopyTo(td)
		proc.ConsumeTraces(context.Background(), td)
	}
}

func BenchmarkTracesProcessor_HalfDropped(b *testing.B) {
	policyFile := createTempPolicyFile(b, []model.Policy{
		tokenBucketPolicy("bench-traces-half", "bench traces half", "span", 1_000_000, 50, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true, CacheTTL: 60 * time.Second}
	sink := new(consumertest.TracesSink)
	proc, _ := NewFactory().CreateTraces(context.Background(), nopSettings(), cfg, sink)
	proc.Start(context.Background(), componenttest.NewNopHost())
	defer proc.Shutdown(context.Background())

	batch := buildTraceBatch(100, "bench-svc", "GET /api")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		td := ptrace.NewTraces()
		batch.CopyTo(td)
		proc.ConsumeTraces(context.Background(), td)
	}
}

// --- Benchmark: Metrics Processor ---

func BenchmarkMetricsProcessor_AllAllowed(b *testing.B) {
	policyFile := createTempPolicyFile(b, []model.Policy{
		tokenBucketPolicy("bench-metrics", "bench metrics", "metric", 1_000_000, 1_000_000, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true, CacheTTL: 60 * time.Second}
	sink := new(consumertest.MetricsSink)
	proc, _ := NewFactory().CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	proc.Start(context.Background(), componenttest.NewNopHost())
	defer proc.Shutdown(context.Background())

	batch := buildMetricBatch(100, "bench-svc")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		md := pmetric.NewMetrics()
		batch.CopyTo(md)
		proc.ConsumeMetrics(context.Background(), md)
	}
}

func BenchmarkMetricsProcessor_HalfDropped(b *testing.B) {
	policyFile := createTempPolicyFile(b, []model.Policy{
		tokenBucketPolicy("bench-metrics-half", "bench metrics half", "metric", 1_000_000, 50, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true, CacheTTL: 60 * time.Second}
	sink := new(consumertest.MetricsSink)
	proc, _ := NewFactory().CreateMetrics(context.Background(), nopSettings(), cfg, sink)
	proc.Start(context.Background(), componenttest.NewNopHost())
	defer proc.Shutdown(context.Background())

	batch := buildMetricBatch(100, "bench-svc")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		md := pmetric.NewMetrics()
		batch.CopyTo(md)
		proc.ConsumeMetrics(context.Background(), md)
	}
}

// --- Benchmark: Engine evaluate (raw, no OTel wrapper) ---

func BenchmarkEngine_Evaluate(b *testing.B) {
	policyFile := createTempPolicyFile(b, []model.Policy{
		tokenBucketPolicy("bench-engine", "bench engine", "log", 1_000_000, 1_000_000, model.ActionDrop),
	})

	cfg := &Config{
		PolicyFile:  policyFile,
		FailOpen:    true,
		CacheTTL:    60 * time.Second,
		OrgID:       "bench-org",
		Environment: "bench",
	}

	eng, err := newEngine(cfg, zap.NewNop())
	if err != nil {
		b.Fatalf("newEngine: %v", err)
	}

	req := model.RequestContext{
		Service:    "bench-svc",
		SignalType: "log",
		Severity:   "INFO",
		Quantity:   1,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		eng.evaluate(context.Background(), req)
	}
}

// --- Benchmark: Context builder ---

func BenchmarkBuildLogsContext(b *testing.B) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "bench-svc")
	rl.Resource().Attributes().PutStr("host.name", "bench-host")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Body().SetStr("benchmark log")
	lr.SetSeverityText("INFO")
	lr.Attributes().PutStr("http.method", "GET")
	lr.Attributes().PutStr("http.status_code", "200")

	resource := rl.Resource()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buildLogsContext(resource, lr)
	}
}

func BenchmarkBuildTracesContext(b *testing.B) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "bench-svc")
	ss := rs.ScopeSpans().AppendEmpty()
	span := ss.Spans().AppendEmpty()
	span.SetName("GET /api/bench")
	span.Attributes().PutStr("http.method", "GET")
	span.Attributes().PutInt("http.status_code", 200)

	resource := rs.Resource()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buildTracesContext(resource, span)
	}
}

func BenchmarkBuildMetricsContext(b *testing.B) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "bench-svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http.server.request.duration")
	m.SetEmptyGauge()

	resource := rm.Resource()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buildMetricsContext(resource, m)
	}
}

// --- Benchmark: Batch sizes ---

func BenchmarkLogsProcessor_BatchSize10(b *testing.B) {
	benchLogsWithBatchSize(b, 10)
}

func BenchmarkLogsProcessor_BatchSize100(b *testing.B) {
	benchLogsWithBatchSize(b, 100)
}

func BenchmarkLogsProcessor_BatchSize1000(b *testing.B) {
	benchLogsWithBatchSize(b, 1000)
}

func benchLogsWithBatchSize(b *testing.B, size int) {
	b.Helper()
	policyFile := createTempPolicyFile(b, []model.Policy{
		tokenBucketPolicy("bench-batch", "bench batch", "log", 1_000_000, 1_000_000, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true, CacheTTL: 60 * time.Second}
	sink := new(consumertest.LogsSink)
	proc, _ := NewFactory().CreateLogs(context.Background(), nopSettings(), cfg, sink)
	proc.Start(context.Background(), componenttest.NewNopHost())
	defer proc.Shutdown(context.Background())

	batch := buildLogBatch(size, "bench-svc", "INFO")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ld := plog.NewLogs()
		batch.CopyTo(ld)
		proc.ConsumeLogs(context.Background(), ld)
	}
}
