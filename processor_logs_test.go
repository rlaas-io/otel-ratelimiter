package ratelimiterprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/rlaas-io/rlaas/pkg/model"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogsProcessor_Basic(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "limit-logs", "log", 5, 5, model.ActionDrop),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Create 10 log records from one service.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "web-api")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 10; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr("test log message")
		lr.SetSeverityText("INFO")
	}

	err = proc.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	// With limit=5, first 5 logs should pass, rest dropped.
	allLogs := sink.AllLogs()
	require.Len(t, allLogs, 1)

	totalRecords := 0
	for _, l := range allLogs {
		totalRecords += l.LogRecordCount()
	}
	assert.Equal(t, 5, totalRecords, "expected 5 log records to pass through")
}

func TestLogsProcessor_AllDropped(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "limit-logs", "log", 2, 2, model.ActionDrop),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// First batch: exhaust the limit.
	ld1 := plog.NewLogs()
	rl1 := ld1.ResourceLogs().AppendEmpty()
	rl1.Resource().Attributes().PutStr("service.name", "web-api")
	sl1 := rl1.ScopeLogs().AppendEmpty()
	for i := 0; i < 2; i++ {
		lr := sl1.LogRecords().AppendEmpty()
		lr.Body().SetStr("allowed log")
		lr.SetSeverityText("INFO")
	}
	require.NoError(t, proc.ConsumeLogs(context.Background(), ld1))

	// Second batch: should be entirely dropped.
	ld2 := plog.NewLogs()
	rl2 := ld2.ResourceLogs().AppendEmpty()
	rl2.Resource().Attributes().PutStr("service.name", "web-api")
	sl2 := rl2.ScopeLogs().AppendEmpty()
	for i := 0; i < 5; i++ {
		lr := sl2.LogRecords().AppendEmpty()
		lr.Body().SetStr("dropped log")
		lr.SetSeverityText("INFO")
	}
	err = proc.ConsumeLogs(context.Background(), ld2)
	require.NoError(t, err)

	// Only the first batch's 2 records should survive; the second batch is fully dropped.
	// Note: processorhelper still forwards the empty batch to the sink, so we check
	// total record count rather than batch count.
	totalRecords := 0
	for _, l := range sink.AllLogs() {
		totalRecords += l.LogRecordCount()
	}
	assert.Equal(t, 2, totalRecords)
}

func TestLogsProcessor_ShadowMode(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		shadowPolicy("p1", "shadow-logs", "log", 2),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	// Send 5 logs with limit of 2 in shadow mode — all should pass.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "web-api")
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 5; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetStr("shadow log")
		lr.SetSeverityText("INFO")
	}

	err = proc.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)

	totalRecords := 0
	for _, l := range sink.AllLogs() {
		totalRecords += l.LogRecordCount()
	}
	assert.Equal(t, 5, totalRecords, "shadow mode should pass all records")
}

func TestLogsProcessor_EmptyLogs(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "test", "log", 100, 100, model.ActionDrop),
	})
	cfg := &Config{PolicyFile: policyFile, FailOpen: true}

	sink := new(consumertest.LogsSink)
	factory := NewFactory()

	proc, err := factory.CreateLogs(context.Background(), nopSettings(), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	defer proc.Shutdown(context.Background())

	ld := plog.NewLogs()
	err = proc.ConsumeLogs(context.Background(), ld)
	require.NoError(t, err)
}
