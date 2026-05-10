// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestBuildLogsContext(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "test-service")
	resource.Attributes().PutStr("deployment.environment", "production")

	lr := plog.NewLogRecord()
	lr.SetSeverityText("INFO")
	lr.Attributes().PutStr("log.file.name", "app.log")

	ctx := buildLogsContext(resource, lr)

	assert.Equal(t, "test-service", ctx.Service)
	assert.Equal(t, "log", ctx.SignalType)
	assert.Equal(t, "INFO", ctx.Severity)
	assert.Equal(t, "otel_log", ctx.Operation)
	assert.Equal(t, int64(1), ctx.Quantity)
	assert.NotNil(t, ctx.Tags)
	assert.NotNil(t, ctx.Attributes)
}

func TestBuildTracesContext(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "trace-service")
	resource.Attributes().PutStr("service.version", "v1.0.0")

	span := ptrace.NewSpan()
	span.SetName("GET /api/users")
	span.Attributes().PutStr("http.method", "GET")
	span.Attributes().PutStr("http.url", "/api/users")

	ctx := buildTracesContext(resource, span)

	assert.Equal(t, "trace-service", ctx.Service)
	assert.Equal(t, "span", ctx.SignalType)
	assert.Equal(t, "GET /api/users", ctx.SpanName)
	assert.Equal(t, "otel_span", ctx.Operation)
	assert.Equal(t, int64(1), ctx.Quantity)
	assert.NotNil(t, ctx.Tags)
	assert.NotNil(t, ctx.Attributes)
}

func TestBuildMetricsContext(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "metrics-service")
	resource.Attributes().PutStr("host.name", "server-01")

	metric := pmetric.NewMetric()
	metric.SetName("http.server.duration")

	ctx := buildMetricsContext(resource, metric)

	assert.Equal(t, "metrics-service", ctx.Service)
	assert.Equal(t, "metric", ctx.SignalType)
	assert.Equal(t, "http.server.duration", ctx.Resource)
	assert.Equal(t, "otel_metric", ctx.Operation)
	assert.Equal(t, int64(1), ctx.Quantity)
	assert.NotNil(t, ctx.Tags)
	assert.NotNil(t, ctx.Attributes)
}

func TestBuildLogsContextWithSelectors(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "default-service")
	resource.Attributes().PutStr("k8s.deployment.name", "my-app")
	resource.Attributes().PutStr("org.id", "acme-corp")

	lr := plog.NewLogRecord()
	lr.SetSeverityText("ERROR")
	lr.Attributes().PutStr("custom.service", "override-service")
	lr.Attributes().PutStr("tenant.id", "tenant-123")

	selectors := fieldSelectors{
		service:  "attributes.custom.service || k8s.deployment.name",
		orgID:    "resource.attributes.org.id",
		tenantID: "attributes.tenant.id",
	}

	ctx := buildLogsContextWithSelectors(resource, lr, selectors)

	assert.Equal(t, "override-service", ctx.Service, "should use attributes.custom.service first")
	assert.Equal(t, "acme-corp", ctx.OrgID, "should extract org from resource")
	assert.Equal(t, "tenant-123", ctx.TenantID, "should extract tenant from record attributes")
	assert.Equal(t, "log", ctx.SignalType)
	assert.Equal(t, "ERROR", ctx.Severity)
}

func TestBuildTracesContextWithSelectors(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "default-service")
	resource.Attributes().PutStr("deployment.environment", "staging")

	span := ptrace.NewSpan()
	span.SetName("database.query")
	span.Attributes().PutStr("service.override", "db-service")
	span.Attributes().PutStr("app.name", "inventory")

	selectors := fieldSelectors{
		service:     "attributes.service.override || service.name",
		environment: "resource.attributes.deployment.environment",
		application: "attributes.app.name",
	}

	ctx := buildTracesContextWithSelectors(resource, span, selectors)

	assert.Equal(t, "db-service", ctx.Service)
	assert.Equal(t, "staging", ctx.Environment)
	assert.Equal(t, "inventory", ctx.Application)
	assert.Equal(t, "span", ctx.SignalType)
	assert.Equal(t, "database.query", ctx.SpanName)
}

func TestBuildMetricsContextWithSelectors(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("service.name", "metrics-svc")
	resource.Attributes().PutStr("cloud.region", "us-east-1")

	metric := pmetric.NewMetric()
	metric.SetName("cpu.utilization")

	selectors := fieldSelectors{
		service:     "service.name",
		environment: "resource.attributes.cloud.region || 'production'",
	}

	ctx := buildMetricsContextWithSelectors(resource, metric, selectors)

	assert.Equal(t, "metrics-svc", ctx.Service)
	assert.Equal(t, "us-east-1", ctx.Environment)
	assert.Equal(t, "metric", ctx.SignalType)
	assert.Equal(t, "cpu.utilization", ctx.Resource)
}

func TestResourceAttr(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("key1", "value1")
	resource.Attributes().PutInt("key2", 42)

	assert.Equal(t, "value1", resourceAttr(resource, "key1"))
	assert.Equal(t, "42", resourceAttr(resource, "key2"))
	assert.Equal(t, "", resourceAttr(resource, "nonexistent"))
}

func TestExtractAllAttributes(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("resource.key", "resource.value")
	resource.Attributes().PutStr("service.name", "my-service")

	recordAttrs := pcommon.NewMap()
	recordAttrs.PutStr("record.key", "record.value")
	recordAttrs.PutStr("service.name", "override-service")

	merged := extractAllAttributes(resource, recordAttrs)

	assert.Equal(t, "resource.value", merged["resource.key"], "should include resource attributes")
	assert.Equal(t, "record.value", merged["record.key"], "should include record attributes")
	assert.Equal(t, "override-service", merged["service.name"], "record attrs should override resource attrs")
}

func TestExtractResourceAttributes(t *testing.T) {
	resource := pcommon.NewResource()
	resource.Attributes().PutStr("string.key", "string.value")
	resource.Attributes().PutInt("int.key", 123)
	resource.Attributes().PutBool("bool.key", true)
	resource.Attributes().PutDouble("double.key", 45.67)

	attrs := extractResourceAttributes(resource)

	assert.Equal(t, "string.value", attrs["string.key"])
	assert.Equal(t, "123", attrs["int.key"])
	assert.Equal(t, "true", attrs["bool.key"])
	assert.Equal(t, "45.67", attrs["double.key"])
}
