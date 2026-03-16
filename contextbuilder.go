package ratelimiterprocessor

import (
	"github.com/suresh-p26/RLAAS/pkg/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// buildLogsContext builds a RLAAS RequestContext from an OTel log record.
func buildLogsContext(resource pcommon.Resource, lr plog.LogRecord) model.RequestContext {
	tags := extractAllAttributes(resource, lr.Attributes())

	return model.RequestContext{
		Service:    resourceAttr(resource, "service.name"),
		SignalType: "log",
		Severity:   lr.SeverityText(),
		Operation:  "otel_log",
		Quantity:   1,
		Tags:       tags,
		Attributes: tags,
	}
}

// buildTracesContext builds a RLAAS RequestContext from an OTel span.
func buildTracesContext(resource pcommon.Resource, span ptrace.Span) model.RequestContext {
	tags := extractAllAttributes(resource, span.Attributes())

	return model.RequestContext{
		Service:    resourceAttr(resource, "service.name"),
		SignalType: "span",
		SpanName:   span.Name(),
		Operation:  "otel_span",
		Quantity:   1,
		Tags:       tags,
		Attributes: tags,
	}
}

// buildMetricsContext builds a RLAAS RequestContext from an OTel metric.
func buildMetricsContext(resource pcommon.Resource, metric pmetric.Metric) model.RequestContext {
	tags := extractResourceAttributes(resource)

	return model.RequestContext{
		Service:    resourceAttr(resource, "service.name"),
		SignalType: "metric",
		Resource:   metric.Name(),
		Operation:  "otel_metric",
		Quantity:   1,
		Tags:       tags,
		Attributes: tags,
	}
}

// resourceAttr returns a single string attribute from the resource, or "" if absent.
func resourceAttr(resource pcommon.Resource, key string) string {
	if v, ok := resource.Attributes().Get(key); ok {
		return v.AsString()
	}
	return ""
}

// extractAllAttributes merges resource-level and record-level attributes
// into a flat map suitable for RLAAS tags/attributes.
func extractAllAttributes(resource pcommon.Resource, recordAttrs pcommon.Map) map[string]string {
	result := make(map[string]string)

	// Resource attributes come first.
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})

	// Record-level attributes override resource-level.
	recordAttrs.Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})

	return result
}

// extractResourceAttributes extracts only resource-level attributes.
func extractResourceAttributes(resource pcommon.Resource) map[string]string {
	result := make(map[string]string)
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		result[k] = v.AsString()
		return true
	})
	return result
}
