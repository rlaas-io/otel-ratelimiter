// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import (
	"github.com/rlaas-io/rlaas/pkg/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// buildLogsContext builds a RLAAS RequestContext from an OTel log record.
func buildLogsContext(resource pcommon.Resource, lr plog.LogRecord) model.RequestContext {
	return buildLogsContextWithSelectors(resource, lr, fieldSelectors{})
}

func buildLogsContextWithSelectors(resource pcommon.Resource, lr plog.LogRecord, selectors fieldSelectors) model.RequestContext {
	resourceAttrs := extractResourceAttributes(resource)
	tags := extractAllAttributes(resource, lr.Attributes())
	service := evalSelector(selectors.service, resourceAttrs, tags)
	if service == "" {
		service = resourceAttr(resource, "service.name")
	}

	return model.RequestContext{
		Service:     service,
		SignalType:  "log",
		Severity:    lr.SeverityText(),
		Operation:   "otel_log",
		OrgID:       evalSelector(selectors.orgID, resourceAttrs, tags),
		TenantID:    evalSelector(selectors.tenantID, resourceAttrs, tags),
		Application: evalSelector(selectors.application, resourceAttrs, tags),
		Environment: evalSelector(selectors.environment, resourceAttrs, tags),
		Quantity:    1,
		Tags:        tags,
		Attributes:  tags,
	}
}

// buildTracesContext builds a RLAAS RequestContext from an OTel span.
func buildTracesContext(resource pcommon.Resource, span ptrace.Span) model.RequestContext {
	return buildTracesContextWithSelectors(resource, span, fieldSelectors{})
}

func buildTracesContextWithSelectors(resource pcommon.Resource, span ptrace.Span, selectors fieldSelectors) model.RequestContext {
	resourceAttrs := extractResourceAttributes(resource)
	tags := extractAllAttributes(resource, span.Attributes())
	service := evalSelector(selectors.service, resourceAttrs, tags)
	if service == "" {
		service = resourceAttr(resource, "service.name")
	}

	return model.RequestContext{
		Service:     service,
		SignalType:  "span",
		SpanName:    span.Name(),
		Operation:   "otel_span",
		OrgID:       evalSelector(selectors.orgID, resourceAttrs, tags),
		TenantID:    evalSelector(selectors.tenantID, resourceAttrs, tags),
		Application: evalSelector(selectors.application, resourceAttrs, tags),
		Environment: evalSelector(selectors.environment, resourceAttrs, tags),
		Quantity:    1,
		Tags:        tags,
		Attributes:  tags,
	}
}

// buildMetricsContext builds a RLAAS RequestContext from an OTel metric.
func buildMetricsContext(resource pcommon.Resource, metric pmetric.Metric) model.RequestContext {
	return buildMetricsContextWithSelectors(resource, metric, fieldSelectors{})
}

func buildMetricsContextWithSelectors(resource pcommon.Resource, metric pmetric.Metric, selectors fieldSelectors) model.RequestContext {
	resourceAttrs := extractResourceAttributes(resource)
	tags := extractResourceAttributes(resource)
	service := evalSelector(selectors.service, resourceAttrs, tags)
	if service == "" {
		service = resourceAttr(resource, "service.name")
	}

	return model.RequestContext{
		Service:     service,
		SignalType:  "metric",
		Resource:    metric.Name(),
		Operation:   "otel_metric",
		OrgID:       evalSelector(selectors.orgID, resourceAttrs, tags),
		TenantID:    evalSelector(selectors.tenantID, resourceAttrs, tags),
		Application: evalSelector(selectors.application, resourceAttrs, tags),
		Environment: evalSelector(selectors.environment, resourceAttrs, tags),
		Quantity:    1,
		Tags:        tags,
		Attributes:  tags,
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
