package ratelimiterprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/plog"
)

func TestEvalSelector(t *testing.T) {
	resource := map[string]string{
		"service.name": "checkout",
		"org.id":       "org-r",
	}
	merged := map[string]string{
		"service":                "checkout-alt",
		"tenant.id":              "tenant-1",
		"deployment.environment": "prod",
	}

	assert.Equal(t, "checkout", evalSelector("resource.attributes.service.name", resource, merged))
	assert.Equal(t, "tenant-1", evalSelector("attributes.tenant.id", resource, merged))
	assert.Equal(t, "checkout-alt", evalSelector("service", resource, merged))
	assert.Equal(t, "fallback", evalSelector("missing.key || \"fallback\"", resource, merged))
	assert.Equal(t, "", evalSelector("", resource, merged))
}

func TestEvalSelectorTags(t *testing.T) {
	resource := map[string]string{
		"service.name": "my-service",
	}
	merged := map[string]string{
		"custom.tag": "tag-value",
		"env":        "production",
	}

	// Test tags. prefix
	assert.Equal(t, "tag-value", evalSelector("tags.custom.tag", resource, merged))
	assert.Equal(t, "production", evalSelector("tags.env || 'default'", resource, merged))
	
	// Test single-quoted literals
	assert.Equal(t, "literal-value", evalSelector("'literal-value'", resource, merged))
	assert.Equal(t, "quoted", evalSelector("missing.key || 'quoted'", resource, merged))
	
	// Test double-quoted literals
	assert.Equal(t, "double-quoted", evalSelector("\"double-quoted\"", resource, merged))
	
	// Test empty token handling
	assert.Equal(t, "production", evalSelector(" || || env", resource, merged))
	
	// Test whitespace trimming
	assert.Equal(t, "my-service", evalSelector("  resource.attributes.service.name  ", resource, merged))
	
	// Test fallback chain
	assert.Equal(t, "my-service", evalSelector("missing1 || missing2 || service.name", resource, merged))
	
	// Test empty result
	assert.Equal(t, "", evalSelector("missing.key", resource, merged))
}

func TestEvalSelectorFallbackPriority(t *testing.T) {
	resource := map[string]string{
		"key": "resource-value",
	}
	merged := map[string]string{
		"key": "merged-value",
	}

	// Merged attrs take precedence over resource for bare keys
	assert.Equal(t, "merged-value", evalSelector("key", resource, merged))
	
	// Explicit resource.attributes prefix forces resource lookup
	assert.Equal(t, "resource-value", evalSelector("resource.attributes.key", resource, merged))
	
	// Attributes prefix forces merged lookup
	assert.Equal(t, "merged-value", evalSelector("attributes.key", resource, merged))
}

func TestSelectorsFromConfig(t *testing.T) {
	cfg := &Config{
		ServiceExpr:     "  service.name  ",
		OrgIDExpr:       "org.id",
		TenantIDExpr:    " tenant.id ",
		ApplicationExpr: "",
		EnvironmentExpr: "env",
	}

	selectors := selectorsFromConfig(cfg)

	assert.Equal(t, "service.name", selectors.service)
	assert.Equal(t, "org.id", selectors.orgID)
	assert.Equal(t, "tenant.id", selectors.tenantID)
	assert.Equal(t, "", selectors.application)
	assert.Equal(t, "env", selectors.environment)
}

func TestBuildLogsContextWithSelectorsIntegration(t *testing.T) {
	logs := plog.NewLogs()
	rl := logs.ResourceLogs().AppendEmpty()
	r := rl.Resource().Attributes()
	r.PutStr("service.name", "resource-svc")
	r.PutStr("org.id", "resource-org")

	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.Attributes().PutStr("tenant.id", "tenant-42")
	lr.Attributes().PutStr("deployment.environment", "staging")
	lr.Attributes().PutStr("app.name", "api-gateway")

	selectors := fieldSelectors{
		service:     "attributes.service || resource.attributes.service.name",
		orgID:       "resource.attributes.org.id",
		tenantID:    "attributes.tenant.id",
		application: "attributes.app.name",
		environment: "attributes.deployment.environment",
	}

	req := buildLogsContextWithSelectors(rl.Resource(), lr, selectors)
	assert.Equal(t, "resource-svc", req.Service)
	assert.Equal(t, "resource-org", req.OrgID)
	assert.Equal(t, "tenant-42", req.TenantID)
	assert.Equal(t, "api-gateway", req.Application)
	assert.Equal(t, "staging", req.Environment)
}
