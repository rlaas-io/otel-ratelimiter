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

func TestBuildLogsContextWithSelectors(t *testing.T) {
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
