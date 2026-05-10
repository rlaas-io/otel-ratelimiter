// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ratelimiterprocessor // import "github.com/rlaas-io/otel-ratelimiter"

import "strings"

// fieldSelectors define optional attribute lookup expressions for request fields.
type fieldSelectors struct {
	service     string
	orgID       string
	tenantID    string
	application string
	environment string
}

func selectorsFromConfig(cfg *Config) fieldSelectors {
	return fieldSelectors{
		service:     strings.TrimSpace(cfg.ServiceExpr),
		orgID:       strings.TrimSpace(cfg.OrgIDExpr),
		tenantID:    strings.TrimSpace(cfg.TenantIDExpr),
		application: strings.TrimSpace(cfg.ApplicationExpr),
		environment: strings.TrimSpace(cfg.EnvironmentExpr),
	}
}

// evalSelector resolves the first non-empty token in expr.
// Tokens are separated by "||".
func evalSelector(expr string, resourceAttrs, mergedAttrs map[string]string) string {
	if strings.TrimSpace(expr) == "" {
		return ""
	}
	parts := strings.Split(expr, "||")
	for _, raw := range parts {
		token := strings.TrimSpace(raw)
		if token == "" {
			continue
		}
		if strings.HasPrefix(token, "resource.attributes.") {
			key := strings.TrimPrefix(token, "resource.attributes.")
			if val := strings.TrimSpace(resourceAttrs[key]); val != "" {
				return val
			}
			continue
		}
		if strings.HasPrefix(token, "attributes.") {
			key := strings.TrimPrefix(token, "attributes.")
			if val := strings.TrimSpace(mergedAttrs[key]); val != "" {
				return val
			}
			continue
		}
		if strings.HasPrefix(token, "tags.") {
			key := strings.TrimPrefix(token, "tags.")
			if val := strings.TrimSpace(mergedAttrs[key]); val != "" {
				return val
			}
			continue
		}

		if val := strings.TrimSpace(mergedAttrs[token]); val != "" {
			return val
		}
		if val := strings.TrimSpace(resourceAttrs[token]); val != "" {
			return val
		}

		// Quoted literal fallback for static mapping expressions.
		if len(token) >= 2 {
			if token[0] == '"' && token[len(token)-1] == '"' {
				return token[1 : len(token)-1]
			}
			if token[0] == '\'' && token[len(token)-1] == '\'' {
				return token[1 : len(token)-1]
			}
		}
	}
	return ""
}
