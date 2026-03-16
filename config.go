package ratelimiterprocessor

import (
	"errors"
	"time"
)

// Config defines the configuration for the rate limiter processor.
// Policies are defined in a RLAAS policy JSON file referenced by PolicyFile.
// The RLAAS engine handles algorithm selection, matching, and decisions.
type Config struct {
	// PolicyFile is the path to a RLAAS policy JSON file.
	// The file should contain an array of model.Policy objects (or a RLAAS
	// payload struct with "policies", "audits", "versions" keys).
	// Required.
	PolicyFile string `mapstructure:"policy_file"`

	// FailOpen determines behavior when the RLAAS engine encounters an error.
	// If true, records are allowed through on error. Default: true.
	FailOpen bool `mapstructure:"fail_open"`

	// CacheTTL controls how long policies are cached in memory before
	// being reloaded from the policy file. Default: 30s.
	CacheTTL time.Duration `mapstructure:"cache_ttl"`

	// KeyPrefix is a namespace prefix added to all counter keys.
	// Useful when sharing a counter store across multiple processors.
	// Default: "otel".
	KeyPrefix string `mapstructure:"key_prefix"`

	// OrgID is the default organization ID populated on every request context.
	OrgID string `mapstructure:"org_id"`

	// TenantID is the default tenant ID populated on every request context.
	TenantID string `mapstructure:"tenant_id"`

	// Application is the default application name populated on every request context.
	Application string `mapstructure:"application"`

	// Environment is the default environment name (e.g. "production", "staging").
	Environment string `mapstructure:"environment"`
}

// Validate checks the configuration for errors.
func (cfg *Config) Validate() error {
	if cfg.PolicyFile == "" {
		return errors.New("policy_file is required")
	}
	if cfg.CacheTTL < 0 {
		return errors.New("cache_ttl must not be negative")
	}
	return nil
}
