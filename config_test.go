package ratelimiterprocessor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate_Valid(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "minimal",
			cfg: Config{
				PolicyFile: "/path/to/policies.json",
			},
		},
		{
			name: "all fields",
			cfg: Config{
				PolicyFile:  "/path/to/policies.json",
				FailOpen:    true,
				CacheTTL:    30 * time.Second,
				KeyPrefix:   "otel",
				OrgID:       "org-1",
				TenantID:    "tenant-1",
				Application: "my-app",
				Environment: "production",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, tt.cfg.Validate())
		})
	}
}

func TestConfig_Validate_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name:    "missing policy file",
			cfg:     Config{},
			wantErr: "policy_file or policies_inline is required",
		},
		{
			name: "negative cache TTL",
			cfg: Config{
				PolicyFile: "/path/to/policies.json",
				CacheTTL:   -1,
			},
			wantErr: "cache_ttl must not be negative",
		},
		{
			name: "mutually exclusive policy fields",
			cfg: Config{
				PolicyFile:     "/path/to/policies.json",
				PoliciesInline: "[]",
			},
			wantErr: "policy_file and policies_inline are mutually exclusive",
		},
		{
			name: "negative watch interval",
			cfg: Config{
				PolicyFile:    "/path/to/policies.json",
				WatchInterval: -1,
			},
			wantErr: "watch_interval must not be negative",
		},
		{
			name: "negative max batch size",
			cfg: Config{
				PolicyFile:   "/path/to/policies.json",
				MaxBatchSize: -1,
			},
			wantErr: "max_batch_size must not be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}
