package ratelimiterprocessor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/rlaas-io/rlaas/pkg/model"
	"go.uber.org/zap/zaptest"
)

func TestEngine_Evaluate_Allow(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "allow-logs", "log", 10, 10, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	eng := newEngine(cfg, zaptest.NewLogger(t))

	req := model.RequestContext{
		Service:    "web-api",
		SignalType: "log",
		Severity:   "INFO",
		Quantity:   1,
	}

	// First request should be allowed.
	dec, err := eng.evaluate(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, dec.Allowed)
}

func TestEngine_Evaluate_Deny(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "limit-logs", "log", 2, 2, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	eng := newEngine(cfg, zaptest.NewLogger(t))

	req := model.RequestContext{
		Service:    "web-api",
		SignalType: "log",
		Severity:   "INFO",
		Quantity:   1,
	}

	// Exhaust the limit.
	for i := 0; i < 2; i++ {
		dec, err := eng.evaluate(context.Background(), req)
		require.NoError(t, err)
		assert.True(t, dec.Allowed, "request %d should be allowed", i)
	}

	// 3rd request should be denied.
	dec, err := eng.evaluate(context.Background(), req)
	require.NoError(t, err)
	assert.False(t, dec.Allowed)
}

func TestEngine_Evaluate_ShadowMode(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		shadowPolicy("p1", "shadow-logs", "log", 1),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	eng := newEngine(cfg, zaptest.NewLogger(t))

	req := model.RequestContext{
		Service:    "web-api",
		SignalType: "log",
		Severity:   "INFO",
		Quantity:   1,
	}

	// First within limit.
	eng.evaluate(context.Background(), req)

	// Second exceeds limit but shadow mode should still indicate shadow.
	dec, err := eng.evaluate(context.Background(), req)
	require.NoError(t, err)
	// In shadow mode, RLAAS should set ShadowMode=true on the decision.
	assert.True(t, shouldKeep(dec), "shadow mode should keep records")
}

func TestEngine_Evaluate_DefaultFields(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "allow-all", "", 1000, 1000, model.ActionDrop),
	})

	cfg := &Config{
		PolicyFile:  policyFile,
		FailOpen:    true,
		OrgID:       "default-org",
		TenantID:    "default-tenant",
		Application: "my-app",
		Environment: "staging",
	}
	eng := newEngine(cfg, zaptest.NewLogger(t))

	// Request with empty fields — defaults should be filled.
	req := model.RequestContext{
		Service:    "web-api",
		SignalType: "log",
		Quantity:   1,
	}

	dec, err := eng.evaluate(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, dec.Allowed)
}

func TestEngine_Stats(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "limit", "log", 2, 2, model.ActionDrop),
	})

	cfg := &Config{PolicyFile: policyFile, FailOpen: true}
	eng := newEngine(cfg, zaptest.NewLogger(t))

	req := model.RequestContext{
		Service:    "web-api",
		SignalType: "log",
		Quantity:   1,
	}

	// 2 allowed, then denials.
	for i := 0; i < 5; i++ {
		eng.evaluate(context.Background(), req)
	}

	allowed, denied, _, _ := eng.Stats()
	assert.Equal(t, int64(2), allowed)
	assert.Equal(t, int64(3), denied)
}

func TestShouldKeep(t *testing.T) {
	tests := []struct {
		name     string
		decision model.Decision
		want     bool
	}{
		{
			name:     "allowed",
			decision: model.Decision{Allowed: true, Action: model.ActionAllow},
			want:     true,
		},
		{
			name:     "denied",
			decision: model.Decision{Allowed: false, Action: model.ActionDrop},
			want:     false,
		},
		{
			name:     "shadow mode denied",
			decision: model.Decision{Allowed: false, ShadowMode: true, Action: model.ActionDrop},
			want:     true,
		},
		{
			name:     "shadow mode allowed",
			decision: model.Decision{Allowed: true, ShadowMode: true, Action: model.ActionAllow},
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shouldKeep(tt.decision))
		})
	}
}
