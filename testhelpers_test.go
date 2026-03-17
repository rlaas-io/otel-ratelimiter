package ratelimiterprocessor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/rlaas-io/rlaas/pkg/model"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"
)

// nopSettings returns processor settings with the ratelimiter component type.
func nopSettings() processor.Settings {
	return processortest.NewNopSettings(component.MustNewType("ratelimiter"))
}

// createTempPolicyFile writes RLAAS policies to a temp JSON file and returns
// the path. The file is automatically cleaned up when the test finishes.
// Accepts testing.TB so it works with both *testing.T and *testing.B.
func createTempPolicyFile(t testing.TB, policies []model.Policy) string {
	t.Helper()
	data, err := json.Marshal(policies)
	if err != nil {
		t.Fatalf("marshal policies: %v", err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "policies.json")
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("write policy file: %v", err)
	}
	return path
}

// tokenBucketPolicy creates a simple token bucket policy for testing.
func tokenBucketPolicy(id, name, signalType string, limit, burst int64, action model.ActionType) model.Policy {
	p := model.Policy{
		PolicyID:        id,
		Name:            name,
		Enabled:         true,
		Priority:        1,
		Algorithm:       model.AlgorithmConfig{Type: model.AlgoTokenBucket, Limit: limit, Window: "1m", Burst: burst},
		Action:          action,
		FailureMode:     model.FailOpen,
		EnforcementMode: model.EnforceMode,
		RolloutPercent:  100,
	}
	if signalType != "" {
		p.Scope = model.PolicyScope{SignalType: signalType}
	}
	return p
}

// fixedWindowPolicy creates a simple fixed window policy for testing.
func fixedWindowPolicy(id, name, signalType string, limit int64, action model.ActionType) model.Policy {
	p := model.Policy{
		PolicyID:        id,
		Name:            name,
		Enabled:         true,
		Priority:        1,
		Algorithm:       model.AlgorithmConfig{Type: model.AlgoFixedWindow, Limit: limit, Window: "1m"},
		Action:          action,
		FailureMode:     model.FailOpen,
		EnforcementMode: model.EnforceMode,
		RolloutPercent:  100,
	}
	if signalType != "" {
		p.Scope = model.PolicyScope{SignalType: signalType}
	}
	return p
}

// shadowPolicy creates a shadow-mode policy for testing.
func shadowPolicy(id, name, signalType string, limit int64) model.Policy {
	return model.Policy{
		PolicyID:        id,
		Name:            name,
		Enabled:         true,
		Priority:        1,
		Algorithm:       model.AlgorithmConfig{Type: model.AlgoTokenBucket, Limit: limit, Window: "1m", Burst: limit},
		Action:          model.ActionDrop,
		FailureMode:     model.FailOpen,
		EnforcementMode: model.ShadowMode,
		RolloutPercent:  100,
		Scope:           model.PolicyScope{SignalType: signalType},
	}
}

// servicePolicy creates a policy scoped to a specific service.
func servicePolicy(id, name, service, signalType string, limit int64, action model.ActionType) model.Policy {
	return model.Policy{
		PolicyID:        id,
		Name:            name,
		Enabled:         true,
		Priority:        1,
		Algorithm:       model.AlgorithmConfig{Type: model.AlgoTokenBucket, Limit: limit, Window: "1m", Burst: limit},
		Action:          action,
		FailureMode:     model.FailOpen,
		EnforcementMode: model.EnforceMode,
		RolloutPercent:  100,
		Scope:           model.PolicyScope{Service: service, SignalType: signalType},
	}
}
