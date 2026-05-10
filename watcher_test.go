package ratelimiterprocessor

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rlaas-io/rlaas/pkg/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestNewPolicyWatcher_InvalidPath(t *testing.T) {
	_, err := newPolicyWatcher("/definitely/missing/policies.json", 10*time.Millisecond, func() {}, zaptest.NewLogger(t))
	require.Error(t, err)
}

func TestPolicyWatcher_RunDetectsChange(t *testing.T) {
	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "watch", "log", 10, 10, model.ActionDrop),
	})

	changed := make(chan struct{}, 1)
	pw, err := newPolicyWatcher(policyFile, 20*time.Millisecond, func() {
		select {
		case changed <- struct{}{}:
		default:
		}
	}, zaptest.NewLogger(t))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		pw.run(ctx)
	}()

	// Write an updated policy payload to trigger fsnotify and/or polling fallback.
	time.Sleep(50 * time.Millisecond)
	err = os.WriteFile(policyFile, []byte("[]"), 0644)
	require.NoError(t, err)

	select {
	case <-changed:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not detect policy change")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watcher did not stop after cancellation")
	}
}
