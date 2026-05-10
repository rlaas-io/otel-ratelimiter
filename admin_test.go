package ratelimiterprocessor

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/rlaas-io/rlaas/pkg/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

func stopAdminServerForTest(t *testing.T) {
	t.Helper()
	globalAdmin.mu.Lock()
	srv := globalAdmin.server
	globalAdmin.server = nil
	globalAdmin.processors = nil
	globalAdmin.startedAt = time.Time{}
	globalAdmin.mu.Unlock()

	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

func waitForHTTP(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url) //nolint:gosec
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("server did not become ready: %s", url)
}

func TestAdminAPI_Endpoints(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p1", "admin", "log", 10, 10, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile:    policyFile,
		FailOpen:      true,
		CacheTTL:      30 * time.Second,
		WatchPolicies: true,
		WatchInterval: 15 * time.Second,
		KeyPrefix:     "otel",
		AdminAddr:     addr,
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 11 }, func() int64 { return 3 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	resp, err := http.Get(baseURL + "/health") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(baseURL + "/stats") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var stats map[string]map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&stats))
	require.Contains(t, stats, "logs")
	assert.EqualValues(t, 11, stats["logs"]["received"])
	assert.EqualValues(t, 3, stats["logs"]["dropped"])

	resp, err = http.Get(baseURL + "/config") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var conf map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&conf))
	assert.Equal(t, cfg.PolicyFile, conf["policy_file"])
	assert.Equal(t, cfg.AdminAddr, conf["admin_addr"])

	resp, err = http.Get(baseURL + "/config/policies") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var policyCfg map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&policyCfg))
	assert.Equal(t, "file", policyCfg["source"])
	assert.EqualValues(t, 1, policyCfg["policy_count"])

	req, err := http.NewRequest(http.MethodPost, baseURL+"/reload", nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	_, _, _, _, reloads := eng.Stats()
	assert.GreaterOrEqual(t, reloads, int64(1))

	resp, err = http.Get(baseURL + "/metrics") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	metricsBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	metricsText := string(metricsBody)
	assert.Contains(t, metricsText, "ratelimiter_records_received_total")
	assert.Contains(t, metricsText, "signal=\"logs\"")

	deregisterFromAdmin("logs")
}

func TestAdminAPI_AuthToken(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p-auth", "admin auth", "log", 5, 5, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile:       policyFile,
		FailOpen:         true,
		AdminAddr:        addr,
		AdminAuthToken:   "super-secret-token",
		AdminTokenHeader: "Authorization",
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	resp, err := http.Get(baseURL + "/health") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	req, err := http.NewRequest(http.MethodGet, baseURL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer super-secret-token")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAdminAPI_CORSPreflight(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p-cors", "admin cors", "log", 5, 5, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile:              policyFile,
		FailOpen:                true,
		AdminAddr:               addr,
		AdminCORSAllowedOrigins: []string{"https://docs.example.com"},
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	req, err := http.NewRequest(http.MethodOptions, baseURL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://docs.example.com")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.Equal(t, "https://docs.example.com", resp.Header.Get("Access-Control-Allow-Origin"))
}
