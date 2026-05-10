package ratelimiterprocessor

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

func TestBuildAdminTLSConfig_NoTLS(t *testing.T) {
	cfg := &Config{}
	tlsCfg, err := buildAdminTLSConfig(cfg)
	require.NoError(t, err)
	assert.Nil(t, tlsCfg, "should return nil when no TLS cert configured")
}

func TestBuildAdminTLSConfig_TLSWithoutClientCA(t *testing.T) {
	cfg := &Config{
		AdminTLSCertFile: "/path/to/cert.pem",
	}
	tlsCfg, err := buildAdminTLSConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, tlsCfg)
	assert.Equal(t, uint16(tls.VersionTLS12), tlsCfg.MinVersion)
	assert.Nil(t, tlsCfg.ClientCAs, "should not require client certs when no CA file")
}

func TestBuildAdminTLSConfig_InvalidCAFile(t *testing.T) {
	cfg := &Config{
		AdminTLSCertFile:     "/path/to/cert.pem",
		AdminTLSClientCAFile: "/nonexistent/ca.pem",
	}
	tlsCfg, err := buildAdminTLSConfig(cfg)
	assert.Error(t, err)
	assert.Nil(t, tlsCfg)
	assert.Contains(t, err.Error(), "read admin_tls_client_ca_file")
}

func TestBuildAdminTLSConfig_InvalidCAPEM(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "invalid_ca.pem")
	err := os.WriteFile(tmpFile, []byte("this is not a valid PEM certificate"), 0600)
	require.NoError(t, err)

	cfg := &Config{
		AdminTLSCertFile:     "/path/to/cert.pem",
		AdminTLSClientCAFile: tmpFile,
	}
	tlsCfg, err := buildAdminTLSConfig(cfg)
	assert.Error(t, err)
	assert.Nil(t, tlsCfg)
	assert.Contains(t, err.Error(), "no valid CA certs found")
}

func TestContainsWildcard(t *testing.T) {
	assert.True(t, containsWildcard([]string{"*"}))
	assert.True(t, containsWildcard([]string{"https://example.com", "*"}))
	assert.False(t, containsWildcard([]string{"https://example.com"}))
	assert.False(t, containsWildcard([]string{}))
}

func TestAdminPolicyConfig_InlinePolicy(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyJSON := `[{"id":"inline-p1","algorithm":"token_bucket","rate":100,"burst":200,"action":"drop","enabled":true}]`
	addr := freeTCPAddr(t)
	cfg := &Config{
		PoliciesInline: policyJSON,
		FailOpen:       true,
		AdminAddr:      addr,
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	resp, err := http.Get(baseURL + "/config/policies") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var policyCfg map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&policyCfg))
	assert.Equal(t, "inline", policyCfg["source"])
	assert.EqualValues(t, 1, policyCfg["policy_count"])
	assert.NotEmpty(t, policyCfg["content_sha256"])
}

func TestAdminUIHandler(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p-ui", "admin ui", "log", 5, 5, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile: policyFile,
		FailOpen:   true,
		AdminAddr:  addr,
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	// Test UI endpoint
	resp, err := http.Get(baseURL + "/ui/") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	// UI should contain HTML
	assert.Contains(t, string(body), "<!DOCTYPE html>", "should serve HTML content")
}

func TestAdminAuthMiddleware_CustomHeader(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p-custom", "custom header", "log", 5, 5, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile:       policyFile,
		FailOpen:         true,
		AdminAddr:        addr,
		AdminAuthToken:   "my-token",
		AdminTokenHeader: "X-API-Key",
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	// Without header should fail
	resp, err := http.Get(baseURL + "/health") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// With custom header should succeed
	req, err := http.NewRequest(http.MethodGet, baseURL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("X-API-Key", "my-token")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAdmin_UIBypassesAuth(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p-ui-bypass", "UI bypass auth", "log", 5, 5, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile:     policyFile,
		FailOpen:       true,
		AdminAddr:      addr,
		AdminAuthToken: "secret-token",
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/ui/")

	// /ui/ should be accessible without auth (UI page itself contains no secrets)
	resp, err := http.Get(baseURL + "/ui/") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "UI should bypass auth")

	// But /health requires auth
	resp, err = http.Get(baseURL + "/health") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "API endpoints should require auth")
}

func TestAdmin_UIRedirect(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p-redirect", "UI redirect", "log", 5, 5, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile: policyFile,
		FailOpen:   true,
		AdminAddr:  addr,
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	// Test that /ui redirects to /ui/
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get(baseURL + "/ui") //nolint:gosec
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusMovedPermanently, resp.StatusCode)
	assert.Equal(t, "/ui/", resp.Header.Get("Location"))
}

func TestAdminCORS_Wildcard(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p-cors-wild", "CORS wildcard", "log", 5, 5, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile:              policyFile,
		FailOpen:                true,
		AdminAddr:               addr,
		AdminCORSAllowedOrigins: []string{"*"},
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	req, err := http.NewRequest(http.MethodGet, baseURL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Origin", "https://any-origin.com")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"),
		"wildcard should return * for any origin")
}

func TestAdminAuthMiddleware_BearerToken(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p-bearer", "bearer token", "log", 5, 5, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile:     policyFile,
		FailOpen:       true,
		AdminAddr:      addr,
		AdminAuthToken: "my-bearer-token",
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	// Test Bearer prefix is stripped
	req, err := http.NewRequest(http.MethodGet, baseURL+"/health", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer my-bearer-token")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Bearer prefix should be stripped automatically")

	// Test direct token also works
	req2, err := http.NewRequest(http.MethodGet, baseURL+"/health", nil)
	require.NoError(t, err)
	req2.Header.Set("Authorization", "my-bearer-token")
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode, "Direct token should also work")
}

func TestAdminCORS_NoOrigin(t *testing.T) {
	stopAdminServerForTest(t)
	t.Cleanup(func() { stopAdminServerForTest(t) })

	policyFile := createTempPolicyFile(t, []model.Policy{
		tokenBucketPolicy("p-cors-none", "CORS no origin", "log", 5, 5, model.ActionDrop),
	})
	addr := freeTCPAddr(t)
	cfg := &Config{
		PolicyFile:              policyFile,
		FailOpen:                true,
		AdminAddr:               addr,
		AdminCORSAllowedOrigins: []string{"https://example.com"},
	}
	eng, err := newEngine(cfg, zaptest.NewLogger(t))
	require.NoError(t, err)

	registerWithAdmin(cfg, eng, "logs", func() int64 { return 0 }, func() int64 { return 0 }, zaptest.NewLogger(t))

	baseURL := "http://" + addr
	waitForHTTP(t, baseURL+"/health")

	// Request without Origin header - CORS headers should not be added
	req, err := http.NewRequest(http.MethodGet, baseURL+"/health", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Empty(t, resp.Header.Get("Access-Control-Allow-Origin"),
		"no CORS headers should be added without Origin header")
}
