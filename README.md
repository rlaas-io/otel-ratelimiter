# OpenTelemetry Collector Rate Limiter Processor

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/rlaas-io/otel-ratelimiter/actions/workflows/ci.yml/badge.svg)](https://github.com/rlaas-io/otel-ratelimiter/actions/workflows/ci.yml)

A production-ready **OpenTelemetry Collector processor** that applies configurable rate limiting to **logs**, **traces**, and **metrics** pipelines — powered by [RLAAS (Rate Limiting As A Service)](https://github.com/rlaas-io/rlaas) as the core decision engine.

---

## Why This Exists

Most rate limiting happens at the API gateway. But telemetry data has its own scaling problems:

- **Log floods** — a noisy service sends millions of error logs; Splunk/Datadog costs spike overnight
- **Trace storms** — a retry loop generates 100x normal span volume during an incident
- **Metric explosions** — a chatty service emits high-cardinality metrics that overwhelm your backend
- **No per-service control** — the collector receives everything; there's no way to enforce per-service budgets

This processor sits inside the OpenTelemetry Collector pipeline and drops, throttles, or samples telemetry **before** it reaches your backend — saving cost, protecting infrastructure, and giving you per-service control.

---

## RLAAS: The Decision Engine

This processor delegates **all rate limiting decisions** to [RLAAS](https://github.com/rlaas-io/rlaas). Rather than reimplementing algorithms from scratch, it uses RLAAS as the heart of its decision-making, giving you:

### 7 Algorithms (via RLAAS)

| Algorithm | Best For |
|---|---|
| **Token Bucket** | Smoothing bursty traffic while allowing short spikes |
| **Sliding Window Log** | Precise per-key quota enforcement |
| **Sliding Window Counter** | Efficient approximate sliding window |
| **Fixed Window** | Simple, low-overhead coarse-grained limits |
| **Leaky Bucket** | Enforcing a steady output rate |
| **Concurrency** | Capping in-flight records |
| **Quota** | Period-based budget allocation |

### 8 Decision Actions (via RLAAS)

| Action | Behavior |
|---|---|
| `allow` | Record passes through |
| `deny` | Reject and signal overload |
| `delay` | Queue and delay processing |
| `sample` | Probabilistically keep excess records |
| `drop` | Silently discard excess records |
| `downgrade` | Reduce record priority |
| `drop_low_priority` | Drop only low-priority records |
| `shadow_only` | Evaluate limits but allow everything through (dry-run) |

### Policy-Based Control

Policies are defined in a standard RLAAS JSON file with:
- **Scope matching** — match by org, tenant, service, signal type, severity, span name, etc.
- **Priority ordering** — higher priority policies are evaluated first
- **Enforcement modes** — `enforce` (production) or `shadow` (dry-run)
- **Rollout controls** — gradual rollout percentages
- **Failure modes** — `fail_open` or `fail_closed`

### Signals

- **Logs** — per-service, per-severity rate limiting
- **Traces** — per-service, per-span-name span rate limiting
- **Metrics** — per-service, per-metric-name metric rate limiting

---

## Architecture

```
                    OTel Collector Pipeline
                    
Receivers ──> [ratelimiter processor] ──> Exporters
                       │
              ┌────────┴─────────┐
              │   Context Builder │  (converts OTel pdata → RLAAS RequestContext)
              └────────┬─────────┘
                       │
              ┌────────┴─────────┐
              │   RLAAS Engine   │  (policy matching → algorithm evaluation → decision)
              │                  │
              │  ┌─────────────┐ │
              │  │ Policy Store│ │  (JSON file with RLAAS policies)
              │  └─────────────┘ │
              │  ┌─────────────┐ │
              │  │Counter Store│ │  (in-memory, sharded, TTL-aware)
              │  └─────────────┘ │
              │  ┌─────────────┐ │
              │  │ 7 Algorithms│ │  (token bucket, sliding window, fixed window, ...)
              │  └─────────────┘ │
              └──────────────────┘
                       │
              Decision: allow / deny / drop / shadow / ...
                       │
              Keep or remove record from batch
```

**Flow per record:**
1. Convert OTel record (log/span/metric) to `model.RequestContext`
2. RLAAS matches request against configured policies
3. RLAAS runs the matched algorithm (token bucket, sliding window, etc.)
4. RLAAS returns a `Decision` (allowed, action, shadow mode, remaining, etc.)
5. Processor keeps or removes the record based on the decision

---

## Installation

### Option 1: Build with OpenTelemetry Collector Builder (ocb) — Recommended

The recommended way to use this processor is via the [OpenTelemetry Collector Builder](https://opentelemetry.io/docs/collector/extend/ocb/). A `builder-config.yaml` is included in the repo.

```bash
# Install the builder
go install go.opentelemetry.io/collector/cmd/builder@v0.147.0

# Build the custom collector distribution
builder --config builder-config.yaml

# Run it
./otelcol-ratelimiter/otelcol-ratelimiter --config collector-config.yaml
```

Or use the Makefile shortcut (Linux/macOS — downloads ocb via curl):

```bash
make ocb-build
```

> See the [Development](#development) section below for detailed local build instructions, including Windows-specific steps.

### Option 2: Docker

```bash
make docker-build
make docker-run
```

See the `Dockerfile` for the multi-stage build that uses ocb internally.

### Option 3: Go module (embed in your own collector)

```bash
go get github.com/rlaas-io/otel-ratelimiter
```

Register the processor in your custom collector build:

```go
import ratelimiterprocessor "github.com/rlaas-io/otel-ratelimiter"

func components() (otelcol.Factories, error) {
    processors, err := processor.MakeFactoryMap(
        ratelimiterprocessor.NewFactory(),
        // ... other processors
    )
    // ...
}
```

---

## Configuration

### Processor Config

```yaml
processors:
  ratelimiter:
    # Path to the RLAAS policy JSON file (required).
    policy_file: /etc/otel/policies.json

    # Allow records through when RLAAS encounters an error (default: true).
    fail_open: true

    # How long to cache policies in memory (default: 30s).
    cache_ttl: 30s

    # Namespace prefix for counter keys (default: "otel").
    key_prefix: otel

    # Default context fields applied to every request.
    org_id: my-org
    tenant_id: my-tenant
    application: my-app
    environment: production
```

### RLAAS Policy File

Policies are standard RLAAS policy JSON. Example `policies.json`:

```json
[
  {
    "policy_id": "limit-logs",
    "name": "Limit log records per service",
    "enabled": true,
    "priority": 10,
    "scope": {
      "signal_type": "log"
    },
    "algorithm": {
      "type": "token_bucket",
      "limit": 5000,
      "window": "1m",
      "burst": 1000
    },
    "action": "drop",
    "failure_mode": "fail_open",
    "enforcement_mode": "enforce",
    "rollout_percent": 100
  },
  {
    "policy_id": "shadow-traces",
    "name": "Shadow mode for trace limiting",
    "enabled": true,
    "priority": 5,
    "scope": {
      "signal_type": "span",
      "service": "payment-svc"
    },
    "algorithm": {
      "type": "sliding_window_log",
      "limit": 2000,
      "window": "1m"
    },
    "action": "drop",
    "failure_mode": "fail_open",
    "enforcement_mode": "shadow",
    "rollout_percent": 100
  },
  {
    "policy_id": "limit-metrics",
    "name": "Limit high-cardinality metrics",
    "enabled": true,
    "priority": 10,
    "scope": {
      "signal_type": "metric"
    },
    "algorithm": {
      "type": "fixed_window",
      "limit": 10000,
      "window": "1m"
    },
    "action": "drop",
    "failure_mode": "fail_open",
    "enforcement_mode": "enforce",
    "rollout_percent": 100
  }
]
```

### Pipeline Configuration

```yaml
service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [ratelimiter, batch]
      exporters: [otlphttp]

    traces:
      receivers: [otlp]
      processors: [ratelimiter, batch]
      exporters: [otlp]

    metrics:
      receivers: [otlp]
      processors: [ratelimiter, batch]
      exporters: [prometheusremotewrite]
```

---

## Configuration Reference

### Processor Settings

| Parameter | Type | Default | Description |
|---|---|---|---|
| `policy_file` | string | *required* | Path to RLAAS policy JSON file |
| `fail_open` | bool | `true` | Allow records through on engine errors |
| `cache_ttl` | duration | `30s` | Policy cache duration |
| `key_prefix` | string | `"otel"` | Counter key namespace prefix |
| `org_id` | string | `""` | Default organization ID on request context |
| `tenant_id` | string | `""` | Default tenant ID on request context |
| `application` | string | `""` | Default application name on request context |
| `environment` | string | `""` | Default environment name on request context |

### RLAAS Policy Fields

| Field | Type | Description |
|---|---|---|
| `policy_id` | string | Unique policy identifier |
| `name` | string | Human-readable policy name |
| `enabled` | bool | Whether this policy is active |
| `priority` | int | Higher priority evaluated first |
| `scope` | object | Matching criteria (service, signal_type, severity, etc.) |
| `algorithm.type` | string | `token_bucket`, `sliding_window_log`, `sliding_window_counter`, `fixed_window`, `leaky_bucket`, `concurrency`, `quota` |
| `algorithm.limit` | int | Max requests per window |
| `algorithm.window` | string | Time window (e.g. `"1m"`, `"1h"`) |
| `algorithm.burst` | int | Burst capacity |
| `action` | string | `allow`, `deny`, `delay`, `sample`, `drop`, `downgrade`, `drop_low_priority`, `shadow_only` |
| `failure_mode` | string | `fail_open` or `fail_closed` |
| `enforcement_mode` | string | `enforce` or `shadow` |
| `rollout_percent` | int | 0-100 gradual rollout |

See [RLAAS Documentation](https://rlaas-io.github.io/rlaas/) for the full policy schema.

### Request Context Mapping

Each OTel record is converted to a RLAAS `RequestContext`:

| RLAAS Field | Logs | Traces | Metrics |
|---|---|---|---|
| `service` | `service.name` resource attr | `service.name` resource attr | `service.name` resource attr |
| `signal_type` | `"log"` | `"span"` | `"metric"` |
| `severity` | Log severity text | — | — |
| `span_name` | — | Span name | — |
| `resource` | — | — | Metric name |
| `tags` | Resource + record attributes | Resource + span attributes | Resource attributes |

---

## Development

### Prerequisites

- Go 1.25+
- [OpenTelemetry Collector Builder (ocb)](https://opentelemetry.io/docs/collector/extend/ocb/) v0.147.0+ (for building the custom collector)

### Build the processor module

```bash
go build ./...
```

### Run unit tests

```bash
go test -v -race ./...
```

### Code coverage

```bash
# Generate coverage report (target: ≥90%)
go test -race -coverprofile=coverage.out ./...

# View in browser
go tool cover -html=coverage.out

# Print summary to terminal
go tool cover -func=coverage.out
```

---

### Build the Custom Collector Locally (OCB)

The project includes a `builder-config.yaml` that produces a standalone collector binary with the rate limiter processor baked in.

#### Option A — Using `go install` (recommended for local dev)

```bash
# Install the builder tool
go install go.opentelemetry.io/collector/cmd/builder@v0.147.0

# Build the custom collector
# On Windows, if your system Go is older than 1.25, set the toolchain explicitly:
#   set GOTOOLCHAIN=go1.25.8    (cmd)
#   $env:GOTOOLCHAIN="go1.25.8" (PowerShell)
builder --config builder-config.yaml
```

#### Option B — Using `curl` + Makefile

```bash
# Downloads the ocb binary and builds in one step
make ocb-build
```

#### Option C — Docker (Linux amd64)

```bash
make docker-build
make docker-run
```

After a successful build, the binary is at `./otelcol-ratelimiter/otelcol-ratelimiter` (or `.exe` on Windows).

> **Note:** The entire `otelcol-ratelimiter/` directory is auto-generated and git-ignored. Never commit it — it is fully reproducible from `builder-config.yaml`.

---

### Run the Collector Locally

```bash
# Using the local dev config (debug exporter, policies from ./example/policies.json)
./otelcol-ratelimiter/otelcol-ratelimiter --config local-collector-config.yaml

# Or on Windows PowerShell:
& .\otelcol-ratelimiter\otelcol-ratelimiter.exe --config local-collector-config.yaml
```

The collector starts listening on:
- **gRPC** — `0.0.0.0:4317`
- **HTTP** — `0.0.0.0:4318`

---

### Verify the Collector Is Running (Reading Logs)

On successful startup you should see log lines like this:

```
info  service/service.go  Starting otelcol-ratelimiter...
        Version: 1.0.0, NumCPU: 12

info  extensions/extensions.go  Starting extensions...

info  OTEL-RATELIMITER/processor_logs.go
        RLAAS rate limiter logs processor started
        policy_file: ./example/policies.json, fail_open: true

info  otlpreceiver/otlp.go  Starting GRPC server  endpoint: [::]:4317
info  otlpreceiver/otlp.go  Starting HTTP server  endpoint: [::]:4318

info  OTEL-RATELIMITER/processor_traces.go
        RLAAS rate limiter traces processor started

info  OTEL-RATELIMITER/processor_metrics.go
        RLAAS rate limiter metrics processor started

info  service/service.go  Everything is ready. Begin running and processing data.
```

**Key things to look for:**
- `RLAAS rate limiter logs/traces/metrics processor started` — confirms all three signal processors initialized and loaded the policy file
- `Starting GRPC server` / `Starting HTTP server` — confirms the OTLP receiver is accepting data
- `Everything is ready. Begin running and processing data.` — collector is fully running

#### Send test data with `telemetrygen`

```bash
# Install telemetrygen
go install github.com/open-telemetry/opentelemetry-collector-contrib/cmd/telemetrygen@latest

# Send 50 log records
telemetrygen logs --otlp-insecure --duration 5s --rate 10

# Send 50 traces
telemetrygen traces --otlp-insecure --duration 5s --rate 10

# Send 50 metrics
telemetrygen metrics --otlp-insecure --duration 5s --rate 10
```

With the `debug` exporter set to `verbosity: detailed`, you will see each received record printed in the collector's terminal output. Records that exceed the rate limit will be silently dropped (you'll see fewer records in the debug output than were sent).

#### Check if rate limiting is working

1. **Send a burst above the limit** — e.g., send 200 logs/sec when the policy allows 5000/min (~83/sec). Observe the debug exporter output: early batches pass through fully, later batches have records removed.
2. **Shadow mode** — If a policy has `enforcement_mode: shadow`, all records pass through but the engine still evaluates limits. Check for the `shadow: true` decision in logs.
3. **Fail-closed test** — Point `policy_file` at a non-existent file with `fail_open: false`. The processor will drop all records.

---

### Integration Tests

Integration tests validate end-to-end behavior: building a real processor with real policies, sending OTel `pdata` through it, and asserting records are correctly allowed or dropped.

```bash
# Run all integration tests
go test -v -race -run TestIntegration ./...

# Run a specific integration test
go test -v -race -run TestIntegration_LogsProcessor_DropsExcessLogs ./...
go test -v -race -run TestIntegration_FullPipeline_AllSignals ./...
```

**What the integration tests cover (18 tests):**

| Test | What it validates |
|---|---|
| `DropsExcessLogs` | Token bucket drops records beyond the limit |
| `MultiServiceIsolation` | Separate counters per service name |
| `ShadowModePassesAll` | Shadow policies evaluate but never drop |
| `SequentialBatches` | Counter state persists across batches |
| `DropsExcessSpans` | Traces pipeline drops excess spans |
| `DropsExcessMetrics` | Metrics pipeline drops excess data points |
| `FullPipeline_AllSignals` | All three signals in one test |
| `ServiceScopedPolicy` | Policy scoped to a specific service |
| `FailClosed` | Invalid policy file + `fail_open: false` drops everything |
| `Capabilities` | Processor reports `MutatesData: true` |
| `MixedSeverities` | Different policies per severity level |
| `NoServiceName` (logs/traces/metrics) | Records without `service.name` still rate-limit |
| `EmptyAttributes` | Records with no attributes at all |
| `Engine_DefaultsNotOverridden` | Engine config defaults are preserved |
| `Engine_ZeroQuantityDefault` | Zero quantity defaults to 1 |
| `Engine_NegativeQuantityDefault` | Negative quantity defaults to 1 |

---

### Benchmark Tests

Benchmark tests measure per-operation latency and memory allocation to catch performance regressions.

```bash
# Run all benchmarks
go test -bench=. -benchmem -count=3 -run=^$ ./...

# Run specific benchmark groups
go test -bench=BenchmarkLogsProcessor -benchmem ./...
go test -bench=BenchmarkEngine -benchmem ./...
go test -bench=BenchmarkBuildLogsContext -benchmem ./...

# Run batch-size scaling benchmarks
go test -bench=BenchmarkLogsProcessor_BatchSize -benchmem ./...
```

**What the benchmarks cover (14 benchmarks):**

| Benchmark | What it measures |
|---|---|
| `LogsProcessor_AllAllowed` | Throughput when no records are dropped |
| `LogsProcessor_HalfDropped` | Throughput when ~50% of records are rate-limited |
| `LogsProcessor_ShadowMode` | Overhead of shadow mode (evaluate but don't drop) |
| `TracesProcessor_AllAllowed/HalfDropped` | Traces pipeline throughput |
| `MetricsProcessor_AllAllowed/HalfDropped` | Metrics pipeline throughput |
| `Engine_Evaluate` | Raw RLAAS engine evaluation latency (~2.8µs/op) |
| `BuildLogsContext` | Log → RequestContext conversion (~350ns/op) |
| `BuildTracesContext` | Span → RequestContext conversion (~370ns/op) |
| `BuildMetricsContext` | Metric → RequestContext conversion (~460ns/op) |
| `BatchSize10/100/1000` | Scaling behavior across batch sizes |

**Example output:**

```
BenchmarkEngine_Evaluate-12          425920    2812 ns/op    1592 B/op    25 allocs/op
BenchmarkBuildLogsContext-12        3447814     349 ns/op     544 B/op     5 allocs/op
BenchmarkBuildTracesContext-12      3274620     366 ns/op     544 B/op     5 allocs/op
BenchmarkBuildMetricsContext-12     2614750     458 ns/op     624 B/op     7 allocs/op
```

---

### CI Pipeline

The GitHub Actions CI workflow (`.github/workflows/ci.yml`) runs on every push and PR:

| Job | What it does |
|---|---|
| **build-and-test** | `go build`, `go test -race`, enforces **≥90% code coverage** |
| **integration-test** | Runs all `TestIntegration_*` tests |
| **benchmark** | Runs all benchmarks with `-benchmem -count=3` |

---

## Relationship to RLAAS

This processor is a first-class consumer of the [RLAAS](https://github.com/rlaas-io/rlaas) engine. RLAAS provides the complete rate limiting platform:

- **7 algorithms** — token bucket, sliding window (log & counter), fixed window, leaky bucket, concurrency, quota
- **8 decision actions** — allow, deny, delay, sample, drop, downgrade, drop_low_priority, shadow_only
- **Policy engine** — scope-based matching, priority ordering, enforcement/shadow modes, rollout controls
- **Multiple stores** — in-memory, Redis, PostgreSQL, Oracle counter stores
- **Multi-provider adapters** — OTel, Datadog, Fluent Bit, Envoy, Kafka, gRPC, HTTP
- **SDKs** — Go, Python, Java, TypeScript, .NET

This OTel processor uses RLAAS with an **in-memory counter store** and a **file-based policy store**, making it ideal for embedded, collector-local rate limiting without external dependencies.

**RLAAS GitHub:** https://github.com/rlaas-io/rlaas  
**RLAAS Documentation:** https://rlaas-io.github.io/rlaas/

---

## License

MIT License — see [LICENSE](LICENSE) for details.
