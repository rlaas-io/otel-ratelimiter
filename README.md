# OpenTelemetry Collector Rate Limiter Processor

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![CI](https://github.com/suresh-p26/OTEL-RATELIMITER/actions/workflows/ci.yml/badge.svg)](https://github.com/suresh-p26/OTEL-RATELIMITER/actions/workflows/ci.yml)

A production-ready **OpenTelemetry Collector processor** that applies configurable rate limiting to **logs**, **traces**, and **metrics** pipelines — powered by [RLAAS (Rate Limiting As A Service)](https://github.com/suresh-p26/RLAAS) as the core decision engine.

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

This processor delegates **all rate limiting decisions** to [RLAAS](https://github.com/suresh-p26/RLAAS). Rather than reimplementing algorithms from scratch, it uses RLAAS as the heart of its decision-making, giving you:

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

```bash
go get github.com/suresh-p26/OTEL-RATELIMITER
```

Register the processor in your custom collector build:

```go
import ratelimiterprocessor "github.com/suresh-p26/OTEL-RATELIMITER"

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

See [RLAAS Documentation](https://suresh-p26.github.io/RLAAS/) for the full policy schema.

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

### Build

```bash
go build ./...
```

### Test

```bash
go test -v ./...
```

### Coverage

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

---

## Relationship to RLAAS

This processor is a first-class consumer of the [RLAAS](https://github.com/suresh-p26/RLAAS) engine. RLAAS provides the complete rate limiting platform:

- **7 algorithms** — token bucket, sliding window (log & counter), fixed window, leaky bucket, concurrency, quota
- **8 decision actions** — allow, deny, delay, sample, drop, downgrade, drop_low_priority, shadow_only
- **Policy engine** — scope-based matching, priority ordering, enforcement/shadow modes, rollout controls
- **Multiple stores** — in-memory, Redis, PostgreSQL, Oracle counter stores
- **Multi-provider adapters** — OTel, Datadog, Fluent Bit, Envoy, Kafka, gRPC, HTTP
- **SDKs** — Go, Python, Java, TypeScript, .NET

This OTel processor uses RLAAS with an **in-memory counter store** and a **file-based policy store**, making it ideal for embedded, collector-local rate limiting without external dependencies.

**RLAAS GitHub:** https://github.com/suresh-p26/RLAAS  
**RLAAS Documentation:** https://suresh-p26.github.io/RLAAS/

---

## License

MIT License — see [LICENSE](LICENSE) for details.
