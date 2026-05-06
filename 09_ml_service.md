# ML Service — Architecture & Integration Guide

## Overview

The ML Service is a Python microservice that provides real-time, per-tenant
anomaly detection for the Conduit fintech platform.

It is the **only Python component** in the Conduit stack.  Everything else is
Node.js.  The two ecosystems communicate exclusively via Kafka — no direct HTTP
calls between Python and Node.js services.

---

## Position in the Architecture

```
                         ┌──────────────────────────────────────┐
                         │        conduit.metrics.computed       │
                         │      (produced by Metrics Service)    │
                         └───────────────┬──────────────────────┘
                                         │ consume
                                         ▼
                              ┌──────────────────────┐
                              │   ML SERVICE  (:4008) │
                              │   (Python)            │
                              │                       │
                              │  per-tenant           │
                              │  IsolationForest      │
                              │  + Z-score detector   │
                              └──────────┬────────────┘
                                         │ produce
                                         ▼
                         ┌──────────────────────────────────────┐
                         │        conduit.ml.predictions         │
                         │  (6 partitions, 14d retention)        │
                         └───┬─────────────────┬───────────┬────┘
                             │ consume          │ consume   │ consume
                             ▼                  ▼           ▼
                    ┌─────────────┐  ┌──────────────┐  ┌──────────────┐
                    │  Incident   │  │    Query     │  │  WebSocket   │
                    │  Service   │  │   Service    │  │   Service    │
                    │            │  │              │  │              │
                    │ evaluates  │  │ stores in    │  │ broadcasts   │
                    │ anomaly    │  │ MongoDB      │  │ ml:preds     │
                    │ score      │  │ (predictions │  │ channel      │
                    │ → incident │  │  collection) │  │              │
                    └─────────────┘  └──────────────┘  └──────────────┘
```

---

## Detection Pipeline

### Step 1 — Feature Extraction

Each `conduit.metrics.computed` message is converted to a five-dimensional
feature vector:

```
[error_rate, p95_latency_ms, success_rate, throughput, avg_latency_ms]
```

### Step 2 — Per-Tenant Anomaly Detection

A separate `TenantAnomalyDetector` instance is maintained for every tenant.

**Warm-up phase** (first `ML_WARMUP_SAMPLES` snapshots):
- Only Welford Z-score statistics are used.
- `modelId` = `"zscore-warmup-v1"`.
- Confidence is capped at 60% to reflect limited certainty.

**Production phase** (after warm-up):
- `IsolationForest` (scikit-learn) is fit on the latest sliding window.
- Re-fitted every `ML_REFIT_EVERY` new samples.
- Z-score is blended as a secondary signal (max of IF score and 0.7 × Z score).
- `modelId` = `ML_MODEL_ID` (default: `"isolation-forest-v1"`).

### Step 3 — Label Assignment (rule-based)

| Label                       | Rule                                           | Severity in incident-svc |
|-----------------------------|------------------------------------------------|--------------------------|
| `system_failure`            | error_rate > 0.15 AND success_rate < 0.85      | critical                 |
| `performance_degradation`   | error_rate > 0.05 AND p95 > 5 000 ms           | high                     |
| `traffic_spike`             | throughput Z-score > `ML_ZSCORE_THRESHOLD`     | high                     |
| `latency_drift`             | p95 and avg Z-scores both elevated             | medium                   |
| `metric_anomaly`            | IF anomaly but no stronger label               | medium                   |

### Step 4 — Prediction Publishing

A prediction message is published to `conduit.ml.predictions` for every
processed metric snapshot (regardless of anomaly score, unless
`ML_MIN_PUBLISH_SCORE` is set above 0.0).

Downstream components act only when `anomalyScore ≥ 0.75`.

---

## Prediction Message Schema

```json
{
  "predictionId": "<uuid>",
  "tenantId":     "tenant-abc",
  "modelId":      "isolation-forest-v1",
  "anomalyScore": 0.832,
  "confidence":   0.74,
  "label":        "performance_degradation",
  "labels":       ["performance_degradation"],
  "features": {
    "error_rate":       0.08,
    "p95_latency_ms":   6100.0,
    "success_rate":     0.92,
    "throughput":       120.0,
    "avg_latency_ms":   4200.0
  },
  "metadata": {
    "tenantId":    "tenant-abc",
    "window":      "60s",
    "sampleSize":  120,
    "source":      "conduit.metrics.computed"
  },
  "source":    "ml-service",
  "timestamp": "2024-01-01T00:00:00.000000+00:00"
}
```

---

## Kafka Integration

| Direction | Topic                       | Role                            |
|-----------|-----------------------------|---------------------------------|
| Consume   | `conduit.metrics.computed`  | Primary anomaly detection input |
| Consume   | `conduit.events.ingested`   | Statistics warm-up (no output)  |
| Produce   | `conduit.ml.predictions`    | Anomaly detection output        |
| Produce   | `conduit.dlq`               | Failed message routing          |

Consumer group: `conduit-ml-group`

---

## HTTP Endpoints

| Method | Path       | Description                                    |
|--------|------------|------------------------------------------------|
| GET    | `/health`  | Liveness — returns 200 while process is alive  |
| GET    | `/ready`   | Readiness — 200 once Kafka consumer is running |
| GET    | `/metrics` | Pipeline message counts and error rate         |
| GET    | `/models`  | Per-tenant detector state and warm-up status   |
| GET    | `/docs`    | OpenAPI (Swagger UI)                           |

---

## Deployment

| Port | Service     | Health check URL                  |
|------|-------------|-----------------------------------|
| 4008 | ML Service  | `http://localhost:4008/ready`     |

The service is stateless between restarts (all model state is in-memory).
After a restart each per-tenant detector re-enters the warm-up phase.

For production deployments, consider persisting the trained models (e.g.,
serialising with `joblib`) to an object store so the warm-up phase is skipped
on startup.

---

## Environment Variables

See `services/ml-service/.env.example` for the full list with descriptions.

Key variables:

| Variable             | Default                | Description                              |
|----------------------|------------------------|------------------------------------------|
| `KAFKA_BROKERS`      | `localhost:9092`       | Kafka broker addresses                   |
| `ML_WARMUP_SAMPLES`  | `50`                   | Samples before IsolationForest is trained|
| `ML_CONTAMINATION`   | `0.05`                 | Expected anomaly fraction                |
| `ML_ZSCORE_THRESHOLD`| `3.0`                  | Z-score anomaly threshold                |
| `ML_MODEL_ID`        | `isolation-forest-v1`  | Model identifier in predictions          |
| `KAFKA_DRY_RUN`      | `false`                | If `true`, Kafka not connected           |
| `ML_PORT`            | `4008`                 | HTTP server port                         |
