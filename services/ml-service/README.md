# Conduit ML Service (Python)

Real-time, per-tenant anomaly-detection microservice for the Conduit fintech platform.

## What it does

1. **Consumes** `conduit.metrics.computed` and `conduit.events.ingested` from Kafka.
2. **Extracts** a five-dimensional feature vector from each aggregated metric snapshot.
3. **Scores** the snapshot with a per-tenant **IsolationForest** model (scikit-learn).
   During the warm-up phase, a **Z-score** baseline is used instead.
4. **Publishes** a structured prediction envelope to `conduit.ml.predictions`.
5. The **Incident Service** (Node.js) reads from `conduit.ml.predictions` and creates
   incidents when `anomalyScore ≥ 0.75`.

## Architecture position

```
Kafka (conduit.metrics.computed)
   │
   ▼
ML Service (:4008)
   ├─ IsolationForest / Z-score detector (per tenant)
   └─ publishes → conduit.ml.predictions
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
   Incident Svc   Query Svc    WebSocket Svc
   (creates        (stores in   (broadcasts to
    incidents)      MongoDB)     ml:predictions)
```

## Feature vector (from `conduit.metrics.computed`)

| Index | Name              | Unit       | Description                      |
|------:|-------------------|------------|----------------------------------|
|     0 | `error_rate`      | [0, 1]     | Fraction of events that errored  |
|     1 | `p95_latency_ms`  | ms         | 95th-percentile latency          |
|     2 | `success_rate`    | [0, 1]     | Fraction of successful events    |
|     3 | `throughput`      | events/win | Events in the 1-minute window    |
|     4 | `avg_latency_ms`  | ms         | Mean latency                     |

## Anomaly labels

Compatible with `LABEL_SEVERITY_MAP` in `incident-service/src/detectors/mlDetector.js`:

| Label                       | Trigger condition                                 |
|-----------------------------|---------------------------------------------------|
| `system_failure`            | error_rate > 15% AND success_rate < 85%           |
| `performance_degradation`   | error_rate > 5% AND p95 latency > 5 000 ms        |
| `traffic_spike`             | throughput Z-score > 3.0                          |
| `latency_drift`             | p95 and avg latency Z-scores both elevated        |
| `metric_anomaly`            | IsolationForest anomaly (no stronger label match) |

## Quick start

### Prerequisites

- Python 3.12+
- Running Docker Compose stack (Kafka, Redis, Postgres, MongoDB)

### Local development

```bash
cd services/ml-service

# Create virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# Install runtime + dev dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Copy and configure environment
cp .env.example .env
# Edit .env if your Kafka is not at localhost:9092

# Start the service
python -m src.main
```

The HTTP server starts on **`:4008`** by default.

### Running without Kafka (dry-run)

Set `KAFKA_DRY_RUN=true` in your `.env` to start the service without connecting to
Kafka.  Messages are logged but never sent or received.  Useful for testing the HTTP
API in isolation.

```bash
KAFKA_DRY_RUN=true python -m src.main
```

## Running tests

```bash
# From services/ml-service/
pytest

# With coverage report
pytest --cov=src --cov-report=term-missing
```

All tests run without a live Kafka cluster.

## Docker

```bash
# Build
docker build -t conduit-ml-service .

# Run (with .env)
docker run --env-file .env -p 4008:4008 conduit-ml-service
```

The `docker-compose.yml` in the project root includes an `ml-service` service that
starts automatically with `docker-compose up`.

## HTTP endpoints

| Method | Path       | Description                               |
|--------|------------|-------------------------------------------|
| GET    | `/health`  | Liveness — uptime, pipeline stats         |
| GET    | `/ready`   | Readiness — 200 once Kafka consumer runs  |
| GET    | `/metrics` | Message counts, prediction counts, errors |
| GET    | `/models`  | Per-tenant detector state                 |
| GET    | `/docs`    | Interactive OpenAPI docs (Swagger UI)     |
| GET    | `/redoc`   | ReDoc API documentation                   |

## Prediction message schema

Published to `conduit.ml.predictions` with `tenantId` as the Kafka message key:

```json
{
  "predictionId": "<uuid>",
  "tenantId": "tenant-abc",
  "modelId": "isolation-forest-v1",
  "anomalyScore": 0.832,
  "confidence": 0.74,
  "label": "performance_degradation",
  "labels": ["performance_degradation"],
  "features": {
    "error_rate": 0.08,
    "p95_latency_ms": 6100.0,
    "success_rate": 0.92,
    "throughput": 120.0,
    "avg_latency_ms": 4200.0
  },
  "metadata": {
    "tenantId": "tenant-abc",
    "window": "60s",
    "sampleSize": 120,
    "source": "conduit.metrics.computed"
  },
  "source": "ml-service",
  "timestamp": "2024-01-01T00:00:00.000000+00:00"
}
```

## Configuration reference

See `.env.example` for all environment variables with descriptions and defaults.

## Project structure

```text
services/ml-service/
├── src/
│   ├── main.py                 # Entry point (FastAPI + Kafka thread)
│   ├── config.py               # All configuration from env vars
│   ├── kafka/
│   │   ├── consumer.py         # Kafka consumer with DLQ routing
│   │   └── producer.py         # Kafka producer (idempotent delivery)
│   ├── models/
│   │   ├── feature_extractor.py  # Feature vectors from Kafka messages
│   │   └── anomaly_detector.py   # Per-tenant IsolationForest + Z-score
│   ├── pipeline/
│   │   └── processor.py        # Orchestrates consumer → model → producer
│   └── api/
│       └── health.py           # FastAPI health / management endpoints
├── tests/
│   ├── test_feature_extractor.py
│   ├── test_anomaly_detector.py
│   └── test_processor.py
├── requirements.txt
├── requirements-dev.txt
├── Dockerfile
├── .env.example
└── pyproject.toml
```
