# Conduit — Event-Driven Fintech Backend Platform

[![Architecture: CQRS](https://img.shields.io/badge/Architecture-CQRS-blueviolet)](#1-system-architecture)
[![Messaging: Kafka](https://img.shields.io/badge/Messaging-Kafka-orange)](#2-kafka-topic-design)
[![ML: IsolationForest](https://img.shields.io/badge/ML-IsolationForest-blue)](#ml-service)
[![Python: 3.12](https://img.shields.io/badge/Python-3.12-yellow)](#ml-service)
[![License: MIT](https://img.shields.io/badge/License-MIT-green)](https://opensource.org/licenses/MIT)

Conduit is a high-performance, event-driven microservices ecosystem designed for real-time fintech data processing, anomaly detection, and automated remediation.

## 🚀 Quick Start

### Prerequisites
- Node.js >= 20.0.0
- Python >= 3.12 (for ML Service)
- Docker & Docker Compose
- Kafka Cluster (via Docker)

### Installation & Development
```bash
# Install Node.js dependencies
npm install

# Install Python ML service dependencies
cd services/ml-service
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cd ../..

# Start infrastructure (Kafka, Postgres, Redis, TimescaleDB, MongoDB)
docker-compose up -d

# Run all Node.js services in development mode
npm run dev:all

# Run Python ML service (separate terminal)
cd services/ml-service && python -m src.main
```

---

## 📖 Table of Contents
- [1. System Architecture](#1-system-architecture)
- [2. Kafka Topic Design](#2-kafka-topic-design)
- [3. Ingestion Service](#3-ingestion_service)
- [4. Incident Service](#4-incident-service)
- [5. Remediation Service](#5-remediation-service)
- [6. Metrics Service](#6-metrics-service)
- [7. Query Service](#7-query-service)
- [8. WebSocket Service](#8-websocket-service)
- [9. ML Service](#9-ml-service)
- [10. Tech Stack & Structure](#10-tech-stack--structure)

---

## 1. System Architecture

### High-Level Diagram
```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                CLIENTS                                           │
│            (Web Dashboard · Mobile · Partner APIs · CLI)                         │
└─────────────┬──────────────────────────────────────────────┬────────────────────┘
              │ HTTPS (REST)                                 │ WSS (persistent)
              ▼                                              ▼
┌──────────────────────────────┐              ┌──────────────────────────────────┐
│   ① API GATEWAY  (:4000)    │              │  ⑦ WEBSOCKET SERVICE  (:4006)   │
│                              │              │                                  │
│  • Auth / Rate Limit        │              │  • Real-time Fan-out             │
│  • Request Routing          │              │  • Client Room Management        │
└──────┬────────────┬─────────┘              └──────────▲───────────────────────┘
       │            │                                   │ Consume
       │ HTTP       │ HTTP                              │
       ▼            ▼                                   │
┌────────────┐ ┌─────────────┐                          │
│ ② INGEST   │ │ ⑥ QUERY     │                          │
│   SERVICE  │ │   SERVICE   │                          │
│   (:4001)  │ │   (:4004)   │                          │
│            │ │             │                          │
│ • Write Gt │ │ • Read Model│                          │
└─────┬──────┘ └──────▲──────┘                          │
      │               │                                 │
      │ Produce       │ Read (Materialized)             │
      ▼               │                                 │
┌──────────────────────────────────────────────────────────────────────────────────┐
│                          ③ KAFKA EVENT BUS                                      │
│                                                                                  │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐│
│  │ conduit.events  │ │ conduit.metrics │ │ conduit         │ │ conduit         ││
│  │ .ingested       │ │ .computed       │ │ .incidents      │ │ .remediations   ││
│  └────────┬────────┘ └────────▲────────┘ └───────┬─────────┘ └───────▲─────────┘│
│           │                   │                  │                   │          │
└───────────┼───────────────────┼──────────────────┼───────────────────┼──────────┘
            │                   │                  │                   │
     ┌──────┴──────┐     ┌─────┴──────┐    ┌──────┴──────┐     ┌──────┴──────┐
     │ Consume     │     │ Produce    │    │ Consume     │     │ Produce     │
     ▼             ▼     │            │    ▼             ▼     │             │
┌──────────┐ ┌──────────┐│            │ ┌──────────┐ ┌──────────┐│             │
│ ④ METRIC │ │ ⑤ INCID. ││            │ │ ⑧ REMED. │ │ ⑤ INCID. ││             │
│  SERVICE │ │  SERVICE ││            │ │  SERVICE │ │  SERVICE ││             │
│  (:4003) │ │  (:4005) │┘            │ │  (Auto)  │ │ (CRUD)   │┘             │
└──────────┘ └──────────┘             │ └──────────┘ └──────────┘              │
                                      │        │                               │
                                      └────────┴───────────────────────────────┘
```

### Strict Service Responsibilities
| Service | Primary Responsibility | CQRS Role | Communication |
|---|---|---|---|
| **API Gateway** | Perimeter security, routing | Entry Point | HTTP (Inbound) |
| **Ingestion** | Write validation, enrichment | Command | Kafka (Outbound) |
| **Kafka Bus** | Persistent event log | Message Hub | Event Stream |
| **Metrics** | Pure aggregation / windowing | Processor | Kafka (In/Out) |
| **Incident** | Detection / Status CRUD | Processor | Kafka (In/Out) + HTTP (Reads) |
| **Remediation** | Autonomous corrective actions | Actor | Kafka (In/Out) |
| **Query** | High-perf materialized reads | Query | HTTP (Outbound) |
| **WebSocket** | Live state synchronization | Push | WSS (Outbound) |

---

## 2. Kafka Topic Design

### Topic Registry
| Topic | Partitions | Key | Retention | Compression | Cleanup Policy |
|---|:---:|---|---|---|---|
| `conduit.events.ingested` | 12 | `tenantId` | 7 days | Snappy | delete |
| `conduit.metrics.computed` | 6 | `tenantId` | 3 days | Snappy | compact+delete |
| `conduit.incidents.events` | 6 | `tenantId` | 30 days | Snappy | delete |
| `conduit.ml.predictions` | 6 | `tenantId` | 14 days | Snappy | delete |
| `conduit.remediations` | 3 | `tenantId` | 30 days | Snappy | delete |

### Consumer Groups
| Group ID | Service | Subscribed Topics |
|---|---|---|
| `conduit-metrics-group` | Metrics Service | `events.ingested` |
| `conduit-query-group` | Query Service | `events.ingested`, `metrics.computed`, `incidents.events`, `ml.predictions` |
| `conduit-incident-group` | Incident Service | `events.ingested`, `metrics.computed`, `ml.predictions` |
| `conduit-websocket-group` | WebSocket Service | `events.ingested`, `metrics.computed`, `incidents.events`, `remediations`, `ml.predictions` |
| `conduit-remediation-group` | Remediation Service | `incidents.events` |

---

## 3. Ingestion Service

The entry point for all raw events. Optimized for high throughput and durability.

### Key Features
- **Persistent Producer**: Zero TCP/TLS handshake overhead per request.
- **Redis Idempotency**: `SETNX` with 24h TTL to prevent duplicate processing.
- **Batch Support**: `POST /ingest/batch` for up to 500 events in one round-trip.
- **Snappy Compression**: Optimized for network throughput.

---

## 4. Incident Service

Anomaly detection and lifecycle management engine.

### State Machine
`DETECTED → ACTIVE → RESOLVED`
- **Detected**: Initial trigger from threshold breach or ML anomaly.
- **Active**: Acknowledged by operator or auto-escalated.
- **Resolved**: Condition cleared or manual fix confirmed.

### Detectors
- **Threshold Detector**: Monitored via `metrics.computed` (Error Rate, P95, Success Rate).
- **ML Detector**: Monitored via `ml.predictions` (Anomaly Score, Label Classification).

---

## 5. Remediation Service

Autonomous corrective actor that executes fixes when high-severity incidents are detected.

### Execution Engine
- **Exponential Backoff**: 2s × 2^n + jitter, max 3 retries.
- **Action Registry**:
  - `auto_rollback` (Error Rate)
  - `scale_out` (Latency)
  - `health_check_sweep` (Success Rate)
  - `adaptive_throttle` (ML Anomaly)

---

## 6. Metrics Service

Pure aggregation pipeline for windowed statistical snapshots.

### Aggregation Mechanics
- **Sliding Window**: 1-minute window with p50/p95/p99 percentiles.
- **Dual-Write Path**: 
  - **Hot Path**: Instant Kafka snapshots for live dashboards.
  - **Cold Path**: TimescaleDB hypertable with tiered retention (7d raw → 90d agg).

---

## 7. Query Service

The read-side of the CQRS pattern. Optimized for sub-200ms materialized views.

### Storage Split
- **PostgreSQL**: Structured events, incidents, and remediations.
- **MongoDB**: Schema-less ML predictions and feature vectors.
- **Redis**: Hot-path metrics and 30s query cache.

---

## 8. WebSocket Service

Scalable real-time push service using Redis Pub/Sub for horizontal fan-out.

### Features
- **Horizontal Scaling**: Redis Pub/Sub fanz messages to all pods.
- **Backpressure**: Drops clients exceeding 64KB buffer to protect the event loop.
- **Latency Tracking**: End-to-end SLA tracking in every message envelope.

---

## 9. ML Service

Real-time per-tenant anomaly detection — the Python component of Conduit.

### Architecture
- **Consumes**: `conduit.metrics.computed` → feature extraction → per-tenant IsolationForest / Z-score
- **Produces**: `conduit.ml.predictions` → consumed by Incident, Query, and WebSocket services

### Model Pipeline
- **Warm-up phase**: Z-score baseline until 50 samples collected per tenant
- **Production phase**: IsolationForest (scikit-learn) with periodic re-fitting
- **Labels**: `system_failure`, `performance_degradation`, `traffic_spike`, `latency_drift`, `metric_anomaly`
- Anomaly score ≥ 0.75 → incident created by Incident Service

### HTTP Endpoints
- `GET /health` — liveness probe
- `GET /ready` — readiness probe (K8s-compatible)
- `GET /metrics` — pipeline statistics
- `GET /models` — per-tenant detector state
- `GET /docs` — OpenAPI documentation

See [09_ml_service.md](09_ml_service.md) and [services/ml-service/README.md](services/ml-service/README.md) for full documentation.

---

## 10. Tech Stack & Structure

| Component | Technology |
|---|---|
| **Language (services)** | Node.js (ESM) |
| **Language (ML)** | Python 3.12 |
| **Messaging** | Apache Kafka |
| **Databases** | PostgreSQL, TimescaleDB, MongoDB, Redis |
| **ML Framework** | scikit-learn (IsolationForest) |
| **Testing** | Jest (Node.js), pytest (Python) |

### Repository Structure
```text
.
├── packages/             # Shared libraries (Kafka, Errors, Auth)
├── services/             # Microservices
│   ├── api-gateway/      # Entry Point
│   ├── ingestion-service/ # Command Side
│   ├── incident-service/  # Logic/State
│   ├── metrics-service/   # Processing
│   ├── ml-service/        # ★ Anomaly Detection (Python)
│   ├── remediation-service/ # Acting
│   ├── query-service/     # Query Side
│   └── websocket-service/ # Real-time Push
├── infra/                # Docker & DB Config
└── ...
```

---
*Conduit — Hardened for Production.*
