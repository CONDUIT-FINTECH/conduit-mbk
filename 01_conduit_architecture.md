# Conduit v3 — Finalized Production Architecture

## 1. High-Level Diagram

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

---

## 2. Strict Service Responsibilities

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

## 3. Communication Matrix & Rules

### Protocol Rules
1.  **NO SERVICE-TO-SERVICE HTTP**: Services must not call each other directly via REST. All state propagation must happen via Kafka topics.
2.  **GATEWAY EXCLUSION**: The API Gateway is the *only* component allowed to call services via HTTP (Proxy mode).
3.  **KAFKA AS SOURCE OF TRUTH**: Any service needing data from another service (e.g., Incident Svc needing Metrics) must consume from the relevant Kafka topic.
4.  **CQRS COMPLIANCE**:
    *   **Write Path**: Client → Gateway → Ingest → Kafka.
    *   **Read Path**: Client → Gateway → Query → Materialized DB.

### Topic Registry
| Topic | Source | Purpose |
|---|---|---|
| `conduit.events.ingested` | Ingestion | Raw stream of system events |
| `conduit.metrics.computed` | Metrics | Aggregated statistical snapshots |
| `conduit.incidents` | Incident | Detected anomalies and status changes |
| `conduit.remediations` | Remediation | Actions taken by the auto-healing engine |

---

## 4. Final Review Checklist

*   **[✅] No mixed responsibilities**:
    *   `Metrics` only aggregates; it has no REST API.
    *   `Query` only serves reads; it has no business logic.
    *   `Incident` only detects and manages state; it doesn't aggregate.
*   **[✅] Real-time layer**: `WebSocket Service` provides sub-second push for all system topics.
*   **[✅] Separation of Incident/Remediation/Metrics**:
    *   Metrics computes stats.
    *   Incident detects breaches.
    *   Remediation acts on breaches.
*   **[✅] No direct dependencies**: All inter-service data flow is mediated by Kafka.

---

## 5. Deployment Map

| Port | Service | Database Requirement |
|---|---|---|
| 4000 | API Gateway | Redis (Rate limiting) |
| 4001 | Ingestion | Redis (Idempotency) |
| 4003 | Metrics | TimescaleDB (Write-only) |
| 4004 | Query | PG + Mongo + Redis (Read models) |
| 4005 | Incident | PostgreSQL (Incidents) |
| 4006 | WebSocket | N/A (State in Kafka) |
| N/A | Remediation | N/A (Stateless actor) |
