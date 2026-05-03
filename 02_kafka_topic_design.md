# Conduit Kafka Topic Design v3

## Topic Registry

| Topic | Partitions | Key | Retention | Compression | Cleanup Policy |
|---|:---:|---|---|---|---|
| `conduit.events.ingested` | **12** | `tenantId` | 7 days | Snappy | delete |
| `conduit.metrics.computed` | **6** | `tenantId` | 3 days | Snappy | compact+delete |
| `conduit.incidents.events` | **6** | `tenantId` | 30 days | Snappy | delete |
| `conduit.ml.predictions` | **6** | `tenantId` | 14 days | Snappy | delete |
| `conduit.remediations` | **3** | `tenantId` | 30 days | Snappy | delete |
| `conduit.dlq` | **3** | `originalTopic` | 90 days | GZIP | delete |

---

## Partition Strategy

```mermaid
flowchart LR
    subgraph PartitionKey["Partition Key: tenantId"]
        direction TB
        T1["Tenant A → Partition 0"]
        T2["Tenant B → Partition 1"]
        T3["Tenant C → Partition 2"]
        T4["..."]
    end

    subgraph Guarantees["Ordering Guarantees"]
        G1["✅ All events for Tenant A arrive in order"]
        G2["✅ Metrics for Tenant A arrive in order"]
        G3["✅ Incidents for Tenant A arrive in order"]
        G4["⚠️ No global ordering across tenants (by design)"]
    end

    PartitionKey --> Guarantees
```

**Rationale for partition counts:**

| Topic | Count | Reasoning |
|---|:---:|---|
| `events.ingested` | 12 | Highest volume (1000+ msg/sec). 12 allows 12 parallel consumers per group for scale-out. |
| `metrics.computed` | 6 | Aggregated snapshots, lower volume (~10x fewer than raw events). |
| `incidents.events` | 6 | Low volume but critical. 6 allows future multi-AZ consumer parallelism. |
| `ml.predictions` | 6 | Moderate volume, aligned with metrics for balanced consumer assignment. |
| `remediations` | 3 | Lowest volume (only triggered incidents produce actions). |
| `dlq` | 3 | Minimal expected volume. Keyed by `originalTopic` for routing. |

---

## Retention Strategy

```
                    Hot Path                            Cold Path
              ┌─────────────────┐               ┌──────────────────┐
  events      │     7 days      │   incidents    │     30 days      │
  metrics     │     3 days      │   remediations │     30 days      │
              └─────────────────┘   ml.predict.  │     14 days      │
                                    dlq          │     90 days      │
                                                 └──────────────────┘
```

| Topic | Retention | Why |
|---|---|---|
| `events.ingested` | **7d** | High volume, consumed within seconds. 7d covers replay after extended outages. |
| `metrics.computed` | **3d** | Compact+delete: latest per-key is always available; older snapshots auto-purge. |
| `incidents.events` | **30d** | Compliance: incident audit trail must survive monthly review cycles. |
| `ml.predictions` | **14d** | Model retraining windows typically span 7–14 days of historical predictions. |
| `remediations` | **30d** | Audit: automated actions must be traceable for 30 days post-incident. |
| `dlq` | **90d** | Ops investigation: failed messages may not be reviewed immediately. |

---

## Consumer Groups

| Group ID | Service | Subscribed Topics |
|---|---|---|
| `conduit-metrics-group` | Metrics Service | `events.ingested` |
| `conduit-query-group` | Query Service | `events.ingested`, `metrics.computed`, `incidents.events`, `ml.predictions` |
| `conduit-incident-group` | Incident Service | `events.ingested`, `metrics.computed`, `ml.predictions` |
| `conduit-websocket-group` | WebSocket Service | `events.ingested`, `metrics.computed`, `incidents.events`, `remediations`, `ml.predictions` |
| `conduit-remediation-group` | Remediation Service | `incidents.events` |
| `conduit-ml-group` | ML Pipeline | `ml.predictions` (self-monitoring) |
| `conduit-dlq-monitor-group` | Ops Tooling | `dlq` |

---

## Fan-Out Topology

```mermaid
flowchart TD
    subgraph Producers
        ING["Ingestion Service"]
        MET["Metrics Service"]
        INC["Incident Service"]
        REM["Remediation Service"]
        MLP["ML Pipeline"]
    end

    subgraph Topics
        T1["conduit.events.ingested\n12 partitions | 7d"]
        T2["conduit.metrics.computed\n6 partitions | 3d"]
        T3["conduit.incidents.events\n6 partitions | 30d"]
        T4["conduit.remediations\n3 partitions | 30d"]
        T5["conduit.ml.predictions\n6 partitions | 14d"]
        T6["conduit.dlq\n3 partitions | 90d"]
    end

    subgraph Consumers
        C_MET["metrics-group"]
        C_QRY["query-group"]
        C_INC["incident-group"]
        C_WS["websocket-group"]
        C_REM["remediation-group"]
        C_ML["ml-group"]
        C_DLQ["dlq-monitor-group"]
    end

    ING --> T1
    MET --> T2
    INC --> T3
    REM --> T4
    MLP --> T5

    T1 --> C_MET
    T1 --> C_QRY
    T1 --> C_INC
    T1 --> C_WS

    T2 --> C_QRY
    T2 --> C_INC
    T2 --> C_WS

    T3 --> C_REM
    T3 --> C_QRY
    T3 --> C_WS

    T4 --> C_QRY
    T4 --> C_WS

    T5 --> C_INC
    T5 --> C_QRY
    T5 --> C_WS
    T5 --> C_ML

    T6 --> C_DLQ

    style T1 fill:#3b82f6,color:#fff
    style T2 fill:#22c55e,color:#fff
    style T3 fill:#ef4444,color:#fff
    style T4 fill:#f59e0b,color:#000
    style T5 fill:#8b5cf6,color:#fff
    style T6 fill:#6b7280,color:#fff
```

---

## WebSocket Channel Mapping

| Kafka Topic | WebSocket Channel | Client Subscription |
|---|---|---|
| `conduit.events.ingested` | `events:live` | Real-time event feed |
| `conduit.metrics.computed` | `metrics:dashboard` | Dashboard metric updates |
| `conduit.incidents.events` | `incidents:alerts` | Incident alert notifications |
| `conduit.remediations` | `remediations:actions` | Autonomous action feed |
| `conduit.ml.predictions` | `ml:predictions` | ML anomaly score updates |

---

## Files Modified

| File | Change |
|---|---|
| [topics.js](file:///d:/congnigant/backend-v1/packages/shared/src/kafka/topics.js) | Added `ML_PREDICTIONS`, `CONSUMER_GROUPS`, `SUBSCRIPTIONS`. Renamed `conduit.incidents` → `conduit.incidents.events` |
| [topics.sh](file:///d:/congnigant/backend-v1/infra/kafka/topics.sh) | Complete rewrite with differentiated retention, compression, segment policies |
| [index.js](file:///d:/congnigant/backend-v1/packages/shared/src/index.js) | Barrel now exports `CONSUMER_GROUPS` and `SUBSCRIPTIONS` |
| [kafkaBridge.js](file:///d:/congnigant/backend-v1/services/websocket-service/src/bridge/kafkaBridge.js) | Added `ML_PREDICTIONS` to subscription |
| [subscriptionEngine.js](file:///d:/congnigant/backend-v1/services/websocket-service/src/subscriptions/subscriptionEngine.js) | Added `ml:predictions` channel |
| [connectionManager.js](file:///d:/congnigant/backend-v1/services/websocket-service/src/connections/connectionManager.js) | Added `ml:predictions` to whitelist |
| [detectionConsumer.js](file:///d:/congnigant/backend-v1/services/incident-service/src/consumers/detectionConsumer.js) | Now consumes `ML_PREDICTIONS` for ML-driven detection |
| [materializer.js](file:///d:/congnigant/backend-v1/services/query-service/src/consumers/materializer.js) | Materializes incidents + ML predictions |
| [.env.example](file:///d:/congnigant/backend-v1/.env.example) | Added `KAFKA_GROUP_ID_ML`, `KAFKA_GROUP_ID_DLQ` |
