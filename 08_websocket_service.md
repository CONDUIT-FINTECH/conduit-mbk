# WebSocket Service v2 — Scalable Real-Time Push

## What Changed (Before → After)

| Aspect | v1 (Before) | v2 (After) |
|---|---|---|
| **Horizontal Scaling** | Instance-local broadcast only | **Redis Pub/Sub** fan-out to all instances |
| **Backpressure** | None — slow clients stall the loop | Drops clients exceeding `WS_MAX_BUFFERED_BYTES` (64KB) |
| **Event Format** | Ad-hoc `{ type, channel, data }` | Structured envelope with `eventType`, `latency`, `meta` |
| **Latency Tracking** | None | Bridge latency + client-facing latency in every message |
| **Batch Subscribe** | One channel at a time | `{ type: "subscribe", channels: [...] }` |
| **Health Endpoint** | Basic connection count | Connection stats + bridge latency + per-channel breakdown |

---

## Architecture

```mermaid
flowchart TD
    subgraph Kafka["Kafka Topics"]
        T1["conduit.events.ingested"]
        T2["conduit.metrics.computed"]
        T3["conduit.incidents.events"]
        T4["conduit.remediations"]
        T5["conduit.ml.predictions"]
    end

    subgraph Instance1["WS Instance 1"]
        KB["Kafka Bridge\n(consumer)"]
        SE1["Subscription Engine"]
        CM1["Connection Manager"]
        C1["Clients A, B"]
    end

    subgraph Redis["Redis Pub/Sub"]
        R["5 channels\nconduit:ws:*"]
    end

    subgraph Instance2["WS Instance 2"]
        SE2["Subscription Engine"]
        CM2["Connection Manager"]
        C2["Clients C, D"]
    end

    subgraph Instance3["WS Instance 3"]
        SE3["Subscription Engine"]
        CM3["Connection Manager"]
        C3["Clients E, F"]
    end

    T1 & T2 & T3 & T4 & T5 --> KB
    KB -->|"PUBLISH"| R
    R -->|"SUBSCRIBE"| SE1
    R -->|"SUBSCRIBE"| SE2
    R -->|"SUBSCRIBE"| SE3
    SE1 --> CM1 --> C1
    SE2 --> CM2 --> C2
    SE3 --> CM3 --> C3

    style Redis fill:#ef4444,color:#fff
    style KB fill:#3b82f6,color:#fff
```

> [!IMPORTANT]
> Only **one instance** runs the Kafka consumer (partition assignment). Redis Pub/Sub fans every message to **all instances**, ensuring every connected client receives updates regardless of which pod they're on.

---

## Data Flow (Latency Path)

```mermaid
sequenceDiagram
    participant K as Kafka
    participant B as Kafka Bridge (Instance 1)
    participant R as Redis Pub/Sub
    participant S as Sub Engine (Instance N)
    participant W as WebSocket Client

    Note over K,W: Target: < 500ms end-to-end

    K->>B: Message consumed (~5ms)
    B->>B: Build envelope + track latency
    B->>R: PUBLISH conduit:ws:channel (~1ms)
    R->>S: Fan-out to all instances (~1ms)
    S->>S: Build client message + compute latency
    S->>W: ws.send() (~1ms)

    Note over K,W: Typical total: 8-15ms
```

---

## Event Envelope (Client-Facing)

Every message pushed to WebSocket clients follows this format:

```json
{
  "type": "event",
  "channel": "incidents:alerts",
  "eventType": "incident.detected",
  "data": {
    "incidentId": "a1b2c3d4-...",
    "tenantId": "acme-corp",
    "severity": "high",
    "type": "error_rate_breach",
    "description": "Error rate 12.3% exceeds threshold 5%",
    "detectedAt": "2026-05-03T00:10:00.000Z"
  },
  "timestamp": "2026-05-03T00:10:00.012Z",
  "latency": 12
}
```

| Field | Description |
|---|---|
| `type` | Always `"event"` for data messages |
| `channel` | The WebSocket channel (e.g., `incidents:alerts`) |
| `eventType` | Derived from Kafka payload or topic (e.g., `incident.detected`) |
| `data` | Raw payload from the upstream service |
| `timestamp` | Server-side timestamp when broadcast was initiated |
| `latency` | Milliseconds from Redis bridge to WebSocket send (SLA tracking) |

---

## Channel Registry

| WebSocket Channel | Kafka Source | Content |
|---|---|---|
| `events:live` | `conduit.events.ingested` | Raw ingested events |
| `metrics:dashboard` | `conduit.metrics.computed` | Aggregated metric snapshots |
| `incidents:alerts` | `conduit.incidents.events` | Incident lifecycle events |
| `remediations:actions` | `conduit.remediations` | Autonomous remediation actions |
| `ml:predictions` | `conduit.ml.predictions` | ML anomaly predictions |

---

## Client Protocol

### Connection

```
ws://host:4006?token=<JWT>
```

### Welcome Message (on connect)

```json
{
  "type": "connected",
  "clientId": "a1b2c3d4-...",
  "availableChannels": ["events:live", "metrics:dashboard", "incidents:alerts", "remediations:actions", "ml:predictions"],
  "serverTime": "2026-05-03T00:10:00.000Z"
}
```

### Subscribe (single or batch)

```json
{ "type": "subscribe", "channel": "incidents:alerts" }
```

```json
{ "type": "subscribe", "channels": ["incidents:alerts", "metrics:dashboard"] }
```

### Unsubscribe

```json
{ "type": "unsubscribe", "channel": "incidents:alerts" }
```

### Application Ping

```json
{ "type": "ping" }
→ { "type": "pong", "serverTime": "..." }
```

---

## Code Structure

```
websocket-service/
├── package.json
└── src/
    ├── index.js                          # Boot lifecycle + health + readiness
    ├── infra/
    │   └── redisPubSub.js                # Redis Pub/Sub adapter (horizontal scaling)
    ├── bridge/
    │   └── kafkaBridge.js                # Kafka → Redis (envelope builder + latency)
    ├── subscriptions/
    │   └── subscriptionEngine.js         # Redis → local broadcast
    └── connections/
        └── connectionManager.js          # WS server, auth, heartbeat, backpressure
```

---

## Horizontal Scaling Strategy

```
                    ┌─────────────────────┐
                    │  Load Balancer       │
                    │  (sticky sessions    │
                    │   via IP hash)       │
                    └────────┬────────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
        ┌─────┴─────┐ ┌─────┴─────┐ ┌─────┴─────┐
        │  WS Pod 1  │ │  WS Pod 2  │ │  WS Pod 3  │
        │  Kafka ✓   │ │  Kafka ✗   │ │  Kafka ✗   │
        │  Redis Sub │ │  Redis Sub │ │  Redis Sub │
        └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
              │              │              │
              └──────────────┴──────────────┘
                             │
                    ┌────────┴────────┐
                    │   Redis Server   │
                    └─────────────────┘
```

**Key points:**
- Only 1 pod consumes Kafka (partition assignment via consumer group)
- ALL pods subscribe to Redis Pub/Sub channels
- Clients use sticky sessions (IP hash) at the load balancer
- Any pod can serve any client — Redis ensures message delivery

---

## Backpressure Protection

```
  Client send buffer > 64KB?
      │
      ├── YES → Skip this client (log warning)
      │          Client will "catch up" on next message
      │
      └── NO  → ws.send(serialized)
```

This prevents slow clients (e.g., mobile on 3G) from blocking the broadcast loop and stalling all other clients on the same instance.

---

## Tuning Knobs

| Env Variable | Default | Description |
|---|---|---|
| `WS_PORT` | `4006` | HTTP + WebSocket server port |
| `WS_MAX_BUFFERED_BYTES` | `65536` | Backpressure threshold (64KB) |
| `JWT_SECRET` | `dev-secret` | JWT verification secret |

---

## Files Modified

| File | Change |
|---|---|
| [redisPubSub.js](file:///d:/congnigant/backend-v1/services/websocket-service/src/infra/redisPubSub.js) | **NEW** — Redis pub/sub adapter for horizontal scaling |
| [kafkaBridge.js](file:///d:/congnigant/backend-v1/services/websocket-service/src/bridge/kafkaBridge.js) | **REBUILT** — Kafka → Redis with structured envelope + latency tracking |
| [subscriptionEngine.js](file:///d:/congnigant/backend-v1/services/websocket-service/src/subscriptions/subscriptionEngine.js) | **REBUILT** — Redis subscriber with client-facing latency |
| [connectionManager.js](file:///d:/congnigant/backend-v1/services/websocket-service/src/connections/connectionManager.js) | **REBUILT** — Backpressure, batch subscribe, per-channel stats |
| [index.js](file:///d:/congnigant/backend-v1/services/websocket-service/src/index.js) | **REBUILT** — Deterministic boot, readiness probe, detailed health |
| [.env.example](file:///d:/congnigant/backend-v1/.env.example) | **UPDATED** — Added `WS_MAX_BUFFERED_BYTES` |
