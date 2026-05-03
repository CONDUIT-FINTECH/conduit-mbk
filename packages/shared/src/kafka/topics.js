/**
 * ═══════════════════════════════════════════════════════════════
 *  Conduit Kafka Topic Registry
 *  Single source of truth — never hardcode topic strings in services.
 * ═══════════════════════════════════════════════════════════════
 *
 *  Topic Naming Convention:
 *    conduit.<domain>.<event-type>
 *
 *  Partition Key Strategy:
 *    All topics partition by `tenantId` unless noted otherwise.
 *    This guarantees per-tenant ordering within each partition.
 *
 *  See infra/kafka/topics.sh for creation commands with
 *  partition counts and retention policies.
 * ═══════════════════════════════════════════════════════════════
 */

const TOPICS = Object.freeze({
  // ─── Write Path ────────────────────────────────
  // Produced by: Ingestion Service
  // Consumed by: Metrics, Incident, Query, WebSocket
  // Partitions: 12 | Retention: 7d | Key: tenantId
  EVENTS_INGESTED: 'conduit.events.ingested',

  // ─── Compute Path ─────────────────────────────
  // Produced by: Metrics Service
  // Consumed by: Incident, Query, WebSocket
  // Partitions: 6 | Retention: 3d | Key: tenantId
  METRICS_COMPUTED: 'conduit.metrics.computed',

  // ─── Incident Path ────────────────────────────
  // Produced by: Incident Service
  // Consumed by: Remediation, WebSocket, Query
  // Partitions: 6 | Retention: 30d | Key: tenantId
  INCIDENTS: 'conduit.incidents.events',

  // ─── Remediation Path ─────────────────────────
  // Produced by: Remediation Service
  // Consumed by: WebSocket, Query
  // Partitions: 3 | Retention: 30d | Key: tenantId
  REMEDIATIONS: 'conduit.remediations',

  // ─── ML / Predictions Path ────────────────────
  // Produced by: ML Pipeline (external or internal)
  // Consumed by: Incident, Query, WebSocket
  // Partitions: 6 | Retention: 14d | Key: tenantId
  ML_PREDICTIONS: 'conduit.ml.predictions',

  // ─── Dead Letter Queue ────────────────────────
  // Produced by: Any consumer on processing failure
  // Consumed by: Ops tooling / alerting
  // Partitions: 3 | Retention: 90d | Key: originalTopic
  DLQ: 'conduit.dlq',
});

/**
 * Consumer group registry.
 * Each group gets independent offset tracking.
 * Multiple groups on the same topic = fan-out pattern.
 */
const CONSUMER_GROUPS = Object.freeze({
  METRICS:      process.env.KAFKA_GROUP_ID_METRICS      || 'conduit-metrics-group',
  QUERY:        process.env.KAFKA_GROUP_ID_QUERY         || 'conduit-query-group',
  INCIDENT:     process.env.KAFKA_GROUP_ID_INCIDENT      || 'conduit-incident-group',
  WEBSOCKET:    process.env.KAFKA_GROUP_ID_WS             || 'conduit-websocket-group',
  REMEDIATION:  process.env.KAFKA_GROUP_ID_REMEDIATION    || 'conduit-remediation-group',
  ML:           process.env.KAFKA_GROUP_ID_ML             || 'conduit-ml-group',
  DLQ_MONITOR:  process.env.KAFKA_GROUP_ID_DLQ            || 'conduit-dlq-monitor-group',
});

/**
 * Topic → Consumer Group subscription matrix.
 * Documents which groups read from which topics.
 */
const SUBSCRIPTIONS = Object.freeze({
  [TOPICS.EVENTS_INGESTED]: [
    CONSUMER_GROUPS.METRICS,
    CONSUMER_GROUPS.INCIDENT,
    CONSUMER_GROUPS.QUERY,
    CONSUMER_GROUPS.WEBSOCKET,
  ],
  [TOPICS.METRICS_COMPUTED]: [
    CONSUMER_GROUPS.INCIDENT,
    CONSUMER_GROUPS.QUERY,
    CONSUMER_GROUPS.WEBSOCKET,
  ],
  [TOPICS.INCIDENTS]: [
    CONSUMER_GROUPS.REMEDIATION,
    CONSUMER_GROUPS.QUERY,
    CONSUMER_GROUPS.WEBSOCKET,
  ],
  [TOPICS.REMEDIATIONS]: [
    CONSUMER_GROUPS.QUERY,
    CONSUMER_GROUPS.WEBSOCKET,
  ],
  [TOPICS.ML_PREDICTIONS]: [
    CONSUMER_GROUPS.INCIDENT,
    CONSUMER_GROUPS.QUERY,
    CONSUMER_GROUPS.WEBSOCKET,
    CONSUMER_GROUPS.ML,
  ],
  [TOPICS.DLQ]: [
    CONSUMER_GROUPS.DLQ_MONITOR,
  ],
});

module.exports = { ...TOPICS, TOPICS, CONSUMER_GROUPS, SUBSCRIPTIONS };
