#!/bin/bash
# ═══════════════════════════════════════════════════════════════
#  Conduit Kafka Topic Provisioning (v3)
#  Run after Kafka is healthy:
#    docker exec -it <kafka-container> bash /topics.sh
#
#  Partition Strategy:
#    - Key:   tenantId (all topics) → per-tenant ordering
#    - DLQ:   keyed by originalTopic for routing
#
#  Retention Strategy:
#    - Hot path (events, metrics):  short retention, high throughput
#    - Incident/remediation:        30d for audit trail
#    - ML predictions:              14d for model retraining windows
#    - DLQ:                         90d for ops investigation
# ═══════════════════════════════════════════════════════════════

BOOTSTRAP="localhost:9092"

echo "🔧 Creating Conduit Kafka topics..."
echo ""

# ─── conduit.events.ingested ─────────────────────
# Highest volume topic. 12 partitions for parallelism.
# Key: tenantId | Retention: 7 days
kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists \
  --topic conduit.events.ingested \
  --partitions 12 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config segment.ms=86400000 \
  --config cleanup.policy=delete \
  --config compression.type=snappy \
  --config max.message.bytes=1048576
echo "  ✅ conduit.events.ingested  (12p, 7d, snappy)"

# ─── conduit.metrics.computed ────────────────────
# Aggregated snapshots. Lower volume, shorter retention.
# Key: tenantId | Retention: 3 days
kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists \
  --topic conduit.metrics.computed \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=259200000 \
  --config segment.ms=86400000 \
  --config cleanup.policy=compact,delete \
  --config compression.type=snappy \
  --config min.compaction.lag.ms=3600000
echo "  ✅ conduit.metrics.computed  (6p, 3d, compact+delete)"

# ─── conduit.incidents.events ────────────────────
# Incident lifecycle events. 30d for compliance audit.
# Key: tenantId | Retention: 30 days
kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists \
  --topic conduit.incidents.events \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config segment.ms=86400000 \
  --config cleanup.policy=delete \
  --config compression.type=snappy
echo "  ✅ conduit.incidents.events  (6p, 30d)"

# ─── conduit.remediations ───────────────────────
# Autonomous corrective actions. 30d for audit.
# Key: tenantId | Retention: 30 days
kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists \
  --topic conduit.remediations \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=2592000000 \
  --config segment.ms=86400000 \
  --config cleanup.policy=delete \
  --config compression.type=snappy
echo "  ✅ conduit.remediations      (3p, 30d)"

# ─── conduit.ml.predictions ─────────────────────
# ML pipeline outputs (anomaly scores, churn predictions, etc.)
# Key: tenantId | Retention: 14 days
kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists \
  --topic conduit.ml.predictions \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=1209600000 \
  --config segment.ms=86400000 \
  --config cleanup.policy=delete \
  --config compression.type=snappy \
  --config max.message.bytes=2097152
echo "  ✅ conduit.ml.predictions    (6p, 14d)"

# ─── conduit.dlq ────────────────────────────────
# Dead letter queue. 90d for ops investigation.
# Key: originalTopic | Retention: 90 days
kafka-topics.sh --bootstrap-server $BOOTSTRAP \
  --create --if-not-exists \
  --topic conduit.dlq \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=7776000000 \
  --config segment.ms=604800000 \
  --config cleanup.policy=delete \
  --config compression.type=gzip \
  --config max.message.bytes=5242880
echo "  ✅ conduit.dlq               (3p, 90d, gzip)"

echo ""
echo "═══════════════════════════════════════════════"
echo "✅ All Conduit topics created (v3)"
echo "═══════════════════════════════════════════════"
echo ""
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list
