const { kafka } = require('@conduit/shared');
const { createConsumer, consumeMessages, TOPICS } = kafka;
const { evaluateMetrics } = require('../detectors/thresholdDetector');
const { evaluateMLPrediction } = require('../detectors/mlDetector');
const { tryAcquireDedupLock, addIncident } = require('../state/incidentStore');
const { publishIncidentEvent } = require('../publishers/incidentPublisher');

const GROUP_ID = process.env.KAFKA_GROUP_ID_INCIDENT || 'conduit-incident-group';

/**
 * ═══════════════════════════════════════════════════
 *  Incident Detection Consumer (Scalable Edition)
 * ═══════════════════════════════════════════════════
 * 
 * Scalability Fixes:
 *   1. Distributed Dedup: Uses Redis SET NX EX (Atomic).
 *   2. Multi-instance safe: No local race conditions for same tenant/type/source.
 */

const DETECTOR_MAP = {
  [TOPICS.METRICS_COMPUTED]: evaluateMetrics,
  [TOPICS.ML_PREDICTIONS]:   evaluateMLPrediction,
};

const SUBSCRIBED_TOPICS = [
  TOPICS.METRICS_COMPUTED,
  TOPICS.ML_PREDICTIONS,
];

async function startDetectionConsumer() {
  try {
    const { consumer } = await createConsumer({ groupId: GROUP_ID });

    await consumeMessages(consumer, SUBSCRIBED_TOPICS, async (value, topic) => {
      const detect = DETECTOR_MAP[topic];
      if (!detect) return;

      const candidate = detect(value);
      if (!candidate) return;

      // ─── Scalable Dedup Check ──────────────────────
      // Atomic distributed lock prevents multiple instances from creating the same incident
      const isNew = await tryAcquireDedupLock(candidate.tenantId, candidate.type, candidate.source);
      
      if (!isNew) {
        // Log at debug level in production — this is normal operation for high volume
        return;
      }

      // ─── Persist & Publish ─────────────────────────
      const incident = addIncident(candidate);
      await publishIncidentEvent(incident);

      console.log(
        `[Detection] ${incident.severity.toUpperCase()} ${incident.type} ` +
        `created for tenant=${incident.tenantId}`
      );
    });

    console.log(`[Detection Consumer] Subscribed to: ${SUBSCRIBED_TOPICS.join(', ')}`);
  } catch (err) {
    console.error('[Detection Consumer] Failed to start:', err);
    process.exit(1);
  }
}

module.exports = { startDetectionConsumer };
