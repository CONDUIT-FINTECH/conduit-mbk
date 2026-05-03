const { kafka } = require('@conduit/shared');
const { createConsumer, consumeMessages, publish, TOPICS } = kafka;
const { addEvent, computeSnapshot } = require('../aggregators/slidingWindow');
const { enqueue } = require('../pipeline/writeBuffer');

const GROUP_ID = process.env.KAFKA_GROUP_ID_METRICS || 'conduit-metrics-group';

/**
 * Metrics Event Consumer
 * 
 * Dual-write pipeline:
 *   1. In-memory sliding window → real-time Kafka snapshots (hot path)
 *   2. Write buffer → TimescaleDB batch INSERT (persistent path)
 * 
 * Both paths are non-blocking and operate on the same event stream.
 */
async function startConsumer() {
  const { consumer } = await createConsumer({ groupId: GROUP_ID });

  await consumeMessages(consumer, TOPICS.EVENTS_INGESTED, async (event) => {
    // ── Hot Path: In-memory aggregation ───────────
    addEvent(event);

    const snapshot = computeSnapshot(event.tenantId);
    if (snapshot) {
      await publish(TOPICS.METRICS_COMPUTED, event.tenantId, snapshot);
    }

    // ── Persistent Path: Buffer → TimescaleDB ────
    enqueue(event);
  });

  console.log(`[Metrics Consumer] Subscribed to ${TOPICS.EVENTS_INGESTED}`);
  console.log(`[Metrics Consumer] Hot path  → ${TOPICS.METRICS_COMPUTED} (Kafka)`);
  console.log(`[Metrics Consumer] Cold path → TimescaleDB (buffered writes)`);
}

module.exports = { startConsumer };
