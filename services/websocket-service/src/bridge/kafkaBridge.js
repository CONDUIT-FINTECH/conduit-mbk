const { kafka } = require('@conduit/shared');
const { createConsumer, consumeMessages, TOPICS } = kafka;
const { publishToChannel } = require('../infra/redisPubSub');

// ─── Topic → Channel Mapping ───────────────────
const TOPIC_CHANNEL_MAP = {
  [TOPICS.EVENTS_INGESTED]:  'events:live',
  [TOPICS.METRICS_COMPUTED]: 'metrics:dashboard',
  [TOPICS.INCIDENTS]:        'incidents:alerts',
  [TOPICS.REMEDIATIONS]:     'remediations:actions',
  [TOPICS.ML_PREDICTIONS]:   'ml:predictions',
};

const SUBSCRIBED_TOPICS = Object.keys(TOPIC_CHANNEL_MAP);
const GROUP_ID = process.env.KAFKA_GROUP_ID_WS || 'conduit-websocket-group';

// ─── Latency Tracking ──────────────────────────
let _messageCount = 0;
let _totalLatencyMs = 0;

function getLatencyStats() {
  return {
    messagesProcessed: _messageCount,
    avgBridgeLatencyMs: _messageCount > 0
      ? Math.round((_totalLatencyMs / _messageCount) * 100) / 100
      : 0,
  };
}

/**
 * ═══════════════════════════════════════════════════
 *  Kafka → Redis Pub/Sub Bridge
 * ═══════════════════════════════════════════════════
 * 
 * Flow:
 *   Kafka Consumer → Build envelope → Redis PUBLISH → All WS instances
 *
 * This decouples Kafka partition assignment from WebSocket broadcast.
 * Kafka consumer runs on ONE instance; Redis fans out to ALL instances.
 */
async function startKafkaBridge() {
  try {
    const { consumer } = await createConsumer({ groupId: GROUP_ID });

    await consumeMessages(consumer, SUBSCRIBED_TOPICS, async (value, topic) => {
      const channel = TOPIC_CHANNEL_MAP[topic];
      if (!channel) return;

      const bridgeTimestamp = Date.now();

      // Build the standardized event envelope
      const envelope = {
        channel,
        eventType: deriveEventType(topic, value),
        tenantId: value.tenantId || null,
        data: value,
        meta: {
          kafkaTopic: topic,
          bridgedAt: new Date(bridgeTimestamp).toISOString(),
          sourceTimestamp: value.computedAt || value.detectedAt || value.publishedAt || null,
        },
      };

      // Publish to Redis for fan-out to all WS instances
      await publishToChannel(channel, envelope);

      // Track latency
      const sourceTs = value.computedAt || value.detectedAt || value.publishedAt;
      if (sourceTs) {
        const latency = bridgeTimestamp - new Date(sourceTs).getTime();
        if (latency >= 0 && latency < 60000) { // sanity check
          _totalLatencyMs += latency;
          _messageCount++;
        }
      }
    });

    console.log(`[KafkaBridge] Subscribed to ${SUBSCRIBED_TOPICS.length} topics`);
    console.log(`[KafkaBridge] Publishing via Redis Pub/Sub to all instances`);
  } catch (err) {
    console.error('[KafkaBridge] Failed to start:', err);
    process.exit(1);
  }
}

/**
 * Derive a human-readable event type from the Kafka topic and payload.
 */
function deriveEventType(topic, value) {
  // Incident events carry their own eventType
  if (value.eventType) return value.eventType;

  // Derive from topic
  const topicShort = topic.replace('conduit.', '');
  return topicShort;
}

module.exports = { startKafkaBridge, getLatencyStats };
