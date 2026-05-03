const { sendOne, sendBatch, isProducerReady } = require('../infra/producerPool');
const { kafka } = require('@conduit/shared');
const { TOPICS } = kafka;

const TOPIC = TOPICS.EVENTS_INGESTED;

/**
 * Publish a single enriched event to Kafka.
 * Uses the persistent producer — zero connection overhead per request.
 * Returns real broker metadata (partition, offset, timestamp).
 */
async function publishEvent(event) {
  const metadata = await sendOne(
    TOPIC,
    event.tenantId,
    event,
    {
      'x-event-type': event.eventType,
      'x-correlation-id': event.correlationId || '',
    }
  );

  return {
    partition: metadata.partition,
    offset: metadata.baseOffset,
    timestamp: metadata.logAppendTime,
  };
}

/**
 * Publish a batch of enriched events in a single broker round-trip.
 * Maps all events into a single topicMessages payload.
 */
async function publishEventBatch(events) {
  const messages = events.map((event) => ({
    key: String(event.tenantId),
    value: JSON.stringify(event),
    headers: {
      'content-type': 'application/json',
      'x-event-type': event.eventType,
      'x-correlation-id': event.correlationId || '',
      'published-at': new Date().toISOString(),
    },
  }));

  const result = await sendBatch([{ topic: TOPIC, messages }]);
  return result;
}

module.exports = { publishEvent, publishEventBatch, isProducerReady };
