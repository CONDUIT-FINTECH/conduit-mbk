const { createProducer, publishEvent } = require('./producer');

/**
 * Singleton Kafka producer factory.
 * All services use this instead of duplicating lazy-init boilerplate.
 */
let _instance = null;

async function getSingletonProducer() {
  if (!_instance) {
    const { producer, shutdown } = await createProducer();
    _instance = { producer, shutdown };

    // Cleanup on process exit
    const graceful = async () => {
      if (_instance) {
        await _instance.shutdown();
        _instance = null;
      }
    };
    process.once('SIGTERM', graceful);
    process.once('SIGINT', graceful);
  }
  return _instance.producer;
}

/**
 * Convenience: publish a message using the singleton producer.
 * @param {string} topic - Kafka topic name (use TOPICS constants)
 * @param {string} key - Partition key (typically tenantId)
 * @param {object} value - Message payload
 */
async function publish(topic, key, value) {
  const producer = await getSingletonProducer();
  return publishEvent(producer, topic, key, value);
}

/**
 * Publishes a failed message to the Dead Letter Queue.
 * @param {string} originalTopic
 * @param {object} payload - Original message payload
 * @param {Error} error - The error that occurred
 * @param {string} dlqTopic - The DLQ topic name
 */
async function publishToDLQ(originalTopic, payload, error, dlqTopic = 'conduit.dlq') {
  const dlqMessage = {
    originalTopic,
    error: error.message,
    stack: error.stack,
    payload,
    failedAt: new Date().toISOString(),
  };

  // Key by original topic to group errors in partitions
  return publish(dlqTopic, originalTopic, dlqMessage);
}

module.exports = { getSingletonProducer, publish, publishToDLQ };
