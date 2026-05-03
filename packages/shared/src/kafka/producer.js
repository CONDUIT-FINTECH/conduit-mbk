const { Kafka } = require('kafkajs');

/**
 * Creates a reusable Kafka producer with connection management.
 */
async function createProducer(config = {}) {
  const kafka = new Kafka({
    clientId: config.clientId || process.env.KAFKA_CLIENT_ID || 'conduit',
    brokers: (config.brokers || process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  });

  const producer = kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: true,
  });

  await producer.connect();
  console.log('[Kafka Producer] Connected');

  // Graceful shutdown
  const shutdown = async () => {
    await producer.disconnect();
    console.log('[Kafka Producer] Disconnected');
  };

  return { producer, shutdown };
}

/**
 * Publish a message to a Kafka topic.
 */
async function publishEvent(producer, topic, key, value) {
  await producer.send({
    topic,
    messages: [
      {
        key,
        value: JSON.stringify(value),
        headers: {
          'content-type': 'application/json',
          'published-at': new Date().toISOString(),
        },
      },
    ],
  });
}

module.exports = { createProducer, publishEvent };
