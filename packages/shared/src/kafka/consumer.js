const { Kafka } = require('kafkajs');

/**
 * Creates a reusable Kafka consumer with auto-commit and error handling.
 */
async function createConsumer(config = {}) {
  const kafka = new Kafka({
    clientId: config.clientId || process.env.KAFKA_CLIENT_ID || 'conduit',
    brokers: (config.brokers || process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  });

  const consumer = kafka.consumer({
    groupId: config.groupId,
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
  });

  await consumer.connect();
  console.log(`[Kafka Consumer:${config.groupId}] Connected`);

  // Graceful shutdown
  const shutdown = async () => {
    await consumer.disconnect();
    console.log(`[Kafka Consumer:${config.groupId}] Disconnected`);
  };

  return { consumer, shutdown };
}

const { publishToDLQ } = require('./singletonProducer');
const TOPICS = require('./topics');

/**
 * Subscribe and run a handler for each message.
 * @param {object} consumer - KafkaJS consumer instance
 * @param {string|string[]} topics - Topic(s) to subscribe to
 * @param {function} handler - async (message, topic, partition) => void
 */
async function consumeMessages(consumer, topics, handler) {
  const topicsArray = Array.isArray(topics) ? topics : [topics];
  
  for (const t of topicsArray) {
    await consumer.subscribe({ topic: t, fromBeginning: false });
  }

  await consumer.run({
    eachMessage: async ({ topic: msgTopic, partition, message }) => {
      let value;
      try {
        value = JSON.parse(message.value.toString());
        await handler(value, msgTopic, partition);
      } catch (err) {
        console.error(`[Kafka Consumer] Error processing message on ${msgTopic}:`, err.message);
        
        // Publish to DLQ
        try {
          await publishToDLQ(msgTopic, value || message.value.toString(), err, TOPICS.DLQ);
          console.log(`[Kafka Consumer] Message routed to DLQ: ${TOPICS.DLQ}`);
        } catch (dlqErr) {
          console.error(`[Kafka Consumer] FATAL: Failed to publish to DLQ:`, dlqErr.message);
        }
      }
    },
  });
}

module.exports = { createConsumer, consumeMessages };
