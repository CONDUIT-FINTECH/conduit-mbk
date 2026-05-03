const { Kafka, CompressionTypes, logLevel } = require('kafkajs');

/**
 * Persistent Kafka producer with pre-connect lifecycle.
 * 
 * Design decisions for 1000+ req/sec:
 *  - Connected ONCE at boot, reused for all requests (no cold starts)
 *  - Idempotent: exactly-once semantics with KafkaJS idempotent producer
 *  - Batching: linger.ms equivalent via KafkaJS internal batching
 *  - Compression: Snappy for throughput, GZIP for size
 *  - Graceful shutdown: drain inflight messages before disconnect
 */

let _producer = null;
let _connected = false;

const KAFKA_CONFIG = {
  clientId: process.env.KAFKA_CLIENT_ID || 'conduit-ingestion',
  brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 100,
    retries: 8,
    maxRetryTime: 30000,
    factor: 2,
  },
};

const PRODUCER_CONFIG = {
  allowAutoTopicCreation: false,
  idempotent: true,              // Exactly-once semantics at the producer level
  maxInFlightRequests: 5,        // Max concurrent inflight batches (idempotent safe up to 5)
  transactionTimeout: 30000,
};

/**
 * Connects the persistent producer at boot time.
 * MUST be called during service startup, before any requests are served.
 */
async function connectProducer() {
  if (_connected) return;

  const kafka = new Kafka(KAFKA_CONFIG);
  _producer = kafka.producer(PRODUCER_CONFIG);

  // Event hooks for observability
  _producer.on('producer.connect', () => {
    _connected = true;
    console.log('[KafkaProducer] Connected (persistent, idempotent)');
  });

  _producer.on('producer.disconnect', () => {
    _connected = false;
    console.log('[KafkaProducer] Disconnected');
  });

  _producer.on('producer.network.request_timeout', (payload) => {
    console.warn(`[KafkaProducer] Request timeout: broker ${payload.broker}`);
  });

  await _producer.connect();
}

/**
 * Send a single message to a topic.
 * Returns real RecordMetadata { topicName, partition, errorCode, offset, timestamp }.
 */
async function sendOne(topic, key, value, headers = {}) {
  if (!_connected || !_producer) {
    throw new Error('Kafka producer not connected — call connectProducer() at boot');
  }

  const recordMetadata = await _producer.send({
    topic,
    compression: CompressionTypes.Snappy,
    acks: -1,                    // Wait for all ISRs (durability guarantee)
    messages: [{
      key: String(key),
      value: JSON.stringify(value),
      headers: {
        'content-type': 'application/json',
        'published-at': new Date().toISOString(),
        ...headers,
      },
    }],
  });

  // recordMetadata is [{ topicName, partition, errorCode, baseOffset, logAppendTime, logStartOffset }]
  return recordMetadata[0];
}

/**
 * Batch send: publish multiple messages in a single broker round-trip.
 * Critical for /ingest/batch endpoint throughput.
 */
async function sendBatch(topicMessages) {
  if (!_connected || !_producer) {
    throw new Error('Kafka producer not connected — call connectProducer() at boot');
  }

  return _producer.sendBatch({
    compression: CompressionTypes.Snappy,
    acks: -1,
    topicMessages,
  });
}

/**
 * Graceful shutdown: flush inflight, then disconnect.
 */
async function disconnectProducer() {
  if (_producer) {
    await _producer.disconnect();
    _producer = null;
    _connected = false;
    console.log('[KafkaProducer] Graceful shutdown complete');
  }
}

function isProducerReady() {
  return _connected;
}

module.exports = {
  connectProducer,
  disconnectProducer,
  sendOne,
  sendBatch,
  isProducerReady,
};
