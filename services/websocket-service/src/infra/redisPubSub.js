const Redis = require('ioredis');

/**
 * ═══════════════════════════════════════════════════
 *  Redis Pub/Sub Adapter — Horizontal Scaling
 * ═══════════════════════════════════════════════════
 * 
 * Problem: Kafka consumer partitions are assigned to ONE instance.
 *   If WS instance-1 receives a Kafka message, instance-2 clients
 *   won't see it — broadcasts are local.
 *
 * Solution: After consuming from Kafka, publish to Redis Pub/Sub.
 *   ALL WS instances subscribe to the same Redis channels,
 *   so every instance broadcasts to its local clients.
 *
 *   Kafka → Instance-1 → Redis Pub/Sub → [Instance-1, Instance-2, Instance-3]
 *                                              ↓           ↓           ↓
 *                                          Broadcast   Broadcast   Broadcast
 */

const REDIS_CHANNEL_PREFIX = 'conduit:ws:';

let _pub = null;
let _sub = null;

function createPubClient() {
  if (_pub) return _pub;
  _pub = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: 3,
    lazyConnect: true,
  });
  _pub.on('error', (err) => console.error('[Redis Pub] Error:', err.message));
  return _pub;
}

function createSubClient() {
  if (_sub) return _sub;
  _sub = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: 3,
    lazyConnect: true,
  });
  _sub.on('error', (err) => console.error('[Redis Sub] Error:', err.message));
  return _sub;
}

/**
 * Publish a message to a Redis channel.
 * Called by the Kafka bridge after consuming a message.
 */
async function publishToChannel(channel, message) {
  const pub = createPubClient();
  const redisChannel = `${REDIS_CHANNEL_PREFIX}${channel}`;
  await pub.publish(redisChannel, JSON.stringify(message));
}

/**
 * Subscribe to Redis channels and invoke the callback when a message arrives.
 * Called at boot by each WS instance.
 *
 * @param {string[]} channels - WebSocket channels to subscribe to
 * @param {function} onMessage - (channel, message) => void
 */
async function subscribeToChannels(channels, onMessage) {
  const sub = createSubClient();
  await sub.connect();

  const redisChannels = channels.map(ch => `${REDIS_CHANNEL_PREFIX}${ch}`);
  await sub.subscribe(...redisChannels);

  sub.on('message', (redisChannel, raw) => {
    const channel = redisChannel.replace(REDIS_CHANNEL_PREFIX, '');
    try {
      const message = JSON.parse(raw);
      onMessage(channel, message);
    } catch (err) {
      console.error(`[Redis Sub] Parse error on ${channel}:`, err.message);
    }
  });

  console.log(`[Redis Pub/Sub] Subscribed to ${redisChannels.length} channels`);
}

async function shutdown() {
  if (_pub) { _pub.disconnect(); _pub = null; }
  if (_sub) { _sub.disconnect(); _sub = null; }
  console.log('[Redis Pub/Sub] Disconnected');
}

module.exports = { publishToChannel, subscribeToChannels, shutdown, createPubClient };
