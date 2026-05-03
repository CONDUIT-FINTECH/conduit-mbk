const { subscribeToChannels } = require('../infra/redisPubSub');

/**
 * ═══════════════════════════════════════════════════
 *  Subscription Engine — Redis Pub/Sub → WebSocket
 * ═══════════════════════════════════════════════════
 * 
 * Receives messages from Redis Pub/Sub (fan-out from any WS instance)
 * and broadcasts to locally-connected WebSocket clients.
 *
 * This is the key component for horizontal scaling:
 *   Kafka Bridge (1 instance) → Redis Pub/Sub → SubscriptionEngine (N instances)
 */

const CHANNELS = [
  'events:live',
  'metrics:dashboard',
  'incidents:alerts',
  'remediations:actions',
  'ml:predictions',
];

class SubscriptionEngine {
  constructor(connectionManager) {
    this.connManager = connectionManager;
    this._messageCount = 0;
  }

  /**
   * Start subscribing to Redis channels.
   * Called once during boot.
   */
  async start() {
    await subscribeToChannels(CHANNELS, (channel, envelope) => {
      this._onRedisMessage(channel, envelope);
    });

    console.log(`[SubEngine] Listening on ${CHANNELS.length} channels via Redis`);
  }

  /**
   * Called when a message arrives from Redis Pub/Sub.
   * The envelope has been standardized by the Kafka bridge.
   */
  _onRedisMessage(channel, envelope) {
    const { tenantId, data, meta } = envelope;

    // Build the final client-facing message
    const clientMessage = {
      type: 'event',
      channel,
      eventType: envelope.eventType,
      data,
      timestamp: new Date().toISOString(),
      latency: meta?.bridgedAt
        ? Date.now() - new Date(meta.bridgedAt).getTime()
        : null,
    };

    const sent = this.connManager.broadcast(channel, clientMessage, tenantId);

    this._messageCount++;

    // Log periodically (every 500 messages) to avoid noise
    if (this._messageCount % 500 === 0) {
      console.log(`[SubEngine] ${this._messageCount} messages broadcast total`);
    }
  }

  getStats() {
    return { totalBroadcasts: this._messageCount };
  }
}

module.exports = { SubscriptionEngine, CHANNELS };
