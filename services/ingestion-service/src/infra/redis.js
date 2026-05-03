const Redis = require('ioredis');

/**
 * Managed Redis client with connection lifecycle.
 * Used for idempotency dedup and optional rate limiting.
 */

let _client = null;
let _isReady = false;

function getRedisClient() {
  if (_client) return _client;

  _client = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: 3,
    retryStrategy(times) {
      const delay = Math.min(times * 200, 5000);
      console.log(`[Redis] Reconnecting in ${delay}ms (attempt ${times})`);
      return delay;
    },
    lazyConnect: false,
    enableReadyCheck: true,
    connectTimeout: 5000,
  });

  _client.on('connect', () => console.log('[Redis] Connected'));
  _client.on('ready', () => { _isReady = true; console.log('[Redis] Ready'); });
  _client.on('error', (err) => console.error('[Redis] Error:', err.message));
  _client.on('close', () => { _isReady = false; console.log('[Redis] Connection closed'); });

  return _client;
}

function isRedisReady() {
  return _isReady;
}

async function shutdownRedis() {
  if (_client) {
    await _client.quit();
    _client = null;
    _isReady = false;
    console.log('[Redis] Disconnected');
  }
}

module.exports = { getRedisClient, isRedisReady, shutdownRedis };
