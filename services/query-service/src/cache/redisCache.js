const Redis = require('ioredis');

/**
 * ═══════════════════════════════════════════════════
 *  Redis Cache Layer — Cache-Aside Pattern
 * ═══════════════════════════════════════════════════
 *
 * Data Boundary (CQRS Read Side):
 * 
 * ┌─────────────────────┬───────────────────────────────────┐
 * │  Store              │  What Lives Here                  │
 * ├─────────────────────┼───────────────────────────────────┤
 * │  PostgreSQL (Knex)  │  Structured relational data:      │
 * │                     │  - Events (write-once append log) │
 * │                     │  - Incidents (lifecycle + MTTR)   │
 * │                     │  - Remediations (state machine)   │
 * │                     │  → Cursor pagination via PK/ts    │
 * ├─────────────────────┼───────────────────────────────────┤
 * │  MongoDB (Mongoose) │  Flexible schema / analytics:     │
 * │                     │  - ML Predictions (nested JSON)   │
 * │                     │  - Audit trail (polymorphic docs) │
 * │                     │  → Schema-less for ML payloads    │
 * ├─────────────────────┼───────────────────────────────────┤
 * │  Redis              │  Hot-path cache:                  │
 * │                     │  - Latest metrics per tenant      │
 * │                     │  - Recent query results (60s TTL) │
 * │                     │  - Dashboard aggregates           │
 * │                     │  → Sub-200ms reads                │
 * └─────────────────────┴───────────────────────────────────┘
 *
 * Cache-Aside Flow:
 *   1. Read from Redis
 *   2. Cache HIT  → return cached data
 *   3. Cache MISS → read from DB → write to Redis → return
 */

const DEFAULT_TTL = parseInt(process.env.CACHE_TTL_SECONDS || '60', 10);

let _redis = null;

function getRedisClient() {
  if (_redis) return _redis;
  _redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: 3,
    lazyConnect: true,
  });
  _redis.on('error', (err) => console.error('[Cache] Redis error:', err.message));
  return _redis;
}

/**
 * Cache-aside GET.
 * @param {string} key
 * @returns {object|null} Parsed value, or null on miss
 */
async function cacheGet(key) {
  try {
    const raw = await getRedisClient().get(key);
    if (raw) return JSON.parse(raw);
  } catch (err) {
    console.error('[Cache] GET error:', err.message);
  }
  return null;
}

/**
 * Cache-aside SET with TTL.
 * @param {string} key
 * @param {object} value
 * @param {number} ttl - Seconds (default 60)
 */
async function cacheSet(key, value, ttl = DEFAULT_TTL) {
  try {
    await getRedisClient().set(key, JSON.stringify(value), 'EX', ttl);
  } catch (err) {
    console.error('[Cache] SET error:', err.message);
  }
}

/**
 * Invalidate cache by key pattern (e.g., on write).
 * Uses SCAN instead of KEYS to prevent blocking the Redis event loop.
 */
async function cacheInvalidate(pattern) {
  try {
    const redis = getRedisClient();
    let cursor = '0';
    let deletedCount = 0;

    do {
      const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
      cursor = nextCursor;

      if (keys.length > 0) {
        await redis.del(...keys);
        deletedCount += keys.length;
      }
    } while (cursor !== '0');

    if (deletedCount > 0) {
      console.log(`[Cache] Invalidated ${deletedCount} keys for pattern: ${pattern}`);
    }
  } catch (err) {
    console.error('[Cache] INVALIDATE error:', err.message);
  }
}

/**
 * Build a deterministic cache key.
 */
function buildCacheKey(domain, params = {}) {
  const parts = Object.entries(params)
    .filter(([, v]) => v != null)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join(':');
  return `conduit:query:${domain}:${parts || 'all'}`;
}

async function connectCache() {
  await getRedisClient().connect();
  console.log('[Cache] Redis connected');
}

async function disconnectCache() {
  if (_redis) {
    _redis.disconnect();
    _redis = null;
  }
}

module.exports = {
  cacheGet,
  cacheSet,
  cacheInvalidate,
  buildCacheKey,
  connectCache,
  disconnectCache,
  DEFAULT_TTL,
};
