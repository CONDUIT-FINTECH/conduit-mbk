const { getRedisClient, isRedisReady } = require('../infra/redis');

/**
 * Redis-backed idempotency guard.
 * 
 * Strategy: SETNX (SET if Not eXists) with TTL.
 *  - Key:   `dedup:{idempotencyKey}`
 *  - Value: eventId assigned on first accept
 *  - TTL:   24h (configurable) — auto-expires, no manual cleanup
 * 
 * Guarantees:
 *  - Exactly-once semantics for the ingestion boundary
 *  - Atomic check-and-set (single Redis round-trip)
 *  - Horizontal scale safe (all instances share the same Redis)
 *  - Graceful degradation: if Redis is down, falls back to reject (fail-closed)
 */

const DEDUP_TTL_SECONDS = parseInt(process.env.DEDUP_TTL_SECONDS || '86400', 10); // 24h
const KEY_PREFIX = 'dedup:';

/**
 * Check if an idempotency key has been seen before.
 * If not, atomically reserves it.
 * 
 * @param {string} idempotencyKey - Client-generated UUID
 * @param {string} eventId - Server-assigned event ID for this attempt
 * @returns {{ isDuplicate: boolean, existingEventId: string | null }}
 */
async function checkAndReserve(idempotencyKey, eventId) {
  if (!isRedisReady()) {
    // Fail-closed: reject requests if dedup backend is unavailable
    throw new Error('Idempotency service unavailable (Redis not ready)');
  }

  const redis = getRedisClient();
  const key = `${KEY_PREFIX}${idempotencyKey}`;

  // SET key value NX EX ttl — atomic check + set + expiry in one command
  const result = await redis.set(key, eventId, 'EX', DEDUP_TTL_SECONDS, 'NX');

  if (result === 'OK') {
    // First time seeing this key — reserved successfully
    return { isDuplicate: false, existingEventId: null };
  }

  // Key already exists — fetch the original eventId
  const existingEventId = await redis.get(key);
  return { isDuplicate: true, existingEventId };
}

/**
 * Bulk dedup check for batch ingestion.
 * Uses Redis pipeline for minimal round-trips.
 * 
 * @param {Array<{ idempotencyKey: string, eventId: string }>} entries
 * @returns {Map<string, { isDuplicate: boolean, existingEventId: string | null }>}
 */
async function checkAndReserveBatch(entries) {
  if (!isRedisReady()) {
    throw new Error('Idempotency service unavailable (Redis not ready)');
  }

  const redis = getRedisClient();
  const results = new Map();

  // Phase 1: Pipeline SET NX for all keys
  const pipeline = redis.pipeline();
  for (const { idempotencyKey, eventId } of entries) {
    pipeline.set(`${KEY_PREFIX}${idempotencyKey}`, eventId, 'EX', DEDUP_TTL_SECONDS, 'NX');
  }
  const setResults = await pipeline.exec();

  // Phase 2: For duplicates, fetch existing eventIds
  const dupPipeline = redis.pipeline();
  const dupIndices = [];
  
  for (let i = 0; i < entries.length; i++) {
    const [err, result] = setResults[i];
    if (err) {
      results.set(entries[i].idempotencyKey, { isDuplicate: false, existingEventId: null, error: err.message });
      continue;
    }

    if (result === 'OK') {
      results.set(entries[i].idempotencyKey, { isDuplicate: false, existingEventId: null });
    } else {
      dupPipeline.get(`${KEY_PREFIX}${entries[i].idempotencyKey}`);
      dupIndices.push(i);
    }
  }

  if (dupIndices.length > 0) {
    const getResults = await dupPipeline.exec();
    for (let j = 0; j < dupIndices.length; j++) {
      const idx = dupIndices[j];
      const [err, existingEventId] = getResults[j];
      results.set(entries[idx].idempotencyKey, {
        isDuplicate: true,
        existingEventId: err ? null : existingEventId,
      });
    }
  }

  return results;
}

module.exports = { checkAndReserve, checkAndReserveBatch };
