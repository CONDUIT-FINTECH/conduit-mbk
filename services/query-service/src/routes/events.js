const { Router } = require('express');
const { getDB } = require('../db/postgres');
const { cacheGet, cacheSet, buildCacheKey } = require('../cache/redisCache');

const router = Router();

/**
 * GET /events
 *
 * Cursor pagination using the auto-increment PK.
 * Cache-aside: cached for 30s per unique query signature.
 *
 * Query params:
 *   tenantId   (required via header or query)
 *   eventType  (optional filter)
 *   source     (optional filter)
 *   cursor     (opaque, base64-encoded PK)
 *   limit      (default 25, max 100)
 */
router.get('/', async (req, res) => {
  try {
    const tenantId  = req.headers['x-tenant-id'] || req.query.tenantId;
    const eventType = req.query.eventType;
    const source    = req.query.source;
    const limit     = Math.min(parseInt(req.query.limit || '25', 10), 100);
    const cursor    = req.query.cursor;

    if (!tenantId) {
      return res.status(400).json({ error: 'tenantId required (header or query)' });
    }

    // ─── Cache Check ──────────────────────────────
    const cacheKey = buildCacheKey('events', { tenantId, eventType, source, cursor, limit });
    const cached = await cacheGet(cacheKey);
    if (cached) {
      return res.json({ ...cached, _cache: 'hit' });
    }

    // ─── DB Query ─────────────────────────────────
    let query = getDB()('events')
      .where('tenant_id', tenantId)
      .orderBy('id', 'desc')
      .limit(limit + 1);  // +1 to detect hasMore

    // Decode cursor (base64 of PK id)
    if (cursor) {
      const cursorId = parseInt(Buffer.from(cursor, 'base64').toString(), 10);
      if (!isNaN(cursorId)) {
        query = query.where('id', '<', cursorId);
      }
    }

    if (eventType) query = query.where('event_type', eventType);
    if (source)    query = query.where('source', source);

    const rows = await query;

    // Determine pagination
    const hasMore = rows.length > limit;
    const data = hasMore ? rows.slice(0, limit) : rows;
    const nextCursor = hasMore
      ? Buffer.from(String(data[data.length - 1].id)).toString('base64')
      : null;

    const result = {
      data: data.map(formatEvent),
      pagination: {
        cursor: nextCursor,
        hasMore,
        limit,
      },
    };

    // Cache for 30s
    await cacheSet(cacheKey, result, 30);

    res.json(result);
  } catch (err) {
    console.error('[Events Route] Error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

function formatEvent(row) {
  return {
    eventId:   row.event_id,
    tenantId:  row.tenant_id,
    eventType: row.event_type,
    source:    row.source,
    payload:   typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload,
    ingestedAt: row.ingested_at,
  };
}

module.exports = router;
