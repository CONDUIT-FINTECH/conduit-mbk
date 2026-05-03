const { Router } = require('express');
const { getDB } = require('../db/postgres');
const { cacheGet, cacheSet, buildCacheKey } = require('../cache/redisCache');

const router = Router();

/**
 * GET /remediations
 * Cursor pagination + cache-aside.
 */
router.get('/', async (req, res) => {
  try {
    const tenantId = req.headers['x-tenant-id'] || req.query.tenantId;
    const status   = req.query.status;
    const limit    = Math.min(parseInt(req.query.limit || '25', 10), 100);
    const cursor   = req.query.cursor;

    if (!tenantId) return res.status(400).json({ error: 'tenantId required' });

    const cacheKey = buildCacheKey('remediations', { tenantId, status, cursor, limit });
    const cached = await cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, _cache: 'hit' });

    let query = getDB()('remediations')
      .where('tenant_id', tenantId)
      .orderBy('id', 'desc')
      .limit(limit + 1);

    if (cursor) {
      const cursorId = parseInt(Buffer.from(cursor, 'base64').toString(), 10);
      if (!isNaN(cursorId)) query = query.where('id', '<', cursorId);
    }
    if (status) query = query.where('status', status);

    const rows = await query;
    const hasMore = rows.length > limit;
    const data = hasMore ? rows.slice(0, limit) : rows;
    const nextCursor = hasMore
      ? Buffer.from(String(data[data.length - 1].id)).toString('base64')
      : null;

    const result = {
      data: data.map(formatRemediation),
      pagination: { cursor: nextCursor, hasMore, limit },
    };

    await cacheSet(cacheKey, result, 30);
    res.json(result);
  } catch (err) {
    console.error('[Remediations Route] Error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

/**
 * GET /remediations/by-incident/:incidentId
 */
router.get('/by-incident/:incidentId', async (req, res) => {
  try {
    const rows = await getDB()('remediations')
      .where('incident_id', req.params.incidentId)
      .orderBy('id', 'desc');

    res.json(rows.map(formatRemediation));
  } catch (err) {
    console.error('[Remediations Route] Error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

function formatRemediation(row) {
  return {
    remediationId: row.remediation_id,
    incidentId:    row.incident_id,
    tenantId:      row.tenant_id,
    action:        row.action,
    status:        row.status,
    attempts:      row.attempts,
    error:         row.error,
    details:       row.details,
    createdAt:     row.created_at,
    completedAt:   row.completed_at,
  };
}

module.exports = router;
