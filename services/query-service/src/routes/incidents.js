const { Router } = require('express');
const { getDB } = require('../db/postgres');
const { cacheGet, cacheSet, buildCacheKey } = require('../cache/redisCache');

const router = Router();

/**
 * GET /incidents
 *
 * Cursor pagination + cache-aside.
 * Filters: tenantId (required), status, severity
 */
router.get('/', async (req, res) => {
  try {
    const tenantId = req.headers['x-tenant-id'] || req.query.tenantId;
    const status   = req.query.status;
    const severity = req.query.severity;
    const limit    = Math.min(parseInt(req.query.limit || '25', 10), 100);
    const cursor   = req.query.cursor;

    if (!tenantId) {
      return res.status(400).json({ error: 'tenantId required' });
    }

    const cacheKey = buildCacheKey('incidents', { tenantId, status, severity, cursor, limit });
    const cached = await cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, _cache: 'hit' });

    let query = getDB()('incidents')
      .where('tenant_id', tenantId)
      .orderBy('id', 'desc')
      .limit(limit + 1);

    if (cursor) {
      const cursorId = parseInt(Buffer.from(cursor, 'base64').toString(), 10);
      if (!isNaN(cursorId)) query = query.where('id', '<', cursorId);
    }
    if (status)   query = query.where('status', status);
    if (severity) query = query.where('severity', severity);

    const rows = await query;
    const hasMore = rows.length > limit;
    const data = hasMore ? rows.slice(0, limit) : rows;
    const nextCursor = hasMore
      ? Buffer.from(String(data[data.length - 1].id)).toString('base64')
      : null;

    const result = {
      data: data.map(formatIncident),
      pagination: { cursor: nextCursor, hasMore, limit },
    };

    await cacheSet(cacheKey, result, 30);
    res.json(result);
  } catch (err) {
    console.error('[Incidents Route] Error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

/**
 * GET /incidents/counts
 * Dashboard: count by status + severity.
 */
router.get('/counts', async (req, res) => {
  try {
    const tenantId = req.headers['x-tenant-id'] || req.query.tenantId;
    if (!tenantId) return res.status(400).json({ error: 'tenantId required' });

    const cacheKey = buildCacheKey('incidents-counts', { tenantId });
    const cached = await cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, _cache: 'hit' });

    const byStatus = await getDB()('incidents')
      .where('tenant_id', tenantId)
      .select('status')
      .count('* as count')
      .groupBy('status');

    const bySeverity = await getDB()('incidents')
      .where('tenant_id', tenantId)
      .select('severity')
      .count('* as count')
      .groupBy('severity');

    const result = {
      byStatus:   Object.fromEntries(byStatus.map(r => [r.status, parseInt(r.count, 10)])),
      bySeverity: Object.fromEntries(bySeverity.map(r => [r.severity, parseInt(r.count, 10)])),
    };

    await cacheSet(cacheKey, result, 15);  // shorter TTL for dashboard
    res.json(result);
  } catch (err) {
    console.error('[Incidents Counts] Error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

/**
 * GET /incidents/:id
 */
router.get('/:id', async (req, res) => {
  try {
    const row = await getDB()('incidents')
      .where('incident_id', req.params.id)
      .first();

    if (!row) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(formatIncident(row));
  } catch (err) {
    console.error('[Incidents Route] Error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

function formatIncident(row) {
  return {
    incidentId:  row.incident_id,
    tenantId:    row.tenant_id,
    type:        row.type,
    severity:    row.severity,
    status:      row.status,
    description: row.description,
    source:      row.source,
    detectedAt:  row.detected_at,
    resolvedAt:  row.resolved_at,
    durationMs:  row.duration_ms,
  };
}

module.exports = router;
