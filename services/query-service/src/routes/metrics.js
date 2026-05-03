const { Router } = require('express');
const { cacheGet, buildCacheKey } = require('../cache/redisCache');

const router = Router();

/**
 * GET /metrics/summary
 *
 * Returns the latest computed metrics snapshot for a tenant.
 * Metrics are ALWAYS served from Redis cache (hot-path).
 * There is no DB fallback — metrics are ephemeral by design.
 * If the cache is empty, the metrics haven't been computed yet.
 */
router.get('/summary', async (req, res) => {
  try {
    const tenantId = req.headers['x-tenant-id'] || req.query.tenantId || 'default';
    const cacheKey = buildCacheKey('metrics', { tenantId });

    const snapshot = await cacheGet(cacheKey);
    if (!snapshot) {
      return res.status(404).json({
        error: 'NOT_FOUND',
        message: `No metrics snapshot for tenant: ${tenantId}. Awaiting first metrics.computed event.`,
      });
    }

    res.json({ ...snapshot, _cache: 'hit' });
  } catch (err) {
    console.error('[Metrics Route] Error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

module.exports = router;
