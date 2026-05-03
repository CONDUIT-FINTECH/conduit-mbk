const { Router } = require('express');
const { Prediction } = require('../db/mongo');
const { cacheGet, cacheSet, buildCacheKey } = require('../cache/redisCache');

const router = Router();

/**
 * GET /predictions
 * Cursor pagination via MongoDB _id.
 * Cache-aside: 60s TTL.
 */
router.get('/', async (req, res) => {
  try {
    const tenantId = req.headers['x-tenant-id'] || req.query.tenantId;
    const limit    = Math.min(parseInt(req.query.limit || '25', 10), 100);
    const cursor   = req.query.cursor;

    if (!tenantId) return res.status(400).json({ error: 'tenantId required' });

    const cacheKey = buildCacheKey('predictions', { tenantId, cursor, limit });
    const cached = await cacheGet(cacheKey);
    if (cached) return res.json({ ...cached, _cache: 'hit' });

    const filter = { tenantId };
    if (cursor) {
      filter._id = { $lt: cursor };
    }

    const docs = await Prediction.find(filter)
      .sort({ _id: -1 })
      .limit(limit + 1)
      .lean();

    const hasMore = docs.length > limit;
    const data = hasMore ? docs.slice(0, limit) : docs;
    const nextCursor = hasMore ? String(data[data.length - 1]._id) : null;

    const result = {
      data: data.map(formatPrediction),
      pagination: { cursor: nextCursor, hasMore, limit },
    };

    await cacheSet(cacheKey, result, 60);
    res.json(result);
  } catch (err) {
    console.error('[Predictions Route] Error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

/**
 * GET /predictions/latest
 * Latest prediction for a tenant.
 */
router.get('/latest', async (req, res) => {
  try {
    const tenantId = req.headers['x-tenant-id'] || req.query.tenantId;
    if (!tenantId) return res.status(400).json({ error: 'tenantId required' });

    const doc = await Prediction.findOne({ tenantId })
      .sort({ createdAt: -1 })
      .lean();

    if (!doc) return res.status(404).json({ error: 'NOT_FOUND' });
    res.json(formatPrediction(doc));
  } catch (err) {
    console.error('[Predictions Route] Error:', err.message);
    res.status(500).json({ error: 'Internal error' });
  }
});

function formatPrediction(doc) {
  return {
    predictionId: doc.predictionId,
    tenantId:     doc.tenantId,
    modelId:      doc.modelId,
    anomalyScore: doc.anomalyScore,
    confidence:   doc.confidence,
    labels:       doc.labels,
    features:     doc.features,
    metadata:     doc.metadata,
    createdAt:    doc.createdAt,
  };
}

module.exports = router;
