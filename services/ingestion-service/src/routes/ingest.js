const { Router } = require('express');
const { v4: uuidv4 } = require('uuid');
const validateEvent = require('../validators/eventValidator');
const { enrichTimestamp } = require('../enrichers/timestampEnricher');
const { publishEvent, publishEventBatch } = require('../publishers/kafkaPublisher');
const { checkAndReserve, checkAndReserveBatch } = require('../dedup/idempotencyGuard');
const { errors } = require('@conduit/shared');

const router = Router();

// ─────────────────────────────────────────────────
//  POST /ingest — Single Event Ingestion
// ─────────────────────────────────────────────────
router.post('/', async (req, res, next) => {
  try {
    // 1. Schema validation (Ajv strict — compiled once at module load)
    const validation = validateEvent(req.body);
    if (!validation.valid) {
      throw new errors.ValidationError(validation.errors);
    }

    // 2. Assign server-side event ID early (needed for idempotency value)
    const eventId = uuidv4();

    // 3. Redis SETNX idempotency check (atomic, TTL-based, cluster-safe)
    const { isDuplicate, existingEventId } = await checkAndReserve(
      req.body.idempotencyKey,
      eventId
    );

    if (isDuplicate) {
      // 409 with the original eventId so clients can reconcile
      return res.status(409).json({
        error: 'DUPLICATE_EVENT',
        message: 'Event already ingested',
        existingEventId,
        idempotencyKey: req.body.idempotencyKey,
      });
    }

    // 4. Enrich with tenant, correlation, and normalized timestamps
    const enrichedEvent = {
      eventId,
      ...enrichTimestamp(req.body),
      tenantId: req.headers['x-tenant-id'] || 'default',
      correlationId: req.correlationId,
      ingestedAt: new Date().toISOString(),
    };

    // 5. Publish to Kafka (persistent producer, zero connect overhead)
    const metadata = await publishEvent(enrichedEvent);

    // 6. ACK with real broker metadata
    res.status(202).json({
      eventId,
      status: 'accepted',
      partition: metadata.partition,
      offset: metadata.offset,
    });
  } catch (err) {
    next(err);
  }
});

// ─────────────────────────────────────────────────
//  POST /ingest/batch — Batch Ingestion (high throughput)
//  Accepts up to 500 events per request, deduplicates in bulk,
//  and publishes in a single Kafka round-trip.
// ─────────────────────────────────────────────────
const MAX_BATCH_SIZE = parseInt(process.env.MAX_BATCH_SIZE || '500', 10);

router.post('/batch', async (req, res, next) => {
  try {
    const { events } = req.body;

    // 1. Input guard
    if (!Array.isArray(events) || events.length === 0) {
      throw new errors.ValidationError([{ message: 'Request body must contain a non-empty "events" array' }]);
    }
    if (events.length > MAX_BATCH_SIZE) {
      throw new errors.ValidationError([{ message: `Batch size ${events.length} exceeds max ${MAX_BATCH_SIZE}` }]);
    }

    // 2. Validate all events (fail-fast: reject entire batch if any invalid)
    const validationErrors = [];
    for (let i = 0; i < events.length; i++) {
      const result = validateEvent(events[i]);
      if (!result.valid) {
        validationErrors.push({ index: i, errors: result.errors });
      }
    }
    if (validationErrors.length > 0) {
      throw new errors.ValidationError(validationErrors);
    }

    // 3. Assign event IDs
    const entries = events.map((event) => ({
      idempotencyKey: event.idempotencyKey,
      eventId: uuidv4(),
    }));

    // 4. Bulk Redis dedup (single pipeline round-trip)
    const dedupResults = await checkAndReserveBatch(entries);

    // 5. Split into new vs duplicate
    const newEvents = [];
    const duplicates = [];
    const tenantId = req.headers['x-tenant-id'] || 'default';

    for (let i = 0; i < events.length; i++) {
      const dedupResult = dedupResults.get(entries[i].idempotencyKey);
      if (dedupResult.isDuplicate) {
        duplicates.push({
          index: i,
          idempotencyKey: entries[i].idempotencyKey,
          existingEventId: dedupResult.existingEventId,
        });
      } else {
        newEvents.push({
          eventId: entries[i].eventId,
          ...enrichTimestamp(events[i]),
          tenantId,
          correlationId: req.correlationId,
          ingestedAt: new Date().toISOString(),
        });
      }
    }

    // 6. Bulk Kafka publish (single broker round-trip for all new events)
    let publishResult = null;
    if (newEvents.length > 0) {
      publishResult = await publishEventBatch(newEvents);
    }

    // 7. Respond with detailed breakdown
    res.status(202).json({
      status: 'accepted',
      total: events.length,
      accepted: newEvents.length,
      duplicates: duplicates.length,
      results: {
        accepted: newEvents.map((e) => ({ eventId: e.eventId })),
        duplicates,
      },
    });
  } catch (err) {
    next(err);
  }
});

module.exports = router;
