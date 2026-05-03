const { kafka } = require('@conduit/shared');
const { createConsumer, consumeMessages, TOPICS } = kafka;
const { getDB } = require('../db/postgres');
const { Prediction } = require('../db/mongo');
const { cacheSet, cacheInvalidate, buildCacheKey } = require('../cache/redisCache');

/**
 * ═══════════════════════════════════════════════════
 *  Materializer — Kafka → Read Stores
 * ═══════════════════════════════════════════════════
 *
 * Consumes ALL write-path topics and materializes them
 * into read-optimized stores:
 *
 *   events.ingested   → PostgreSQL (events table)
 *   incidents.events   → PostgreSQL (incidents table)  + cache invalidation
 *   remediations       → PostgreSQL (remediations table) + cache invalidation
 *   metrics.computed   → Redis only (hot cache, latest-wins)
 *   ml.predictions     → MongoDB (predictions collection)
 *
 * Batch buffering for high-throughput inserts.
 */

const SUBSCRIBED_TOPICS = [
  TOPICS.EVENTS_INGESTED,
  TOPICS.METRICS_COMPUTED,
  TOPICS.INCIDENTS,
  TOPICS.REMEDIATIONS,
  TOPICS.ML_PREDICTIONS,
];

// ─── Batch Buffer (events) ──────────────────────
const BATCH_SIZE = parseInt(process.env.QUERY_BATCH_SIZE || '50', 10);
const FLUSH_INTERVAL_MS = parseInt(process.env.QUERY_FLUSH_INTERVAL_MS || '2000', 10);

let _eventBuffer = [];
let _flushTimer = null;
let _messageCount = 0;

async function startMaterializer() {
  try {
    const { consumer } = await createConsumer({
      groupId: process.env.KAFKA_GROUP_ID_QUERY || 'conduit-query-group',
    });

    // Start batch flush timer for events
    _flushTimer = setInterval(() => flushEventBuffer(), FLUSH_INTERVAL_MS);

    await consumeMessages(consumer, SUBSCRIBED_TOPICS, async (value, topic) => {
      _messageCount++;

      switch (topic) {
        case TOPICS.EVENTS_INGESTED:
          await materializeEvent(value);
          break;

        case TOPICS.METRICS_COMPUTED:
          await materializeMetrics(value);
          break;

        case TOPICS.INCIDENTS:
          await materializeIncident(value);
          break;

        case TOPICS.REMEDIATIONS:
          await materializeRemediation(value);
          break;

        case TOPICS.ML_PREDICTIONS:
          await materializePrediction(value);
          break;
      }
    });

    console.log(`[Materializer] Subscribed to ${SUBSCRIBED_TOPICS.length} topics`);
  } catch (err) {
    console.error('[Materializer] Failed to start:', err);
    process.exit(1);
  }
}

// ─── Events → PostgreSQL (batched) ──────────────
async function materializeEvent(event) {
  _eventBuffer.push({
    event_id:    event.eventId,
    tenant_id:   event.tenantId,
    event_type:  event.eventType,
    source:      event.source || null,
    payload:     JSON.stringify(event),
    ingested_at: event.ingestedAt || new Date().toISOString(),
  });

  if (_eventBuffer.length >= BATCH_SIZE) {
    await flushEventBuffer();
  }
}

async function flushEventBuffer() {
  if (_eventBuffer.length === 0) return;

  const batch = _eventBuffer.splice(0);
  try {
    await getDB()('events')
      .insert(batch)
      .onConflict('event_id')
      .ignore();
  } catch (err) {
    console.error('[Materializer] Event batch insert error:', err.message);
  }
}

// ─── Metrics → Redis Cache (latest-wins) ────────
async function materializeMetrics(metrics) {
  const key = buildCacheKey('metrics', { tenantId: metrics.tenantId });
  // Metrics are hot-path: always in cache, no DB write
  // TTL of 5 minutes — will be refreshed by next metrics.computed event
  await cacheSet(key, metrics, 300);
}

// ─── Incidents → PostgreSQL + cache invalidation ─
async function materializeIncident(incident) {
  try {
    const row = {
      incident_id: incident.incidentId,
      tenant_id:   incident.tenantId,
      type:        incident.type,
      severity:    incident.severity,
      status:      incident.status || 'detected',
      description: incident.description,
      source:      incident.source,
      detected_at: incident.detectedAt,
      resolved_at: incident.resolvedAt || null,
      duration_ms: incident.durationMs || null,
    };

    await getDB()('incidents')
      .insert(row)
      .onConflict('incident_id')
      .merge(['status', 'resolved_at', 'duration_ms']);

    // Invalidate cached incident queries for this tenant
    await cacheInvalidate(`conduit:query:incidents:*tenantId=${incident.tenantId}*`);
  } catch (err) {
    console.error('[Materializer] Incident upsert error:', err.message);
  }
}

// ─── Remediations → PostgreSQL + cache invalidation
async function materializeRemediation(remediation) {
  try {
    const row = {
      remediation_id: remediation.remediationId,
      incident_id:    remediation.incidentId,
      tenant_id:      remediation.tenantId,
      action:         remediation.action,
      status:         remediation.status,
      attempts:       remediation.attempts || 0,
      error:          remediation.error || null,
      details:        remediation.details,
      completed_at:   remediation.completedAt || null,
    };

    await getDB()('remediations')
      .insert(row)
      .onConflict('remediation_id')
      .merge(['status', 'attempts', 'error', 'completed_at']);

    await cacheInvalidate(`conduit:query:remediations:*tenantId=${remediation.tenantId}*`);
  } catch (err) {
    console.error('[Materializer] Remediation upsert error:', err.message);
  }
}

// ─── ML Predictions → MongoDB ───────────────────
async function materializePrediction(prediction) {
  try {
    await Prediction.findOneAndUpdate(
      { predictionId: prediction.predictionId },
      { $set: prediction },
      { upsert: true }
    );
  } catch (err) {
    console.error('[Materializer] Prediction upsert error:', err.message);
  }
}

function getStats() {
  return { messagesProcessed: _messageCount, eventBufferSize: _eventBuffer.length };
}

function stopFlushTimer() {
  if (_flushTimer) clearInterval(_flushTimer);
}

module.exports = { startMaterializer, getStats, stopFlushTimer, flushEventBuffer };
