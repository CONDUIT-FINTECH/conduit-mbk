const { insertBatch } = require('../infra/timescaledb');

/**
 * Write buffer that batches metric datapoints before flushing
 * to TimescaleDB in a single multi-row INSERT.
 * 
 * Tuning knobs:
 *  - FLUSH_SIZE: Max rows before forced flush (throughput)
 *  - FLUSH_INTERVAL_MS: Max time before forced flush (latency)
 */

const FLUSH_SIZE = parseInt(process.env.METRICS_FLUSH_SIZE || '100', 10);
const FLUSH_INTERVAL_MS = parseInt(process.env.METRICS_FLUSH_INTERVAL_MS || '5000', 10);

let _buffer = [];
let _flushTimer = null;
let _flushCount = 0;

/**
 * Enqueue a raw event into the write buffer.
 * Extracts metric-relevant fields and normalizes the schema.
 */
function enqueue(event) {
  const datapoint = {
    time:           event.timestamp || new Date().toISOString(),
    tenant_id:      event.tenantId  || 'default',
    event_type:     event.eventType || 'unknown',
    source:         event.source    || 'unknown',
    latency_ms:     event.payload?.latencyMs ?? null,
    is_error:       !!(event.eventType && event.eventType.startsWith('error.')),
    payload_size:   event.payload ? JSON.stringify(event.payload).length : 0,
    correlation_id: event.correlationId || null,
  };

  _buffer.push(datapoint);

  // Flush if buffer is full
  if (_buffer.length >= FLUSH_SIZE) {
    flush();
  }
}

/**
 * Flush the current buffer to TimescaleDB.
 * Called on buffer full OR timer tick — whichever comes first.
 */
async function flush() {
  if (_buffer.length === 0) return;

  // Swap buffer atomically to prevent data loss during async write
  const batch = _buffer;
  _buffer = [];

  try {
    await insertBatch(batch);
    _flushCount++;

    if (_flushCount % 100 === 0) {
      console.log(`[WriteBuffer] Flushed ${batch.length} rows (total flushes: ${_flushCount})`);
    }
  } catch (err) {
    console.error(`[WriteBuffer] Flush failed (${batch.length} rows):`, err.message);
    // Re-enqueue on failure (bounded retry — drop if buffer overflows)
    if (_buffer.length + batch.length <= FLUSH_SIZE * 10) {
      _buffer = batch.concat(_buffer);
      console.warn(`[WriteBuffer] Re-enqueued ${batch.length} rows for retry`);
    } else {
      console.error(`[WriteBuffer] Buffer overflow — dropped ${batch.length} rows`);
    }
  }
}

/**
 * Start the periodic flush timer.
 * Ensures data reaches TimescaleDB even during low-traffic periods.
 */
function startFlushTimer() {
  if (_flushTimer) return;

  _flushTimer = setInterval(() => {
    flush();
  }, FLUSH_INTERVAL_MS);

  // Don't prevent Node from exiting
  _flushTimer.unref();

  console.log(`[WriteBuffer] Timer started (size=${FLUSH_SIZE}, interval=${FLUSH_INTERVAL_MS}ms)`);
}

/**
 * Graceful shutdown: flush remaining buffer, stop timer.
 */
async function stopFlushTimer() {
  if (_flushTimer) {
    clearInterval(_flushTimer);
    _flushTimer = null;
  }

  // Final flush
  await flush();
  console.log('[WriteBuffer] Shutdown complete');
}

function getBufferSize() {
  return _buffer.length;
}

function getFlushCount() {
  return _flushCount;
}

module.exports = {
  enqueue,
  flush,
  startFlushTimer,
  stopFlushTimer,
  getBufferSize,
  getFlushCount,
};
