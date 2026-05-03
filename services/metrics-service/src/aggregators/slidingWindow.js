/**
 * ═══════════════════════════════════════════════════
 *  In-Memory Sliding Window Aggregator
 * ═══════════════════════════════════════════════════
 * 
 * Purpose: Real-time aggregation for snapshot publishing to Kafka.
 * TimescaleDB handles persistent, query-able aggregation — this module
 * provides fast, hot-path stats that are pushed to downstream consumers.
 * 
 * Computes per tenant:
 *   - avg latency
 *   - p95 latency (via sorted insertion)
 *   - success rate
 *   - throughput
 *   - error count
 */

const WINDOW_DURATION_MS = parseInt(process.env.METRICS_WINDOW_MS || '60000', 10);   // 1 minute
const SNAPSHOT_INTERVAL  = parseInt(process.env.METRICS_SNAPSHOT_INTERVAL || '25', 10); // Emit every N events

// tenantId → { events[], meta }
const windows = new Map();

/**
 * Add an event to the tenant's sliding window.
 */
function addEvent(event) {
  const { tenantId, eventType, timestampEpochMs } = event;
  const latencyMs = event.payload?.latencyMs ?? null;
  const isError = !!(eventType && eventType.startsWith('error.'));
  const now = Date.now();

  if (!windows.has(tenantId)) {
    windows.set(tenantId, {
      events: [],
      count: 0,
    });
  }

  const win = windows.get(tenantId);

  // Push event data
  win.events.push({
    ts: timestampEpochMs || now,
    latencyMs,
    isError,
  });

  win.count++;

  // Evict stale events outside the window
  const cutoff = now - WINDOW_DURATION_MS;
  while (win.events.length > 0 && win.events[0].ts < cutoff) {
    win.events.shift();
  }
}

/**
 * Compute a metric snapshot for a tenant.
 * Returns snapshot only if it's time to emit, null otherwise.
 */
function computeSnapshot(tenantId) {
  const win = windows.get(tenantId);
  if (!win || win.events.length === 0) return null;

  // Only emit every SNAPSHOT_INTERVAL events
  if (win.count % SNAPSHOT_INTERVAL !== 0) return null;

  const events = win.events;
  const total = events.length;

  // ─── Latency aggregation ────────────────────
  const latencies = events
    .map(e => e.latencyMs)
    .filter(l => l !== null && l !== undefined)
    .sort((a, b) => a - b);

  const latencyCount = latencies.length;

  let avgLatencyMs = 0;
  let p50LatencyMs = 0;
  let p95LatencyMs = 0;
  let p99LatencyMs = 0;
  let maxLatencyMs = 0;

  if (latencyCount > 0) {
    const sum = latencies.reduce((acc, v) => acc + v, 0);
    avgLatencyMs = Math.round((sum / latencyCount) * 100) / 100;
    p50LatencyMs = percentile(latencies, 0.50);
    p95LatencyMs = percentile(latencies, 0.95);
    p99LatencyMs = percentile(latencies, 0.99);
    maxLatencyMs = latencies[latencyCount - 1];
  }

  // ─── Error / success rates ──────────────────
  const errorCount   = events.filter(e => e.isError).length;
  const successCount = total - errorCount;
  const successRate  = total > 0 ? Math.round((successCount / total) * 10000) / 10000 : 1;
  const errorRate    = total > 0 ? Math.round((errorCount / total) * 10000) / 10000 : 0;

  return {
    tenantId,
    window: `${WINDOW_DURATION_MS / 1000}s`,
    sampleSize: total,
    metrics: {
      throughput:    total,
      avgLatencyMs,
      p50LatencyMs,
      p95LatencyMs,
      p99LatencyMs,
      maxLatencyMs,
      successRate,
      errorRate,
      errorCount,
      successCount,
    },
    computedAt: new Date().toISOString(),
  };
}

/**
 * Compute the p-th percentile from a sorted array.
 * Uses linear interpolation (same method as PostgreSQL PERCENTILE_CONT).
 */
function percentile(sortedArr, p) {
  if (sortedArr.length === 0) return 0;
  if (sortedArr.length === 1) return sortedArr[0];

  const idx = p * (sortedArr.length - 1);
  const lower = Math.floor(idx);
  const upper = Math.ceil(idx);
  const fraction = idx - lower;

  if (lower === upper) return sortedArr[lower];
  return Math.round((sortedArr[lower] * (1 - fraction) + sortedArr[upper] * fraction) * 100) / 100;
}

/**
 * Reset a tenant's window (used for testing).
 */
function resetWindow(tenantId) {
  windows.delete(tenantId);
}

/**
 * Get active tenant count.
 */
function getActiveTenants() {
  return windows.size;
}

module.exports = {
  addEvent,
  computeSnapshot,
  resetWindow,
  getActiveTenants,
};
