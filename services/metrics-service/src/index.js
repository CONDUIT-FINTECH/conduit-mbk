const express = require('express');
const { middleware } = require('@conduit/shared');
const { startConsumer } = require('./consumers/eventConsumer');
const { startFlushTimer, stopFlushTimer, getBufferSize, getFlushCount } = require('./pipeline/writeBuffer');
const { isHealthy: isTimescaleHealthy, shutdown: shutdownTimescale } = require('./infra/timescaledb');
const { getActiveTenants } = require('./aggregators/slidingWindow');

const app = express();
const PORT = process.env.METRICS_PORT || 4003;

// ─── Health Check (with dependency status) ──────
app.get('/health', async (req, res) => {
  const tsHealthy = await isTimescaleHealthy();
  const status = tsHealthy ? 'healthy' : 'degraded';

  res.status(tsHealthy ? 200 : 503).json({
    service: 'metrics-service',
    status,
    uptime: process.uptime(),
    dependencies: {
      timescaledb: tsHealthy ? 'connected' : 'disconnected',
    },
    pipeline: {
      activeTenants: getActiveTenants(),
      writeBufferSize: getBufferSize(),
      totalFlushes: getFlushCount(),
    },
    timestamp: new Date().toISOString(),
  });
});

// ─── Readiness Probe ────────────────────────────
app.get('/ready', async (req, res) => {
  const tsHealthy = await isTimescaleHealthy();
  res.status(tsHealthy ? 200 : 503).json({
    status: tsHealthy ? 'ready' : 'not_ready',
  });
});

// ─── Lifecycle ──────────────────────────────────
async function boot() {
  console.log('[Metrics Service] Booting...');

  // 1. Start health server
  app.listen(PORT, () => {
    console.log(`[Metrics Service] Health server on :${PORT}`);
  });

  // 2. Start write buffer flush timer
  startFlushTimer();

  // 3. Start Kafka consumer (connects to broker + begins consuming)
  await startConsumer();

  console.log('[Metrics Service] Running — aggregation-only mode');
  console.log('[Metrics Service] Pipeline: Kafka → SlidingWindow + WriteBuffer → TimescaleDB');
}

// ─── Graceful Shutdown ──────────────────────────
async function shutdown(signal) {
  console.log(`[Metrics Service] ${signal} received, shutting down...`);

  // 1. Stop accepting health checks
  // 2. Flush remaining buffered writes
  await stopFlushTimer();

  // 3. Close TimescaleDB pool
  await shutdownTimescale();

  console.log('[Metrics Service] Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// ─── Boot ───────────────────────────────────────
boot().catch((err) => {
  console.error('[Metrics Service] Fatal boot error:', err);
  process.exit(1);
});
