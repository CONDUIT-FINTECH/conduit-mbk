const express = require('express');
const { middleware } = require('@conduit/shared');
const ingestRoute = require('./routes/ingest');
const { connectProducer, disconnectProducer, isProducerReady } = require('./infra/producerPool');
const { getRedisClient, shutdownRedis, isRedisReady } = require('./infra/redis');

const app = express();
const PORT = process.env.INGESTION_PORT || 4001;

// ─── Global Middleware ──────────────────────────
app.use(express.json({ limit: '5mb' }));
app.use(middleware.correlationId);
app.use(middleware.requestLogger);

// ─── Health Check (reports dependency status) ───
app.get('/health', middleware.healthCheck('ingestion-service', '2.0.0', async () => ({
  kafka: isProducerReady() ? 'connected' : 'disconnected',
  redis: isRedisReady() ? 'connected' : 'disconnected',
})));

// ─── Readiness Probe (K8s: don't route until ready) ─
app.get('/ready', (req, res) => {
  const kafkaReady = isProducerReady();
  const redisReady = isRedisReady();

  if (kafkaReady && redisReady) {
    return res.status(200).json({ status: 'ready' });
  }

  res.status(503).json({
    status: 'not_ready',
    dependencies: {
      kafka: kafkaReady ? 'ready' : 'not_ready',
      redis: redisReady ? 'ready' : 'not_ready',
    },
  });
});

// ─── Ingestion Routes ───────────────────────────
app.use('/ingest', ingestRoute);

// ─── Error Handler ──────────────────────────────
app.use(middleware.errorHandler);

// ─── Lifecycle ──────────────────────────────────
async function boot() {
  console.log('[Ingestion Service] Booting...');

  // 1. Connect Redis FIRST (idempotency backend)
  getRedisClient();
  console.log('[Ingestion Service] Redis client initialized');

  // 2. Connect Kafka producer (persistent, reused for all requests)
  await connectProducer();
  console.log('[Ingestion Service] Kafka producer connected (persistent)');

  // 3. Start accepting traffic only after both deps are connected
  app.listen(PORT, () => {
    console.log(`[Ingestion Service] Listening on :${PORT}`);
    console.log(`[Ingestion Service] Ready for traffic (Kafka + Redis connected)`);
  });
}

// ─── Graceful Shutdown ──────────────────────────
async function shutdown(signal) {
  console.log(`[Ingestion Service] ${signal} received, shutting down...`);

  // 1. Stop accepting new connections
  // 2. Drain inflight Kafka messages
  await disconnectProducer();

  // 3. Close Redis
  await shutdownRedis();

  console.log('[Ingestion Service] Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// ─── Boot ───────────────────────────────────────
boot().catch((err) => {
  console.error('[Ingestion Service] Fatal boot error:', err);
  process.exit(1);
});

module.exports = app;
