const express = require('express');
const { middleware } = require('@conduit/shared');

// ─── DB & Cache ─────────────────────────────────
const { migrate, shutdown: shutdownPG } = require('./db/postgres');
const { connectMongo, disconnectMongo } = require('./db/mongo');
const { connectCache, disconnectCache } = require('./cache/redisCache');

// ─── Kafka Materializer ─────────────────────────
const { startMaterializer, getStats, flushEventBuffer, stopFlushTimer } = require('./consumers/materializer');

// ─── Routes ─────────────────────────────────────
const eventsRoute        = require('./routes/events');
const metricsRoute       = require('./routes/metrics');
const incidentsRoute     = require('./routes/incidents');
const remediationsRoute  = require('./routes/remediations');
const predictionsRoute   = require('./routes/predictions');

const app = express();
const PORT = process.env.QUERY_PORT || 4004;

app.use(express.json());
app.use(middleware.correlationId);
app.use(middleware.requestLogger);

// ─── Health & Readiness ─────────────────────────
let _ready = false;

app.get('/health', (req, res) => {
  const stats = getStats();
  res.json({
    service: 'query-service',
    status: 'healthy',
    uptime: Math.floor(process.uptime()),
    materializer: stats,
    timestamp: new Date().toISOString(),
  });
});

app.get('/ready', (req, res) => {
  res.status(_ready ? 200 : 503).json({ status: _ready ? 'ready' : 'not_ready' });
});

// ─── CQRS Read Routes ──────────────────────────
app.use('/events',        eventsRoute);
app.use('/metrics',       metricsRoute);
app.use('/incidents',     incidentsRoute);
app.use('/remediations',  remediationsRoute);
app.use('/predictions',   predictionsRoute);

app.use(middleware.errorHandler);

// ─── Boot Lifecycle ─────────────────────────────
async function boot() {
  console.log('[Query Service] Booting...');

  // 1. Connect infrastructure
  await connectCache();
  console.log('[Query Service] Redis cache connected');

  await migrate();
  console.log('[Query Service] PostgreSQL migrated');

  await connectMongo();
  console.log('[Query Service] MongoDB connected');

  // 2. Start HTTP server
  await new Promise((resolve) => {
    app.listen(PORT, () => {
      console.log(`[Query Service] HTTP on :${PORT}`);
      resolve();
    });
  });

  // 3. Start Kafka materializer
  await startMaterializer();

  // 4. Mark ready
  _ready = true;
  console.log('[Query Service] Ready — CQRS read side active');
}

// ─── Graceful Shutdown ──────────────────────────
async function shutdown(signal) {
  console.log(`[Query Service] ${signal} received, shutting down...`);
  _ready = false;

  // Flush pending events
  stopFlushTimer();
  await flushEventBuffer();

  // Close connections
  await disconnectCache();
  await disconnectMongo();
  await shutdownPG();

  console.log('[Query Service] Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));

boot().catch((err) => {
  console.error('[Query Service] Fatal boot error:', err);
  process.exit(1);
});
