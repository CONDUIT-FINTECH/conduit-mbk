const express = require('express');
const { middleware } = require('@conduit/shared');
const incidentRoutes = require('./routes/incidents');
const { startDetectionConsumer } = require('./consumers/detectionConsumer');
const { getStatusCounts } = require('./state/incidentStore');

const app = express();
const PORT = process.env.INCIDENT_PORT || 4005;

app.use(express.json());
app.use(middleware.correlationId);
app.use(middleware.requestLogger);

// ─── Health Check ───────────────────────────────
app.get('/health', (req, res) => {
  const counts = getStatusCounts();
  res.json({
    service: 'incident-service',
    status: 'healthy',
    uptime: process.uptime(),
    incidents: counts,
    timestamp: new Date().toISOString(),
  });
});

// ─── Readiness Probe ────────────────────────────
let _ready = false;
app.get('/ready', (req, res) => {
  res.status(_ready ? 200 : 503).json({ status: _ready ? 'ready' : 'not_ready' });
});

// ─── Routes ─────────────────────────────────────
app.use('/incidents', incidentRoutes);
app.use(middleware.errorHandler);

// ─── Boot Lifecycle ─────────────────────────────
async function boot() {
  console.log('[Incident Service] Booting...');

  // 1. Start HTTP server (health check available immediately)
  app.listen(PORT, () => {
    console.log(`[Incident Service] HTTP on :${PORT}`);
  });

  // 2. Start Kafka detection consumer
  await startDetectionConsumer();

  // 3. Mark ready
  _ready = true;
  console.log('[Incident Service] Ready — Detection pipeline active');
  console.log('[Incident Service] Inputs: metrics.computed + ml.predictions');
  console.log('[Incident Service] Output: incidents.events');
}

// ─── Graceful Shutdown ──────────────────────────
process.on('SIGTERM', () => {
  console.log('[Incident Service] SIGTERM received, shutting down...');
  _ready = false;
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('[Incident Service] SIGINT received, shutting down...');
  _ready = false;
  process.exit(0);
});

// ─── Boot ───────────────────────────────────────
boot().catch((err) => {
  console.error('[Incident Service] Fatal boot error:', err);
  process.exit(1);
});
