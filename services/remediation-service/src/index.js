const express = require('express');
const { middleware } = require('@conduit/shared');
const remediationRoutes = require('./routes/remediations');
const { startRemediationConsumer } = require('./consumers/incidentConsumer');
const { getStatusCounts } = require('./state/remediationStore');

const app = express();
const PORT = process.env.REMEDIATION_PORT || 4007;

app.use(express.json());
app.use(middleware.correlationId);
app.use(middleware.requestLogger);

// ─── Health Check ───────────────────────────────
app.get('/health', (req, res) => {
  const counts = getStatusCounts();
  res.json({
    service: 'remediation-service',
    status: 'healthy',
    uptime: Math.floor(process.uptime()),
    remediations: counts,
    timestamp: new Date().toISOString(),
  });
});

// ─── Readiness Probe ────────────────────────────
let _ready = false;
app.get('/ready', (req, res) => {
  res.status(_ready ? 200 : 503).json({ status: _ready ? 'ready' : 'not_ready' });
});

// ─── Routes ─────────────────────────────────────
app.use('/', remediationRoutes);
app.use(middleware.errorHandler);

// ─── Boot Lifecycle ─────────────────────────────
async function boot() {
  console.log('[Remediation Service] Booting...');

  // 1. Start HTTP server
  app.listen(PORT, () => {
    console.log(`[Remediation Service] HTTP on :${PORT}`);
  });

  // 2. Start Kafka consumer (auto-trigger path)
  await startRemediationConsumer();

  // 3. Mark ready
  _ready = true;
  console.log('[Remediation Service] Ready');
  console.log('[Remediation Service] Auto-fix: POST /incidents/:id/auto-fix');
  console.log('[Remediation Service] Auto-trigger: Kafka incidents.events (critical/high)');
}

// ─── Graceful Shutdown ──────────────────────────
process.on('SIGTERM', () => {
  console.log('[Remediation Service] SIGTERM, shutting down...');
  _ready = false;
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('[Remediation Service] SIGINT, shutting down...');
  _ready = false;
  process.exit(0);
});

boot().catch((err) => {
  console.error('[Remediation Service] Fatal boot error:', err);
  process.exit(1);
});
