const http = require('http');
const { ConnectionManager } = require('./connections/connectionManager');
const { SubscriptionEngine } = require('./subscriptions/subscriptionEngine');
const { startKafkaBridge, getLatencyStats } = require('./bridge/kafkaBridge');
const { shutdown: shutdownRedis, createPubClient } = require('./infra/redisPubSub');

const PORT = process.env.WS_PORT || 4006;
const JWT_SECRET = process.env.JWT_SECRET || 'dev-secret';

let connManager;
let subEngine;
let _ready = false;

// ─── HTTP server (health + readiness) ───────────
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    const wsStats = connManager ? connManager.getStats() : { totalConnections: 0 };
    const bridgeStats = getLatencyStats();
    const engineStats = subEngine ? subEngine.getStats() : { totalBroadcasts: 0 };

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      service: 'websocket-service',
      status: 'healthy',
      uptime: Math.floor(process.uptime()),
      connections: wsStats,
      bridge: bridgeStats,
      engine: engineStats,
      timestamp: new Date().toISOString(),
    }));
    return;
  }

  if (req.url === '/ready') {
    res.writeHead(_ready ? 200 : 503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: _ready ? 'ready' : 'not_ready' }));
    return;
  }

  res.writeHead(404);
  res.end();
});

// ─── Boot Lifecycle ─────────────────────────────
async function boot() {
  console.log('[WebSocket Service] Booting...');

  // 1. Create connection manager (starts WebSocket server on httpServer)
  connManager = new ConnectionManager(server, JWT_SECRET);

  // 2. Create subscription engine (Redis subscriber → local broadcast)
  subEngine = new SubscriptionEngine(connManager);

  // 3. Start HTTP + WebSocket server
  await new Promise((resolve) => {
    server.listen(PORT, () => {
      console.log(`[WebSocket Service] HTTP + WS on :${PORT}`);
      resolve();
    });
  });

  // 4. Connect Redis pub/sub (subscriber side)
  await createPubClient().connect();
  await subEngine.start();

  // 5. Start Kafka bridge (publisher side: Kafka → Redis)
  await startKafkaBridge();

  // 6. Mark ready
  _ready = true;
  console.log('[WebSocket Service] Ready — Pipeline active');
  console.log('[WebSocket Service] Flow: Kafka → Redis Pub/Sub → WebSocket clients');
}

// ─── Graceful Shutdown ──────────────────────────
async function shutdown(signal) {
  console.log(`[WebSocket Service] ${signal} received, shutting down...`);
  _ready = false;

  // Close all WS connections
  if (connManager) connManager.closeAll();

  // Disconnect Redis
  await shutdownRedis();

  // Close HTTP server
  server.close(() => {
    console.log('[WebSocket Service] Shutdown complete');
    process.exit(0);
  });
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// ─── Boot ───────────────────────────────────────
boot().catch((err) => {
  console.error('[WebSocket Service] Fatal boot error:', err);
  process.exit(1);
});
