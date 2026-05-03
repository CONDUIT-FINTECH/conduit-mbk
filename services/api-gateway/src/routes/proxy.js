const { createProxyMiddleware } = require('http-proxy-middleware');

const SERVICES = {
  ingestion: process.env.INGESTION_URL || 'http://localhost:4001',
  query:     process.env.QUERY_URL     || 'http://localhost:4004',
  incident:  process.env.INCIDENT_URL  || 'http://localhost:4005',
};

/**
 * Register proxy routes on the Express app.
 * Metrics Service has NO REST — dashboard reads go through Query Service.
 * WebSocket Service is accessed directly by clients (WSS, not proxied).
 */
module.exports = function proxyRoutes(app) {
  // POST /api/v1/ingest → Ingestion Service (write path)
  app.use(
    '/api/v1/ingest',
    createProxyMiddleware({
      target: SERVICES.ingestion,
      changeOrigin: true,
      pathRewrite: { '^/api/v1/ingest': '/ingest' },
    })
  );

  // GET /api/v1/events → Query Service (read path — CQRS)
  app.use(
    '/api/v1/events',
    createProxyMiddleware({
      target: SERVICES.query,
      changeOrigin: true,
      pathRewrite: { '^/api/v1/events': '/events' },
    })
  );

  // GET /api/v1/metrics → Query Service (read path — CQRS)
  // Metrics Service has no REST; Query materializes metric snapshots for reads
  app.use(
    '/api/v1/metrics',
    createProxyMiddleware({
      target: SERVICES.query,
      changeOrigin: true,
      pathRewrite: { '^/api/v1/metrics': '/metrics' },
    })
  );

  // /api/v1/incidents → Incident Service (CRUD lifecycle)
  app.use(
    '/api/v1/incidents',
    createProxyMiddleware({
      target: SERVICES.incident,
      changeOrigin: true,
      pathRewrite: { '^/api/v1/incidents': '/incidents' },
    })
  );
};
