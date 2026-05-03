const express = require('express');
const helmet = require('helmet');
const cors = require('cors');
const { middleware } = require('@conduit/shared');
const proxyRoutes = require('./routes/proxy');
const authMiddleware = require('./middleware/auth');
const rateLimiter = require('./middleware/rateLimiter');

const app = express();
const PORT = process.env.GATEWAY_PORT || 4000;

// ─── Global Middleware ──────────────────────────
app.use(helmet());
app.use(cors());
app.use(express.json({ limit: '1mb' }));
app.use(middleware.correlationId);
app.use(middleware.requestLogger);

// ─── Health Check ───────────────────────────────
app.get('/health', middleware.healthCheck('api-gateway'));

// ─── Auth & Rate Limiting ───────────────────────
app.use('/api', authMiddleware);
app.use('/api', rateLimiter);

// ─── Proxy Routes ───────────────────────────────
proxyRoutes(app);

// ─── Error Handler ──────────────────────────────
app.use(middleware.errorHandler);

// ─── Start ──────────────────────────────────────
app.listen(PORT, () => {
  console.log(`[API Gateway] Listening on :${PORT}`);
});

module.exports = app;
