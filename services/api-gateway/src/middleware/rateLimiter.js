const { errors } = require('@conduit/shared');

// In-memory fallback; production uses Redis (ioredis)
const windowMs = 60 * 1000; // 1 minute
const maxRequests = 100;
const clients = new Map();

/**
 * Simple token-bucket rate limiter.
 * Production: replace with Redis INCR + EXPIRE for distributed limiting.
 */
module.exports = function rateLimiter(req, res, next) {
  const clientId = req.user?.tenantId || req.ip;
  const now = Date.now();

  if (!clients.has(clientId)) {
    clients.set(clientId, { count: 1, resetAt: now + windowMs });
    return next();
  }

  const bucket = clients.get(clientId);

  if (now > bucket.resetAt) {
    bucket.count = 1;
    bucket.resetAt = now + windowMs;
    return next();
  }

  bucket.count++;

  if (bucket.count > maxRequests) {
    const retryAfter = Math.ceil((bucket.resetAt - now) / 1000);
    res.setHeader('Retry-After', retryAfter);
    throw new errors.RateLimitError(retryAfter);
  }

  next();
};
