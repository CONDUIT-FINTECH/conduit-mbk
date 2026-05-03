const crypto = require('crypto');

/**
 * Express middleware — injects X-Correlation-Id into every request.
 * If the client provides one, it is preserved; otherwise, a new UUID is generated.
 */
function correlationId(req, res, next) {
  const id = req.headers['x-correlation-id'] || crypto.randomUUID();
  req.correlationId = id;
  res.setHeader('X-Correlation-Id', id);
  next();
}

module.exports = correlationId;
