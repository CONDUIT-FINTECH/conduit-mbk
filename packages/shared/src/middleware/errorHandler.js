const { AppError } = require('../errors');

/**
 * Global Express error handler.
 * Catches all errors and returns a consistent JSON response.
 */
function errorHandler(err, req, res, _next) {
  const correlationId = req.correlationId || 'unknown';

  if (err instanceof AppError) {
    console.error(`[${correlationId}] ${err.code}: ${err.message}`);
    return res.status(err.statusCode).json(err.toJSON());
  }

  // Unexpected errors
  console.error(`[${correlationId}] UNHANDLED_ERROR:`, err);
  return res.status(500).json({
    error: 'INTERNAL_ERROR',
    message: 'An unexpected error occurred',
    timestamp: new Date().toISOString(),
  });
}

module.exports = errorHandler;
