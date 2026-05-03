/**
 * Express middleware — logs every incoming request with timing.
 */
function requestLogger(req, res, next) {
  const start = Date.now();

  res.on('finish', () => {
    const duration = Date.now() - start;
    const log = {
      method: req.method,
      path: req.originalUrl,
      status: res.statusCode,
      duration: `${duration}ms`,
      correlationId: req.correlationId || '-',
      userAgent: req.headers['user-agent'] || '-',
    };
    console.log(JSON.stringify(log));
  });

  next();
}

module.exports = requestLogger;
