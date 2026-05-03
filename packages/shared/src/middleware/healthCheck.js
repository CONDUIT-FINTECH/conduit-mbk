/**
 * Standard health check endpoint factory.
 * Eliminates duplicated health JSON across all services.
 *
 * @param {string} serviceName - e.g. 'api-gateway'
 * @param {string} version - semver string
 * @param {function} [dependencyChecker] - optional async () => { kafka: 'connected', ... }
 */
function healthCheck(serviceName, version = '1.0.0', dependencyChecker = null) {
  return async (req, res) => {
    const health = {
      status: 'healthy',
      service: serviceName,
      version,
      uptime: Math.floor(process.uptime()),
      timestamp: new Date().toISOString(),
    };

    if (dependencyChecker) {
      try {
        health.dependencies = await dependencyChecker();
      } catch (err) {
        health.status = 'degraded';
        health.dependencies = { error: err.message };
      }
    }

    const statusCode = health.status === 'healthy' ? 200 : 503;
    res.status(statusCode).json(health);
  };
}

module.exports = healthCheck;
