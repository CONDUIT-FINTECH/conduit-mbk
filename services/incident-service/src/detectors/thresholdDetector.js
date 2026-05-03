const { createIncident } = require('../state/incidentStateMachine');

/**
 * ═══════════════════════════════════════════════════
 *  Metric Threshold Detector
 * ═══════════════════════════════════════════════════
 * 
 * Evaluates computed metric snapshots from conduit.metrics.computed
 * against configurable thresholds.
 * 
 * Detects:
 *   - Error rate breaches
 *   - P95 latency breaches
 *   - Success rate drops
 */

// Production: load from config service or database per tenant
const THRESHOLDS = {
  errorRate:      parseFloat(process.env.THRESHOLD_ERROR_RATE    || '0.05'),     // 5%
  p95LatencyMs:   parseInt(process.env.THRESHOLD_P95_LATENCY_MS  || '3000', 10), // 3s
  successRate:    parseFloat(process.env.THRESHOLD_SUCCESS_RATE  || '0.95'),     // 95%
};

const SEVERITY_TIERS = {
  errorRate: [
    { threshold: 0.20, severity: 'critical' },
    { threshold: 0.10, severity: 'high' },
    { threshold: 0.05, severity: 'medium' },
    { threshold: 0.03, severity: 'low' },
  ],
  p95LatencyMs: [
    { threshold: 10000, severity: 'critical' },
    { threshold: 5000,  severity: 'high' },
    { threshold: 3000,  severity: 'medium' },
    { threshold: 2000,  severity: 'low' },
  ],
  successRate: [
    { threshold: 0.80, severity: 'critical' },  // below 80% = critical
    { threshold: 0.90, severity: 'high' },
    { threshold: 0.95, severity: 'medium' },
    { threshold: 0.98, severity: 'low' },
  ],
};

/**
 * Evaluate a metric snapshot against thresholds.
 * @param {object} snapshot - Metric snapshot from metrics service
 * @returns {object|null} - Incident object or null if no breach
 */
function evaluateMetrics(snapshot) {
  const { tenantId, metrics } = snapshot;
  if (!metrics) return null;

  const { errorRate, p95LatencyMs, successRate } = metrics;

  // ─── Error Rate Breach ────────────────────────
  if (errorRate !== undefined && errorRate > THRESHOLDS.errorRate) {
    return createIncident({
      type:         'error_rate_breach',
      severity:     classifySeverity('errorRate', errorRate, 'above'),
      source:       snapshot.source || 'metrics-service',
      tenantId,
      detectorType: 'threshold',
      description:  `Error rate ${(errorRate * 100).toFixed(1)}% exceeds threshold ${(THRESHOLDS.errorRate * 100)}%`,
      triggerData:  { errorRate, threshold: THRESHOLDS.errorRate, window: snapshot.window },
    });
  }

  // ─── P95 Latency Breach ───────────────────────
  if (p95LatencyMs !== undefined && p95LatencyMs > THRESHOLDS.p95LatencyMs) {
    return createIncident({
      type:         'latency_breach',
      severity:     classifySeverity('p95LatencyMs', p95LatencyMs, 'above'),
      source:       snapshot.source || 'metrics-service',
      tenantId,
      detectorType: 'threshold',
      description:  `P95 latency ${p95LatencyMs}ms exceeds threshold ${THRESHOLDS.p95LatencyMs}ms`,
      triggerData:  { p95LatencyMs, threshold: THRESHOLDS.p95LatencyMs, window: snapshot.window },
    });
  }

  // ─── Success Rate Drop ────────────────────────
  if (successRate !== undefined && successRate < THRESHOLDS.successRate) {
    return createIncident({
      type:         'success_rate_drop',
      severity:     classifySeverity('successRate', successRate, 'below'),
      source:       snapshot.source || 'metrics-service',
      tenantId,
      detectorType: 'threshold',
      description:  `Success rate ${(successRate * 100).toFixed(1)}% below threshold ${(THRESHOLDS.successRate * 100)}%`,
      triggerData:  { successRate, threshold: THRESHOLDS.successRate, window: snapshot.window },
    });
  }

  return null;
}

/**
 * Classify severity based on tiered thresholds.
 * @param {string} metric - The metric name
 * @param {number} value - The actual value
 * @param {string} direction - 'above' (higher = worse) or 'below' (lower = worse)
 */
function classifySeverity(metric, value, direction) {
  const tiers = SEVERITY_TIERS[metric];
  if (!tiers) return 'medium';

  for (const tier of tiers) {
    if (direction === 'above' && value >= tier.threshold) return tier.severity;
    if (direction === 'below' && value <= tier.threshold) return tier.severity;
  }

  return 'low';
}

module.exports = { evaluateMetrics };
