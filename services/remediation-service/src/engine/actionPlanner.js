/**
 * ═══════════════════════════════════════════════════
 *  Action Planner
 * ═══════════════════════════════════════════════════
 *
 * Maps incident types to remediation actions.
 * Each action has a type, description, estimated duration,
 * and whether it should be auto-triggered or manual-only.
 */

const ACTION_REGISTRY = {
  // ─── Threshold-detected incidents ─────────────
  error_rate_breach: {
    action:       'auto_rollback',
    details:      'Triggering canary rollback to previous stable version',
    autoTrigger:  true,
    estimatedMs:  15000,
  },
  latency_breach: {
    action:       'scale_out',
    details:      'Increasing replica count by 2x to absorb load',
    autoTrigger:  true,
    estimatedMs:  30000,
  },
  success_rate_drop: {
    action:       'health_check_sweep',
    details:      'Running deep health checks across all service endpoints',
    autoTrigger:  true,
    estimatedMs:  10000,
  },

  // ─── ML-detected incidents ────────────────────
  ml_anomaly: {
    action:       'adaptive_throttle',
    details:      'Applying ML-recommended rate limits to affected endpoints',
    autoTrigger:  true,
    estimatedMs:  5000,
  },

  // ─── Fallback ─────────────────────────────────
  _default: {
    action:       'notify_on_call',
    details:      'No automated action available. Paging on-call engineer.',
    autoTrigger:  false,
    estimatedMs:  0,
  },
};

/**
 * Plan a remediation action based on the incident type.
 * @param {object} incident - The incident to remediate
 * @returns {{ action, details, autoTrigger, estimatedMs }}
 */
function planAction(incident) {
  const plan = ACTION_REGISTRY[incident.type] || ACTION_REGISTRY._default;

  return {
    ...plan,
    details: `${plan.details} (source: ${incident.source || 'unknown'})`,
  };
}

/**
 * Check if an incident qualifies for auto-remediation.
 */
function canAutoRemediate(incident) {
  // Only auto-fix critical and high severity
  if (!['critical', 'high'].includes(incident.severity)) return false;

  const plan = ACTION_REGISTRY[incident.type];
  return plan ? plan.autoTrigger : false;
}

module.exports = { planAction, canAutoRemediate, ACTION_REGISTRY };
