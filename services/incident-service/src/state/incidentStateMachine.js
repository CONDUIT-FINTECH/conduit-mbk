const { v4: uuidv4 } = require('uuid');

/**
 * ═══════════════════════════════════════════════════
 *  Incident State Machine
 * ═══════════════════════════════════════════════════
 * 
 *  Lifecycle:
 *    DETECTED → ACTIVE → RESOLVED
 * 
 *  Transitions:
 *    DETECTED  → ACTIVE     (acknowledged by operator or auto-escalation)
 *    DETECTED  → RESOLVED   (auto-resolved if condition clears within grace period)
 *    ACTIVE    → RESOLVED   (manual resolve or remediation confirms fix)
 * 
 *  Terminal state: RESOLVED (immutable after transition)
 */

// ─── Valid Transitions ──────────────────────────
const VALID_TRANSITIONS = {
  detected: ['active', 'resolved'],
  active:   ['resolved'],
  resolved: [],  // terminal
};

const SEVERITY_LEVELS = ['critical', 'high', 'medium', 'low'];

/**
 * Validate a state transition.
 * @returns {{ valid: boolean, reason?: string }}
 */
function validateTransition(currentStatus, targetStatus) {
  const allowed = VALID_TRANSITIONS[currentStatus];
  if (!allowed) {
    return { valid: false, reason: `Unknown current status: ${currentStatus}` };
  }
  if (!allowed.includes(targetStatus)) {
    return {
      valid: false,
      reason: `Cannot transition from '${currentStatus}' to '${targetStatus}'. Allowed: [${allowed.join(', ')}]`,
    };
  }
  return { valid: true };
}

/**
 * Apply a state transition to an incident object.
 * Returns a new incident object (immutable pattern).
 */
function applyTransition(incident, targetStatus) {
  const result = validateTransition(incident.status, targetStatus);
  if (!result.valid) {
    throw new Error(result.reason);
  }

  const now = new Date().toISOString();
  const updated = { ...incident, status: targetStatus, updatedAt: now };

  if (targetStatus === 'active') {
    updated.acknowledgedAt = now;
  }

  if (targetStatus === 'resolved') {
    updated.resolvedAt = now;
    updated.duration = computeDuration(incident.detectedAt, now);
  }

  return updated;
}

/**
 * Create a new incident in DETECTED state.
 */
function createIncident({ type, severity, source, tenantId, description, triggerData, detectorType }) {
  const now = new Date().toISOString();

  return {
    incidentId:     uuidv4(),
    type,
    severity:       SEVERITY_LEVELS.includes(severity) ? severity : 'medium',
    status:         'detected',
    source,
    tenantId,
    description,
    detectorType,    // 'threshold' | 'ml_anomaly'
    triggerData,
    detectedAt:     now,
    acknowledgedAt: null,
    resolvedAt:     null,
    duration:       null,
    updatedAt:      now,
  };
}

/**
 * Compute incident duration in milliseconds.
 */
function computeDuration(startISO, endISO) {
  return new Date(endISO).getTime() - new Date(startISO).getTime();
}

module.exports = {
  VALID_TRANSITIONS,
  SEVERITY_LEVELS,
  validateTransition,
  applyTransition,
  createIncident,
};
