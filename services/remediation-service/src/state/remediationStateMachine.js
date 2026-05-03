const { v4: uuidv4 } = require('uuid');

/**
 * ═══════════════════════════════════════════════════
 *  Remediation State Machine (Upgraded)
 * ═══════════════════════════════════════════════════
 *
 *  Lifecycle:
 *    PENDING → EXECUTING → SUCCESS | FAILED
 *    FAILED  → EXECUTING  (retry)
 *    FAILED  → ROLLING_BACK → ROLLED_BACK (on terminal failure)
 *
 *  Terminal states: SUCCESS, ROLLED_BACK, FAILED (if no rollback defined)
 */

const VALID_TRANSITIONS = {
  pending:      ['executing'],
  executing:    ['success', 'failed'],
  failed:       ['executing', 'rolling_back'],
  rolling_back: ['rolled_back', 'failed'], // can fail during rollback too
  success:      [],
  rolled_back:  [],
};

const SEVERITY_LEVELS = ['critical', 'high', 'medium', 'low'];

function createRemediation({ incidentId, tenantId, action, details, source }) {
  const now = new Date().toISOString();

  return {
    remediationId: uuidv4(),
    incidentId,
    tenantId,
    action,
    details,
    source,
    status:      'pending',
    attempts:    0,
    maxRetries:  parseInt(process.env.REMEDIATION_MAX_RETRIES || '3', 10),
    error:       null,
    createdAt:   now,
    startedAt:   null,
    completedAt: null,
    updatedAt:   now,
  };
}

function applyTransition(remediation, targetStatus, meta = {}) {
  const allowed = VALID_TRANSITIONS[remediation.status];
  if (!allowed || !allowed.includes(targetStatus)) {
    throw new Error(
      `Cannot transition from '${remediation.status}' to '${targetStatus}'. ` +
      `Allowed: [${(allowed || []).join(', ')}]`
    );
  }

  const now = new Date().toISOString();
  const updated = { ...remediation, status: targetStatus, updatedAt: now };

  if (targetStatus === 'executing') {
    updated.startedAt = now;
    updated.attempts = remediation.attempts + 1;
    updated.error = null;
  }

  if (targetStatus === 'success' || targetStatus === 'rolled_back') {
    updated.completedAt = now;
    if (meta.result) updated.result = meta.result;
  }

  if (targetStatus === 'failed') {
    updated.error = meta.error || 'Unknown error';
    if (updated.attempts >= updated.maxRetries) {
      // If we don't trigger rollback, this becomes terminal
      if (!meta.triggerRollback) {
        updated.completedAt = now;
      }
    }
  }

  return updated;
}

function canRetry(remediation) {
  return remediation.status === 'failed' && remediation.attempts < remediation.maxRetries;
}

module.exports = {
  VALID_TRANSITIONS,
  createRemediation,
  applyTransition,
  canRetry,
};
