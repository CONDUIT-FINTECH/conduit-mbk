const Redis = require('ioredis');
const { applyTransition } = require('./remediationStateMachine');

/**
 * ═══════════════════════════════════════════════════
 *  Remediation Store & Locking Layer
 * ═══════════════════════════════════════════════════
 * 
 * Production: uses Redis for distributed locking 
 * and PostgreSQL for persistence.
 */

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  maxRetriesPerRequest: 3,
});

const MAX_RECORDS = 5000;
const LOCK_TTL_SECONDS = 300; // 5 minutes

// In-memory store (fallback for demo)
const remediations = new Map();

/**
 * Atomic Incident Lock
 * Prevents multiple instances from remediating the same incident simultaneously.
 */
async function tryAcquireRemediationLock(incidentId) {
  const lockKey = `remediation:lock:${incidentId}`;
  const result = await redis.set(lockKey, '1', 'EX', LOCK_TTL_SECONDS, 'NX');
  return result === 'OK';
}

/**
 * Release incident lock
 */
async function releaseRemediationLock(incidentId) {
  const lockKey = `remediation:lock:${incidentId}`;
  await redis.del(lockKey);
}

function addRemediation(remediation) {
  if (remediations.size >= MAX_RECORDS) {
    const oldestKey = remediations.keys().next().value;
    remediations.delete(oldestKey);
  }
  remediations.set(remediation.remediationId, remediation);
  return remediation;
}

function getRemediation(remediationId) {
  return remediations.get(remediationId) || null;
}

function updateRemediation(remediation) {
  remediations.set(remediation.remediationId, remediation);
  return remediation;
}

async function transitionRemediation(remediationId, targetStatus, meta = {}) {
  const current = remediations.get(remediationId);
  if (!current) throw new Error(`Remediation not found: ${remediationId}`);

  const updated = applyTransition(current, targetStatus, meta);
  remediations.set(remediationId, updated);

  // If we reach a terminal state, we could release the lock, 
  // but usually we keep it for the duration of the incident 
  // to prevent re-remediation of the same instance.
  if (['success', 'rolled_back', 'failed'].includes(targetStatus) && !meta.retryPending) {
    // Optional: releaseRemediationLock(updated.incidentId);
  }

  return updated;
}

function listRemediations({ status, tenantId, limit = 50, offset = 0 } = {}) {
  let result = Array.from(remediations.values());
  if (status)   result = result.filter(r => r.status === status);
  if (tenantId) result = result.filter(r => r.tenantId === tenantId);
  result.sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  return { data: result.slice(offset, offset + limit), total: result.length };
}

function getStatusCounts() {
  const counts = { pending: 0, executing: 0, success: 0, failed: 0, rolling_back: 0, rolled_back: 0 };
  for (const r of remediations.values()) {
    counts[r.status] = (counts[r.status] || 0) + 1;
  }
  return counts;
}

module.exports = {
  tryAcquireRemediationLock,
  releaseRemediationLock,
  addRemediation,
  getRemediation,
  updateRemediation,
  transitionRemediation,
  listRemediations,
  getStatusCounts,
};
