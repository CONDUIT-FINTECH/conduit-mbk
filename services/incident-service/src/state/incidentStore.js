const Redis = require('ioredis');
const { applyTransition } = require('./incidentStateMachine');

/**
 * ═══════════════════════════════════════════════════
 *  Incident Store & Deduplication Layer
 * ═══════════════════════════════════════════════════
 * 
 * Production: uses Redis for distributed deduplication
 * and PostgreSQL for incident persistence.
 */

// Redis client for distributed deduplication
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  maxRetriesPerRequest: 3,
});

// In-memory store (fallback persistence for demo)
const incidents = new Map();

const DEDUP_WINDOW_SECONDS = parseInt(process.env.INCIDENT_DEDUP_WINDOW_MS || '300000', 10) / 1000;
const MAX_INCIDENTS_IN_MEMORY = 10000;

/**
 * Atomic Deduplication Check & Set
 * Uses Redis SET NX EX to ensure only one instance creates the incident.
 * Returns true if the incident is NEW (not a duplicate).
 */
async function tryAcquireDedupLock(tenantId, type, source) {
  const dedupKey = `incident:dedup:${tenantId}:${type}:${source}`;
  
  // Try to set the key only if it doesn't exist
  const result = await redis.set(dedupKey, '1', 'EX', DEDUP_WINDOW_SECONDS, 'NX');
  
  return result === 'OK';
}

/**
 * Release dedup lock (used when an incident is resolved manually)
 */
async function releaseDedupLock(tenantId, type, source) {
  const dedupKey = `incident:dedup:${tenantId}:${type}:${source}`;
  await redis.del(dedupKey);
}

/**
 * Store a new incident in the local store.
 */
function addIncident(incident) {
  // Prevent memory leak in demo mode
  if (incidents.size >= MAX_INCIDENTS_IN_MEMORY) {
    const oldestKey = incidents.keys().next().value;
    incidents.delete(oldestKey);
  }

  incidents.set(incident.incidentId, incident);
  return incident;
}

/**
 * Retrieve an incident by ID.
 */
function getIncident(incidentId) {
  return incidents.get(incidentId) || null;
}

/**
 * Transition an incident's state using the state machine.
 */
async function transitionIncident(incidentId, targetStatus) {
  const current = incidents.get(incidentId);
  if (!current) {
    throw new Error(`Incident not found: ${incidentId}`);
  }

  const updated = applyTransition(current, targetStatus);
  incidents.set(incidentId, updated);

  // If resolved, release the dedup lock so new incidents can be detected
  if (targetStatus === 'resolved') {
    await releaseDedupLock(updated.tenantId, updated.type, updated.source);
  }

  return updated;
}

/**
 * List incidents with filters.
 */
function listIncidents({ status, severity, tenantId, limit = 50, offset = 0 } = {}) {
  let result = Array.from(incidents.values());

  if (status)   result = result.filter(i => i.status === status);
  if (severity) result = result.filter(i => i.severity === severity);
  if (tenantId) result = result.filter(i => i.tenantId === tenantId);

  result.sort((a, b) => new Date(b.detectedAt) - new Date(a.detectedAt));

  return {
    data: result.slice(offset, offset + limit),
    total: result.length,
    hasMore: offset + limit < result.length,
  };
}

/**
 * Get status counts for health/dashboards.
 */
function getStatusCounts() {
  const counts = { detected: 0, active: 0, resolved: 0 };
  for (const inc of incidents.values()) {
    counts[inc.status] = (counts[inc.status] || 0) + 1;
  }
  return counts;
}

module.exports = {
  tryAcquireDedupLock,
  addIncident,
  getIncident,
  transitionIncident,
  listIncidents,
  getStatusCounts,
};
