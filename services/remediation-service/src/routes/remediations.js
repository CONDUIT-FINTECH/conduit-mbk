const { Router } = require('express');
const { createRemediation } = require('../state/remediationStateMachine');
const { 
  addRemediation, 
  getRemediation, 
  listRemediations, 
  getStatusCounts,
  tryAcquireRemediationLock,
  releaseRemediationLock
} = require('../state/remediationStore');
const { planAction } = require('../engine/actionPlanner');
const { executeWithRetry } = require('../engine/remediationEngine');
const { publishRemediationEvent } = require('../publishers/remediationPublisher');

const router = Router();

router.post('/incidents/:id/auto-fix', async (req, res) => {
  const { id: incidentId } = req.params;
  const incident = req.body;

  if (!incident || !incident.incidentId || incident.incidentId !== incidentId) {
    return res.status(400).json({ error: 'INVALID_REQUEST' });
  }

  if (!['detected', 'active'].includes(incident.status)) {
    return res.status(422).json({ error: 'INVALID_STATE', message: 'Incident must be detected or active' });
  }

  // ─── 1. Distributed Lock Check ────────────────
  const acquired = await tryAcquireRemediationLock(incidentId);
  if (!acquired) {
    return res.status(409).json({ 
      error: 'ALREADY_IN_PROGRESS', 
      message: 'A remediation is already active for this incident across the cluster.' 
    });
  }

  try {
    // ─── 2. Plan and Create ───────────────────────
    const plan = planAction(incident);
    const remediation = createRemediation({
      incidentId,
      tenantId: incident.tenantId,
      action:   plan.action,
      details:  plan.details,
      source:   incident.source,
    });

    addRemediation(remediation);

    // ─── 3. Background Execution ──────────────────
    executeInBackground(remediation);

    res.status(202).json({
      message: 'Remediation initiated',
      remediationId: remediation.remediationId,
      action: remediation.action,
    });
  } catch (err) {
    await releaseRemediationLock(incidentId);
    res.status(500).json({ error: 'INTERNAL_ERROR', message: err.message });
  }
});

router.get('/remediations', (req, res) => {
  const { status, tenantId, limit, offset } = req.query;
  res.json(listRemediations({ status, tenantId, limit, offset }));
});

router.get('/remediations/counts', (req, res) => res.json(getStatusCounts()));

router.get('/remediations/:id', (req, res) => {
  const rem = getRemediation(req.params.id);
  rem ? res.json(rem) : res.status(404).json({ error: 'NOT_FOUND' });
});

async function executeInBackground(remediation) {
  try {
    await publishRemediationEvent(remediation, 'remediation.started');
    const final = await executeWithRetry(remediation);
    await publishRemediationEvent(final);
    
    // We only release the lock on SUCCESS or ROLLED_BACK.
    // If it FAILED terminally without rollback, we might keep the lock 
    // to prevent infinite retry loops of the same failed fix.
    if (['success', 'rolled_back'].includes(final.status)) {
      await releaseRemediationLock(final.incidentId);
    }
  } catch (err) {
    console.error(`[Route] Background error:`, err.message);
  }
}

module.exports = router;
