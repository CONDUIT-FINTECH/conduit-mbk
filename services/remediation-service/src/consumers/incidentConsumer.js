const { kafka } = require('@conduit/shared');
const { createConsumer, consumeMessages, TOPICS } = kafka;
const { createRemediation } = require('../state/remediationStateMachine');
const { 
  addRemediation, 
  tryAcquireRemediationLock, 
  releaseRemediationLock 
} = require('../state/remediationStore');
const { planAction, canAutoRemediate } = require('../engine/actionPlanner');
const { executeWithRetry } = require('../engine/remediationEngine');
const { publishRemediationEvent } = require('../publishers/remediationPublisher');

const GROUP_ID = process.env.KAFKA_GROUP_ID_REMEDIATION || 'conduit-remediation-group';

async function startRemediationConsumer() {
  try {
    const { consumer } = await createConsumer({ groupId: GROUP_ID });

    await consumeMessages(consumer, TOPICS.INCIDENTS, async (event) => {
      if (event.eventType !== 'incident.detected') return;

      if (!canAutoRemediate(event)) return;

      // ─── 1. Distributed Lock Check ────────────────
      const acquired = await tryAcquireRemediationLock(event.incidentId);
      if (!acquired) {
        console.log(`[Auto-Remediation] Lock already held for incident ${event.incidentId}. Skipping.`);
        return;
      }

      try {
        // ─── 2. Plan and Create ───────────────────────
        const plan = planAction(event);
        const remediation = createRemediation({
          incidentId: event.incidentId,
          tenantId:   event.tenantId,
          action:     plan.action,
          details:    plan.details,
          source:     event.source,
        });

        addRemediation(remediation);

        console.log(`[Auto-Remediation] Triggered ${plan.action} for ${event.incidentId}`);

        // ─── 3. Background Execution ──────────────────
        executeInBackground(remediation);

      } catch (err) {
        console.error(`[Auto-Remediation] Error initiating:`, err.message);
        await releaseRemediationLock(event.incidentId);
      }
    });

    console.log(`[Remediation Consumer] Subscribed to ${TOPICS.INCIDENTS}`);
  } catch (err) {
    console.error('[Remediation Consumer] Failed to start:', err);
    process.exit(1);
  }
}

async function executeInBackground(remediation) {
  try {
    await publishRemediationEvent(remediation, 'remediation.started');
    const final = await executeWithRetry(remediation);
    await publishRemediationEvent(final);

    if (['success', 'rolled_back'].includes(final.status)) {
      await releaseRemediationLock(final.incidentId);
    }
  } catch (err) {
    console.error(`[Auto-Remediation] Background error:`, err.message);
  }
}

module.exports = { startRemediationConsumer };
