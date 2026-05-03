const { canRetry } = require('../state/remediationStateMachine');
const { updateRemediation, transitionRemediation } = require('../state/remediationStore');

/**
 * ═══════════════════════════════════════════════════
 *  Remediation Executor (v2 with Rollback)
 * ═══════════════════════════════════════════════════
 */

const BASE_RETRY_DELAY_MS = parseInt(process.env.REMEDIATION_RETRY_DELAY_MS || '2000', 10);
const ACTION_TIMEOUT_MS   = parseInt(process.env.REMEDIATION_TIMEOUT_MS || '30000', 10);

async function executeWithRetry(remediation) {
  let current = remediation;

  while (true) {
    // ─── Step 1: Execute ──────────────────────────
    try {
      current = await transitionRemediation(current.remediationId, 'executing');
      
      const result = await withTimeout(
        simulateAction(current.action, current.details),
        ACTION_TIMEOUT_MS
      );

      // Success!
      current = await transitionRemediation(current.remediationId, 'success', { result });
      return current;

    } catch (err) {
      // ─── Step 2: Handle Failure ──────────────────
      const canStillRetry = canRetry(current);
      
      current = await transitionRemediation(current.remediationId, 'failed', { 
        error: err.message,
        retryPending: canStillRetry
      });

      if (canStillRetry) {
        const delay = computeBackoff(current.attempts);
        console.log(`[Executor] Retrying in ${delay}ms... (Attempt ${current.attempts})`);
        await sleep(delay);
        continue;
      }

      // ─── Step 3: Terminal Failure -> Rollback? ───
      console.error(`[Executor] Terminal failure after ${current.attempts} attempts.`);
      
      if (shouldRollback(current.action)) {
        return await executeRollback(current);
      }

      return current;
    }
  }
}

/**
 * Execute rollback for a failed remediation.
 */
async function executeRollback(remediation) {
  try {
    let current = await transitionRemediation(remediation.remediationId, 'rolling_back');
    console.log(`[Executor] Initiating rollback for ${current.action}...`);

    const result = await simulateAction(`rollback_${current.action}`, `Undoing ${current.details}`);
    
    current = await transitionRemediation(current.remediationId, 'rolled_back', { result });
    console.log(`[Executor] Rollback successful.`);
    return current;
  } catch (err) {
    console.error(`[Executor] Rollback FAILED: ${err.message}`);
    // Transition back to failed but marked as rolled_back_failed
    return await transitionRemediation(remediation.remediationId, 'failed', { 
      error: `Rollback failed: ${err.message}` 
    });
  }
}

function shouldRollback(action) {
  // Define which actions support rollback
  const ROLLBACKABLE_ACTIONS = ['auto_rollback', 'scale_out', 'adaptive_throttle'];
  return ROLLBACKABLE_ACTIONS.includes(action);
}

async function simulateAction(action, details) {
  const DURATIONS = {
    auto_rollback:      2000,
    scale_out:          3000,
    health_check_sweep: 1000,
    adaptive_throttle:  1000,
    rollback_scale_out: 2000,
  };

  const duration = DURATIONS[action] || 1500;
  await sleep(duration);

  // 10% failure rate for demo
  if (Math.random() < 0.1) {
    throw new Error(`Execution failed for ${action}`);
  }

  return { message: `${action} complete`, duration };
}

function computeBackoff(attempt) {
  return Math.min(BASE_RETRY_DELAY_MS * Math.pow(2, attempt - 1), 30000);
}

function withTimeout(promise, ms) {
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('Action timed out')), ms);
    promise.then(r => { clearTimeout(t); resolve(r); }).catch(e => { clearTimeout(t); reject(e); });
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

module.exports = { executeWithRetry };
