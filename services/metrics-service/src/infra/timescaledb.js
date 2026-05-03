const { Pool } = require('pg');

/**
 * TimescaleDB connection pool for the Metrics Service.
 * 
 * Uses pg Pool for connection reuse — critical when writing
 * at 1000+ datapoints/sec from the Kafka consumer.
 */

let _pool = null;

function getPool() {
  if (_pool) return _pool;

  _pool = new Pool({
    host:     process.env.TIMESCALE_HOST     || 'localhost',
    port:     parseInt(process.env.TIMESCALE_PORT || '5433', 10),
    user:     process.env.TIMESCALE_USER     || 'conduit',
    password: process.env.TIMESCALE_PASSWORD || 'conduit_secret',
    database: process.env.TIMESCALE_DATABASE || 'conduit_metrics',

    // Pool tuning for high-throughput writes
    max: parseInt(process.env.TIMESCALE_POOL_MAX || '20', 10),
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });

  _pool.on('connect', () => {
    console.log('[TimescaleDB] New client connected');
  });

  _pool.on('error', (err) => {
    console.error('[TimescaleDB] Pool error:', err.message);
  });

  return _pool;
}

async function query(text, params) {
  const pool = getPool();
  return pool.query(text, params);
}

/**
 * Batch insert using a single multi-row INSERT for efficiency.
 * @param {Array<object>} rows - Array of datapoint objects
 */
async function insertBatch(rows) {
  if (rows.length === 0) return;

  const pool = getPool();

  // Build parameterized multi-row INSERT
  const columns = '(time, tenant_id, event_type, source, latency_ms, is_error, payload_size, correlation_id)';
  const values = [];
  const params = [];
  let paramIdx = 1;

  for (const row of rows) {
    values.push(`($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2}, $${paramIdx + 3}, $${paramIdx + 4}, $${paramIdx + 5}, $${paramIdx + 6}, $${paramIdx + 7})`);
    params.push(
      row.time,
      row.tenant_id,
      row.event_type,
      row.source,
      row.latency_ms,
      row.is_error,
      row.payload_size,
      row.correlation_id
    );
    paramIdx += 8;
  }

  const sql = `INSERT INTO metric_datapoints ${columns} VALUES ${values.join(', ')}`;
  return pool.query(sql, params);
}

async function isHealthy() {
  try {
    const result = await query('SELECT 1');
    return result.rows.length > 0;
  } catch {
    return false;
  }
}

async function shutdown() {
  if (_pool) {
    await _pool.end();
    _pool = null;
    console.log('[TimescaleDB] Pool closed');
  }
}

module.exports = { getPool, query, insertBatch, isHealthy, shutdown };
