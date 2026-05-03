const knex = require('knex');

/**
 * ═══════════════════════════════════════════════════
 *  PostgreSQL Connection (Knex)
 * ═══════════════════════════════════════════════════
 *
 * Used for: Events, Incidents, Remediations
 * Why PG: Structured relational data with strong consistency,
 *         cursor-friendly PKs, and rich filtering.
 */

let _db = null;

function getDB() {
  if (_db) return _db;

  _db = knex({
    client: 'pg',
    connection: {
      host:     process.env.QUERY_PG_HOST || process.env.PG_HOST || 'localhost',
      port:     parseInt(process.env.QUERY_PG_PORT || process.env.PG_PORT || '5432', 10),
      user:     process.env.QUERY_PG_USER || process.env.PG_USER || 'conduit',
      password: process.env.QUERY_PG_PASSWORD || process.env.PG_PASSWORD || 'conduit_secret',
      database: process.env.QUERY_PG_DATABASE || process.env.PG_DATABASE || 'conduit_query',
    },
    pool: {
      min: 2,
      max: parseInt(process.env.QUERY_PG_POOL_MAX || process.env.PG_POOL_MAX || '10', 10),
    },
  });

  return _db;
}

/**
 * Run DB migrations for the query service materialized views.
 */
async function migrate() {
  const db = getDB();

  // Events table — write-once append log
  await db.raw(`
    CREATE TABLE IF NOT EXISTS events (
      id            BIGSERIAL PRIMARY KEY,
      event_id      VARCHAR(64) UNIQUE NOT NULL,
      tenant_id     VARCHAR(64) NOT NULL,
      event_type    VARCHAR(64) NOT NULL,
      source        VARCHAR(128),
      payload       JSONB NOT NULL DEFAULT '{}',
      ingested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.raw(`CREATE INDEX IF NOT EXISTS idx_events_tenant_id ON events (tenant_id, id DESC)`);
  await db.raw(`CREATE INDEX IF NOT EXISTS idx_events_type_id   ON events (tenant_id, event_type, id DESC)`);
  await db.raw(`CREATE INDEX IF NOT EXISTS idx_events_source_id ON events (tenant_id, source, id DESC)`);

  // Incidents table — lifecycle tracking
  await db.raw(`
    CREATE TABLE IF NOT EXISTS incidents (
      id            BIGSERIAL PRIMARY KEY,
      incident_id   VARCHAR(64) UNIQUE NOT NULL,
      tenant_id     VARCHAR(64) NOT NULL,
      type          VARCHAR(64) NOT NULL,
      severity      VARCHAR(16) NOT NULL,
      status        VARCHAR(16) NOT NULL DEFAULT 'detected',
      description   TEXT,
      source        VARCHAR(128),
      detected_at   TIMESTAMPTZ NOT NULL,
      resolved_at   TIMESTAMPTZ,
      duration_ms   BIGINT,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await db.raw(`CREATE INDEX IF NOT EXISTS idx_incidents_tenant_id ON incidents (tenant_id, id DESC)`);
  await db.raw(`CREATE INDEX IF NOT EXISTS idx_incidents_status_id ON incidents (tenant_id, status, id DESC)`);
  await db.raw(`CREATE INDEX IF NOT EXISTS idx_incidents_sev_id    ON incidents (tenant_id, severity, id DESC)`);

  // Remediations table — action tracking
  await db.raw(`
    CREATE TABLE IF NOT EXISTS remediations (
      id               BIGSERIAL PRIMARY KEY,
      remediation_id   VARCHAR(64) UNIQUE NOT NULL,
      incident_id      VARCHAR(64) NOT NULL,
      tenant_id        VARCHAR(64) NOT NULL,
      action           VARCHAR(64) NOT NULL,
      status           VARCHAR(16) NOT NULL DEFAULT 'pending',
      attempts         INT DEFAULT 0,
      error            TEXT,
      details          TEXT,
      created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      completed_at     TIMESTAMPTZ
    )
  `);

  await db.raw(`CREATE INDEX IF NOT EXISTS idx_rem_tenant ON remediations (tenant_id, id DESC)`);
  await db.raw(`CREATE INDEX IF NOT EXISTS idx_rem_status ON remediations (status)`);

  console.log('[PostgreSQL] Schema initialized');
}

async function shutdown() {
  if (_db) {
    await _db.destroy();
    _db = null;
  }
}

module.exports = { getDB, migrate, shutdown };
