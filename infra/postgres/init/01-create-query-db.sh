#!/bin/bash
# Creates additional databases on first boot.
# Referenced by docker-compose postgres init volume.

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE conduit_query' 
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'conduit_query')\gexec
EOSQL

echo "[init-db] conduit_query database ready"
