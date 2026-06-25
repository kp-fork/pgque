#!/usr/bin/env bash
# Provision a throwaway Postgres, install pgque core + the demo schema.
# Idempotent. Targets Debian/Ubuntu (the common fresh-VM case); see README for
# other distros. Run with sudo on a fresh VM.
set -Eeuo pipefail

DB="${PGQUE_DB:-pgque_repro}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"          # repo root
PGQUE_SQL="$ROOT/sql/pgque.sql"

if [[ ! -f "$PGQUE_SQL" ]]; then
  echo "cannot find $PGQUE_SQL — run from inside the pgque repo" >&2
  exit 1
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "==> installing PostgreSQL + psycopg2 (apt)"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq postgresql postgresql-contrib python3-psycopg2
fi

# make sure a cluster is running
pg_lsclusters -h 2>/dev/null | grep -q online || service postgresql start || true

echo "==> (re)creating database '$DB'"
sudo -u postgres psql -v ON_ERROR_STOP=1 -q <<SQL
select 'create database ${DB}'
 where not exists (select from pg_database where datname = '${DB}')\gexec
SQL

echo "==> installing pgque core"
sudo -u postgres psql -v ON_ERROR_STOP=1 -q -d "$DB" -f "$PGQUE_SQL" >/dev/null

echo "==> installing demo schema"
sudo -u postgres psql -v ON_ERROR_STOP=1 -q -d "$DB" -f "$HERE/schema.sql"

echo "==> done. database='$DB'. Run: bash $HERE/run.sh"
