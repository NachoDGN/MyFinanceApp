#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TIMESTAMP="$(date +"%Y%m%d-%H%M%S")"
BACKUP_ROOT="${BACKUP_ROOT:-$ROOT_DIR/backups/supabase}"
BACKUP_DIR="${1:-$BACKUP_ROOT/$TIMESTAMP}"

mkdir -p "$BACKUP_DIR"

dump_args=()

if [[ -n "${DATABASE_URL:-}" ]]; then
  dump_args=(--db-url "$DATABASE_URL")
  connection_target="DATABASE_URL"
else
  dump_args=(--local)
  connection_target="local Supabase instance"
fi

echo "Creating Supabase backup in $BACKUP_DIR"
echo "Connection target: $connection_target"

pnpm db:cli db dump "${dump_args[@]}" --role-only --file "$BACKUP_DIR/roles.sql"
pnpm db:cli db dump "${dump_args[@]}" --file "$BACKUP_DIR/schema.sql"
pnpm db:cli db dump "${dump_args[@]}" --data-only --use-copy --file "$BACKUP_DIR/data.sql"

cat >"$BACKUP_DIR/README.txt" <<EOF
Supabase backup created at $(date -u +"%Y-%m-%dT%H:%M:%SZ")

Files:
- roles.sql: cluster roles
- schema.sql: database schema
- data.sql: table data

Restore order:
1. roles.sql
2. schema.sql
3. data.sql

Restore note:
- Some tables may have circular foreign keys. If data restore fails, load data with triggers disabled or set session_replication_role=replica during import.
- Restoring these files requires a Postgres client such as psql or pg_restore-compatible tooling.

Command used:
pnpm db:cli db dump ${connection_target}
EOF

echo "Backup complete:"
echo "  $BACKUP_DIR/roles.sql"
echo "  $BACKUP_DIR/schema.sql"
echo "  $BACKUP_DIR/data.sql"
echo "Note: data restores may require triggers disabled for circular foreign keys."
