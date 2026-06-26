#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: bash scripts/restore_postgres.sh db/backups/anomx_backup_YYYYMMDD_HHMMSS.sql" >&2
  exit 1
fi

BACKUP_FILE="$1"
if [[ ! -f "$BACKUP_FILE" ]]; then
  echo "[restore] Backup file not found: $BACKUP_FILE" >&2
  exit 1
fi

echo "[restore] Restoring $BACKUP_FILE into PostgreSQL..."
docker compose exec -T postgres psql \
  -U "${POSTGRES_USER:-anomx}" \
  -d "${POSTGRES_DB:-anomx_db}" \
  < "$BACKUP_FILE"

echo "[restore] Done."
