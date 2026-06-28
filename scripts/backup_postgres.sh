#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="infra/db/backups"
mkdir -p "$BACKUP_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BACKUP_FILE="$BACKUP_DIR/anomx_backup_${TIMESTAMP}.sql"

echo "[backup] Writing PostgreSQL SQL dump to $BACKUP_FILE"
docker compose exec -T postgres pg_dump \
  -U "${POSTGRES_USER:-anomx}" \
  -d "${POSTGRES_DB:-anomx_db}" \
  --clean --if-exists --no-owner --no-privileges \
  > "$BACKUP_FILE"

echo "[backup] Done: $BACKUP_FILE"
ls -lh "$BACKUP_FILE"
