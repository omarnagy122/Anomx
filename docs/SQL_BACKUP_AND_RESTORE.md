# SQL Backup and Restore

This project keeps PostgreSQL data in the Docker volume `pgdata`. The SQL backup flow is optional and is meant for snapshots before demos, supervisor handoff, or risky schema changes.

## Create a backup

PowerShell / Windows:

```powershell
.\scripts\backup_postgres.ps1
```

Git Bash / Linux / macOS:

```bash
bash scripts/backup_postgres.sh
```

The generated file is written to:

```text
infra/db/backups/anomx_backup_YYYYMMDD_HHMMSS.sql
```

You can also run the Docker Compose backup service directly:

```powershell
docker compose run --rm db-backup
```

## Restore a backup

PowerShell / Windows:

```powershell
.\scripts\restore_postgres.ps1 -BackupFile .\infra\db\backups\anomx_backup_YYYYMMDD_HHMMSS.sql
```

Git Bash / Linux / macOS:

```bash
bash scripts/restore_postgres.sh infra/db/backups/anomx_backup_YYYYMMDD_HHMMSS.sql
```

Restore only when you intentionally want to replace/rebuild database objects from the SQL dump.

## Why Omar's backup was not used as the default database init file

Omar's `anomx_backup.sql` was useful because it introduced the idea of keeping database snapshots. It was not used as the default restore file because its schema is older than the incremental version. The current schema includes the incremental tables and constraints used by the prediction flow, including:

```text
processing_checkpoints
prediction_runs
prediction_results
alerts
processed_sensor_data.raw_id
unique constraints for idempotent ingestion/processing
```

Using the old dump as automatic init would remove or conflict with these tables. The safer approach is to keep a fresh backup mechanism for the current schema.
