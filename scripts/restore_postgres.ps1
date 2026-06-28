param(
    [Parameter(Mandatory=$true)]
    [string]$BackupFile
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $BackupFile)) {
    throw "Backup file not found: $BackupFile"
}

Write-Host "[restore] Restoring $BackupFile into PostgreSQL..."
Get-Content -Raw -Encoding utf8 $BackupFile | docker compose exec -T postgres psql -U anomx -d anomx_db
if ($LASTEXITCODE -ne 0) { throw "psql restore failed" }
Write-Host "[restore] Done."
