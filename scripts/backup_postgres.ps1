$ErrorActionPreference = "Stop"

$BackupDir = Join-Path (Join-Path "infra" "db") "backups"
New-Item -ItemType Directory -Force -Path $BackupDir | Out-Null
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$BackupFile = Join-Path $BackupDir "anomx_backup_$Timestamp.sql"

Write-Host "[backup] Writing PostgreSQL SQL dump to $BackupFile"
$dump = docker compose exec -T postgres pg_dump -U anomx -d anomx_db --clean --if-exists --no-owner --no-privileges
if ($LASTEXITCODE -ne 0) { throw "pg_dump failed" }
$dump | Out-File -FilePath $BackupFile -Encoding utf8

Write-Host "[backup] Done: $BackupFile"
Get-Item $BackupFile | Format-List FullName, Length
