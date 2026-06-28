# AnomX Command Reference

This file keeps all operational commands in one place. Run commands from the project root unless another path is stated.

## 1. Environment setup

### Install Python dependencies locally

```powershell
pip install -r requirements.txt
```

### Copy NASA C-MAPSS data

Copy the training files into:

```text
data/raw/CMAPSSData/
```

Expected examples:

```text
data/raw/CMAPSSData/train_FD001.txt
data/raw/CMAPSSData/train_FD002.txt
data/raw/CMAPSSData/train_FD003.txt
data/raw/CMAPSSData/train_FD004.txt
```

## 2. Docker Compose validation

### Validate the main Docker Compose file

```powershell
docker compose config
```

### Validate the Airflow Docker Compose file

```powershell
docker compose -f docker-compose.airflow.yml config
```

## 3. Start and stop the main stack

### Clean start from scratch

```powershell
docker compose down -v
docker compose up -d --build
docker compose ps
```

### Start without deleting data

```powershell
docker compose up -d
```

### Stop without deleting data

```powershell
docker compose down
```

### Stop and delete PostgreSQL volume

```powershell
docker compose down -v
```

### View all logs

```powershell
docker compose logs -f
```

### View one service log

```powershell
docker compose logs -f consumer
```

```powershell
docker compose logs -f mqtt-bridge
```

```powershell
docker compose logs -f postgres
```

## 4. Direct Kafka simulation flow

Flow:

```text
Kafka producer -> Kafka -> raw consumer -> PostgreSQL raw_sensor_data
```

### Send first raw batch

```powershell
docker compose run --rm producer FD001 --start-row 1 --limit 1000 --sleep 0
```

### Send next incremental raw batch

```powershell
docker compose run --rm producer FD001 --start-row 1001 --limit 500 --sleep 0
```

### Send another dataset

```powershell
docker compose run --rm producer FD002 --start-row 1 --limit 1000 --sleep 0
```

## 5. Optional MQTT machine simulation flow

Flow:

```text
MQTT simulator -> Mosquitto -> MQTT bridge -> Kafka -> raw consumer -> PostgreSQL raw_sensor_data
```

### Publish simulated machine readings through MQTT

```powershell
docker compose run --rm mqtt-simulator FD001 --start-row 1 --limit 1000 --sleep 0
```

### Publish next incremental MQTT batch

```powershell
docker compose run --rm mqtt-simulator FD001 --start-row 1001 --limit 500 --sleep 0
```

### Send one custom machine test message

```powershell
docker compose run --rm machine-mqtt-test
```

### View MQTT bridge logs

```powershell
docker compose logs -f mqtt-bridge
```

## 6. PostgreSQL checks

### Open PostgreSQL shell

```powershell
docker compose exec postgres psql -U anomx -d anomx_db
```

### Count raw sensor rows

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM raw_sensor_data;"
```

### Count rows by source file

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT source_file, COUNT(*) FROM raw_sensor_data GROUP BY source_file ORDER BY source_file;"
```

### Check latest raw rows

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT id, source_file, engine_id, time_in_cycles, inserted_at FROM raw_sensor_data ORDER BY id DESC LIMIT 10;"
```

### Check checkpoint

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT * FROM processing_checkpoints;"
```

### Check prediction runs

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT run_id, run_type, status, raw_rows_used, from_raw_id, to_raw_id, checkpoint_before, checkpoint_after, notes FROM prediction_runs ORDER BY run_id DESC LIMIT 10;"
```

### Count processed rows, predictions, and alerts

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM processed_sensor_data;"
```

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM prediction_results;"
```

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM alerts;"
```

### Check latest alerts

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT id, source_file, engine_id, severity, message, created_at FROM alerts ORDER BY id DESC LIMIT 10;"
```

## 7. Manual prediction commands

### Run prediction manually

```powershell
docker compose run --rm prediction --run-type manual --window-cycles 50
```

### Prove no-new-data behaviour

Run prediction again without sending any new producer or MQTT batch:

```powershell
docker compose run --rm prediction --run-type manual --window-cycles 50
```

Expected database result:

```text
raw_rows_used = 0
notes contains: No new raw rows to process
```

### Run a longer context window

```powershell
docker compose run --rm prediction --run-type manual --window-cycles 100
```

## 8. Export processed data for model training

Use this when the processed PostgreSQL table is ready and the ML/model-training teammate needs a CSV file.

### Confirm processed data exists

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM processed_sensor_data;"
```

If the count is `0`, run prediction first:

```powershell
docker compose run --rm prediction --run-type manual --window-cycles 50
```

### Export processed data using Docker Compose

```powershell
docker compose run --rm data-exporter
```

Default output:

```text
exports/processed/processed_sensor_data.csv
```

### Export processed data from local Python

Use this option when PostgreSQL is reachable from your machine on `localhost:5432` and Python dependencies are installed.

```powershell
python scripts/export_processed_data_to_csv.py
```

### Export to a custom CSV filename

```powershell
python scripts/export_processed_data_to_csv.py --output exports/processed/roman_training_data.csv
```

### Export another processed-like table if needed

```powershell
python scripts/export_processed_data_to_csv.py --table processed_sensor_data --output exports/processed/processed_sensor_data.csv
```

### Check the exported file

```powershell
dir exports\processed
```

```powershell
python -c "import pandas as pd; df = pd.read_csv('exports/processed/processed_sensor_data.csv'); print(df.shape); print(df.head())"
```

## 9. Airflow commands

Start the main stack first, then start Airflow.

### Build Airflow image

```powershell
docker compose -f docker-compose.airflow.yml build --no-cache
```

### Initialise Airflow database and admin user

```powershell
docker compose -f docker-compose.airflow.yml up airflow-init
```

### Start Airflow webserver and scheduler

```powershell
docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

### Check Airflow services

```powershell
docker compose -f docker-compose.airflow.yml ps
```

### View Airflow scheduler logs

```powershell
docker compose -f docker-compose.airflow.yml logs -f airflow-scheduler
```

### Stop Airflow services

```powershell
docker compose -f docker-compose.airflow.yml down
```

### Airflow UI

```text
URL: http://localhost:8080
Username: admin
Password: admin
DAG: anomx_incremental_prediction_pipeline
Schedule: hourly
```

## 10. Backup and restore commands

### Create backup using Docker Compose service

```powershell
docker compose run --rm db-backup
```

### Create backup on Windows PowerShell

```powershell
.\scripts\backup_postgres.ps1
```

### Create backup on Git Bash, Linux, or macOS

```bash
bash scripts/backup_postgres.sh
```

### Restore backup on Windows PowerShell

```powershell
.\scripts\restore_postgres.ps1 -BackupFile .\infra\db\backups\anomx_backup_YYYYMMDD_HHMMSS.sql
```

### Restore backup on Git Bash, Linux, or macOS

```bash
bash scripts/restore_postgres.sh infra/db/backups/anomx_backup_YYYYMMDD_HHMMSS.sql
```

## 11. Local tests and checks

### Run complete local check suite

```powershell
python scripts/run_all_checks.py
```

### Run all tests

```powershell
python -m pytest -q
```

### Run tests with verbose output

```powershell
python -m pytest -vv
```

### Run one test file

```powershell
python -m pytest tests/test_incremental_pipeline.py -q
```

### Run local demo without Kafka

```powershell
python scripts/quick_local_demo_without_kafka.py
```

### Compile Python files

```powershell
python -m compileall -q .
```

## 12. Cleanup commands

### Remove Python cache files

```powershell
Get-ChildItem -Recurse -Directory -Filter __pycache__ | Remove-Item -Recurse -Force
Get-ChildItem -Recurse -Directory -Filter .pytest_cache | Remove-Item -Recurse -Force
```

### Linux / Git Bash cache cleanup

```bash
find . -type d -name "__pycache__" -prune -exec rm -rf {} +
find . -type d -name ".pytest_cache" -prune -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
```

### Remove old project containers and image if needed

```powershell
docker rm -f anomx-raw-consumer anomx-kafka anomx-postgres anomx-zookeeper anomx-mosquitto anomx-mqtt-bridge 2>$null
docker network rm anomx_default 2>$null
docker volume rm anomx-two-flow_pgdata 2>$null
docker image rm anomx-incremental-fixed-app:latest 2>$null
```

## 13. Git commands

### Check current branch and changes

```powershell
git branch
git status
```

### Add and commit all changes

```powershell
git add .
git commit -m "Clean project structure and update documentation"
```

### Push to Mohamed_haythem branch

```powershell
git push origin Mohamed_haythem
```

### Check recent commits

```powershell
git log --oneline --decorate -5
```

## 14. Common troubleshooting

### Rebuild after dependency or Dockerfile changes

```powershell
docker compose build --no-cache
```

### Restart only the raw consumer

```powershell
docker compose restart consumer
```

### Restart only the MQTT bridge

```powershell
docker compose restart mqtt-bridge
```

### Check container health and status

```powershell
docker compose ps
```

### Check whether PostgreSQL is ready

```powershell
docker compose exec postgres pg_isready -U anomx -d anomx_db
```

### Reset everything for a clean demo

```powershell
docker compose down -v
docker compose up -d --build
```
