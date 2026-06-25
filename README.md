# AnomX — Clean Incremental Two-Flow Pipeline

This version keeps the approved architecture:

```text
Flow 1 — real-time raw ingestion only
Factory sensors / producer -> Kafka -> consumer -> PostgreSQL raw_sensor_data

Flow 2 — hourly incremental processing
Airflow/manual trigger -> read only new raw rows -> context window -> features/prediction -> results/alerts
```

## What is fixed in this clean version

- Prediction no longer reprocesses the same window when no new data arrives.
- `processing_checkpoints.last_processed_raw_id` tracks the last successful raw row.
- Prediction reads only `raw_sensor_data.id > last_processed_raw_id`.
- Output writes and checkpoint update are committed in one transaction.
- Consumer logs distinguish inserted rows from duplicate Kafka messages.
- Producer supports `--start-row` so you can simulate incremental sensor arrivals cleanly.
- Docker image build is lighter because unnecessary system compiler packages were removed.
- `data/raw/CMAPSSData/` is intentionally empty; copy your local C-MAPSS files there.

## Copy data first

Put the NASA C-MAPSS files here:

```text
data/raw/CMAPSSData/train_FD001.txt
data/raw/CMAPSSData/train_FD002.txt
data/raw/CMAPSSData/train_FD003.txt
data/raw/CMAPSSData/train_FD004.txt
```

## Start from scratch

From the project root:

```powershell
docker compose down -v
docker compose up -d --build
docker compose ps
```

Expected core services:

```text
postgres   healthy
kafka      healthy
consumer   up
zookeeper  up
```

## Send the first raw batch

```powershell
docker compose run --rm producer FD001 --start-row 1 --limit 1000 --sleep 0
```

Check raw storage:

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM raw_sensor_data;"
```

## Run prediction manually

```powershell
docker compose run --rm prediction --run-type manual --window-cycles 50
```

Check runs:

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT run_id, run_type, status, raw_rows_used, from_raw_id, to_raw_id, checkpoint_before, checkpoint_after, notes FROM prediction_runs ORDER BY run_id DESC LIMIT 5;"
```

Check outputs:

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM processed_sensor_data;"
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM prediction_results;"
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM alerts;"
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT * FROM processing_checkpoints;"
```

## Prove incremental behaviour

Run prediction again without sending producer:

```powershell
docker compose run --rm prediction --run-type manual --window-cycles 50
```

Expected:

```text
raw_rows_used = 0
notes contains: No new raw rows to process
```

Now send only new source-file rows:

```powershell
docker compose run --rm producer FD001 --start-row 1001 --limit 500 --sleep 0
```

Run prediction again:

```powershell
docker compose run --rm prediction --run-type manual --window-cycles 50
```

Expected:

```text
raw_rows_used = 500
checkpoint_after > checkpoint_before
```

## Airflow hourly DAG

Start the main Docker stack first, then start Airflow:

```powershell
docker compose -f docker-compose.airflow.yml build --no-cache
docker compose -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

Open:

```text
http://localhost:8080
username: admin
password: admin
```

DAG:

```text
anomx_incremental_prediction_pipeline
schedule: @hourly
```

## Local tests without Docker

```powershell
pip install -r requirements.txt
pytest -q
python scripts/quick_local_demo_without_kafka.py
```

The local demo falls back to synthetic data if C-MAPSS files are not copied yet.

## Important design rule

Do not add cleaning, feature engineering, Spark processing, or prediction to:

```text
ingestion/kafka_consumer.py
```

The consumer remains raw row-by-row storage only.

## Important rebuild note

This version has a distinct Docker image name: `anomx-incremental-fixed-app:latest`.
If an old container/image exists, remove it before testing:

```powershell
docker rm -f anomx-raw-consumer anomx-kafka anomx-postgres anomx-zookeeper 2>$null
docker network rm anomx_default 2>$null
docker volume rm anomx-two-flow_pgdata 2>$null
docker image rm anomx-incremental-fixed-app:latest anomx-two-flow-consumer:latest anomx-two-flow-producer:latest anomx-two-flow-prediction:latest 2>$null
```

The producer now supports `--start-row`:

```powershell
docker compose run --rm producer FD001 --start-row 1 --limit 1000 --sleep 0
docker compose run --rm producer FD001 --start-row 1001 --limit 500 --sleep 0
```

The producer reads C-MAPSS files from the host through the bind mount `./data:/app/data:ro`, so copy the data before running the producer. You do not need to rebuild just because you copied new dataset files.
