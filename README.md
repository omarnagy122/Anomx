# AnomX — Cleaned Airflow/Kafka/PostgreSQL Pipeline

This is a cleaned local version of the `Mohamed_haythem` branch for the AnomX predictive maintenance data engineering project.

The current runnable scope is Phase 1 + Phase 3:

```text
C-MAPSS dataset -> Kafka producer -> Kafka topic -> Kafka consumer -> PostgreSQL raw_sensor_data
                                      Airflow orchestration
```

## What was cleaned

- Kept one production-facing Airflow DAG only: `airflow/dags/anomx_airflow_pipeline.py`.
- Removed experimental DAGs, editor settings, `.save` files, Airflow runtime logs, local Airflow DB/config files, and generated backups.
- Kept PostgreSQL duplicate protection using:
  - `UNIQUE (source_file, engine_id, time_in_cycles)`
  - `ON CONFLICT (source_file, engine_id, time_in_cycles) DO NOTHING`
- Improved Kafka consumer reliability by disabling auto offset commit and committing offsets only after PostgreSQL commit succeeds.
- Reworked `processing/spark_processor.py` to remove hardcoded local paths and top-level Spark execution.
- Added `processed_sensor_data` schema for future Spark processing.
- Added Docker healthchecks for Kafka, Zookeeper, project PostgreSQL, and Airflow metadata PostgreSQL.
- Cleaned `.gitignore`.

## Dataset location

The ZIP version includes the NASA C-MAPSS files under:

```text
data/raw/CMAPSSData/
```

This directory is intentionally ignored by Git, so it can exist locally without being committed to GitHub.

Expected default training file:

```text
data/raw/CMAPSSData/train_FD001.txt
```

To use a different dataset, update environment variables in `docker-compose.airflow.yml`:

```yaml
ANOMX_DATASET: FD002
ANOMX_DATASET_PATH: /opt/airflow/data/raw/CMAPSSData/train_FD002.txt
```

## Run with Docker on Windows PowerShell

From the project root:

```powershell
docker compose up -d

docker compose -f docker-compose.airflow.yml up airflow-init

docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

Open Airflow:

```text
http://localhost:8080
```

Login:

```text
username: admin
password: admin
```

Trigger this DAG manually:

```text
anomx_airflow_pipeline
```

## Useful checks

```powershell
docker compose ps

docker compose -f docker-compose.airflow.yml ps

docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags list

docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM raw_sensor_data;"

docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM processed_sensor_data;"
```

## Run producer/consumer locally without Airflow

Start Kafka and PostgreSQL first:

```powershell
docker compose up -d
```

Then from a Python virtual environment:

```powershell
pip install -r requirements.txt

$env:KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:9092"
$env:POSTGRES_HOST="localhost"
$env:ANOMX_DATASET="FD001"
$env:PRODUCER_SLEEP_SECONDS="0"

python ingestion/kafka_producer.py FD001
python ingestion/kafka_consumer.py
```

## Run Spark processing locally

After PostgreSQL is running:

```powershell
pip install -r requirements.txt
$env:POSTGRES_HOST="localhost"
python processing/spark_processor.py FD001
```

## Reset everything

```powershell
docker compose -f docker-compose.airflow.yml down -v

docker compose down -v
```

Then start again using the run commands above.

## Notes before merging to GitHub

Before pushing this to the branch, check:

```powershell
git status
```

Do not commit:

- `data/`
- `airflow/logs/`
- `.env`
- runtime DB/config files
- generated backup files

The data is included in the ZIP only for local testing. It is ignored by Git on purpose.
