# AnomX — Kafka + Airflow + PySpark + PostgreSQL Pipeline

This ZIP is a Spark-ready local version of the `Mohamed_haythem` branch for the AnomX predictive maintenance data engineering project.

## Current runnable scope

```text
C-MAPSS dataset
  -> Kafka producer
  -> Kafka topic
  -> Kafka consumer
  -> PostgreSQL raw_sensor_data
  -> PySpark processing
  -> PostgreSQL processed_sensor_data
  -> Airflow orchestration and validation
```

## What changed in this ZIP

- Added a real `run_spark_processing` task to the Airflow DAG.
- Added `pyspark==3.5.3` to both local and Airflow Python requirements.
- Kept Java inside the Airflow image so PySpark can start correctly.
- Rewrote `processing/spark_processor.py` to remove hardcoded local paths and top-level Spark execution.
- Spark now reads `raw_sensor_data` from PostgreSQL, cleans it, engineers features, calculates RUL, and writes `processed_sensor_data`.
- Added PostgreSQL schema for `processed_sensor_data`.
- Added validation after Spark processing:
  - raw row count
  - processed row count
  - null RUL check
  - duplicate processed key check
- Fixed Kafka consumer offset handling:
  - `enable_auto_commit=False`
  - Kafka offsets are committed only after PostgreSQL commit succeeds.
- Reduced noisy producer/consumer logs using `PRODUCER_LOG_EVERY` and `CONSUMER_LOG_EVERY`.
- Included the full NASA C-MAPSS files inside `data/raw/CMAPSSData/` for local testing.

## Dataset location

The ZIP includes the NASA C-MAPSS files under:

```text
data/raw/CMAPSSData/
```

Default training file:

```text
data/raw/CMAPSSData/train_FD001.txt
```

To use another dataset, update the environment variables in `docker-compose.airflow.yml`:

```yaml
ANOMX_DATASET: FD002
ANOMX_DATASET_PATH: /opt/airflow/data/raw/CMAPSSData/train_FD002.txt
```

## Run with Docker on Windows PowerShell

From the project root:

```powershell
docker compose up -d

docker compose -f docker-compose.airflow.yml build --no-cache

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

Expected task flow:

```text
start
 -> check_kafka
 -> check_postgres
 -> check_dataset
 -> run_kafka_producer
 -> run_kafka_consumer
 -> run_spark_processing
 -> validate_processed_data
 -> show_postgres_counts
 -> end
```

## Useful checks

```powershell
docker compose ps

docker compose -f docker-compose.airflow.yml ps

docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags list

docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM raw_sensor_data;"

docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM processed_sensor_data;"

docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT source_file, COUNT(*) FROM processed_sensor_data GROUP BY source_file;"
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
$env:ANOMX_DATASET_PATH="data/raw/CMAPSSData/train_FD001.txt"
$env:PRODUCER_SLEEP_SECONDS="0"

python ingestion/kafka_producer.py FD001
python ingestion/kafka_consumer.py
python processing/spark_processor.py
```

## Reset everything

```powershell
docker compose -f docker-compose.airflow.yml down -v

docker compose down -v
```

Then start again using the run commands above.

## Do not commit these files to GitHub

- `data/`
- `airflow/logs/`
- `.env`
- runtime DB/config files
- generated backup files

The data is included in this ZIP only for local testing. It is ignored by Git on purpose.
