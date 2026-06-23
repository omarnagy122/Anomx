# AnomX Improved Airflow Pipeline

This ZIP is a cleaned and runnable version that combines:

- the Airflow orchestration idea from `Mohamed_haythem`
- the configurable producer/consumer logic from `main`
- duplicate-safe PostgreSQL inserts
- no committed Airflow runtime logs

## What it runs

Airflow DAG:

```text
start -> check_kafka -> check_postgres -> run_kafka_producer -> run_kafka_consumer -> show_postgres_count -> end
```

The sample dataset included here is intentionally tiny so the pipeline can run immediately. Replace it with the real C-MAPSS dataset when needed.

Expected real dataset path:

```text
data/raw/CMAPSSData/train_FD001.txt
```

## Run on Windows PowerShell

From inside this folder:

```powershell
docker compose up -d

docker compose -f docker-compose.airflow.yml up airflow-init

docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

Then open:

```text
http://localhost:8080
```

Login:

```text
username: admin
password: admin
```

Trigger DAG:

```text
anomx_airflow_pipeline
```

## Useful checks

```powershell
docker compose ps

docker compose -f docker-compose.airflow.yml ps

docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags list

docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM raw_sensor_data;"
```

## Reset database if needed

```powershell
docker compose down -v

docker compose -f docker-compose.airflow.yml down -v
```

Then run the startup commands again.
