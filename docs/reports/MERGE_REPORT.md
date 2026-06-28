# AnomX Merge Report — Omar_Nagy MQTT Features into Incremental Fixed Version

## Base selected

The base project is `Anomx_incremental_fixed_ready`, because it already contains the safer two-flow architecture:

```text
Flow 1: Kafka -> PostgreSQL raw_sensor_data
Flow 2: incremental prediction from only new raw rows
```

## Omar_Nagy features integrated

- Mosquitto MQTT broker configuration.
- MQTT machine simulator.
- MQTT-to-Kafka bridge.
- Optional Docker Compose services for MQTT demo runs.

## Main fixes applied

1. Fixed the likely `EQTT/MQTT` error source by using `paho-mqtt==2.1.0` and callback API v2.
2. Added missing dependency declarations in both `requirements.txt` and `orchestration/airflow/requirements.txt`.
3. Removed old hardcoded path logic from Omar's Airflow DAG approach.
4. Kept the approved incremental pipeline untouched.
5. Prevented MQTT messages from bypassing Kafka/PostgreSQL raw ingestion.
6. Kept duplicate protection through the existing PostgreSQL unique key on `(source_file, engine_id, time_in_cycles)`.
7. Removed generated local runtime files from Omar's version, including Airflow DB/PID/log artifacts and SQL backup data.

## New runtime path

```text
mqtt-simulator -> mosquitto -> mqtt-bridge -> kafka -> consumer -> postgres
```

## Commands to test MQTT path

```powershell
docker compose down -v
docker compose up -d --build
docker compose run --rm mqtt-simulator FD001 --start-row 1 --limit 1000 --sleep 0
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM raw_sensor_data;"
```

## Validation performed in this sandbox

- Python syntax compilation passed for project modules.
- Full pytest passed after installing the project requirements.
- Docker Compose YAML parsed successfully.
- Live Docker container validation was not run because the sandbox does not have the Docker CLI available.

## Follow-up improvement: SQL backup and real-machine handoff

Added after review:

- `scripts/backup_postgres.ps1` and `scripts/backup_postgres.sh`
- `scripts/restore_postgres.ps1` and `scripts/restore_postgres.sh`
- `infra/db/backups/.gitkeep`
- `docs/SQL_BACKUP_AND_RESTORE.md`
- `docs/REAL_MACHINE_MQTT_INTEGRATION.md`
- `scripts/send_mqtt_test_message.py`
- Docker Compose tool services: `db-backup` and `machine-mqtt-test`

Omar's old SQL dump is treated as a legacy idea/reference, not as a default init file, because the current incremental schema contains more tables and idempotency constraints.
