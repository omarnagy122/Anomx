# AnomX — Predictive Maintenance Data Pipeline

AnomX is a data-engineering capstone project for predictive maintenance. It simulates industrial machine sensor readings, stores raw telemetry, processes only new data incrementally, and produces machine-health predictions and alerts for maintenance decision-making.

## Project purpose

The project demonstrates how a modern data pipeline can move from raw machine telemetry to operational maintenance insights. The goal is to support proactive maintenance by detecting risky machine behaviour before failure and giving operators a clear view of machine health.

## Architecture overview

AnomX uses a two-flow architecture:

### Flow 1 — Raw real-time ingestion

Sensor data is streamed into the system and stored exactly as raw readings. The ingestion layer is intentionally lightweight and does not perform cleaning, feature engineering, or prediction.

### Flow 2 — Incremental prediction

The prediction pipeline reads only raw rows that have not been processed before, adds a small context window for feature calculation, writes processed records, generates risk scores, creates alerts, and advances the checkpoint only after a successful transaction.

## Optional MQTT machine layer

The repository supports two ingestion entry points:

- Direct simulation through Kafka.
- Optional machine-protocol simulation through MQTT, Mosquitto, and an MQTT-to-Kafka bridge.

The MQTT path is useful for demonstrating how a real factory machine protocol can feed the same Kafka and PostgreSQL pipeline without changing the raw consumer or prediction flow.

## Technology stack

| Layer | Technology | Role |
| --- | --- | --- |
| Machine protocol | MQTT / Mosquitto | Optional simulated factory-machine telemetry layer |
| Streaming | Apache Kafka | Sensor-event transport |
| Raw storage | PostgreSQL | Durable raw and processed data storage |
| Orchestration | Apache Airflow | Scheduled incremental prediction workflow |
| Processing | Python / Pandas | Cleaning, feature engineering, incremental prediction logic |
| Testing | Pytest | Unit and architecture guard tests |
| Containerisation | Docker Compose | Local reproducible runtime |

## Main repository structure

```text
.
├── src/                      # Application source code: ingestion, processing, prediction, config
├── orchestration/airflow/     # Airflow DAG and Airflow container files
├── infra/                    # Infrastructure files: Dockerfile, PostgreSQL schema, MQTT config
├── data/raw/CMAPSSData/      # Local NASA C-MAPSS data location
├── docs/                     # Architecture, limitations, MQTT, backup/restore docs
├── scripts/                  # Helper scripts for demo, backup, restore, and MQTT testing
├── tests/                    # Pytest suite
├── COMMANDS.md               # Full command reference
└── README.md                 # Project description
```

## Key design rules

- The Kafka consumer stores raw rows only.
- Raw rows are deduplicated by source file, engine ID, and cycle.
- Prediction is checkpoint-based and incremental.
- Prediction outputs and checkpoint updates are committed together.
- Re-running prediction with no new data exits successfully without duplicating outputs.
- SQL backups are generated locally and should not be committed.


## Final dashboard integration

The delivery flow is now connected end to end:

```text
Dashboard -> PostgreSQL Database -> trained XGBoost model -> prediction_results / alerts -> Dashboard Display
```

From the dashboard, press **Run Prediction** to load `model/xgboost_predictive_model.pkl`, read the latest engine rows from `processed_sensor_data`, save outputs into `prediction_results` and `alerts`, and refresh the displayed counts/tables.

See `COMMANDS.md` and `docs/TRAINED_MODEL_DASHBOARD_INTEGRATION.md` for the exact run and verification commands.

## Documentation map

- `COMMANDS.md` — all setup, Docker, Kafka, MQTT, PostgreSQL, Airflow, backup, restore, testing, and Git commands.
- `docs/ARCHITECTURE.md` — detailed two-flow architecture.
- `docs/KNOWN_LIMITATIONS.md` — current demo limitations and production notes.
- `docs/REAL_MACHINE_MQTT_INTEGRATION.md` — MQTT machine-protocol integration details.
- `docs/SQL_BACKUP_AND_RESTORE.md` — PostgreSQL backup and restore workflow.
- `docs/EXPORT_PROCESSED_DATA.md` — processed-data CSV export workflow for model training.
- `docs/TRAINED_MODEL_DASHBOARD_INTEGRATION.md` — final dashboard-to-trained-model integration notes.
- `docs/reports/` — merge and test reports.

## Project status

This version is a final integration handoff build focused on incremental raw ingestion, optional MQTT integration, prediction checkpoints, trained XGBoost model inference from PostgreSQL, Streamlit dashboard execution/display, processed-data CSV export for model training, SQL backup/restore, and documentation clarity. The deterministic demo scorer remains available for compatibility, while the dashboard and tool prediction service run the trained model.
