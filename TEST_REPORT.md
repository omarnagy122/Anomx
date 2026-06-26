# AnomX Incremental Fixed Test Report

## Scope
This version fixes the incremental prediction flow after the Docker run exposed two issues:

1. the producer image used by the user did not accept `--start-row`;
2. prediction failed because `processed_sensor_data.raw_id` received NULL in the running copy.

## Fixes verified

- `raw_id` is preserved from raw DB rows through cleaning, feature engineering, filtering, scoring, processed table inserts, prediction results, and alerts.
- `prediction_pipeline.py` reads only `raw_sensor_data.id > last_processed_raw_id`.
- If there are no new rows, the run finishes successfully with `raw_rows_used = 0` and does not save outputs.
- Checkpoint is updated only inside the same successful transaction that saves processed features/results/alerts.
- Producer supports `--start-row` for clean incremental tests.
- Docker image name changed to `anomx-incremental-fixed-app:latest` to avoid accidentally reusing stale `anomx-two-flow-*` images.
- Producer reads dataset files through bind mount `./data:/app/data:ro`; dataset files are not baked into the Docker image.

## Local checks run in this environment

```text
python -m compileall -q .        PASSED
python -m pytest -q              PASSED: 13 passed
python scripts/quick_local_demo_without_kafka.py  PASSED
YAML parse for docker-compose.yml and docker-compose.airflow.yml  PASSED
```

## Not run here
Docker containers were not run in this environment because Docker is unavailable here. The previous version was run successfully on the user's local Docker Desktop, and this package is designed to be tested there with a fresh rebuild.
