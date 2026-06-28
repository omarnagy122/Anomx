# AnomX clean architecture

## Flow 1: raw ingestion

```text
producer/factory sensor -> Kafka topic sensor-data -> src/ingestion/kafka_consumer.py -> raw_sensor_data
```

Rules:

- Consumer writes raw rows only.
- Consumer commits Kafka offsets only after PostgreSQL commit.
- PostgreSQL deduplicates raw rows by `(source_file, engine_id, time_in_cycles)`.
- Duplicate Kafka messages are consumed but not inserted again.

## Flow 2: incremental processing and prediction

```text
manual/Airflow @hourly -> src/prediction/prediction_pipeline.py
    -> read processing_checkpoints.last_processed_raw_id
    -> read raw rows where id > checkpoint
    -> read small historical context rows per engine/source
    -> clean/build features
    -> save processed rows for new raw rows only
    -> save latest prediction per engine/source for the new batch
    -> save alerts
    -> update checkpoint after success
```

## Transaction rule

The following happen in one database transaction:

```text
processed_sensor_data write
prediction_results write
alerts write
processing_checkpoints update
prediction_runs SUCCESS update
```

If any output write fails, the checkpoint is not moved.

## Why context rows exist

Rolling features and deltas need a small number of old rows. The pipeline reads old context rows only for feature calculation. It does not save predictions for context rows.
