# Known limitations

This handoff is a clean incremental pipeline version, not the final industrial ML product.

## What is production-safe in this version

- Kafka consumer stores raw sensor rows only.
- PostgreSQL deduplicates raw rows by `(source_file, engine_id, time_in_cycles)`.
- Prediction processing is incremental using `processing_checkpoints.last_processed_raw_id`.
- If no new rows arrive, prediction exits successfully with `raw_rows_used = 0`.
- Output writes and checkpoint movement happen in one transaction.

## What is still a demo placeholder

- `score_risk()` is deterministic demo scoring, not a trained AI model.
- `demo_rul` is only a C-MAPSS historical placeholder.
- The processing module is DB/Pandas-based; the `src/processing/spark_processor.py` wrapper keeps the interface ready for a future PySpark implementation.

## Operational note

When moving from an older ZIP to this one, reset the local Docker database unless you have applied the schema migration manually:

```powershell
docker compose down -v
```
