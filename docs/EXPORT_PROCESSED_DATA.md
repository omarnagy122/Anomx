# Export Processed Data for Model Training

This workflow exports the `processed_sensor_data` table from PostgreSQL into a CSV file that can be handed to the model-training team.

## Output location

Default CSV output:

```text
exports/processed/processed_sensor_data.csv
```

The exported CSV is intentionally ignored by Git because training datasets can become large and should be generated locally from the database.

## Recommended flow

1. Start the main Docker Compose stack.
2. Ingest raw data through Kafka or MQTT.
3. Run the prediction pipeline so new raw rows become processed rows.
4. Export `processed_sensor_data` as CSV.
5. Share the generated CSV with the model-training teammate.

## Docker command

```powershell
docker compose run --rm data-exporter
```

## Local Python command

Use this when PostgreSQL is reachable on the host machine and Python dependencies are installed:

```powershell
python scripts/export_processed_data_to_csv.py
```

## Custom output name

```powershell
python scripts/export_processed_data_to_csv.py --output exports/processed/roman_training_data.csv
```

## Validation commands

Confirm processed rows exist before exporting:

```powershell
docker compose exec postgres psql -U anomx -d anomx_db -c "SELECT COUNT(*) FROM processed_sensor_data;"
```

Inspect the exported CSV quickly:

```powershell
python -c "import pandas as pd; df = pd.read_csv('exports/processed/processed_sensor_data.csv'); print(df.shape); print(df.head())"
```

## Notes

- If `processed_sensor_data` is empty, run the prediction pipeline first.
- The exporter writes CSV headers.
- The default export order is by the `id` column when it exists.
- The exporter reads the same PostgreSQL environment variables used by the rest of the project.
