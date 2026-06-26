from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_consumer_does_not_run_heavy_processing_or_prediction():
    consumer_source = (PROJECT_ROOT / "ingestion" / "kafka_consumer.py").read_text(encoding="utf-8").lower()
    forbidden_tokens = ["pyspark", "spark", "run_prediction", "prediction_pipeline", "build_features"]
    assert not any(token in consumer_source for token in forbidden_tokens)


def test_prediction_pipeline_reads_incrementally_from_raw_table_not_cmapss_file():
    prediction_source = (PROJECT_ROOT / "prediction" / "prediction_pipeline.py").read_text(encoding="utf-8").lower()
    assert "from raw_sensor_data" in prediction_source
    assert "where id >" in prediction_source
    assert "processing_checkpoints" in prediction_source
    assert "read_csv" not in prediction_source
    assert "train_fd001" not in prediction_source


def test_schema_contains_required_tables_and_incremental_keys():
    sql = (PROJECT_ROOT / "db" / "init.sql").read_text(encoding="utf-8").lower()
    for table in [
        "raw_sensor_data",
        "processing_checkpoints",
        "processed_sensor_data",
        "prediction_runs",
        "prediction_results",
        "alerts",
    ]:
        assert f"create table if not exists {table}" in sql
    assert "unique (source_file, engine_id, time_in_cycles)" in sql
    assert "unique (raw_id, model_version)" in sql
    assert "last_processed_raw_id" in sql
    assert "from_raw_id" in sql
    assert "to_raw_id" in sql


def test_airflow_runs_incremental_prediction_hourly():
    dag_source = (PROJECT_ROOT / "airflow" / "dags" / "anomx_prediction_dags.py").read_text(encoding="utf-8").lower()
    assert 'schedule="@hourly"' in dag_source
    assert "hourly_incremental" in dag_source
    assert "anomx_incremental_prediction_pipeline" in dag_source
