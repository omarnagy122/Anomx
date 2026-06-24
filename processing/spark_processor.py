from __future__ import annotations

import math
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config import (  # noqa: E402
    FEATURE_SENSOR_COLUMNS,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    SENSOR_COLUMNS,
    USELESS_SENSOR_COLUMNS,
)

RAW_COLUMNS = ["source_file", *SENSOR_COLUMNS]
SELECTED_SENSOR_COLUMNS = [column for column in SENSOR_COLUMNS if column not in USELESS_SENSOR_COLUMNS]
BASE_PROCESSED_COLUMNS = ["source_file", *SELECTED_SENSOR_COLUMNS]
FEATURE_COLUMNS = [
    feature
    for sensor in FEATURE_SENSOR_COLUMNS
    for feature in (f"rolling_avg_{sensor}", f"rolling_std_{sensor}", f"delta_{sensor}")
]
PROCESSED_COLUMNS = [*BASE_PROCESSED_COLUMNS, *FEATURE_COLUMNS, "rul"]
UNIQUE_COLUMNS = ["source_file", "engine_id", "time_in_cycles"]

CREATE_PROCESSED_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS processed_sensor_data (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL DEFAULT 'FD001',
    engine_id INTEGER NOT NULL,
    time_in_cycles INTEGER NOT NULL,
    op_setting_1 DOUBLE PRECISION,
    op_setting_2 DOUBLE PRECISION,
    op_setting_3 DOUBLE PRECISION,
    s2 DOUBLE PRECISION,
    s3 DOUBLE PRECISION,
    s4 DOUBLE PRECISION,
    s7 DOUBLE PRECISION,
    s8 DOUBLE PRECISION,
    s9 DOUBLE PRECISION,
    s11 DOUBLE PRECISION,
    s12 DOUBLE PRECISION,
    s13 DOUBLE PRECISION,
    s14 DOUBLE PRECISION,
    s15 DOUBLE PRECISION,
    s17 DOUBLE PRECISION,
    s20 DOUBLE PRECISION,
    s21 DOUBLE PRECISION,
    rolling_avg_s2 DOUBLE PRECISION,
    rolling_std_s2 DOUBLE PRECISION,
    delta_s2 DOUBLE PRECISION,
    rolling_avg_s3 DOUBLE PRECISION,
    rolling_std_s3 DOUBLE PRECISION,
    delta_s3 DOUBLE PRECISION,
    rolling_avg_s4 DOUBLE PRECISION,
    rolling_std_s4 DOUBLE PRECISION,
    delta_s4 DOUBLE PRECISION,
    rolling_avg_s7 DOUBLE PRECISION,
    rolling_std_s7 DOUBLE PRECISION,
    delta_s7 DOUBLE PRECISION,
    rolling_avg_s11 DOUBLE PRECISION,
    rolling_std_s11 DOUBLE PRECISION,
    delta_s11 DOUBLE PRECISION,
    rolling_avg_s12 DOUBLE PRECISION,
    rolling_std_s12 DOUBLE PRECISION,
    delta_s12 DOUBLE PRECISION,
    rolling_avg_s15 DOUBLE PRECISION,
    rolling_std_s15 DOUBLE PRECISION,
    delta_s15 DOUBLE PRECISION,
    rolling_avg_s20 DOUBLE PRECISION,
    rolling_std_s20 DOUBLE PRECISION,
    delta_s20 DOUBLE PRECISION,
    rolling_avg_s21 DOUBLE PRECISION,
    rolling_std_s21 DOUBLE PRECISION,
    delta_s21 DOUBLE PRECISION,
    rul INTEGER NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT processed_sensor_data_unique_row UNIQUE (source_file, engine_id, time_in_cycles)
);

CREATE INDEX IF NOT EXISTS idx_processed_sensor_data_source_engine_cycle
    ON processed_sensor_data (source_file, engine_id, time_in_cycles);
"""


def _connect_postgres():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", POSTGRES_HOST),
        port=int(os.getenv("POSTGRES_PORT", str(POSTGRES_PORT))),
        database=os.getenv("POSTGRES_DB", POSTGRES_DB),
        user=os.getenv("POSTGRES_USER", POSTGRES_USER),
        password=os.getenv("POSTGRES_PASSWORD", POSTGRES_PASSWORD),
    )


def _build_spark() -> SparkSession:
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "2g")

    spark = (
        SparkSession.builder
        .appName("AnomX-PySpark-Processing")
        .master(spark_master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def ensure_processed_table() -> None:
    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_PROCESSED_TABLE_SQL)
        conn.commit()


def load_raw_data() -> pd.DataFrame:
    query = f"""
        SELECT {", ".join(RAW_COLUMNS)}
        FROM raw_sensor_data
        ORDER BY source_file, engine_id, time_in_cycles;
    """
    with _connect_postgres() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=RAW_COLUMNS)


def transform_with_spark(spark: SparkSession, raw_pdf: pd.DataFrame):
    if raw_pdf.empty:
        raise RuntimeError("raw_sensor_data is empty. Run Kafka producer and consumer before Spark processing.")

    df = spark.createDataFrame(raw_pdf)

    df = df.dropDuplicates(UNIQUE_COLUMNS)

    df = df.withColumn("source_file", F.col("source_file").cast("string"))
    df = df.withColumn("engine_id", F.col("engine_id").cast("int"))
    df = df.withColumn("time_in_cycles", F.col("time_in_cycles").cast("int"))

    numeric_columns = [column for column in SENSOR_COLUMNS if column not in ("engine_id", "time_in_cycles")]
    for column in numeric_columns:
        df = df.withColumn(column, F.col(column).cast("double"))

    # Basic cleaning: keep deterministic unique rows, fill missing sensor values, remove constant/low-value sensors.
    for column in numeric_columns:
        df = df.withColumn(column, F.coalesce(F.col(column), F.lit(0.0)))

    engine_window = Window.partitionBy("source_file", "engine_id")
    ordered_window = Window.partitionBy("source_file", "engine_id").orderBy("time_in_cycles")
    rolling_window = ordered_window.rowsBetween(-4, 0)

    # RUL for training data: max cycle per engine - current cycle.
    df = df.withColumn("max_cycle", F.max("time_in_cycles").over(engine_window))
    df = df.withColumn("rul", (F.col("max_cycle") - F.col("time_in_cycles")).cast("int"))
    df = df.drop("max_cycle")

    # Rolling statistics and inter-reading deltas for ML-ready features.
    for sensor in FEATURE_SENSOR_COLUMNS:
        df = df.withColumn(f"rolling_avg_{sensor}", F.avg(sensor).over(rolling_window))
        df = df.withColumn(
            f"rolling_std_{sensor}",
            F.coalesce(F.stddev(sensor).over(rolling_window), F.lit(0.0)),
        )
        df = df.withColumn(
            f"delta_{sensor}",
            F.coalesce(F.col(sensor) - F.lag(sensor).over(ordered_window), F.lit(0.0)),
        )

    df = df.drop(*USELESS_SENSOR_COLUMNS)
    df = df.fillna(0)
    return df.select(*PROCESSED_COLUMNS).orderBy("source_file", "engine_id", "time_in_cycles")


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def save_processed_data(processed_pdf: pd.DataFrame) -> int:
    if processed_pdf.empty:
        raise RuntimeError("No processed rows generated by Spark.")

    update_columns = [column for column in PROCESSED_COLUMNS if column not in UNIQUE_COLUMNS]
    insert_columns_sql = ", ".join(PROCESSED_COLUMNS)
    update_sql = ", ".join([f"{column} = EXCLUDED.{column}" for column in update_columns])

    upsert_sql = f"""
        INSERT INTO processed_sensor_data ({insert_columns_sql})
        VALUES %s
        ON CONFLICT (source_file, engine_id, time_in_cycles)
        DO UPDATE SET
            {update_sql},
            processed_at = NOW();
    """

    rows = [
        tuple(_clean_value(row[column]) for column in PROCESSED_COLUMNS)
        for _, row in processed_pdf.iterrows()
    ]
    page_size = int(os.getenv("POSTGRES_BATCH_SIZE", "500"))

    with _connect_postgres() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, upsert_sql, rows, page_size=page_size)
        conn.commit()

    return len(rows)


def run_processing() -> int:
    ensure_processed_table()
    raw_pdf = load_raw_data()
    print(f"[spark] Loaded raw rows from PostgreSQL: {len(raw_pdf)}")

    spark = _build_spark()
    try:
        processed_df = transform_with_spark(spark, raw_pdf)
        processed_count = processed_df.count()
        print(f"[spark] Spark transformed rows: {processed_count}")
        processed_df.show(5, truncate=False)

        processed_pdf = processed_df.toPandas()
    finally:
        spark.stop()

    saved_count = save_processed_data(processed_pdf)
    print(f"[spark] Done — upserted {saved_count} rows into processed_sensor_data.")
    return saved_count


if __name__ == "__main__":
    run_processing()
