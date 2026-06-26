from __future__ import annotations

import math
import os
import sys
from pathlib import Path
from typing import Any

os.environ['SPARK_HOME'] = str(Path(sys.executable).parent.parent / 'lib/python3.12/site-packages/pyspark')
os.environ['PYSPARK_PYTHON'] = sys.executable

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append(str(Path(__file__).resolve().parents[1]))

from config import (
    DATA_SOURCES, SENSOR_COLUMNS,
    USELESS_SENSOR_COLUMNS, FEATURE_SENSOR_COLUMNS,
    POSTGRES_HOST, POSTGRES_PORT,
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASS,
    POSTGRES_BATCH_SIZE
)

SELECTED_SENSOR_COLUMNS = [c for c in SENSOR_COLUMNS if c not in USELESS_SENSOR_COLUMNS]
FEATURE_COLUMNS = [
    feature
    for sensor in FEATURE_SENSOR_COLUMNS
    for feature in (f"rolling_avg_{sensor}", f"rolling_std_{sensor}", f"delta_{sensor}")
]
PROCESSED_COLUMNS = ["source_file", *SELECTED_SENSOR_COLUMNS, *FEATURE_COLUMNS, "rul"]
UNIQUE_COLUMNS = ["source_file", "engine_id", "time_in_cycles"]


def _connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS
    )


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def process_dataset(dataset="FD001"):
    spark = SparkSession.builder \
        .appName(f"AnomX-Processing-{dataset}") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    path = DATA_SOURCES["simulation"][dataset]
    pandas_df = pd.read_csv(path, sep=r'\s+', header=None, names=SENSOR_COLUMNS)
    df = spark.createDataFrame(pandas_df)

    df = df.withColumn("source_file", F.lit(dataset))
    df = df.dropDuplicates(UNIQUE_COLUMNS)

    numeric_cols = [c for c in SENSOR_COLUMNS if c not in ("engine_id", "time_in_cycles")]
    for col in numeric_cols:
        df = df.withColumn(col, F.coalesce(F.col(col).cast("double"), F.lit(0.0)))

    engine_window = Window.partitionBy("source_file", "engine_id")
    ordered_window = Window.partitionBy("source_file", "engine_id").orderBy("time_in_cycles")
    rolling_window = ordered_window.rowsBetween(-4, 0)

    df = df.withColumn("max_cycle", F.max("time_in_cycles").over(engine_window))
    df = df.withColumn("rul", (F.col("max_cycle") - F.col("time_in_cycles")).cast("int"))
    df = df.drop("max_cycle")

    for sensor in FEATURE_SENSOR_COLUMNS:
        df = df.withColumn(f"rolling_avg_{sensor}", F.avg(sensor).over(rolling_window))
        df = df.withColumn(f"rolling_std_{sensor}",
            F.coalesce(F.stddev(sensor).over(rolling_window), F.lit(0.0)))
        df = df.withColumn(f"delta_{sensor}",
            F.coalesce(F.col(sensor) - F.lag(sensor).over(ordered_window), F.lit(0.0)))

    df = df.drop(*USELESS_SENSOR_COLUMNS)
    df = df.fillna(0)
    df = df.select(*PROCESSED_COLUMNS).orderBy("source_file", "engine_id", "time_in_cycles")

    print(f"[{dataset}] Total rows: {df.count()}")
    df.show(5)

    pandas_out = df.toPandas()
    spark.stop()

    update_cols = [c for c in PROCESSED_COLUMNS if c not in UNIQUE_COLUMNS]
    update_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
    insert_cols = ", ".join(PROCESSED_COLUMNS)

    upsert_sql = f"""
        INSERT INTO processed_sensor_data ({insert_cols})
        VALUES %s
        ON CONFLICT (source_file, engine_id, time_in_cycles)
        DO UPDATE SET {update_sql}, processed_at = NOW();
    """

    rows = [
        tuple(_clean_value(row[c]) for c in PROCESSED_COLUMNS)
        for _, row in pandas_out.iterrows()
    ]

    with _connect() as conn:
        with conn.cursor() as cursor:
            execute_values(cursor, upsert_sql, rows, page_size=POSTGRES_BATCH_SIZE)
        conn.commit()

    print(f"[{dataset}] Saved {len(rows)} rows to PostgreSQL.")


if __name__ == "__main__":
    dataset = sys.argv[1] if len(sys.argv) > 1 else "FD001"
    process_dataset(dataset)
