import sys
import os

os.environ['SPARK_HOME'] = '/media/data/omar/programming course/DEPI/DEPI project/anomx/venv/lib/python3.12/site-packages/pyspark'
os.environ['PYSPARK_PYTHON'] = '/media/data/omar/programming course/DEPI/DEPI project/anomx/venv/bin/python3'

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from config import (
    DATA_SOURCES, SENSOR_COLUMNS,
    POSTGRES_HOST, POSTGRES_PORT,
    POSTGRES_DB, POSTGRES_USER, POSTGRES_PASS
)

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

    window_max = Window.partitionBy("engine_id")
    df = df.withColumn("max_cycle", F.max("time_in_cycles").over(window_max))
    df = df.withColumn("rul", F.col("max_cycle") - F.col("time_in_cycles"))
    df = df.drop("max_cycle")

    window_roll = Window.partitionBy("engine_id").orderBy("time_in_cycles").rowsBetween(-4, 0)
    df = df.withColumn("rolling_avg_s2", F.avg("s2").over(window_roll))
    df = df.withColumn("rolling_std_s2", F.stddev("s2").over(window_roll))
    df = df.fillna(0)

    print(f"[{dataset}] Total rows: {df.count()}")
    df.show(5)

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        database=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASS
    )
    cursor = conn.cursor()

    pandas_out = df.toPandas()
    records = [tuple(row) for _, row in pandas_out.iterrows()]
    cols = pandas_out.columns.tolist()
    placeholders = ','.join(['%s'] * len(cols))
    col_names = ','.join(cols)

    cursor.executemany(
        f"INSERT INTO processed_sensor_data ({col_names}) VALUES ({placeholders})",
        records
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[{dataset}] Saved to PostgreSQL successfully.")
    spark.stop()

if __name__ == "__main__":
    dataset = sys.argv[1] if len(sys.argv) > 1 else "FD001"
    process_dataset(dataset)
