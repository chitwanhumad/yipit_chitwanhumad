"""Entry point: start connections and run bronze → silver → gold."""

import sys
from datetime import datetime

import duckdb
from pyspark.sql import SparkSession

from src.bronze_raw import run as run_bronze
from src.gold import run as run_gold
from src.silver import run as run_silver
from src.spark import get_duckdb_connection, get_spark_session


# Run medallion layers in order. Spark and DuckDB are opened once in main.
def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    print(f"The session starts at {datetime.now()}")
    run_bronze(spark, conn)
    run_silver(spark, conn)
    run_gold(spark, conn)


# Open Spark and DuckDB, run the pipeline, then close both connections.
def main() -> None:
    spark = get_spark_session()
    conn = get_duckdb_connection()
    try:
        run(spark, conn)
        print("Pipeline executed successfully")
    except ValueError as exc:
        message = str(exc)
        if "Schema mismatch" in message:
            print(f"ERROR: {message}")
            print("Pipeline stopped.")
            sys.exit(1)
        raise
    finally:
        conn.close()
        spark.stop()


if __name__ == "__main__":
    main()
