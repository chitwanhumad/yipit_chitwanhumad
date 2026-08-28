"""Entry point: start a local Spark session and DuckDB connection."""

from datetime import datetime

import duckdb
from pyspark.sql import SparkSession

from src.bin._currency_conversion import get_usd_arr
from src.spark import get_duckdb_connection, get_spark_session


# Print when the session started. DuckDB conn is available for later queries.
def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    print(f"The session starts at {datetime.now()}")


# Open Spark and DuckDB, run the pipeline, then close both connections.
def main() -> None:
    spark = get_spark_session()
    conn = get_duckdb_connection()
    try:
        run(spark, conn)
        result = conn.sql("SELECT 'DuckDB connection successful' as Output").fetchall()
        print(result[0][0])
        print("EUR 100 ->", get_usd_arr(conn, "SSS", 100))
    finally:
        conn.close()
        spark.stop()


if __name__ == "__main__":
    main()
