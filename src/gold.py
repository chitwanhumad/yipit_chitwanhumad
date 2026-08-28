"""Gold layer: business-ready tables for reporting."""

import duckdb
from pyspark.sql import SparkSession

from src.spark import get_path, load_config

config = load_config()
output_data = get_path("output_data", config)


# Build gold aggregates and converted metrics. Connections come from main.
def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    print(f"Gold: publish business tables to {output_data}")
