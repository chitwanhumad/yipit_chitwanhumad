"""Silver layer: clean and conform bronze data."""

import duckdb
from pyspark.sql import SparkSession

from src.spark import load_config

config = load_config()


# Validate, type, and dedupe bronze into silver. Connections come from main.
def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    print("Silver: clean and conform bronze data")
