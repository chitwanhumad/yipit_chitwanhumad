"""Gold layer: business-ready tables for reporting."""

from __future__ import annotations

import duckdb
from pyspark.sql import SparkSession

from src.spark import get_path


# Build gold aggregates and converted metrics. Connections come from main.
def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    output_dir = get_path("output_data")
    print(f"Gold: publish business tables to {output_dir}")
