"""Silver layer: clean and conform bronze data."""

from __future__ import annotations

import duckdb
from pyspark.sql import SparkSession

from src.spark import get_path


# Validate, type, and dedupe bronze into silver. Connections come from main.
def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    output_dir = get_path("output_data")
    print(f"Silver: clean and conform data toward {output_dir}")
