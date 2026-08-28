"""Bronze layer: ingest raw source files with no business transforms."""

from __future__ import annotations

import duckdb
from pyspark.sql import SparkSession

from src.spark import get_path


# Read landing files from source_data into bronze. Connections come from main.
def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    source_dir = get_path("source_data")
    print(f"Bronze: raw ingest from {source_dir}")
