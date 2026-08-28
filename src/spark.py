"""Spark session factory for local PySpark jobs."""

from __future__ import annotations

import configparser
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config" / "config.ini"


def load_config(path: Path = CONFIG_PATH) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    if not config.read(path):
        raise FileNotFoundError(f"Config not found: {path}")
    return config


def get_project_root(config: configparser.ConfigParser | None = None) -> Path:
    config = config or load_config()
    return Path(config["paths"]["PROJECT_ROOT"]).expanduser().resolve()


def get_path(name: str, config: configparser.ConfigParser | None = None) -> Path:
    """Resolve a [paths] entry relative to PROJECT_ROOT."""
    config = config or load_config()
    value = Path(config["paths"][name])
    if value.is_absolute():
        return value
    return get_project_root(config) / value


def apply_sql_file(conn, path: Path) -> None:
    sql = path.read_text()
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            conn.execute(statement)


def get_duckdb_connection():
    """Open the project DuckDB file and ensure dbscript tables exist."""
    import duckdb

    conn = duckdb.connect(str(get_path("duckdb_path")))
    apply_sql_file(conn, REPO_ROOT / "dbscript" / "alltables.sql")
    return conn


def get_spark_session(app_name: str | None = None) -> SparkSession:
    config = load_config()
    spark_cfg = config["spark"]
    python = sys.executable
    os.environ["PYSPARK_PYTHON"] = python
    os.environ["PYSPARK_DRIVER_PYTHON"] = python

    warehouse_dir = get_project_root(config) / spark_cfg.get(
        "warehouse_dir", "spark-warehouse"
    )

    session = (
        SparkSession.builder.appName(app_name or spark_cfg.get("app_name"))
        .master(spark_cfg.get("master", "local[*]"))
        .config("spark.pyspark.python", python)
        .config("spark.pyspark.driver.python", python)
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel(spark_cfg.get("log_level", "WARN"))
    return session
