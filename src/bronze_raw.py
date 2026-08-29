"""Bronze layer: ingest raw source files with no business transforms."""

import json
from datetime import datetime
from pathlib import Path

import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from src.spark import get_path, load_config

config = load_config()
source_data = get_path("source_data", config)

# Schema evaluation. Define the columns that are expected in the bronze_articles table.
ARTICLE_COLUMNS = [
    "article_id",
    "title",
    "company_name",
    "published_date",
    "category",
    "revenue",
    "summary",
    "url",
    "author",
    "word_count",
]

# JSON object values for bronze_company_metadata. Company name is the object key.
COMPANY_VALUE_FIELDS = [
    "founded_year",
    "headquarters",
    "employee_count",
    "industry",
    "is_public",
    "stock_ticker",
]

COMPANY_SCHEMA = StructType(
    [
        StructField("company_name", StringType(), True),
        StructField("founded_year", IntegerType(), True),
        StructField("headquarters", StringType(), True),
        StructField("employee_count", IntegerType(), True),
        StructField("industry", StringType(), True),
        StructField("is_public", BooleanType(), True),
        StructField("stock_ticker", StringType(), True),
    ]
)


# Only CSV files are allowed for bronze_articles ingest and schema checks.
def _is_csv_file(path: Path | str) -> bool:
    name = path.name if isinstance(path, Path) else str(path)
    return name.lower().endswith(".csv")


def _is_json_file(path: Path | str) -> bool:
    name = path.name if isinstance(path, Path) else str(path)
    return name.lower().endswith(".json")


# Fail the pipeline if required bronze_articles columns are missing.
# Schema checks run on CSV files only (json and other types are ignored).
def _assert_article_schema(columns: list[str], file_name: str) -> None:
    if not _is_csv_file(file_name):
        return
    present = {col.strip() for col in columns}
    missing = [col for col in ARTICLE_COLUMNS if col not in present]
    if missing:
        raise ValueError(
            f"Schema mismatch for {file_name}. Missing columns: {missing}. "
            f"Found: {list(columns)}. Pipeline stopped."
        )


# Next batch_id is 1 on the first load, then MAX(batch_id) + 1 for each new run.
def _next_batch_id(conn: duckdb.DuckDBPyConnection) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(batch_id), 0) + 1 FROM bronze_articles"
    ).fetchone()
    return int(row[0])


# Add file_name, file mtime, load timestamp, and batch_id, then insert into bronze_articles.
# Reads CSV only; other files under source_data are skipped.
def _insert_articles(
    spark: SparkSession,
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    batch_id: int,
) -> None:
    if not _is_csv_file(csv_path):
        print(f"Bronze: skip {csv_path.name}, not a CSV file")
        return

    df = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", False)
        .load(str(csv_path))
    )
    _assert_article_schema(df.columns, csv_path.name)

    file_timestamp = datetime.fromtimestamp(csv_path.stat().st_mtime)

    # Skip insert when this exact file (name + mtime) was already loaded.
    # Same name with a newer timestamp is treated as a new file and will insert.
    already_loaded = conn.execute(
        """
        SELECT 1
        FROM bronze_articles
        WHERE file_name = $file_name
          AND file_timestamp = $file_timestamp
        LIMIT 1
        """,
        {
            "file_name": csv_path.name,
            "file_timestamp": file_timestamp,
        },
    ).fetchone()
    if already_loaded:
        print(
            f"Bronze: skip {csv_path.name} at {file_timestamp}, "
            "already loaded (avoid duplicates)"
        )
        return

    staged = (
        df.select(*ARTICLE_COLUMNS)
        .withColumn("file_name", lit(csv_path.name))
        .withColumn("file_timestamp", lit(file_timestamp))
        .withColumn("insert_datetime", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
    )
    # Collect the Spark DataFrame to pandas so DuckDB can register and INSERT it.
    pdf = staged.toPandas()
    conn.register("tmp_bronze_articles", pdf)
    conn.execute(
        """
        INSERT INTO bronze_articles
        SELECT
            article_id, title, company_name, published_date, category,
            revenue, summary, url, author, word_count,
            file_name, file_timestamp, insert_datetime, batch_id
        FROM tmp_bronze_articles
        """
    )
    conn.unregister("tmp_bronze_articles")
    print(
        f"Bronze: loaded {len(pdf)} rows from {csv_path.name} "
        f"into bronze_articles (batch_id={batch_id})"
    )


# Fail the pipeline if a company object is missing required value fields.
def _assert_company_schema(payload: object, file_name: str) -> None:
    if not _is_json_file(file_name):
        return
    if not isinstance(payload, dict) or not payload:
        raise ValueError(
            f"Schema mismatch for {file_name}. Expected an object keyed by company_name. "
            "Pipeline stopped."
        )
    for company_name, attrs in payload.items():
        if not isinstance(attrs, dict):
            raise ValueError(
                f"Schema mismatch for {file_name} company {company_name!r}: "
                "value must be an object. Pipeline stopped."
            )
        missing = [field for field in COMPANY_VALUE_FIELDS if field not in attrs]
        if missing:
            raise ValueError(
                f"Schema mismatch for {file_name} company {company_name!r}. "
                f"Missing fields: {missing}. Pipeline stopped."
            )


# Flatten company_name keys, add file metadata, insert into bronze_company_metadata.
# Reads JSON only. Same file_name + file_timestamp is skipped to avoid duplicates.
# New data for a company_name: set the prior current row to false, then insert is_current=true.
def _insert_company_metadata(
    spark: SparkSession,
    conn: duckdb.DuckDBPyConnection,
    json_path: Path,
) -> None:
    if not _is_json_file(json_path):
        print(f"Bronze: skip {json_path.name}, not a JSON file")
        return

    payload = json.loads(json_path.read_text())
    _assert_company_schema(payload, json_path.name)

    file_timestamp = datetime.fromtimestamp(json_path.stat().st_mtime)

    # Skip insert when this exact file (name + mtime) was already loaded.
    already_loaded = conn.execute(
        """
        SELECT 1
        FROM bronze_company_metadata
        WHERE file_name = $file_name
          AND file_timestamp = $file_timestamp
        LIMIT 1
        """,
        {
            "file_name": json_path.name,
            "file_timestamp": file_timestamp,
        },
    ).fetchone()
    if already_loaded:
        print(
            f"Bronze: skip {json_path.name} at {file_timestamp}, "
            "already loaded (avoid duplicates)"
        )
        return

    rows = [
        (
            company_name,
            attrs.get("founded_year"),
            attrs.get("headquarters"),
            attrs.get("employee_count"),
            attrs.get("industry"),
            attrs.get("is_public"),
            attrs.get("stock_ticker"),
        )
        for company_name, attrs in payload.items()
    ]
    staged = (
        spark.createDataFrame(rows, COMPANY_SCHEMA)
        .withColumn("file_name", lit(json_path.name))
        .withColumn("file_timestamp", lit(file_timestamp))
        .withColumn("insert_datetime", current_timestamp())
        .withColumn("is_current", lit(True))
    )
    # Collect the Spark DataFrame to pandas so DuckDB can register and INSERT it.
    pdf = staged.toPandas()
    conn.register("tmp_bronze_company_metadata", pdf)
    # Keep one current row per company_name: expire the previous current version first.
    conn.execute(
        """
        UPDATE bronze_company_metadata
        SET is_current = FALSE
        WHERE is_current = TRUE
          AND company_name IN (
              SELECT company_name FROM tmp_bronze_company_metadata
          )
        """
    )
    conn.execute(
        """
        INSERT INTO bronze_company_metadata
        SELECT
            company_name, founded_year, headquarters, employee_count,
            industry, is_public, stock_ticker,
            file_name, file_timestamp, insert_datetime, is_current
        FROM tmp_bronze_company_metadata
        """
    )
    conn.unregister("tmp_bronze_company_metadata")
    print(
        f"Bronze: loaded {len(pdf)} rows from {json_path.name} "
        "into bronze_company_metadata (is_current=true)"
    )


# Read landing CSVs and JSON from source_data into bronze. Connections come from main.
def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    print("### Data Ingetion starts #####")
    print(f"Bronze: raw ingest from {source_data}")
    csv_files = sorted(path for path in source_data.rglob("*.csv") if path.is_file())
    json_files = sorted(path for path in source_data.rglob("*.json") if path.is_file())
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under {source_data}. Pipeline stopped.")
    if not json_files:
        raise FileNotFoundError(f"No JSON files found under {source_data}. Pipeline stopped.")

    # One batch_id per pipeline run; skipped files do not consume a new id.
    batch_id = _next_batch_id(conn)
    for csv_path in csv_files:
        _insert_articles(spark, conn, csv_path, batch_id)
    for json_path in json_files:
        _insert_company_metadata(spark, conn, json_path)
    print("### Data Ingetion ends #####")
