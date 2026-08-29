"""Gold layer: business-ready tables for reporting."""

import duckdb
from pyspark.sql import SparkSession

from src.spark import get_path, load_config

config = load_config()
output_data = get_path("output_data", config)

_conn: duckdb.DuckDBPyConnection | None = None

GOLD_FACT_ARR_OBSERVATION_SQL = """
SELECT
    *,
    COALESCE(
        s.revenue_actual_usd,
        (s.revenue_min_usd + s.revenue_max_usd) / 2
    ) AS arr_usd,
    current_timestamp() AS gold_loaded_at
FROM silver_articles s
"""


def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    global _conn
    _conn = conn
    print(f"Gold: publish business tables to {output_data}")
    # Silver lives in DuckDB; Spark SQL can only query a registered view.
    spark.createDataFrame(
        conn.execute("SELECT * FROM silver_articles").df()
    ).createOrReplaceTempView("silver_articles")
    # Spark default catalog cannot CREATE OR REPLACE TABLE, so run a SELECT only.
    gold_df = spark.sql(GOLD_FACT_ARR_OBSERVATION_SQL)
    # Same result in DuckDB so the warehouse file can be queried in DBeaver.
    gold_pdf = gold_df.toPandas()
    conn.register("tmp_gold_fact_arr_observation", gold_pdf)
    conn.execute(
        """
        CREATE OR REPLACE TABLE gold_fact_arr_observation AS
        SELECT * FROM tmp_gold_fact_arr_observation
        """
    )
    conn.unregister("tmp_gold_fact_arr_observation")
    print("Gold: gold_fact_arr_observation created")
    export_gold_table()


# Write current gold table to CSV under output_data.
def export_gold_table() -> None:
    output_data.mkdir(parents=True, exist_ok=True)
    _conn.execute("SELECT * FROM gold_fact_arr_observation").df().to_csv(
        output_data / "ai_articles_enriched.csv", index=False
    )
    print("Gold: file has been exported")
