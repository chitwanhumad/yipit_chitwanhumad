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
    export_gold_use_case_ai_arr()


# Write current gold table to CSV under output_data.
def export_gold_table() -> None:
    output_data.mkdir(parents=True, exist_ok=True)
    _conn.execute("SELECT * FROM gold_fact_arr_observation").df().to_csv(
        output_data / "ai_articles_enriched.csv", index=False
    )
    print("Gold: file has been exported")


# AI/ML articles, 2022-2024, valid ARR over $50M USD.
GOLD_USE_CASE_AI_ARR_SQL = """
SELECT *
FROM gold_fact_arr_observation
WHERE (
        category_group = 'AI/ML'
        OR lower(COALESCE(category_name, '')) IN (
            'machine learning',
            'ai/ml',
            'ai & ml',
            'artificial intelligence'
        )
        OR lower(COALESCE(industry, '')) IN (
            'ai/ml',
            'machine learning',
            'artificial intelligence'
        )
    )
  AND year BETWEEN 2022 AND 2024
  AND arr_usd IS NOT NULL
  AND arr_usd > 50000000
"""


def export_gold_use_case_ai_arr() -> None:
    output_data.mkdir(parents=True, exist_ok=True)
    csv_path = output_data / "ai_ml_arr_gt_50m_2022_2024.csv"
    pdf = _conn.execute(GOLD_USE_CASE_AI_ARR_SQL).df()
    pdf.to_csv(csv_path, index=False)
    print(f"Gold: file has been exported ({len(pdf)} rows)")
