"""Silver layer: clean and conform bronze data."""

import duckdb
from pyspark.sql import SparkSession

from src.bin._currency_conversion import get_usd_arr
from src.bin._date_parser import parse_date
from src.bin._revenue_parser import normalize_revenue
from src.bin._revenue_range import get_revenue_range_id
from src.spark import load_config

config = load_config()

# Pipeline DuckDB connection from main.run(); set in run().
_conn: duckdb.DuckDBPyConnection | None = None


# Next company batch: 1 on first load, then max of insert/update batch + 1.
def _next_company_batch_id() -> int:
    row = _conn.execute(
        """
        SELECT COALESCE(
            MAX(GREATEST(COALESCE(insert_batch_id, 0), COALESCE(updated_batch_id, 0))),
            0
        ) + 1
        FROM dim_company
        """
    ).fetchone()
    return int(row[0])


# Upsert dim_company from the current bronze_company_metadata row per company_name.
def _build_dim_company() -> None:
    print("### Silver dim_company merge starts #####")
    batch_id = _next_company_batch_id()
    _conn.execute(
        """
        MERGE INTO dim_company AS tgt
        USING (
            SELECT
                CASE
                    WHEN d.id IS NULL THEN
                        (SELECT COALESCE(MAX(id), 0) FROM dim_company)
                        + ROW_NUMBER() OVER (
                            PARTITION BY (d.id IS NULL)
                            ORDER BY b.company_name
                        )
                    ELSE d.id
                END AS id,
                b.company_name,
                b.founded_year,
                b.headquarters,
                b.employee_count,
                b.industry,
                b.is_public,
                b.stock_ticker
            FROM bronze_company_metadata b
            LEFT JOIN dim_company d
                ON d.company_name = b.company_name
            WHERE b.is_current = TRUE
        ) AS src
        ON tgt.company_name = src.company_name
        WHEN MATCHED THEN UPDATE SET
            founded_year = src.founded_year,
            headquarters = src.headquarters,
            employee_count = src.employee_count,
            industry = src.industry,
            is_public = src.is_public,
            stock_ticker = src.stock_ticker,
            updated_batch_id = $batch_id,
            updated_at = CURRENT_TIMESTAMP
        WHEN NOT MATCHED THEN INSERT (
            id, company_name, founded_year, headquarters, employee_count,
            industry, is_public, stock_ticker,
            insert_batch_id, updated_batch_id, inserted_at, updated_at
        ) VALUES (
            src.id,
            src.company_name,
            src.founded_year,
            src.headquarters,
            src.employee_count,
            src.industry,
            src.is_public,
            src.stock_ticker,
            $batch_id,
            NULL,
            CURRENT_TIMESTAMP,
            NULL
        )
        """,
        {"batch_id": batch_id},
    )
    counts = _conn.execute(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE updated_batch_id IS NULL) AS inserted_only
        FROM dim_company
        """
    ).fetchone()
    print(
        f"Silver: dim_company merge done (batch_id={batch_id}, "
        f"total={counts[0]}, never_updated={counts[1]})"
    )
    print("### Silver dim_company merge ends #####")


# Register parse_date so SQL can turn bronze date strings into DATE or NULL.
def _register_parse_date() -> None:
    try:
        _conn.remove_function("parse_date")
    except Exception:
        pass
    _conn.create_function(
        "parse_date",
        parse_date,
        return_type="DATE",
        null_handling="special",
    )


# Unpack normalize_revenue: [currency, actual, min, max] -> typed SQL struct.
def parse_revenue(revenue: str | None) -> tuple:
    parts = normalize_revenue(revenue)
    if not parts:
        return (None, None, None, None)
    padded = list(parts) + [None, None, None, None]
    currency, actual, min_value, max_value = padded[:4]
    return (
        currency,
        None if actual is None else float(actual),
        None if min_value is None else (
            min_value if isinstance(min_value, str) else str(min_value)
        ),
        None if max_value is None else (
            max_value if isinstance(max_value, str) else str(max_value)
        ),
    )


# Register parse_revenue so SQL can fill the four normalized revenue columns.
def _register_parse_revenue() -> None:
    try:
        _conn.remove_function("parse_revenue")
    except Exception:
        pass
    _conn.create_function(
        "parse_revenue",
        parse_revenue,
        return_type=(
            "STRUCT(revenue_currency VARCHAR, revenue_actual FLOAT, "
            "revenue_min_currency VARCHAR, revenue_max_currency VARCHAR)"
        ),
        null_handling="special",
    )


def _as_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# Re-parse current-row revenue_as_source into the four normalized columns.
def _update_revenue_parsed() -> None:
    rows = _conn.execute(
        """
        SELECT observation_id, revenue_as_source
        FROM _build_silver_articles
        WHERE is_current = TRUE
        """
    ).fetchall()
    parsed = [
        (observation_id, *parse_revenue(revenue_as_source))
        for observation_id, revenue_as_source in rows
    ]
    _conn.execute("DROP TABLE IF EXISTS tmp_revenue_parsed")
    _conn.execute(
        """
        CREATE TEMP TABLE tmp_revenue_parsed (
            observation_id INTEGER,
            revenue_currency VARCHAR,
            revenue_actual FLOAT,
            revenue_min_currency VARCHAR,
            revenue_max_currency VARCHAR
        )
        """
    )
    if parsed:
        _conn.executemany(
            "INSERT INTO tmp_revenue_parsed VALUES (?, ?, ?, ?, ?)",
            parsed,
        )
    _conn.execute(
        """
        UPDATE _build_silver_articles
        SET
            revenue_currency = src.revenue_currency,
            revenue_actual = src.revenue_actual,
            revenue_min_currency = src.revenue_min_currency,
            revenue_max_currency = src.revenue_max_currency,
            updated_at = CURRENT_TIMESTAMP
        FROM tmp_revenue_parsed src
        WHERE _build_silver_articles.observation_id = src.observation_id
        """
    )
    _conn.execute("DROP TABLE IF EXISTS tmp_revenue_parsed")


# Call get_usd_arr; USD is 1:1 when dim_currency_conversion has no USD row.
def _to_usd(currency: str | None, amount: float | None) -> float | None:
    if currency is None or amount is None:
        return None
    result = get_usd_arr(_conn, str(currency), float(amount))
    if isinstance(result, dict) and result.get("success") is not None:
        return float(result["success"])
    if str(currency).upper() == "USD":
        return round(float(amount), 2)
    return None


# Convert current-row revenue amounts to USD via get_usd_arr.
def _update_revenue_usd() -> None:
    rows = _conn.execute(
        """
        SELECT
            observation_id,
            revenue_currency,
            revenue_actual,
            revenue_min_currency,
            revenue_max_currency
        FROM _build_silver_articles
        WHERE is_current = TRUE
        """
    ).fetchall()
    converted = [
        (
            observation_id,
            _to_usd(currency, actual),
            _to_usd(currency, _as_float(min_currency)),
            _to_usd(currency, _as_float(max_currency)),
        )
        for observation_id, currency, actual, min_currency, max_currency in rows
    ]
    _conn.execute("DROP TABLE IF EXISTS tmp_revenue_usd")
    _conn.execute(
        """
        CREATE TEMP TABLE tmp_revenue_usd (
            observation_id INTEGER,
            revenue_actual_usd FLOAT,
            revenue_min_actual_usd FLOAT,
            revenue_max_actual_usd FLOAT
        )
        """
    )
    if converted:
        _conn.executemany(
            "INSERT INTO tmp_revenue_usd VALUES (?, ?, ?, ?)",
            converted,
        )
    _conn.execute(
        """
        UPDATE _build_silver_articles
        SET
            revenue_actual_usd = src.revenue_actual_usd,
            revenue_min_actual_usd = src.revenue_min_actual_usd,
            revenue_max_actual_usd = src.revenue_max_actual_usd,
            updated_at = CURRENT_TIMESTAMP
        FROM tmp_revenue_usd src
        WHERE _build_silver_articles.observation_id = src.observation_id
        """
    )
    _conn.execute("DROP TABLE IF EXISTS tmp_revenue_usd")


# Assign dim_revenue_range.range_id from the USD amounts on current rows.
def _update_revenue_range_id() -> None:
    rows = _conn.execute(
        """
        SELECT
            observation_id,
            revenue_actual_usd,
            revenue_min_actual_usd,
            revenue_max_actual_usd
        FROM _build_silver_articles
        WHERE is_current = TRUE
        """
    ).fetchall()
    assigned = [
        (
            observation_id,
            get_revenue_range_id(
                _conn,
                [actual_usd, min_usd, max_usd],
            ),
        )
        for observation_id, actual_usd, min_usd, max_usd in rows
    ]
    _conn.execute("DROP TABLE IF EXISTS tmp_revenue_range")
    _conn.execute(
        """
        CREATE TEMP TABLE tmp_revenue_range (
            observation_id INTEGER,
            revenue_range_id INTEGER
        )
        """
    )
    if assigned:
        _conn.executemany(
            "INSERT INTO tmp_revenue_range VALUES (?, ?)",
            assigned,
        )
    _conn.execute(
        """
        UPDATE _build_silver_articles
        SET
            revenue_range_id = src.revenue_range_id,
            updated_at = CURRENT_TIMESTAMP
        FROM tmp_revenue_range src
        WHERE _build_silver_articles.observation_id = src.observation_id
        """
    )
    _conn.execute("DROP TABLE IF EXISTS tmp_revenue_range")


# Fill unmatched company_id via normalize_company_name; mark company_metched Fuzzy.
def _company_fuzzy_match() -> None:
    _conn.execute(
        """
        UPDATE _build_silver_articles AS bs
        SET
            company_id = src.id,
            company_metched = 'Fuzzy',
            updated_at = CURRENT_TIMESTAMP
        FROM (
            SELECT
                a.article_id,
                d.id
            FROM bronze_articles a
            JOIN dim_company d
                ON (
                    normalize_company_name(d.company_name)
                    = normalize_company_name(a.company_name)
                    OR normalize_company_compact(d.company_name)
                    = normalize_company_compact(a.company_name)
                    OR len(list_intersect(
                        company_name_aliases(a.company_name),
                        company_name_aliases(d.company_name)
                    )) > 0
                    OR (
                        length(normalize_company_compact(a.company_name)) >= 4
                        AND length(normalize_company_compact(d.company_name)) >= 4
                        AND (
                            position(
                                normalize_company_compact(a.company_name)
                                IN normalize_company_compact(d.company_name)
                            ) > 0
                            OR position(
                                normalize_company_compact(d.company_name)
                                IN normalize_company_compact(a.company_name)
                            ) > 0
                        )
                    )
                )
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY a.article_id
                ORDER BY
                    CASE
                        WHEN normalize_company_name(d.company_name)
                             = company_name_aliases(a.company_name)[-1]
                        THEN 0
                        ELSE 1
                    END,
                    d.id
            ) = 1
        ) AS src
        WHERE bs.company_id IS NULL
          AND bs.article_id = src.article_id
          AND bs.is_current = TRUE
        """
    )
    _conn.execute(
        """
        UPDATE _build_silver_articles AS bs
        SET
            company_metched = a.company_name,
            updated_at = CURRENT_TIMESTAMP
        FROM bronze_articles a
        WHERE bs.company_id IS NULL
          AND bs.is_current = TRUE
          AND bs.article_id = a.article_id
          AND a.batch_id = bs.insert_batch_id
        """
    )


# Build _build_silver_articles from bronze_articles and current dimension keys.
# article_ids in this run: expire prior current rows, then insert is_current=true.
# article_ids not in this run: leave is_current unchanged.
def _build_silver_articles() -> None:
    print("### Silver _build_silver_articles table starts #####")
    _register_parse_date()
    _register_parse_revenue()
    _conn.execute(
        """
        CREATE OR REPLACE TEMP TABLE tmp_build_silver_articles AS
        SELECT
            (SELECT COALESCE(MAX(observation_id), 0) FROM _build_silver_articles)
                + ROW_NUMBER() OVER (ORDER BY src.article_id, src.batch_id) AS observation_id,
            src.article_id,
            src.title,
            src.company_id,
            src.company_metched,
            src.published_date_as_source,
            src.published_date,
            src.category_id,
            src.revenue_as_source,
            src.rev.revenue_currency AS revenue_currency,
            src.rev.revenue_actual AS revenue_actual,
            CAST(NULL AS FLOAT) AS revenue_actual_usd,
            src.rev.revenue_min_currency AS revenue_min_currency,
            CAST(NULL AS FLOAT) AS revenue_min_actual_usd,
            src.rev.revenue_max_currency AS revenue_max_currency,
            CAST(NULL AS FLOAT) AS revenue_max_actual_usd,
            CAST(NULL AS INTEGER) AS revenue_range_id,
            src.summary,
            src.url,
            src.author,
            src.word_count,
            src.batch_id AS insert_batch_id,
            src.batch_id AS updated_batch_id
        FROM (
            SELECT
                b.article_id,
                b.title,
                c.id AS company_id,
                CASE WHEN c.id IS NOT NULL THEN 'Exact' END AS company_metched,
                b.published_date AS published_date_as_source,
                parse_date(b.published_date) AS published_date,
                cat.id AS category_id,
                b.revenue AS revenue_as_source,
                parse_revenue(b.revenue) AS rev,
                b.summary,
                b.url,
                b.author,
                CAST(TRY_CAST(b.word_count AS DOUBLE) AS INTEGER) AS word_count,
                b.batch_id
            FROM bronze_articles b
            LEFT JOIN dim_company c
                ON c.company_name = b.company_name
            LEFT JOIN dim_category cat
                ON cat.name = b.category
            WHERE NOT EXISTS (
                SELECT 1
                FROM _build_silver_articles s
                WHERE s.article_id = b.article_id
                  AND s.insert_batch_id = b.batch_id
            )
        ) AS src
        """
    )
    # Expire prior current versions by observation_id; ids not in this run are left alone.
    _conn.execute(
        """
        UPDATE _build_silver_articles
        SET is_current = FALSE
        WHERE observation_id IN (
            SELECT s.observation_id
            FROM _build_silver_articles s
            WHERE s.is_current = TRUE
              AND s.article_id IN (SELECT article_id FROM tmp_build_silver_articles)
        )
        """
    )
    _conn.execute(
        """
        INSERT INTO _build_silver_articles (
            observation_id, article_id, title, company_id, company_metched,
            published_date_as_source, published_date, category_id,
            revenue_as_source, revenue_currency, revenue_actual, revenue_actual_usd,
            revenue_min_currency, revenue_min_actual_usd,
            revenue_max_currency, revenue_max_actual_usd,
            revenue_range_id,
            summary, url, author, word_count,
            insert_batch_id, updated_batch_id, inserted_at, updated_at, is_current
        )
        SELECT
            observation_id, article_id, title, company_id, company_metched,
            published_date_as_source, published_date, category_id,
            revenue_as_source, revenue_currency, revenue_actual, revenue_actual_usd,
            revenue_min_currency, revenue_min_actual_usd,
            revenue_max_currency, revenue_max_actual_usd,
            revenue_range_id,
            summary, url, author, word_count,
            insert_batch_id, updated_batch_id,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE
        FROM tmp_build_silver_articles
        """
    )
    inserted = _conn.execute("SELECT COUNT(*) FROM tmp_build_silver_articles").fetchone()[0]
    _company_fuzzy_match()
    _update_revenue_parsed()
    _update_revenue_usd()
    _update_revenue_range_id()

    current = _conn.execute(
        "SELECT COUNT(*) FROM _build_silver_articles WHERE is_current = TRUE"
    ).fetchone()[0]
    _conn.execute("DROP TABLE IF EXISTS tmp_build_silver_articles")
    print(
        f"Silver: inserted {inserted} current rows; "
        f"{current} article versions are is_current=true"
    )
    print("### Silver articles build ends #####")


# Validate, type, and dedupe bronze into silver. Reuse the connection from main.
def run(spark: SparkSession, conn: duckdb.DuckDBPyConnection) -> None:
    global _conn
    _conn = conn
    print("Silver: clean and conform bronze data")
    _build_dim_company()
    _build_silver_articles()
