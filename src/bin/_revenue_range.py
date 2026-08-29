"""Map USD revenue amounts to dim_revenue_range.range_id."""

import duckdb

# Load (range_id, min_inclusive, max_inclusive) from dim_revenue_range.
# Skip range_id 0 (Unknown); max NULL means open-ended (1B+).
def _load_revenue_ranges(conn: duckdb.DuckDBPyConnection) -> list[tuple]:
    rows = conn.execute(
        """
        SELECT range_id, min_revenue_usd, max_revenue_usd
        FROM dim_revenue_range
        WHERE range_id <> 0
        ORDER BY range_order
        """
    ).fetchall()
    return [
        (
            int(range_id),
            float(min_usd),
            None if max_usd is None else float(max_usd),
        )
        for range_id, min_usd, max_usd in rows
    ]


def _lookup_range_id(ranges: list[tuple], value: float | None) -> int | None:
    """Find which dim_revenue_range bucket `value` falls into (first match wins)."""
    if value is None:
        return None
    for range_id, low, high in ranges:
        if value < low:
            continue
        if high is None or value <= high:
            return range_id
    return None


def get_revenue_range_id(conn: duckdb.DuckDBPyConnection, inputs: list) -> int:
    """inputs = [revenue_actual_usd, revenue_min_actual_usd, revenue_max_actual_usd]"""
    revenue_actual, revenue_min, revenue_max = inputs
    ranges = _load_revenue_ranges(conn)

    if revenue_actual is not None:
        range_id = _lookup_range_id(ranges, revenue_actual)
        return range_id if range_id is not None else 0

    if revenue_min is not None and revenue_max is not None:
        midpoint = (revenue_min + revenue_max) / 2
        range_id = _lookup_range_id(ranges, midpoint)
        return range_id if range_id is not None else 0

    return 0
