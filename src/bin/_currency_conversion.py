"""Convert amounts to USD using dim_currency_conversion."""

import duckdb


# Look up usd_conversion for input_currency and return in_arr * rate.
# Success: {"success": usd_value}. Error: -1.
def get_usd_arr(
    conn: duckdb.DuckDBPyConnection,
    input_currency: str,
    in_arr: float,
) -> dict | int:
    try:
        row = conn.execute(
            """
            SELECT usd_conversion * $in_arr AS usd_arr
            FROM dim_currency_conversion
            WHERE base_currency = $input_currency
              AND is_current = TRUE
            """,
            {
                "in_arr": float(in_arr),
                "input_currency": input_currency,
            },
        ).fetchone()
    except Exception as e:
        print(e)
        return {"error": -1}

    if row is None or row[0] is None:
        return {"error": -1}
    return {"success": round(row[0], 2)}
