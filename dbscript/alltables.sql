CREATE TABLE IF NOT EXISTS dim_currency_conversion (
    id INTEGER,
    base_currency VARCHAR,
    usd_conversion DOUBLE,
    is_current BOOLEAN
);

INSERT INTO dim_currency_conversion (id, base_currency, usd_conversion, is_current)
SELECT * FROM (
    VALUES
        (1, 'EUR', 1.1, TRUE),
        (2, 'GBP', 1.27, TRUE),
        (3, 'JPY', 1.0 / 150, TRUE)
) AS v(id, base_currency, usd_conversion, is_current)
WHERE NOT EXISTS (SELECT 1 FROM dim_currency_conversion);
