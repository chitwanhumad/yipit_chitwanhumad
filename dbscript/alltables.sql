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


CREATE TABLE IF NOT EXISTS bronze_articles (
    article_id       STRING,
    title             STRING,
    company_name      STRING,
    published_date    STRING,
    category           STRING,
    revenue            STRING,
    summary            STRING,
    url                STRING,
    author             STRING,
    word_count         STRING,
    file_name          STRING,
    file_timestamp     TIMESTAMP,
    insert_datetime    TIMESTAMP,
    batch_id           INTEGER
);


CREATE TABLE IF NOT EXISTS bronze_company_metadata (
    company_name       VARCHAR,
    founded_year       INTEGER,
    headquarters       VARCHAR,
    employee_count     INTEGER,
    industry           VARCHAR,
    is_public          BOOLEAN,
    stock_ticker       VARCHAR,
    file_name          VARCHAR,
    file_timestamp     TIMESTAMP,
    insert_datetime    TIMESTAMP,
    is_current         BOOLEAN
);