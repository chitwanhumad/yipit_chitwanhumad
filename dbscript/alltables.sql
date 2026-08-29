-- bronze tables
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

-- silver tables
CREATE TABLE IF NOT EXISTS dim_company (
    id                 INTEGER,
    company_name       VARCHAR,
    founded_year       INTEGER,
    headquarters       VARCHAR,
    employee_count     INTEGER,
    industry           VARCHAR,
    is_public          BOOLEAN,
    stock_ticker       VARCHAR,
    insert_batch_id    INTEGER,
    updated_batch_id   INTEGER,
    inserted_at        TIMESTAMP,
    updated_at         TIMESTAMP
);

ALTER TABLE dim_company ADD COLUMN IF NOT EXISTS insert_batch_id INTEGER;
ALTER TABLE dim_company ADD COLUMN IF NOT EXISTS updated_batch_id INTEGER;
ALTER TABLE dim_company ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP;
ALTER TABLE dim_company ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;

CREATE TABLE IF NOT EXISTS dim_category (
    id               INTEGER,
    name             VARCHAR,
    insert_datetime  TIMESTAMP
);

INSERT INTO dim_category (id, name, insert_datetime)
SELECT * FROM (
    VALUES
        (1, 'Finance', CURRENT_TIMESTAMP),
        (2, 'Cybersecurity', CURRENT_TIMESTAMP),
        (3, 'Enterprise Software', CURRENT_TIMESTAMP),
        (4, 'Data Analytics', CURRENT_TIMESTAMP),
        (5, 'InfoSec', CURRENT_TIMESTAMP),
        (6, 'Machine Learning', CURRENT_TIMESTAMP),
        (7, 'Software', CURRENT_TIMESTAMP),
        (8, 'Cloud', CURRENT_TIMESTAMP),
        (9, 'FinTech', CURRENT_TIMESTAMP),
        (10, 'AI/ML', CURRENT_TIMESTAMP),
        (11, 'Financial Technology', CURRENT_TIMESTAMP),
        (12, 'Analytics', CURRENT_TIMESTAMP),
        (13, 'Security', CURRENT_TIMESTAMP),
        (14, 'AI & ML', CURRENT_TIMESTAMP),
        (15, 'Artificial Intelligence', CURRENT_TIMESTAMP),
        (16, 'Big Data', CURRENT_TIMESTAMP),
        (17, 'SaaS', CURRENT_TIMESTAMP),
        (18, 'Cloud Services', CURRENT_TIMESTAMP),
        (19, 'Cloud Computing', CURRENT_TIMESTAMP)
) AS v(id, name, insert_datetime)
WHERE NOT EXISTS (SELECT 1 FROM dim_category);

CREATE TABLE IF NOT EXISTS dim_revenue_range (
    range_id           INTEGER,
    min_revenue_usd    FLOAT,
    max_revenue_usd    FLOAT,
    range_name         VARCHAR,
    range_order        INTEGER
);

INSERT INTO dim_revenue_range (range_id, min_revenue_usd, max_revenue_usd, range_name, range_order)
SELECT * FROM (
    VALUES
        (0, 0, 0, 'Unknown', 0),
        (1, 1, 10000, 'Up to 10K USD', 1),
        (2, 10001, 100000, '10K -100K USD', 2),
        (3, 100001, 1000000, '100K - 1M USD', 3),
        (4, 1000001, 10000000, '1M-10M USD', 4),
        (5, 10000001, 100000000, '10M-100M', 5),
        (6, 100000001, 1000000000, '100M-1B', 6),
        (7, 1000000001, 10000000000, '1B - 10B', 7),
        (8, 10000000001, 100000000000, '10B - 100B', 8),
        (9, 100000000001, NULL, '100B+', 9)
) AS v(range_id, min_revenue_usd, max_revenue_usd, range_name, range_order)
WHERE NOT EXISTS (SELECT 1 FROM dim_revenue_range);

CREATE TABLE IF NOT EXISTS _build_silver_articles (
    observation_id       INTEGER,
    article_id           STRING,
    title                STRING,
    company_id           INTEGER,
    published_date_as_source STRING,
    published_date       DATE,
    category_id          INTEGER,
    revenue_as_source    STRING,
    revenue_currency     VARCHAR,
    revenue_actual       FLOAT,
    revenue_actual_usd   FLOAT,
    revenue_min_currency     VARCHAR,
    revenue_min_actual_usd   FLOAT,
    revenue_max_currency     VARCHAR,
    revenue_max_actual_usd   FLOAT,
    revenue_range_id     INTEGER,
    summary              STRING,
    url                  STRING,
    author               STRING,
    word_count           INTEGER,
    insert_batch_id      INTEGER,
    updated_batch_id     INTEGER,
    inserted_at          TIMESTAMP,
    updated_at           TIMESTAMP,
    is_current           BOOLEAN
);

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
        (3, 'JPY', 1.0 / 150, TRUE),
        (4, 'USD', 1.0, TRUE)
) AS v(id, base_currency, usd_conversion, is_current)
WHERE NOT EXISTS (SELECT 1 FROM dim_currency_conversion);

INSERT INTO dim_currency_conversion (id, base_currency, usd_conversion, is_current)
SELECT 4, 'USD', 1.0, TRUE
WHERE NOT EXISTS (
    SELECT 1 FROM dim_currency_conversion WHERE base_currency = 'USD'
);
