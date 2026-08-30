-- log table for bronze csv and json files status
CREATE TABLE IF NOT EXISTS bronze_file_log (
    batch_id        INTEGER,
    file_name       VARCHAR,
    reason          VARCHAR,
    status          VARCHAR,
    quarantined_at  TIMESTAMP,
    processed_at    TIMESTAMP,
    alert_processed VARCHAR
);

UPDATE bronze_file_log SET alert_processed = 'N' WHERE alert_processed IS NULL
    OR lower(cast(alert_processed AS VARCHAR)) IN ('false', 'f', '0');

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


CREATE TABLE IF NOT EXISTS dim_category (
    id               INTEGER,
    name             VARCHAR,
    category_group   VARCHAR,
    insert_datetime  TIMESTAMP
);

ALTER TABLE dim_category ADD COLUMN IF NOT EXISTS category_group VARCHAR;

INSERT INTO dim_category (id, name, category_group, insert_datetime)
SELECT * FROM (
    VALUES
        (1,  'Finance',                 'Finance',   CURRENT_TIMESTAMP),
        (2,  'Cybersecurity',           'Security',  CURRENT_TIMESTAMP),
        (3,  'Enterprise Software',     'Software',  CURRENT_TIMESTAMP),
        (4,  'Data Analytics',          'Analytics', CURRENT_TIMESTAMP),
        (5,  'InfoSec',                 'Security',  CURRENT_TIMESTAMP),
        (6,  'Machine Learning',        'AI/ML',     CURRENT_TIMESTAMP),
        (7,  'Software',                'Software',  CURRENT_TIMESTAMP),
        (8,  'Cloud',                   'Cloud',     CURRENT_TIMESTAMP),
        (9,  'FinTech',                 'Finance',   CURRENT_TIMESTAMP),
        (10, 'AI/ML',                   'AI/ML',     CURRENT_TIMESTAMP),
        (11, 'Financial Technology',    'Finance',   CURRENT_TIMESTAMP),
        (12, 'Analytics',               'Analytics', CURRENT_TIMESTAMP),
        (13, 'Security',                'Security',  CURRENT_TIMESTAMP),
        (14, 'AI & ML',                 'AI/ML',     CURRENT_TIMESTAMP),
        (15, 'Artificial Intelligence', 'AI/ML',     CURRENT_TIMESTAMP),
        (16, 'Big Data',                'Analytics', CURRENT_TIMESTAMP),
        (17, 'SaaS',                    'Software',  CURRENT_TIMESTAMP),
        (18, 'Cloud Services',          'Cloud',     CURRENT_TIMESTAMP),
        (19, 'Cloud Computing',         'Cloud',     CURRENT_TIMESTAMP)
) AS v(id, name, category_group, insert_datetime)
WHERE NOT EXISTS (SELECT 1 FROM dim_category);

UPDATE dim_category
SET category_group = v.category_group
FROM (
    VALUES
        (1,  'Finance'),
        (2,  'Security'),
        (3,  'Software'),
        (4,  'Analytics'),
        (5,  'Security'),
        (6,  'AI/ML'),
        (7,  'Software'),
        (8,  'Cloud'),
        (9,  'Finance'),
        (10, 'AI/ML'),
        (11, 'Finance'),
        (12, 'Analytics'),
        (13, 'Security'),
        (14, 'AI/ML'),
        (15, 'AI/ML'),
        (16, 'Analytics'),
        (17, 'Software'),
        (18, 'Cloud'),
        (19, 'Cloud')
) AS v(id, category_group)
WHERE dim_category.id = v.id
  AND dim_category.category_group IS NULL;

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

CREATE TABLE IF NOT EXISTS dim_date (
    date_id      INTEGER,
    date         DATE,
    year         INTEGER,
    month        INTEGER,
    day          INTEGER,
    quarter_no   INTEGER
);

INSERT INTO dim_date (date_id, date, year, month, day, quarter_no)
SELECT
    year(d) * 10000 + month(d) * 100 + day(d) AS date_id,
    d AS date,
    year(d) AS year,
    month(d) AS month,
    day(d) AS day,
    quarter(d) AS quarter_no
FROM generate_series(DATE '2020-01-01', DATE '2026-12-31', INTERVAL 1 DAY) AS t(d)
WHERE NOT EXISTS (SELECT 1 FROM dim_date);

CREATE TABLE IF NOT EXISTS _build_silver_articles (
    observation_id       INTEGER,
    article_id           STRING,
    title                STRING,
    company_id           INTEGER,
    company_metched      STRING,
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

CREATE OR REPLACE MACRO normalize_company_name(name) AS
    regexp_replace(
        regexp_replace(
            lower(trim(CAST(name AS VARCHAR))),
            '\s*\([^)]*\)',
            '',
            'g'
        ),
        '\s+(corporation|corp\.?|incorporated|inc\.?|limited|ltd\.?|llc|plc|gmbh|ag|co\.?|company|group|holdings|technologies|research|labs)\.?\s*$',
        '',
        'g'
    );

CREATE OR REPLACE MACRO normalize_company_compact(name) AS
    regexp_replace(normalize_company_name(name), '[^a-z0-9]', '', 'g');

CREATE OR REPLACE MACRO company_name_initials(name) AS
    lower(regexp_replace(
        regexp_replace(normalize_company_name(name), '([^[:space:]])[^[:space:]]*', '\1', 'g'),
        '\s+',
        '',
        'g'
    ));

CREATE OR REPLACE MACRO company_name_synonyms(name) AS
    CASE
        WHEN normalize_company_compact(name) IN (
            'facebook', 'facebookai', 'facebookairesearch'
        ) THEN ['meta ai', 'metaai']
        WHEN normalize_company_compact(name) IN (
            'azure', 'microsoftazure'
        ) THEN ['microsoft']
        ELSE CAST([] AS VARCHAR[])
    END;

CREATE OR REPLACE MACRO company_name_aliases(name) AS
    list_distinct(list_concat(
        list_concat(
            list_transform(
                regexp_split_to_array(CAST(name AS VARCHAR), '\s*/\s*'),
                x -> normalize_company_name(x)
            ),
            list_transform(
                regexp_split_to_array(CAST(name AS VARCHAR), '\s*/\s*'),
                x -> normalize_company_compact(x)
            )
        ),
        list_concat(
            list_filter(
                [
                    normalize_company_compact(name),
                    company_name_initials(name),
                    normalize_company_compact(
                        regexp_extract(CAST(name AS VARCHAR), '\(([^)]+)\)', 1)
                    )
                ],
                x -> x IS NOT NULL AND length(x) > 0
            ),
            company_name_synonyms(name)
        )
    ));