-- =============================================================================
-- Retail Sales ETL Pipeline — Redshift Star Schema
-- =============================================================================
-- Run this file in Redshift Query Editor v2 or psql
-- Replace <YOUR_BUCKET> and <YOUR_IAM_ROLE_ARN> with real values
-- =============================================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS retail;

-- =============================================================================
-- DIMENSION: dim_date
-- =============================================================================
DROP TABLE IF EXISTS retail.dim_date CASCADE;

CREATE TABLE retail.dim_date (
    date_id       INT         NOT NULL,
    full_date     DATE        NOT NULL,
    year          SMALLINT    NOT NULL,
    quarter       SMALLINT    NOT NULL,
    month         SMALLINT    NOT NULL,
    month_name    VARCHAR(10) NOT NULL,
    week_of_year  SMALLINT    NOT NULL,
    day_of_month  SMALLINT    NOT NULL,
    day_of_week   SMALLINT    NOT NULL,   -- 1=Sun, 7=Sat
    day_name      VARCHAR(10) NOT NULL,
    is_weekend    BOOLEAN     NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_dim_date PRIMARY KEY (date_id)
)
DISTSTYLE ALL   -- small table, broadcast to all nodes
SORTKEY (year, month);

-- =============================================================================
-- DIMENSION: dim_product
-- =============================================================================
DROP TABLE IF EXISTS retail.dim_product CASCADE;

CREATE TABLE retail.dim_product (
    product_sk      BIGINT        NOT NULL,
    stock_code      VARCHAR(20)   NOT NULL,
    description     VARCHAR(255),
    current_price   DECIMAL(10,2),

    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
)
DISTKEY (product_sk)
SORTKEY (stock_code);

-- =============================================================================
-- DIMENSION: dim_customer  (SCD Type 2)
-- =============================================================================
DROP TABLE IF EXISTS retail.dim_customer CASCADE;

CREATE TABLE retail.dim_customer (
    customer_sk     BIGINT        NOT NULL,
    customer_id     INT           NOT NULL,
    country         VARCHAR(100)  NOT NULL,
    version         INT           NOT NULL DEFAULT 1,
    effective_date  TIMESTAMP     NOT NULL,
    expiry_date     TIMESTAMP     NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current      BOOLEAN       NOT NULL DEFAULT TRUE,

    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
)
DISTKEY (customer_sk)
SORTKEY (customer_id, effective_date);

-- =============================================================================
-- FACT: fact_sales
-- =============================================================================
DROP TABLE IF EXISTS retail.fact_sales CASCADE;

CREATE TABLE retail.fact_sales (
    fact_id         BIGINT        NOT NULL,
    invoice_no      VARCHAR(20)   NOT NULL,
    date_id         INT           NOT NULL,
    customer_sk     BIGINT,
    product_sk      BIGINT,
    quantity        INT           NOT NULL,
    unit_price      DECIMAL(10,2) NOT NULL,
    line_total      DECIMAL(12,2) NOT NULL,
    country         VARCHAR(100),
    invoice_date    TIMESTAMP,
    ingestion_ts    TIMESTAMP     DEFAULT SYSDATE,

    CONSTRAINT pk_fact_sales PRIMARY KEY (fact_id),
    CONSTRAINT fk_fact_date     FOREIGN KEY (date_id)     REFERENCES retail.dim_date(date_id),
    CONSTRAINT fk_fact_customer FOREIGN KEY (customer_sk) REFERENCES retail.dim_customer(customer_sk),
    CONSTRAINT fk_fact_product  FOREIGN KEY (product_sk)  REFERENCES retail.dim_product(product_sk)
)
DISTKEY (customer_sk)
SORTKEY (date_id, customer_sk);


-- =============================================================================
-- LOAD DATA: COPY commands  (Parquet from S3)
-- =============================================================================
-- NOTE: Redshift COPY from Parquet auto-maps columns by name.
-- Make sure IAM role has s3:GetObject on your bucket.

COPY retail.dim_date
FROM 's3://<YOUR_BUCKET>/processed/silver/dim_date/'
IAM_ROLE '<YOUR_IAM_ROLE_ARN>'
FORMAT AS PARQUET;

COPY retail.dim_product
FROM 's3://<YOUR_BUCKET>/processed/silver/dim_product/'
IAM_ROLE '<YOUR_IAM_ROLE_ARN>'
FORMAT AS PARQUET;

COPY retail.dim_customer
FROM 's3://<YOUR_BUCKET>/processed/silver/dim_customer/'
IAM_ROLE '<YOUR_IAM_ROLE_ARN>'
FORMAT AS PARQUET;

COPY retail.fact_sales
FROM 's3://<YOUR_BUCKET>/processed/silver/fact_sales/'
IAM_ROLE '<YOUR_IAM_ROLE_ARN>'
FORMAT AS PARQUET;


-- =============================================================================
-- VALIDATION QUERIES  (run after COPY to check data)
-- =============================================================================

-- Row counts
SELECT 'dim_date'     AS tbl, COUNT(*) AS rows FROM retail.dim_date
UNION ALL
SELECT 'dim_product'  AS tbl, COUNT(*) AS rows FROM retail.dim_product
UNION ALL
SELECT 'dim_customer' AS tbl, COUNT(*) AS rows FROM retail.dim_customer
UNION ALL
SELECT 'fact_sales'   AS tbl, COUNT(*) AS rows FROM retail.fact_sales;

-- Referential integrity check — orphaned facts (should return 0)
SELECT COUNT(*) AS orphaned_dates
FROM retail.fact_sales fs
LEFT JOIN retail.dim_date d ON fs.date_id = d.date_id
WHERE d.date_id IS NULL;

-- Sample business query: Monthly revenue by country (Top 5 countries)
SELECT
    d.year,
    d.month_name,
    fs.country,
    ROUND(SUM(fs.line_total), 2)  AS revenue,
    COUNT(DISTINCT fs.invoice_no) AS orders
FROM retail.fact_sales fs
JOIN retail.dim_date d ON fs.date_id = d.date_id
WHERE fs.country IN (
    SELECT country FROM retail.fact_sales
    GROUP BY country ORDER BY SUM(line_total) DESC LIMIT 5
)
GROUP BY d.year, d.month_name, d.month, fs.country
ORDER BY d.year, d.month, revenue DESC;

-- SCD Type 2 check: customers with multiple versions
SELECT customer_id, COUNT(*) AS versions
FROM retail.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY versions DESC
LIMIT 10;
