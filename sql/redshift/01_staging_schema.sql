-- =====================================================================
-- 01_staging_schema.sql
-- OLTP-facing landing zone. Raw extracts land here via AWS Glue jobs
-- (COPY FROM s3://<raw-bucket>/...) before being validated and
-- transformed into the OLAP star schema in 02/03.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id       VARCHAR(20)   NOT NULL,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(255),
    city              VARCHAR(100),
    country           VARCHAR(100),
    signup_date       DATE,
    customer_segment  VARCHAR(50),
    _ingested_at      TIMESTAMP DEFAULT GETDATE()
);

CREATE TABLE IF NOT EXISTS staging.stores (
    store_id     VARCHAR(20) NOT NULL,
    store_name   VARCHAR(200),
    city         VARCHAR(100),
    country      VARCHAR(100),
    store_type   VARCHAR(50),
    _ingested_at TIMESTAMP DEFAULT GETDATE()
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id    VARCHAR(20) NOT NULL,
    product_name  VARCHAR(200),
    category      VARCHAR(100),
    unit_price    DECIMAL(10, 2),
    unit_cost     DECIMAL(10, 2),
    _ingested_at  TIMESTAMP DEFAULT GETDATE()
);

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id         VARCHAR(20) NOT NULL,
    order_date       DATE,
    customer_id      VARCHAR(20),
    store_id         VARCHAR(20),
    product_id       VARCHAR(20),
    quantity         INT,
    unit_price       DECIMAL(10, 2),
    discount_pct     DECIMAL(5, 2),
    gross_amount     DECIMAL(12, 2),
    discount_amount  DECIMAL(12, 2),
    net_amount       DECIMAL(12, 2),
    channel          VARCHAR(50),
    payment_method   VARCHAR(50),
    _ingested_at     TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE EVEN
SORTKEY (order_date);
