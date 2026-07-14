-- =====================================================================
-- 02_dimension_tables.sql
-- Star-schema dimensions (OLAP layer) in the `analytics` schema.
-- Surrogate keys (IDENTITY) decouple the warehouse from source-system
-- keys so slowly changing source IDs never break historical facts.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key      INTEGER IDENTITY(1, 1) PRIMARY KEY,
    customer_id       VARCHAR(20)  NOT NULL,
    first_name        VARCHAR(100),
    last_name         VARCHAR(100),
    email             VARCHAR(255),
    city              VARCHAR(100),
    country           VARCHAR(100),
    customer_segment  VARCHAR(50),
    signup_date       DATE,
    is_current        BOOLEAN DEFAULT TRUE,
    effective_date    DATE DEFAULT GETDATE(),
    UNIQUE (customer_id)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key    INTEGER IDENTITY(1, 1) PRIMARY KEY,
    product_id     VARCHAR(20) NOT NULL,
    product_name   VARCHAR(200),
    category       VARCHAR(100),
    unit_price     DECIMAL(10, 2),
    unit_cost      DECIMAL(10, 2),
    margin_pct     DECIMAL(6, 2),
    UNIQUE (product_id)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS analytics.dim_store (
    store_key    INTEGER IDENTITY(1, 1) PRIMARY KEY,
    store_id     VARCHAR(20) NOT NULL,
    store_name   VARCHAR(200),
    city         VARCHAR(100),
    country      VARCHAR(100),
    store_type   VARCHAR(50),
    UNIQUE (store_id)
)
DISTSTYLE ALL;

-- Standard calendar dimension, generated once and reused across facts.
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key      INTEGER PRIMARY KEY,      -- YYYYMMDD
    full_date     DATE NOT NULL,
    day_of_week   VARCHAR(10),
    day_num       INT,
    month_num     INT,
    month_name    VARCHAR(10),
    quarter       INT,
    year          INT,
    is_weekend    BOOLEAN
)
DISTSTYLE ALL;
