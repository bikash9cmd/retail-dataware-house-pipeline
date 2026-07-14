-- =====================================================================
-- 03_fact_tables.sql
-- Grain: one row per order line. Foreign keys reference dimension
-- surrogate keys, not source-system IDs (standard Kimball practice).
-- =====================================================================

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    sales_key         BIGINT IDENTITY(1, 1) PRIMARY KEY,
    order_id          VARCHAR(20) NOT NULL,
    date_key          INTEGER     NOT NULL REFERENCES analytics.dim_date(date_key),
    customer_key      INTEGER     NOT NULL REFERENCES analytics.dim_customer(customer_key),
    product_key       INTEGER     NOT NULL REFERENCES analytics.dim_product(product_key),
    store_key         INTEGER     NOT NULL REFERENCES analytics.dim_store(store_key),
    quantity          INT,
    unit_price        DECIMAL(10, 2),
    discount_pct      DECIMAL(5, 2),
    gross_amount      DECIMAL(12, 2),
    discount_amount   DECIMAL(12, 2),
    net_amount        DECIMAL(12, 2),
    channel           VARCHAR(50),
    payment_method    VARCHAR(50),
    loaded_at         TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (customer_key)
SORTKEY (date_key);
