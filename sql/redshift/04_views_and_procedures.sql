-- =====================================================================
-- 04_views_and_procedures.sql
-- BI-facing views + a stored procedure for scheduled aggregate refresh.
-- These views are what the FastAPI layer (api/main.py) and Power BI
-- both query — application code never touches the fact/dim tables
-- directly, so the physical model can evolve without breaking clients.
-- =====================================================================

-- Daily revenue, orders, and margin, ready for a Power BI trend chart.
CREATE OR REPLACE VIEW analytics.vw_daily_sales AS
SELECT
    d.full_date,
    d.year,
    d.month_name,
    COUNT(DISTINCT f.order_id)                              AS order_count,
    SUM(f.quantity)                                          AS units_sold,
    SUM(f.net_amount)                                        AS net_revenue,
    SUM(f.net_amount) - SUM(f.quantity * p.unit_cost)        AS gross_margin
FROM analytics.fact_sales f
JOIN analytics.dim_date d    ON f.date_key = d.date_key
JOIN analytics.dim_product p ON f.product_key = p.product_key
GROUP BY d.full_date, d.year, d.month_name;

-- Top products by revenue — backs the /kpis/top-products API endpoint.
CREATE OR REPLACE VIEW analytics.vw_top_products AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    SUM(f.quantity)      AS units_sold,
    SUM(f.net_amount)    AS net_revenue,
    RANK() OVER (ORDER BY SUM(f.net_amount) DESC) AS revenue_rank
FROM analytics.fact_sales f
JOIN analytics.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_id, p.product_name, p.category;

-- Customer segmentation — backs the /kpis/customer-segments API endpoint.
CREATE OR REPLACE VIEW analytics.vw_customer_segments AS
SELECT
    c.customer_segment,
    c.country,
    COUNT(DISTINCT c.customer_key)  AS customer_count,
    COUNT(DISTINCT f.order_id)      AS order_count,
    SUM(f.net_amount)               AS net_revenue,
    SUM(f.net_amount) / NULLIF(COUNT(DISTINCT c.customer_key), 0) AS revenue_per_customer
FROM analytics.dim_customer c
LEFT JOIN analytics.fact_sales f ON c.customer_key = f.customer_key
GROUP BY c.customer_segment, c.country;

-- Store performance — used by regional ops stakeholders in Power BI.
CREATE OR REPLACE VIEW analytics.vw_store_performance AS
SELECT
    s.store_id,
    s.store_name,
    s.city,
    s.country,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.net_amount)          AS net_revenue
FROM analytics.fact_sales f
JOIN analytics.dim_store s ON f.store_key = s.store_key
GROUP BY s.store_id, s.store_name, s.city, s.country;

-- ---------------------------------------------------------------------
-- Stored procedure: recomputes dim_product.margin_pct after a price or
-- cost update lands in staging. Called by the Airflow DAG's
-- `refresh_derived_columns` task via a Redshift Data API call.
-- ---------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE analytics.sp_refresh_product_margin()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE analytics.dim_product
    SET margin_pct = CASE
        WHEN unit_price > 0 THEN ROUND(((unit_price - unit_cost) / unit_price) * 100, 2)
        ELSE 0
    END;

    RAISE INFO 'dim_product.margin_pct refreshed for % rows', (SELECT COUNT(*) FROM analytics.dim_product);
END;
$$;

-- ---------------------------------------------------------------------
-- Athena reference: fact_sales is also exposed as an external table
-- over the S3 "curated" zone (Parquet, partitioned by year/month) for
-- ad-hoc analyst queries that shouldn't run against live Redshift.
-- Table is created/updated by an AWS Glue crawler; DDL kept here for
-- reference and local review.
-- ---------------------------------------------------------------------
-- CREATE EXTERNAL TABLE analytics_athena.fact_sales (
--     order_id         string,
--     date_key         int,
--     customer_id      string,
--     product_id       string,
--     store_id         string,
--     quantity         int,
--     net_amount       decimal(12,2),
--     channel          string,
--     payment_method   string
-- )
-- PARTITIONED BY (year int, month int)
-- STORED AS PARQUET
-- LOCATION 's3://retail-dw-curated/fact_sales/';
