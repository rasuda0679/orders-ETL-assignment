-- Star schema: 4 dimensions + 1 fact table (one row per order line).
-- Written in plain ANSI SQL so it runs on DuckDB here and Snowflake with minimal change.

CREATE TABLE dim_customer AS
SELECT
    cust_id        AS customer_id,
    created_at     AS created_at,
    country,
    region,
    customer_type,
    zip,
    is_active
FROM customers_df;

CREATE TABLE dim_product AS
SELECT
    prod_id        AS product_id,
    prod_name      AS product_name,
    cat_id         AS category_id,
    category_name,
    category_group,            -- top-level group: Sweet / salt
    price          AS list_price,
    currency
FROM products_df;

CREATE TABLE dim_currency_rate AS
SELECT rate_date, currency, rate_to_usd
FROM rates_df;

-- calendar covering the full range of order dates
CREATE TABLE dim_date AS
SELECT
    d::DATE                       AS date_key,
    EXTRACT(year  FROM d)         AS year,
    EXTRACT(month FROM d)         AS month,
    EXTRACT(day   FROM d)         AS day,
    STRFTIME(d, '%A')             AS day_name
FROM (
    SELECT UNNEST(GENERATE_SERIES(
        (SELECT MIN(order_date) FROM orders_df)::DATE,
        (SELECT MAX(order_date) FROM orders_df)::DATE,
        INTERVAL 1 DAY)) AS d
);

CREATE TABLE fact_order_line AS
SELECT
    ROW_NUMBER() OVER (ORDER BY order_ts, order_id, prod_id) AS order_line_id,
    order_id,
    cust_id           AS customer_id,
    prod_id           AS product_id,
    order_date::DATE  AS date_key,
    order_ts,
    status,
    quantity,
    currency,
    price             AS unit_price_local,
    rate_to_usd,
    unit_price_usd,
    revenue_usd,
    is_faulty,
    fault_reason,
    is_valid_sale
FROM orders_df;
