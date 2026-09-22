-- name: 01_daily_active_users_by_region
-- placed at least one non-faulty, non-cancelled order line that day (created orders count as activity)
SELECT
    f.date_key                      AS order_date,
    c.region,
    COUNT(DISTINCT f.customer_id)   AS daily_active_users
FROM fact_order_line f
JOIN dim_customer c ON c.customer_id = f.customer_id
WHERE NOT f.is_faulty AND f.status <> 'cancelled'
GROUP BY f.date_key, c.region
ORDER BY f.date_key, c.region;

-- name: 02_sweet_category_revenue
-- Revenue in USD (price * rate on order date * quantity) for everything under "Sweet".
SELECT
    p.category_group,
    p.category_name,
    COUNT(DISTINCT f.order_id)      AS order_count,
    SUM(f.quantity)                 AS total_quantity,
    ROUND(SUM(f.revenue_usd), 2)    AS revenue_usd
FROM fact_order_line f
JOIN dim_product p ON p.product_id = f.product_id
WHERE p.category_group = 'Sweet'
  AND f.is_valid_sale
GROUP BY p.category_group, p.category_name
ORDER BY revenue_usd DESC;

-- name: 03a_top3_products_by_revenue
SELECT
    p.product_id,
    p.product_name,
    p.category_name,
    ROUND(SUM(f.revenue_usd), 2)    AS revenue_usd
FROM fact_order_line f
JOIN dim_product p ON p.product_id = f.product_id
WHERE f.is_valid_sale
GROUP BY p.product_id, p.product_name, p.category_name
ORDER BY revenue_usd DESC
LIMIT 3;

-- name: 03b_top3_products_by_region
SELECT region, product_id, product_name, revenue_usd, rank_in_region
FROM (
    SELECT
        c.region,
        p.product_id,
        p.product_name,
        ROUND(SUM(f.revenue_usd), 2) AS revenue_usd,
        RANK() OVER (PARTITION BY c.region ORDER BY SUM(f.revenue_usd) DESC) AS rank_in_region
    FROM fact_order_line f
    JOIN dim_product  p ON p.product_id  = f.product_id
    JOIN dim_customer c ON c.customer_id = f.customer_id
    WHERE f.is_valid_sale
    GROUP BY c.region, p.product_id, p.product_name
) t
WHERE rank_in_region <= 3
ORDER BY region, rank_in_region;

-- name: 04_customer_lifetime_value
-- CLV proxy = total valid revenue per customer, split by active flag.
SELECT
    c.customer_id,
    c.region,
    c.customer_type,
    CASE WHEN c.is_active THEN 'active' ELSE 'inactive' END AS customer_status,
    COUNT(DISTINCT f.order_id)                       AS order_count,
    ROUND(COALESCE(SUM(f.revenue_usd), 0), 2)        AS lifetime_revenue_usd
FROM dim_customer c
LEFT JOIN fact_order_line f
       ON f.customer_id = c.customer_id AND f.is_valid_sale
GROUP BY c.customer_id, c.region, c.customer_type, c.is_active
ORDER BY lifetime_revenue_usd DESC;

-- name: 05a_duplicate_orders
-- Same order_id + product + timestamp appearing more than once in the raw file.
-- (Exact duplicates were already removed in cleaning; this shows what was in the source.)
SELECT order_id, cust_id, "order date" AS order_ts, prod_id, quantity, status,
       COUNT(*) AS times_seen
FROM read_csv_auto('data/raw/orders.csv')
GROUP BY ALL
HAVING COUNT(*) > 1
ORDER BY order_id;

-- name: 05b_faulty_transactions
SELECT
    order_line_id, order_id, customer_id, product_id, order_ts,
    status, quantity, fault_reason
FROM fact_order_line
WHERE is_faulty OR status = 'cancelled'
ORDER BY order_ts;
