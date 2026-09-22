# Metric queries and sample outputs

### 01_daily_active_users_by_region

```sql
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
```

| order_date          | region   |   daily_active_users |
|:--------------------|:---------|---------------------:|
| 2018-12-15 00:00:00 | east     |                    1 |
| 2019-01-12 00:00:00 | central  |                    1 |
| 2019-01-16 00:00:00 | west     |                    1 |
| 2019-01-17 00:00:00 | west     |                    1 |
| 2020-01-17 00:00:00 | central  |                    1 |
| 2020-01-18 00:00:00 | east     |                    1 |
| 2020-01-19 00:00:00 | west     |                    1 |
| 2020-01-20 00:00:00 | west     |                    1 |
| 2020-01-21 00:00:00 | west     |                    1 |
| 2020-01-23 00:00:00 | west     |                    1 |
| 2020-01-24 00:00:00 | west     |                    1 |
| 2020-01-25 00:00:00 | west     |                    1 |
| 2020-01-26 00:00:00 | west     |                    1 |
| 2020-01-27 00:00:00 | west     |                    1 |
| 2020-01-28 00:00:00 | west     |                    1 |
| 2020-01-29 00:00:00 | west     |                    1 |

### 02_sweet_category_revenue

```sql
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
```

| category_group   | category_name   |   order_count |   total_quantity |   revenue_usd |
|:-----------------|:----------------|--------------:|-----------------:|--------------:|
| Sweet            | candy           |             3 |                7 |        276.62 |
| Sweet            | chocolate       |             5 |                8 |        243.08 |
| Sweet            | Sweet           |             1 |                2 |         78.16 |

### 03a_top3_products_by_revenue

```sql
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
```

|   product_id | product_name   | category_name   |   revenue_usd |
|-------------:|:---------------|:----------------|--------------:|
|           14 | kjhhjk         | crisp           |       2241.15 |
|           13 | ruy5u          | chip            |        951.39 |
|            4 | dddd           | candy           |        243.59 |

### 03b_top3_products_by_region

```sql
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
```

| region   |   product_id | product_name   |   revenue_usd |   rank_in_region |
|:---------|-------------:|:---------------|--------------:|-----------------:|
| central  |            6 | ffff           |         35.47 |                1 |
| central  |            3 | cccc           |         33.03 |                2 |
| central  |            2 | bbbb           |         19.82 |                3 |
| east     |            4 | dddd           |        107.47 |                1 |
| east     |           11 | ergertg        |        103.44 |                2 |
| east     |            7 | asdfdf         |         78.16 |                3 |
| west     |           14 | kjhhjk         |       2241.15 |                1 |
| west     |           13 | ruy5u          |        951.39 |                2 |
| west     |           12 | yioyuio        |        183.52 |                3 |

### 04_customer_lifetime_value

```sql
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
```

|   customer_id | region   | customer_type   | customer_status   |   order_count |   lifetime_revenue_usd |
|--------------:|:---------|:----------------|:------------------|--------------:|-----------------------:|
|         42492 | west     | enterprise      | active            |             8 |                3430.42 |
|         32483 | east     | consumer        | inactive          |             2 |                 319.07 |
|         42491 | west     | enterprise      | inactive          |             1 |                 136.12 |
|         21456 | central  | small business  | active            |             2 |                  88.32 |

### 05a_duplicate_orders

```sql
-- Same order_id + product + timestamp appearing more than once in the raw file.
-- (Exact duplicates were already removed in cleaning; this shows what was in the source.)
SELECT order_id, cust_id, "order date" AS order_ts, prod_id, quantity, status,
       COUNT(*) AS times_seen
FROM read_csv_auto('data/raw/orders.csv')
GROUP BY ALL
HAVING COUNT(*) > 1
ORDER BY order_id;
```

| order_id   |   cust_id | order_ts       |   prod_id |   quantity | status   |   times_seen |
|:-----------|----------:|:---------------|----------:|-----------:|:---------|-------------:|
| A-005      |     21456 | 1/12/2019 9:28 |         2 |          1 | shipped  |            3 |

### 05b_faulty_transactions

```sql
SELECT
    order_line_id, order_id, customer_id, product_id, order_ts,
    status, quantity, fault_reason
FROM fact_order_line
WHERE is_faulty OR status = 'cancelled'
ORDER BY order_ts;
```

|   order_line_id | order_id   | customer_id   |   product_id | order_ts            | status    |   quantity | fault_reason                 |
|----------------:|:-----------|:--------------|-------------:|:--------------------|:----------|-----------:|:-----------------------------|
|              13 | A-013      | 42492         |           11 | 2020-01-22 02:52:00 | shipped   |        0.1 | non-integer or zero quantity |
|              15 | A-021      | <NA>          |            7 | 2020-01-23 02:52:00 | created   |        2   | missing cust_id              |
|              16 | A-022      | <NA>          |            8 | 2020-01-23 02:52:00 | cancelled |        3   | missing cust_id              |
