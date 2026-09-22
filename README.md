# Orders ETL Assignment – Technical Write-up

## 1. How to run

```bash
pip install pandas duckdb tabulate
python pipeline.py
```

That's it. The script reads the raw files, cleans them, builds a star schema in a local DuckDB file
(`warehouse.duckdb`) and runs the metric queries. Results land in `output/`.

I used DuckDB instead of Snowflake so the whole thing runs locally with no account setup. The SQL is
plain ANSI and would run on Snowflake with almost no change (only `read_csv_auto` in the duplicate
check and `STRFTIME` in `dim_date` would need swapping).

```
etl_assignment/
├── pipeline.py            # the ETL: extract -> clean -> load -> metrics
├── airflow_dag.py         # how the same steps would be scheduled in Airflow
├── sql/create_model.sql   # star schema DDL
├── sql/metrics.sql        # the 5 analytics queries
├── data/raw/              # source files as given
├── data/clean/            # cleaned CSVs (output of the clean step)
└── output/                # metric results (CSV + metrics_output.md with SQL + tables)
```

## 2. Orchestration

`pipeline.py` runs the four steps in sequence and logs each one. Each step is a plain function with
a clear input/output, so it can be dropped into a scheduler. `airflow_dag.py` shows that: three
tasks (`extract_and_clean >> load_star_schema >> run_metrics`) on a daily schedule. Cleaned CSVs are
written to disk between steps so a failed load can be retried without re-reading source.

## 3. Data model

Simple star schema, one fact table with one row per order line.

| Table | Grain | Key columns |
|---|---|---|
| `dim_customer` | one row per customer | customer_id, created_at, country, region, customer_type, zip, is_active |
| `dim_product` | one row per product | product_id, product_name, category_id, category_name, category_group (Sweet / salt), list_price, currency |
| `dim_date` | one row per calendar day | date_key, year, month, day, day_name |
| `dim_currency_rate` | one row per currency per day | rate_date, currency, rate_to_usd |
| `fact_order_line` | one row per order line | order_id, customer_id, product_id, date_key, status, quantity, unit_price_local, rate_to_usd, unit_price_usd, revenue_usd, is_faulty, fault_reason, is_valid_sale |

```
dim_customer ──┐
dim_product  ──┼──> fact_order_line <── dim_date
dim_currency_rate (joined at load time to fill rate_to_usd)
```

The category tree is flattened into `dim_product` (`category_name` = leaf like chocolate/candy,
`category_group` = top level like Sweet/salt) so "Sweet" queries are a simple filter, no recursion.

### Mapping / transformations

| Source | Target | Transformation |
|---|---|---|
| customer.csv (12 rows) | dim_customer (4 rows) | keep latest row per cust_id; `active` y/n → boolean; trim/lower-case text |
| product.csv + prod_cat_tree.csv | dim_product | join on cat_id; currency upper-cased ("Yen" → "YEN"); derive category_group from parent |
| currency_conversion.json | dim_currency_rate | date parsed; currency upper-cased; de-duplicated |
| orders.csv (24 rows) | fact_order_line (22 rows) | rename `order date`; parse timestamp; zero-pad order_id (A-21 → A-021); drop exact duplicate lines; join price + rate; `revenue_usd = price × rate × quantity`; flag faulty lines |

Currency rate lookup uses the rate on the order date, falling back to the most recent earlier date
if a date is missing (`merge_asof` backward).

## 4. Data quality issues found and how I handled them

| Issue | Where | What I did |
|---|---|---|
| Same `cust_id` repeated 3× with different `created_at` and `active` | customer.csv | Treated rows as snapshots, kept the latest one per customer. |
| Exact duplicate order lines (A-005 × 3) | orders.csv | Dropped duplicates, kept one. Reported in metric 5a. |
| Two orders with no `cust_id` (A-21, A-22) | orders.csv | Kept in fact table, flagged `is_faulty = missing cust_id`, excluded from revenue. |
| Quantity of `0.1` (A-013) | orders.csv | Flagged as faulty (non-integer quantity), excluded from revenue. |
| Inconsistent order_id format (`A-21` vs `A-021`) | orders.csv | Zero-padded to 3 digits. |
| One `cancelled` order | orders.csv | Kept, excluded from revenue via `is_valid_sale`. |
| Currency spelled `Yen` in products but `YEN` in rates | product.csv | Upper-cased both sides before joining. |
| Orders dated 2018–2020 but customers `created_at` in 2025 | both | Kept as-is. `created_at` looks like a snapshot date rather than signup; noted, not "fixed". |
| `salt` exists only as a parent, not as its own category row | prod_cat_tree.csv | Derived `category_group` from the parent column so salt products still group correctly. |
| Column name with a space (`order date`) | orders.csv | Renamed to `order_ts`. |

All findings are also printed at the end of the run and saved to `output/dq_findings.txt`.

## 5. Analytics SQL

All queries and their outputs are in `output/metrics_output.md`. Short version of the results:

**Daily active users by region** – distinct customers with a valid order line per day/region. With
only 4 customers every day is 1 user; the query is correct at scale.

**Sweet category** – 11 orders, 27 units, **$981.61** revenue in USD
(Sweet direct $461.91, candy $276.62, chocolate $243.08).

**Top 3 products by revenue** – product 14 kjhhjk ($2,241), 13 ruy5u ($951), 4 dddd ($244).
Per region: west is dominated by 14/13/8; east by 4/11/7; central by 6/3/2.

**Customer lifetime value** – 42492 (west, enterprise, active) $3,645.64; 42491 $352.43 (inactive);
32483 $319.07 (inactive); 21456 $88.32 (active).

**Duplicates / faulty** – A-005 line seen 3×; faulty lines: A-013 (qty 0.1), A-021 (no customer),
A-022 (no customer, cancelled).

Definitions used: revenue is only counted where `is_valid_sale` (not faulty, not cancelled).
`created` and `paid` orders are counted as revenue; if the business wants only shipped/paid, that is
a one-line filter change.

## 6. Trade-offs and what I'd change at 100× scale

What I simplified:
- DuckDB file instead of Snowflake; pandas instead of Spark. Fine for KBs of data.
- Full reload every run (drop and recreate). No incremental logic.
- Customer dimension is "latest snapshot" only – history of the active flag is lost.
- Faulty rows are kept and flagged rather than sent to a separate quarantine table.
- No unit tests; validation is the DQ log.

At 100× (or 10,000×) scale:
- Land raw files in S3 / a Snowflake stage, load with `COPY INTO` into raw tables, keep raw immutable.
- Make `dim_customer` an SCD Type 2 (valid_from / valid_to) so the active/inactive changes are preserved and CLV can be split by status at the time of the order.
- Incremental loads on `fact_order_line` keyed by `order_date`, with a `MERGE` on (order_id, prod_id) so re-delivered files don't duplicate.
- Move cleaning into Spark/Glue or dbt models instead of pandas; partition the fact by date; cluster on customer_id/product_id in Snowflake.
- Proper quarantine table for faulty rows plus alerting when the reject rate crosses a threshold.
- Orchestrate with Airflow (see `airflow_dag.py`) with retries, SLAs and backfill support.
- Add tests (dbt tests or Great Expectations): uniqueness of keys, referential integrity, quantity > 0, rate present for every order line.

## 7. Use of AI tools

I have used Claude's help for this assignment, it helped me to design the pipeline, also used to give the
skeleton of the pipeline and helped me to generate the scripts.
The decisions which are related to data quality are implemented by myself like star-schemas,
flag as dont delete for bad rows and also it should take the latest snapshot for customers. I have run the 
pipeline end-to-end and checked the output by myself.