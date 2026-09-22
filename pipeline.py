"""
Simple end-to-end ETL pipeline: raw files -> cleaned data -> star schema -> metrics.

Run:  python pipeline.py

Steps (each one is a plain function, run in order by main()):
  1. extract()   - read the 5 source files
  2. clean()     - fix data quality issues, write cleaned CSVs to data/clean/
  3. load()      - build dim/fact tables in DuckDB (warehouse.duckdb)
  4. metrics()   - run sql/metrics.sql and save results to output/
"""

import json
import os
from datetime import datetime

import duckdb
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(BASE, "data", "raw")
CLEAN = os.path.join(BASE, "data", "clean")
OUT = os.path.join(BASE, "output")
DB = os.path.join(BASE, "warehouse.duckdb")

dq_log = []  # simple list of data-quality findings, printed at the end


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}")


# ---------------------------------------------------------------- 1. EXTRACT
def extract():
    log("Extract: reading raw files")
    customers = pd.read_csv(f"{RAW}/customer.csv")
    orders = pd.read_csv(f"{RAW}/orders.csv")
    products = pd.read_csv(f"{RAW}/product.csv")
    categories = pd.read_csv(f"{RAW}/prod_cat_tree.csv")
    with open(f"{RAW}/currency_conversion.json") as f:
        rates = pd.DataFrame(json.load(f))
    log(f"  customers={len(customers)} orders={len(orders)} products={len(products)} "
        f"categories={len(categories)} rates={len(rates)}")
    return customers, orders, products, categories, rates


# ------------------------------------------------------------------ 2. CLEAN
def clean(customers, orders, products, categories, rates):
    log("Clean: standardising and fixing data")

    # --- customers -------------------------------------------------------
    # Same cust_id appears many times with different created_at / active flag.
    # Treat each row as a snapshot; keep the latest row per customer.
    customers["created_at"] = pd.to_datetime(customers["created_at"], format="%m/%d/%Y")
    dupes = customers["cust_id"].duplicated().sum()
    dq_log.append(f"customers: {dupes} repeated cust_id rows -> kept latest snapshot per customer")
    customers = (customers.sort_values("created_at")
                          .groupby("cust_id", as_index=False).last())
    customers["is_active"] = customers["active"].str.strip().str.lower().eq("y")
    customers["country"] = customers["country"].str.strip().str.upper()
    customers["region"] = customers["region"].str.strip().str.lower()
    customers["customer_type"] = customers["type"].str.strip().str.lower()
    customers = customers[["cust_id", "created_at", "country", "region",
                           "customer_type", "zip", "is_active"]]

    # --- categories ------------------------------------------------------
    # child/parent tree. "Sweet" is a category (cat_id 1) whose parent is "all".
    # "salt" is only referenced as a parent, never as its own row.
    categories["child"] = categories["child"].str.strip()
    categories["parent"] = categories["parent"].str.strip()
    categories = categories.rename(columns={"child": "category_name",
                                            "parent": "parent_category"})
    # top-level group = the category itself if its parent is "all", else the parent
    categories["category_group"] = categories.apply(
        lambda r: r["category_name"] if r["parent_category"].lower() == "all"
        else r["parent_category"], axis=1)

    # --- products --------------------------------------------------------
    products["prod_name"] = products["prod_name"].str.strip()
    products["currency"] = products["currency"].str.strip().str.upper()  # "Yen" -> "YEN"
    dq_log.append("products: currency values normalised to upper case (Yen -> YEN)")
    products = products.merge(categories[["cat_id", "category_name", "category_group"]],
                              on="cat_id", how="left")
    missing_cat = products["category_name"].isna().sum()
    if missing_cat:
        dq_log.append(f"products: {missing_cat} products with unknown cat_id")

    # --- currency rates --------------------------------------------------
    rates["date"] = pd.to_datetime(rates["date"])
    rates["currency"] = rates["currency"].str.strip().str.upper()
    rates = rates.drop_duplicates(["date", "currency"])
    rates = rates.rename(columns={"date": "rate_date", "conversion": "rate_to_usd"})

    # --- orders ----------------------------------------------------------
    orders = orders.rename(columns={"order date": "order_ts"})
    orders["order_ts"] = pd.to_datetime(orders["order_ts"], format="%m/%d/%Y %H:%M")
    orders["order_date"] = orders["order_ts"].dt.normalize()
    orders["status"] = orders["status"].str.strip().str.lower()

    # order_id format is inconsistent (A-21 vs A-021) -> zero-pad the number
    orders["order_id"] = orders["order_id"].str.strip().str.replace(
        r"^A-(\d+)$", lambda m: f"A-{int(m.group(1)):03d}", regex=True)

    # exact duplicate rows (same order, product, qty, time) -> keep one
    before = len(orders)
    orders = orders.drop_duplicates()
    dq_log.append(f"orders: {before - len(orders)} exact duplicate lines removed")

    # flag issues but keep the rows so they can be reported (task 5)
    orders["is_faulty"] = False
    orders["fault_reason"] = None

    no_cust = orders["cust_id"].isna()
    orders.loc[no_cust, ["is_faulty", "fault_reason"]] = [True, "missing cust_id"]

    bad_qty = (orders["quantity"] <= 0) | (orders["quantity"] % 1 != 0)
    orders.loc[bad_qty, ["is_faulty", "fault_reason"]] = [True, "non-integer or zero quantity"]

    unknown_cust = orders["cust_id"].notna() & ~orders["cust_id"].isin(customers["cust_id"])
    orders.loc[unknown_cust, ["is_faulty", "fault_reason"]] = [True, "cust_id not in customer table"]

    unknown_prod = ~orders["prod_id"].isin(products["prod_id"])
    orders.loc[unknown_prod, ["is_faulty", "fault_reason"]] = [True, "prod_id not in product table"]

    dq_log.append(f"orders: {no_cust.sum()} lines missing cust_id, "
                  f"{bad_qty.sum()} lines with bad quantity, "
                  f"{unknown_cust.sum()} unknown customers, {unknown_prod.sum()} unknown products")

    # orders placed before the customer record was created (2018-2020 orders, 2025 customers)
    tmp = orders.merge(customers[["cust_id", "created_at"]], on="cust_id", how="left")
    early = (tmp["order_ts"] < tmp["created_at"]).sum()
    dq_log.append(f"orders: {early} lines dated before customer created_at "
                  f"(kept, looks like created_at is a snapshot date not a signup date)")

    # attach price / currency and convert to USD using the rate on the order date
    orders = orders.merge(products[["prod_id", "price", "currency"]], on="prod_id", how="left")
    orders = orders.sort_values("order_date")
    rates_sorted = rates.sort_values("rate_date")
    orders = pd.merge_asof(orders, rates_sorted, left_on="order_date", right_on="rate_date",
                           by="currency", direction="backward")  # latest rate on/before date
    no_rate = orders["rate_to_usd"].isna().sum()
    if no_rate:
        dq_log.append(f"orders: {no_rate} lines had no currency rate available")
    orders["unit_price_usd"] = (orders["price"] * orders["rate_to_usd"]).round(4)
    orders["revenue_usd"] = (orders["unit_price_usd"] * orders["quantity"]).round(4)

    # revenue only from paid/shipped; created = activity only; cancelled excluded
    orders["is_valid_sale"] = (~orders["is_faulty"]) & (orders["status"].isin(["paid", "shipped"]))

    orders["cust_id"] = orders["cust_id"].astype("Int64")
    orders = orders[["order_id", "cust_id", "prod_id", "order_ts", "order_date", "status",
                     "quantity", "currency", "price", "rate_to_usd", "unit_price_usd",
                     "revenue_usd", "is_faulty", "fault_reason", "is_valid_sale"]]

    # save cleaned copies
    os.makedirs(CLEAN, exist_ok=True)
    customers.to_csv(f"{CLEAN}/customers.csv", index=False)
    products.to_csv(f"{CLEAN}/products.csv", index=False)
    categories.to_csv(f"{CLEAN}/categories.csv", index=False)
    rates.to_csv(f"{CLEAN}/currency_rates.csv", index=False)
    orders.to_csv(f"{CLEAN}/order_lines.csv", index=False)
    log("  cleaned files written to data/clean/")
    return customers, orders, products, categories, rates


# ------------------------------------------------------------------- 3. LOAD
def load(customers, orders, products, categories, rates):
    log("Load: building star schema in DuckDB")
    if os.path.exists(DB):
        os.remove(DB)
    con = duckdb.connect(DB)

    con.register("customers_df", customers)
    con.register("products_df", products)
    con.register("rates_df", rates)
    con.register("orders_df", orders)

    with open(os.path.join(BASE, "sql", "create_model.sql")) as f:
        con.execute(f.read())

    for t in ["dim_customer", "dim_product", "dim_date", "dim_currency_rate", "fact_order_line"]:
        n = con.execute(f"select count(*) from {t}").fetchone()[0]
        log(f"  {t}: {n} rows")
    con.close()


# ---------------------------------------------------------------- 4. METRICS
def metrics():
    log("Metrics: running sql/metrics.sql")
    os.makedirs(OUT, exist_ok=True)
    con = duckdb.connect(DB, read_only=True)
    with open(os.path.join(BASE, "sql", "metrics.sql")) as f:
        sql = f.read()

    # metrics.sql has queries separated by "-- name: <metric>" comments
    blocks = [b for b in sql.split("-- name: ") if b.strip()]
    report = []
    for b in blocks:
        name, query = b.split("\n", 1)
        name = name.strip()
        df = con.execute(query).df()
        df.to_csv(f"{OUT}/{name}.csv", index=False)
        report.append(f"### {name}\n\n```sql\n{query.strip()}\n```\n\n{df.to_markdown(index=False)}\n")
        log(f"  {name}: {len(df)} rows")
    con.close()
    with open(f"{OUT}/metrics_output.md", "w") as f:
        f.write("# Metric queries and sample outputs\n\n" + "\n".join(report))


def main():
    raw = extract()
    cleaned = clean(*raw)
    load(*cleaned)
    metrics()
    log("Done. Data quality findings:")
    for line in dq_log:
        print("  -", line)
    with open(f"{OUT}/dq_findings.txt", "w") as f:
        f.write("\n".join(dq_log))


if __name__ == "__main__":
    main()
