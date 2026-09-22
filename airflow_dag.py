"""
Orchestration example (Airflow). Not required to run the assignment locally -
`python pipeline.py` already runs the same four steps in order.
This just shows how the same functions would be scheduled in production.
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

import pipeline

with DAG(
    dag_id="orders_etl_daily",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",      # every day at 02:00
    catchup=False,
) as dag:

    def _extract_and_clean(**_):
        raw = pipeline.extract()
        pipeline.clean(*raw)          # writes data/clean/*.csv

    def _load(**_):
        import pandas as pd
        c = pipeline.CLEAN
        pipeline.load(
            pd.read_csv(f"{c}/customers.csv", parse_dates=["created_at"]),
            pd.read_csv(f"{c}/order_lines.csv", parse_dates=["order_ts", "order_date", "rate_date"]),
            pd.read_csv(f"{c}/products.csv"),
            pd.read_csv(f"{c}/categories.csv"),
            pd.read_csv(f"{c}/currency_rates.csv", parse_dates=["rate_date"]),
        )

    extract_clean = PythonOperator(task_id="extract_and_clean", python_callable=_extract_and_clean)
    load = PythonOperator(task_id="load_star_schema", python_callable=_load)
    metrics = PythonOperator(task_id="run_metrics", python_callable=pipeline.metrics)

    extract_clean >> load >> metrics
