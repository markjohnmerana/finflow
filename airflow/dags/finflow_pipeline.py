# airflow/dags/finflow_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from ingestion.ingest import ingest_entity
from ingestion.loader import load_entity_to_postgres
from config.settings import (
    CUSTOMER_LIMIT,
    ACCOUNT_LIMIT,
    TRANSACTION_LIMIT
)

# ============================================
# DEFAULT ARGUMENTS
# ============================================
default_args = {
    "owner": "finflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# ============================================
# DAG DEFINITION
# ============================================
with DAG(
    dag_id="finflow_pipeline",
    description="FinFlow: API → Bronze Lake → PostgreSQL → dbt",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    tags=["finflow", "finance", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    # ── Ingestion (parallel) ──────────────────────────────────
    ingest_customers = PythonOperator(
        task_id="ingest_customers",
        python_callable=ingest_entity,
        op_kwargs={"entity": "customers", "limit": CUSTOMER_LIMIT},
    )
    ingest_accounts = PythonOperator(
        task_id="ingest_accounts",
        python_callable=ingest_entity,
        op_kwargs={"entity": "accounts", "limit": ACCOUNT_LIMIT},
    )
    ingest_transactions = PythonOperator(
        task_id="ingest_transactions",
        python_callable=ingest_entity,
        op_kwargs={"entity": "transactions", "limit": TRANSACTION_LIMIT},
    )

    # ── Bronze Load (parallel) ────────────────────────────────
    load_customers = PythonOperator(
        task_id="load_customers_to_postgres",
        python_callable=load_entity_to_postgres,
        op_kwargs={"entity": "customers"},
    )
    load_accounts = PythonOperator(
        task_id="load_accounts_to_postgres",
        python_callable=load_entity_to_postgres,
        op_kwargs={"entity": "accounts"},
    )
    load_transactions = PythonOperator(
        task_id="load_transactions_to_postgres",
        python_callable=load_entity_to_postgres,
        op_kwargs={"entity": "transactions"},
    )

    # ── dbt Placeholders ─────────────────────────────────────
    run_dbt_silver = EmptyOperator(task_id="run_dbt_silver")
    run_dbt_gold   = EmptyOperator(task_id="run_dbt_gold")

    end = EmptyOperator(task_id="end")

    # ── Dependencies ─────────────────────────────────────────
    start >> [ingest_customers, ingest_accounts, ingest_transactions]

    ingest_customers    >> load_customers
    ingest_accounts     >> load_accounts
    ingest_transactions >> load_transactions

    [load_customers, load_accounts, load_transactions] >> run_dbt_silver
    run_dbt_silver >> run_dbt_gold >> end