# airflow/dags/finflow_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from helpers import (
    ingest_entity,
    load_entity_to_postgres
)

# ============================================
# DAG DEFAULT ARGUMENTS
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
    schedule_interval="0 6 * * *",  # Run daily at 6AM UTC
    catchup=False,
    tags=["finflow", "finance", "etl"],
) as dag:

    # ------------------------------------------
    # PIPELINE START
    # ------------------------------------------
    start = EmptyOperator(task_id="start")

    # ------------------------------------------
    # LAYER 1: INGEST FROM FASTAPI → MINIO
    # (runs in parallel — fan-out)
    # ------------------------------------------
    ingest_customers = PythonOperator(
        task_id="ingest_customers",
        python_callable=ingest_entity,
        op_kwargs={"entity": "customers", "limit": 100},
    )

    ingest_accounts = PythonOperator(
        task_id="ingest_accounts",
        python_callable=ingest_entity,
        op_kwargs={"entity": "accounts", "limit": 200},
    )

    ingest_transactions = PythonOperator(
        task_id="ingest_transactions",
        python_callable=ingest_entity,
        op_kwargs={"entity": "transactions", "limit": 500},
    )

    # ------------------------------------------
    # LAYER 2: LOAD BRONZE → POSTGRESQL
    # (runs after all ingestions complete — fan-in)
    # ------------------------------------------
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

    # ------------------------------------------
    # LAYER 3: DBT TRANSFORMATIONS
    # (placeholder — we wire these in Layer 5)
    # ------------------------------------------
    run_dbt_silver = EmptyOperator(task_id="run_dbt_silver")
    run_dbt_gold   = EmptyOperator(task_id="run_dbt_gold")

    # ------------------------------------------
    # PIPELINE END
    # ------------------------------------------
    end = EmptyOperator(task_id="end")

    # ------------------------------------------
    # TASK DEPENDENCIES — THE PIPELINE FLOW
    # ------------------------------------------
    # Start
    start >> [ingest_customers, ingest_accounts, ingest_transactions]

    # Fan-in: all ingestions must complete before loading
    ingest_customers   >> load_customers
    ingest_accounts    >> load_accounts
    ingest_transactions >> load_transactions

    # After all loads complete → dbt
    [load_customers, load_accounts, load_transactions] >> run_dbt_silver

    # Silver → Gold → End
    run_dbt_silver >> run_dbt_gold >> end