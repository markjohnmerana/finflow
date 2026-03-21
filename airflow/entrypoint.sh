#!/bin/bash
set -e

echo "==> Installing additional Python packages..."
pip install --no-cache-dir \
  apache-airflow-providers-postgres==5.10.0 \
  apache-airflow-providers-amazon==8.19.0 \
  boto3==1.34.0 \
  requests==2.31.0 \
  psycopg2-binary==2.9.9

echo "==> Running Airflow DB migration..."
airflow db migrate

echo "==> Creating Airflow admin user..."
airflow users create --username "${_AIRFLOW_WWW_USER_USERNAME}" --password "${_AIRFLOW_WWW_USER_PASSWORD}" --firstname Admin --lastname User --role Admin --email admin@finflow.com || true

echo "==> Starting Airflow scheduler in background..."
airflow scheduler &

echo "==> Starting Airflow webserver..."
airflow webserver