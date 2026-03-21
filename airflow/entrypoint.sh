#!/bin/bash
set -e

echo "==> Running Airflow DB migration..."
airflow db migrate

echo "==> Creating Airflow admin user..."
airflow users create --username "${_AIRFLOW_WWW_USER_USERNAME}" --password "${_AIRFLOW_WWW_USER_PASSWORD}" --firstname Admin --lastname User --role Admin --email admin@finflow.com || true

echo "==> Starting Airflow scheduler in background..."
airflow scheduler &

echo "==> Starting Airflow webserver..."
airflow webserver