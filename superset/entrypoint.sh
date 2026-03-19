#!/bin/bash
set -e

echo "==> Installing PostgreSQL driver into Superset venv..."
pip install --no-cache-dir \
  psycopg2-binary==2.9.9

echo "==> Upgrading Superset DB..."
superset db upgrade

echo "==> Creating admin user..."
superset fab create-admin \
  --username "${ADMIN_USERNAME}" \
  --firstname "Admin" \
  --lastname "User" \
  --email "admin@finflow.com" \
  --password "${ADMIN_PASSWORD}" || true

echo "==> Initializing Superset..."
superset init

echo "==> Starting Superset server..."
superset run -h 0.0.0.0 -p 8088 --with-threads --reload