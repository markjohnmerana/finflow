#!/bin/bash
set -e

echo "==> Upgrading Superset DB..."
superset db upgrade

echo "==> Creating admin user..."
superset fab create-admin \
  --username "${ADMIN_USERNAME}" \
  --firstname "Admin" \
  --lastname "User" \
  --email "admin@finflow.com" \
  --password "${ADMIN_PASSWORD}"

echo "==> Initializing Superset..."
superset init

echo "==> Starting Superset server..."
superset run -h 0.0.0.0 -p 8088 --with-threads --reload