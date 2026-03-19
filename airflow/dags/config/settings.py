# airflow/dags/config/settings.py
import os

# ============================================
# FastAPI
# ============================================
FASTAPI_BASE_URL = os.getenv("FASTAPI_BASE_URL", "http://fastapi:8000")

# ============================================
# MinIO
# ============================================
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "finflow_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "finflow2024")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "finflow-bronze")

# ============================================
# PostgreSQL
# ============================================
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB   = os.getenv("POSTGRES_DB", "finflow_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "finflow_user")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "finflow2024")

# ============================================
# Pipeline
# ============================================
CUSTOMER_LIMIT    = int(os.getenv("CUSTOMER_LIMIT", "100"))
ACCOUNT_LIMIT     = int(os.getenv("ACCOUNT_LIMIT", "200"))
TRANSACTION_LIMIT = int(os.getenv("TRANSACTION_LIMIT", "500"))