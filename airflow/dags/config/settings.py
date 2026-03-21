# airflow/dags/config/settings.py
import os

def _require(key: str) -> str:
    """
    Reads an environment variable.
    Raises an error immediately if it's missing.
    No silent fallbacks — fail fast in production.
    """
    value = os.getenv(key)
    if value is None:
        raise EnvironmentError(
            f"Required environment variable '{key}' is not set. "
            f"Check your .env file and docker-compose.yml."
        )
    return value

def _optional(key: str, default: str) -> str:
    """
    Reads an optional environment variable.
    Uses default only for non-sensitive config like limits.
    """
    return os.getenv(key, default)


# ============================================
# FastAPI
# ============================================
FASTAPI_BASE_URL = _optional("FASTAPI_BASE_URL", "http://fastapi:8000")

# ============================================
# MinIO
# ============================================
MINIO_ENDPOINT   = _optional("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = _require("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = _require("MINIO_SECRET_KEY")
MINIO_BUCKET     = _optional("MINIO_BUCKET", "finflow-bronze")

# ============================================
# PostgreSQL
# ============================================
POSTGRES_HOST = _optional("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(_optional("POSTGRES_PORT", "5432"))
POSTGRES_DB   = _require("POSTGRES_DB")
POSTGRES_USER = _require("POSTGRES_USER")
POSTGRES_PASS = _require("POSTGRES_PASSWORD")

# ============================================
# Pipeline
# ============================================
CUSTOMER_LIMIT    = int(_optional("CUSTOMER_LIMIT",    "100"))
ACCOUNT_LIMIT     = int(_optional("ACCOUNT_LIMIT",     "200"))
TRANSACTION_LIMIT = int(_optional("TRANSACTION_LIMIT", "500"))