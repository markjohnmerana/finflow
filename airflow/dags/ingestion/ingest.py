# airflow/dags/ingestion/ingest.py
import json
import requests
from config.settings import FASTAPI_BASE_URL, MINIO_BUCKET
from clients.minio_client import get_minio_client, ensure_bucket_exists
from utils.logger import get_logger

logger = get_logger(__name__)


def ingest_entity(entity: str, limit: int, **context):
    """
    Pulls data from FastAPI endpoint.
    Saves raw JSON to MinIO bronze layer partitioned by date.

    Args:
        entity: one of 'customers', 'accounts', 'transactions'
        limit:  number of records to fetch
    """
    execution_date = context["ds"]  # YYYY-MM-DD

    # ── 1. Call FastAPI ────────────────────────────────────────
    url = f"{FASTAPI_BASE_URL}/api/{entity}?limit={limit}"
    logger.info(f"Calling FastAPI: {url}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    logger.info(f"Received {data['count']} {entity} records from FastAPI.")

    # ── 2. Save to MinIO (Bronze Layer) ───────────────────────
    client     = get_minio_client()
    ensure_bucket_exists(MINIO_BUCKET)

    object_key = f"bronze/{entity}/{execution_date}/{entity}_raw.json"
    payload    = json.dumps(data, indent=2).encode("utf-8")

    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=object_key,
        Body=payload,
        ContentType="application/json"
    )

    logger.info(f"Saved to MinIO → s3://{MINIO_BUCKET}/{object_key}")
    return object_key