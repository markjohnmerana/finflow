# airflow/dags/clients/minio_client.py
import boto3
from botocore.client import Config
from config.settings import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET
)
from utils.logger import get_logger

logger = get_logger(__name__)


def get_minio_client():
    """Returns a configured MinIO/S3 client."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )


def ensure_bucket_exists(bucket_name: str = MINIO_BUCKET):
    """Creates the bucket if it doesn't already exist."""
    client = get_minio_client()
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists.")
    except Exception:
        client.create_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' created successfully.")
    return client