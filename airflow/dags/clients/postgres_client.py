# airflow/dags/clients/postgres_client.py
import psycopg2
from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASS
)
from utils.logger import get_logger

logger = get_logger(__name__)


def get_postgres_conn():
    """Returns a live PostgreSQL connection."""
    logger.info(f"Connecting to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}")
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASS
    )


def execute_query(query: str, params: tuple = None):
    """
    Executes a single query and commits.
    Use for DDL (CREATE TABLE) and simple DML.
    """
    conn   = get_postgres_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(query, params)
        conn.commit()
        logger.info("Query executed successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Query failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()