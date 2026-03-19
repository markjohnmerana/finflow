# airflow/dags/ingestion/loader.py
import json
from clients.minio_client import get_minio_client
from clients.postgres_client import get_postgres_conn
from config.settings import MINIO_BUCKET
from utils.logger import get_logger

logger = get_logger(__name__)


def load_entity_to_postgres(entity: str, **context):
    """
    Reads raw JSON from MinIO.
    Loads into bronze schema in PostgreSQL.

    Args:
        entity: one of 'customers', 'accounts', 'transactions'
    """
    execution_date = context["ds"]
    object_key     = f"bronze/{entity}/{execution_date}/{entity}_raw.json"

    # ── 1. Read from MinIO ────────────────────────────────────
    logger.info(f"Reading from MinIO: {object_key}")
    client   = get_minio_client()
    response = client.get_object(Bucket=MINIO_BUCKET, Key=object_key)
    raw_data = json.loads(response["Body"].read().decode("utf-8"))
    records  = raw_data["data"]
    logger.info(f"Loaded {len(records)} {entity} records from MinIO.")

    # ── 2. Load into PostgreSQL ───────────────────────────────
    conn   = get_postgres_conn()
    cursor = conn.cursor()

    try:
        if entity == "customers":
            _load_customers(cursor, records, execution_date)
        elif entity == "accounts":
            _load_accounts(cursor, records, execution_date)
        elif entity == "transactions":
            _load_transactions(cursor, records, execution_date)

        conn.commit()
        logger.info(f"Successfully committed {entity} to bronze schema.")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to load {entity}: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# ── Private table loaders ─────────────────────────────────────

def _load_customers(cursor, records, execution_date):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_customers (
            customer_id      TEXT,
            full_name        TEXT,
            email            TEXT,
            phone            TEXT,
            date_of_birth    TEXT,
            address          TEXT,
            risk_level       TEXT,
            customer_segment TEXT,
            created_at       TEXT,
            is_active        BOOLEAN,
            ingested_at      TIMESTAMP DEFAULT NOW(),
            partition_date   DATE
        )
    """)
    cursor.execute(
        "DELETE FROM bronze.raw_customers WHERE partition_date = %s",
        (execution_date,)
    )
    for r in records:
        cursor.execute("""
            INSERT INTO bronze.raw_customers VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)
        """, (
            r["customer_id"], r["full_name"], r["email"],
            r["phone"], r["date_of_birth"], r["address"],
            r["risk_level"], r["customer_segment"],
            r["created_at"], r["is_active"], execution_date
        ))
    logger.info(f"Inserted {len(records)} customers into bronze.")


def _load_accounts(cursor, records, execution_date):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_accounts (
            account_id          TEXT,
            customer_id         TEXT,
            account_type        TEXT,
            account_status      TEXT,
            balance             NUMERIC,
            currency            TEXT,
            credit_limit        NUMERIC,
            opened_date         TEXT,
            last_activity_date  TEXT,
            is_negative_balance BOOLEAN,
            ingested_at         TIMESTAMP DEFAULT NOW(),
            partition_date      DATE
        )
    """)
    cursor.execute(
        "DELETE FROM bronze.raw_accounts WHERE partition_date = %s",
        (execution_date,)
    )
    for r in records:
        cursor.execute("""
            INSERT INTO bronze.raw_accounts VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)
        """, (
            r["account_id"], r["customer_id"], r["account_type"],
            r["account_status"], r["balance"], r["currency"],
            r["credit_limit"], r["opened_date"],
            r["last_activity_date"], r["is_negative_balance"],
            execution_date
        ))
    logger.info(f"Inserted {len(records)} accounts into bronze.")


def _load_transactions(cursor, records, execution_date):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.raw_transactions (
            transaction_id             TEXT,
            account_id                 TEXT,
            transaction_type           TEXT,
            amount                     NUMERIC,
            currency                   TEXT,
            merchant_name              TEXT,
            merchant_category          TEXT,
            transaction_status         TEXT,
            timestamp                  TEXT,
            location_country           TEXT,
            is_foreign_transaction     BOOLEAN,
            signal_high_value          BOOLEAN,
            signal_rapid_succession    BOOLEAN,
            signal_foreign_transaction BOOLEAN,
            avg_account_amount         NUMERIC,
            ingested_at                TIMESTAMP DEFAULT NOW(),
            partition_date             DATE
        )
    """)
    cursor.execute(
        "DELETE FROM bronze.raw_transactions WHERE partition_date = %s",
        (execution_date,)
    )
    for r in records:
        cursor.execute("""
            INSERT INTO bronze.raw_transactions VALUES
            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s)
        """, (
            r["transaction_id"], r["account_id"], r["transaction_type"],
            r["amount"], r["currency"], r["merchant_name"],
            r["merchant_category"], r["transaction_status"],
            r["timestamp"], r["location_country"],
            r["is_foreign_transaction"], r["signal_high_value"],
            r["signal_rapid_succession"], r["signal_foreign_transaction"],
            r["avg_account_amount"], execution_date
        ))
    logger.info(f"Inserted {len(records)} transactions into bronze.")