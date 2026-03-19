from fastapi import APIRouter
from faker import Faker
import random
import uuid
from datetime import datetime, timedelta

router = APIRouter()
fake = Faker()

Faker.seed(42)
random.seed(42)

TRANSACTION_TYPES = ["debit", "credit", "transfer", "withdrawal", "deposit"]
TRANSACTION_STATUS = ["completed", "completed", "completed", "pending", "failed"]
MERCHANT_CATEGORIES = [
    "grocery", "electronics", "restaurant", "travel",
    "utilities", "healthcare", "entertainment", "retail"
]

def generate_transaction():
    amount = round(random.uniform(1, 50000), 2)
    avg_amount = round(random.uniform(100, 5000), 2)

    # Bake in fraud signals — some transactions are suspicious
    is_high_value = amount > (avg_amount * 3)
    is_rapid = random.random() < 0.05          # 5% chance
    is_foreign = random.random() < 0.08        # 8% chance

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": f"ACC-{str(uuid.uuid4())[:8].upper()}",
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "amount": amount,
        "currency": random.choice(["USD", "EUR", "PHP", "GBP"]),
        "merchant_name": fake.company(),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "transaction_status": random.choice(TRANSACTION_STATUS),
        "timestamp": fake.date_time_this_year().isoformat(),
        "location_country": fake.country_code(),
        "is_foreign_transaction": is_foreign,
        # Fraud signals — our dbt Gold layer will use these
        "signal_high_value": is_high_value,
        "signal_rapid_succession": is_rapid,
        "signal_foreign_transaction": is_foreign,
        "avg_account_amount": avg_amount,
    }

@router.get("/transactions")
def get_transactions(limit: int = 500):
    """
    Returns a list of mock financial transactions with fraud signals.
    limit: number of transactions to generate (default 500)
    """
    return {
        "source": "FinFlow Core Banking System",
        "count": limit,
        "data": [generate_transaction() for _ in range(limit)]
    }