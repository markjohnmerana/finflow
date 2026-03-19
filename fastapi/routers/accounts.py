from fastapi import APIRouter
from faker import Faker
import random
import uuid

router = APIRouter()
fake = Faker()

Faker.seed(42)
random.seed(42)

ACCOUNT_TYPES = ["savings", "checking", "credit", "investment"]
ACCOUNT_STATUS = ["active", "active", "active", "dormant", "closed"]
CURRENCIES = ["USD", "EUR", "PHP", "GBP"]

def generate_account(customer_id: str = None):
    balance = round(random.uniform(-500, 150000), 2)
    return {
        "account_id": f"ACC-{str(uuid.uuid4())[:8].upper()}",
        "customer_id": customer_id or str(uuid.uuid4()),
        "account_type": random.choice(ACCOUNT_TYPES),
        "account_status": random.choice(ACCOUNT_STATUS),
        "balance": balance,
        "currency": random.choice(CURRENCIES),
        "credit_limit": round(random.uniform(1000, 50000), 2),
        "opened_date": fake.date_this_decade().isoformat(),
        "last_activity_date": fake.date_this_year().isoformat(),
        # Flag accounts with negative balance — useful for Gold layer later
        "is_negative_balance": balance < 0
    }

@router.get("/accounts")
def get_accounts(limit: int = 200):
    """
    Returns a list of mock bank accounts.
    limit: number of accounts to generate (default 200)
    """
    return {
        "source": "FinFlow Core Banking System",
        "count": limit,
        "data": [generate_account() for _ in range(limit)]
    }