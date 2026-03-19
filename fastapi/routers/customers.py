from fastapi import APIRouter
from faker import Faker
import random
import uuid

router = APIRouter()
fake = Faker()

# Seed for reproducibility — same data every run
Faker.seed(42)
random.seed(42)

RISK_LEVELS = ["low", "medium", "high"]
CUSTOMER_SEGMENTS = ["retail", "corporate", "premium", "basic"]

def generate_customer():
    return {
        "customer_id": str(uuid.uuid4()),
        "full_name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "date_of_birth": fake.date_of_birth(
            minimum_age=18, 
            maximum_age=70
        ).isoformat(),
        "address": fake.address().replace("\n", ", "),
        "risk_level": random.choice(RISK_LEVELS),
        "customer_segment": random.choice(CUSTOMER_SEGMENTS),
        "created_at": fake.date_time_this_decade().isoformat(),
        "is_active": random.choice([True, True, True, False])
    }

@router.get("/customers")
def get_customers(limit: int = 100):
    """
    Returns a list of mock bank customers.
    limit: number of customers to generate (default 100)
    """
    return {
        "source": "FinFlow Core Banking System",
        "count": limit,
        "data": [generate_customer() for _ in range(limit)]
    }