from fastapi import FastAPI
from routers import customers, accounts, transactions

app = FastAPI(
    title="FinFlow Core Banking API",
    description="Mock banking data source for the FinFlow pipeline",
    version="1.0.0"
)

# Register routers
app.include_router(customers.router, prefix="/api", tags=["Customers"])
app.include_router(accounts.router, prefix="/api", tags=["Accounts"])
app.include_router(transactions.router, prefix="/api", tags=["Transactions"])

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "FinFlow Core Banking API"}