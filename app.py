import sqlalchemy
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from starlette.requests import Request
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, List

from sandbox.settings import DatabaseSettings

app = FastAPI(title="Interview Sandbox API")

# ============ Response Models ============

class UserResponse(BaseModel):
    id: int
    email: str
    created: datetime
    updated: datetime

class TransactionResponse(BaseModel):
    id: str
    user_id: int
    amount: float
    currency: str
    subid: str
    pending: bool
    paid: bool
    created: datetime
    updated: datetime

class TransactionSummary(BaseModel):
    total_amount: float
    transaction_count: int
    average_amount: float
    currency: str

class UserStats(BaseModel):
    user_id: int
    email: str
    total_amount: float
    transaction_count: int
    average_amount: float
    pending_count: int
    paid_count: int

class DailySummary(BaseModel):
    date: str
    transaction_count: int
    total_amount: float

class TopUser(BaseModel):
    user_id: int
    email: str
    total_amount: float
    transaction_count: int

class SuspiciousTransaction(BaseModel):
    id: str
    user_id: int
    amount: float
    reason: str
    created: datetime

# ============ Middleware ============

@app.middleware("http")
async def open_connection(request: Request, call_next):
    s = DatabaseSettings()
    uri = (
        f"postgresql+psycopg://"
        f"{s.user.get_secret_value()}:"
        f"{s.password.get_secret_value()}@"
        f"{s.host.get_secret_value()}:"
        f"{s.port.get_secret_value()}/{s.database_name}"
    )
    with sqlalchemy.create_engine(uri).connect() as connection:
        request.state.connection = connection
        return await call_next(request)

# ============ Health Check ============

@app.get("/health")
async def health_check(request: Request):
    """Simple health check endpoint to verify database connection"""
    try:
        result = request.state.connection.execute(sqlalchemy.text("SELECT 1"))
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

# ============ User Endpoints ============

@app.get("/users", response_model=List[UserResponse])
async def list_users(request: Request, skip: int = Query(0, ge=0), limit: int = Query(10, ge=1, le=100)):
    """Get all users with pagination"""
    query = "SELECT id, email, created, updated FROM users ORDER BY id LIMIT :limit OFFSET :skip"
    result = request.state.connection.execute(
        sqlalchemy.text(query),
        {"limit": limit, "skip": skip}
    )
    users = [dict(row._mapping) for row in result]
    return users

@app.get("/users/{user_id}", response_model=UserResponse)
async def get_user(request: Request, user_id: int):
    """Get a specific user by ID"""
    query = "SELECT id, email, created, updated FROM users WHERE id = :user_id"
    result = request.state.connection.execute(
        sqlalchemy.text(query),
        {"user_id": user_id}
    )
    user = result.first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return dict(user._mapping)

@app.get("/users/{user_id}/transactions", response_model=List[TransactionResponse])
async def get_user_transactions(request: Request, user_id: int, limit: int = Query(50, ge=1, le=500)):
    """Get all transactions for a specific user"""
    query = """
        SELECT id, user_id, amount, currency, subid, pending, paid, created, updated
        FROM transactions
        WHERE user_id = :user_id
        ORDER BY created DESC
        LIMIT :limit
    """
    result = request.state.connection.execute(
        sqlalchemy.text(query),
        {"user_id": user_id, "limit": limit}
    )
    transactions = [dict(row._mapping) for row in result]
    return transactions

# ============ Transaction Endpoints ============

@app.get("/transactions", response_model=List[TransactionResponse])
async def list_transactions(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    pending: Optional[bool] = None,
    paid: Optional[bool] = None
):
    """Get transactions with optional filtering by status"""
    where_clauses = []
    params = {"limit": limit, "skip": skip}

    if pending is not None:
        where_clauses.append("pending = :pending")
        params["pending"] = pending

    if paid is not None:
        where_clauses.append("paid = :paid")
        params["paid"] = paid

    where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    query = f"""
        SELECT id, user_id, amount, currency, subid, pending, paid, created, updated
        FROM transactions
        {where_clause}
        ORDER BY created DESC
        LIMIT :limit OFFSET :skip
    """
    result = request.state.connection.execute(sqlalchemy.text(query), params)
    transactions = [dict(row._mapping) for row in result]
    return transactions

@app.get("/transactions/summary", response_model=TransactionSummary)
async def transaction_summary(request: Request):
    """Get aggregate transaction statistics across all users"""
    query = """
        SELECT
            COALESCE(SUM(amount), 0) as total_amount,
            COUNT(*) as transaction_count,
            ROUND(COALESCE(AVG(amount), 0)::numeric, 2)::float as average_amount,
            currency
        FROM transactions
        GROUP BY currency
        LIMIT 1
    """
    result = request.state.connection.execute(sqlalchemy.text(query))
    row = result.first()
    if not row:
        raise HTTPException(status_code=500, detail="Failed to fetch summary")
    data = dict(row._mapping)
    return data

@app.get("/transactions/user/{user_id}/stats", response_model=UserStats)
async def user_transaction_stats(request: Request, user_id: int):
    """Get transaction statistics for a specific user"""
    query = """
        SELECT
            :user_id as user_id,
            u.email,
            COALESCE(SUM(t.amount), 0) as total_amount,
            COUNT(t.id) as transaction_count,
            ROUND(COALESCE(AVG(t.amount), 0)::numeric, 2)::float as average_amount,
            COALESCE(SUM(CASE WHEN t.pending THEN 1 ELSE 0 END), 0) as pending_count,
            COALESCE(SUM(CASE WHEN t.paid THEN 1 ELSE 0 END), 0) as paid_count
        FROM users u
        LEFT JOIN transactions t ON u.id = t.user_id
        WHERE u.id = :user_id
        GROUP BY u.id, u.email
    """
    result = request.state.connection.execute(
        sqlalchemy.text(query),
        {"user_id": user_id}
    )
    row = result.first()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")
    return dict(row._mapping)

@app.get("/transactions/daily", response_model=List[DailySummary])
async def daily_transaction_summary(request: Request, days: int = Query(7, ge=1, le=365)):
    """Get transaction summaries grouped by day"""
    query = """
        SELECT
            DATE(created)::text as date,
            COUNT(*) as transaction_count,
            ROUND(SUM(amount)::numeric, 2)::float as total_amount
        FROM transactions
        WHERE created >= NOW() - INTERVAL '1 day' * :days
        GROUP BY DATE(created)
        ORDER BY date DESC
    """
    result = request.state.connection.execute(
        sqlalchemy.text(query),
        {"days": days}
    )
    summaries = [dict(row._mapping) for row in result]
    return summaries

@app.get("/transactions/pending", response_model=List[TransactionResponse])
async def get_pending_transactions(request: Request, limit: int = Query(50, ge=1, le=500)):
    """Get all pending transactions"""
    query = """
        SELECT id, user_id, amount, currency, subid, pending, paid, created, updated
        FROM transactions
        WHERE pending = true
        ORDER BY created DESC
        LIMIT :limit
    """
    result = request.state.connection.execute(
        sqlalchemy.text(query),
        {"limit": limit}
    )
    transactions = [dict(row._mapping) for row in result]
    return transactions

# ============ Interview Challenge Endpoints ============

@app.get("/reports/top-users", response_model=List[TopUser])
async def top_users_by_volume(request: Request, limit: int = Query(10, ge=1, le=100)):
    """Get top users ranked by total transaction amount (common interview task)"""
    query = """
        SELECT
            u.id as user_id,
            u.email,
            ROUND(SUM(t.amount)::numeric, 2)::float as total_amount,
            COUNT(t.id) as transaction_count
        FROM users u
        LEFT JOIN transactions t ON u.id = t.user_id
        GROUP BY u.id, u.email
        ORDER BY total_amount DESC
        LIMIT :limit
    """
    result = request.state.connection.execute(
        sqlalchemy.text(query),
        {"limit": limit}
    )
    users = [dict(row._mapping) for row in result]
    return users

@app.get("/reports/suspicious-transactions", response_model=List[SuspiciousTransaction])
async def suspicious_transactions(request: Request, limit: int = Query(50, ge=1, le=500)):
    """Find suspicious transactions (unusually large or rapid patterns)"""
    query = """
        SELECT
            id,
            user_id,
            amount,
            'Large transaction (>90th percentile)' as reason,
            created
        FROM transactions
        WHERE amount > (
            SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount) FROM transactions
        )
        ORDER BY amount DESC
        LIMIT :limit
    """
    result = request.state.connection.execute(
        sqlalchemy.text(query),
        {"limit": limit}
    )
    transactions = [dict(row._mapping) for row in result]
    return transactions

@app.get("/transactions/unpaid", response_model=List[TransactionResponse])
async def unpaid_transactions(request: Request, limit: int = Query(50, ge=1, le=500)):
    """Get unpaid but confirmed (non-pending) transactions"""
    query = """
        SELECT id, user_id, amount, currency, subid, pending, paid, created, updated
        FROM transactions
        WHERE pending = false AND paid = false
        ORDER BY created DESC
        LIMIT :limit
    """
    result = request.state.connection.execute(
        sqlalchemy.text(query),
        {"limit": limit}
    )
    transactions = [dict(row._mapping) for row in result]
    return transactions

if __name__ == "__main__":
    uvicorn.run("sandbox.app:app", host="0.0.0.0", port=5000, reload=True)
