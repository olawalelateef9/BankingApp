import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, text
# New imports for Prometheus
from prometheus_client import Counter, make_asgi_app

DATABASE_URL = os.getenv("DATABASE_URL")
FRONTEND_ORIGIN = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

engine = create_engine(f"postgresql+psycopg2://{DATABASE_URL.split('://', 1)[1]}")

app = FastAPI(title="Techbleat Global Bank - User Service")

# 1. Define the Prometheus Counter
# This matches the metric name expected by your Grafana dashboard
REGISTERED_USERS_COUNTER = Counter(
    'banking_users_registered_total', 
    'Total number of registered users'
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_ORIGIN, "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 2. Expose the /metrics endpoint for Prometheus to scrape
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


class UserCreate(BaseModel):
    id: str
    full_name: str
    email: EmailStr


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/users")
def create_user(user: UserCreate):
    with engine.begin() as conn:
        existing = conn.execute(
            text("SELECT id FROM users WHERE id = :id OR email = :email"),
            {"id": user.id, "email": user.email},
        ).fetchone()

        if existing:
            raise HTTPException(status_code=400, detail="User ID or email already exists")

        conn.execute(
            text(
                '''
                INSERT INTO users (id, full_name, email)
                VALUES (:id, :full_name, :email)
                '''
            ),
            {"id": user.id, "full_name": user.full_name.title(), "email": user.email},
        )

        conn.execute(
            text(
                '''
                INSERT INTO accounts (user_id, balance)
                VALUES (:user_id, 0)
                '''
            ),
            {"user_id": user.id},
        )
        
        # 3. Increment the counter after successful database insertion
        REGISTERED_USERS_COUNTER.inc()

    return {"message": "User created successfully", "user_id": user.id}


@app.get("/users")
def list_users():
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                '''
                SELECT id, full_name, email, created_at
                FROM users
                ORDER BY created_at DESC
                '''
            )
        ).mappings().all()
        return [dict(row) for row in rows]