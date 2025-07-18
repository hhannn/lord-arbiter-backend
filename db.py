# db.py
import os
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from contextlib import contextmanager

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

db_pool: SimpleConnectionPool = None  # Will be initialized in lifespan

def init_pool(minconn=1, maxconn=10):
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    db_pool = SimpleConnectionPool(minconn, maxconn, dsn=DATABASE_URL)
    print("✅ Initialized DB connection pool")


def get_conn():
    if db_pool is None:
        print("❌ DB pool is None inside get_conn()")
        raise RuntimeError("DB pool not initialized")
    return db_pool.getconn()


def put_conn(conn):
    if db_pool is not None and conn:
        db_pool.putconn(conn)


def close_pool():
    if db_pool is not None:
        db_pool.closeall()
        print("🧹 Closed DB connection pool")
        
@contextmanager
def with_db_conn():
    conn = get_conn()
    try:
        yield conn
    finally:
        put_conn(conn)
