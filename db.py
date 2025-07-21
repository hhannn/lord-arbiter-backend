# db.py
import os
import threading
import time
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from contextlib import contextmanager
from typing import Dict, Any, Union

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
        
class CacheEntry:
    """Represents an entry in the cache with its data and expiration timestamp."""
    def __init__(self, data: Dict[str, Any], ttl_seconds: int = 300):  # Default 5 minute TTL
        self.data = data
        self.expires_at = time.time() + ttl_seconds

class SmartCache:
    """
    A thread-safe in-memory cache with Time-To-Live (TTL) for entries.
    Designed for storing user API keys.
    """
    def __init__(self):
        self._cache: Dict[int, CacheEntry] = {}
        self._lock = threading.Lock() # Ensures thread safety for cache operations

    def get(self, user_id: int) -> Union[Dict[str, Any], None]:
        """
        Retrieves data for a given user_id from the cache.
        Returns None if not found or expired.
        """
        with self._lock:
            entry = self._cache.get(user_id)
            if entry and time.time() < entry.expires_at:
                print(f"DEBUG: Cache hit for user_id {user_id}") # Optional: for debugging cache hits
                return entry.data
            print(f"DEBUG: Cache miss or expired for user_id {user_id}") # Optional: for debugging cache misses
            return None

    def set(self, user_id: int, data: Dict[str, Any], ttl_seconds: int = 300):
        """
        Stores data for a given user_id in the cache with a specified TTL.
        """
        with self._lock:
            self._cache[user_id] = CacheEntry(data, ttl_seconds)
            print(f"DEBUG: Cache set for user_id {user_id}, expires in {ttl_seconds}s") # Optional: for debugging cache sets

    def invalidate(self, user_id: int):
        """Removes a specific user's entry from the cache."""
        with self._lock:
            self._cache.pop(user_id, None)
            print(f"DEBUG: Cache invalidated for user_id {user_id}") # Optional: for debugging invalidation
