# db.py
import os
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from contextlib import contextmanager
from typing import Dict, Any, Union
import threading
import time
import traceback # Added for logging tracebacks in DB functions

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

db_pool: SimpleConnectionPool = None

def init_pool(minconn=1, maxconn=10):
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    db_pool = SimpleConnectionPool(minconn, maxconn, dsn=DATABASE_URL)
    print("✅ Initialized DB connection pool")

def get_conn():
    if db_pool is None:
        print("❌ DB pool is None inside get_conn() - Pool not initialized?")
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

# --- NEW CACHE CLASSES ---
class CacheEntry:
    def __init__(self, data: Dict[str, Any], ttl_seconds: int = 300):
        self.data = data
        self.expires_at = time.time() + ttl_seconds

class SmartCache:
    def __init__(self):
        self._cache: Dict[int, CacheEntry] = {}
        self._lock = threading.Lock()

    def get(self, user_id: int) -> Union[Dict[str, Any], None]:
        with self._lock:
            entry = self._cache.get(user_id)
            if entry and time.time() < entry.expires_at:
                return entry.data
            return None

    def set(self, user_id: int, data: Dict[str, Any], ttl_seconds: int = 300):
        with self._lock:
            self._cache[user_id] = CacheEntry(data, ttl_seconds)

    def invalidate(self, user_id: int):
        with self._lock:
            self._cache.pop(user_id, None)

# Initialize the global smart cache instance for user keys
_user_keys_smart_cache = SmartCache()

# --- MOVED: get_user_keys function is now in db.py ---
def get_user_keys(user_id: Union[int, str]) -> Union[Dict[str, Any], None]:
    """
    Fetches user API keys for a single user, prioritizing the SmartCache.
    This function is now part of db.py.
    """
    try:
        user_id_int = int(user_id)
    except ValueError:
        print(f"❌ Invalid user_id format received in get_user_keys: {user_id}")
        return None

    # Try to get from the smart cache first
    cached_keys = _user_keys_smart_cache.get(user_id_int)
    if cached_keys:
        return cached_keys

    # If not in cache or expired, fetch from DB (single query)
    print(f"DEBUG: DB: User keys for {user_id_int} not in cache, fetching from DB (single query).")
    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT api_key, api_secret FROM users WHERE id = %s", (user_id_int,))
                user_keys = cur.fetchone()
                if user_keys:
                    _user_keys_smart_cache.set(user_id_int, user_keys) # Store newly fetched key in cache
                    return user_keys
                return None
    except Exception as e:
        print(f"❌ Error fetching user keys for {user_id_int} from DB (get_user_keys): {e}")
        traceback.print_exc()
        return None