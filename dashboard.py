# dashboard.py

import psycopg2
import os
import threading
import traceback

from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter, HTTPException, BackgroundTasks, Body, Response, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pybit.unified_trading import HTTP
from psycopg2.extras import RealDictCursor
from typing import Dict, Set, Any, Union

from bot_runner import BotRunner
from dotenv import load_dotenv
from runner_registry import running_threads, running_threads_lock
# Import the SmartCache instance from db.py
from db import init_pool, get_conn, put_conn, close_pool, with_db_conn, _user_keys_smart_cache

load_dotenv()

def get_user_keys(user_id: Union[int, str]) -> Union[Dict[str, Any], None]:
    """
    Fetches user API keys for a single user, prioritizing the SmartCache.
    This function is used by individual requests and BotRunner instances.
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
    print(f"DEBUG: Dashboard: User keys for {user_id_int} not in cache, fetching from DB (single query).")
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
        print(f"❌ Error fetching user keys for {user_id_int} from DB (Dashboard): {e}")
        traceback.print_exc()
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan event handler.
    Initializes DB pool and resumes running bots.
    (No batch pre-population of cache in this version)
    """
    try:
        init_pool()

        try:
            with with_db_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    # Fetch all necessary bot data to resume
                    cur.execute("SELECT * FROM bots WHERE status = 'running'")
                    bots_to_resume = cur.fetchall()

                    # Removed batch fetching and smart cache pre-population
                    # unique_user_ids = set()
                    # for bot in bots_to_resume: unique_user_ids.add(bot["user_id"])
                    # if unique_user_ids:
                    #     all_user_keys_map = get_multiple_user_keys(list(unique_user_ids))
                    #     print(f"✅ Pre-populated cache with {len(all_user_keys_map)} user keys for resuming bots.")

                    # Now, restart the bots. Their BotRunner instances will call get_user_keys
                    # which will use the smart cache on an individual basis.
                    for bot in bots_to_resume:
                        bot_id = bot["id"]
                        with running_threads_lock:
                            if bot_id in running_threads and running_threads[bot_id].is_alive():
                                print(f"⚠️ Bot {bot_id} already running, skipping resume.")
                                continue

                            # Create and start the BotRunner thread
                            runner = BotRunner(bot)
                            thread = threading.Thread(target=runner.run, daemon=True)
                            running_threads[bot_id] = thread
                            thread.start()
                            print(f"🔁 Resumed bot {bot_id}")

        except Exception as e:
            print("❌ Error in lifespan (resuming bots):", e)
            traceback.print_exc()

    except Exception as e:
        print("❌ Failed to initialize DB pool or resume bots (outer error):", e)
        traceback.print_exc()

    yield # Yield control to the application
    close_pool() # Close DB pool when application shuts down

router = APIRouter()

# --- Pydantic Models (unchanged, but included for context) ---
class LoginPayload(BaseModel):
    username: str
    password: str

class UserPayload(BaseModel):
    name: str
    api_key: str
    api_secret: str
    uid: str

class APIKeyPayload(BaseModel):
    apiKey: str
    apiSecret: str

class BotPositionPayload(BaseModel):
    asset: str
    user_id: int

class RegisterPayload(BaseModel):
    username: str
    password: str
    api_key: str
    api_secret: str

class CreateBotPayload(BaseModel):
    asset: str
    start_size: float
    leverage: int
    multiplier: float
    take_profit: float
    rebuy: float
    start_type: str

# --- API Endpoints (unchanged, but included for context) ---

@router.post("/api/user/register")
def register_user(payload: RegisterPayload):
    try:
        session = HTTP(api_key=payload.api_key, api_secret=payload.api_secret)
        user_data = session.get_api_key_information()
        uid = user_data["result"]["id"]
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid API credentials")

    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id FROM users WHERE username = %s", (payload.username,))
                if cur.fetchone():
                    raise HTTPException(status_code=400, detail="Username already exists")

                cur.execute("SELECT id FROM users WHERE uid = %s", (uid,))
                if cur.fetchone():
                    raise HTTPException(status_code=400, detail="Bybit account already registered")

                cur.execute("""
                    INSERT INTO users (username, password, api_key, api_secret, uid)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (payload.username, payload.password, payload.api_key, payload.api_secret, uid))

                new_user = cur.fetchone()
                conn.commit()
                return {"user_id": new_user["id"]}
    except Exception as e:
        print(f"❌ Register error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Database error during registration")


@router.post("/api/user/login")
def login_user(payload: LoginPayload, response: Response):
    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM users WHERE username = %s", (payload.username,))
                user = cur.fetchone()

                if not user or user["password"] != payload.password:
                    raise HTTPException(status_code=401, detail="Invalid credentials")

                # Set cookie
                response.set_cookie(
                    key="user_id",
                    value=str(user["id"]),
                    httponly=True,
                    samesite="None",
                    secure=True
                )
                # Invalidate cache for this user on login, to ensure fresh keys are loaded next time
                _user_keys_smart_cache.invalidate(user["id"])

                return {
                    "user_id": user["id"],
                    "username": user["username"],
                    "uid": user["uid"],
                }
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"❌ Login failed: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Login failed due to an internal error.")


@router.get("/api/user/data")
def get_user_data(request: Request):
    user_id = request.cookies.get("user_id")

    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")

    # This will use the smart cache
    user = get_user_keys(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found or keys unavailable")
    try:
        session = HTTP(
            testnet=False,
            api_key=user["api_key"],
            api_secret=user["api_secret"]
        )

        balance_data = session.get_wallet_balance(accountType="UNIFIED")
        pnl_data = session.get_closed_pnl(category="linear")
        user_data = session.get_api_key_information()

        return {
            "balance": balance_data,
            "closedPnL": pnl_data,
            "userData": user_data,
        }

    except Exception as e:
        print("❌ Error calling Bybit in get_user_data:", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Failed to fetch data from Bybit")

@router.post("/api/user/bots")
def get_bot_data(request: Request):
    user_id_str = request.cookies.get("user_id")

    if not user_id_str:
        raise HTTPException(status_code=401, detail="Not authenticated")

    try:
        user_id = int(user_id_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid user ID format.")

    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM bots WHERE user_id = %s", (user_id,))
                bots_data = cur.fetchall()

                print(f"DEBUG: Fetched bots for user {user_id}: {bots_data}")

                return bots_data if bots_data is not None else []
    except Exception as e:
        print(f"❌ Query failed for user {user_id}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Failed to fetch bots due to an internal error.")

@router.post("/api/bots/create")
def create_bot(payload: CreateBotPayload, request: Request):
    user_id = request.cookies.get("user_id")

    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                print("🧾 SQL values:", (
                    payload.asset,
                    payload.start_size,
                    payload.leverage,
                    payload.multiplier,
                    payload.take_profit,
                    payload.rebuy,
                    "idle",
                    int(user_id),
                    payload.start_type
                ))
                cur.execute("""
                    INSERT INTO bots (
                        asset, start_size, leverage, multiplier,
                        take_profit, rebuy, status, user_id, created_at, start_type
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                    RETURNING *;
                """, (
                    payload.asset,
                    payload.start_size,
                    payload.leverage,
                    payload.multiplier,
                    payload.take_profit,
                    payload.rebuy,
                    "idle",
                    int(user_id),
                    payload.start_type
                ))

                new_bot = cur.fetchone()
                conn.commit()
                return new_bot
    except Exception as e:
        print("❌ Error inserting bot:", e)
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Failed to create bot")

@router.post("/api/bots/start/{bot_id}")
def start_bot(bot_id: int, background_tasks: BackgroundTasks):
    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("BEGIN;")
                cur.execute("""
                    SELECT * FROM bots
                    WHERE id = %s AND status = 'idle'
                    FOR UPDATE
                """, (bot_id,))
                bot = cur.fetchone()

                if not bot:
                    cur.execute("ROLLBACK;")
                    raise HTTPException(404, "Bot not found or already running")

                with running_threads_lock:
                    if bot_id in running_threads and running_threads[bot_id].is_alive():
                        cur.execute("ROLLBACK;")
                        raise HTTPException(status_code=400, detail="Bot is already running")

                    cur.execute("UPDATE bots SET status = 'running' WHERE id = %s", (bot_id,))
                    conn.commit()

                    runner = BotRunner(bot)
                    thread = threading.Thread(target=runner.run, daemon=True)
                    running_threads[bot_id] = thread
                    thread.start()

        return {"message": f"Bot {bot_id} started"}

    except Exception as e:
        print(f"❌ Start error: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Bot start failed")


@router.post("/api/bots/stop/{bot_id}")
def stop_bot(bot_id: int):
    try:
        with with_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("BEGIN;")
                cur.execute("SELECT id FROM bots WHERE id = %s FOR UPDATE", (bot_id,))
                if cur.fetchone() is None:
                    cur.execute("ROLLBACK;")
                    raise HTTPException(404, "Bot not found")

                cur.execute("UPDATE bots SET status = 'stopping' WHERE id = %s", (bot_id,))
                conn.commit()
                print(f"DEBUG: Bot {bot_id} status updated to 'stopping' in DB.")

        with running_threads_lock:
            if bot_id in running_threads:
                runner_instance = running_threads[bot_id]
                if isinstance(runner_instance, BotRunner):
                    runner_instance.stop_event.set()
                    print(f"DEBUG: Signaled bot {bot_id} via threading.Event.")
                else:
                    print(f"WARNING: Bot {bot_id} in registry is not a BotRunner instance.")
            else:
                print(f"WARNING: Bot {bot_id} not found in in-memory registry. Relying on DB poll.")

        return {"message": f"Bot {bot_id} stop initiated."}
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"❌ Error stopping bot {bot_id}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Failed to stop bot due to an internal error.")

@router.post("/api/bot/position")
def get_bot_position(payload: BotPositionPayload):
    asset = payload.asset
    user_id = payload.user_id

    if not asset or not user_id:
        raise HTTPException(status_code=400, detail="Missing asset or user_id")

    # This will use the smart cache
    user = get_user_keys(user_id)

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    try:
        session = HTTP(api_key=user["api_key"], api_secret=user["api_secret"])
        data = session.get_positions(category="linear", symbol=asset)
        position = data["result"]["list"][0]

        return {
            "size": position.get("size", 0),
            "unrealizedPnL": position.get("unrealisedPnl", 0)
        }

    except Exception as e:
        print(f"❌ Error in get_bot_position: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Internal server error")