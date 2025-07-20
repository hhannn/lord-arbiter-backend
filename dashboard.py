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
from typing import Dict

from bot_runner import BotRunner  # This is the function you'll move your bot logic into
from dotenv import load_dotenv
from runner_registry import running_threads, running_threads_lock
from db import init_pool, get_conn, put_conn, close_pool, with_db_conn

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        init_pool()  # ✅ Initialize pool on startup

        try:
            with with_db_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT * FROM bots WHERE status = 'running'")
                    bots = cur.fetchall()

                    for bot in bots:
                        bot_id = bot["id"]
                        with running_threads_lock:
                            if bot_id in running_threads and running_threads[bot_id].is_alive():
                                continue

                            runner = BotRunner(bot)
                            thread = threading.Thread(target=runner.run, daemon=True)
                            running_threads[bot_id] = thread
                            thread.start()
                            print(f"🔁 Resumed bot {bot_id}")
                            
        except Exception as e:
            print("❌ Error in lifespan:", e)

    except Exception as e:
        print("❌ Failed to initialize DB pool or resume bots:", e)

    yield
    close_pool()
    
router = APIRouter()

# API cache
_dashboard_user_keys_cache = {}
_dashboard_user_keys_cache_lock = threading.Lock()

def get_user_keys(user_id):
    # Ensure user_id is an integer for consistent caching and DB query
    try:
        user_id = int(user_id)
    except ValueError:
        print(f"❌ Invalid user_id format received: {user_id}")
        return None

    # Check cache first
    with _dashboard_user_keys_cache_lock:
        if user_id in _dashboard_user_keys_cache:
            # print(f"DEBUG: Dashboard: User keys for {user_id} found in cache.")
            return _dashboard_user_keys_cache[user_id]

    # If not in cache, fetch from DB
    print(f"DEBUG: Dashboard: User keys for {user_id} not in cache, fetching from DB.")
    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT api_key, api_secret FROM users WHERE id = %s", (user_id,))
                user_keys = cur.fetchone()
                if user_keys:
                    with _dashboard_user_keys_cache_lock:
                        _dashboard_user_keys_cache[user_id] = user_keys # Store in cache
                    return user_keys
                return None
    except Exception as e:
        print(f"❌ Error fetching user keys for {user_id} from DB (Dashboard): {e}")
        traceback.print_exc() # Print traceback for dashboard errors
        return None

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
                # Check username
                cur.execute("SELECT id FROM users WHERE username = %s", (payload.username,))
                if cur.fetchone():
                    raise HTTPException(status_code=400, detail="Username already exists")

                # Check UID
                cur.execute("SELECT id FROM users WHERE uid = %s", (uid,))
                if cur.fetchone():
                    raise HTTPException(status_code=400, detail="Bybit account already registered")

                # Insert new user
                cur.execute("""
                    INSERT INTO users (username, password, api_key, api_secret, uid)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (payload.username, payload.password, payload.api_key, payload.api_secret, uid))

                new_user = cur.fetchone()
                conn.commit()
                return {"user_id": new_user["id"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database error")
    finally:
        conn.close()

@router.post("/api/user/login")
def login_user(payload: LoginPayload, response: Response):
    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM users WHERE username = %s", (payload.username,))
                user = cur.fetchone()

                if not user or user["password"] != payload.password:
                    raise HTTPException(status_code=401, detail="Invalid credentials")
                
                # ✅ Set cookie here
                response.set_cookie(
                    key="user_id",
                    value=str(user["id"]),
                    httponly=True,
                    samesite="None",  # or "None" if cross-site on HTTPS
                    secure=True      # ⚠️ Required for cookies to be sent over HTTPS
                )

                return {
                    "user_id": user["id"],
                    "username": user["username"],
                    "uid": user["uid"],  # 👈 Add this
                }
    except Exception as e:
        raise HTTPException(status_code=500, detail="Login failed")
    finally:
        conn.close()


@router.get("/api/user/data")
def get_user_data(request: Request):
    user_id = request.cookies.get("user_id")
    
    if not user_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    user = get_user_keys(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
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
        print("❌ Error calling Bybit:", e)
        raise HTTPException(status_code=500, detail="Failed to fetch data from Bybit")
    
@router.post("/api/user/bots")
def get_bot_data(request: Request):
    user_id = request.cookies.get("user_id")
    
    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM bots WHERE user_id = %s", (user_id,))
                result = cur.fetchone()
                print("Result:", result)
                return result
    except Exception as e:
        print("❌ Query failed:", e)
        conn.rollback()
        raise HTTPException(status_code=500, detail="Query error")
    finally:
        conn.close()
        
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
                    "idle",              # default status
                    int(user_id),
                    payload.start_type
                ))

                new_bot = cur.fetchone()
                conn.commit()
                return new_bot
    except Exception as e:
        print("❌ Error inserting bot:", e)
        raise HTTPException(status_code=500, detail="Failed to create bot")

@router.post("/api/bots/start/{bot_id}")
def start_bot(bot_id: int, background_tasks: BackgroundTasks):
    try:
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("BEGIN;")
                # Lock the row in DB
                cur.execute("""
                    SELECT * FROM bots
                    WHERE id = %s AND status = 'idle'
                    FOR UPDATE
                """, (bot_id,))
                bot = cur.fetchone()

                if not bot:
                    cur.execute("ROLLBACK;")
                    raise HTTPException(404, "Bot not found or already running")

                # Ensure no thread is already running
                if bot_id in running_threads and running_threads[bot_id].is_alive():
                    cur.execute("ROLLBACK;")
                    raise HTTPException(status_code=400, detail="Bot is already running")
                
                with running_threads_lock:  # Lock thread registry first

                    # Update DB to running
                    cur.execute("UPDATE bots SET status = 'running' WHERE id = %s", (bot_id,))
                    conn.commit()

                    # Start the bot
                    runner = BotRunner(bot)
                    thread = threading.Thread(target=runner.run, daemon=True)
                    running_threads[bot_id] = thread
                    thread.start()

        return {"message": f"Bot {bot_id} started"}

    except Exception as e:
        print(f"❌ Start error: {e}")
        raise HTTPException(status_code=500, detail="Bot start failed")


        
@router.post("/api/bots/stop/{bot_id}")
def stop_bot(bot_id: int):
    conn = None # Initialize conn for outer except block
    try:
        # First, update DB status to 'stopping'
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
        
        # Then, signal the in-memory thread directly
        with running_threads_lock:
            if bot_id in running_threads:
                runner_instance = running_threads[bot_id]
                if isinstance(runner_instance, BotRunner): # Ensure it's a BotRunner instance
                    runner_instance.stop_event.set() # <--- Set the event
                    print(f"DEBUG: Signaled bot {bot_id} via threading.Event.")
                else:
                    print(f"WARNING: Bot {bot_id} in registry is not a BotRunner instance.")
            else:
                print(f"WARNING: Bot {bot_id} not found in in-memory registry. Relying on DB poll.")
                # This is okay, the bot will eventually stop when it polls the DB.

        return {"message": f"Bot {bot_id} stop initiated."}
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"❌ Error stopping bot {bot_id}: {e}")
        traceback.print_exc()
        if conn:
            try:
                conn.rollback() 
            except Exception as rb_exc:
                print(f"ERROR: Failed to rollback connection for bot {bot_id}: {rb_exc}")
        raise HTTPException(status_code=500, detail="Failed to stop bot due to an internal error.")

@router.post("/api/bot/position")
def get_bot_position(payload: BotPositionPayload):
    asset = payload.asset
    user_id = payload.user_id

    if not asset or not user_id:
        raise HTTPException(status_code=400, detail="Missing asset or user_id")
    
    try:
        user = get_user_keys(payload.user_id)

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Call Bybit
        session = HTTP(api_key=user["api_key"], api_secret=user["api_secret"])
        data = session.get_positions(category="linear", symbol=asset)
        position = data["result"]["list"][0]

        return {
            "size": position.get("size", 0),
            "unrealizedPnL": position.get("unrealisedPnl", 0)
        }

    except Exception as e:
        print(f"❌ Error in get_bot_position: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    


