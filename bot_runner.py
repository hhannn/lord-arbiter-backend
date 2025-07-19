# bot_runner.py

import os
import math
import threading
import signal
import psycopg2
import traceback

from time import sleep
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from psycopg2.extras import RealDictCursor
from pybit.unified_trading import HTTP
from runner_registry import running_threads, running_threads_lock
from db import with_db_conn

class BotRunner:
    def __init__(self, bot_data):
        self.bot = bot_data
        self.session = None
        self.symbol = bot_data["asset"]
        self.category = "linear"
        self.poll_interval = 5
        self.stop_check_interval = 5
        self.running = True
        self.stop_requested_via_db = False
        self.db_status_on_exit = "error"
        self.stop_event = threading.Event() 
        
        # Cache for user API keys (class-level cache)
        # This dictionary will store fetched API keys for all users
        if not hasattr(BotRunner, '_user_keys_cache'):
            BotRunner._user_keys_cache = {}
            
        self.user_id = bot_data["user_id"]
        self.user_api_key = None
        self.user_api_secret = None

        # bot settings
        self.start_size = float(bot_data["start_size"])
        self.leverage = int(bot_data["leverage"])
        self.multiplier = float(bot_data["multiplier"])
        self.take_profit = float(bot_data["take_profit"])
        self.rebuy_percent = float(bot_data["rebuy"])
        self.max_rebuys = 23

        self.min_order_qty = None
        self.tick_size = None

        self.prev_order_size = 0
        self.last_tp_price = None

    # def stop(self, *args):
    #     print(f"\n🛑 Shutdown signal received for bot {self.bot['id']}")
    #     self.running = False

    def format_qty(self, qty):
        return str(Decimal(str(qty)).quantize(Decimal(str(self.min_order_qty)), rounding=ROUND_DOWN))

    def format_price(self, price):
        return str(Decimal(str(price)).quantize(Decimal(str(self.tick_size)), rounding=ROUND_DOWN))
    
    def chunk_list(self, data, size):
        for i in range(0, len(data), size):
            yield data[i:i + size]
            sleep(1)
    
    def get_user_keys(self, user_id):
        # Check cache first
        if user_id in BotRunner._user_keys_cache:
            print(f"DEBUG: Bot {self.bot['id']}: User keys for {user_id} found in cache.")
            return BotRunner._user_keys_cache[user_id]

        # If not in cache, fetch from DB
        print(f"DEBUG: Bot {self.bot['id']}: User keys for {user_id} not in cache, fetching from DB.")
        try:
            with with_db_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT api_key, api_secret FROM users WHERE id = %s", (user_id,))
                    user_keys = cur.fetchone()
                    if user_keys:
                        BotRunner._user_keys_cache[user_id] = user_keys # Store in cache
                        return user_keys
                    return None
        except Exception as e:
            print(f"❌ Error fetching user keys for {user_id} from DB: {e}")
            return None

    def get_session(self):
        try:
            # This call will now use the caching get_user_keys
            user = self.get_user_keys(self.user_id) 
            if not user:
                raise ValueError(f"User API keys not found for user_id {self.user_id}.")

            return HTTP(api_key=user["api_key"], api_secret=user["api_secret"], testnet=False)
        except Exception as e:
            print(f"❌ Failed to create Bybit session for bot {self.bot['id']}: {e}")
            return None

    def get_price(self):
        try:
            ticker = self.session.get_tickers(category=self.category, symbol=self.symbol)
            return float(ticker['result']['list'][0]['lastPrice'])
        except Exception as e:
            print(f"⚠️ Price error: {e}")
            return None

    def get_position(self):
        try:
            data = self.session.get_positions(category=self.category, symbol=self.symbol)['result']['list'][0]
            return {
                'size': float(data.get("size", "0") or 0),
                'avg_price': float(data.get("avgPrice", "0") or 0),
                'unrealised_pnl': float(data.get("unrealisedPnl", "0") or 0),
                'take_profit': data.get("takeProfit", None)
            }
        except Exception as e:
            print(f"⚠️ Position error: {e}")
            return None

    def check_stop_signal(self):
        # Check internal event first - this is the fast path
        if self.stop_event.is_set():
            print(f"🛑 Internal stop event set for bot {self.bot['id']}. Exiting.")
            self.stop_requested_via_db = True # Indicate DB stop was requested (for consistency)
            return True

        # Fallback: Check DB if event is not set (e.g., if another instance set it, or for robustness)
        try:
            with with_db_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT status FROM bots WHERE id = %s", (self.bot["id"],))
                    result = cur.fetchone()
                    if result and result["status"] == "stopping":
                        print(f"🛑 Stop requested via DB for bot {self.bot['id']}.")
                        self.stop_requested_via_db = True
                        self.stop_event.set() # Set the event if DB says stopping
                        return True
            return False
        except Exception as e:
            print(f"❌ Stop check DB error for bot {self.bot['id']}: {e}")
            # If DB error, assume we should stop to prevent operating blind
            self.running = False 
            self.db_status_on_exit = "error"
            return False

    def run(self):
            """
            The main entry point for the bot's thread.
            Handles overall lifecycle, error recovery, and final DB status update.
            """
            print(f"🚀 Bot {self.bot['id']} starting run for {self.symbol}.")
            
            # Default to error status on exit, will be overridden if clean shutdown
            self.db_status_on_exit = "error" 

            try:
                # Delegate the core trading logic loop to a private method
                self._run_logic() 
            except Exception as e:
                # This catches any unhandled exceptions from _run_logic or initial setup
                print(f"\n💥 Bot {self.bot['id']} crashed with unhandled exception: {e}")
                traceback.print_exc()
                self.db_status_on_exit = "error" # Ensure status is error on crash
                
                # Auto-restart logic
                print(f"❗ Unexpected crash — attempting auto-restart for bot {self.bot['id']} in 5s")
                sleep(5)
                try:
                    # IMPORTANT: Fetch latest bot data from DB for restart
                    # This ensures the new runner has the most up-to-date configuration
                    with with_db_conn() as conn_restart:
                        with conn_restart.cursor(cursor_factory=RealDictCursor) as cur_restart:
                            cur_restart.execute("SELECT * FROM bots WHERE id = %s", (self.bot['id'],))
                            updated_bot_data = cur_restart.fetchone()
                    if updated_bot_data:
                        new_runner = BotRunner(updated_bot_data)
                        new_thread = threading.Thread(target=new_runner.run, daemon=True)
                        with running_threads_lock:
                            running_threads[self.bot["id"]] = new_thread
                        new_thread.start()
                        print(f"✅ Bot {self.bot['id']} successfully queued for restart.")
                    else:
                        print(f"❌ Could not retrieve updated bot data for restart of bot {self.bot['id']}. Not restarting.")
                except Exception as restart_error:
                    print(f"❌ Failed to initiate auto-restart for bot {self.bot['id']}: {restart_error}")
                    traceback.print_exc()
                # The current thread will now exit, leading to the finally block

            finally:
                print(f"👋 Bot {self.bot['id']} final cleanup (thread exiting).")
                # Update database status based on why the thread is exiting
                try:
                    with with_db_conn() as conn_final:
                        with conn_final.cursor() as cur_final:
                            # Only update if the status is currently 'running', 'stopping', or 'error' in DB.
                            # This prevents overwriting a 'completed' status if you introduce it later,
                            # or if another process has already updated the status.
                            cur_final.execute(
                                "UPDATE bots SET status = %s WHERE id = %s AND status IN ('running', 'stopping', 'error')",
                                (self.db_status_on_exit, self.bot["id"])
                            )
                            conn_final.commit()
                            print(f"DB Status for bot {self.bot['id']} updated to '{self.db_status_on_exit}'.")
                except Exception as db_update_error:
                    print(f"❌ Error updating DB status for bot {self.bot['id']} on exit: {db_update_error}")
                    traceback.print_exc()

                # Clean up in-memory registry
                with running_threads_lock:
                    if self.bot["id"] in running_threads:
                        # Crucially, only remove THIS specific thread instance
                        # if it's still the one registered, to avoid removing a newly
                        # restarted thread if auto-restart was triggered.
                        if running_threads[self.bot["id"]] == threading.current_thread():
                            print(f"🧹 Removing bot {self.bot['id']} from in-memory registry.")
                            running_threads.pop(self.bot["id"], None)
                        else:
                            print(f"❗ Bot {self.bot['id']} was already replaced in registry; not removing.")
                    print(f"🧵 Current running bots in memory: {list(running_threads.keys())}")


    def _run_logic(self):
        """
        Contains the main trading logic loop of the bot.
        This method is called by the `run` method.
        """
        print(f"🚀 Bot {self.bot['id']} entering main trading loop.")
        
        # Initial setup for Bybit session and instrument info
        self.session = self.get_session()
        if not self.session:
            print(f"❌ Bot {self.bot['id']} failed to initialize Bybit session. Exiting _run_logic.")
            self.running = False # Signal to stop the loop
            self.db_status_on_exit = "error" # Mark for error status in DB
            return # Exit this function

        try:
            # Set leverage
            pos_info = self.session.get_positions(category=self.category, symbol=self.symbol)
            current_lev = pos_info['result']['list'][0]['leverage']
            if str(current_lev) != str(self.leverage):
                self.session.set_leverage(category=self.category, symbol=self.symbol,
                                            buyLeverage=str(self.leverage), sellLeverage=str(self.leverage))
                print(f"✅ Bot {self.bot['id']}: Leverage set to {self.leverage}.")
        except Exception as e:
            print(f"⚠️ Bot {self.bot['id']}: Leverage setup failed: {e}. Exiting _run_logic.")
            self.running = False
            self.db_status_on_exit = "error"
            return

        try:
            # Get instrument info (min_order_qty, tick_size)
            info = self.session.get_instruments_info(category=self.category, symbol=self.symbol)
            instrument = info['result']['list'][0]
            self.min_order_qty = instrument['lotSizeFilter']['minOrderQty']
            self.tick_size = instrument['priceFilter']['tickSize']
            print(f"✅ Bot {self.bot['id']}: Instrument info fetched. Min Qty: {self.min_order_qty}, Tick Size: {self.tick_size}.")
            
        except Exception as e:
            print(f"⚠️ Bot {self.bot['id']}: Instrument info error: {e}. Exiting _run_logic.")
            self.running = False
            self.db_status_on_exit = "error"
            return

        # If we reached here, initial setup was successful.
        # Set expected exit status to 'idle' unless a crash occurs later.
        self.db_status_on_exit = "idle" 

        rebuy_prices = [] # Moved these local variables into _run_logic scope
        rebuy_sizes = []
        error_retries = 0
        last_stop_check = 0 # Moved this here too

        while self.running:
            # Check for stop signal more frequently without hitting DB
            # Use stop_event.wait(timeout) for non-blocking check with sleep
            if self.stop_event.wait(timeout=self.poll_interval): # Wait for event or timeout
                print(f"🛑 Bot {self.bot['id']}: Stop event triggered, exiting main loop.")
                self.running = False # Ensure loop terminates
                self.db_status_on_exit = "idle" # Assume clean shutdown if event set
                break # Exit the while loop immediately

            now = datetime.now().timestamp()

            # or if the DB is the ultimate source of truth for stop commands.
            # However, the primary stop mechanism is now the event.
            if now - last_stop_check >= self.stop_check_interval:
                if self.check_stop_signal(): # This will now also check self.stop_event
                    self.running = False # Signal to stop the loop
                    self.db_status_on_exit = "idle" # Explicitly set for clean shutdown
                    break # Exit the while loop immediately
                last_stop_check = now

            try:
                # Get current price, with exponential backoff for transient errors
                try:
                    price = self.get_price()
                    if price is None: # Handle case where get_price returns None
                        raise ValueError("Failed to get price.")
                    error_retries = 0  # reset if successful
                except Exception as price_error:
                    error_retries += 1
                    sleep_time = min(2 ** error_retries, 60) # Max 60s backoff
                    print(f"⚠️ Bot {self.bot['id']}: Price fetch failed ({price_error}). Retrying in {sleep_time}s.")
                    sleep(sleep_time)
                    continue # Skip to next loop iteration

                pos = self.get_position()
                if pos is None:
                    print(f"⚠️ Bot {self.bot['id']}: Position fetch failed. Retrying in 1s.")
                    sleep(1)
                    continue

                # Trading Logic
                if pos["size"] == 0:
                    print(f"🔄 Bot {self.bot['id']}: No position, placing initial order.")
                    rebuy_prices.clear()
                    rebuy_sizes.clear()
                    
                    initial_qty = self.start_size / price
                    
                    base_qty = self.format_qty(initial_qty)
                    tp_price = self.format_price(price * (1 + self.take_profit / 100))
                    
                    # Calculate rebuy ladder prices and quantities
                    for x in range(self.max_rebuys):
                        rebuy_price = self.format_price(price * math.pow(1 - self.rebuy_percent / 100, x + 1))
                        rebuy_qty = self.format_qty(initial_qty * math.pow(self.multiplier, x + 1))
                        rebuy_prices.append(rebuy_price)
                        rebuy_sizes.append(rebuy_qty)
                    
                    self.session.cancel_all_orders(category=self.category, symbol=self.symbol)
                    print(f"🗑️ Bot {self.bot['id']}: Canceled all existing orders.")

                    self.session.place_order(
                        category="linear", symbol=self.symbol,
                        side="Buy", orderType="Market", qty=base_qty, takeProfit=tp_price
                    )
                    print(f"📈 Bot {self.bot['id']}: Initial market order placed: {base_qty} with TP {tp_price}.")

                    sleep(2)  # Allow position to open and API to update
                    
                    self.prev_order_size = base_qty
                    
                    updated_position = self.get_position()
                    if updated_position and updated_position['size'] > 0:
                        rebuys_to_place = [{
                            "symbol": self.symbol,
                            "side": "Buy",
                            "orderType": "Limit",
                            "qty": qty,
                            "price": price,
                        } for qty, price in zip(rebuy_sizes, rebuy_prices)]

                        for chunk in self.chunk_list(rebuys_to_place, 10):
                            self.session.place_batch_order(
                                category=self.category,
                                request=chunk
                            )
                        print(f"✅ Placed {len(rebuys_to_place)} rebuy orders")
                    else:
                        print(f"⚠️ Bot {self.bot['id']}: Position not opened after initial order. Rebuy orders not placed.")

                else: # Position exists
                    unrealized = pos["unrealised_pnl"]
                    current_time = datetime.now().strftime('%H:%M:%S')
                    
                    # Update Take Profit if position size changed (meaning a rebuy filled)
                    if pos["size"] > float(self.prev_order_size):
                        tp_price = self.format_price(pos["avg_price"] * (1 + self.take_profit / 100))
                        if tp_price != self.last_tp_price:
                            try:
                                self.session.set_trading_stop(
                                    category=self.category,
                                    symbol=self.symbol,
                                    tpslMode='Full',
                                    positionIdx=0,
                                    takeProfit=tp_price
                                )
                                self.last_tp_price = tp_price
                                print(f"✅ Updated TP to {tp_price}")
                            except Exception as e:
                                if "not modified" in str(e):
                                    print(f"⚠️ Bot {self.bot['id']}: TP update skipped: not modified")
                                else:
                                    print(f"❌ Bot {self.bot['id']}: TP update failed: {e}")
                                    
                        self.prev_order_size = pos["size"]
                    
                    # Optional: Print current status to console (can be verbose)
                    # print(f"\r💰 {self.bot['id']} - {self.symbol} - Pos: {pos['size']} | Avg: {pos['avg_price']} | PnL: {unrealized:.2f} - {current_time}",
                    #       end='', flush=True)

                sleep(self.poll_interval) # Wait before next loop iteration
                
            except Exception as bot_loop_error:
                print(f"💥 Bot {self.bot['id']}: Runtime error in main loop: {bot_loop_error}")
                traceback.print_exc()
                self.db_status_on_exit = "error" # Mark as error if loop breaks due to this
                self.running = False # Signal to stop the loop
                break  # Exit the loop so finally block can execute


