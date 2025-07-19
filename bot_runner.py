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
        self.running = True

        # bot settings
        self.start_size = float(bot_data["start_size"])
        self.leverage = int(bot_data["leverage"])
        self.multiplier = float(bot_data["multiplier"])
        self.take_profit = float(bot_data["take_profit"])
        self.rebuy_percent = float(bot_data["rebuy"])
        self.max_rebuys = 4

        self.min_order_qty = None
        self.tick_size = None

        self.prev_order_size = 0
        self.last_tp_price = None

    def stop(self, *args):
        print(f"\n🛑 Shutdown signal received for bot {self.bot['id']}")
        self.running = False

    def format_qty(self, qty):
        return str(Decimal(str(qty)).quantize(Decimal(str(self.min_order_qty)), rounding=ROUND_DOWN))

    def format_price(self, price):
        return str(Decimal(str(price)).quantize(Decimal(str(self.tick_size)), rounding=ROUND_DOWN))
    
    def chunk_list(self, data, size):
        for i in range(0, len(data), size):
            yield data[i:i + size]
            sleep(1)
    
    def get_user_keys(self, user_id):
        with with_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT api_key, api_secret FROM users WHERE id = %s", (user_id,))
                return cur.fetchone()

    def get_session(self):
        try:
            user = self.get_user_keys(self.bot["user_id"])
            if not user:
                raise ValueError("User not found")

            return HTTP(api_key=user["api_key"], api_secret=user["api_secret"], testnet=False)
        except Exception as e:
            print(f"❌ Failed to create Bybit session: {e}")
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
        try:
            # 1. Check if bot status is "stopping"
            with with_db_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT status FROM bots WHERE id = %s", (self.bot["id"],))
                    result = cur.fetchone()
                    if result and result["status"] == "stopping":
                        print("🛑 Stop requested via DB. Exiting...")

            # 2. Set bot status back to idle using a new connection
            with with_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE bots SET status = 'idle' WHERE id = %s", (self.bot["id"],))
                    conn.commit()

            return result and result["status"] == "stopping"

        except Exception as e:
            print("❌ Stop check error:", e)
        return False

    def run(self):
        print(f"🚀 Running bot for {self.symbol} with {self.start_size} USDT")
        
        self.stop_check_interval = 5
        last_stop_check = 0
        
        rebuy_prices = []
        rebuy_sizes = []
        error_retries = 0

        try:
            self.session = self.get_session()
            if not self.session:
                print(f"❌ Bot {self.bot['id']} failed to initialize session.")
                return

            try:
                pos_info = self.session.get_positions(category=self.category, symbol=self.symbol)
                current_lev = pos_info['result']['list'][0]['leverage']
                if str(current_lev) != str(self.leverage):
                    self.session.set_leverage(category=self.category, symbol=self.symbol,
                                            buyLeverage=str(self.leverage), sellLeverage=str(self.leverage))
            except Exception as e:
                print("⚠️ Leverage setup failed:", e)
                return

            try:
                info = self.session.get_instruments_info(category=self.category, symbol=self.symbol)
                instrument = info['result']['list'][0]
                self.min_order_qty = instrument['lotSizeFilter']['minOrderQty']
                self.tick_size = instrument['priceFilter']['tickSize']
                
            except Exception as e:
                print("⚠️ Instrument info error:", e)
                print(f"❌ Bot {self.bot['id']} exiting due to instrument info failure.")
                return

            while self.running:
                now = datetime.now().timestamp()

                # Check stop signal only if enough time has passed
                if now - last_stop_check >= self.stop_check_interval:
                    if self.check_stop_signal():
                        break
                    last_stop_check = now

                try:
                    try:
                        price = self.get_price()
                        error_retries = 0  # reset if successful
                    except Exception:
                        error_retries += 1
                        sleep(min(2 ** error_retries, 60))  # exponential backoff up to 60s
                        continue

                    pos = self.get_position()
                    if pos is None:
                        sleep(1)
                        continue

                    # Place initial order if no position
                    if pos["size"] == 0:
                        print("🔄 No position, placing initial order")
                        rebuy_prices.clear()
                        rebuy_sizes.clear()
                        
                        initial_qty = self.start_size / price
                        
                        base_qty = self.format_qty(initial_qty)
                        tp_price = self.format_price(price * (1 + self.take_profit / 100))
                        
                        # Rebuy ladder
                        for x in range(self.max_rebuys):
                            rebuy_price = self.format_price(price * math.pow(1 - self.rebuy_percent / 100, x + 1))
                            rebuy_qty = self.format_qty(initial_qty * math.pow(self.multiplier, x + 1))
                            rebuy_prices.append(rebuy_price)
                            rebuy_sizes.append(rebuy_qty)
                        
                        self.session.cancel_all_orders(category=self.category, symbol=self.symbol)

                        self.session.place_order(
                            category="linear", symbol=self.symbol,
                            side="Buy", orderType="Market", qty=base_qty, takeProfit=tp_price
                        )
                            
                        print(f"📈 Initial market order placed: {base_qty} with TP {tp_price}")

                        sleep(2)  # allow position to open
                        
                        self.prev_order_size = base_qty
                        
                        updated_position = self.get_position()
                        if updated_position and updated_position['size'] > 0:
                            rebuys = [{
                                "symbol": self.symbol,
                                "side": "Buy",
                                "orderType": "Limit",
                                "qty": qty,
                                "price": price,
                            } for qty, price in zip(rebuy_sizes, rebuy_prices)]

                            for chunk in self.chunk_list(rebuys, 10):
                                self.session.place_batch_order(
                                    category=self.category,
                                    request=chunk
                                )
                            print(f"✅ Placed {len(rebuys)} rebuy orders")

                    else:
                        unrealized = pos["unrealised_pnl"]
                        current_time = datetime.now().strftime('%H:%M:%S')
                        
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
                                        print("⚠️ TP update skipped: not modified")
                                    else:
                                        print(f"❌ TP update failed: {e}")
                                        
                            self.prev_order_size = pos["size"]
                        
                        # print(f"\r💰 {self.bot['id']} - {self.symbol} - Pos: {pos['size']} | Avg: {pos['avg_price']} | PnL: {unrealized:.2f} - {current_time}",
                        #       end='', flush=True)

                    sleep(self.poll_interval)
                    
                except Exception as bot_loop_error:
                    print(f"💥 Runtime error in bot loop: {bot_loop_error}")
                    traceback.print_exc()
                    break  # 🚨 break the loop so thread exits

        except Exception as fatal:
            print(f"\n💥 Bot {self.bot['id']} crashed: {fatal}")
            traceback.print_exc()
            
            print(f"❗ Unexpected crash — attempting auto-restart for bot {self.bot['id']} in 5s")
            sleep(5)
            new_runner = BotRunner(self.bot)
            new_thread = threading.Thread(target=new_runner.run, daemon=True)
            with running_threads_lock:
                running_threads[self.bot["id"]] = new_thread
            new_thread.start()
            
        finally:
            print(f"👋 Bot {self.bot['id']} exited.")
            with running_threads_lock:
                print(f"🧵 Current running bots: {list(running_threads.keys())}")
                if not self.running:
                    print(f"🧹 Clean shutdown for bot {self.bot['id']}")
                    running_threads.pop(self.bot["id"], None)
                else:
                    print(f"❗ Bot {self.bot['id']} exited unexpectedly, but will be restarted (already handled)")


