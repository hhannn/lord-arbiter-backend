# bot_runner.py

import os
import math
import threading
import signal
import psycopg2

from time import sleep
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from psycopg2.extras import RealDictCursor
from pybit.unified_trading import HTTP
from runner_registry import running_threads

class BotRunner:
    def __init__(self, bot_data, db_url):
        self.bot = bot_data
        self.db_url = db_url
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

    def get_db_conn(self):
        return psycopg2.connect(self.db_url)

    def get_session(self):
        try:
            with self.get_db_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT api_key, api_secret FROM users WHERE id = %s", (self.bot["user_id"],))
                    user = cur.fetchone()

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
            with self.get_db_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT status FROM bots WHERE id = %s", (self.bot["id"],))
                    result = cur.fetchone()
                    if result and result["status"] == "stopping":
                        print("🛑 Stop requested via DB. Exiting...")
                        cur.execute("UPDATE bots SET status = 'idle' WHERE id = %s", (self.bot["id"],))
                        conn.commit()
                        return True
        except Exception as e:
            print("❌ Stop check error:", e)
        return False

    def run(self):
        print(f"🚀 Running bot for {self.symbol} with {self.start_size} USDT")
        
        rebuy_prices = []
        rebuy_sizes = []

        self.session = self.get_session()
        if not self.session:
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
            return

        while self.running:
            if self.check_stop_signal():
                break

            try:
                price = self.get_price()
                if price is None:
                    sleep(1)
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
                    
                    initial_price = self.get_price()
                    initial_qty = self.start_size / initial_price
                    
                    base_qty = self.format_qty(initial_qty)
                    tp_price = self.format_price(initial_price * (1 + self.take_profit / 100))
                    
                    # Rebuy ladder
                    for x in range(self.max_rebuys):
                        rebuy_price = self.format_price(initial_price * math.pow(1 - self.rebuy_percent / 100, x + 1))
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
                    
                    if pos["size"] > self.prev_order_size:
                        tp_price = self.format_price(pos["avg_price"] * (1 + self.take_profit / 100))
                        self.session.set_trading_stop(category=self.category, symbol=self.symbol,
                                                    tpslMode='Full', positionIdx=0, takeProfit=tp_price)
                        print(f"✅ Updated TP to {tp_price}")
                    
                    print(f"\r💰 {self.bot['id']} - {self.symbol} - Pos: {pos['size']} | Avg: {pos['avg_price']} | PnL: {unrealized:.2f} - {current_time}",
                          end='', flush=True)

                sleep(self.poll_interval)

            except Exception as e:
                print(f"❌ Bot loop error: {e}")
                sleep(3)

        print(f"👋 Bot {self.bot['id']} exited.")
        running_threads.pop(self.bot["id"], None)
        print(f"🧹 Removed bot {self.bot['id']} from thread registry")
