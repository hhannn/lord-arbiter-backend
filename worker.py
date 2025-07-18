import time
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from bot_runner import run_bot

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def main():
    while True:
        conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("SELECT * FROM bots WHERE status = 'idle'")
        bots = cursor.fetchall()

        for bot in bots:
            try:
                cursor.execute("UPDATE bots SET status = 'running' WHERE id = %s", (bot["id"],))
                conn.commit()
                run_bot(bot)
            except Exception as e:
                cursor.execute("UPDATE bots SET status = 'error' WHERE id = %s", (bot["id"],))
                conn.commit()
                print(f"❌ Error running bot {bot['id']}: {e}")

        cursor.close()
        conn.close()
        time.sleep(5)
