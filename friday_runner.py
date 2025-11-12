#!/usr/bin/env python3
# Auto runner: if manual pipeline not done this week, run every Friday 20:00 Central.

import os, sqlite3, time, subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(ROOT, "employees.sqlite3")
CENTRAL_TZ = ZoneInfo("America/Chicago")

def iso_week_key(dt: datetime) -> str:
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"

def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def already_done_this_week(conn, week_key: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM runs WHERE type='PIPELINE' AND weekKey=? LIMIT 1", (week_key,)
    ).fetchone()
    return row is not None

def run_pipeline():
    print("▶️  Launching manual pipeline (subprocess)...")
    subprocess.run(["python", os.path.join(ROOT, "run_pipeline.py")], check=False)

def main_loop():
    print("🕒 friday_runner started. Will run Fridays at 20:00 Central if not already done.")
    last_checked_week = None
    while True:
        now_ct = datetime.now(tz=CENTRAL_TZ)
        week_key = iso_week_key(now_ct)

        # Only reopen DB occasionally
        try:
            conn = connect()
            done = already_done_this_week(conn, week_key)
            conn.close()
        except Exception as e:
            print(f"DB check error: {e}")
            done = False

        if now_ct.weekday() == 5 and last_checked_week != week_key:
            # Safety: if the system was off exactly at 20:00 Friday, run on Saturday 00:00–23:59 once.
            if not done:
                print(f"⚠️ Missed Friday window; running pipeline once for {week_key}")
                run_pipeline()
                last_checked_week = week_key

        if now_ct.weekday() == 4 and now_ct.hour == 20 and now_ct.minute == 0:
            if not done:
                print(f"⏰ Friday 20:00 CT — not done yet for {week_key}. Running now.")
                run_pipeline()
                last_checked_week = week_key
            else:
                print(f"✅ Already done for {week_key}. Skipping this week.")
                last_checked_week = week_key

            time.sleep(65)  # avoid double-fire in same minute

        time.sleep(5)

if __name__ == "__main__":
    main_loop()
