#!/usr/bin/env python3
# Auto runner: if manual pipeline not done this ISO week, run Friday 20:00 CT.

import os, sqlite3, time, subprocess
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(ROOT, "employees.sqlite3")
CENTRAL_TZ = ZoneInfo("America/Chicago")

def iso_week_key(dt: datetime) -> str:
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"

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
    print("▶️  Launching pipeline…")
    subprocess.run(["python", os.path.join(ROOT, "run_pipeline.py")], check=False)

def main_loop():
    print("🕒 friday_runner started — runs Fridays at 20:00 CT if not already done.")
    last_seen_week = None
    while True:
        now_ct = datetime.now(tz=CENTRAL_TZ)
        week_key = iso_week_key(now_ct)
        try:
            conn = connect()
            done = already_done_this_week(conn, week_key)
            conn.close()
        except Exception as e:
            print(f"DB check error: {e}")
            done = False

        # Normal trigger at exactly 20:00 Friday (Central)
        if now_ct.weekday() == 4 and now_ct.hour == 20 and now_ct.minute == 0:
            if not done:
                print(f"⏰ Friday 20:00 CT — running pipeline for {week_key}")
                run_pipeline()
                last_seen_week = week_key
            else:
                print(f"✅ Already processed {week_key}; skip.")
                last_seen_week = week_key
            time.sleep(65)  # avoid double-fire same minute

        # Safety catch-up on Saturday if missed
        if now_ct.weekday() == 5 and last_seen_week != week_key and not done:
            print(f"⚠️ Missed Friday window — running once for {week_key}")
            run_pipeline()
            last_seen_week = week_key
            time.sleep(65)

        time.sleep(5)

if __name__ == "__main__":
    main_loop()
