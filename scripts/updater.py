#!/usr/bin/env python3
# Self-contained JSON -> SQLite updater with visible console output and summaries.

import json
import os
import sqlite3
from datetime import datetime
import time

# ----- Config -----
DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "employees.sqlite3"))
INPUT_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "employees.json"))

EXPECTED_HOURS = 40.0
MAX_HOURS_PER_ROW = 100.0
RUN_SCHEDULER = False  # Set True for automatic weekly schedule
SCHEDULE_HHMM = "20:00"

# ----- Schema -----
SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS employees (
  email TEXT PRIMARY KEY,
  employeeNum TEXT UNIQUE,
  firstName TEXT NOT NULL,
  lastName  TEXT NOT NULL,
  department TEXT,
  role TEXT,
  hoursWorked REAL DEFAULT 0,
  active INTEGER DEFAULT 1,
  updatedAt TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  startedAt TEXT NOT NULL,
  finishedAt TEXT,
  inserted INTEGER DEFAULT 0,
  updated INTEGER DEFAULT 0,
  rejected INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS weekly_summary (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  runId INTEGER NOT NULL,
  employeeKey TEXT NOT NULL,
  total_hours REAL NOT NULL,
  expected_hours REAL NOT NULL,
  delta REAL NOT NULL,
  status TEXT NOT NULL,
  createdAt TEXT DEFAULT (datetime('now')),
  FOREIGN KEY(runId) REFERENCES runs(id)
);
"""

# ----- Helpers -----
def connect_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn):
    with conn:
        conn.executescript(SCHEMA)

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def clean_row(r):
    return {
        "email": str(r.get("email", "")).strip().lower(),
        "employeeNum": str(r.get("employeeNum", "")).strip() or None,
        "firstName": str(r.get("firstName", "")).strip(),
        "lastName": str(r.get("lastName", "")).strip(),
        "department": str(r.get("department", "")).strip() or None,
        "role": str(r.get("role", "")).strip() or "Staff",
        "hoursWorked": round(float(r.get("hoursWorked", 0) or 0), 2),
        "active": 1 if r.get("active", True) else 0,
    }

def validate_row(c):
    if not c["email"]:
        return "Missing email"
    if not c["firstName"] or not c["lastName"]:
        return "Missing name"
    if c["hoursWorked"] < 0:
        return "Negative hours"
    if c["hoursWorked"] > MAX_HOURS_PER_ROW:
        return f"Hours exceed limit ({MAX_HOURS_PER_ROW})"
    return None

def start_run(conn):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur = conn.cursor()
    cur.execute("INSERT INTO runs (startedAt) VALUES (?)", (ts,))
    conn.commit()
    return cur.lastrowid

def finish_run(conn, run_id, inserted, updated, rejected):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with conn:
        conn.execute(
            "UPDATE runs SET finishedAt=?, inserted=?, updated=?, rejected=? WHERE id=?",
            (ts, inserted, updated, rejected, run_id),
        )

def upsert_employee(conn, c):
    with conn:
        conn.execute("""
            INSERT INTO employees (email, employeeNum, firstName, lastName, department, role, hoursWorked, active, updatedAt)
            VALUES (:email, :employeeNum, :firstName, :lastName, :department, :role, :hoursWorked, :active, datetime('now'))
            ON CONFLICT(email) DO UPDATE SET
              employeeNum=excluded.employeeNum,
              firstName=excluded.firstName,
              lastName=excluded.lastName,
              department=excluded.department,
              role=excluded.role,
              hoursWorked=excluded.hoursWorked,
              active=excluded.active,
              updatedAt=datetime('now');
        """, c)

def weekly_summary(conn, run_id):
    cur = conn.cursor()
    cur.execute("SELECT email, hoursWorked FROM employees")
    rows = cur.fetchall()
    results = []
    print("\n📊 Weekly Summary:")
    print(f"{'Email':35} {'Hours':>8} {'Delta':>8} {'Status':>8}")
    print("-" * 65)
    for email, hrs in rows:
        delta = round(hrs - EXPECTED_HOURS, 2)
        if hrs == 0:
            status = "FAIL"
        elif 0 < hrs < EXPECTED_HOURS:
            status = "WARN"
        elif hrs >= EXPECTED_HOURS:
            status = "PASS"
        results.append((run_id, email, hrs, EXPECTED_HOURS, delta, status))
        print(f"{email:35} {hrs:8.2f} {delta:8.2f} {status:>8}")
    with conn:
        conn.executemany("""
            INSERT INTO weekly_summary (runId, employeeKey, total_hours, expected_hours, delta, status)
            VALUES (?, ?, ?, ?, ?, ?)
        """, results)
    return len(rows)

def run_update():
    print(f"\n🚀 Running updater at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    conn = connect_db()
    init_db(conn)
    run_id = start_run(conn)

    try:
        raw_rows = load_json(INPUT_FILE)
    except Exception as e:
        print(f"❌ Error loading JSON: {e}")
        return

    inserted = updated = rejected = 0
    for r in raw_rows:
        c = clean_row(r)
        error = validate_row(c)
        if error:
            print(f"⚠️  Skipped {c['email'] or '[no email]'} — {error}")
            rejected += 1
            continue

        # Check if exists
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM employees WHERE email=?", (c["email"],))
        exists = cur.fetchone() is not None
        upsert_employee(conn, c)
        if exists:
            updated += 1
            print(f"🔁 Updated: {c['email']:<30} {c['hoursWorked']} hrs")
        else:
            inserted += 1
            print(f"🆕 Inserted: {c['email']:<30} {c['hoursWorked']} hrs")

    finish_run(conn, run_id, inserted, updated, rejected)
    total = inserted + updated + rejected
    print(f"\n✅ Summary: {inserted} inserted, {updated} updated, {rejected} rejected (total {total})")

    # Show weekly summary
    weekly_summary(conn, run_id)
    conn.close()
    print(f"\n📦 Database saved: {DB_PATH}\n")

def _run_forever_at(hhmm="20:00"):
    h, m = map(int, hhmm.split(":"))
    print(f"🕒 Scheduler active: runs every Friday at {hhmm}. Ctrl+C to stop.")
    while True:
        now = datetime.now()
        if now.weekday() == 4 and now.hour == h and now.minute == m:
            run_update()
            time.sleep(61)
        time.sleep(5)

if __name__ == "__main__":
    run_update()
    if RUN_SCHEDULER:
        _run_forever_at(SCHEDULE_HHMM)
