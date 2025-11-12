#!/usr/bin/env python3
# Manual pipeline: upsert users, process current week, promotions, write CSV, log run

import os, json, csv, sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ROOT = os.path.abspath(os.path.dirname(__file__))
DB_PATH = os.path.join(ROOT, "employees.sqlite3")
DATA_USERS = os.path.join(ROOT, "data", "users.json")
WEEKS_DIR = os.path.join(ROOT, "weeks")
OUT_DIR = os.path.join(ROOT, "out")

EXPECTED_HOURS_DEFAULT = 40.0
CENTRAL_TZ = ZoneInfo("America/Chicago")
TIER_MAX = 3
ROLE_BY_TIER = {1: "Staff", 2: "Senior", 3: "Lead"}  # adjust as you wish

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS employees (
  email TEXT PRIMARY KEY,
  employeeNum TEXT UNIQUE,
  firstName TEXT NOT NULL,
  lastName  TEXT NOT NULL,
  department TEXT,
  role TEXT,
  tier INTEGER DEFAULT 1,
  hireDate TEXT,
  majorIssues INTEGER DEFAULT 0,
  active INTEGER DEFAULT 1,

  hoursTotal REAL DEFAULT 0,
  totalWeeks INTEGER DEFAULT 0,
  weeksOnTime INTEGER DEFAULT 0,

  updatedAt TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,           -- PIPELINE
  weekKey TEXT,                 -- e.g., 2025-W46
  startedAt TEXT NOT NULL,
  finishedAt TEXT,
  info TEXT,
  inserted INTEGER DEFAULT 0,
  updated INTEGER DEFAULT 0,
  rejected INTEGER DEFAULT 0,
  affected INTEGER DEFAULT 0,
  promoted INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS weekly_attendance (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  weekKey TEXT NOT NULL,
  weekStart TEXT,
  weekEnd TEXT,
  email TEXT NOT NULL,
  hoursWorked REAL NOT NULL,
  onTimeRatio REAL NOT NULL,
  lateCount INTEGER DEFAULT 0,
  majorIssues INTEGER DEFAULT 0,
  createdAt TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS promotion_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL,
  fromTier INTEGER NOT NULL,
  toTier INTEGER NOT NULL,
  reason TEXT,
  createdAt TEXT DEFAULT (datetime('now'))
);
"""

def connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db(conn):
    with conn:
        conn.executescript(SCHEMA)

def now_central_str():
    return datetime.now(tz=CENTRAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

def iso_week_key(dt: datetime) -> str:
    iso = dt.isocalendar()  # (year, week, weekday)
    return f"{iso[0]}-W{iso[1]:02d}"

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def newest_week_file():
    # If a file for "current" week exists use it; else fallback to newest Week*.json
    if not os.path.isdir(WEEKS_DIR):
        return None
    files = [os.path.join(WEEKS_DIR, f) for f in os.listdir(WEEKS_DIR) if f.lower().endswith(".json")]
    if not files:
        return None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]

def ensure_out():
    os.makedirs(OUT_DIR, exist_ok=True)

def as_bool(v, default=True):
    if isinstance(v, bool): return v
    if v is None: return default
    return str(v).strip().lower() in ("true","1","yes","y")

def start_run(conn, week_key: str):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO runs (type, weekKey, startedAt, info) VALUES (?, ?, ?, ?)",
        ("PIPELINE", week_key, now_central_str(), "manual pipeline")
    )
    conn.commit()
    return cur.lastrowid

def finish_run(conn, run_id, i=0, u=0, r=0, a=0, p=0):
    with conn:
        conn.execute(
            "UPDATE runs SET finishedAt=?, inserted=?, updated=?, rejected=?, affected=?, promoted=? WHERE id=?",
            (now_central_str(), i, u, r, a, p, run_id)
        )

def upsert_users(conn, users):
    inserted = updated = rejected = 0
    for u in users:
        email = str(u.get("email","")).strip().lower()
        if not email or not u.get("firstName") or not u.get("lastName"):
            rejected += 1
            print(f"⚠️  Rejected user (missing fields): {u}")
            continue
        employeeNum = (str(u.get("employeeNum","")).strip() or None)
        department  = (str(u.get("department","")).strip() or None)
        role        = (str(u.get("role","")).strip() or "Staff")
        tier        = int(u.get("tier", 1) or 1)
        hireDate    = (str(u.get("hireDate") or "").strip() or None)
        majorIssues = int(u.get("majorIssues", 0) or 0)
        active      = 1 if as_bool(u.get("active", True)) else 0

        exists = conn.execute("SELECT 1 FROM employees WHERE email=?", (email,)).fetchone() is not None
        with conn:
            conn.execute("""
                INSERT INTO employees (email, employeeNum, firstName, lastName, department, role, tier, hireDate, majorIssues, active, updatedAt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(email) DO UPDATE SET
                  employeeNum=excluded.employeeNum,
                  firstName=excluded.firstName,
                  lastName=excluded.lastName,
                  department=excluded.department,
                  role=excluded.role,
                  tier=excluded.tier,
                  hireDate=COALESCE(excluded.hireDate, hireDate),
                  majorIssues=excluded.majorIssues,
                  active=excluded.active,
                  updatedAt=datetime('now')
            """, (email, employeeNum, u["firstName"], u["lastName"], department, role, tier, hireDate, majorIssues, active))
        if exists:
            updated += 1
            print(f"🔁 User updated: {email}")
        else:
            inserted += 1
            print(f"🆕 User inserted: {email}")
    return inserted, updated, rejected

def process_week(conn, week_payload, week_key_for_csv) -> tuple:
    wk = week_payload
    weekKey    = wk.get("weekKey") or week_key_for_csv
    weekStart  = wk.get("weekStart")
    weekEnd    = wk.get("weekEnd")
    expected   = float(wk.get("expectedHours", EXPECTED_HOURS_DEFAULT))
    entries    = wk.get("entries", [])

    rows_csv = []
    affected = rejected = 0

    for e in entries:
        email = str(e.get("email","")).strip().lower()
        hours = float(e.get("hoursWorked", 0) or 0)
        workDays = int(e.get("workDays", 0) or 0)
        onTimeDays = int(e.get("onTimeDays", 0) or 0)
        lateCount = int(e.get("lateCount", 0) or 0)
        majorIssues = int(e.get("majorIssues", 0) or 0)

        if not email or workDays < 0 or onTimeDays < 0:
            rejected += 1
            print(f"⚠️  Rejected weekly entry: {e}")
            continue

        onTimeRatio = (onTimeDays / workDays) if workDays > 0 else 0.0
        exists = conn.execute("SELECT 1 FROM employees WHERE email=?", (email,)).fetchone()
        if not exists:
            rejected += 1
            print(f"⚠️  Rejected: employee not found → {email}")
            continue

        with conn:
            conn.execute("""
                INSERT INTO weekly_attendance (weekKey, weekStart, weekEnd, email, hoursWorked, onTimeRatio, lateCount, majorIssues)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (weekKey, weekStart, weekEnd, email, hours, onTimeRatio, lateCount, majorIssues))

        row = conn.execute("""
            SELECT hoursTotal, totalWeeks, weeksOnTime, majorIssues
            FROM employees WHERE email=?
        """, (email,)).fetchone()
        hoursTotal = (row[0] or 0) + hours
        totalWeeks = (row[1] or 0) + 1
        weeksOnTime = (row[2] or 0) + (1 if onTimeRatio >= 0.90 else 0)
        majorSum = (row[3] or 0) + max(0, majorIssues)

        with conn:
            conn.execute("""
                UPDATE employees
                   SET hoursTotal=?, totalWeeks=?, weeksOnTime=?, majorIssues=?, updatedAt=datetime('now')
                 WHERE email=?
            """, (hoursTotal, totalWeeks, weeksOnTime, majorSum, email))

        # status for CSV
        if hours == 0:
            status = "FAIL"
        elif 0 < hours < expected:
            status = "WARN"
        else:
            status = "PASS"
        rows_csv.append([email, hours, f"{int(onTimeRatio*100)}%", status])
        affected += 1
        print(f"🗓️  {email:<30} {hours:5.2f}h on-time={onTimeDays}/{workDays} ({onTimeRatio:.0%}) → {status}")

    # Write CSV summary
    ensure_out()
    csv_path = os.path.join(OUT_DIR, f"summary-{weekKey.replace(' ', '_')}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["email", "hoursWorked", "onTime%", "status"])
        w.writerows(rows_csv)
    print(f"📄 CSV saved: {csv_path}")

    return affected, rejected, csv_path

def ensure_out():
    os.makedirs(OUT_DIR, exist_ok=True)

def eligible_for_promo(hireDate: str, majorIssues: int, weeksOnTime: int, totalWeeks: int) -> bool:
    if not hireDate or (majorIssues or 0) > 0 or (totalWeeks or 0) <= 0:
        return False
    try:
        hired = datetime.fromisoformat(hireDate)
    except Exception:
        return False
    tenure_ok = (datetime.now(tz=CENTRAL_TZ) - hired.replace(tzinfo=CENTRAL_TZ)) >= timedelta(days=365*2)
    on_time_ratio = (weeksOnTime / totalWeeks) if totalWeeks else 0
    return tenure_ok and on_time_ratio >= 0.90

def promotions(conn) -> int:
    promoted = 0
    rows = conn.execute("""
        SELECT email, tier, role, hireDate, majorIssues, weeksOnTime, totalWeeks
          FROM employees
         WHERE active=1
    """).fetchall()
    for email, tier, role, hireDate, majorIssues, weeksOnTime, totalWeeks in rows:
        t = int(tier or 1)
        if t >= TIER_MAX:
            continue
        if eligible_for_promo(hireDate, int(majorIssues or 0), int(weeksOnTime or 0), int(totalWeeks or 0)):
            new_tier = t + 1
            new_role = ROLE_BY_TIER.get(new_tier, role)
            with conn:
                conn.execute("UPDATE employees SET tier=?, role=?, updatedAt=datetime('now') WHERE email=?",
                             (new_tier, new_role, email))
                conn.execute("INSERT INTO promotion_log (email, fromTier, toTier, reason) VALUES (?, ?, ?, ?)",
                             (email, t, new_tier, "2y tenure, 0 major issues, ≥90% on-time"))
            promoted += 1
            print(f"🏅 PROMOTED: {email:<30} Tier {t} → {new_tier} (role: {role} → {new_role})")
    if promoted == 0:
        print("🏅 Promotions: none this run")
    return promoted

def already_done_this_week(conn, week_key: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM runs WHERE type='PIPELINE' AND weekKey=? LIMIT 1", (week_key,)
    ).fetchone()
    return row is not None

def main():
    # Determine our "current week key"
    now_ct = datetime.now(tz=CENTRAL_TZ)
    week_key = iso_week_key(now_ct)

    conn = connect()
    init_db(conn)

    # Users
    users = load_json(DATA_USERS) if os.path.exists(DATA_USERS) else []
    run_id = start_run(conn, week_key)
    ins, upd, rej = upsert_users(conn, users)

    # Week payload (use newest weeks/*.json)
    wk_file = newest_week_file()
    if not wk_file:
        print("⚠️  No weekly file found in ./weeks — skipping weekly processing.")
        affected = 0
        rej_week = 0
        csv_path = None
    else:
        wk_payload = load_json(wk_file)
        # If weekKey missing in file, we still use our current computed week_key for CSV name
        affected, rej_week, csv_path = process_week(conn, wk_payload, week_key)

    # Promotions
    promoted = promotions(conn)

    # Finish run
    finish_run(conn, run_id, i=ins, u=upd, r=rej+rej_week, a=affected, p=promoted)
    conn.close()

    print("\n✅ PIPELINE DONE")
    print(f"   week_key: {week_key}")
    print(f"   users: {ins} inserted, {upd} updated, {rej} rejected")
    print(f"   weekly affected: {affected}, rejected: {rej+rej_week}")
    print(f"   promoted: {promoted}")
    if csv_path:
        print(f"   summary CSV: {csv_path}")

if __name__ == "__main__":
    main()
