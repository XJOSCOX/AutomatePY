#!/usr/bin/env python3
# Manual pipeline: upsert users, process ONLY not-yet-processed weeks, recompute aggregates,
# run promotions, write weekly + overtime CSVs, and log a PIPELINE run.

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
ROLE_BY_TIER = {1: "Staff", 2: "Senior", 3: "Lead"}  # adjust as desired

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
  weekKey TEXT,                 -- current ISO week when pipeline was invoked
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
  onTimeRatio REAL NOT NULL,    -- 0..1
  lateCount INTEGER DEFAULT 0,
  majorIssues INTEGER DEFAULT 0,
  createdAt TEXT DEFAULT (datetime('now'))
);
-- unique per (weekKey, email) for dedupe safety
CREATE UNIQUE INDEX IF NOT EXISTS idx_weekly_attendance_unique
ON weekly_attendance(weekKey, email);

CREATE TABLE IF NOT EXISTS promotion_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT NOT NULL,
  fromTier INTEGER NOT NULL,
  toTier INTEGER NOT NULL,
  reason TEXT,
  createdAt TEXT DEFAULT (datetime('now'))
);

-- NEW: marks a week as fully processed once (primary guard)
CREATE TABLE IF NOT EXISTS processed_weeks (
  weekKey TEXT PRIMARY KEY,
  processedAt TEXT NOT NULL,
  runId INTEGER
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

def now_ct_str():
    return datetime.now(tz=CENTRAL_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

def iso_week_key(dt: datetime) -> str:
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

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
        ("PIPELINE", week_key, now_ct_str(), "manual pipeline (new weeks only)")
    )
    conn.commit()
    return cur.lastrowid

def finish_run(conn, run_id, i=0, u=0, r=0, a=0, p=0):
    with conn:
        conn.execute(
            "UPDATE runs SET finishedAt=?, inserted=?, updated=?, rejected=?, affected=?, promoted=? WHERE id=?",
            (now_ct_str(), i, u, r, a, p, run_id)
        )

# ---------- processed_weeks helpers ----------
def is_week_processed(conn, week_key: str) -> bool:
    row = conn.execute("SELECT 1 FROM processed_weeks WHERE weekKey=?", (week_key,)).fetchone()
    return row is not None

def mark_week_processed(conn, week_key: str, run_id: int):
    with conn:
        conn.execute(
            "INSERT OR IGNORE INTO processed_weeks (weekKey, processedAt, runId) VALUES (?, ?, ?)",
            (week_key, now_ct_str(), run_id)
        )

# ---------- Users (roster) ----------
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

# ---------- Weeks processing ----------
def derive_week_key_from_payload(wk_payload) -> str:
    if wk_payload.get("weekKey"):
        return wk_payload["weekKey"]
    for k in ("weekStart","weekEnd"):
        if wk_payload.get(k):
            try:
                dt = datetime.fromisoformat(wk_payload[k]).replace(tzinfo=CENTRAL_TZ)
                y, w, _ = dt.isocalendar()
                return f"{y}-W{w:02d}"
            except Exception:
                pass
    return None

def upsert_week_entry(conn, weekKey, weekStart, weekEnd, email, hours, onTimeRatio, lateCount, majorIssues):
    with conn:
        conn.execute("""
            INSERT INTO weekly_attendance (weekKey, weekStart, weekEnd, email, hoursWorked, onTimeRatio, lateCount, majorIssues)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(weekKey, email) DO UPDATE SET
              weekStart=excluded.weekStart,
              weekEnd=excluded.weekEnd,
              hoursWorked=excluded.hoursWorked,
              onTimeRatio=excluded.onTimeRatio,
              lateCount=excluded.lateCount,
              majorIssues=excluded.majorIssues,
              createdAt=datetime('now');
        """, (weekKey, weekStart, weekEnd, email, hours, onTimeRatio, lateCount, majorIssues))

def process_one_week(conn, wk_payload, csv_week_key_fallback) -> tuple:
    """Insert/Update entries for a single weekly file, return (affected, rejected, weekKey, csv_path)."""
    weekKey = derive_week_key_from_payload(wk_payload) or csv_week_key_fallback
    weekStart  = wk_payload.get("weekStart")
    weekEnd    = wk_payload.get("weekEnd")
    expected   = float(wk_payload.get("expectedHours", EXPECTED_HOURS_DEFAULT))
    entries    = wk_payload.get("entries", [])

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

        exists = conn.execute("SELECT 1 FROM employees WHERE email=?", (email,)).fetchone()
        if not exists:
            rejected += 1
            print(f"⚠️  Rejected: employee not found → {email}")
            continue

        onTimeRatio = (onTimeDays / workDays) if workDays > 0 else 0.0
        upsert_week_entry(conn, weekKey, weekStart, weekEnd, email, hours, onTimeRatio, lateCount, majorIssues)

        status = "FAIL" if hours == 0 else ("WARN" if 0 < hours < expected else "PASS")
        rows_csv.append([weekKey, email, hours, f"{int(onTimeRatio*100)}%", status])
        affected += 1

    # write per-week CSV
    ensure_out()
    csv_path = os.path.join(OUT_DIR, f"summary-{weekKey.replace(' ', '_')}.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["weekKey", "email", "hoursWorked", "onTime%", "status"])
        w.writerows(rows_csv)
    print(f"📄 CSV saved: {csv_path}")

    return affected, rejected, weekKey, csv_path

def recompute_employee_aggregates(conn):
    with conn:
        conn.execute("UPDATE employees SET hoursTotal=0, totalWeeks=0, weeksOnTime=0;")
    rows = conn.execute("""
        SELECT email,
               SUM(hoursWorked) AS hsum,
               COUNT(*)         AS wcount,
               SUM(CASE WHEN onTimeRatio >= 0.90 THEN 1 ELSE 0 END) AS w_on_time
          FROM weekly_attendance
         GROUP BY email
    """).fetchall()
    with conn:
        for email, hsum, wcount, w_on_time in rows:
            conn.execute("""
                UPDATE employees
                   SET hoursTotal=?, totalWeeks=?, weeksOnTime=?, updatedAt=datetime('now')
                 WHERE email=?
            """, (hsum or 0.0, wcount or 0, w_on_time or 0, email))

# ---------- Promotions ----------
def eligible_for_promo(hireDate: str, majorIssues: int, weeksOnTime: int, totalWeeks: int) -> bool:
    if not hireDate or (majorIssues or 0) > 0 or (totalWeeks or 0) <= 0:
        return False
    try:
        hired = datetime.fromisoformat(hireDate).replace(tzinfo=CENTRAL_TZ)
    except Exception:
        return False
    tenure_ok = (datetime.now(tz=CENTRAL_TZ) - hired) >= timedelta(days=365*2)
    on_time_ratio = (weeksOnTime / totalWeeks) if totalWeeks else 0
    return tenure_ok and on_time_ratio >= 0.90

def run_promotions(conn) -> int:
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

# ---------- Overtime CSV ----------
def write_overtime_csv(conn):
    ensure_out()
    out_path = os.path.join(OUT_DIR, "performance_overtime.csv")
    rows = conn.execute("""
        SELECT weekKey, email, hoursWorked, onTimeRatio,
               CASE
                 WHEN hoursWorked=0 THEN 'FAIL'
                 WHEN hoursWorked < 40 THEN 'WARN'
                 ELSE 'PASS'
               END AS status
          FROM weekly_attendance
         ORDER BY weekKey, email
    """).fetchall()
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["weekKey","email","hoursWorked","onTime%","status"])
        for wk, email, hrs, r, status in rows:
            w.writerow([wk, email, round(hrs or 0.0, 2), f"{int((r or 0)*100)}%", status])
    print(f"📈 Overtime CSV saved: {out_path}")
    return out_path

# ---------- Main ----------
def main():
    now_ct = datetime.now(tz=CENTRAL_TZ)
    current_week_key = iso_week_key(now_ct)

    conn = connect()
    init_db(conn)
    run_id = start_run(conn, current_week_key)

    # 1) Users roster
    users = load_json(DATA_USERS) if os.path.exists(DATA_USERS) else []
    ins, upd, rej_users = upsert_users(conn, users)

    # 2) Process ONLY not-yet-processed weeks (oldest → newest)
    week_files = []
    if os.path.isdir(WEEKS_DIR):
        for fname in os.listdir(WEEKS_DIR):
            if fname.lower().endswith(".json"):
                full = os.path.join(WEEKS_DIR, fname)
                try:
                    payload = load_json(full)
                except Exception:
                    continue
                # compute the weekKey we would use
                wk_key = derive_week_key_from_payload(payload)
                if not wk_key and payload.get("weekStart"):
                    try:
                        dt = datetime.fromisoformat(payload["weekStart"]).replace(tzinfo=CENTRAL_TZ)
                        wk_key = iso_week_key(dt)
                    except Exception:
                        pass
                if not wk_key:
                    # last resort: sort by mtime; we'll compute fallback later
                    wk_key = f"mtime-{int(os.path.getmtime(full))}"

                week_files.append((wk_key, os.path.getmtime(full), full))

        # sort ascending so we process old weeks first
        week_files.sort(key=lambda x: (x[0], x[1]))

    total_affected = 0
    total_rejected = rej_users
    skipped_weeks = []
    processed_weeks = []
    last_csv_for_current = None

    for wk_key, _, wfile in week_files:
        # If this entry is an mtime-sentinel, we still need a proper key from payload
        payload = load_json(wfile)
        fallback = current_week_key
        computed_key = derive_week_key_from_payload(payload) or fallback

        # HARD CHECK: if week already processed, skip completely
        if is_week_processed(conn, computed_key):
            print(f"⏭️  Week already processed → {computed_key} (skip)")
            skipped_weeks.append(computed_key)
            continue

        affected, rej, effective_wk_key, csv_path = process_one_week(conn, payload, csv_week_key_fallback=fallback)
        total_affected += affected
        total_rejected += rej
        processed_weeks.append(effective_wk_key)

        # mark the week as processed
        mark_week_processed(conn, effective_wk_key, run_id)

        if effective_wk_key == current_week_key:
            last_csv_for_current = csv_path

    # 3) Recompute aggregates
    recompute_employee_aggregates(conn)

    # 4) Promotions
    promoted = run_promotions(conn)

    # 5) Overtime CSV (all weeks)
    overtime_csv = write_overtime_csv(conn)

    # Finish run log
    finish_run(conn, run_id, i=ins, u=upd, r=total_rejected, a=total_affected, p=promoted)
    conn.close()

    print("\n✅ PIPELINE COMPLETE")
    print(f"   current week: {current_week_key}")
    print(f"   users: {ins} inserted, {upd} updated, {rej_users} rejected")
    print(f"   weekly rows affected: {total_affected}, rejected: {total_rejected - rej_users}")
    print(f"   promoted: {promoted}")
    if skipped_weeks:
        print(f"   skipped weeks (already processed): {sorted(set(skipped_weeks))}")
    if processed_weeks:
        print(f"   newly processed weeks: {sorted(set(processed_weeks))}")
    if last_csv_for_current:
        print(f"   this-week CSV: {last_csv_for_current}")
    print(f"   overtime CSV: {overtime_csv}")

if __name__ == "__main__":
    main()
