# Auto HR Pipeline (SQLite + Python)

Data automation demo you can show in the interview. It ingests weekly workforce data, prevents duplicate processing, computes performance metrics **since hire date**, runs **auto-promotion** rules, and exports **full database tables to CSV** for audit/BI.

## Features

- **Manual pipeline (`run_pipeline.py`)**
  - Upserts employees from `data/users.json`
  - Processes **only new week files** from `weeks/` (oldest → newest)
  - Guards against duplicates via `processed_weeks` + unique `(weekKey,email)`
  - Recomputes aggregates **since hire date** (hours, total weeks, on-time weeks)
  - Applies **promotion** rule: ≥2 years tenure, 0 major issues, ≥90% on-time
  - Outputs per-week CSVs and an **overtime** CSV with **cumulative hours since hire**
  - Exports **every table** to CSV + a **denormalized weekly+employee** CSV

- **Auto runner (`friday_runner.py`)**
  - Keeps running and triggers the pipeline on **Fridays @ 20:00 Central** if not already run
  - Saturday catch-up if the Friday window was missed

## Project Structure

```
auto_hr/
├─ data/
│  └─ users.json
├─ weeks/
│  ├─ Week October 27 2025.json
│  ├─ Week November 3 2025.json
│  └─ Week November 10 2025.json
├─ out/
├─ employees.sqlite3
├─ run_pipeline.py
└─ friday_runner.py
```

## Requirements

- Python 3.11+
- No external packages required

## Quick Start

```bash
git clone <your-repo-url> auto_hr
cd auto_hr
mkdir -p data weeks out
python run_pipeline.py
python friday_runner.py
```

## Data Formats

### data/users.json
```json
[
  {
    "email": "john.doe@example.com",
    "employeeNum": "E-1001",
    "firstName": "John",
    "lastName": "Doe",
    "department": "Engineering",
    "role": "Staff",
    "hireDate": "2022-06-15",
    "majorIssues": 0,
    "tier": 1,
    "active": true
  }
]
```

### weeks/Week <Month> <DD> <YYYY>.json
```json
{
  "weekKey": "Week November 10 2025",
  "weekStart": "2025-11-10",
  "weekEnd": "2025-11-16",
  "expectedHours": 40,
  "entries": [
    {
      "email": "john.doe@example.com",
      "hoursWorked": 39.5,
      "onTimeDays": 5,
      "workDays": 5,
      "lateCount": 0,
      "majorIssues": 0
    }
  ]
}
```

## Step-by-Step Pipeline

1. Upsert users
2. Discover and process only new week files
3. Skip already processed weeks
4. Recompute since-hire aggregates
5. Apply promotions
6. Export all data to CSVs

## Database Schema

- **employees**
- **weekly_attendance**
- **processed_weeks**
- **promotion_log**
- **runs**

## Outputs

- `out/summary-<weekKey>.csv`
- `out/performance_overtime.csv`
- `out/employees.csv`
- `out/weekly_attendance.csv`
- `out/promotion_log.csv`
- `out/processed_weeks.csv`
- `out/runs.csv`
- `out/weekly_with_employee.csv`

## License

MIT
