# NL-RailTraffic-ETL-Pipeline

> **End-to-end data pipeline for Dutch Railways disruption analysis**  
> Python · SQL · SQLite → Azure-ready · GitHub Actions CI/CD · Docker

[![Pipeline Status](https://github.com/yourname/nl-railtraffic-etl-pipeline/actions/workflows/daily_pipeline.yml/badge.svg)](https://github.com/yourname/nl-railtraffic-etl-pipeline/actions)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![SQL](https://img.shields.io/badge/SQL-SQLite%20%7C%20Azure%20SQL-orange)
![License](https://img.shields.io/badge/License-MIT-green)

---

## Project Overview

### The Problem
NS (Nederlandse Spoorwegen) publishes real-time disruption data via API, but no persistent, queryable history exists. Analysts cannot answer questions like: *"Which stations experience the most delays on Monday mornings?"* or *"Has maintenance downtime improved over the past 30 days?"*

### The Solution
An automated ETL pipeline that fetches, validates, transforms, and stores NS disruption data daily — producing a clean, analytics-ready database with pre-calculated KPIs.

### Business Value
| Question | Answer from this pipeline |
|---|---|
| Which stations are most disruption-prone? | `station_disruption_stats` view, ranked by incident count |
| What's the 7-day rolling trend? | `rolling_7day_total` window function in analytics query |
| Are cancellations increasing? | `cancellation_rate_pct` calculated daily via CTR logic |
| How severe are current disruptions? | `impact_level` 1–5 score, queryable in real time |

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| **Ingestion** | Python `requests`, retry logic | NS API with exponential backoff |
| **Validation** | Custom validators + Great Expectations | Schema & data quality checks |
| **Transformation** | `pandas`, custom business logic | Clean, type-cast, derive metrics |
| **Storage** | SQLite (local) → Azure SQL (prod) | Normalized relational schema |
| **Orchestration** | GitHub Actions (cron daily 06:00 UTC) | Automated scheduling + CI/CD |
| **Containerization** | Docker + Docker Compose | Reproducible runtime environment |
| **Analytics** | Complex SQL: CTEs, window functions | Pre-aggregated KPI tables |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     GitHub Actions (06:00 UTC)                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │ triggers
                           ▼
┌──────────────────────────────────────────────────────────────────┐
│                        ETL Pipeline                              │
│                                                                  │
│  ┌─────────────┐    ┌──────────────┐    ┌───────────────────┐   │
│  │  EXTRACT    │    │  TRANSFORM   │    │      LOAD         │   │
│  │             │    │              │    │                   │   │
│  │ NS API v3   │───▶│ cleaners.py  │───▶│ raw_disruptions   │   │
│  │ (disruptions│    │ - type norm  │    │ disruptions       │   │
│  │  endpoint)  │    │ - timestamp  │    │ daily_stats       │   │
│  │             │    │ - impact lvl │    │                   │   │
│  │ Retry: 3x   │    │ - station    │    │ UPSERT logic      │   │
│  │ Backoff: 2^n│    │   extraction │    │ (idempotent)      │   │
│  └─────────────┘    └──────────────┘    └───────────────────┘   │
│                                                 │                │
└─────────────────────────────────────────────────┼────────────────┘
                                                  │
                           ┌──────────────────────┼───────────────┐
                           │         SQLite / Azure SQL            │
                           │                                       │
                           │  raw_disruptions  disruptions         │
                           │  stations         daily_stats         │
                           │  ── Views ──────────────────          │
                           │  active_disruptions                   │
                           │  station_disruption_stats             │
                           └───────────────────────────────────────┘
```

**Azure Migration Path** (in progress):
```
Local SQLite ──▶ Azure SQL Database (Basic tier)
data/raw/      ──▶ Azure Blob Storage (hierarchical: year/month/day)
GitHub Actions ──▶ Azure Data Factory trigger
```

---

## Key Features

### 1. Idempotent Incremental Loading
Every run is safe to re-run. New records are inserted; existing records are updated (UPSERT). No duplicates, no data loss.

```python
# src/pipeline.py — UPSERT logic
self.database.cursor.execute(
    "SELECT id FROM disruptions WHERE disruption_id = ?",
    (row['disruption_id'],)
)
exists = self.database.cursor.fetchone()

if exists:
    # UPDATE — refresh mutable fields only
else:
    # INSERT — new disruption
```

### 2. Resilient API Client with Exponential Backoff
NS API occasionally times out. The client retries up to 3 times with exponential wait (2s → 4s → 8s), then fails gracefully with structured logging.

```python
wait_time = 2 ** attempt  # 2, 4, 8 seconds
```

### 3. Business-Logic Impact Scoring
Each disruption gets an `impact_level` (1–5) based on type and duration — enabling priority filtering without manual annotation.

```python
def _calculate_impact_level(self, row):
    if disruption_type == 'calamity':        return 5
    elif disruption_type == 'maintenance':
        return 4 if duration > 240 else 3   # >4hrs = critical
    elif disruption_type == 'disruption':
        return 4 if duration > 120 else 3   # >2hrs = high
    else:                                    return 2
```

### 4. Normalized Schema with Analytics-Ready Views

Two views pre-join the data for common analyst queries:
```sql
-- Active disruptions with time-remaining calculation
SELECT *, 
    CAST((julianday(end_time) - julianday('now')) * 1440 AS INTEGER) 
    AS remaining_minutes
FROM disruptions
WHERE is_resolved = 0 AND end_time > datetime('now');
```

---

## SQL Skills Showcase

The analytics layer (`src/transformation/aggregators.py`) demonstrates production-grade SQL patterns. Below are highlights with explanations — all queries run against real pipeline data.

### Query 1 — 7-Day Rolling Window + Type Breakdown
```sql
WITH disruption_metrics AS (
    SELECT 
        DATE(start_time)   AS disruption_date,
        type,
        COUNT(*)           AS incident_count,
        AVG(
            (julianday(end_time) - julianday(start_time)) * 1440
        )                  AS avg_duration_minutes,

        -- Running total over the past 7 days (sliding window)
        SUM(COUNT(*)) OVER (
            ORDER BY DATE(start_time)
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )                  AS rolling_7day_total

    FROM disruptions
    WHERE start_time >= date('now', '-30 days')
    GROUP BY DATE(start_time), type
)
SELECT * FROM disruption_metrics
ORDER BY disruption_date DESC;
```
**Concepts used:** CTE, window function with frame clause (`ROWS BETWEEN`), date arithmetic

---

### Query 2 — Station Severity Percentile Ranking
```sql
WITH station_impact AS (
    SELECT 
        station_code,
        COUNT(*)  AS disruption_count,

        -- Percentile rank: 0.9 = top 10% most-disrupted stations
        PERCENT_RANK() OVER (ORDER BY COUNT(*)) AS severity_percentile

    FROM (
        -- Unnest comma-separated station codes into rows
        SELECT TRIM(value) AS station_code
        FROM disruptions,
        json_each('["' || REPLACE(affected_stations, ',', '","') || '"]')
    )
    GROUP BY station_code
)
SELECT * FROM station_impact
WHERE severity_percentile > 0.9   -- Only show worst stations
ORDER BY disruption_count DESC;
```
**Concepts used:** `PERCENT_RANK()`, string manipulation to unnest arrays, subquery flattening

---

### Query 3 — Day-over-Day Change with LAG
```sql
SELECT
    DATE(start_time)                        AS disruption_date,
    COUNT(*)                                AS daily_count,
    LAG(COUNT(*)) OVER (ORDER BY DATE(start_time)) AS prev_day_count,
    COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE(start_time)) AS day_over_day_delta,
    ROUND(
        100.0 * (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE(start_time)))
              / NULLIF(LAG(COUNT(*)) OVER (ORDER BY DATE(start_time)), 0),
        1
    )                                       AS pct_change
FROM disruptions
GROUP BY DATE(start_time)
ORDER BY disruption_date DESC;
```
**Concepts used:** `LAG()`, `NULLIF()` for safe division, percentage calculation

---

### Query 4 — Peak Hour Analysis with RANK
```sql
SELECT
    STRFTIME('%H', start_time)              AS hour_of_day,
    COUNT(*)                                AS disruption_count,
    ROUND(AVG(duration_minutes), 1)         AS avg_duration,
    RANK() OVER (ORDER BY COUNT(*) DESC)    AS severity_rank
FROM disruptions
WHERE type = 'disruption'
GROUP BY hour_of_day
ORDER BY severity_rank;
```
**Concepts used:** `STRFTIME` for time bucketing, `RANK()` window function

---

## Data Model

```
raw_disruptions          disruptions                 stations
─────────────────        ─────────────────────────   ────────────────
id (PK)                  id (PK)                     station_code (PK)
disruption_id (UQ)  ──▶  disruption_id (UQ, FK)      station_name
raw_json                 type                        latitude
fetched_at               title                       longitude
                         description
                         start_time          daily_stats
                         end_time            ───────────────
                         duration_minutes    date (PK)
                         impact_level        total_disruptions
                         affected_stations   avg_duration_minutes
                         is_resolved         most_affected_station
                         created_at          peak_hour
                         updated_at          calculated_at
```

**Design decisions:**
- `raw_disruptions` stores original JSON for reprocessing without re-fetching
- `affected_stations` is stored as comma-separated codes (denormalized) for query simplicity; a junction table would be used at scale
- `impact_level` is derived, not stored in raw data — business logic lives in Python, not the DB

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/yourname/nl-railtraffic-etl-pipeline
cd nl-railtraffic-etl-pipeline
pip install -r requirements.txt

# 2. Configure API key
cp .env.example .env
# Edit .env: NS_API_KEY=your_key_here
# Get a free key at: https://apiportal.ns.nl

# 3. Initialize database
python src/storage/database.py

# 4. Run pipeline
python src/pipeline.py

# 5. Query results
sqlite3 data/nl_rail.db "SELECT * FROM station_disruption_stats LIMIT 10;"
sqlite3 data/nl_rail.db "SELECT * FROM active_disruptions;"
```

### Docker
```bash
docker-compose -f docker/docker-compose.yml up
```

---

## Project Highlights (for Interviewers)

| Topic | What I built | Why it matters |
|---|---|---|
| **Data Modeling** | 4-table normalized schema with 2 views | Separates raw from clean; supports reprocessing |
| **SQL Complexity** | CTEs, window functions, percentile ranking | Mirrors dbt model patterns |
| **Idempotency** | UPSERT on every run | Safe for daily scheduling — no manual deduplication |
| **Error Handling** | Retry with backoff, per-record exception isolation | Pipeline doesn't crash on a single bad record |
| **CI/CD** | GitHub Actions cron job, artifact upload | Production-grade automation |
| **Observability** | Structured logging (file + stdout), daily stats report | Monitoring without a full observability stack |
| **Cloud-readiness** | Env-var config, Docker, clear Azure migration path | Easy to move from SQLite → Azure SQL |

---

## Roadmap

### Phase 2 — Azure Migration (in progress)
- [ ] `api_client.py`: dual-write raw JSON to Azure Blob Storage
- [ ] `database.py`: environment-switched connection (SQLite local / Azure SQL prod)
- [ ] GitHub Actions: add `AZURE_CONNECTION_STRING` secret
- [ ] Azure Data Factory: replace cron with ADF trigger

### Phase 3 — Analytics Layer
- [ ] dbt project for transformation layer (replacing `cleaners.py`)
- [ ] Power BI dashboard connected to Azure SQL
- [ ] Data quality alerts via Great Expectations

### Phase 4 — Scale
- [ ] Replace SQLite with PostgreSQL for concurrent access
- [ ] Partition `disruptions` table by month
- [ ] Add Airflow DAG for dependency management

---

## About

**Skills Track:** Python ETL · SQL (CTEs, window functions) · Data modeling · CI/CD pipelines · Cloud architecture design · Incremental loading patterns
