# Crypto Market Pipeline — Full Project Plan

**Stack:** CoinGecko API · Snowflake · dbt Core · GitHub Actions · Snowflake Streams & Tasks

---

## Project overview

A production-grade daily crypto market data pipeline that ingests OHLC prices and market cap data for the top 50 tokens from CoinGecko, lands them in a shared Snowflake landing database via a scheduled GitHub Actions job (daily, 8AM Bangkok time), and promotes them through dbt-built Silver/Gold layers across three isolated environment databases (dev → qa → prod) — with CI/CD and environment promotion gates enforced through GitHub Actions.

> A Snowflake-native Python Task alternative for ingestion exists in the codebase (`snowflake/ingest_task.sql`, Step 3b) but is currently **disabled** — it requires an External Access Integration, which isn't available on the Snowflake trial account this was built against. The code is kept commented out for later, once the account is upgraded. Production ingestion runs the GitHub Actions way (Step 3a / Step 5) for now.

### Database / schema architecture

Instead of a single database with Bronze/Silver/Gold schemas, the pipeline is split across **four databases**, separating the shared landing zone from environment-specific transformed data:

| Database | Purpose | Schemas |
|---|---|---|
| `CRYPTO_DB` | Landing zone — raw CoinGecko data, shared across all environments | `STG` |
| `DEV_DB` | dbt development target | `SILVER_CRYPTO`, `GOLD_CRYPTO` |
| `QA_DB` | dbt QA target — validated before prod promotion | `SILVER_CRYPTO`, `GOLD_CRYPTO` |
| `PROD_DB` | dbt production target | `SILVER_CRYPTO`, `GOLD_CRYPTO` |

Raw ingestion always lands in `CRYPTO_DB.STG` — there is only ever one copy of raw data. dbt then builds Silver/Gold into whichever database the active `--target` (`dev` / `qa` / `prod`) points to, reading from the same shared source every time. A single Snowflake role (`CRYPTO_PIPELINE_ROLE`) and warehouse (`CRYPTO_WH`) are shared across all four databases.

---

## Folder structure

```
coin-dwh-ml/
├── .sqlfluff                        # dialect=snowflake, used by infra-ci.yml
├── .github/
│   └── workflows/
│       ├── infra-ci.yml             # PR: sqlfluff syntax check on snowflake/**
│       ├── infra-deploy.yml         # merge to main: apply setup/streams/tasks to Snowflake (ingest_task disabled)
│       ├── de-ingest.yml            # cron 8AM Bangkok: run ingest_coingecko.py -> CRYPTO_DB.STG (standalone)
│       ├── de-ci.yml                # PR: dbt build + test against dev_db
│       └── de-deploy.yml            # promote dev_db -> qa_db -> prod_db (dbt only, no ingestion)
└── de-pipeline/
    ├── ingestion/
    │   ├── ingest_coingecko.py      # PRODUCTION ingestion (run by de-ingest.yml) + local testing
    │   ├── requirements.txt
    │   ├── config.py                # env-based config (INGEST_MODE=local|snowflake)
    │   ├── .env                     # gitignored — local secrets/mode override
    │   └── data/                    # gitignored — CSV output from local runs
    ├── dbt/
    │   ├── dbt_project.yml
    │   ├── profiles.yml             # uses env vars — never commit secrets
    │   ├── packages.yml
    │   ├── models/
    │   │   ├── silver/
    │   │   │   ├── sources.yml               # source: CRYPTO_DB.STG.COINGECKO_RAW
    │   │   │   ├── stg_crypto_prices.sql
    │   │   │   └── stg_crypto_prices.yml     # schema + tests
    │   │   └── gold/
    │   │       ├── mart_daily_returns.sql
    │   │       ├── mart_top_movers.sql
    │   │       └── mart_volatility_30d.sql
    │   ├── snapshots/
    │   │   └── snap_market_cap_history.sql
    │   ├── seeds/
    │   │   └── coin_metadata.csv             # token name, symbol, category
    │   ├── macros/
    │   │   └── generate_schema_name.sql      # pins schema to silver_crypto/gold_crypto (no target prefix)
    │   └── tests/
    │       └── assert_price_positive.sql
    ├── snowflake/
    │   ├── setup.sql                # warehouses, databases, roles, schemas
    │   ├── streams.sql              # Snowflake Stream definition
    │   ├── tasks.sql                # Snowflake Task (new-data signal)
    │   ├── ingest_task.sql          # DISABLED — Snowflake Task ingestion, needs External Access Integration (not on trial)
    │   ├── run_sql.py               # applies *.sql via key-pair auth (used by infra-deploy.yml)
    │   └── requirements.txt         # snowflake-connector-python, cryptography
    ├── .keys/                       # gitignored — local rsa_key.p8 / rsa_key.pub
    └── README.md
```

---
Update README.md file
- instruct to create a service account to use for the developement using KEY-PAIR. Give permission for SYSADMIN and USERADMIN
- how to setup python locally and run the ingestion locally. Local instructions must be for windows 11 CMD and linux/mac
---

## Step 1 — Snowflake setup

Run `snowflake/setup.sql` once as SYSADMIN to create all objects.

```sql
-- snowflake/setup.sql

-- Warehouse (shared across all environments)
CREATE WAREHOUSE IF NOT EXISTS CRYPTO_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

-- ============================================================
-- CRYPTO_DB — landing zone, shared across all environments
-- ============================================================
CREATE DATABASE IF NOT EXISTS CRYPTO_DB;
CREATE SCHEMA IF NOT EXISTS CRYPTO_DB.STG;   -- raw ingestion (append-only)

CREATE TABLE IF NOT EXISTS CRYPTO_DB.STG.COINGECKO_RAW (
  id                       VARCHAR,
  symbol                   VARCHAR,
  name                     VARCHAR,
  current_price            FLOAT,
  market_cap               FLOAT,
  total_volume             FLOAT,
  price_change_24h         FLOAT,
  price_change_pct_24h     FLOAT,
  high_24h                 FLOAT,
  low_24h                  FLOAT,
  circulating_supply       FLOAT,
  ath                      FLOAT,
  fetched_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  _loaded_at               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- DEV_DB — dbt development target
-- ============================================================
CREATE DATABASE IF NOT EXISTS DEV_DB;
CREATE SCHEMA IF NOT EXISTS DEV_DB.SILVER_CRYPTO;
CREATE SCHEMA IF NOT EXISTS DEV_DB.GOLD_CRYPTO;

-- ============================================================
-- QA_DB — dbt QA target, validated before prod promotion
-- ============================================================
CREATE DATABASE IF NOT EXISTS QA_DB;
CREATE SCHEMA IF NOT EXISTS QA_DB.SILVER_CRYPTO;
CREATE SCHEMA IF NOT EXISTS QA_DB.GOLD_CRYPTO;

-- ============================================================
-- PROD_DB — dbt production target
-- ============================================================
CREATE DATABASE IF NOT EXISTS PROD_DB;
CREATE SCHEMA IF NOT EXISTS PROD_DB.SILVER_CRYPTO;
CREATE SCHEMA IF NOT EXISTS PROD_DB.GOLD_CRYPTO;

-- ============================================================
-- Service account role for ingestion + dbt — shared across all databases
-- ============================================================
CREATE ROLE IF NOT EXISTS CRYPTO_PIPELINE_ROLE;
GRANT USAGE ON WAREHOUSE CRYPTO_WH TO ROLE CRYPTO_PIPELINE_ROLE;

GRANT ALL ON DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE CRYPTO_DB TO ROLE CRYPTO_PIPELINE_ROLE;

GRANT ALL ON DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL VIEWS IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE VIEWS IN DATABASE DEV_DB TO ROLE CRYPTO_PIPELINE_ROLE;

GRANT ALL ON DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL VIEWS IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE VIEWS IN DATABASE QA_DB TO ROLE CRYPTO_PIPELINE_ROLE;

GRANT ALL ON DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL SCHEMAS IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL TABLES IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE TABLES IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON ALL VIEWS IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
GRANT ALL ON FUTURE VIEWS IN DATABASE PROD_DB TO ROLE CRYPTO_PIPELINE_ROLE;
```

---

## Step 2 — Snowflake Stream & Task

A single Stream/Task pair lives on the shared landing table — it only signals that new raw data has arrived. It does **not** chain Bronze→Silver→Gold anymore, since Silver/Gold now live in separate per-environment databases and are built by dbt (`de-deploy.yml`), not by a Snowflake Task DAG.

```sql
-- snowflake/streams.sql
-- IF NOT EXISTS (not OR REPLACE) — this script re-runs on every infra
-- deploy, and CREATE OR REPLACE STREAM would reset the tracked offset,
-- silently re-surfacing already-consumed rows as "new" every deploy.

-- Captures every new INSERT on the raw landing table
CREATE STREAM IF NOT EXISTS CRYPTO_DB.STG.STREAM_COINGECKO_RAW
  ON TABLE CRYPTO_DB.STG.COINGECKO_RAW
  APPEND_ONLY = TRUE;
```

```sql
-- snowflake/tasks.sql

-- Fires when the stream has unconsumed rows (polls hourly).
-- This is a lightweight signal only — actual Silver/Gold builds
-- run via dbt in GitHub Actions (de-deploy.yml), scoped per
-- environment (dev_db / qa_db / prod_db) by --target.
CREATE OR REPLACE TASK CRYPTO_DB.STG.TASK_NOTIFY_NEW_DATA
  WAREHOUSE = CRYPTO_WH
  SCHEDULE  = '60 MINUTE'
WHEN
  SYSTEM$STREAM_HAS_DATA('CRYPTO_DB.STG.STREAM_COINGECKO_RAW')
AS
  SELECT CURRENT_TIMESTAMP();

ALTER TASK CRYPTO_DB.STG.TASK_NOTIFY_NEW_DATA RESUME;
```

---

## Step 3 — Ingestion

- **3a. `ingestion/ingest_coingecko.py`** — this is what actually runs in production. The same script serves double duty: run it locally (`INGEST_MODE=local`, the default) to test against a CSV with no Snowflake credentials, or let its **own dedicated workflow `de-ingest.yml`** run it on a daily schedule (`INGEST_MODE=snowflake`) to load `CRYPTO_DB.STG.COINGECKO_RAW`. Ingestion is deliberately a **separate** GitHub Action from dbt promotion (`de-deploy.yml`, Step 5) — the two run on independent schedules and neither blocks the other.
- **3b. A Snowflake Python Task** (`snowflake/ingest_task.sql`) — a from-inside-Snowflake alternative, **currently disabled**. It requires an External Access Integration for network egress, which the Snowflake trial account this was built against can't create. The code stays in the repo, commented out, for when the account is upgraded — see the note at the end of 3b for what re-enabling it involves.

### 3a. Production + local ingestion script

`config.py` / `INGEST_MODE` supports two modes:

- **`local`** (default) — fetches from CoinGecko and writes a CSV to `ingestion/data/`. No Snowflake credentials required. Use this to test the script and eyeball the data before it ever touches Snowflake.
- **`snowflake`** — same fetch, but loads rows into the shared landing table `CRYPTO_DB.STG.COINGECKO_RAW` instead of writing a CSV. This is the mode `de-ingest.yml` runs in production (see below); running it locally is for manually verifying the write path before trusting the scheduled job.

Both modes share the same `fetch_coins()` / `build_rows()` logic, so there's nothing to keep in sync between "what I tested locally" and "what gets loaded."

#### `config.py` — env-based config

```python
# ingestion/config.py
import os

from dotenv import load_dotenv

load_dotenv()  # loads ingestion/.env if present — never commit that file

# "local" writes a CSV under CSV_OUTPUT_DIR; "snowflake" loads into Snowflake
INGEST_MODE = os.getenv("INGEST_MODE", "local")

CSV_OUTPUT_DIR = os.getenv("CSV_OUTPUT_DIR", "data")

SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "CRYPTO_PIPELINE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "CRYPTO_WH")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "CRYPTO_DB")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "STG")
```

#### `ingest_coingecko.py`

```python
# ingestion/ingest_coingecko.py
import csv
import os
from datetime import datetime, timezone

import requests
import snowflake.connector

import config

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1,
    "sparkline": False,
}

FIELDNAMES = [
    "id", "symbol", "name", "current_price", "market_cap", "total_volume",
    "price_change_24h", "price_change_pct_24h", "high_24h", "low_24h",
    "circulating_supply", "ath", "fetched_at",
]

def fetch_coins():
    resp = requests.get(COINGECKO_URL, params=PARAMS, timeout=30)
    resp.raise_for_status()
    return resp.json()

def build_rows(coins: list, fetched_at: str) -> list[dict]:
    return [
        {
            "id": c["id"],
            "symbol": c["symbol"],
            "name": c["name"],
            "current_price": c.get("current_price"),
            "market_cap": c.get("market_cap"),
            "total_volume": c.get("total_volume"),
            "price_change_24h": c.get("price_change_24h"),
            "price_change_pct_24h": c.get("price_change_percentage_24h"),
            "high_24h": c.get("high_24h"),
            "low_24h": c.get("low_24h"),
            "circulating_supply": c.get("circulating_supply"),
            "ath": c.get("ath"),
            "fetched_at": fetched_at,
        }
        for c in coins
    ]

def save_to_csv(rows: list[dict], fetched_at: str) -> str:
    os.makedirs(config.CSV_OUTPUT_DIR, exist_ok=True)
    stamp = fetched_at.replace("+00:00", "Z").replace(":", "").replace("-", "")
    path = os.path.join(config.CSV_OUTPUT_DIR, f"coingecko_raw_{stamp}.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[local] Wrote {len(rows)} rows to {path}")
    return path

def get_snowflake_conn():
    return snowflake.connector.connect(
        account   = config.SNOWFLAKE_ACCOUNT,
        user      = config.SNOWFLAKE_USER,
        password  = config.SNOWFLAKE_PASSWORD,
        role      = config.SNOWFLAKE_ROLE,
        warehouse = config.SNOWFLAKE_WAREHOUSE,
        database  = config.SNOWFLAKE_DATABASE,
        schema    = config.SNOWFLAKE_SCHEMA,
    )

def save_to_snowflake(rows: list[dict]) -> None:
    insert_sql = f"""
        INSERT INTO {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.COINGECKO_RAW (
            id, symbol, name, current_price, market_cap,
            total_volume, price_change_24h, price_change_pct_24h,
            high_24h, low_24h, circulating_supply, ath, fetched_at
        ) VALUES (
            %(id)s, %(symbol)s, %(name)s, %(current_price)s, %(market_cap)s,
            %(total_volume)s, %(price_change_24h)s, %(price_change_pct_24h)s,
            %(high_24h)s, %(low_24h)s, %(circulating_supply)s, %(ath)s, %(fetched_at)s
        )
    """
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        cur.executemany(insert_sql, rows)
        conn.commit()
        print(f"[snowflake] Loaded {len(rows)} rows into "
              f"{config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.COINGECKO_RAW")
    finally:
        conn.close()

def main():
    coins = fetch_coins()
    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = build_rows(coins, fetched_at)

    if config.INGEST_MODE == "snowflake":
        save_to_snowflake(rows)
    else:
        save_to_csv(rows, fetched_at)

if __name__ == "__main__":
    main()
```

```
# ingestion/requirements.txt
requests==2.31.0
snowflake-connector-python==3.13.2
python-dotenv==1.0.1
```

#### Local virtual environment & testing

```cmd
:: Windows cmd, from repo root
cd de-pipeline
python -m venv .venv
.venv\Scripts\activate.bat
pip install -r ingestion\requirements.txt
```

```bash
# Linux / macOS
cd de-pipeline
python -m venv .venv
source .venv/bin/activate
pip install -r ingestion/requirements.txt
```

Run in local mode first — no Snowflake credentials needed, output lands in `ingestion/data/coingecko_raw_<timestamp>.csv`:

```cmd
python ingestion\ingest_coingecko.py
```

```bash
python ingestion/ingest_coingecko.py
```

Once the CSV looks correct, switch to Snowflake mode by setting `INGEST_MODE=snowflake` plus credentials (either export env vars or drop them in `ingestion/.env`, which `config.py` loads automatically via `python-dotenv`):

```
# ingestion/.env (gitignored — never commit)
INGEST_MODE=snowflake
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=DEVELOPER_SVC
SNOWFLAKE_PASSWORD=********
```

```cmd
python ingestion\ingest_coingecko.py
```

```bash
python ingestion/ingest_coingecko.py
```

Add `.venv/`, `ingestion/data/`, and `ingestion/.env` to `.gitignore` — none of them should be committed.

#### Scheduled ingestion — `de-ingest.yml`

Production ingestion runs as its **own** GitHub Actions workflow, separate from `de-deploy.yml`. Keeping it standalone means: the daily data pull can be retried on its own without re-running dbt, its logs and run history aren't interleaved with promotion runs, and a change to the ingestion script triggers nothing else. It runs daily at **8:00 AM Bangkok time (ICT, UTC+7)** — `1:00 AM UTC`, since GitHub Actions cron is always specified in UTC and Thailand has no daylight saving to complicate the offset — plus a manual **Run workflow** button.

```yaml
# .github/workflows/de-ingest.yml
name: DE Ingest

on:
  schedule:
    - cron: '0 1 * * *'   # 1:00 AM UTC = 8:00 AM Bangkok time (ICT, UTC+7, no DST)
  workflow_dispatch:        # manual run button

jobs:
  ingest:
    name: Ingest CoinGecko -> CRYPTO_DB.STG.COINGECKO_RAW
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r de-pipeline/ingestion/requirements.txt

      - name: Run ingestion (INGEST_MODE=snowflake)
        working-directory: de-pipeline/ingestion
        run: python ingest_coingecko.py
        env:
          INGEST_MODE:        snowflake
          SNOWFLAKE_ACCOUNT:  ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER:     ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
```

It authenticates with the same `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` (`CRYPTO_PIPELINE_ROLE`) secrets as the dbt workflows — `config.py` reads them straight from the environment (Step 6). There's no `push:` trigger: the script's behaviour doesn't change on merge, only on the schedule, so a code push doesn't need to fire a real data load.

### 3b. Snowflake Python Task ingestion (disabled — kept for later)

**Status: not in use.** This was designed as a from-inside-Snowflake alternative to 3a — a Snowflake Task calling a Python stored procedure (Snowpark) that fetches CoinGecko and appends straight into `CRYPTO_DB.STG.COINGECKO_RAW`. It requires an **External Access Integration** for network egress to `api.coingecko.com`, and creating one needs the global `CREATE INTEGRATION` privilege — which is not available on the Snowflake trial account this project was built against. Rather than delete the work, the file stays in the repo with its SQL commented out, and is **not** included in `infra-deploy.yml`'s applied-file list, so it has zero effect on the running pipeline. Production ingestion is 3a (`de-ingest.yml`, GitHub Actions cron) instead.

The full commented-out file is at [`snowflake/ingest_task.sql`](../de-pipeline/snowflake/ingest_task.sql); its header documents the exact steps to re-enable it (create the network rule + integration + `USAGE` grant as `ACCOUNTADMIN`, uncomment the body, add it to `run_sql.py`'s file list in `infra-deploy.yml`, and decide whether to retire `de-ingest.yml` so the two don't double-insert).

If it's ever re-enabled, rows it appends would still flow through the existing `STREAM_COINGECKO_RAW` / `TASK_NOTIFY_NEW_DATA` from Step 2 unchanged — that stream tracks inserts on the table regardless of what inserted them.

---

## Deployment Step-1 — CI/CD for the Snowflake infra scripts (Steps 1–2)

`infra-ci.yml` / `infra-deploy.yml` deploy **only** `snowflake/setup.sql`, `streams.sql`, and `tasks.sql` — the infra created in Steps 1–2. `ingest_task.sql` (Step 3b) is deliberately **not** in this list — it's disabled (see 3b), and its SQL is fully commented out, so there's nothing for `infra-deploy.yml` to apply. This is separate from `de-ci.yml` / `de-deploy.yml` (Step 5, dbt) and `de-ingest.yml` (Step 3a, ingestion). Splitting them means an infra change (e.g. a new schema), a dbt change (e.g. a new mart), and an ingestion change each trigger independent, correctly-scoped pipelines.

- **CI does not touch real Snowflake.** It runs `sqlfluff parse` against the SQL — a genuine syntax check (catches malformed DDL) without needing credentials in a PR context, which matters once forks can open PRs.
- **Deploy actually applies the SQL to Snowflake**, using the `DEVELOPER_SVC` key-pair service account from `README.md` (`SYSADMIN` + `USERADMIN`) via a small connector script, since GitHub's `ubuntu-latest` runners don't ship SnowSQL.
- All three scripts are safe to re-run: `setup.sql` uses `CREATE ... IF NOT EXISTS` throughout, and `streams.sql` was changed from `CREATE OR REPLACE STREAM` to `CREATE STREAM IF NOT EXISTS` specifically so repeat deploys don't reset the stream's tracked offset.
- If Step 3b is ever re-enabled, add `ingest_task.sql` back to the `run_sql.py` file list below — but only after its `ACCOUNTADMIN` prerequisites exist, or `CREATE PROCEDURE` will fail every deploy (it references an integration that must already exist at creation time).

### `run_sql.py` — applies SQL files via key-pair auth

```python
# snowflake/run_sql.py
import argparse
import os
import sys

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def load_private_key_der(pem: str, passphrase: str | None) -> bytes:
    key = serialization.load_pem_private_key(
        pem.encode(),
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection():
    return snowflake.connector.connect(
        account            = os.environ["SNOWFLAKE_ACCOUNT"],
        user               = os.environ["SNOWFLAKE_DEV_SVC_USER"],
        private_key        = load_private_key_der(
            os.environ["SNOWFLAKE_DEV_SVC_PRIVATE_KEY"],
            os.environ.get("SNOWFLAKE_DEV_SVC_PRIVATE_KEY_PASSPHRASE"),
        ),
        warehouse          = os.environ.get("SNOWFLAKE_WAREHOUSE", "CRYPTO_WH"),
    )


def run_file(cur, path: str) -> None:
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()

    statements = [s.strip() for s in sql.split(";")]
    for statement in statements:
        if not statement:
            continue
        code_lines = [
            line for line in statement.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        preview = code_lines[0][:80] if code_lines else statement.splitlines()[0][:80]
        print(f"  -> {preview}")
        cur.execute(statement)


def main():
    parser = argparse.ArgumentParser(description="Apply Snowflake infra SQL files in order")
    parser.add_argument("files", nargs="+", help="SQL files to run, in order")
    args = parser.parse_args()

    conn = get_connection()
    try:
        cur = conn.cursor()
        for path in args.files:
            print(f"=== Running {path} ===")
            run_file(cur, path)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
```

```
# snowflake/requirements.txt
snowflake-connector-python==3.13.2
cryptography==42.0.5
```

### CI — syntax-check on every pull request

```yaml
# .github/workflows/infra-ci.yml
name: Infra CI

on:
  pull_request:
    branches: [main]
    paths:
      - 'de-pipeline/snowflake/**'

jobs:
  sql-syntax-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install sqlfluff
        run: pip install sqlfluff==3.1.1

      # Parse-only, not lint: this catches real syntax errors without
      # requiring Snowflake credentials in a PR context (safer for forks)
      # and without forcing a specific formatting style on the DDL.
      - name: Validate Snowflake SQL syntax
        run: sqlfluff parse de-pipeline/snowflake --dialect snowflake
```

`.sqlfluff` at the repo root pins the dialect so both this workflow and local runs agree:

```ini
[sqlfluff]
dialect = snowflake
templater = raw
```

### Deploy — apply to Snowflake on merge to main

```yaml
# .github/workflows/infra-deploy.yml
name: Infra Deploy

on:
  push:
    branches: [main]
    paths:
      - 'de-pipeline/snowflake/**'
  workflow_dispatch:

jobs:
  deploy-snowflake-infra:
    name: Apply setup.sql, streams.sql, tasks.sql
    runs-on: ubuntu-latest
    environment: infra   # configure required reviewers: Settings -> Environments -> infra
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r de-pipeline/snowflake/requirements.txt

      - name: Apply Snowflake infra
        working-directory: de-pipeline/snowflake
        run: python run_sql.py setup.sql streams.sql tasks.sql
        env:
          SNOWFLAKE_ACCOUNT:             ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_DEV_SVC_USER:        ${{ secrets.SNOWFLAKE_DEV_SVC_USER }}
          SNOWFLAKE_DEV_SVC_PRIVATE_KEY: ${{ secrets.SNOWFLAKE_DEV_SVC_PRIVATE_KEY }}
```

`environment: infra` gates this the same way `qa`/`prod` are gated in `de-deploy.yml` (Step 5) — add required reviewers under **Settings → Environments → infra** if infra changes should need manual approval before hitting Snowflake.

New secrets needed (see updated Step 6): `SNOWFLAKE_DEV_SVC_USER` and `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` hold the `DEVELOPER_SVC` key-pair credentials from `README.md` — the contents of `.keys/rsa_key.p8`, not the `CRYPTO_PIPELINE_ROLE` password used by `de-deploy.yml`. These are different accounts with different privileges (`SYSADMIN`/`USERADMIN` for infra vs. `CRYPTO_PIPELINE_ROLE` for data) and should stay separate.

---

## Step 4 — dbt models

There is no dbt "bronze" layer anymore — `CRYPTO_DB.STG.COINGECKO_RAW` already *is* the raw landing layer, and dbt reads it directly as a `source()`. dbt only builds Silver and Gold, into whichever database the active target points to.

### `dbt_project.yml`

```yaml
name: crypto_pipeline
version: '1.0.0'
profile: crypto_pipeline

models:
  crypto_pipeline:
    silver:
      +schema: silver_crypto
      +materialized: incremental
    gold:
      +schema: gold_crypto
      +materialized: table
```

### `profiles.yml` — one target per environment

```yaml
# dbt/profiles.yml
crypto_pipeline:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: CRYPTO_PIPELINE_ROLE
      warehouse: CRYPTO_WH
      database: DEV_DB
      schema: PUBLIC
      threads: 4
    qa:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: CRYPTO_PIPELINE_ROLE
      warehouse: CRYPTO_WH
      database: QA_DB
      schema: PUBLIC
      threads: 4
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: CRYPTO_PIPELINE_ROLE
      warehouse: CRYPTO_WH
      database: PROD_DB
      schema: PUBLIC
      threads: 4
```

`database` is the only thing that changes between targets — the schema name (`silver_crypto` / `gold_crypto`) is identical across dev/qa/prod, which is what promotes a model unchanged from one environment to the next.

### Macro — pin schema names across environments

By default dbt prefixes a custom `+schema` with the target's default schema (e.g. `public_silver_crypto`). Since environment separation here comes from the **database**, not a schema prefix, override it:

```sql
-- dbt/macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

### Source — shared landing table

```yaml
# dbt/models/silver/sources.yml
version: 2

sources:
  - name: landing
    database: CRYPTO_DB   # always the shared landing db, regardless of target
    schema: STG
    tables:
      - name: coingecko_raw
```

### Silver — cleaned, typed, deduplicated (incremental)

```sql
-- dbt/models/silver/stg_crypto_prices.sql
{{
  config(
    materialized     = 'incremental',
    unique_key       = ['id', 'fetched_at'],
    on_schema_change = 'sync_all_columns'
  )
}}

WITH source AS (
  SELECT * FROM {{ source('landing', 'coingecko_raw') }}

  {% if is_incremental() %}
    WHERE fetched_at > (SELECT MAX(fetched_at) FROM {{ this }})
  {% endif %}
),

cleaned AS (
  SELECT
    id                                         AS coin_id,
    UPPER(symbol)                              AS symbol,
    name,
    CAST(current_price    AS DECIMAL(20,8))    AS price_usd,
    CAST(market_cap       AS DECIMAL(28,2))    AS market_cap_usd,
    CAST(total_volume     AS DECIMAL(28,2))    AS volume_24h_usd,
    CAST(price_change_24h AS DECIMAL(20,8))    AS price_change_24h,
    ROUND(price_change_pct_24h, 4)             AS price_change_pct_24h,
    CAST(high_24h         AS DECIMAL(20,8))    AS high_24h,
    CAST(low_24h          AS DECIMAL(20,8))    AS low_24h,
    CAST(ath              AS DECIMAL(20,8))    AS all_time_high,
    DATE_TRUNC('hour', fetched_at)             AS fetched_hour,
    CAST(fetched_at AS DATE)                   AS price_date,
    fetched_at,
    _loaded_at
  FROM source
  WHERE current_price > 0
    AND id IS NOT NULL
)

SELECT * FROM cleaned
```

```yaml
# dbt/models/silver/stg_crypto_prices.yml
version: 2
models:
  - name: stg_crypto_prices
    description: Cleaned and typed crypto price records from CoinGecko
    columns:
      - name: coin_id
        tests: [not_null]
      - name: price_usd
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: false
      - name: fetched_at
        tests: [not_null]
      - name: symbol
        tests: [not_null]
```

### Gold mart 1 — daily returns

```sql
-- dbt/models/gold/mart_daily_returns.sql
{{ config(materialized='table') }}

WITH daily AS (
  SELECT
    coin_id,
    symbol,
    name,
    price_date,
    FIRST_VALUE(price_usd) OVER (
      PARTITION BY coin_id, price_date
      ORDER BY fetched_at ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS open_price,
    LAST_VALUE(price_usd) OVER (
      PARTITION BY coin_id, price_date
      ORDER BY fetched_at ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS close_price,
    MAX(high_24h)        AS day_high,
    MIN(low_24h)         AS day_low,
    AVG(volume_24h_usd)  AS avg_volume
  FROM {{ ref('stg_crypto_prices') }}
  GROUP BY coin_id, symbol, name, price_date, price_usd, fetched_at
)

SELECT
  coin_id, symbol, name, price_date,
  open_price, close_price, day_high, day_low,
  ROUND(
    (close_price - open_price) / NULLIF(open_price, 0) * 100, 4
  ) AS daily_return_pct,
  avg_volume
FROM daily
```

### Gold mart 2 — 30-day volatility

```sql
-- dbt/models/gold/mart_volatility_30d.sql
{{ config(materialized='table') }}

SELECT
  coin_id,
  symbol,
  price_date,
  STDDEV(daily_return_pct) OVER (
    PARTITION BY coin_id
    ORDER BY price_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS volatility_30d,
  AVG(daily_return_pct) OVER (
    PARTITION BY coin_id
    ORDER BY price_date
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) AS avg_return_30d
FROM {{ ref('mart_daily_returns') }}
ORDER BY price_date DESC, volatility_30d DESC
```

### Gold mart 3 — top movers

```sql
-- dbt/models/gold/mart_top_movers.sql
{{ config(materialized='table') }}

WITH latest AS (
  SELECT *
  FROM {{ ref('mart_daily_returns') }}
  WHERE price_date = (
    SELECT MAX(price_date) FROM {{ ref('mart_daily_returns') }}
  )
),

ranked AS (
  SELECT *,
    RANK() OVER (ORDER BY daily_return_pct DESC) AS gainer_rank,
    RANK() OVER (ORDER BY daily_return_pct ASC)  AS loser_rank
  FROM latest
)

SELECT
  coin_id, symbol, name, price_date,
  close_price, daily_return_pct,
  CASE
    WHEN gainer_rank <= 10 THEN 'top_gainer'
    WHEN loser_rank  <= 10 THEN 'top_loser'
  END AS mover_type
FROM ranked
WHERE gainer_rank <= 10 OR loser_rank <= 10
ORDER BY daily_return_pct DESC
```

### Snapshot — track market cap changes over time

```sql
-- dbt/snapshots/snap_market_cap_history.sql
{% snapshot snap_market_cap_history %}

{{
  config(
    target_schema = 'silver_crypto',
    unique_key    = 'coin_id',
    strategy      = 'check',
    check_cols    = ['market_cap_usd', 'price_usd'],
  )
}}

SELECT coin_id, symbol, name, price_usd, market_cap_usd, price_date
FROM {{ ref('stg_crypto_prices') }}

{% endsnapshot %}
```

---

## Step 5 — GitHub Actions CI/CD (dbt only)

Promotion between environments is driven purely by the dbt `--target` flag against the **same** models — dev, qa, and prod run identical SQL against different databases. `qa` and `prod` are wired as [GitHub Environments](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment) so you can require manual approval before either promotion runs.

Ingestion is **not** part of this pipeline — it's its own workflow, `de-ingest.yml` (Step 3a), on an independent schedule. `de-ci.yml`/`de-deploy.yml` here cover dbt only, so their path filters watch `dbt/**` alone.

### CI — runs on every pull request (validates against dev_db)

```yaml
# .github/workflows/de-ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]
    paths:
      - 'dbt/**'

jobs:
  dbt-ci:
    runs-on: ubuntu-latest
    env:
      SNOWFLAKE_ACCOUNT:  ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_USER:     ${{ secrets.SNOWFLAKE_USER }}
      SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dbt
        run: pip install dbt-snowflake==1.7.0

      - name: dbt deps
        working-directory: dbt
        run: dbt deps

      - name: dbt build against dev_db (modified models only)
        working-directory: dbt
        run: dbt build --target dev --profiles-dir . --select state:modified+ --defer --state ./target
```

### Deploy — promote dev_db → qa_db → prod_db

Runs daily at **8:30 AM Bangkok time** — `1:30 AM UTC`, staggered 30 minutes after `de-ingest.yml`'s 8:00 AM run so the day's fresh rows have already landed in `CRYPTO_DB.STG` before dbt builds on top of them. It also runs on every merge to `main` (to deploy model changes) and via the manual button. Because the two workflows are separate, this is a loose time-based coupling, not a hard dependency — if ingestion is late or fails, dbt still runs against whatever data is currently in the landing table (incremental models simply pick up less new data that day).

```yaml
# .github/workflows/de-deploy.yml
name: dbt promote

on:
  schedule:
    - cron: '30 1 * * *'   # 1:30 AM UTC = 8:30 AM Bangkok, 30 min after de-ingest.yml
  push:
    branches: [main]       # also triggers on merge to main
  workflow_dispatch:        # manual run button

env:
  SNOWFLAKE_ACCOUNT:  ${{ secrets.SNOWFLAKE_ACCOUNT }}
  SNOWFLAKE_USER:     ${{ secrets.SNOWFLAKE_USER }}
  SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}

jobs:
  deploy-dev:
    name: dbt build -> dev_db
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install dbt-snowflake==1.7.0
      - working-directory: dbt
        run: dbt deps
      - working-directory: dbt
        run: dbt build --target dev --profiles-dir .

  deploy-qa:
    name: dbt build -> qa_db
    runs-on: ubuntu-latest
    needs: deploy-dev
    environment: qa          # configure required reviewers: Settings -> Environments -> qa
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install dbt-snowflake==1.7.0
      - working-directory: dbt
        run: dbt deps
      - working-directory: dbt
        run: dbt build --target qa --profiles-dir .

  deploy-prod:
    name: dbt build -> prod_db
    runs-on: ubuntu-latest
    needs: deploy-qa
    environment: prod         # configure required reviewers: Settings -> Environments -> prod
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install dbt-snowflake==1.7.0
      - working-directory: dbt
        run: dbt deps
      - name: dbt snapshot
        working-directory: dbt
        run: dbt snapshot --target prod --profiles-dir .
      - name: dbt build
        working-directory: dbt
        run: dbt build --target prod --profiles-dir .
      - name: dbt docs generate
        working-directory: dbt
        run: dbt docs generate --target prod --profiles-dir .
```

---

## Step 6 — GitHub Secrets & Environments to configure

**Secrets** — Go to **GitHub repo → Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Used by | Example value |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | all | `xy12345.us-east-1` |
| `SNOWFLAKE_USER` | de-ingest.yml / de-ci.yml / de-deploy.yml | `pipeline_svc` (has `CRYPTO_PIPELINE_ROLE`) |
| `SNOWFLAKE_PASSWORD` | de-ingest.yml / de-ci.yml / de-deploy.yml | your service account password |
| `SNOWFLAKE_DEV_SVC_USER` | infra-deploy.yml | `DEVELOPER_SVC` |
| `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` | infra-deploy.yml | full contents of `.keys/rsa_key.p8` |

`SNOWFLAKE_DEV_SVC_USER` / `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` are the `DEVELOPER_SVC` key-pair account from `README.md` (`SYSADMIN` + `USERADMIN`) — deliberately separate from `SNOWFLAKE_USER`/`SNOWFLAKE_PASSWORD`, which only has `CRYPTO_PIPELINE_ROLE`'s narrower data-plane grants and is what `de-ingest.yml` (ingestion) and the dbt workflows all authenticate with (`config.py` and dbt's `profiles.yml` read the same three env vars).

**Environments** — Go to **GitHub repo → Settings → Environments** and create `infra`, `qa`, and `prod`. Add required reviewers on each to gate, respectively: applying infra changes to Snowflake, promotion out of dev, and promotion out of qa.

---

## Pipeline execution flow

Ingestion and dbt promotion are two independent workflows on staggered daily schedules:

```
08:00 Bangkok / 01:00 UTC daily (cron) OR manual dispatch — de-ingest.yml (Step 3a)
│
└── Job: ingest
      └── ingest_coingecko.py runs (INGEST_MODE=snowflake)
            └── 50 rows → CRYPTO_DB.STG.COINGECKO_RAW
                  └── STREAM_COINGECKO_RAW captures new rows
                        └── TASK_NOTIFY_NEW_DATA fires (signal only)

08:30 Bangkok / 01:30 UTC daily (cron) OR push to main OR manual dispatch — de-deploy.yml (Step 5)
│   (staggered 30 min after ingestion so the day's rows have landed)
│
├── Job 1: deploy-dev
│     └── dbt build --target dev
│           ├── DEV_DB.SILVER_CRYPTO.STG_CRYPTO_PRICES (incremental merge)
│           └── DEV_DB.GOLD_CRYPTO.MART_* (daily returns, volatility, top movers)
│
├── Job 2: deploy-qa (after deploy-dev, gated by "qa" environment approval)
│     └── dbt build --target qa
│           ├── QA_DB.SILVER_CRYPTO.STG_CRYPTO_PRICES
│           └── QA_DB.GOLD_CRYPTO.MART_*
│
└── Job 3: deploy-prod (after deploy-qa, gated by "prod" environment approval)
      ├── dbt snapshot  → PROD_DB.SILVER_CRYPTO.SNAP_MARKET_CAP_HISTORY
      ├── dbt build     → PROD_DB.SILVER_CRYPTO.STG_CRYPTO_PRICES
      │                 → PROD_DB.GOLD_CRYPTO.MART_*
      └── dbt test      → all schema tests must pass or job fails
```

Merges to `main` touching `de-pipeline/snowflake/**` also trigger `infra-deploy.yml` (Deployment Step-1) independently, applying `setup.sql`/`streams.sql`/`tasks.sql`. `ingest_task.sql` (Step 3b) is excluded — it's disabled and has no effect until re-enabled.

---

## Key design decisions

**Shared landing database, per-environment transform databases.** `CRYPTO_DB.STG` is the single source of truth for raw data — every environment's Silver/Gold layer reads from the exact same rows. There is no per-environment copy of raw data to drift out of sync.

**dbt `--target` is the promotion mechanism.** dev/qa/prod are dbt targets pointing at `DEV_DB`/`QA_DB`/`PROD_DB` respectively, running the *same* SQL. A model is "promoted" simply by re-running the identical build against the next target — not by copying data or maintaining environment-specific SQL branches.

**Custom `generate_schema_name` macro.** Without it, dbt would prefix `silver_crypto`/`gold_crypto` with the target's default schema (e.g. `public_silver_crypto`). The override makes the schema name identical across all three databases, so environment separation comes entirely from `database`, keeping profiles.yml as the only thing that changes per environment.

**Incremental models over full refresh.** Silver uses `unique_key = ['id', 'fetched_at']` so dbt only merges new rows, not reprocessing full history, in whichever database it targets.

**Append-only landing table.** Raw data in `CRYPTO_DB.STG.COINGECKO_RAW` is never modified — every run appends rows with `fetched_at`. This gives a complete audit trail and lets you rebuild Silver/Gold in any environment from any past point in time.

**Production ingestion runs via GitHub Actions, not a Snowflake Task — for now.** A Snowflake-native Python Task (Step 3b, `ingest_task.sql`) was built and is kept in the repo, but it needs an External Access Integration, which requires the `CREATE INTEGRATION` privilege — unavailable on the Snowflake trial account this project runs on. Rather than block on that, ingestion reverted to `ingest_coingecko.py`. `ingest_task.sql`'s SQL is fully commented out and excluded from `infra-deploy.yml`'s file list, so it has zero effect until the account is upgraded and someone deliberately re-enables it (instructions are in Step 3b's comment header).

**Ingestion is a standalone workflow, decoupled from dbt promotion.** `de-ingest.yml` (Step 3a) runs `ingest_coingecko.py` on its own daily cron; `de-deploy.yml` (Step 5) runs dbt on a separate cron staggered 30 minutes later. Splitting them — rather than making ingestion a first job inside `de-deploy.yml` — means each can be retried, rerun, and read in isolation, and a flaky data pull doesn't show up as a "deploy" failure. The tradeoff is there's no hard `needs:` ordering across the two workflows (GitHub Actions can't express that cleanly); the 30-minute stagger is a deliberately loose coupling, and because Silver is incremental, a missed or late ingestion just means that day's dbt run merges fewer new rows rather than breaking.

**Snowflake Stream as a zero-cost trigger.** `APPEND_ONLY = TRUE` on the stream means Snowflake only tracks inserts, the cheapest change-tracking mode. `SYSTEM$STREAM_HAS_DATA` prevents the Task from consuming credits when there's nothing new — it exists purely as a monitoring signal, since the real Silver/Gold builds run through dbt.

**GitHub Environments gate qa/prod promotion.** `deploy-qa` and `deploy-prod` are tied to GitHub `environment: qa` / `environment: prod`, so promotion out of dev (and out of qa) can require manual reviewer approval without adding any custom logic to the workflow.

**`state:modified+` in CI.** The CI workflow only builds models that changed in the PR plus their downstream dependencies, against `dev_db` — making CI runs fast, cheap, and safe to iterate on without touching qa/prod.
