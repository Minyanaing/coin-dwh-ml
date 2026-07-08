# Crypto Market Pipeline — Full Project Plan

**Stack:** CoinGecko API · Snowflake · dbt Core · GitHub Actions · Snowflake Streams & Tasks

---

## Project overview

A production-grade daily crypto market data pipeline that ingests OHLC prices and market cap data for the top 50 tokens from CoinGecko, lands them in a shared Snowflake landing database via a scheduled Snowflake Python Task, and promotes them through dbt-built Silver/Gold layers across three isolated environment databases (dev → qa → prod) — with CI/CD and environment promotion gates enforced through GitHub Actions.

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
│       ├── infra-deploy.yml         # merge to main: apply setup/streams/tasks/ingest_task to Snowflake
│       ├── de-ci.yml                # PR: dbt build + test against dev_db
│       └── de-deploy.yml            # promote dev_db -> qa_db -> prod_db (no ingestion here)
└── de-pipeline/
    ├── ingestion/
    │   ├── ingest_coingecko.py      # LOCAL TESTING ONLY — not scheduled, not deployed
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
    │   ├── ingest_task.sql          # PRODUCTION ingestion: network rule + integration + Python proc + Task
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

Ingestion has two separate implementations that intentionally do **not** share deployment infrastructure:

- **3a. `ingestion/ingest_coingecko.py`** — a local Python script, kept purely as a **local testing / reference tool**. It is never scheduled and never deployed; nothing in CI/CD invokes it. Use it to eyeball CoinGecko's response shape or sanity-check a write against a dev schema by hand.
- **3b. A Snowflake Python Task** (`snowflake/ingest_task.sql`) — this is what actually runs in production, on a schedule, entirely inside Snowflake. There's no external host, no separate secret to rotate for the CoinGecko call, and no GitHub Actions job to babysit — the schedule, the code, and the data all live in one system.

### 3a. Local testing script (reference only — not used for deployment)

`config.py` / `INGEST_MODE` supports two modes, both purely for local use:

- **`local`** (default) — fetches from CoinGecko and writes a CSV to `ingestion/data/`. No Snowflake credentials required. Use this to test the script and eyeball the data before it ever touches Snowflake.
- **`snowflake`** — same fetch, but loads rows into the shared landing table `CRYPTO_DB.STG.COINGECKO_RAW` instead of writing a CSV. Useful for manually verifying the write path against a dev schema — **not** how production data gets loaded (see 3b).

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

### 3b. Production ingestion — Snowflake Python Task

This is what actually runs on a schedule. A Snowflake Task calls a Python stored procedure (Snowpark) that fetches CoinGecko and appends straight into `CRYPTO_DB.STG.COINGECKO_RAW` — no external script, no separate credentials for the CoinGecko call, no GitHub Actions job. The task authenticates as whatever role owns it (no password/key to manage for ingestion itself), and reaching `api.coingecko.com` from inside Snowflake requires an **External Access Integration**.

**Two things here require `ACCOUNTADMIN` and are deliberately kept manual, documented in `README.md` — not granted to `SYSADMIN` and not automated in `infra-deploy.yml`:**

1. **`EXECUTE TASK`** — an account-level privilege only `ACCOUNTADMIN` holds by default. Without it, `ALTER TASK ... RESUME` fails for *any* task `SYSADMIN` owns, including `TASK_NOTIFY_NEW_DATA` from Step 2. This one **is** granted to `SYSADMIN` once (folded into README's Step 1.2, alongside the `SYSADMIN`/`USERADMIN` grants) since ongoing automation depends on it.
2. **`CREATE EXTERNAL ACCESS INTEGRATION`** — also account-level, requiring the global `CREATE INTEGRATION` privilege. Unlike `EXECUTE TASK`, this is **not** granted to `SYSADMIN` — it's a rarely-changed, high-privilege capability (arbitrary network egress) better left as an audited manual step than something CI can invoke. `ACCOUNTADMIN` creates the network rule + integration once by hand (README Step 1.4); `SYSADMIN` only needs `USAGE` on the already-created integration to reference it when creating the procedure.

```sql
-- Run once, manually, as ACCOUNTADMIN — after snowflake/setup.sql has
-- already created CRYPTO_DB.STG (see README.md "1.5 One-time ACCOUNTADMIN
-- setup for production ingestion")

-- Egress allowlist: only CoinGecko's public REST API, nothing else
CREATE NETWORK RULE IF NOT EXISTS CRYPTO_DB.STG.COINGECKO_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.coingecko.com');

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS COINGECKO_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (CRYPTO_DB.STG.COINGECKO_NETWORK_RULE)
  ENABLED = TRUE;

-- SYSADMIN creates and owns the procedure/task below, and needs USAGE on
-- the integration to reference it in CREATE PROCEDURE.
GRANT USAGE ON INTEGRATION COINGECKO_ACCESS_INTEGRATION TO ROLE SYSADMIN;
```

Everything below this point is ordinary `SYSADMIN` work — once the manual block above has run, `ingest_task.sql` is exactly as idempotent as `setup.sql`/`streams.sql`/`tasks.sql` and applies cleanly through the normal `infra-deploy.yml` pipeline, with no further `ACCOUNTADMIN` involvement:

```sql
-- snowflake/ingest_task.sql
-- Requires the manual ACCOUNTADMIN block above (network rule, integration,
-- USAGE grant) plus the EXECUTE TASK grant from README Step 1.2 to already
-- exist — otherwise CREATE PROCEDURE fails (integration not found) and
-- ALTER TASK ... RESUME fails (missing EXECUTE TASK).

USE ROLE SYSADMIN;

-- Fetches CoinGecko's top-50 markets and appends to the landing table.
-- Runs inside Snowflake's Python runtime (Snowpark) — same shape as
-- ingest_coingecko.py's fetch_coins()/build_rows(), reimplemented as a
-- stored procedure since it can't import the local ingestion/ package.
CREATE OR REPLACE PROCEDURE CRYPTO_DB.STG.INGEST_COINGECKO()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.11'
  PACKAGES = ('snowflake-snowpark-python', 'requests')
  HANDLER = 'run'
  EXTERNAL_ACCESS_INTEGRATIONS = (COINGECKO_ACCESS_INTEGRATION)
AS
$$
import requests
from datetime import datetime, timezone

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 50,
    "page": 1,
    "sparkline": False,
}

COLUMNS = [
    "id", "symbol", "name", "current_price", "market_cap", "total_volume",
    "price_change_24h", "price_change_pct_24h", "high_24h", "low_24h",
    "circulating_supply", "ath", "fetched_at",
]

def run(session):
    resp = requests.get(COINGECKO_URL, params=PARAMS, timeout=30)
    resp.raise_for_status()
    coins = resp.json()
    fetched_at = datetime.now(timezone.utc).isoformat()

    rows = [
        (
            c["id"], c["symbol"], c["name"],
            c.get("current_price"), c.get("market_cap"), c.get("total_volume"),
            c.get("price_change_24h"), c.get("price_change_percentage_24h"),
            c.get("high_24h"), c.get("low_24h"),
            c.get("circulating_supply"), c.get("ath"), fetched_at,
        )
        for c in coins
    ]

    session.create_dataframe(rows, schema=COLUMNS) \
        .write.save_as_table("CRYPTO_DB.STG.COINGECKO_RAW", mode="append")

    return f"Loaded {len(rows)} rows at {fetched_at}"
$$;

-- Runs the procedure hourly on the same warehouse as everything else.
CREATE TASK IF NOT EXISTS CRYPTO_DB.STG.TASK_INGEST_COINGECKO
  WAREHOUSE = CRYPTO_WH
  SCHEDULE  = '60 MINUTE'
AS
  CALL CRYPTO_DB.STG.INGEST_COINGECKO();

ALTER TASK CRYPTO_DB.STG.TASK_INGEST_COINGECKO RESUME;
```

Rows this task appends still flow through the existing `STREAM_COINGECKO_RAW` / `TASK_NOTIFY_NEW_DATA` from Step 2 unchanged — that stream tracks inserts on the table regardless of what inserted them, so nothing there needs to change.

---

## Deployment Step-1 — CI/CD for the Snowflake infra scripts (Steps 1–3b)

`infra-ci.yml` / `infra-deploy.yml` deploy **only** `snowflake/setup.sql`, `streams.sql`, `tasks.sql`, and `ingest_task.sql` — everything created in Steps 1, 2, and 3b, including production ingestion now that it's a Snowflake Task rather than an externally-scheduled script. This is separate from `de-ci.yml` / `de-deploy.yml` (Step 5), which now handle **dbt only**. Splitting them means an infra/ingestion change (e.g. a new schema, or editing the ingestion procedure) and a dbt change (e.g. a new mart) trigger independent, correctly-scoped pipelines.

- **CI does not touch real Snowflake.** It runs `sqlfluff parse` against the SQL — a genuine syntax check (catches malformed DDL) without needing credentials in a PR context, which matters once forks can open PRs.
- **Deploy actually applies the SQL to Snowflake**, using the `DEVELOPER_SVC` key-pair service account from `README.md` (`SYSADMIN` + `USERADMIN`) via a small connector script, since GitHub's `ubuntu-latest` runners don't ship SnowSQL.
- All four scripts are safe to re-run: `setup.sql`/`ingest_task.sql` use `CREATE ... IF NOT EXISTS` throughout, and `streams.sql` was changed from `CREATE OR REPLACE STREAM` to `CREATE STREAM IF NOT EXISTS` specifically so repeat deploys don't reset the stream's tracked offset.
- `ingest_task.sql` additionally depends on two one-time `ACCOUNTADMIN` actions documented in `README.md`, neither of which `infra-deploy.yml` ever performs itself: the `EXECUTE TASK` grant (README 1.2, needed for *any* task `SYSADMIN` owns) and the network rule + External Access Integration + `USAGE` grant (README 1.5). Run those manually first, then `infra-deploy.yml` handles `ingest_task.sql` like any other file.

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
    name: Apply setup.sql, streams.sql, tasks.sql, ingest_task.sql
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
        run: python run_sql.py setup.sql streams.sql tasks.sql ingest_task.sql
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

`de-ci.yml`/`de-deploy.yml` no longer touch ingestion at all — production ingestion is the Snowflake Task from Step 3b, deployed by `infra-deploy.yml` (Deployment Step-1), not by this pipeline. `ingestion/**` was dropped from the path filter below for the same reason: changes to the local-testing script don't need to trigger a dbt build.

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

Ingestion is no longer a job in this workflow — `CRYPTO_DB.STG.COINGECKO_RAW` is populated independently by the Snowflake Task from Step 3b on its own hourly schedule. This workflow only needs to run dbt often enough to pick up whatever's landed since the last run; the daily cron below is unrelated to (and no longer waits on) the ingestion cadence.

```yaml
# .github/workflows/de-deploy.yml
name: Daily pipeline

on:
  schedule:
    - cron: '0 6 * * *'   # 6:00 AM UTC every day
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
| `SNOWFLAKE_ACCOUNT` | both | `xy12345.us-east-1` |
| `SNOWFLAKE_USER` | de-ci.yml / de-deploy.yml (dbt) | `pipeline_svc` (has `CRYPTO_PIPELINE_ROLE`) |
| `SNOWFLAKE_PASSWORD` | de-ci.yml / de-deploy.yml (dbt) | your service account password |
| `SNOWFLAKE_DEV_SVC_USER` | infra-deploy.yml | `DEVELOPER_SVC` |
| `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` | infra-deploy.yml | full contents of `.keys/rsa_key.p8` |

Production ingestion (Step 3b) needs none of these — it runs as a Snowflake Task authenticating with whatever role owns it, not via GitHub Actions.

`SNOWFLAKE_DEV_SVC_USER` / `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` are the `DEVELOPER_SVC` key-pair account from `README.md` (`SYSADMIN` + `USERADMIN`) — deliberately separate from `SNOWFLAKE_USER`/`SNOWFLAKE_PASSWORD`, which only has `CRYPTO_PIPELINE_ROLE`'s narrower data-plane grants.

**Environments** — Go to **GitHub repo → Settings → Environments** and create `infra`, `qa`, and `prod`. Add required reviewers on each to gate, respectively: applying infra changes to Snowflake, promotion out of dev, and promotion out of qa.

---

## Pipeline execution flow

Two independent schedules now, deliberately decoupled — ingestion no longer gates dbt:

```
Inside Snowflake, every 60 minutes (TASK_INGEST_COINGECKO, Step 3b)
│
└── CALL CRYPTO_DB.STG.INGEST_COINGECKO()
      └── 50 rows → CRYPTO_DB.STG.COINGECKO_RAW
            └── STREAM_COINGECKO_RAW captures new rows
                  └── TASK_NOTIFY_NEW_DATA fires (signal only)

06:00 UTC daily (cron) OR push to main OR manual dispatch — de-deploy.yml (Step 5)
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

Merges to `main` touching `de-pipeline/snowflake/**` also trigger `infra-deploy.yml` (Deployment Step-1) independently, applying `setup.sql`/`streams.sql`/`tasks.sql`/`ingest_task.sql` — including any edit to the ingestion procedure or its schedule.

---

## Key design decisions

**Shared landing database, per-environment transform databases.** `CRYPTO_DB.STG` is the single source of truth for raw data — every environment's Silver/Gold layer reads from the exact same rows. There is no per-environment copy of raw data to drift out of sync.

**dbt `--target` is the promotion mechanism.** dev/qa/prod are dbt targets pointing at `DEV_DB`/`QA_DB`/`PROD_DB` respectively, running the *same* SQL. A model is "promoted" simply by re-running the identical build against the next target — not by copying data or maintaining environment-specific SQL branches.

**Custom `generate_schema_name` macro.** Without it, dbt would prefix `silver_crypto`/`gold_crypto` with the target's default schema (e.g. `public_silver_crypto`). The override makes the schema name identical across all three databases, so environment separation comes entirely from `database`, keeping profiles.yml as the only thing that changes per environment.

**Incremental models over full refresh.** Silver uses `unique_key = ['id', 'fetched_at']` so dbt only merges new rows, not reprocessing full history, in whichever database it targets.

**Append-only landing table.** Raw data in `CRYPTO_DB.STG.COINGECKO_RAW` is never modified — every run appends rows with `fetched_at`. This gives a complete audit trail and lets you rebuild Silver/Gold in any environment from any past point in time.

**Production ingestion is a Snowflake Task, not an external script.** `ingest_coingecko.py` (Step 3a) stays purely as a local reference/testing tool — nothing schedules or deploys it. Production ingestion (Step 3b) is a Python stored procedure on a Snowflake Task instead: one fewer system to host and monitor, no external secret to rotate for the CoinGecko call (the task runs as whichever role owns it), and the schedule lives next to the data it populates. The tradeoff is Snowflake-side setup: an External Access Integration for network egress, Python packages limited to what's on the Snowflake Anaconda channel, and two manual, one-time `ACCOUNTADMIN` steps (README 1.2/1.4) that are deliberately kept out of `infra-deploy.yml` rather than granted to `SYSADMIN` — see the `ACCOUNTADMIN` callouts in Step 3b.

**Ingestion and dbt promotion are decoupled schedules.** The Snowflake Task runs hourly regardless of whether `de-deploy.yml` has run; `de-deploy.yml` runs on its own daily cron and just picks up whatever has landed since its last run. Neither waits on the other — there's no `needs: ingest` anymore because ingestion isn't a GitHub Actions job at all.

**Snowflake Stream as a zero-cost trigger.** `APPEND_ONLY = TRUE` on the stream means Snowflake only tracks inserts, the cheapest change-tracking mode. `SYSTEM$STREAM_HAS_DATA` prevents the Task from consuming credits when there's nothing new — it exists purely as a monitoring signal, since the real Silver/Gold builds run through dbt.

**GitHub Environments gate qa/prod promotion.** `deploy-qa` and `deploy-prod` are tied to GitHub `environment: qa` / `environment: prod`, so promotion out of dev (and out of qa) can require manual reviewer approval without adding any custom logic to the workflow.

**`state:modified+` in CI.** The CI workflow only builds models that changed in the PR plus their downstream dependencies, against `dev_db` — making CI runs fast, cheap, and safe to iterate on without touching qa/prod.

**`needs:` chain in deploy.yml.** `deploy-qa` waits on `deploy-dev`, `deploy-prod` waits on `deploy-qa`. If a dbt build or test fails at any stage, nothing downstream is promoted on broken data.
