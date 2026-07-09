# Crypto Market Pipeline — Full Project Plan

**Stack:** CoinGecko API · Snowflake · dbt Core · GitHub Actions · Snowflake Streams & Tasks

> This document is the **plan and context** — what each piece does and why. It intentionally avoids line-by-line code for the ingestion/infra layers (Steps 1–3); the implementation lives in the referenced files under `de-pipeline/` and `.github/workflows/`. Steps 4+ still include representative dbt code.

---

## Project overview

A daily crypto market-data pipeline for a **curated set of coins** (top market-cap coins + major stablecoins). Each day a scheduled GitHub Actions job pulls the latest snapshot from CoinGecko into a shared Snowflake landing database; dbt then builds Silver/Gold marts and promotes them across three isolated environment databases (dev → qa → prod). A separate, manual workflow backfills daily history for the same coins. CI/CD and environment promotion gates are enforced through GitHub Actions.

> A Snowflake-native Python Task alternative for ingestion exists in the codebase (`snowflake/ingest_task.sql`, Step 3b) but is **disabled** — it needs an External Access Integration, unavailable on the Snowflake trial account this was built against. It's kept commented out for later. Production ingestion runs the GitHub Actions way (Step 3a).

---

### Database / schema architecture

The pipeline is split across **four databases**, separating the shared landing zone from environment-specific transformed data:

| Database | Purpose | Schemas |
|---|---|---|
| `CRYPTO_DB` | Landing zone — raw CoinGecko data, shared across all environments | `STG` |
| `DEV_DB` | dbt development target | `SILVER_CRYPTO`, `GOLD_CRYPTO` |
| `QA_DB` | dbt QA target — validated before prod promotion | `SILVER_CRYPTO`, `GOLD_CRYPTO` |
| `PROD_DB` | dbt production target | `SILVER_CRYPTO`, `GOLD_CRYPTO` |

Raw ingestion always lands in `CRYPTO_DB.STG` — there is only ever one copy of raw data. dbt then builds Silver/Gold into whichever database the active `--target` (`dev` / `qa` / `prod`) points to, reading from the same shared source every time. A single warehouse (`CRYPTO_WH`) is shared across all four databases.

### Snowflake accounts & auth

Two Snowflake identities, kept deliberately separate:

- **`DEVELOPER_SVC`** — the infra/CI service account (`TYPE = SERVICE`, key-pair auth only). Holds `SYSADMIN` + `USERADMIN`. Used by `infra-deploy.yml` to apply the SQL scripts, and reused by the ingestion workflows (running as `SYSADMIN`, which owns `CRYPTO_DB.STG`). Setup is documented in `de-pipeline/README.md`.
- **`CRYPTO_PIPELINE_ROLE`** — the narrower data-plane role dbt uses (`profiles.yml`). Password auth in CI.

Full account-creation steps (key-pair generation, grants, one-time `ACCOUNTADMIN` grants) live in `de-pipeline/README.md`.

---

## Folder structure

```
coin-dwh-ml/
├── .sqlfluff                        # dialect=snowflake, used by infra-ci.yml
├── .github/
│   └── workflows/
│       ├── infra-ci.yml             # PR: sqlfluff syntax check on snowflake/**
│       ├── infra-deploy.yml         # merge to main: apply setup/streams/tasks to Snowflake (key-pair)
│       ├── de-ingest.yml            # cron (daily): run ingest_coingecko.py -> CRYPTO_DB.STG (standalone)
│       ├── de-ingest-history.yml    # MANUAL only: backfill daily history -> COINGECKO_HISTORY_RAW
│       ├── de-ci.yml                # PR: dbt build + test against dev_db
│       └── de-deploy.yml            # promote dev_db -> qa_db -> prod_db (dbt only)
└── de-pipeline/
    ├── ingestion/
    │   ├── config.py                     # env-based config (modes, coin set, creds) — shared
    │   ├── fetch_data.py                 # CoinGecko API access (fetch_coins, fetch_history) — shared
    │   ├── snowflake_connection.py       # get_snowflake_conn (key-pair OR password) — shared
    │   ├── transforms.py                 # round5() and other small shared helpers
    │   ├── ingest_coingecko.py           # daily snapshot (production + local; --format csv|json)
    │   ├── backfill_coingecko_history.py # manual historical daily backfill
    │   ├── requirements.txt
    │   ├── .env.example                  # copy to .env (gitignored) for local runs
    │   └── data/                         # gitignored — local CSV/JSON output
    ├── dbt/                              # see Steps 4–5
    ├── snowflake/
    │   ├── setup.sql                # warehouse, 4 databases, schemas, landing tables, role + grants
    │   ├── streams.sql              # append-only Stream on COINGECKO_RAW
    │   ├── tasks.sql                # "new data" signal Task
    │   ├── ingest_task.sql          # DISABLED — Snowflake Task ingestion (needs External Access Integration)
    │   ├── run_sql.py               # applies *.sql via key-pair auth (used by infra-deploy.yml)
    │   └── requirements.txt         # snowflake-connector-python, cryptography
    ├── .keys/                       # gitignored — local rsa_key.p8 / rsa_key.pub
    └── README.md                    # service-account setup + local run instructions
```

---

## Step 1 — Snowflake setup

**File:** `snowflake/setup.sql` — the single, idempotent (`CREATE ... IF NOT EXISTS`) script that stands up all Snowflake objects. Applied by `infra-deploy.yml` (Deployment Step-1), or runnable by hand.

It creates:

- **Warehouse** `CRYPTO_WH` (X-Small, auto-suspend/resume) — shared by everything.
- **Four databases**: `CRYPTO_DB` (schema `STG`), and `DEV_DB` / `QA_DB` / `PROD_DB` (each with `SILVER_CRYPTO` + `GOLD_CRYPTO`).
- **Two landing tables** in `CRYPTO_DB.STG`, both append-only with `fetched_at` / `_loaded_at` audit columns:
  - `COINGECKO_RAW` — daily snapshot (current price, market cap, volume, 24h changes, ATH, etc.).
  - `COINGECKO_HISTORY_RAW` — one row per coin per day (`price_date`, `price`, `market_cap`, `total_volume`) for the manual backfill.
- **Role** `CRYPTO_PIPELINE_ROLE` + grants across all four databases, including `ON FUTURE TABLES/SCHEMAS/VIEWS` so dbt-created objects are auto-covered.

**Role model inside the script:** `SYSADMIN` owns warehouses/databases/schemas/tables; `USERADMIN` owns role creation. The script switches roles itself (`USE ROLE ...`), because `SYSADMIN` alone cannot run `CREATE ROLE`.

**One-time `ACCOUNTADMIN` prerequisites** (documented in `README.md`, run once by hand — *not* automated):

- `GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN` — without it, `ALTER TASK ... RESUME` fails for any task `SYSADMIN` owns (Step 2).
- `GRANT MANAGE GRANTS ON ACCOUNT TO ROLE SYSADMIN` — without it, the `GRANT ... ON FUTURE ...` statements fail even though `SYSADMIN` owns the databases.

---

## Step 2 — Snowflake Stream & Task

A single Stream/Task pair on the shared landing table — it only **signals** that new raw data arrived. It does not chain Bronze→Silver→Gold; the real Silver/Gold builds run through dbt (Step 5).

- **`streams.sql`** — `STREAM_COINGECKO_RAW`, an `APPEND_ONLY` stream on `COINGECKO_RAW`. Created with `IF NOT EXISTS` (not `OR REPLACE`) on purpose: re-running on every infra deploy must **not** reset the stream's tracked offset and re-surface already-consumed rows.
- **`tasks.sql`** — `TASK_NOTIFY_NEW_DATA`, polls hourly and fires only when `SYSTEM$STREAM_HAS_DATA` is true. It's a lightweight, near-zero-cost signal/monitoring hook, not a transform.

---

## Step 3 — Ingestion

Ingestion is **its own concern**, separate from dbt. There are two implementations:

- **3a. Python + GitHub Actions** (`ingestion/*.py`, `de-ingest.yml`) — **what runs in production.** Same code runs locally for testing.
- **3b. Snowflake Python Task** (`snowflake/ingest_task.sql`) — **disabled** (needs an External Access Integration, not available on trial). Kept commented out.

A separate **manual** workflow backfills history (`backfill_coingecko_history.py`, `de-ingest-history.yml`).

### 3a. Python ingestion (production + local)

**Modular layout** — shared concerns are factored into small modules so the daily snapshot and the backfill reuse them:

| File | Responsibility |
|---|---|
| `config.py` | All env-based config: run mode, coin set, CoinGecko key, Snowflake creds. |
| `fetch_data.py` | CoinGecko API access — `fetch_coins()` (markets snapshot) and `fetch_history()` (daily series), with host/auth selection + 429 backoff. |
| `snowflake_connection.py` | `get_snowflake_conn()` — key-pair **or** password auth (auto-selected). |
| `transforms.py` | `round5()` — round numeric values to 5 decimals (None-safe). |
| `ingest_coingecko.py` | Daily snapshot: fetch → build rows → write. Entry point for `de-ingest.yml`. |
| `backfill_coingecko_history.py` | Manual historical daily backfill. |

**Configuration** (`config.py`, all overridable via env / `.env`):

| Setting | Default | Purpose |
|---|---|---|
| `INGEST_MODE` | `local` | `local` = write a file; `snowflake` = load the landing table. |
| `CSV_OUTPUT_DIR` | `data` | Local output directory. |
| `COIN_IDS` | curated list | The coins to fetch (see below). |
| `HISTORY_START_DATE` | `2026-01-01` | Backfill start date; end is always the run date. |
| `COINGECKO_API_KEY` / `COINGECKO_API_PLAN` | unset / `demo` | Optional CoinGecko key; `pro` unlocks history older than 365 days. |
| `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_USER` | — | Connection identity. |
| `SNOWFLAKE_PASSWORD` **or** `SNOWFLAKE_PRIVATE_KEY` / `SNOWFLAKE_PRIVATE_KEY_PATH` (+ passphrase) | — | Password **or** key-pair auth. |
| `SNOWFLAKE_ROLE` / `_WAREHOUSE` / `_DATABASE` / `_SCHEMA` | `CRYPTO_PIPELINE_ROLE` / `CRYPTO_WH` / `CRYPTO_DB` / `STG` | Session context. |

**Curated coin set** — top market-cap coins plus the major stablecoins, kept small and fixed so runs are deterministic and cheap on the free tier (the backfill makes one API call per coin):

`bitcoin, ethereum, binancecoin, solana, ripple, dogecoin, cardano, tron, avalanche-2, chainlink` + stablecoins `tether, usd-coin, dai`.

**Output modes** (`ingest_coingecko.py`):

- `INGEST_MODE=local` (default) → writes to `ingestion/data/`:
  - `--format csv` (default) — the trimmed, rounded column set (matches the `COINGECKO_RAW` table).
  - `--format json` — the **full raw** CoinGecko response, every field, no trimming/rounding. Local only.
- `INGEST_MODE=snowflake` → loads the trimmed/rounded rows into `CRYPTO_DB.STG.COINGECKO_RAW`. The `--format` flag is ignored.

Numeric values are rounded to 5 decimals (`transforms.round5`) for the CSV/Snowflake paths.

**Auth:** `snowflake_connection.get_snowflake_conn()` uses key-pair auth when a private key is configured, else password. In CI the ingestion workflows reuse the `DEVELOPER_SVC` key pair and run as `SYSADMIN` (which owns `CRYPTO_DB.STG`). Locally, either put a password in `.env` or point `SNOWFLAKE_PRIVATE_KEY_PATH` at `.keys/rsa_key.p8`.

**Local testing** (details + Windows-CMD/Linux commands in `README.md`): create a venv, `pip install -r ingestion/requirements.txt`, run in `local` mode first to eyeball the CSV/JSON, then flip to `snowflake` mode to verify the write path before trusting the scheduled job.

**Scheduled workflow — `de-ingest.yml`:** standalone, daily cron (currently `0 1 * * *` = 08:00 Bangkok / 01:00 UTC; GitHub cron is always UTC) plus a manual **Run workflow** button. Runs `ingest_coingecko.py` with `INGEST_MODE=snowflake` and the `DEVELOPER_SVC` key-pair secrets. No `push:` trigger — a code change shouldn't fire a real data load. Kept separate from dbt promotion so it can be retried/read in isolation.

> Note: GitHub only runs a scheduled workflow from the **default branch**, and never backfills a missed slot. Top-of-hour crons (`minute 0`) are the most likely to be delayed/dropped under load — a non-round minute is more reliable.

### Historical backfill (manual)

**Files:** `backfill_coingecko_history.py` + `de-ingest-history.yml` (**`workflow_dispatch` only — no schedule**).

Each run fetches a daily price/market-cap/volume series for every coin in the curated set from `HISTORY_START_DATE` through the run date, and appends to `CRYPTO_DB.STG.COINGECKO_HISTORY_RAW` (or a CSV locally). It reuses the same `fetch_data` / `snowflake_connection` / `transforms` modules and the same `DEVELOPER_SVC` key-pair auth.

**CoinGecko 365-day limit:** the keyless/Demo API only serves the past 365 days of `market_chart` history. The default start (`2026-01-01`) stays inside that window so it works keyless. Going further back requires a **paid** plan — set `COINGECKO_API_KEY` + `COINGECKO_API_PLAN=pro`; otherwise the script exits early with a clear message. The workflow exposes a `history_start_date` input for one-off ranges.

### 3b. Snowflake Python Task ingestion (disabled — kept for later)

`snowflake/ingest_task.sql` would fetch CoinGecko from inside Snowflake (a Snowpark stored proc on a Task). It needs an **External Access Integration** (requires the account-level `CREATE INTEGRATION` privilege), which the trial account can't create — so its SQL is **fully commented out** and it's **excluded** from `infra-deploy.yml`'s applied-file list, giving it zero effect on the running pipeline.

The file's header documents how to re-enable it later (create the network rule + integration + `USAGE` grant as `ACCOUNTADMIN`, uncomment, add it to `run_sql.py`'s file list, and retire `de-ingest.yml` so the two don't double-insert). If re-enabled, its inserts still flow through the Step 2 stream unchanged.

---

## Deployment Step-1 — CI/CD for the Snowflake infra scripts (Steps 1–2)

Two workflows deploy **only** the infra SQL (`setup.sql`, `streams.sql`, `tasks.sql`), separate from the dbt pipeline (Step 5) and from ingestion (Step 3a) — so an infra change, a dbt change, and an ingestion change each trigger their own correctly-scoped pipeline. `ingest_task.sql` (Step 3b) is excluded (disabled).

- **`infra-ci.yml`** — on PRs touching `de-pipeline/snowflake/**`, runs `sqlfluff parse` (parse-only, not lint) against the SQL. A real syntax check that needs **no** Snowflake credentials, so it's safe for fork PRs and doesn't impose a formatting style. Dialect is pinned by the repo-root `.sqlfluff` (`dialect = snowflake`).
- **`infra-deploy.yml`** — on merge to `main` (and manual dispatch), applies the SQL to Snowflake via `run_sql.py`, authenticating as the `DEVELOPER_SVC` key pair (GitHub `ubuntu-latest` runners don't ship SnowSQL). Gated by a GitHub `infra` environment for optional required-reviewer approval.
- **`run_sql.py`** — a small connector script: loads the PEM private key → DER, connects, and executes each `;`-separated statement from the given files in order. Same key-pair approach as `snowflake_connection.py`.

All scripts are safe to re-run (idempotent `IF NOT EXISTS`; the stream deliberately avoids `OR REPLACE`). Remember the one-time `ACCOUNTADMIN` grants from Step 1 must exist first, or the `GRANT ... ON FUTURE` / `ALTER TASK ... RESUME` statements fail.

**Secrets used:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_DEV_SVC_USER`, `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` (the `DEVELOPER_SVC` key-pair credentials — see Step 6 and `README.md`).

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
            └── rows → CRYPTO_DB.STG.COINGECKO_RAW
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

The manual `de-ingest-history.yml` backfill (Step 3a) runs on demand, appending to `CRYPTO_DB.STG.COINGECKO_HISTORY_RAW`. Merges to `main` touching `de-pipeline/snowflake/**` also trigger `infra-deploy.yml` (Deployment Step-1) independently, applying `setup.sql`/`streams.sql`/`tasks.sql`. `ingest_task.sql` (Step 3b) is excluded — it's disabled and has no effect until re-enabled.

---

## Key design decisions

**Shared landing database, per-environment transform databases.** `CRYPTO_DB.STG` is the single source of truth for raw data — every environment's Silver/Gold layer reads from the exact same rows. There is no per-environment copy of raw data to drift out of sync.

**dbt `--target` is the promotion mechanism.** dev/qa/prod are dbt targets pointing at `DEV_DB`/`QA_DB`/`PROD_DB` respectively, running the *same* SQL. A model is "promoted" simply by re-running the identical build against the next target — not by copying data or maintaining environment-specific SQL branches.

**Custom `generate_schema_name` macro.** Without it, dbt would prefix `silver_crypto`/`gold_crypto` with the target's default schema (e.g. `public_silver_crypto`). The override makes the schema name identical across all three databases, so environment separation comes entirely from `database`, keeping profiles.yml as the only thing that changes per environment.

**Incremental models over full refresh.** Silver uses `unique_key = ['id', 'fetched_at']` so dbt only merges new rows, not reprocessing full history, in whichever database it targets.

**Append-only landing table.** Raw data in `CRYPTO_DB.STG.COINGECKO_RAW` is never modified — every run appends rows with `fetched_at`. This gives a complete audit trail and lets you rebuild Silver/Gold in any environment from any past point in time.

**Shared, modularized ingestion code.** `fetch_data.py`, `snowflake_connection.py`, and `transforms.py` hold the common concerns (API access, connection/auth, rounding) so the daily snapshot and the historical backfill reuse one implementation instead of duplicating it.

**Production ingestion runs via GitHub Actions, not a Snowflake Task — for now.** A Snowflake-native Python Task (Step 3b, `ingest_task.sql`) was built and is kept in the repo, but it needs an External Access Integration, which requires the `CREATE INTEGRATION` privilege — unavailable on the Snowflake trial account this project runs on. Rather than block on that, ingestion runs `ingest_coingecko.py`. `ingest_task.sql`'s SQL is fully commented out and excluded from `infra-deploy.yml`'s file list, so it has zero effect until the account is upgraded and someone deliberately re-enables it (instructions are in Step 3b's comment header).

**Ingestion is a standalone workflow, decoupled from dbt promotion.** `de-ingest.yml` (Step 3a) runs `ingest_coingecko.py` on its own daily cron; `de-deploy.yml` (Step 5) runs dbt on a separate cron staggered 30 minutes later. Splitting them — rather than making ingestion a first job inside `de-deploy.yml` — means each can be retried, rerun, and read in isolation, and a flaky data pull doesn't show up as a "deploy" failure. The tradeoff is there's no hard `needs:` ordering across the two workflows (GitHub Actions can't express that cleanly); the 30-minute stagger is a deliberately loose coupling, and because Silver is incremental, a missed or late ingestion just means that day's dbt run merges fewer new rows rather than breaking.

**Snowflake Stream as a zero-cost trigger.** `APPEND_ONLY = TRUE` on the stream means Snowflake only tracks inserts, the cheapest change-tracking mode. `SYSTEM$STREAM_HAS_DATA` prevents the Task from consuming credits when there's nothing new — it exists purely as a monitoring signal, since the real Silver/Gold builds run through dbt.

**GitHub Environments gate qa/prod promotion.** `deploy-qa` and `deploy-prod` are tied to GitHub `environment: qa` / `environment: prod`, so promotion out of dev (and out of qa) can require manual reviewer approval without adding any custom logic to the workflow.

**`state:modified+` in CI.** The CI workflow only builds models that changed in the PR plus their downstream dependencies, against `dev_db` — making CI runs fast, cheap, and safe to iterate on without touching qa/prod.
