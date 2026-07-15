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
│       ├── de-ingest.yml            # external cron (cron-job.org): run ingest_coingecko.py -> CRYPTO_DB.STG (standalone)
│       ├── de-ingest-history.yml    # MANUAL only: backfill daily history -> COINGECKO_HISTORY_RAW
│       ├── dbt-run.yml              # Step 5b: dbt CI (PR) + build DEV_DB (merge / external cron)
│       ├── dbt-snowflake.yml        # Step 5a (disabled): recreate DBT PROJECT in Snowflake on merge
│       └── dbt-promote.yml          # Step 6: build QA_DB or PROD_DB by `target` input (gated, per-env cron)
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
    ├── dbt/                              # dbt project (Step 4)
    │   ├── dbt_project.yml
    │   ├── profiles.yml                  # dev/qa/prod targets -> DEV_DB/QA_DB/PROD_DB (key-pair)
    │   ├── packages.yml                  # dbt_utils
    │   ├── macros/
    │   │   └── generate_schema_name.sql
    │   ├── snapshots/
    │   │   └── snap_coin.sql             # SCD Type-2 history behind dim_coin
    │   └── models/
    │       ├── silver/                   # cleaned, incremental staging of both feeds
    │       │   ├── _landing__sources.yml
    │       │   ├── _silver__models.yml
    │       │   ├── stg_coingecko_snapshot.sql
    │       │   ├── stg_coingecko_history.sql
    │       │   └── int_coin_daily.sql
    │       └── gold/                     # star schema (SCD2 dims + incremental facts, SK/PK/FK)
    │           ├── _gold__models.yml
    │           ├── dim_coin.sql
    │           ├── dim_date.sql
    │           ├── fct_market_snapshot.sql
    │           └── fct_daily_market.sql
    ├── snowflake/
    │   ├── setup.sql                # warehouse, 4 databases, schemas, landing tables, role + grants
    │   ├── streams.sql              # append-only Stream on COINGECKO_RAW
    │   ├── tasks.sql                # "new data" signal Task
    │   ├── ingest_task.sql          # DISABLED — Snowflake Task ingestion (needs External Access Integration)
    │   ├── dbt_project_task.sql     # DISABLED (Step 5a) — EXECUTE DBT PROJECT on a Task (billed account)
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

**Scheduled workflow — `de-ingest.yml`:** standalone, triggered by **external cron (cron-job.org)** — `workflow_dispatch` (also the manual **Run workflow** button) plus `repository_dispatch` (type `de-ingest`), with **no GitHub `schedule:`**. cron-job.org POSTs to the workflow's `dispatches` endpoint on schedule, set directly in Bangkok time (e.g. 6-hourly). Runs `ingest_coingecko.py` with `INGEST_MODE=snowflake` and the `DEVELOPER_SVC` key-pair secrets. No `push:` trigger — a code change shouldn't fire a real data load. Kept separate from dbt so it can be retried/read in isolation. Setup steps are in `README.md` §2.4.

> Why external cron (same choice as the dbt workflows, Step 5b/6): GitHub's built-in `schedule:` only runs from the **default branch**, never backfills a missed slot, and is delayed/dropped under load (worst at top-of-hour). cron-job.org fires on time and lets you set the schedule directly in Bangkok time; the trigger is a fine-grained **PAT** (Actions: read+write) held only in the cron-job.org job, never in the repo.

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

> **Trigger model — merge vs external cron.** This infra deploy runs on **merge to `main`** (a code change to the SQL). The *recurring* pipeline workflows do **not** run on GitHub's built-in `schedule:` (delayed/dropped under load, only fires from the default branch) — `de-ingest.yml` (Step 3a) and the dbt workflows `dbt-run.yml` / `dbt-promote.yml` (Steps 5b/6) are each fired by **cron-job.org** hitting the GitHub REST API (`workflow_dispatch` / `repository_dispatch`). That external trigger uses a fine-grained **PAT** (Actions: read+write) stored only in the cron-job.org job — never a repo secret. So the only GitHub-native triggers in the project are merge (infra + dbt PR/deploy) and manual dispatch; all timed runs come from cron-job.org.

---

## Step 4 — dbt data warehouse (medallion → star schema)

> **Scope: `dev_db` only.** Every dbt run during development targets `DEV_DB` (`dbt build --target dev`), so all transformed data lands in `dev_db`. Promotion to `qa_db` / `prod_db` is a separate later step and is intentionally not covered here. The implementation lives in `de-pipeline/dbt/`.

dbt implements the medallion layers on top of the landing zone:

- **Bronze** = the append-only landing tables in `CRYPTO_DB.STG` (`COINGECKO_RAW`, `COINGECKO_HISTORY_RAW`). dbt reads them as **sources** — it does not build them.
- **Silver** (`dev_db.silver_crypto`) = cleaned, typed, deduplicated staging of *both* raw feeds.
- **Gold** (`dev_db.gold_crypto`) = an analysis-ready **star schema** (conformed dimensions + fact tables) with surrogate / primary / foreign keys.

### Project layout & config (`de-pipeline/dbt/`)

- **`dbt_project.yml`** — project `crypto_pipeline`; `silver/` builds into schema `silver_crypto` (all **incremental** tables), `gold/` into `gold_crypto`. Snapshots also land in `silver_crypto`.
- **`profiles.yml`** — a single `dev` target pointing at `DEV_DB`. Authenticates as **`DEVELOPER_SVC` via key-pair** (role `SYSADMIN`), reading `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_USER` / `SNOWFLAKE_PRIVATE_KEY_PATH` from `env_var`. qa/prod targets are added in the later deployment step.
- **`packages.yml`** — `dbt_utils` (surrogate keys, date spine, extra tests).
- **`macros/generate_schema_name.sql`** — emits the configured schema (`silver_crypto` / `gold_crypto`) verbatim instead of dbt's default target-prefixed name, so the same models build cleanly in any database.
- **`macros/create_schema.sql`** — overrides dbt's `create_schema` to a no-op: the schemas are pre-created by `setup.sql`, so dbt only builds *into* them and never issues `CREATE SCHEMA`.
- **`models/silver/_landing__sources.yml`** — declares the two landing tables as sources, pinned to `CRYPTO_DB.STG` regardless of target.
- **`snapshots/snap_coin.sql`** — the dbt snapshot that powers the SCD Type-2 coin dimension (see Gold).

**Materialization:** everything is a physical **table** loaded **incrementally** — no views. Silver models and the fact tables are dbt `incremental` models (each processes only new rows); `dim_coin` is rebuilt from the incremental SCD2 snapshot; `dim_date` is a small generated calendar table.

Run with `dbt deps` then `dbt build --target dev` from `de-pipeline/dbt/`. `dbt build` runs models, snapshots, and tests together, so the key/type tests below gate the build.

### Silver — `dev_db.silver_crypto` (clean & conform the two feeds)

Silver stages **both** ingestion feeds — the 6-hourly snapshot (`de-ingest.yml` / `ingest_coingecko.py`) and the manual history backfill (`backfill_coingecko_history.py`). Because the landing tables are append-only, every staging model **deduplicates** on its natural key, casts types, uppercases symbols, drops bad rows (null id, non-positive price), and adds a `price_date`. All three are **incremental** tables.

| Model | Source | Grain | Incremental key / watermark |
|---|---|---|---|
| `stg_coingecko_snapshot` | `coingecko_raw` (6-hourly) | one row per coin per snapshot `fetched_at` | merge on `coin_id`+`fetched_at`; new rows where `fetched_at` > max |
| `stg_coingecko_history` | `coingecko_history_raw` (backfill) | one row per coin per day | merge on `coin_id`+`price_date` (latest `fetched_at` wins); new rows where `fetched_at` > max |
| `int_coin_daily` | the two models above | one row per coin per day (unified) | merge on `coin_id`+`price_date`; recomputes only days with new source data (`_synced_at` watermark) |

`int_coin_daily` unifies both feeds onto one **daily** series per coin. Because the 6-hourly snapshot can land several extracts in a day, per `(coin, day)` it computes **both** the day's **average across all extracts** (`avg_*`) and the **latest extract's** values (`latest_*`), plus `extract_count`. The historical backfill has one value per day (so `avg = latest`, `extract_count = 1`), and the **snapshot wins** when a day appears in both feeds — history only fills days with no snapshots. Its incremental logic recomputes each *touched* day from **all** of that day's rows, so the average and the latest are always computed over the complete day. Tests assert `not_null` keys, positive prices, and uniqueness of the grain.

### Gold — `dev_db.gold_crypto` (star schema)

A classic star: two **conformed dimensions** and two **incremental fact tables**. Every row carries a **surrogate key (SK)** built with `dbt_utils.generate_surrogate_key`; facts carry the dimensions' SKs as **foreign keys**. Descriptive attributes live only in the dimensions; facts hold measures.

**Dimensions**

| Table | PK (surrogate) | Natural key | Type | Attributes |
|---|---|---|---|---|
| `dim_coin` | `coin_sk` (per version) | `coin_id` (durable) | **SCD Type-2** | `symbol`, `name`, `asset_type` (coin / stablecoin), `valid_from`, `valid_to`, `is_current` |
| `dim_date` | `date_sk` (`YYYYMMDD`) | calendar date | static | year, quarter, month, month name, day, ISO weekday, `is_weekend` |

- **`dim_coin` is SCD Type-2.** Its history is tracked by the `snap_coin` dbt **snapshot** (`check` strategy on `symbol` / `name` / `asset_type`), which writes a new version row only when a coin's attributes change and stamps `dbt_valid_from` / `dbt_valid_to`. `dim_coin` exposes those as `valid_from` / `valid_to` / `is_current`, with `coin_sk` unique **per version**. The earliest version per coin is back-dated so facts whose event pre-dates the first snapshot still resolve to a version.
- **`dim_date` is a static generated calendar** (a `dbt_utils.date_spine`). SCD Type-2 does not apply — calendar attributes never change.

**Facts** (both `incremental`)

| Table | PK (surrogate) | FKs | Grain | Measures |
|---|---|---|---|---|
| `fct_market_snapshot` | `snapshot_sk` | `coin_sk`, `date_sk` | coin × snapshot timestamp (6-hourly) | current price, market cap, volume, 24h change & %, 24h high/low, circulating supply, ATH |
| `fct_daily_market` | `daily_market_sk` | `coin_sk`, `date_sk` | coin × day | `extract_count`; day-average price / market cap / volume; latest-extract price / market cap / volume; previous close; daily return % |

Each fact resolves `coin_sk` with an **SCD Type-2 range join** to `dim_coin` — matching the coin version whose `[valid_from, valid_to)` window contains the event's timestamp/date — so a fact always points to the coin attributes that were in effect at the time.

Keys are **enforced by dbt tests**, so the model *is* the contract:

- **PK** — `unique` + `not_null` on every SK (`coin_sk` is unique per version; `coin_id` is only `not_null` under SCD2, with a test that exactly one version per coin is `is_current`).
- **FK** — `relationships` tests from each fact's `coin_sk` / `date_sk` back to the parent dimension's SK.

The two facts share the same conformed dimensions, so `fct_market_snapshot` (intraday detail) and `fct_daily_market` (daily series) can be sliced by the same coin/date attributes — ready for BI on `dev_db.gold_crypto`.

---
## Step 5 — Running the dbt project (orchestration)

Step 4 *defines* the dbt project; Step 5 is **how it runs on a schedule**. There are two options — the Snowflake-native one (5a) is the target but needs a billed account, so for now it runs on GitHub Actions triggered by an external scheduler (5b). Both build into `DEV_DB` only (Step 4 scope); dev → qa → prod promotion is a later step.

### 5a — Snowflake-native: `EXECUTE DBT PROJECT` on a Task (target state, billed account)

Snowflake hosts and runs the dbt project itself ("dbt Projects on Snowflake"): `de-pipeline/dbt` is fetched into a Snowflake **Git repository** object and registered as a **`DBT PROJECT`**, then built with `EXECUTE DBT PROJECT ... ARGS='build --target dev'`. No external runner and no credentials leaving Snowflake.

**Recreate and execute are split** across two objects in `snowflake/dbt_project_task.sql` (recreate only when code changes; run on a schedule):

- **`SP_RUN_DBT_PROJECT()`** — *recreate only*: `ALTER GIT REPOSITORY … FETCH` + `CREATE OR REPLACE DBT PROJECT`. No build. Meant to be **called from CI on merge**, so the Snowflake project object mirrors `main` only when there's actually new code.
- **`TASK_DBT_BUILD`** — *execute only*: `EXECUTE DBT PROJECT … 'deps'` then `'build --target dev'`, on a schedule (08:30 Bangkok). It never recreates — it runs whatever the current project is.

The GitHub side is **`dbt-snowflake.yml`** (the 5a CI/CD companion): a **PR** validates the project (`dbt deps` + `dbt debug` + `dbt compile`, no materialization); a **merge to `main`** runs `snow sql` → `CALL SP_RUN_DBT_PROJECT()` to refresh the Snowflake project.

Why it isn't active now — same trial limits as Step 3b: it needs a **billed account** with the dbt-Projects feature and an **External Access Integration** (+ account-level `CREATE INTEGRATION`) to fetch the git repo and run `dbt deps`. So both pieces ship **disabled**:
- `dbt_project_task.sql` is **reference SQL** — excluded from `infra-deploy.yml` (like `ingest_task.sql`) and skipped by `infra-ci`'s sqlfluff check (`.sqlfluffignore`, since the preview DDL isn't in sqlfluff's dialect). Its header carries the one-time `ACCOUNTADMIN` setup (git API integration + secret, external access integration, grants) and the `<placeholders>` to fill in.
- `dbt-snowflake.yml` is **dormant** — only `workflow_dispatch` is wired and its jobs are guarded to `pull_request`/`push`, so nothing runs until the triggers are uncommented.

> The `CREATE DBT PROJECT` / `EXECUTE DBT PROJECT` DDL is a newer Snowflake feature — verify the exact syntax against your account's release before enabling. Don't run 5a **and** 5b at once, or dbt executes from two places.

### 5b — For now: GitHub Actions runner (`dbt-run.yml`)

Until 5a is available, the dbt project runs on a **GitHub-hosted `ubuntu` runner** (`.github/workflows/dbt-run.yml`) — the same `dbt build --target dev` you run locally, authenticating as `DEVELOPER_SVC` via key-pair (matching `profiles.yml`) into `DEV_DB`. One workflow, three entry points via event-guarded jobs:

- **PR touching `de-pipeline/dbt/**`** → the `ci` job: `dbt build` + tests, validating the change. *Note:* it builds into the shared `DEV_DB` schemas — there's no per-PR isolation, because `create_schema` is a no-op.
- **Merge to `main`** touching `de-pipeline/dbt/**` → the `deploy-dev` job: rebuild `DEV_DB`.
- **External cron** → `deploy-dev`, fired by **cron-job.org** hitting the GitHub REST API (`workflow_dispatch`; `repository_dispatch` type `dbt-run` is also wired). GitHub's built-in `schedule:` is deliberately avoided — it's delayed/dropped under load and only fires from the default branch (Step 3a); cron-job.org also lets you set the schedule directly in Bangkok time.

- **cron-job.org job:** `POST https://api.github.com/repos/<owner>/coin-dwh-ml/actions/workflows/dbt-run.yml/dispatches`, headers `Authorization: Bearer <PAT>` / `Accept: application/vnd.github+json` / `X-GitHub-Api-Version: 2022-11-28`, body `{"ref":"main"}`, e.g. daily 08:30 Asia/Bangkok. The PAT is fine-grained (**Actions: read and write**) and lives only in the cron-job.org job header.
- **Secrets used** (same key-pair as the other dbt/infra workflows): `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_DEV_SVC_USER` (= `DEVELOPER_SVC`), `SNOWFLAKE_DEV_SVC_PRIVATE_KEY`. The workflow writes the private key to `.keys/rsa_key.p8` at runtime and points `SNOWFLAKE_PRIVATE_KEY_PATH` at it; role is `SYSADMIN`.

---


## Step 6 — dbt promotion CI/CD (dev → qa → prod)

Step 5 builds **dev**; Step 6 promotes the same dbt project up through **QA** and **PROD**. Promotion is not a data copy: dev/qa/prod are three dbt **targets** in `profiles.yml` that run the *identical* models against different databases (`DEV_DB` / `QA_DB` / `PROD_DB`), all reading the one shared landing zone `CRYPTO_DB.STG`. "Promoting" a model is just re-running its build against the next target — each environment rebuilds a full, independent medallion + star schema in its own database.

**Three targets in `profiles.yml`.** `dev` → `DEV_DB`, `qa` → `QA_DB`, `prod` → `PROD_DB`. Only `database` differs; account / user / private key / role come from the **same** env vars (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH`, `SNOWFLAKE_ROLE`) for all three, authenticating as the `DEVELOPER_SVC` key-pair (role `SYSADMIN`, which owns all three databases). So `profiles.yml` is the only per-environment knob and nothing environment-specific is hardcoded except the database name.

**Two tracks, split by concern (both key-pair, both fired by external cron — Step 5b's reasoning):**
- **`dbt-run.yml` (Step 5b) = the dev track** — PR CI, merge-to-`main` deploy, and the daily cron build into `DEV_DB`.
- **`dbt-promote.yml` (Step 6) = the qa/prod promotion track** — builds `QA_DB` or `PROD_DB`. It does **not** rebuild dev (that's `dbt-run.yml`'s job); it promotes the committed models onward.

### The promotion workflow — `.github/workflows/dbt-promote.yml`

It's **one parameterized job**, not a qa→prod chain. Every target shares identical credentials/secrets; the only thing that changes per environment is a `target` input (`qa` / `prod`) that selects the database via `profiles.yml`. So there are no per-env secrets or env vars — QA and PROD differ purely by which target you dispatch. This lets each environment run on its **own** external-cron schedule.

- **The job:** resolves the target from `github.event.inputs.target` (workflow_dispatch) or `client_payload.target` (repository_dispatch), validates it's `qa`/`prod`, then runs `dbt deps` + `dbt build --target <target>`. `dbt build` runs models **+** the SCD2 snapshot **+** all tests, so a failing PK/FK/type test fails that environment's build.
- **Gating with GitHub Environments:** the job's `environment:` is set to the chosen target, so the matching [environment](https://docs.github.com/en/actions/deployment/targeting-different-environments/using-environments-for-deployment)'s required reviewers (Settings → Environments → `qa` / `prod`) gate that run; with no reviewers it proceeds straight through.
- **Triggers (no GitHub `schedule:`):** `workflow_dispatch` (a `target` choice input — manual button + cron-job.org dispatch target) and `repository_dispatch` (type `dbt-promote`, `client_payload.target`).
- **Auth & secrets:** the same `DEVELOPER_SVC` key-pair as every other workflow — `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_DEV_SVC_USER`, `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` (written to `.keys/rsa_key.p8` at runtime); role `SYSADMIN`.

**cron-job.org — one job per environment** (each on its own schedule, staggered after the dev run): both `POST https://api.github.com/repos/<owner>/coin-dwh-ml/actions/workflows/dbt-promote.yml/dispatches` with the usual headers, differing only in the body's `inputs.target` — `{"ref":"main","inputs":{"target":"qa"}}` (e.g. 09:00 Bangkok) and `{"ref":"main","inputs":{"target":"prod"}}` (e.g. 10:00 Bangkok). Setup steps are in `README.md`.

> Because all three environments read the same shared `CRYPTO_DB.STG` and run identical SQL, QA and PROD each build a complete, independent copy of the warehouse in their own database. The gate is organizational (per-env reviewer approval), not a data dependency — a late/failed dev run doesn't block promotion, it just means that build merges the same rows dev would have.

---

## Step 7 — GitHub Secrets & Environments to configure

**Secrets** — Go to **GitHub repo → Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Used by | Example value |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | all workflows | `xy12345.us-east-1` |
| `SNOWFLAKE_DEV_SVC_USER` | all workflows | `DEVELOPER_SVC` |
| `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` | all workflows | full contents of `.keys/rsa_key.p8` |

Every CI workflow — `infra-deploy.yml`, `de-ingest.yml` / `de-ingest-history.yml`, and the dbt workflows (`dbt-run.yml`, `dbt-promote.yml`, `dbt-snowflake.yml`) — authenticates as the **`DEVELOPER_SVC` key-pair** (`SYSADMIN` + `USERADMIN`, from `README.md`), so only these three secrets are needed. There is no `SNOWFLAKE_PASSWORD` in CI: `DEVELOPER_SVC` is `TYPE = SERVICE` (key-pair only). Password auth with the narrower `CRYPTO_PIPELINE_ROLE` (`SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD`) exists only as an option for **local** ingestion runs (`config.py` — see `README.md` §2.3), never in the workflows.

The external-cron **GitHub PAT** (cron-job.org → `dbt-run.yml` / `dbt-promote.yml`, Steps 5b/6) is **not** a repo secret — it lives only in the cron-job.org job header.

**Environments** — Go to **GitHub repo → Settings → Environments** and create `infra`, `qa`, and `prod`. Add required reviewers on each to gate, respectively: applying infra changes to Snowflake (`infra-deploy.yml`), the QA promotion, and the PROD promotion (`dbt-promote.yml`, whose single job runs in the environment named by its `target` input).

---

## Pipeline execution flow

Ingestion, the dev build, and dbt promotion run as independent workflows on staggered daily schedules (the dbt ones fired by external cron):

```
~08:00 Bangkok (external cron via cron-job.org) OR manual dispatch — de-ingest.yml (Step 3a)
│
└── Job: ingest
      └── ingest_coingecko.py runs (INGEST_MODE=snowflake)
            └── rows → CRYPTO_DB.STG.COINGECKO_RAW
                  └── STREAM_COINGECKO_RAW captures new rows
                        └── TASK_NOTIFY_NEW_DATA fires (signal only)

08:30 Bangkok / 01:30 UTC daily (external cron via cron-job.org) OR push to main OR manual — dbt-run.yml (Step 5b)
│   (dev track; staggered 30 min after ingestion so the day's rows have landed)
│
└── Job: deploy-dev — dbt build --target dev
      ├── DEV_DB.SILVER_CRYPTO  (stg_coingecko_snapshot, stg_coingecko_history, int_coin_daily — incremental)
      └── DEV_DB.GOLD_CRYPTO    (dim_coin, dim_date, fct_market_snapshot, fct_daily_market + tests)

~09:00 Bangkok — cron-job.org POST {"inputs":{"target":"qa"}}   — dbt-promote.yml (Step 6)
└── Job: promote (environment=qa, gated by "qa" approval) — dbt build --target qa
      └── QA_DB.SILVER_CRYPTO + QA_DB.GOLD_CRYPTO   (same models + snapshot + tests)

~10:00 Bangkok — cron-job.org POST {"inputs":{"target":"prod"}} — dbt-promote.yml (Step 6)
└── Job: promote (environment=prod, gated by "prod" approval) — dbt build --target prod
      └── PROD_DB.SILVER_CRYPTO + PROD_DB.GOLD_CRYPTO  (a failing test fails the build)

(one parameterized workflow; each env has its own cron + schedule + `target` input — also runnable by hand)
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

**Ingestion, dev build, and promotion are separate workflows, decoupled by a time stagger.** `de-ingest.yml` (Step 3a) runs `ingest_coingecko.py` on its own external cron; `dbt-run.yml` (Step 5b) builds dev on a cron staggered ~30 minutes later; `dbt-promote.yml` (Step 6) builds qa and prod, each on its own cron staggered later still. Splitting them — rather than chaining ingestion as a first job inside the dbt workflow — means each can be retried, rerun, and read in isolation, and a flaky data pull doesn't show up as a "deploy" failure. The tradeoff is there's no hard `needs:` ordering across workflows (GitHub Actions can't express that cleanly); the staggers are deliberately loose coupling, and because Silver is incremental, a missed or late upstream step just means that run merges fewer new rows rather than breaking.

**Snowflake Stream as a zero-cost trigger.** `APPEND_ONLY = TRUE` on the stream means Snowflake only tracks inserts, the cheapest change-tracking mode. `SYSTEM$STREAM_HAS_DATA` prevents the Task from consuming credits when there's nothing new — it exists purely as a monitoring signal, since the real Silver/Gold builds run through dbt.

**One parameterized promotion workflow, gated by GitHub Environments.** `dbt-promote.yml` is a single job whose `target` (`qa`/`prod`) is chosen per dispatch and whose `environment:` is set to that same name — so per-env required reviewers gate the run and QA/PROD share one set of credentials, differing only by `--target`. Each environment gets its own external-cron schedule (a different `inputs.target` in the cron body), rather than a fixed qa→prod chain, so they can run independently at different times.

**PR CI builds against dev_db.** `dbt-run.yml`'s `ci` job runs a full `dbt build --target dev` (models + snapshot + tests) on every PR touching `de-pipeline/dbt/**`, so a broken model or a failing key/type test blocks the merge. It builds into the shared `DEV_DB` schemas — there's no per-PR schema isolation, since `create_schema` is a no-op — a deliberate simplification for a single-developer project; a larger team would swap in per-PR schemas or `state:modified+` selection.