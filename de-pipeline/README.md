# Crypto Market Pipeline

Daily crypto market data pipeline: CoinGecko → Snowflake (landing → dev/qa/prod via dbt) → GitHub Actions.
Full design doc: [`../.claude/crypto-de-pipeline-project-plan.md`](../.claude/crypto-de-pipeline-project-plan.md).

---

## 1. Create a developer service account (key-pair auth)

This account is for **infra setup** — applying `snowflake/setup.sql`, `streams.sql`, and `tasks.sql`, done automatically by `infra-deploy.yml` (section 1.3–1.4) rather than run by hand — not for the ingestion pipeline itself (that uses `CRYPTO_PIPELINE_ROLE`, created *by* this account, and runs on its own GitHub Actions schedule — see the project plan's Step 5). Key-pair authentication is used instead of a password so nothing secret ever needs to be typed into a SQL client or stored as a plaintext password.

`snowflake/ingest_task.sql` also exists in the repo but is currently **disabled** (section 1.5) — it's not applied by `infra-deploy.yml`.

### 1.1 Generate an RSA key pair

Requires `openssl.exe` on `PATH` — it ships with Git for Windows, so run this from a regular `cmd.exe` after installing Git (or use "Git CMD"):

```cmd
:: Windows cmd
cd de-pipeline
if not exist .keys mkdir .keys
cd .keys

:: private key — unencrypted, never commit (.keys/ is gitignored)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

:: public key — this is what gets registered in Snowflake
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

```bash
# Linux / macOS
cd de-pipeline
mkdir -p .keys && cd .keys

# private key — unencrypted, never commit (.keys/ is gitignored)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# public key — this is what gets registered in Snowflake
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

### 1.2 Create the user and grant SYSADMIN + USERADMIN

Run once as `ACCOUNTADMIN` (Snowsight worksheet or any SQL client). Paste the contents of `rsa_key.pub` with the `-----BEGIN/END PUBLIC KEY-----` header/footer and newlines stripped out:

```sql
CREATE USER IF NOT EXISTS DEVELOPER_SVC
  TYPE = SERVICE
  RSA_PUBLIC_KEY = '<contents of rsa_key.pub>'
  DEFAULT_ROLE = SYSADMIN
  DEFAULT_WAREHOUSE = CRYPTO_WH
  COMMENT = 'Infra CI/CD service account key-pair auth';

GRANT ROLE SYSADMIN  TO USER DEVELOPER_SVC;  -- create/manage warehouses, databases, schemas, tables
GRANT ROLE USERADMIN TO USER DEVELOPER_SVC;  -- create/manage CRYPTO_PIPELINE_ROLE

-- Without this, ALTER TASK ... RESUME fails for every task SYSADMIN owns
-- (streams.sql/tasks.sql from Step 2, ingest_task.sql from Step 3b) —
-- EXECUTE TASK is a global privilege only ACCOUNTADMIN holds by default.
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

-- Without this, setup.sql's `GRANT ... ON FUTURE TABLES/SCHEMAS/VIEWS ...`
-- statements fail with "Insufficient privileges ... must have MANAGE GRANTS".
-- Owning an object lets SYSADMIN grant on EXISTING objects, but FUTURE grants
-- specifically require MANAGE GRANTS, which only ACCOUNTADMIN holds by default.
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE SYSADMIN;
```

`TYPE = SERVICE` marks this as a non-human/programmatic account — Snowflake blocks password-based login for it entirely (key-pair or OAuth only), so there's no interactive-login credential to leak or rotate, which fits an account that only ever authenticates from `infra-deploy.yml`.

Snowflake splits these privileges across two built-in roles — `SYSADMIN` cannot run `CREATE ROLE` on its own, and `USERADMIN` cannot create warehouses/databases — so the dev account needs both. `snowflake/setup.sql` switches between them itself (`USE ROLE SYSADMIN` / `USE ROLE USERADMIN`) as it creates each kind of object. `EXECUTE TASK` and `MANAGE GRANTS` are separate account-level privileges neither built-in role holds by default: without `EXECUTE TASK`, any `ALTER TASK ... RESUME` as `SYSADMIN` fails; without `MANAGE GRANTS`, setup.sql's `GRANT ... ON FUTURE ...` statements fail. Both are granted here once so they're not rediscovered as failed deploys later.

### 1.3 Wire the service account into GitHub Actions (`infra-deploy.yml`)

`infra-deploy.yml` applies `setup.sql`/`streams.sql`/`tasks.sql` automatically, authenticating as `DEVELOPER_SVC` — there's no need to run any of them by hand. (`ingest_task.sql` is excluded — see section 1.5.) Add two repository secrets (**Settings → Secrets and variables → Actions**):

| Secret | Value |
|---|---|
| `SNOWFLAKE_DEV_SVC_USER` | `DEVELOPER_SVC` |
| `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` | the full contents of `.keys/rsa_key.p8`, including the `-----BEGIN/END PRIVATE KEY-----` lines |

`SNOWFLAKE_ACCOUNT` is shared with `de-deploy.yml` (Step 5) and only needs to be added once.

### 1.4 Apply the infra

With the secrets above in place, either:

- Push a change under `de-pipeline/snowflake/**` to `main`, or
- Trigger it manually: **GitHub repo → Actions → Infra Deploy → Run workflow**

This creates `CRYPTO_WH`, all four databases (`CRYPTO_DB`, `DEV_DB`, `QA_DB`, `PROD_DB`), their schemas, the raw landing table, and `CRYPTO_PIPELINE_ROLE` — the role dbt/CI use day-to-day. All three scripts are safe to re-run (`CREATE ... IF NOT EXISTS`), so repeat deploys are idempotent — no destructive drops or offset resets.

Grant `CRYPTO_PIPELINE_ROLE` to whatever user your ingestion / dbt / CI authenticates as (this is the `SNOWFLAKE_USER` secret used by `de-ingest.yml` and the dbt workflows alike — see the project plan's Step 6):

```sql
GRANT ROLE CRYPTO_PIPELINE_ROLE TO USER <your_ingestion_user>;
```

<details>
<summary>Run manually instead (optional — e.g. no GitHub Actions access yet)</summary>

Using [SnowSQL](https://docs.snowflake.com/en/user-guide/snowsql) (or paste each file into a Snowsight worksheet while authenticated as `DEVELOPER_SVC`):

```cmd
:: Windows cmd
snowsql -a <account_identifier> -u DEVELOPER_SVC ^
  --private-key-path .keys\rsa_key.p8 ^
  -f snowflake\setup.sql

snowsql -a <account_identifier> -u DEVELOPER_SVC ^
  --private-key-path .keys\rsa_key.p8 ^
  -f snowflake\streams.sql

snowsql -a <account_identifier> -u DEVELOPER_SVC ^
  --private-key-path .keys\rsa_key.p8 ^
  -f snowflake\tasks.sql
```

```bash
# Linux / macOS
snowsql -a <account_identifier> -u DEVELOPER_SVC \
  --private-key-path .keys/rsa_key.p8 \
  -f snowflake/setup.sql

snowsql -a <account_identifier> -u DEVELOPER_SVC \
  --private-key-path .keys/rsa_key.p8 \
  -f snowflake/streams.sql

snowsql -a <account_identifier> -u DEVELOPER_SVC \
  --private-key-path .keys/rsa_key.p8 \
  -f snowflake/tasks.sql
```

</details>

### 1.5 Snowflake Task ingestion — disabled on trial accounts

`snowflake/ingest_task.sql` is a from-inside-Snowflake alternative to the standalone `de-ingest.yml` GitHub Actions workflow (the project plan's Step 3a). It's **not in use** and **not applied** by `infra-deploy.yml` — skip this section unless you've upgraded off a Snowflake trial account.

It would call the CoinGecko API from *inside* Snowflake, which needs an **External Access Integration** — an account-level object requiring the global `CREATE INTEGRATION` privilege. That privilege isn't available on a Snowflake trial account, so `ingest_task.sql`'s SQL is fully commented out in the repo rather than deleted. Production ingestion runs the GitHub Actions way instead (`ingest_coingecko.py` via `de-ingest.yml`, scheduled daily — see section 2 below).

If you later upgrade the Snowflake account and want to switch to Task-based ingestion, `ingest_task.sql`'s header comment has the exact steps: run the `ACCOUNTADMIN` network-rule/integration setup once, uncomment the file's contents, and add it back to `run_sql.py`'s file list in `infra-deploy.yml`. Decide at that point whether to keep the `de-ingest.yml` workflow running too — running both would double-insert rows.

---

## 2. Local Python setup & running ingestion

No Snowflake credentials are needed to run ingestion locally — it defaults to writing a CSV.

### 2.1 Create a virtual environment

```cmd
:: Windows cmd
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

### 2.2 Run in local mode (default — writes CSV, no Snowflake needed)

```cmd
python ingestion\ingest_coingecko.py
```

```bash
python ingestion/ingest_coingecko.py
```

Output: `ingestion/data/coingecko_raw_<timestamp>.csv` — open it and confirm the rows (one per coin in the curated set) look right.

### 2.3 Switch to loading into Snowflake

This is the same mode `de-ingest.yml` runs in production, on a schedule — see the project plan's Step 3a for the actual GitHub Actions workflow (daily at 8AM Bangkok time / 1AM UTC). Running it here by hand first is how you verify the write path before trusting the scheduled job.

Copy `ingestion/.env.example` to `ingestion/.env` (gitignored — never commit it) and fill in:

```
INGEST_MODE=snowflake
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=<user with CRYPTO_PIPELINE_ROLE granted>
SNOWFLAKE_PASSWORD=********
```

Then run the same command again — it now loads into `CRYPTO_DB.STG.COINGECKO_RAW` instead of writing a CSV:

```cmd
python ingestion\ingest_coingecko.py
```

```bash
python ingestion/ingest_coingecko.py
```

See [`ingest_coingecko.py`](ingestion/ingest_coingecko.py) and [`config.py`](ingestion/config.py) for the full local/Snowflake mode switch, and the [project plan](../.claude/crypto-de-pipeline-project-plan.md) for the dbt/CI/CD steps that follow ingestion.

---

## 3. Local dbt setup & running (dev_db)

The dbt project lives in [`dbt/`](dbt/) and builds the medallion → star schema (Silver `silver_crypto`, Gold `gold_crypto`) into **`DEV_DB`** — the only target during development. `dbt/profiles.yml` is committed and reads all credentials from **environment variables**, so no secrets are stored in the repo.

**Prerequisites:**

1. `snowflake/setup.sql` has been applied, so `DEV_DB.SILVER_CRYPTO` / `DEV_DB.GOLD_CRYPTO` already exist. dbt **uses** those schemas — it does not create them.
2. You have the `DEVELOPER_SVC` private key locally (`.keys/rsa_key.p8` from section 1.1) — dbt authenticates with it.
3. There is raw data in `CRYPTO_DB.STG` to transform — run the ingestion in Snowflake mode first (section 2.3), otherwise Silver/Gold build empty.

### 3.1 Install dbt

Reuse the same `de-pipeline/.venv` from section 2.1 (or make a fresh one), then add the Snowflake adapter:

```cmd
:: Windows cmd
cd de-pipeline
.venv\Scripts\activate.bat
pip install dbt-snowflake==1.7.0
```

```bash
# Linux / macOS
cd de-pipeline
source .venv/bin/activate
pip install dbt-snowflake==1.7.0
```

### 3.2 Set the connection environment variables

`dbt/profiles.yml` authenticates as **`DEVELOPER_SVC` via key-pair** (the same account and key from section 1) and resolves the connection from these variables via `env_var()`:

| Env var | Required | Purpose |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | yes | Account identifier, e.g. `xy12345.us-east-1` |
| `SNOWFLAKE_USER` | yes | `DEVELOPER_SVC` |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | no (default `../.keys/rsa_key.p8`) | Path to the private key `.p8` from section 1.1 |
| `SNOWFLAKE_ROLE` | no (default `SYSADMIN`) | Role dbt builds as |

> `DEVELOPER_SVC` is `TYPE = SERVICE` — key-pair only, no password — so there's **no** `SNOWFLAKE_PASSWORD` here. Its usable role is `SYSADMIN`, which owns `DEV_DB` and `CRYPTO_DB.STG`, so it can read the landing tables and build the Silver/Gold schemas. (A dedicated least-privilege key-pair user with `CRYPTO_PIPELINE_ROLE` is the tidier long-term option, but reusing `DEVELOPER_SVC` keeps one credential for dev.)
> The default key path is relative to `de-pipeline/dbt/`, so it resolves when you run dbt from there (section 3.3). Set an absolute `SNOWFLAKE_PRIVATE_KEY_PATH` if you run from elsewhere. Warehouse (`CRYPTO_WH`) and database (`DEV_DB`) are pinned in `profiles.yml`.

```cmd
:: Windows cmd — session-scoped; re-run in each new shell
set SNOWFLAKE_ACCOUNT=xy12345.us-east-1
set SNOWFLAKE_USER=DEVELOPER_SVC
:: optional — defaults shown
set SNOWFLAKE_PRIVATE_KEY_PATH=..\.keys\rsa_key.p8
set SNOWFLAKE_ROLE=SYSADMIN
```

```bash
# Linux / macOS — session-scoped; add to ~/.bashrc / ~/.zshrc to persist
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=DEVELOPER_SVC
# optional — defaults shown
export SNOWFLAKE_PRIVATE_KEY_PATH=../.keys/rsa_key.p8
export SNOWFLAKE_ROLE=SYSADMIN
```

> If your key is encrypted (a passphrase-protected `.p8`), also add a `private_key_passphrase` line to `profiles.yml`. The section-1.1 key is generated unencrypted (`-nocrypt`), so none is needed here.
> dbt reads OS environment variables directly — it does **not** load `ingestion/.env` (that file is only for the Python ingestion via `python-dotenv`).

### 3.3 Verify the connection

`profiles.yml` sits inside the project dir (not `~/.dbt`), so pass `--profiles-dir .` and run from `de-pipeline/dbt/`:

```cmd
cd dbt
dbt deps --profiles-dir .            :: install dbt_utils (first time only)
dbt debug --profiles-dir .           :: checks creds + DEV_DB reachability
```

```bash
cd dbt
dbt deps --profiles-dir .            # install dbt_utils (first time only)
dbt debug --profiles-dir .           # checks creds + DEV_DB reachability
```

### 3.4 Build & test

`dbt build` runs the models, the SCD2 snapshot, and all tests together (a failing PK/FK/type test fails the build):

```bash
dbt build --target dev --profiles-dir .
```

Handy subsets while developing (all from `de-pipeline/dbt/`, all take `--profiles-dir .`):

| Command | What it does |
|---|---|
| `dbt run --select silver` | build only the Silver staging models |
| `dbt run --select gold` | build only the Gold dims + facts |
| `dbt build --select stg_coingecko_snapshot+` | a model **and** everything downstream of it |
| `dbt snapshot` | refresh the SCD Type-2 `snap_coin` snapshot on its own |
| `dbt test` | run the PK / FK / uniqueness / range tests only |
| `dbt docs generate` then `dbt docs serve` | browse the lineage graph + docs locally |

Everything targets `DEV_DB` only. Promotion to `QA_DB` / `PROD_DB` is a later step and isn't part of local development.

### 3.5 Full refresh — reload the whole pipeline (truncate & load)

Normal runs are **incremental** — each model processes only new rows. When you need to rebuild everything from source (schema change, backfilled/corrected raw data, or just a clean slate), pass `--full-refresh`. dbt then **drops and recreates** each incremental table and reloads it in full from the landing tables — the truncate-and-load of the whole pipeline:

```cmd
:: Windows cmd — from de-pipeline/dbt
dbt build --full-refresh --target dev --profiles-dir .
```

```bash
# Linux / macOS — from de-pipeline/dbt
dbt build --full-refresh --target dev --profiles-dir .
```

This fully reloads the incremental models (`stg_coingecko_snapshot`, `stg_coingecko_history`, `int_coin_daily`, `fct_market_snapshot`, `fct_daily_market`); the plain tables (`dim_coin`, `dim_date`) are rebuilt every run anyway. Scope it to part of the DAG the same way as a normal build, e.g. `dbt build --full-refresh --select silver+`.

**Snapshots are the exception.** `--full-refresh` does **not** rebuild the SCD Type-2 `snap_coin` snapshot — its history is meant to persist across runs. If you want a *true* from-scratch reset that also wipes SCD2 history, drop the snapshot first (a `reset_snapshots` operation macro is included), then full-refresh:

```bash
dbt run-operation reset_snapshots --target dev --profiles-dir .   # drops snap_coin — discards SCD2 history
dbt build --full-refresh --target dev --profiles-dir .            # rebuilds everything, snapshot re-initializes
```

> Skip `reset_snapshots` unless you specifically want to lose the accumulated coin-attribute history — a normal `--full-refresh` already reloads all the actual data.

### 3.6 Schedule the dbt run with cron-job.org (external cron)

The scheduled dbt run is `.github/workflows/dbt-run.yml`. It has **no GitHub `schedule:` trigger** — GitHub's built-in cron is delayed/dropped under load — so an external scheduler, **cron-job.org**, fires it on time by calling the GitHub REST API (`workflow_dispatch`). The same workflow also runs on PRs and merges (see the project plan's Step 5b); cron-job.org only drives the daily run.

**1. Create a GitHub token (PAT).** GitHub → Settings → Developer settings → **Fine-grained personal access tokens** → Generate:
- **Repository access:** only `coin-dwh-ml`.
- **Permissions → Actions: Read and write** (this is what allows dispatching a workflow).
- Copy the token — you'll paste it into cron-job.org, nowhere else. (A classic PAT also works; it needs the `workflow` scope.)

**2. (optional) Confirm it works** from your machine before wiring the scheduler — a `204 No Content` means success and a run should appear in the Actions tab:

```bash
curl -i -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer <PAT>" \
  -H "X-GitHub-Api-Version: 2022-11-28" \
  https://api.github.com/repos/<owner>/coin-dwh-ml/actions/workflows/dbt-run.yml/dispatches \
  -d '{"ref":"main"}'
```

**3. Create the cron-job.org job** (Console → **Create cronjob**):

| Field | Value |
|---|---|
| Title | `dbt build (dev)` |
| URL | `https://api.github.com/repos/<owner>/coin-dwh-ml/actions/workflows/dbt-run.yml/dispatches` |
| Schedule | e.g. every day at **08:30**, timezone **Asia/Bangkok** (cron-job.org has a timezone selector — no UTC math) |
| Request method | **POST** (under "Advanced" / request settings) |
| Request body | `{"ref":"main"}` |
| Headers | `Accept: application/vnd.github+json`  ·  `Authorization: Bearer <PAT>`  ·  `X-GitHub-Api-Version: 2022-11-28`  ·  `Content-Type: application/json` |

Notes:
- **`ref` must be the default branch** (`main`) and `dbt-run.yml` must exist on it — `workflow_dispatch` only dispatches from the default branch.
- GitHub returns **`204 No Content`** on success (empty body); treat any `2xx` as OK in cron-job.org. A `401`/`403` means the PAT is wrong or missing the Actions permission; `404` usually means a wrong `<owner>`/repo/workflow filename or a token without access.
- cron-job.org sends a `User-Agent` automatically (GitHub requires one); if you ever get a `403` about a missing User-Agent, add `User-Agent: cron-job.org` to the headers.
- The workflow authenticates to Snowflake with the same `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_DEV_SVC_USER` / `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` repo secrets used elsewhere — those must be set (project plan Step 6) for the dispatched run to succeed.
