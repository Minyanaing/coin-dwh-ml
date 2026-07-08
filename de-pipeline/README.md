# Crypto Market Pipeline

Daily crypto market data pipeline: CoinGecko → Snowflake (landing → dev/qa/prod via dbt) → GitHub Actions.
Full design doc: [`../.claude/crypto-de-pipeline-project-plan.md`](../.claude/crypto-de-pipeline-project-plan.md).

---

## 1. Create a developer service account (key-pair auth)

This account is for **infra setup** — applying `snowflake/setup.sql`, `streams.sql`, `tasks.sql`, and `ingest_task.sql`, all done automatically by `infra-deploy.yml` (section 1.3–1.4) rather than run by hand — not for the ingestion pipeline itself (that uses `CRYPTO_PIPELINE_ROLE`, created *by* this account). Key-pair authentication is used instead of a password so nothing secret ever needs to be typed into a SQL client or stored as a plaintext password.

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
  RSA_PUBLIC_KEY = '<contents of rsa_key.pub, header/footer/newlines removed>'
  DEFAULT_ROLE = SYSADMIN
  DEFAULT_WAREHOUSE = CRYPTO_WH
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT = 'Infra CI/CD service account (infra-deploy.yml) — key-pair auth only, no interactive login';

GRANT ROLE SYSADMIN  TO USER DEVELOPER_SVC;  -- create/manage warehouses, databases, schemas, tables
GRANT ROLE USERADMIN TO USER DEVELOPER_SVC;  -- create/manage CRYPTO_PIPELINE_ROLE

-- Without this, ALTER TASK ... RESUME fails for every task SYSADMIN owns
-- (streams.sql/tasks.sql from Step 2, ingest_task.sql from Step 3b) —
-- EXECUTE TASK is a global privilege only ACCOUNTADMIN holds by default.
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
```

`TYPE = SERVICE` marks this as a non-human/programmatic account — Snowflake blocks password-based login for it entirely (key-pair or OAuth only), so there's no interactive-login credential to leak or rotate, which fits an account that only ever authenticates from `infra-deploy.yml`.

Snowflake splits these privileges across two built-in roles — `SYSADMIN` cannot run `CREATE ROLE` on its own, and `USERADMIN` cannot create warehouses/databases — so the dev account needs both. `snowflake/setup.sql` switches between them itself (`USE ROLE SYSADMIN` / `USE ROLE USERADMIN`) as it creates each kind of object. `EXECUTE TASK` is a separate global privilege from either of those — without it, any `ALTER TASK ... RESUME` run as `SYSADMIN` fails, so it's granted here once rather than being rediscovered as a bug later.

### 1.3 Wire the service account into GitHub Actions (`infra-deploy.yml`)

`infra-deploy.yml` applies `setup.sql`/`streams.sql`/`tasks.sql`/`ingest_task.sql` automatically, authenticating as `DEVELOPER_SVC` — there's no need to run any of them by hand. Add two repository secrets (**Settings → Secrets and variables → Actions**):

| Secret | Value |
|---|---|
| `SNOWFLAKE_DEV_SVC_USER` | `DEVELOPER_SVC` |
| `SNOWFLAKE_DEV_SVC_PRIVATE_KEY` | the full contents of `.keys/rsa_key.p8`, including the `-----BEGIN/END PRIVATE KEY-----` lines |

`SNOWFLAKE_ACCOUNT` is shared with `de-deploy.yml` (Step 5) and only needs to be added once.

### 1.4 Apply the infra

With the secrets above in place, either:

- Push a change under `de-pipeline/snowflake/**` to `main`, or
- Trigger it manually: **GitHub repo → Actions → Infra Deploy → Run workflow**

This creates `CRYPTO_WH`, all four databases (`CRYPTO_DB`, `DEV_DB`, `QA_DB`, `PROD_DB`), their schemas, the raw landing table, and `CRYPTO_PIPELINE_ROLE` — the role dbt/CI use day-to-day. All four scripts are safe to re-run (`CREATE ... IF NOT EXISTS`), so repeat deploys are idempotent — no destructive drops or offset resets.

**First run only:** the `ingest_task.sql` step will fail with an "integration does not exist" error — that's expected, since it depends on section 1.5 below, which hasn't run yet. `setup.sql`/`streams.sql`/`tasks.sql` still apply successfully before that failure; re-run the workflow after completing 1.5 and it picks up cleanly.

Grant `CRYPTO_PIPELINE_ROLE` to whatever user your dbt/CI authenticates as:

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

### 1.5 One-time ACCOUNTADMIN setup for production ingestion (Snowflake Task)

Production ingestion (`snowflake/ingest_task.sql`) calls the CoinGecko API from *inside* Snowflake, which requires an **External Access Integration** — an account-level object. Creating one requires the global `CREATE INTEGRATION` privilege, which only `ACCOUNTADMIN` holds by default and which we deliberately do **not** grant to `DEVELOPER_SVC` / `SYSADMIN` (unlike `EXECUTE TASK` above, this is a rarely-changed, high-privilege capability better left as a manual, audited step than automated in CI).

Run this once, manually, as `ACCOUNTADMIN` — **after** `setup.sql` has already run (section 1.4), since it creates an object inside `CRYPTO_DB.STG`:

```sql
-- Run once as ACCOUNTADMIN, after snowflake/setup.sql has been applied

-- Egress allowlist: only CoinGecko's public REST API, nothing else
CREATE NETWORK RULE IF NOT EXISTS CRYPTO_DB.STG.COINGECKO_NETWORK_RULE
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('api.coingecko.com');

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS COINGECKO_ACCESS_INTEGRATION
  ALLOWED_NETWORK_RULES = (CRYPTO_DB.STG.COINGECKO_NETWORK_RULE)
  ENABLED = TRUE;

-- SYSADMIN creates and owns the procedure/task in ingest_task.sql, and
-- needs USAGE on the integration to reference it in CREATE PROCEDURE.
GRANT USAGE ON INTEGRATION COINGECKO_ACCESS_INTEGRATION TO ROLE SYSADMIN;
```

Once this exists, re-run `infra-deploy.yml` (push or manual trigger, same as 1.4) — `ingest_task.sql` now applies cleanly as ordinary `SYSADMIN` work. If you ever need to allow a different host, re-run the `CREATE NETWORK RULE` block manually; nothing in CI/CD can do that for you by design.

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

Output: `ingestion/data/coingecko_raw_<timestamp>.csv` — open it and confirm the 50 rows of CoinGecko data look right.

### 2.3 Switch to loading into Snowflake (manual verification only)

This is for manually checking the write path against a dev schema by hand — it is **not** how production data gets loaded. Production ingestion runs on its own schedule as a Snowflake Task (`snowflake/ingest_task.sql`, see section 1.5); nothing ever runs this script automatically.

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
