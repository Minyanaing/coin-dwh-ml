-- snowflake/ingest_task.sql
-- DISABLED — requires an External Access Integration, which needs the
-- global CREATE INTEGRATION privilege (ACCOUNTADMIN only). Not available
-- on a Snowflake trial account. Production ingestion is instead
-- ingest_coingecko.py, run by the standalone de-ingest.yml workflow.
--
-- This file is intentionally NOT passed to run_sql.py in infra-deploy.yml
-- (see .github/workflows/infra-deploy.yml) — nothing here ever executes.
--
-- To re-enable once the account supports External Access Integrations:
--   1. As ACCOUNTADMIN, create the network rule + integration + grant
--      USAGE to SYSADMIN (see README.md "1.5 One-time ACCOUNTADMIN setup
--      for production ingestion" / the project plan's Step 3b for the
--      exact statements this file used to contain before being disabled).
--   2. Uncomment everything below.
--   3. Add "ingest_task.sql" back to run_sql.py's file list in
--      infra-deploy.yml.
--   4. Decide whether to keep the de-ingest.yml workflow running too, or
--      retire it in favor of this Task — running both would double-insert
--      rows into CRYPTO_DB.STG.COINGECKO_RAW.

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
