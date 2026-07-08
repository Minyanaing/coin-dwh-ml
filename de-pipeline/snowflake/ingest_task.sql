-- snowflake/ingest_task.sql
-- Requires one-time ACCOUNTADMIN setup — see README.md
-- "One-time ACCOUNTADMIN setup for production ingestion":
--   - EXECUTE TASK granted to SYSADMIN (needed to resume/run any task, this
--     one included, and also covers streams.sql/tasks.sql from Step 2)
--   - CRYPTO_DB.STG.COINGECKO_NETWORK_RULE + COINGECKO_ACCESS_INTEGRATION created
--   - USAGE on COINGECKO_ACCESS_INTEGRATION granted to SYSADMIN
-- Without those: CREATE PROCEDURE below fails because the integration it
-- references doesn't exist yet, and ALTER TASK ... RESUME fails because
-- SYSADMIN lacks EXECUTE TASK by default.

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
