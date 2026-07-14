import os

from dotenv import load_dotenv

load_dotenv()  # loads ingestion/.env if present — never commit that file

# "local" writes a CSV under CSV_OUTPUT_DIR; "snowflake" loads into Snowflake
INGEST_MODE = os.getenv("INGEST_MODE", "local")

CSV_OUTPUT_DIR = os.getenv("CSV_OUTPUT_DIR", "data")

# Curated default coin set — top market-cap coins plus the major stablecoins.
# Both the daily snapshot (ingest_coingecko.py) and the historical backfill
# (backfill_coingecko_history.py) fetch ONLY these coins. Keeping the set small
# and fixed makes runs deterministic and cheap on CoinGecko's free tier (the
# backfill makes one API call per coin). Override with a comma-separated
# COIN_IDS env var of CoinGecko coin ids.
_DEFAULT_COIN_IDS = [
    # top coins by market cap (non-stable)
    "bitcoin",
    "ethereum",
    "binancecoin",
    "solana",
    "ripple",
    "dogecoin",
    "cardano",
    "tron",
    "avalanche-2",
    "chainlink",
    # major stablecoins
    "tether",
    "usd-coin",
    "dai",
]
COIN_IDS = [
    c.strip() for c in os.getenv("COIN_IDS", ",".join(_DEFAULT_COIN_IDS)).split(",") if c.strip()
]

# Start date for the manual historical backfill (backfill_coingecko_history.py).
# The end date is always the run date. ISO format: YYYY-MM-DD.
# Default is within the last 365 days so it works on the keyless CoinGecko API.
HISTORY_START_DATE = os.getenv("HISTORY_START_DATE", "2026-01-01")

# CoinGecko API key for the historical backfill. The keyless/Demo API only
# serves the past 365 days of market_chart data, so reaching further back
# needs a PAID plan key with PLAN=pro. Leave unset for the daily snapshot
# and start dates within the last 365 days, which work keyless.
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_API_PLAN = os.getenv("COINGECKO_API_PLAN", "demo")  # "demo" or "pro"

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")

# Key-pair auth (used by CI, which reuses the DEVELOPER_SVC key pair). Provide
# EITHER an inline PEM (SNOWFLAKE_PRIVATE_KEY, how GitHub Actions passes the
# secret) OR a path to a .p8 file (SNOWFLAKE_PRIVATE_KEY_PATH, nicer locally).
# If neither is set, get_snowflake_conn() falls back to SNOWFLAKE_PASSWORD.
SNOWFLAKE_PRIVATE_KEY = os.getenv("SNOWFLAKE_PRIVATE_KEY")
SNOWFLAKE_PRIVATE_KEY_PATH = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")

# DEVELOPER_SVC's usable role is SYSADMIN (it does not hold CRYPTO_PIPELINE_ROLE),
# so CI overrides SNOWFLAKE_ROLE=SYSADMIN. SYSADMIN owns CRYPTO_DB.STG and can
# insert into the landing tables.
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "CRYPTO_PIPELINE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "CRYPTO_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "CRYPTO_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "STG")
