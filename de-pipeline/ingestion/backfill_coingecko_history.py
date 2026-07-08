"""Manual one-off backfill of daily historical market data for the curated coin set.

The coin set (top coins + stablecoins) is defined by config.COIN_IDS — the same
set the daily snapshot uses.

Fetches a daily price / market-cap / volume series for each coin from
HISTORY_START_DATE (default 2026-01-01) through the run date, and appends it to
CRYPTO_DB.STG.COINGECKO_HISTORY_RAW (INGEST_MODE=snowflake) or a CSV
(INGEST_MODE=local, the default — no Snowflake credentials needed).

Unlike ingest_coingecko.py (the daily snapshot), this is intended to be run by
hand — locally, or via the manual-only de-ingest-history.yml workflow.
"""

import csv
import os
import time
from datetime import datetime, timezone

import requests

import config
from ingest_coingecko import fetch_coins, get_snowflake_conn

FIELDNAMES = [
    "id", "symbol", "name", "price_date",
    "price", "market_cap", "total_volume", "fetched_at",
]

# The public CoinGecko API rate-limits aggressively; go gently and back off on 429.
REQUEST_SLEEP_SECONDS = 2.5
MAX_RETRIES = 5


def _to_unix(dt: datetime) -> int:
    return int(dt.timestamp())


def _base_and_headers() -> tuple[str, dict]:
    """Pick the CoinGecko host + auth header based on config.

    - No key         -> public host, no header (capped at the past 365 days).
    - Demo key        -> public host, x-cg-demo-api-key (still capped at 365 days).
    - Pro  key        -> pro host, x-cg-pro-api-key (full history from 2013).
    """
    key = config.COINGECKO_API_KEY
    if not key:
        return "https://api.coingecko.com/api/v3", {}
    if config.COINGECKO_API_PLAN == "pro":
        return "https://pro-api.coingecko.com/api/v3", {"x-cg-pro-api-key": key}
    return "https://api.coingecko.com/api/v3", {"x-cg-demo-api-key": key}


def fetch_history(coin_id: str, start_ts: int, end_ts: int) -> dict:
    """Daily market_chart for one coin. A range wider than 90 days makes
    CoinGecko return daily-granularity points automatically."""
    base, headers = _base_and_headers()
    url = f"{base}/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": start_ts, "to": end_ts}

    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, params=params, headers=headers, timeout=60)
        if resp.status_code == 429:
            wait = REQUEST_SLEEP_SECONDS * 2 ** attempt
            print(f"  rate-limited on {coin_id}, retry {attempt}/{MAX_RETRIES} in {wait:.0f}s")
            time.sleep(wait)
            continue
        if resp.status_code in (401, 403):
            raise SystemExit(
                f"\nCoinGecko returned {resp.status_code} for '{coin_id}' over "
                f"{config.HISTORY_START_DATE}..today.\n"
                "The keyless/Demo API only serves the past 365 days of market_chart "
                "history, so a start date older than that is rejected.\n"
                "Either move HISTORY_START_DATE within the last 365 days, or set "
                "COINGECKO_API_KEY and COINGECKO_API_PLAN=pro (a PAID plan) for full history."
            )
        resp.raise_for_status()
        return resp.json()

    resp.raise_for_status()  # exhausted retries — surface the last 429
    return {}


def build_history_rows(coin: dict, payload: dict, fetched_at: str) -> list[dict]:
    def by_date(pairs):
        # CoinGecko returns [timestamp_ms, value]; collapse to one value per UTC day
        # (last observation wins if the final partial day has intraday points).
        out = {}
        for ts_ms, value in pairs or []:
            day = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date().isoformat()
            out[day] = value
        return out

    prices = by_date(payload.get("prices"))
    market_caps = by_date(payload.get("market_caps"))
    total_volumes = by_date(payload.get("total_volumes"))

    return [
        {
            "id": coin["id"],
            "symbol": coin["symbol"],
            "name": coin["name"],
            "price_date": day,
            "price": prices.get(day),
            "market_cap": market_caps.get(day),
            "total_volume": total_volumes.get(day),
            "fetched_at": fetched_at,
        }
        for day in sorted(prices)
    ]


def save_history_to_snowflake(conn, rows: list[dict]) -> None:
    if not rows:
        return
    insert_sql = f"""
        INSERT INTO {config.SNOWFLAKE_DATABASE}.{config.SNOWFLAKE_SCHEMA}.COINGECKO_HISTORY_RAW (
            id, symbol, name, price_date, price, market_cap, total_volume, fetched_at
        ) VALUES (
            %(id)s, %(symbol)s, %(name)s, %(price_date)s,
            %(price)s, %(market_cap)s, %(total_volume)s, %(fetched_at)s
        )
    """
    cur = conn.cursor()
    cur.executemany(insert_sql, rows)
    conn.commit()


def main():
    start_dt = datetime.fromisoformat(config.HISTORY_START_DATE).replace(tzinfo=timezone.utc)
    end_dt = datetime.now(timezone.utc)
    fetched_at = end_dt.isoformat()
    start_ts, end_ts = _to_unix(start_dt), _to_unix(end_dt)

    coins = fetch_coins()
    print(f"Backfilling {len(coins)} coins from {config.HISTORY_START_DATE} to {end_dt.date()} "
          f"(mode={config.INGEST_MODE})")

    conn = None
    csv_file = None
    writer = None
    csv_path = None
    total = 0

    try:
        if config.INGEST_MODE == "snowflake":
            conn = get_snowflake_conn()
        else:
            os.makedirs(config.CSV_OUTPUT_DIR, exist_ok=True)
            stamp = fetched_at.replace("+00:00", "Z").replace(":", "").replace("-", "")
            csv_path = os.path.join(config.CSV_OUTPUT_DIR, f"coingecko_history_{stamp}.csv")
            csv_file = open(csv_path, "w", newline="", encoding="utf-8")
            writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
            writer.writeheader()

        for i, coin in enumerate(coins, 1):
            payload = fetch_history(coin["id"], start_ts, end_ts)
            rows = build_history_rows(coin, payload, fetched_at)

            if config.INGEST_MODE == "snowflake":
                save_history_to_snowflake(conn, rows)
            else:
                writer.writerows(rows)

            total += len(rows)
            print(f"[{i}/{len(coins)}] {coin['id']}: {len(rows)} daily rows")
            time.sleep(REQUEST_SLEEP_SECONDS)
    finally:
        if conn is not None:
            conn.close()
        if csv_file is not None:
            csv_file.close()

    dest = ("CRYPTO_DB.STG.COINGECKO_HISTORY_RAW"
            if config.INGEST_MODE == "snowflake" else csv_path)
    print(f"Done — {total} rows -> {dest}")


if __name__ == "__main__":
    main()
