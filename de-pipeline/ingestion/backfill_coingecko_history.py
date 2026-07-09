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

import config
from fetch_data import REQUEST_SLEEP_SECONDS, fetch_coins, fetch_history
from snowflake_connection import get_snowflake_conn
from transforms import round5

FIELDNAMES = [
    "id", "symbol", "name", "price_date",
    "price", "market_cap", "total_volume", "fetched_at",
]


def _to_unix(dt: datetime) -> int:
    return int(dt.timestamp())


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
            "price": round5(prices.get(day)),
            "market_cap": round5(market_caps.get(day)),
            "total_volume": round5(total_volumes.get(day)),
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
