import csv
import os
from datetime import datetime, timezone

import requests
import snowflake.connector

import config

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
# Fetch only the curated set from config (top coins + stablecoins) via the
# `ids` param, rather than the top N by market cap.
PARAMS = {
    "vs_currency": "usd",
    "ids": ",".join(config.COIN_IDS),
    "order": "market_cap_desc",
    "per_page": len(config.COIN_IDS),
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
