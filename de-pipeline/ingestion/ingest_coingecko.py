import argparse
import csv
import json
import logging
import os
from datetime import datetime, timezone

import config
from fetch_data import fetch_coins
from snowflake_connection import get_snowflake_conn
from transforms import round5

logger = logging.getLogger(__name__)

FIELDNAMES = [
    "id",
    "symbol",
    "name",
    "current_price",
    "market_cap",
    "total_volume",
    "price_change_24h",
    "price_change_pct_24h",
    "high_24h",
    "low_24h",
    "circulating_supply",
    "ath",
    "fetched_at",
]


def build_rows(coins: list, fetched_at: str) -> list[dict]:
    return [
        {
            "id": c["id"],
            "symbol": c["symbol"],
            "name": c["name"],
            "current_price": round5(c.get("current_price")),
            "market_cap": round5(c.get("market_cap")),
            "total_volume": round5(c.get("total_volume")),
            "price_change_24h": round5(c.get("price_change_24h")),
            "price_change_pct_24h": round5(c.get("price_change_percentage_24h")),
            "high_24h": round5(c.get("high_24h")),
            "low_24h": round5(c.get("low_24h")),
            "circulating_supply": round5(c.get("circulating_supply")),
            "ath": round5(c.get("ath")),
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

    logger.info("[local] wrote %d rows to %s", len(rows), path)
    return path


def save_to_json(coins: list, fetched_at: str) -> str:
    # Dump the full, untouched CoinGecko response — every field, no rounding or
    # column selection (unlike CSV/Snowflake, which use the build_rows subset).
    os.makedirs(config.CSV_OUTPUT_DIR, exist_ok=True)
    stamp = fetched_at.replace("+00:00", "Z").replace(":", "").replace("-", "")
    path = os.path.join(config.CSV_OUTPUT_DIR, f"coingecko_raw_{stamp}.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(coins, f, indent=2)

    logger.info("[local] wrote %d coins (full raw fields) to %s", len(coins), path)
    return path


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
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)
        conn.commit()
        logger.info(
            "[snowflake] loaded %d rows into %s.%s.COINGECKO_RAW",
            len(rows),
            config.SNOWFLAKE_DATABASE,
            config.SNOWFLAKE_SCHEMA,
        )
    finally:
        conn.close()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Fetch CoinGecko markets for the curated coin set."
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Local output format (default: csv). 'json' writes the full raw "
        "CoinGecko response (all fields). Ignored when INGEST_MODE=snowflake.",
    )
    args = parser.parse_args()

    coins = fetch_coins()
    fetched_at = datetime.now(timezone.utc).isoformat()

    if config.INGEST_MODE == "snowflake":
        save_to_snowflake(build_rows(coins, fetched_at))
    elif args.format == "json":
        save_to_json(coins, fetched_at)
    else:
        save_to_csv(build_rows(coins, fetched_at), fetched_at)


if __name__ == "__main__":
    main()
