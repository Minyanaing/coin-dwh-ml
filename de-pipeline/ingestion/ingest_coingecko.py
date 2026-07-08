import argparse
import csv
import json
import os
from datetime import datetime, timezone

import requests
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

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

def _round5(value):
    """Round numeric API values to 5 decimals; pass None through untouched."""
    return round(value, 5) if value is not None else None

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
            "current_price": _round5(c.get("current_price")),
            "market_cap": _round5(c.get("market_cap")),
            "total_volume": _round5(c.get("total_volume")),
            "price_change_24h": _round5(c.get("price_change_24h")),
            "price_change_pct_24h": _round5(c.get("price_change_percentage_24h")),
            "high_24h": _round5(c.get("high_24h")),
            "low_24h": _round5(c.get("low_24h")),
            "circulating_supply": _round5(c.get("circulating_supply")),
            "ath": _round5(c.get("ath")),
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

def save_to_json(coins: list, fetched_at: str) -> str:
    # Dump the full, untouched CoinGecko response — every field, no rounding or
    # column selection (unlike CSV/Snowflake, which use the build_rows subset).
    os.makedirs(config.CSV_OUTPUT_DIR, exist_ok=True)
    stamp = fetched_at.replace("+00:00", "Z").replace(":", "").replace("-", "")
    path = os.path.join(config.CSV_OUTPUT_DIR, f"coingecko_raw_{stamp}.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(coins, f, indent=2)

    print(f"[local] Wrote {len(coins)} coins (full raw fields) to {path}")
    return path

def _load_private_key_der():
    """Return the private key as DER bytes for key-pair auth, or None if no
    key is configured (then password auth is used). Accepts an inline PEM
    (SNOWFLAKE_PRIVATE_KEY) or a path to a .p8 file (SNOWFLAKE_PRIVATE_KEY_PATH)."""
    pem = config.SNOWFLAKE_PRIVATE_KEY
    if not pem and config.SNOWFLAKE_PRIVATE_KEY_PATH:
        with open(config.SNOWFLAKE_PRIVATE_KEY_PATH, "r", encoding="utf-8") as f:
            pem = f.read()
    if not pem:
        return None

    passphrase = config.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    key = serialization.load_pem_private_key(
        pem.encode(),
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def get_snowflake_conn():
    kwargs = dict(
        account   = config.SNOWFLAKE_ACCOUNT,
        user      = config.SNOWFLAKE_USER,
        role      = config.SNOWFLAKE_ROLE,
        warehouse = config.SNOWFLAKE_WAREHOUSE,
        database  = config.SNOWFLAKE_DATABASE,
        schema    = config.SNOWFLAKE_SCHEMA,
    )
    private_key_der = _load_private_key_der()
    if private_key_der is not None:
        kwargs["private_key"] = private_key_der   # key-pair auth (e.g. DEVELOPER_SVC)
    else:
        kwargs["password"] = config.SNOWFLAKE_PASSWORD   # password auth
    return snowflake.connector.connect(**kwargs)

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
