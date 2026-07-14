"""Shared CoinGecko API access.

All HTTP calls to CoinGecko live here so both ingest_coingecko.py (daily
snapshot) and backfill_coingecko_history.py (historical backfill) fetch the
same way — same host/auth selection, same rate-limit handling.
"""

import logging
import time

import requests

import config

logger = logging.getLogger(__name__)


class CoinGeckoError(RuntimeError):
    """Non-retryable CoinGecko API error (e.g. an out-of-window 401/403).

    Raised so callers/entry points decide how to exit — the library never
    terminates the process itself.
    """


# The public CoinGecko API rate-limits aggressively; go gently and back off on 429.
REQUEST_SLEEP_SECONDS = 2.5
MAX_RETRIES = 5


def _base_and_headers() -> tuple[str, dict]:
    """CoinGecko host + auth header based on config.

    - No key   -> public host, no header (history capped at the past 365 days).
    - Demo key -> public host, x-cg-demo-api-key (still capped at 365 days).
    - Pro key  -> pro host, x-cg-pro-api-key (full history from 2013).
    """
    key = config.COINGECKO_API_KEY
    if not key:
        return "https://api.coingecko.com/api/v3", {}
    if config.COINGECKO_API_PLAN == "pro":
        return "https://pro-api.coingecko.com/api/v3", {"x-cg-pro-api-key": key}
    return "https://api.coingecko.com/api/v3", {"x-cg-demo-api-key": key}


def fetch_coins() -> list:
    """Current markets snapshot for the curated coin set (config.COIN_IDS)."""
    base, headers = _base_and_headers()
    params = {
        "vs_currency": "usd",
        "ids": ",".join(config.COIN_IDS),
        "order": "market_cap_desc",
        "per_page": len(config.COIN_IDS),
        "page": 1,
        "sparkline": False,
    }
    resp = requests.get(f"{base}/coins/markets", params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_history(coin_id: str, start_ts: int, end_ts: int) -> dict:
    """Daily market_chart for one coin over [start_ts, end_ts] (unix seconds).
    A range wider than 90 days makes CoinGecko return daily-granularity points
    automatically. Retries on 429; raises a clear error on 401/403 (out of the
    keyless 365-day window)."""
    base, headers = _base_and_headers()
    url = f"{base}/coins/{coin_id}/market_chart/range"
    params = {"vs_currency": "usd", "from": start_ts, "to": end_ts}

    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, params=params, headers=headers, timeout=60)
        if resp.status_code == 429:
            wait = REQUEST_SLEEP_SECONDS * 2**attempt
            logger.warning(
                "rate-limited on %s, retry %d/%d in %.0fs", coin_id, attempt, MAX_RETRIES, wait
            )
            time.sleep(wait)
            continue
        if resp.status_code in (401, 403):
            raise CoinGeckoError(
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
