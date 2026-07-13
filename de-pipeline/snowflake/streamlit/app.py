"""Crypto — Daily Prices. Single chart from GOLD_CRYPTO.FCT_DAILY_MARKET.
Reads PROD_DB (clean, promoted data). Pick coins in the sidebar; each is a line."""

import os

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Crypto — Daily Prices", layout="wide")


@st.cache_resource
def get_session():
    # In Snowflake an active session exists; locally build one from env vars.
    try:
        from snowflake.snowpark.context import get_active_session
        return get_active_session()
    except Exception:
        from snowflake.snowpark import Session
        return Session.builder.configs({
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "role": os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "CRYPTO_WH"),
            "private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        }).create()


session = get_session()

DATABASE = "PROD_DB"
SCHEMA = "GOLD_CRYPTO"

st.title("📈 Daily Prices")


@st.cache_data(ttl=600)
def load_symbols(db: str) -> list[str]:
    rows = session.sql(
        f"select distinct symbol from {db}.{SCHEMA}.dim_coin "
        f"where is_current order by symbol"
    ).to_pandas()
    return rows["SYMBOL"].tolist()


@st.cache_data(ttl=600)
def load_daily_prices(db: str, symbols: list[str]) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in symbols)
    return session.sql(
        f"""
        select f.price_date, c.symbol, f.latest_price_usd as price_usd
        from {db}.{SCHEMA}.fct_daily_market f
        join {db}.{SCHEMA}.dim_coin c on f.coin_sk = c.coin_sk
        where c.symbol in ({in_list})
        order by f.price_date
        """
    ).to_pandas()


symbols = load_symbols(DATABASE)
if not symbols:
    st.warning(f"No coins in {DATABASE}.{SCHEMA}.dim_coin — has dbt built prod yet?")
    st.stop()

preferred = [s for s in ("BTC", "ETH", "SOL") if s in symbols]
selected = st.sidebar.multiselect("Coins", symbols, default=preferred or symbols[:3])

df = load_daily_prices(DATABASE, selected)
if df.empty:
    st.info("Pick at least one coin to plot.")
else:
    # wide frame (one column per coin) -> one line each, with a legend
    chart = df.pivot_table(index="PRICE_DATE", columns="SYMBOL", values="PRICE_USD").sort_index()
    st.line_chart(chart)
    st.caption(f"Source: {DATABASE}.{SCHEMA}.fct_daily_market · daily close (USD)")
