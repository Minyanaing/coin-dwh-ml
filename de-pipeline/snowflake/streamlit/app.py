"""Crypto dashboard on GOLD_CRYPTO.FCT_DAILY_MARKET. Reads PROD_DB (clean data)."""

import datetime as dt
import os

import altair as alt
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
FCT = f"{DATABASE}.{SCHEMA}.fct_daily_market"
DIM = f"{DATABASE}.{SCHEMA}.dim_coin"


@st.cache_data(ttl=600)
def date_bounds():
    r = session.sql(f"select min(price_date) mn, max(price_date) mx from {FCT}").to_pandas()
    return r["MN"][0], r["MX"][0]


@st.cache_data(ttl=600)
def load_symbols() -> list[str]:
    r = session.sql(f"select distinct symbol from {DIM} where is_current order by symbol").to_pandas()
    return r["SYMBOL"].tolist()


@st.cache_data(ttl=600)
def load_cards(start, end) -> pd.DataFrame:
    # latest row per coin within the date range, highest price first
    return session.sql(f"""
        select symbol, price_usd, daily_return_pct from (
          select c.symbol, f.latest_price_usd as price_usd, f.daily_return_pct,
                 row_number() over (partition by c.coin_id order by f.price_date desc) rn
          from {FCT} f join {DIM} c on f.coin_sk = c.coin_sk
          where f.price_date between '{start}' and '{end}'
        ) where rn = 1
        order by price_usd desc
    """).to_pandas()


@st.cache_data(ttl=600)
def load_extremes() -> dict:
    # all-time high / low per coin (full history, ignores the date filter)
    r = session.sql(f"""
        select c.symbol, max(f.latest_price_usd) ath, min(f.latest_price_usd) atl
        from {FCT} f join {DIM} c on f.coin_sk = c.coin_sk
        group by c.symbol
    """).to_pandas()
    return {row.SYMBOL: (row.ATH, row.ATL) for row in r.itertuples()}


@st.cache_data(ttl=600)
def load_series(start, end, symbols: tuple) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    in_list = ", ".join("'" + s.replace("'", "''") + "'" for s in symbols)
    return session.sql(f"""
        select f.price_date, c.symbol, f.latest_price_usd as price_usd
        from {FCT} f join {DIM} c on f.coin_sk = c.coin_sk
        where c.symbol in ({in_list}) and f.price_date between '{start}' and '{end}'
        order by f.price_date
    """).to_pandas()


def price_str(v: float) -> str:
    return f"${v:,.2f}" if v is None or v >= 1 else f"${v:,.4f}"


st.title("📈 Daily Prices")

mn, mx = date_bounds()
if mn is None or pd.isna(mn):
    st.warning(f"No data in {FCT} — has dbt built prod yet?")
    st.stop()
mn_d, mx_d = pd.to_datetime(mn).date(), pd.to_datetime(mx).date()

# --- date range filter (drives the cards and the charts) ---
picked = st.date_input(
    "Date range",
    value=(max(mn_d, mx_d - dt.timedelta(days=90)), mx_d),
    min_value=mn_d,
    max_value=mx_d,
)
if not (isinstance(picked, (list, tuple)) and len(picked) == 2):
    st.stop()  # wait until both ends are picked
start, end = picked

# --- cards: latest price per coin (NOT affected by the sidebar selection) ---
st.subheader("Latest price by coin")
cards = load_cards(start, end)
if cards.empty:
    st.info("No prices in the selected date range.")
else:
    per_row = (len(cards) + 1) // 2   # everything in two rows
    for i in range(0, len(cards), per_row):
        chunk = cards.iloc[i:i + per_row]
        for col, (_, r) in zip(st.columns(per_row), chunk.iterrows()):
            ret = r["DAILY_RETURN_PCT"]
            delta = None if pd.isna(ret) else f"{ret:+.2f}%"
            col.metric(r["SYMBOL"], price_str(r["PRICE_USD"]), delta)

# --- sidebar: pick coins -> one chart each (does not affect the cards above) ---
all_symbols = load_symbols()
preferred = [s for s in ("BTC", "ETH", "SOL") if s in all_symbols]
selected = st.sidebar.multiselect("Coins", all_symbols, default=preferred or all_symbols[:3])

def coin_chart(sub: pd.DataFrame, ath, atl):
    # solid price line + dotted all-time-high / all-time-low reference lines
    d = sub[["PRICE_DATE"]].copy()
    d["Price"] = sub["PRICE_USD"]
    d["All-time high"] = ath
    d["All-time low"] = atl
    long = d.melt("PRICE_DATE", var_name="Series", value_name="USD")
    domain = ["Price", "All-time high", "All-time low"]
    return (
        alt.Chart(long)
        .mark_line()
        .encode(
            x=alt.X("PRICE_DATE:T", title=None),
            y=alt.Y("USD:Q", title=None),
            color=alt.Color(
                "Series:N",
                scale=alt.Scale(domain=domain, range=["#1f77b4", "#2ca02c", "#d62728"]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            strokeDash=alt.StrokeDash(
                "Series:N",
                scale=alt.Scale(domain=domain, range=[[1, 0], [3, 3], [3, 3]]),
                legend=None,
            ),
        )
        .properties(height=220)
    )


st.subheader("Price trend")
if not selected:
    st.info("Pick one or more coins in the sidebar.")
else:
    extremes = load_extremes()
    series = load_series(start, end, tuple(selected))
    per_row = min(len(selected), 4)   # 4 per row: 1 row for <=4, 2 rows for 5-8, etc.
    for i in range(0, len(selected), per_row):
        row_syms = selected[i:i + per_row]
        for col, sym in zip(st.columns(per_row), row_syms):
            with col:
                st.markdown(f"**{sym}**")
                sub = series[series["SYMBOL"] == sym]
                if sub.empty:
                    st.caption("no data in range")
                    continue
                ath, atl = extremes.get(sym, (None, None))
                st.altair_chart(coin_chart(sub, ath, atl), use_container_width=True)
