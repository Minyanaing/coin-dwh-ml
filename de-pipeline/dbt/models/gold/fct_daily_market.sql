{{
  config(
    materialized='incremental',
    unique_key='daily_market_sk',
    on_schema_change='sync_all_columns'
  )
}}

-- Fact: one row per coin per day, from the unified daily grain (int_coin_daily).
-- Carries both the day's AVERAGE across its 6-hourly extracts and the value of
-- the LATEST extract, plus how many extracts fed the average (extract_count).
-- The day-over-day return is computed on the latest (close) price with lag()
-- over a bounded recent window (lag_lookback_days) — enough to see each
-- reconsolidated day's prior day, so the return is correct without a full scan.
-- coin_sk is resolved via an SCD Type-2 range join to dim_coin.

{% set lag_lookback_days = 3 %}

with daily as (

    -- Bounded read (not a full scan): recent days + a short lookback so the lag()
    -- below still sees each reconsolidated day's prior day. The final _synced_at
    -- filter decides which of these rows actually get written.
    -- Caveat: a backfill landing a price_date older than the lookback window is
    -- not reprocessed here — widen lag_lookback_days (or --full-refresh) for that.
    select * from {{ ref('int_coin_daily') }}
    {% if is_incremental() %}
    where price_date >= (
        select dateadd(day, -{{ lag_lookback_days }}, coalesce(max(price_date), '1900-01-01'::date))
        from {{ this }}
    )
    {% endif %}

),

with_returns as (

    select
        coin_id,
        price_date,
        extract_count,
        avg_price_usd,
        avg_market_cap_usd,
        avg_volume_usd,
        latest_price_usd,
        latest_market_cap_usd,
        latest_volume_usd,
        _synced_at,
        lag(latest_price_usd) over (
            partition by coin_id order by price_date
        ) as prev_latest_price_usd
    from daily

)

select
    {{ dbt_utils.generate_surrogate_key(['wr.coin_id', 'wr.price_date']) }} as daily_market_sk,  -- PK
    dc.coin_sk,                                                -- FK -> dim_coin (version in effect)
    to_number(to_char(wr.price_date, 'YYYYMMDD'))  as date_sk, -- FK -> dim_date
    wr.price_date,
    wr.extract_count,
    -- day average across the extracts
    wr.avg_price_usd,
    wr.avg_market_cap_usd,
    wr.avg_volume_usd,
    -- value of the latest extract of the day
    wr.latest_price_usd,
    wr.latest_market_cap_usd,
    wr.latest_volume_usd,
    wr.prev_latest_price_usd,
    round(
        (wr.latest_price_usd - wr.prev_latest_price_usd) / nullif(wr.prev_latest_price_usd, 0) * 100, 4
    ) as daily_return_pct,
    wr._synced_at   -- incremental watermark
from with_returns wr
left join {{ ref('dim_coin') }} dc
    on  wr.coin_id = dc.coin_id
    and wr.price_date >= dc.valid_from::date
    and wr.price_date <  dc.valid_to::date

{% if is_incremental() %}
where wr._synced_at > (select coalesce(max(_synced_at), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
