{{
  config(
    materialized='incremental',
    unique_key=['coin_id', 'price_date'],
    on_schema_change='sync_all_columns'
  )
}}

-- Daily grain per coin. The 6-hourly snapshot feed can land several extracts in
-- a single day, so for every (coin, price_date) we compute BOTH:
--   * the day's AVERAGE across all of that day's extracts, and
--   * the value of the LATEST extract of the day,
-- plus extract_count (how many extracts fed the average).
-- The historical backfill has a single value per day, so for a history-only day
-- avg == latest and extract_count = 1 (one extract -> its own average).
--
-- When a day exists in both feeds the intraday snapshot wins (it's what the
-- averages are about); history only fills days that have no snapshots.
--
-- Incremental: recompute only the (coin_id, price_date) days with new source
-- rows since the last run (`_synced_at` watermark), each from ALL of that day's
-- rows — so the average and the latest are always computed over the full day.

with snap as (
    select
        coin_id, symbol, name, price_date,
        price_usd, market_cap_usd, volume_24h_usd, fetched_at
    from {{ ref('stg_coingecko_snapshot') }}
),

hist as (
    select
        coin_id, symbol, name, price_date,
        price_usd, market_cap_usd, volume_usd, fetched_at
    from {{ ref('stg_coingecko_history') }}
),

{% if is_incremental() %}
touched as (
    select distinct coin_id, price_date
    from (
        select coin_id, price_date, fetched_at from snap
        union all
        select coin_id, price_date, fetched_at from hist
    ) x
    where fetched_at > (select coalesce(max(_synced_at), '1900-01-01'::timestamp_ntz) from {{ this }})
),
{% endif %}

snapshot_daily as (
    select
        coin_id,
        price_date,
        max_by(symbol, fetched_at)               as symbol,
        max_by(name, fetched_at)                 as name,
        count(*)                                 as extract_count,
        round(avg(price_usd), 8)                 as avg_price_usd,
        round(avg(market_cap_usd), 2)            as avg_market_cap_usd,
        round(avg(volume_24h_usd), 2)            as avg_volume_usd,
        max_by(price_usd, fetched_at)            as latest_price_usd,
        max_by(market_cap_usd, fetched_at)       as latest_market_cap_usd,
        max_by(volume_24h_usd, fetched_at)       as latest_volume_usd,
        max(fetched_at)                          as _synced_at
    from snap
    {% if is_incremental() %}
    where (coin_id, price_date) in (select coin_id, price_date from touched)
    {% endif %}
    group by coin_id, price_date
),

history_daily as (
    -- single value per (coin, day): avg == latest, one "extract"
    select
        coin_id,
        price_date,
        symbol,
        name,
        1                as extract_count,
        price_usd        as avg_price_usd,
        market_cap_usd   as avg_market_cap_usd,
        volume_usd       as avg_volume_usd,
        price_usd        as latest_price_usd,
        market_cap_usd   as latest_market_cap_usd,
        volume_usd       as latest_volume_usd,
        fetched_at       as _synced_at
    from hist
    {% if is_incremental() %}
    where (coin_id, price_date) in (select coin_id, price_date from touched)
    {% endif %}
),

unioned as (
    select
        coin_id, price_date, symbol, name, extract_count,
        avg_price_usd, avg_market_cap_usd, avg_volume_usd,
        latest_price_usd, latest_market_cap_usd, latest_volume_usd,
        _synced_at, 'snapshot' as source_name
    from snapshot_daily
    union all
    select
        coin_id, price_date, symbol, name, extract_count,
        avg_price_usd, avg_market_cap_usd, avg_volume_usd,
        latest_price_usd, latest_market_cap_usd, latest_volume_usd,
        _synced_at, 'history' as source_name
    from history_daily
),

prioritized as (
    select
        *,
        row_number() over (
            partition by coin_id, price_date
            order by case when source_name = 'snapshot' then 0 else 1 end
        ) as _priority
    from unioned
)

select
    coin_id,
    symbol,
    name,
    price_date,
    extract_count,
    avg_price_usd,
    avg_market_cap_usd,
    avg_volume_usd,
    latest_price_usd,
    latest_market_cap_usd,
    latest_volume_usd,
    source_name,
    _synced_at
from prioritized
where _priority = 1
