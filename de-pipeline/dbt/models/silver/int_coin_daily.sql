{{
  config(
    materialized='incremental',
    unique_key=['coin_id', 'price_date'],
    on_schema_change='sync_all_columns'
  )
}}

-- Unified daily grain per coin, combining both landing feeds:
--   * the historical backfill is the authoritative daily close;
--   * for recent days only covered by the 6-hourly snapshots, the day's close
--     is the last intraday observation (MAX_BY on fetched_at).
-- History wins whenever a day exists in both.
--
-- Incremental: each run recomputes only the (coin_id, price_date) days that
-- received new source rows since the last run — but recomputes each such day
-- from ALL of its source rows, so the current day's evolving close and any
-- late-arriving backfilled day both resolve correctly (no history/snapshot
-- priority inversion). `_synced_at` is the watermark (max source fetched_at).

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
        max_by(symbol, fetched_at)         as symbol,
        max_by(name, fetched_at)           as name,
        max_by(price_usd, fetched_at)      as close_price_usd,
        max_by(market_cap_usd, fetched_at) as market_cap_usd,
        max_by(volume_24h_usd, fetched_at) as volume_usd,
        max(fetched_at)                    as _synced_at
    from snap
    {% if is_incremental() %}
    where (coin_id, price_date) in (select coin_id, price_date from touched)
    {% endif %}
    group by coin_id, price_date
),

history_daily as (
    select
        coin_id,
        price_date,
        symbol,
        name,
        price_usd     as close_price_usd,
        market_cap_usd,
        volume_usd,
        fetched_at    as _synced_at
    from hist
    {% if is_incremental() %}
    where (coin_id, price_date) in (select coin_id, price_date from touched)
    {% endif %}
),

unioned as (
    select coin_id, symbol, name, price_date, close_price_usd, market_cap_usd, volume_usd, _synced_at, 'history'  as source_name from history_daily
    union all
    select coin_id, symbol, name, price_date, close_price_usd, market_cap_usd, volume_usd, _synced_at, 'snapshot' as source_name from snapshot_daily
),

prioritized as (
    select
        *,
        row_number() over (
            partition by coin_id, price_date
            order by case when source_name = 'history' then 0 else 1 end
        ) as _priority
    from unioned
)

select
    coin_id,
    symbol,
    name,
    price_date,
    close_price_usd,
    market_cap_usd,
    volume_usd,
    source_name,
    _synced_at
from prioritized
where _priority = 1
