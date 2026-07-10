{{
  config(
    materialized='incremental',
    unique_key=['coin_id', 'fetched_at'],
    on_schema_change='sync_all_columns'
  )
}}

-- Cleaned, typed, deduplicated 6-hourly market snapshot.
-- Source: CRYPTO_DB.STG.COINGECKO_RAW (append-only; the same coin/fetch can be
-- re-inserted, so we keep one row per (coin_id, fetched_at)).

with source as (

    select * from {{ source('landing', 'coingecko_raw') }}

    {% if is_incremental() %}
    where fetched_at > (select coalesce(max(fetched_at), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by id, fetched_at
            order by _loaded_at desc
        ) as _row_num
    from source

)

select
    id                                          as coin_id,
    upper(symbol)                               as symbol,
    name,
    cast(current_price as decimal(20, 8))       as price_usd,
    cast(market_cap as decimal(28, 2))          as market_cap_usd,
    cast(total_volume as decimal(28, 2))        as volume_24h_usd,
    cast(price_change_24h as decimal(20, 8))    as price_change_24h,
    round(price_change_pct_24h, 4)              as price_change_pct_24h,
    cast(high_24h as decimal(20, 8))            as high_24h,
    cast(low_24h as decimal(20, 8))             as low_24h,
    cast(circulating_supply as decimal(28, 2))  as circulating_supply,
    cast(ath as decimal(20, 8))                 as all_time_high,
    fetched_at,
    cast(fetched_at as date)                    as price_date,
    _loaded_at
from deduplicated
where _row_num = 1
  and id is not null
  and current_price > 0
