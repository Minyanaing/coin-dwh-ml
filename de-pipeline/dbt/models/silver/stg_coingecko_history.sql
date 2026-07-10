{{
  config(
    materialized='incremental',
    unique_key=['coin_id', 'price_date'],
    on_schema_change='sync_all_columns'
  )
}}

-- Cleaned, typed daily historical series — one row per coin per day.
-- Source: CRYPTO_DB.STG.COINGECKO_HISTORY_RAW (append-only; each backfill run
-- re-appends overlapping days, so we keep the latest fetch per (coin_id, day)).

with source as (

    select * from {{ source('landing', 'coingecko_history_raw') }}

    {% if is_incremental() %}
    where fetched_at > (select coalesce(max(fetched_at), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by id, price_date
            order by fetched_at desc, _loaded_at desc
        ) as _row_num
    from source

)

select
    id                                    as coin_id,
    upper(symbol)                         as symbol,
    name,
    cast(price_date as date)              as price_date,
    cast(price as decimal(20, 8))         as price_usd,
    cast(market_cap as decimal(28, 2))    as market_cap_usd,
    cast(total_volume as decimal(28, 2))  as volume_usd,
    fetched_at,
    _loaded_at
from deduplicated
where _row_num = 1
  and id is not null
  and price > 0
