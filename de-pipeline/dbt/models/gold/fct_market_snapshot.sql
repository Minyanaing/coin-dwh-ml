{{
  config(
    materialized='incremental',
    unique_key='snapshot_sk',
    on_schema_change='sync_all_columns'
  )
}}

-- Fact: one row per coin per 6-hourly snapshot. Incremental on fetched_at.
-- coin_sk is resolved via an SCD Type-2 range join to dim_coin: the coin
-- version whose [valid_from, valid_to) contains the snapshot's fetched_at.

with snapshot as (

    select * from {{ ref('stg_coingecko_snapshot') }}

    {% if is_incremental() %}
    where fetched_at > (select coalesce(max(fetched_at), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}

),

keyed as (

    select
        {{ dbt_utils.generate_surrogate_key(['coin_id', 'fetched_at']) }} as snapshot_sk,  -- PK
        coin_id,
        fetched_at,
        price_date,
        price_usd,
        market_cap_usd,
        volume_24h_usd,
        price_change_24h,
        price_change_pct_24h,
        high_24h,
        low_24h,
        circulating_supply,
        all_time_high
    from snapshot

)

select
    k.snapshot_sk,
    dc.coin_sk,                                          -- FK -> dim_coin (version in effect)
    to_number(to_char(k.price_date, 'YYYYMMDD'))  as date_sk,  -- FK -> dim_date
    k.fetched_at,
    k.price_usd,
    k.market_cap_usd,
    k.volume_24h_usd,
    k.price_change_24h,
    k.price_change_pct_24h,
    k.high_24h,
    k.low_24h,
    k.circulating_supply,
    k.all_time_high
from keyed k
left join {{ ref('dim_coin') }} dc
    on  k.coin_id = dc.coin_id
    and k.fetched_at >= dc.valid_from
    and k.fetched_at <  dc.valid_to
