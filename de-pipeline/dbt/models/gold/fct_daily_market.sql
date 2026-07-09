{{
  config(
    materialized='incremental',
    unique_key='daily_market_sk',
    on_schema_change='sync_all_columns'
  )
}}

-- Fact: one row per coin per day, from the unified daily grain (int_coin_daily).
-- The day-over-day return is computed with a window over the FULL per-coin
-- series (so lag() sees the prior day even on incremental runs); only rows
-- whose source data changed since the last run (_synced_at) are then merged.
-- coin_sk is resolved via an SCD Type-2 range join to dim_coin.

with daily as (

    -- full series (unfiltered) so the lag() below is always correct
    select * from {{ ref('int_coin_daily') }}

),

with_returns as (

    select
        coin_id,
        price_date,
        close_price_usd,
        market_cap_usd,
        volume_usd,
        _synced_at,
        lag(close_price_usd) over (
            partition by coin_id order by price_date
        ) as prev_close_price_usd
    from daily

)

select
    {{ dbt_utils.generate_surrogate_key(['wr.coin_id', 'wr.price_date']) }} as daily_market_sk,  -- PK
    dc.coin_sk,                                                -- FK -> dim_coin (version in effect)
    to_number(to_char(wr.price_date, 'YYYYMMDD'))  as date_sk, -- FK -> dim_date
    wr.price_date,
    wr.close_price_usd,
    wr.prev_close_price_usd,
    wr.market_cap_usd,
    wr.volume_usd,
    round(
        (wr.close_price_usd - wr.prev_close_price_usd) / nullif(wr.prev_close_price_usd, 0) * 100, 4
    ) as daily_return_pct,
    wr._synced_at   -- incremental watermark
from with_returns wr
left join {{ ref('dim_coin') }} dc
    on  wr.coin_id = dc.coin_id
    and cast(wr.price_date as timestamp_ntz) >= dc.valid_from
    and cast(wr.price_date as timestamp_ntz) <  dc.valid_to

{% if is_incremental() %}
where wr._synced_at > (select coalesce(max(_synced_at), '1900-01-01'::timestamp_ntz) from {{ this }})
{% endif %}
