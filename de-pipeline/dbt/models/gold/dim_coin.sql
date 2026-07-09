{{ config(materialized='table') }}

-- SCD Type-2 coin dimension, built from the snap_coin snapshot.
-- One row per coin *version*; coin_sk is unique per version, coin_id is the
-- durable natural key. valid_from / valid_to / is_current expose the history.
--
-- The earliest version per coin is back-dated to 1900-01-01 so that daily/
-- snapshot facts whose event pre-dates when snapshotting began still resolve to
-- a version (the range join in the facts would otherwise leave them unmatched).

with snap as (

    select * from {{ ref('snap_coin') }}

),

versioned as (

    select
        *,
        row_number() over (partition by coin_id order by dbt_valid_from) as _version_num
    from snap

)

select
    {{ dbt_utils.generate_surrogate_key(['coin_id', 'dbt_valid_from']) }} as coin_sk,  -- PK (per version)
    coin_id,                                                                          -- durable natural key
    symbol,
    name,
    asset_type,
    case
        when _version_num = 1 then cast('1900-01-01' as timestamp_ntz)
        else dbt_valid_from
    end                                                        as valid_from,
    coalesce(dbt_valid_to, cast('9999-12-31' as timestamp_ntz)) as valid_to,
    (dbt_valid_to is null)                                      as is_current
from versioned
