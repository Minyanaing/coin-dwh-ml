{% snapshot snap_coin %}

{#
  SCD Type-2 history of coin attributes. dbt's snapshot mechanism adds
  dbt_valid_from / dbt_valid_to (and dbt_scd_id): a new version row is written
  only when one of the check_cols changes for a coin_id. This is the incremental
  engine behind the gold dim_coin dimension. Runs as part of `dbt build`.
#}

{{
  config(
    unique_key='coin_id',
    strategy='check',
    check_cols=['symbol', 'name', 'asset_type'],
  )
}}

with coins as (
    select coin_id, symbol, name from {{ ref('stg_coingecko_snapshot') }}
    union
    select coin_id, symbol, name from {{ ref('stg_coingecko_history') }}
),

deduplicated as (
    select
        coin_id,
        max(symbol) as symbol,
        max(name)   as name
    from coins
    group by coin_id
)

select
    coin_id,
    symbol,
    name,
    case
        when coin_id in ('tether', 'usd-coin', 'dai') then 'stablecoin'
        else 'coin'
    end as asset_type
from deduplicated

{% endsnapshot %}
