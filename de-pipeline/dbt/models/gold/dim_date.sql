{{ config(materialized='table') }}

-- Calendar date dimension. Keyed by an integer surrogate key (YYYYMMDD).
-- A static spine from 2024-01-01 to today comfortably covers all data.

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="dateadd(day, 1, current_date)"
    ) }}

)

select
    to_number(to_char(cast(date_day as date), 'YYYYMMDD'))  as date_sk,   -- PK
    cast(date_day as date)                                  as date_day,
    year(date_day)                                          as year,
    quarter(date_day)                                       as quarter,
    month(date_day)                                         as month,
    monthname(date_day)                                     as month_name,
    day(date_day)                                           as day_of_month,
    dayofweekiso(date_day)                                  as iso_day_of_week,
    case when dayofweekiso(date_day) in (6, 7) then true else false end as is_weekend
from spine
