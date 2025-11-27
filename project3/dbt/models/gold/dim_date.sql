{{ config(
    materialized='table'
) }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2005-01-01' as date)",
        end_date="date_add(day, 1, today())"
    ) }}
)

select
    toDate(date_day) as date_key,
    date_day as date,
    toYear(date_day) as year,
    toQuarter(date_day) as quarter,
    toMonth(date_day) as month,
    toDayOfMonth(date_day) as day,
    toDayOfWeek(date_day) as week_day,
    if(toDayOfWeek(date_day) in (6, 7), true, false) as is_weekend
from date_spine