{{
  config(
    materialized='table',
    properties={
      "format": "'PARQUET'"
    }
  )
}}

/*
  Date dimension table.

  Generates one row per calendar date covering the full range of event_date
  values found in the Silver purchases table, plus a 30-day buffer on each
  side for dashboards that need future/past context.

  Uses Trino's SEQUENCE function to generate the date series.
*/

with date_range as (
    select
        min(event_date) as min_date,
        max(event_date) as max_date
    from {{ ref('stg_purchases') }}
),

date_series as (
    select
        cast(t.date_value as date) as date_key
    from date_range dr
    cross join unnest(
        sequence(
            dr.min_date - interval '30' day,
            dr.max_date + interval '30' day,
            interval '1' day
        )
    ) as t(date_value)
)

select
    date_key,
    year(date_key)                                         as year,
    month(date_key)                                        as month,
    day(date_key)                                          as day_of_month,
    day_of_week(date_key)                                  as day_of_week,
    day_of_year(date_key)                                  as day_of_year,
    week(date_key)                                         as week_of_year,
    quarter(date_key)                                      as quarter,
    case when day_of_week(date_key) in (6, 7) then true else false end as is_weekend,
    date_format(date_key, '%Y-%m')                         as year_month,
    date_format(date_key, '%Y-Q')
        || cast(quarter(date_key) as varchar)              as year_quarter,
    date_trunc('month', date_key)                          as first_day_of_month,
    last_day_of_month(date_key)                            as last_day_of_month,
    date_trunc('week', date_key)                           as first_day_of_week
from date_series
