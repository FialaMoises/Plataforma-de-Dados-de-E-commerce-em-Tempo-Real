{{
  config(
    materialized='table',
    properties={
      "format": "'PARQUET'",
      "partitioning": "ARRAY['event_date']"
    }
  )
}}

/*
  Daily Revenue aggregation.

  Mirrors the Spark gold_aggregations.py logic:
    - Groups purchases by (event_date, currency).
    - Counts distinct event_ids as orders.
    - Sums quantity as items_sold.
    - Sums gross_amount as revenue (rounded to 2 decimals).
    - Derives avg_ticket = revenue / orders.

  Partitioned by event_date for efficient time-range queries from dashboards.
*/

select
    event_date,
    currency,
    count(distinct event_id)                          as orders,
    cast(sum(quantity) as bigint)                      as items_sold,
    round(sum(gross_amount), 2)                        as revenue,
    round(sum(gross_amount) / count(distinct event_id), 2) as avg_ticket
from {{ ref('stg_purchases') }}
group by event_date, currency
