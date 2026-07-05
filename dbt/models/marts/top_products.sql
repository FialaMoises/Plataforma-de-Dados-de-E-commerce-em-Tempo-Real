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
  Top Products by revenue per day.

  Mirrors the Spark gold_aggregations.py logic:
    - Groups by (event_date, product_id).
    - Ranks products by revenue descending within each day.
    - Keeps only the top 20 products per day.

  Partitioned by event_date for efficient time-range queries.
*/

with product_revenue as (
    select
        event_date,
        product_id,
        cast(sum(quantity) as bigint)   as items_sold,
        round(sum(gross_amount), 2)     as revenue
    from {{ ref('stg_purchases') }}
    group by event_date, product_id
),

ranked as (
    select
        event_date,
        product_id,
        items_sold,
        revenue,
        row_number() over (
            partition by event_date
            order by revenue desc
        ) as rank
    from product_revenue
)

select
    event_date,
    product_id,
    items_sold,
    revenue,
    cast(rank as integer) as rank
from ranked
where rank <= 20
