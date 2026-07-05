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
  Fact Sales - Star Schema central fact table.

  Joins staged purchases with dimension tables to form the analytical core
  of the e-commerce lakehouse. Each row represents a single purchase event
  enriched with foreign keys to the date, product, and user dimensions.

  Grain: one row per purchase event (event_id).
  Partitioned by event_date for performant time-range scans.
*/

select
    -- Degenerate dimension (transaction identifier)
    p.event_id,

    -- Foreign keys to dimensions
    d.date_key                          as date_key,
    pr.product_id                       as product_id,
    u.user_id                           as user_id,

    -- Measures
    p.quantity,
    p.unit_price,
    p.gross_amount,

    -- Descriptive attributes kept at fact grain
    p.currency,
    p.channel,
    p.event_ts,
    p.event_date

from {{ ref('stg_purchases') }} p

inner join {{ ref('dim_date') }} d
    on p.event_date = d.date_key

inner join {{ ref('dim_products') }} pr
    on  p.product_id = pr.product_id
    and pr.is_current = true

inner join {{ ref('dim_users') }} u
    on  p.user_id = u.user_id
    and u.is_current = true
